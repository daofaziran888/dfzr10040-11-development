"""来源批次文件接入：CSV 校验、内容摘要、批次版本与融合回执。

供应商按批次名称（通常就是文件名）反复上传同一个文件时：

* 文件内容一致 —— 视为同一次上传，只保留一个有效批次；
* 文件内容变化 —— 生成新的数据版本，旧版本作为审计记录保留；
* 文件缺标识字段或行级校验失败 —— 整批拒绝（不会出现半份文件被融合消费），
  失败批次可以针对该来源单独重新接入，不影响其它来源。

融合只从“来源批次回执”读取数据。回执在作业开始取数时固定到某个已受理批次，
因此批次被供应方再次覆盖上传时，本次作业仍会读完原版本的完整明细。
"""
from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any


DEFAULT_IDENTIFIER_FIELDS = ["id"]


class FileValidationError(Exception):
    """文件结构无法解析时的错误，携带可供页面展示的原因。"""


@dataclass
class ParsedFile:
    """一次 CSV 解析的结果：成功行、行数统计与校验原因。"""

    encoding: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    valid_row_count: int
    invalid_row_count: int
    errors: list[str]
    structural_errors: list[str] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return not self.errors and not self.structural_errors


@dataclass
class SourceBatch:
    """来源批次：某个数据源一次可校验的文件上传及其不可变的数据快照。"""

    batch_id: int
    batch_code: str
    source_id: int
    source_code: str
    batch_name: str
    version: int
    file_name: str
    content_sha256: str
    byte_size: int
    encoding: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    valid_row_count: int
    invalid_row_count: int
    errors: list[str]
    status: str
    accepted: bool
    received_at: str
    upload_attempts: int = 1
    duplicate_uploads: int = 0
    fusion_confirmed_at: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "id": self.batch_id,
            "批次编号": self.batch_code,
            "来源编号": self.source_code,
            "批次名称": self.batch_name,
            "数据版本": f"v{self.version}",
            "文件名": self.file_name,
            "内容摘要": self.content_sha256,
            "文件字节": str(self.byte_size),
            "字符编码": self.encoding,
            "数据行数": str(self.row_count),
            "有效行数": str(self.valid_row_count),
            "无效行数": str(self.invalid_row_count),
            "校验结果": "通过" if self.accepted else "未通过",
            "受理状态": self.status,
            "融合确认": "是" if self.fusion_confirmed_at else "否",
            "接收时间": self.received_at,
            "重传次数": str(self.duplicate_uploads),
        }

    def to_detail(self) -> dict[str, Any]:
        record = self.to_record()
        record.update({
            "columns": list(self.columns),
            "errors": list(self.errors),
            "rows": [dict(row) for row in self.rows],
        })
        return record


@dataclass
class SourceBatchReceipt:
    """来源批次回执：一次融合作业对某个来源所要读取的唯一有效批次。"""

    receipt_code: str
    job_id: int
    source_code: str
    batch_id: int
    batch_code: str
    batch_name: str
    batch_version: int
    file_name: str
    content_sha256: str
    valid_row_count: int
    row_count: int
    pinned_at: str
    consumed_rows: int = 0

    def to_record(self) -> dict[str, Any]:
        return {
            "回执编号": self.receipt_code,
            "融合作业": str(self.job_id),
            "来源编号": self.source_code,
            "受理批次": self.batch_code,
            "批次名称": self.batch_name,
            "数据版本": f"v{self.batch_version}",
            "来源文件": self.file_name,
            "内容摘要": self.content_sha256,
            "有效行数": str(self.valid_row_count),
            "已取行数": str(self.consumed_rows),
            "固定时间": self.pinned_at,
        }


@dataclass
class FusionRunState:
    """一次融合取数过程的状态：回执在本次运行内保持不变。"""

    job_id: int
    receipts: dict[str, SourceBatchReceipt] = field(default_factory=dict)
    cursors: dict[str, int] = field(default_factory=dict)
    blocked_sources: list[str] = field(default_factory=list)
    simulated_sources: list[str] = field(default_factory=list)
    status: str = "运行中"
    created_at: str = ""
    completed_at: str = ""

    def receipt_records(self) -> list[dict[str, Any]]:
        return [receipt.to_record() for receipt in self.receipts.values()]


def sha256_digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _decode(content: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return content.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise FileValidationError("文件无法按 UTF-8 或 GB18030 解码，请另存为文本 CSV 后重试")


def parse_csv_content(
    content: bytes,
    *,
    required_fields: list[str] | None = None,
) -> ParsedFile:
    """校验 CSV 文件并返回可消费的数据行。

    采用整批受理策略：任一行校验失败则整份文件不予受理，避免融合读到半份明细。
    每行保留原始行号（表头为第 1 行），供输出回溯到来源文件的原数据行。
    """
    required = [field.strip() for field in (required_fields or DEFAULT_IDENTIFIER_FIELDS)]
    required = [field for field in required if field]
    if not required:
        required = list(DEFAULT_IDENTIFIER_FIELDS)

    if not content.strip():
        raise FileValidationError("文件内容为空")

    text, encoding = _decode(content)
    try:
        reader = csv.DictReader(io.StringIO(text))
        raw_columns = reader.fieldnames
        if not raw_columns:
            raise FileValidationError("文件缺少表头")

        columns = [str(column).strip() for column in raw_columns]
        errors: list[str] = []
        structural_errors: list[str] = []

        seen: set[str] = set()
        for column in columns:
            if not column:
                structural_errors.append("表头存在空字段名")
            elif column in seen:
                structural_errors.append(f"表头字段重复：{column}")
            seen.add(column)
        for field_name in required:
            if field_name not in columns:
                structural_errors.append(f"缺少标识字段：{field_name}")

        valid_rows: list[dict[str, Any]] = []
        row_count = 0
        invalid_row_count = 0
        seen_ids: dict[str, int] = {}

        for raw_row in reader:
            if raw_row.get(None):
                row_count += 1
                invalid_row_count += 1
                errors.append(f"第 {reader.line_num} 行：列数超过表头字段数")
                continue

            row = {
                (str(key).strip() if key is not None else ""): (
                    value.strip() if isinstance(value, str) else value
                )
                for key, value in raw_row.items()
            }
            if not any(str(value or "").strip() for value in row.values()):
                continue

            row_count += 1
            line_number = reader.line_num
            row_errors: list[str] = []

            for field_name in required:
                # 整列缺失已在结构校验里报过，不再逐行重复。
                if field_name in columns and not str(row.get(field_name) or "").strip():
                    row_errors.append(f"第 {line_number} 行：标识字段 {field_name} 为空")

            identifier = ""
            if required[0] in columns:
                identifier = str(row.get(required[0]) or "").strip()
            if identifier:
                if identifier in seen_ids:
                    row_errors.append(
                        f"第 {line_number} 行：标识字段 {required[0]} 重复"
                        f"（首次出现在第 {seen_ids[identifier]} 行）"
                    )
                else:
                    seen_ids[identifier] = line_number

            amount = row.get("amount")
            if amount not in (None, ""):
                try:
                    Decimal(str(amount))
                except (InvalidOperation, ValueError):
                    row_errors.append(f"第 {line_number} 行：amount 不是有效数值")

            if row_errors:
                invalid_row_count += 1
                errors.extend(row_errors)
                continue

            parsed = dict(row)
            parsed["__row_number__"] = line_number
            valid_rows.append(parsed)

        errors = errors[:100]
        accepted = not errors and not structural_errors
        return ParsedFile(
            encoding=encoding,
            columns=columns,
            # 整批未通过校验时不暴露可消费行，杜绝半份文件进入融合。
            rows=valid_rows if accepted else [],
            row_count=row_count,
            valid_row_count=len(valid_rows),
            invalid_row_count=invalid_row_count,
            errors=errors + structural_errors,
            structural_errors=structural_errors,
        )
    except csv.Error as exc:
        raise FileValidationError(f"CSV 解析失败：{exc}") from exc
