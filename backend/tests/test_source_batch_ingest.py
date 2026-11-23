"""来源批次文件接入与融合取数测试。

覆盖需求里的完整时序：同批重传只确认一次、改金额后生成新版本、
分批取数后回执仍指向旧版本、输出可追到文件与原始行号、
缺标识字段的来源单独重新接入且不重复计入已融合的行。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine

ORDERS_V1 = (
    "id,amount\n"
    "1,10.00\n"
    "2,20.00\n"
    "3,30.00\n"
    "4,40.00\n"
    "5,50.00\n"
).encode("utf-8")

# 供应方沿用 orders.csv 这个批次名称，只把第 2 行金额 20 改成 30，其余字节不变。
ORDERS_V2 = ORDERS_V1.replace(b"2,20.00", b"2,30.00")

SUPPLIER_MISSING_ID = (
    "name,amount\n"
    "supplier-a,100\n"
    "supplier-b,200\n"
).encode("utf-8")

SUPPLIER_OK = (
    "id,amount\n"
    "1,100.00\n"
    "2,200.00\n"
).encode("utf-8")


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


@pytest.fixture
def wired(engine: FusionEngine):
    """orders 来源已接入、supplier 来源尚未交文件的双来源作业。"""
    orders = engine.register_source(
        source_code="orders", source_name="orders.csv", source_type="文件",
        connection_string="file://orders.csv",
    )
    supplier = engine.register_source(
        source_code="supplier", source_name="supplier.csv", source_type="文件",
        connection_string="file://supplier.csv",
    )
    job = engine.create_fusion_job(
        target_wide_table="订单宽表",
        participant_sources=["orders", "supplier"],
        join_key="id",
    )
    return engine, orders, supplier, job


class TestBatchIngestion:
    """批次文件校验、摘要去重与版本判定。"""

    def test_first_upload_is_accepted(self, engine: FusionEngine) -> None:
        engine.register_source("orders", "orders.csv", "文件", "file://orders.csv")
        result = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)

        assert result["ok"] is True
        assert result["accepted"] is True
        assert result["duplicate"] is False
        batch = result["batch"]
        assert batch["数据版本"] == "v1"
        assert batch["有效行数"] == "5"
        assert batch["受理状态"] == "已受理"

    def test_identical_bytes_confirm_single_batch(self, engine: FusionEngine) -> None:
        engine.register_source("orders", "orders.csv", "文件", "file://orders.csv")
        first = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        second = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        third = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)

        assert second["duplicate"] is True
        assert third["duplicate"] is True
        assert third["batch"]["批次编号"] == first["batch"]["批次编号"]
        assert third["batch"]["重传次数"] == "2"

        batches = engine.list_source_batches("orders")
        assert len(batches) == 1
        assert batches[0]["受理状态"] == "已受理"

    def test_changed_bytes_create_new_version(self, engine: FusionEngine) -> None:
        engine.register_source("orders", "orders.csv", "文件", "file://orders.csv")
        first = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        second = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V2)

        assert second["duplicate"] is False
        assert second["batch"]["数据版本"] == "v2"
        assert second["batch"]["批次编号"] != first["batch"]["批次编号"]

        batches = engine.list_source_batches("orders", "orders.csv")
        assert [b["数据版本"] for b in batches] == ["v1", "v2"]
        assert batches[0]["受理状态"] == "已被新版本替代"
        assert batches[1]["受理状态"] == "已受理"

    def test_missing_identifier_rejects_whole_file(self, engine: FusionEngine) -> None:
        engine.register_source("supplier", "supplier.csv", "文件", "file://supplier.csv")
        result = engine.import_source_batch(
            "supplier", "supplier.csv", "supplier.csv", SUPPLIER_MISSING_ID)

        assert result["ok"] is False
        assert result["accepted"] is False
        assert "缺少标识字段：id" in result["message"]
        # 整批不受理：不暴露任何可消费行，融合不会读到半份文件。
        assert result["detail"]["rows"] == []
        assert result["detail"]["受理状态"] == "未受理"
        assert engine.get_effective_batch("supplier") is None

    def test_blank_identifier_row_rejects_whole_file(self, engine: FusionEngine) -> None:
        engine.register_source("supplier", "supplier.csv", "文件", "file://supplier.csv")
        content = b"id,amount\n1,10\n,20\n"
        result = engine.import_source_batch("supplier", "supplier.csv", "supplier.csv", content)
        assert result["accepted"] is False
        assert any("标识字段 id 为空" in error for error in result["detail"]["errors"])

    def test_unknown_source_is_rejected(self, engine: FusionEngine) -> None:
        result = engine.import_source_batch("ghost", "ghost.csv", "ghost.csv", ORDERS_V1)
        assert result["ok"] is False
        assert "未登记" in result["message"]

    def test_rows_keep_source_line_numbers(self, engine: FusionEngine) -> None:
        engine.register_source("orders", "orders.csv", "文件", "file://orders.csv")
        result = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        rows = result["detail"]["rows"]
        assert [row["__row_number__"] for row in rows] == [2, 3, 4, 5, 6]


class TestReceiptDrivenFusion:
    """回执固定批次、分批取数与继续作业。"""

    def test_receipt_pins_old_version_when_supplier_reuploads(self, wired) -> None:
        engine, _orders, _supplier, job = wired
        engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)

        started = engine.start_fusion_run(job.job_id)
        assert started["receipts"][0]["受理批次"].startswith("BATC-")
        pinned_batch_code = started["receipts"][0]["受理批次"]

        # 本次只先取到前 2 行
        partial = engine.read_fusion_batch(job.job_id, rows_per_source=2)
        assert partial["consumed_rows"] == 2
        assert partial["run_status"] == "部分取数"

        # 供应方同名称改金额后再次上传
        updated = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V2)
        assert updated["batch"]["数据版本"] == "v2"

        result = engine.continue_fusion_job(job.job_id)
        assert result["ok"] is True
        # 回执仍指向受理时的 v1 批次
        receipt = result["receipts"][0]
        assert receipt["受理批次"] == pinned_batch_code
        assert receipt["数据版本"] == "v1"
        assert receipt["已取行数"] == "5"

        orders_rows = {r["id"]: r for r in result["resolved_records"] if r["_source"] == "orders"}
        assert orders_rows["2"]["amount"] == "20.00"
        assert orders_rows["2"]["_batch_code"] == pinned_batch_code
        assert orders_rows["2"]["_batch_version"] == 1
        assert orders_rows["2"]["_row_number"] == 3
        assert orders_rows["2"]["_file_name"] == "orders.csv"

    def test_each_run_consumes_one_complete_file(self, wired) -> None:
        engine, _orders, _supplier, job = wired
        engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        started = engine.start_fusion_run(job.job_id)
        assert {r["来源编号"] for r in started["receipts"]} == {"orders"}
        assert started["blocked_sources"] == ["supplier"]

        engine.read_fusion_batch(job.job_id, rows_per_source=3)
        result = engine.continue_fusion_job(job.job_id)

        orders_rows = [r for r in result["resolved_records"] if r["_source"] == "orders"]
        # 5 行完整明细，没有因分批读取而重复或遗漏
        assert len(orders_rows) == 5
        assert sorted(int(r["_row_number"]) for r in orders_rows) == [2, 3, 4, 5, 6]

        run = engine.get_fusion_run(job.job_id)
        assert run["receipts"][0]["已取行数"] == "5"

    def test_output_links_back_to_batch_file_and_row(self, wired) -> None:
        engine, _orders, _supplier, job = wired
        uploaded = engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        engine.start_fusion_run(job.job_id)
        result = engine.continue_fusion_job(job.job_id)

        record = next(r for r in result["resolved_records"] if r["id"] == "1")
        assert record["_batch_code"] == uploaded["batch"]["批次编号"]
        assert record["_file_name"] == "orders.csv"
        assert record["_content_sha256"] == uploaded["batch"]["内容摘要"]
        assert record["_row_number"] == 2

        detail = engine.get_source_batch(uploaded["batch"]["id"]).to_detail()
        assert detail["受理状态"] == "已融合"
        assert detail["融合确认"] == "是"

    def test_completed_run_does_not_double_count(self, wired) -> None:
        engine, _orders, _supplier, job = wired
        engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        engine.start_fusion_run(job.job_id)
        first = engine.continue_fusion_job(job.job_id)
        assert first["raw_records"] == 5

        again = engine.continue_fusion_job(job.job_id)
        assert again["already_completed"] is True
        assert again["raw_records"] == 5

    def test_blocked_source_reconnected_separately(self, wired) -> None:
        engine, _orders, _supplier, job = wired
        engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        engine.start_fusion_run(job.job_id)

        broken = engine.import_source_batch(
            "supplier", "supplier.csv", "supplier.csv", SUPPLIER_MISSING_ID)
        assert broken["accepted"] is False

        partial = engine.continue_fusion_job(job.job_id)
        assert partial["run_status"] == "部分完成"
        assert partial["blocked_sources"] == ["supplier"]
        assert partial["raw_records"] == 5

        # 只针对 supplier 来源补交带 id 的文件，orders 不需要重来
        fixed = engine.import_source_batch(
            "supplier", "supplier.csv", "supplier.csv", SUPPLIER_OK)
        assert fixed["accepted"] is True

        completed = engine.continue_fusion_job(job.job_id)
        assert completed["run_status"] == "已完成"
        assert completed["blocked_sources"] == []
        # 先前已融合的 5 行不重复计入，只新增 supplier 的 2 行
        assert completed["raw_records"] == 7
        assert len(completed["resolved_records"]) == 7
        supplier_rows = [r for r in completed["resolved_records"] if r["_source"] == "supplier"]
        assert len(supplier_rows) == 2
        orders_rows = [r for r in completed["resolved_records"] if r["_source"] == "orders"]
        assert len(orders_rows) == 5

    def test_receipt_snapshot_of_valid_batch_size(self, wired) -> None:
        engine, _orders, _supplier, job = wired
        engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        started = engine.start_fusion_run(job.job_id)
        receipt = started["receipts"][0]
        assert receipt["有效行数"] == "5"
        assert receipt["批次名称"] == "orders.csv"
        assert receipt["来源文件"] == "orders.csv"
        assert len(receipt["内容摘要"]) == 64


class TestRunGuards:
    """取数过程的重复入口与拒绝批次的重传处理。"""

    def test_restart_after_partial_read_keeps_original_receipts(self, wired) -> None:
        engine, _orders, _supplier, job = wired
        engine.import_source_batch("orders", "orders.csv", "orders.csv", ORDERS_V1)
        started = engine.start_fusion_run(job.job_id)
        pinned = started["receipts"][0]["受理批次"]
        engine.read_fusion_batch(job.job_id, rows_per_source=2)

        retry = engine.start_fusion_run(job.job_id)
        assert retry["ok"] is False
        assert retry["receipts"][0]["受理批次"] == pinned

        result = engine.continue_fusion_job(job.job_id)
        orders_rows = [r for r in result["resolved_records"] if r["_source"] == "orders"]
        assert len(orders_rows) == 5

    def test_reuploading_rejected_bytes_stays_rejected(self, engine: FusionEngine) -> None:
        engine.register_source("supplier", "supplier.csv", "文件", "file://supplier.csv")
        first = engine.import_source_batch(
            "supplier", "supplier.csv", "supplier.csv", SUPPLIER_MISSING_ID)
        again = engine.import_source_batch(
            "supplier", "supplier.csv", "supplier.csv", SUPPLIER_MISSING_ID)

        assert first["accepted"] is False
        assert again["accepted"] is False
        assert again["ok"] is False
        assert again["batch"]["批次编号"] == first["batch"]["批次编号"]
        assert "仍未受理" in again["message"]
        assert len(engine.list_source_batches("supplier")) == 1

    def test_duplicate_id_rows_reject_file(self, engine: FusionEngine) -> None:
        engine.register_source("orders", "orders.csv", "文件", "file://orders.csv")
        result = engine.import_source_batch(
            "orders", "orders.csv", "orders.csv", b"id,amount\n1,10\n1,20\n")
        assert result["accepted"] is False
        assert any("重复" in error for error in result["detail"]["errors"])
