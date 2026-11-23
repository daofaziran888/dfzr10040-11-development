"""熔炉引擎：跨模块数据融合流水线编排器。

负责执行数据源 -> Schema 映射 -> 宽表定义 -> 融合作业的完整链路，
并管理冲突仲裁、质量门禁、分区快照、水位追踪与增量归并。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.models import (
    BackfillTask,
    ColdStoreStrategy,
    CrossValidatePlan,
    DataDriftRecord,
    ExportPlan,
    FusionJob,
    FusionMonitorMetric,
    LineageEdge,
    MergeConflictRule,
    MetricAggrRule,
    PartitionRule,
    QualityGateRule,
    SchemaMapping,
    SnapshotPartition,
    SourceDef,
    WatermarkRecord,
    WideTableDef,
)
from app.ingest import (
    FusionRunState,
    ParsedFile,
    SourceBatch,
    SourceBatchReceipt,
    parse_csv_content,
    sha256_digest,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_code(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"


class FusionEngine:
    """熔炉引擎：编排跨模块数据融合流水线。"""

    def __init__(self) -> None:
        self._sources: dict[int, SourceDef] = {}
        self._schemas: dict[int, SchemaMapping] = {}
        self._wide_tables: dict[int, WideTableDef] = {}
        self._fusion_jobs: dict[int, FusionJob] = {}
        self._conflict_rules: dict[int, MergeConflictRule] = {}
        self._quality_gates: dict[int, QualityGateRule] = {}
        self._snapshots: dict[int, SnapshotPartition] = {}
        self._backfills: dict[int, BackfillTask] = {}
        self._cold_stores: dict[int, ColdStoreStrategy] = {}
        self._lineages: dict[int, LineageEdge] = {}
        self._metrics: dict[int, MetricAggrRule] = {}
        self._partitions: dict[int, PartitionRule] = {}
        self._watermarks: dict[int, WatermarkRecord] = {}
        self._exports: dict[int, ExportPlan] = {}
        self._cross_validates: dict[int, CrossValidatePlan] = {}
        self._drifts: dict[int, DataDriftRecord] = {}
        self._monitors: dict[int, FusionMonitorMetric] = {}
        self._batches: dict[int, SourceBatch] = {}
        self._batch_seq: dict[str, int] = {}
        self._runs: dict[int, FusionRunState] = {}
        self._run_records: dict[int, dict[str, dict[str, Any]]] = {}
        self._next_id: int = 1

    def _next_seq(self) -> int:
        sid = self._next_id
        self._next_id += 1
        return sid

    # ---- 1. source_def -> schema_map -> wide_table_def -> fusion_job ----

    def register_source(self, source_code: str, source_name: str, source_type: str,
                        connection_string: str, auth_method: str = "jdbc",
                        charset: str = "UTF-8", timezone: str = "Asia/Shanghai") -> SourceDef:
        sid = self._next_seq()
        source = SourceDef(
            source_id=sid, source_code=source_code, source_name=source_name,
            source_type=source_type, connection_string=connection_string,
            auth_method=auth_method, charset=charset, timezone=timezone,
            access_status="待验证",
        )
        self._sources[sid] = source
        return source

    def create_schema_mapping(self, source_table: str, target_wide_table: str,
                              field_mapping_json: str, type_conversion: str = "",
                              missing_policy: str = "skip") -> SchemaMapping:
        sid = self._next_seq()
        mapping = SchemaMapping(
            mapping_id=sid, plan_code=_generate_code("SCHE", sid),
            source_table=source_table, target_wide_table=target_wide_table,
            field_mapping_json=field_mapping_json,
            type_conversion_rules=type_conversion,
            missing_value_policy=missing_policy,
            mapping_order="ASC", plan_status="草稿",
        )
        self._schemas[sid] = mapping
        return mapping

    def define_wide_table(self, table_code: str, table_name: str,
                          field_list: list[str], primary_key: str = "id",
                          partition_key: str = "dt", ttl_days: int = 90) -> WideTableDef:
        sid = self._next_seq()
        wt = WideTableDef(
            table_id=sid, table_code=table_code, table_name=table_name,
            field_list=field_list, primary_key=primary_key,
            partition_key=partition_key, ttl_days=ttl_days,
            source_count=0, last_updated=_now_iso(),
        )
        self._wide_tables[sid] = wt
        return wt

    def create_fusion_job(self, target_wide_table: str, participant_sources: list[str],
                          join_key: str, fusion_window: str = "1d",
                          watermark_field: str = "update_time") -> FusionJob:
        sid = self._next_seq()
        job = FusionJob(
            job_id=sid, job_code=_generate_code("FUSI", sid),
            target_wide_table=target_wide_table,
            participant_sources=participant_sources,
            join_key=join_key, fusion_window=fusion_window,
            watermark_field=watermark_field,
            output_rows=0, skipped_rows=0,
        )
        self._fusion_jobs[sid] = job
        return job

    # ---- 2. fusion_job -> merge_conflict -> quality_gate -> snapshot_mgr ----

    def create_conflict_rule(self, target_wide_table: str, conflict_fields: list[str],
                             arbitration_strategy: str = "source_priority",
                             priority_source: str = "", time_baseline: str = "") -> MergeConflictRule:
        sid = self._next_seq()
        rule = MergeConflictRule(
            rule_id=sid, rule_code=_generate_code("MERG", sid),
            target_wide_table=target_wide_table,
            conflict_fields=conflict_fields,
            arbitration_strategy=arbitration_strategy,
            priority_source=priority_source,
            time_baseline=time_baseline or _now_iso(),
            conflict_count=0, rule_status="草稿",
        )
        self._conflict_rules[sid] = rule
        return rule

    def create_quality_gate(self, target_wide_table: str, check_type: str,
                            threshold_condition: str, trigger_action: str = "block",
                            block_level: str = "warning") -> QualityGateRule:
        sid = self._next_seq()
        gate = QualityGateRule(
            rule_id=sid, rule_code=_generate_code("QUAL", sid),
            target_wide_table=target_wide_table,
            check_type=check_type,
            threshold_condition=threshold_condition,
            trigger_action=trigger_action,
            block_level=block_level,
            checked_rows=0, last_triggered="",
        )
        self._quality_gates[sid] = gate
        return gate

    def create_snapshot(self, target_wide_table: str, partition_key_value: str,
                        dependency_partitions: list[str] | None = None,
                        schema_digest: str = "") -> SnapshotPartition:
        sid = self._next_seq()
        snap = SnapshotPartition(
            partition_id=sid,
            partition_code=_generate_code("SNAP", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            record_count=0,
            schema_digest=schema_digest or f"SHA256-{uuid.uuid4().hex[:8]}",
            status_mark="创建中",
            dependency_partitions=dependency_partitions or [],
            created_time=_now_iso(),
        )
        self._snapshots[sid] = snap
        return snap

    # ---- 3. fusion_job -> watermark_track -> delta_merge ----

    def create_watermark(self, data_source: str, current_watermark: str,
                         data_origin: str = "fusion_job") -> WatermarkRecord:
        sid = self._next_seq()
        wm = WatermarkRecord(
            record_id=sid, record_code=_generate_code("WATE", sid),
            data_source=data_source,
            current_watermark=current_watermark,
            previous_watermark="",
            advance_interval="5m",
            delay_minutes=0,
            data_origin=data_origin,
            last_advance=_now_iso(),
        )
        self._watermarks[sid] = wm
        return wm

    def advance_watermark(self, record_id: int, new_watermark: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(record_id)
        if wm is None:
            return None
        wm.previous_watermark = wm.current_watermark
        wm.current_watermark = new_watermark
        wm.last_advance = _now_iso()
        return wm

    # ---- 4. snapshot_mgr -> backfill_task -> cold_store ----

    def create_backfill_task(self, target_wide_table: str, backfill_range: str,
                             parallel_partitions: int = 4, dependency_check: str = "",
                             checkpoint_position: str = "") -> BackfillTask:
        sid = self._next_seq()
        task = BackfillTask(
            task_id=sid, task_code=_generate_code("BACK", sid),
            target_wide_table=target_wide_table,
            backfill_range=backfill_range,
            parallel_partitions=parallel_partitions,
            dependency_check=dependency_check,
            checkpoint_position=checkpoint_position,
            processed_rows=0, remaining_rows=0,
        )
        self._backfills[sid] = task
        return task

    def create_cold_store_strategy(self, target_wide_table: str, hot_data_days: int,
                                   cold_storage_type: str = "parquet",
                                   compression_method: str = "zstd",
                                   migration_window: str = "02:00-06:00") -> ColdStoreStrategy:
        sid = self._next_seq()
        cs = ColdStoreStrategy(
            strategy_id=sid, strategy_code=_generate_code("COLD", sid),
            target_wide_table=target_wide_table,
            hot_data_days=hot_data_days,
            cold_storage_type=cold_storage_type,
            migration_window=migration_window,
            compression_method=compression_method,
            migrated_rows=0, last_migration="",
        )
        self._cold_stores[sid] = cs
        return cs

    # ---- 5. wide_table_def -> lineage_edge -> metric_aggr ----

    def create_lineage_edge(self, upstream_node: str, downstream_node: str,
                            related_fields: list[str], transform_function: str = "",
                            edge_type: str = "direct", dependency_level: int = 1) -> LineageEdge:
        sid = self._next_seq()
        edge = LineageEdge(
            edge_id=sid, edge_code=_generate_code("LINE", sid),
            upstream_node=upstream_node, downstream_node=downstream_node,
            related_fields=related_fields,
            transform_function=transform_function,
            edge_type=edge_type,
            dependency_level=dependency_level,
            last_traced=_now_iso(),
        )
        self._lineages[sid] = edge
        return edge

    def create_metric_aggr_rule(self, target_wide_table: str, agg_function: str,
                                group_dimensions: list[str], time_granularity: str = "1d",
                                precompute_window: str = "7d",
                                storage_table: str = "") -> MetricAggrRule:
        sid = self._next_seq()
        rule = MetricAggrRule(
            rule_id=sid, rule_code=_generate_code("METR", sid),
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
            time_granularity=time_granularity,
            precompute_window=precompute_window,
            storage_table=storage_table or f"aggr_{target_wide_table}",
            last_computed="",
        )
        self._metrics[sid] = rule
        return rule

    # ---- 6. partition_rule -> cold_store -> archive ----

    def create_partition_rule(self, target_wide_table: str, partition_type: str,
                              partition_field: str, retention_days: int = 30,
                              archive_policy: str = "move_to_cold") -> PartitionRule:
        sid = self._next_seq()
        rule = PartitionRule(
            rule_id=sid, strategy_code=_generate_code("PART", sid),
            target_wide_table=target_wide_table,
            partition_type=partition_type,
            partition_field=partition_field,
            retention_days=retention_days,
            archive_policy=archive_policy,
            partition_count=0,
            last_adjusted=_now_iso(),
        )
        self._partitions[sid] = rule
        return rule

    # ---- 7. snapshot_mgr -> export_snapshot ----

    def create_export_plan(self, target_wide_table: str, export_format: str = "parquet",
                           partition_filter: str = "", field_filter: list[str] | None = None,
                           export_path: str = "/exports", row_limit: int = 0) -> ExportPlan:
        sid = self._next_seq()
        plan = ExportPlan(
            plan_id=sid, plan_code=_generate_code("EXPO", sid),
            target_wide_table=target_wide_table,
            export_format=export_format,
            partition_filter=partition_filter,
            field_filter=field_filter or [],
            export_path=export_path,
            row_limit=row_limit,
            export_status="待导出",
        )
        self._exports[sid] = plan
        return plan

    # ---- 8. cross_validate -> data_drift -> fusion_monitor ----

    def create_cross_validate_plan(self, primary_wide_table: str,
                                   reference_wide_table: str, join_key: str,
                                   validate_fields: list[str],
                                   deviation_threshold: str = "0.05") -> CrossValidatePlan:
        sid = self._next_seq()
        plan = CrossValidatePlan(
            plan_id=sid, plan_code=_generate_code("CROS", sid),
            primary_wide_table=primary_wide_table,
            reference_wide_table=reference_wide_table,
            join_key=join_key,
            validate_fields=validate_fields,
            deviation_threshold=deviation_threshold,
            last_result="",
            validate_status="待校验",
        )
        self._cross_validates[sid] = plan
        return plan

    def create_drift_record(self, wide_table_name: str, drift_type: str,
                            historical_baseline: str, current_distribution: str,
                            kl_divergence: str = "0.0",
                            drift_direction: str = "none") -> DataDriftRecord:
        sid = self._next_seq()
        record = DataDriftRecord(
            record_id=sid, record_code=_generate_code("DATA", sid),
            wide_table_name=wide_table_name,
            drift_type=drift_type,
            historical_baseline=historical_baseline,
            current_distribution=current_distribution,
            kl_divergence=kl_divergence,
            drift_direction=drift_direction,
            detected_at=_now_iso(),
        )
        self._drifts[sid] = record
        return record

    def create_fusion_monitor(self, fusion_job: str, monitor_dimension: str,
                              collection_interval: str = "60s",
                              alert_threshold: str = "",
                              alert_channel: str = "webhook",
                              silence_window: str = "") -> FusionMonitorMetric:
        sid = self._next_seq()
        metric = FusionMonitorMetric(
            metric_id=sid, metric_code=_generate_code("FUSI", sid),
            fusion_job=fusion_job,
            monitor_dimension=monitor_dimension,
            collection_interval=collection_interval,
            alert_threshold=alert_threshold,
            alert_channel=alert_channel,
            silence_window=silence_window,
            last_sample=_now_iso(),
        )
        self._monitors[sid] = metric
        return metric

    # ---- 来源批次文件接入 ----

    def import_source_batch(self, source_code: str, batch_name: str, file_name: str,
                            content: bytes,
                            identifier_fields: list[str] | None = None) -> dict[str, Any]:
        """接入来源批次文件：校验、内容摘要与版本判定。

        同一来源 + 同一批次名称下：
        * 字节完全一致的重传只确认一次，返回原受理批次；
        * 内容变化生成新版本，旧版本保留但不再参与新的融合取数；
        * 缺标识字段等校验失败时整批不受理，可按来源单独重新接入。
        """
        source = self._find_source_by_code(source_code)
        if source is None:
            return {"ok": False, "message": f"数据源 {source_code} 未登记，无法接入批次",
                    "accepted": False}

        batch_name = str(batch_name or "").strip() or str(file_name or "").strip()
        file_name = str(file_name or "").strip() or batch_name
        if not batch_name:
            return {"ok": False, "message": "批量名称与文件名不能同时为空", "accepted": False}

        digest = sha256_digest(content)
        previous = self._current_batch(source_code, batch_name)
        if previous is not None and previous.content_sha256 == digest:
            previous.upload_attempts += 1
            previous.duplicate_uploads += 1
            if previous.accepted:
                message = (
                    f"文件字节与{batch_name}当前有效批次 {previous.batch_code} 一致，"
                    "已按重传处理，不重复受理"
                )
            else:
                message = (
                    f"文件字节与{batch_name}最近一次批次 {previous.batch_code} 一致，"
                    f"该批次仍未受理（{'；'.join(previous.errors[:2])}），请修正文件后再上传"
                )
            return {
                "ok": previous.accepted,
                "accepted": previous.accepted,
                "message": message,
                "duplicate": True,
                "batch": previous.to_record(),
                "detail": previous.to_detail(),
            }

        try:
            parsed = parse_csv_content(content, required_fields=identifier_fields)
        except Exception as exc:  # FileValidationError 及无法解码的文本
            parsed = ParsedFile(
                encoding="unknown", columns=[], rows=[], row_count=0,
                valid_row_count=0, invalid_row_count=0,
                errors=[str(exc)], structural_errors=[str(exc)],
            )

        version = self._next_batch_version(source_code, batch_name)
        batch_id = self._next_seq()
        accepted = parsed.accepted
        status = "已受理" if accepted else "未受理"

        batch = SourceBatch(
            batch_id=batch_id,
            batch_code=_generate_code("BATC", batch_id),
            source_id=source.source_id,
            source_code=source_code,
            batch_name=batch_name,
            version=version,
            file_name=file_name,
            content_sha256=digest,
            byte_size=len(content),
            encoding=parsed.encoding,
            columns=parsed.columns,
            rows=parsed.rows,
            row_count=parsed.row_count,
            valid_row_count=parsed.valid_row_count,
            invalid_row_count=parsed.invalid_row_count,
            errors=parsed.errors,
            status=status,
            accepted=accepted,
            received_at=_now_iso(),
        )
        self._batches[batch_id] = batch
        if accepted and previous is not None:
            previous.status = "已被新版本替代"

        message = (
            f"批次 {batch.batch_code}（{batch_name} v{version}）已受理，"
            f"有效 {batch.valid_row_count} 行"
            if accepted
            else f"批次 {batch.batch_code}（{batch_name} v{version}）未受理：{'；'.join(batch.errors[:3])}"
        )
        return {
            "ok": accepted,
            "accepted": accepted,
            "duplicate": False,
            "message": message,
            "batch": batch.to_record(),
            "detail": batch.to_detail(),
        }

    def list_source_batches(self, source_code: str = "",
                            batch_name: str = "") -> list[dict[str, Any]]:
        """按来源或批次名称列出批次版本，供重新接入与核对版本使用。"""
        rows = [
            batch for batch in sorted(self._batches.values(), key=lambda b: b.batch_id)
            if (not source_code or batch.source_code == source_code)
            and (not batch_name or batch.batch_name == batch_name)
        ]
        return [batch.to_record() for batch in rows]

    def get_source_batch(self, batch_id: int) -> SourceBatch | None:
        return self._batches.get(batch_id)

    def source_by_id(self, source_id: int) -> SourceDef | None:
        return self._sources.get(source_id)

    def get_effective_batch(self, source_code: str,
                            batch_name: str = "") -> SourceBatch | None:
        """返回来源当前（最新版本）的可消费批次，未受理的批次不会返回。"""
        batches = [
            batch for batch in self._batches.values()
            if batch.source_code == source_code and batch.accepted
            and (not batch_name or batch.batch_name == batch_name)
        ]
        if not batches:
            return None
        return max(batches, key=lambda b: b.batch_id)

    def _current_batch(self, source_code: str, batch_name: str) -> SourceBatch | None:
        """取某来源某批次名称的最新版本，含未受理版本（用于重传去重）。"""
        batches = [
            batch for batch in self._batches.values()
            if batch.source_code == source_code and batch.batch_name == batch_name
        ]
        if not batches:
            return None
        return max(batches, key=lambda b: b.batch_id)

    def _next_batch_version(self, source_code: str, batch_name: str) -> int:
        key = f"{source_code}::{batch_name}"
        next_version = self._batch_seq.get(key, 0) + 1
        self._batch_seq[key] = next_version
        return next_version

    # ---- 融合作业的批次回执与分批取数 ----

    def start_fusion_run(self, job_id: int) -> dict[str, Any]:
        """开始一次融合取数：为每个参与来源固定一份批次回执。

        回执固定后，供应方再上传新版本也不会影响本次运行；
        缺少有效批次的来源被记入 blocked_sources，可单独重新接入后继续作业。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        prior = self._runs.get(job_id)
        if prior is not None and (
            prior.status != "运行中" or any(prior.cursors.values())
        ):
            return {
                "ok": False,
                "message": (
                    f"融合作业 {job.job_code} 本次取数已开始（{prior.status}），"
                    "请继续作业完成取数；重新取整批请新建作业"
                ),
                "run_status": prior.status,
                "receipts": prior.receipt_records(),
                "blocked_sources": list(prior.blocked_sources),
            }

        run = FusionRunState(job_id=job_id, status="运行中", created_at=_now_iso())
        for source_code in job.participant_sources:
            batch = self.get_effective_batch(source_code)
            if batch is None:
                run.blocked_sources.append(source_code)
                continue
            receipt = SourceBatchReceipt(
                receipt_code=_generate_code(f"RECT-{job.job_code}", len(run.receipts) + 1),
                job_id=job_id,
                source_code=source_code,
                batch_id=batch.batch_id,
                batch_code=batch.batch_code,
                batch_name=batch.batch_name,
                batch_version=batch.version,
                file_name=batch.file_name,
                content_sha256=batch.content_sha256,
                valid_row_count=batch.valid_row_count,
                row_count=batch.row_count,
                pinned_at=_now_iso(),
            )
            run.receipts[source_code] = receipt
            run.cursors[source_code] = 0

        self._runs[job_id] = run
        self._run_records[job_id] = {}
        job.run_status = "融合中"
        job.run_started_at = run.created_at
        job.run_completed_at = ""

        return {
            "ok": not run.blocked_sources,
            "job_code": job.job_code,
            "run_status": run.status,
            "receipts": run.receipt_records(),
            "blocked_sources": list(run.blocked_sources),
            "message": (
                "已为全部来源固定批次回执"
                if not run.blocked_sources
                else f"来源未就绪：{'、'.join(run.blocked_sources)}，请单独重新接入后再继续"
            ),
        }

    def read_fusion_batch(self, job_id: int, rows_per_source: int | None = None) -> dict[str, Any]:
        """按回执读取一批有效数据行；读完的文件以游标标记，不会重复计入。"""
        run = self._runs.get(job_id)
        if run is None:
            return {"ok": False, "message": "尚未开始取数，请先固定批次回执"}
        if run.status == "已完成":
            return {"ok": False, "message": "本次取数已完成，不会重复消费", "run_status": run.status}

        batch_number = self._run_records.get(job_id, {})
        consumed: list[dict[str, Any]] = []
        exhausted: list[str] = []
        job = self._fusion_jobs.get(job_id)
        join_key = job.join_key if job is not None else "id"

        for source_code, receipt in run.receipts.items():
            batch = self._batches.get(receipt.batch_id)
            if batch is None:
                continue
            cursor = run.cursors.get(source_code, 0)
            if cursor >= len(batch.rows):
                exhausted.append(source_code)
                continue
            limit = len(batch.rows) if rows_per_source is None else max(int(rows_per_source), 0)
            chunk = batch.rows[cursor:cursor + limit]
            for offset, raw in enumerate(chunk):
                row_number = int(raw.get("__row_number__", cursor + offset + 2))
                record = {key: value for key, value in raw.items() if key != "__row_number__"}
                return_key = record.get(join_key, "")
                record.update({
                    "_source": source_code,
                    "_fusion_key": f"{source_code}#{return_key}",
                    "_timestamp": _now_iso(),
                    "_version": batch.version,
                    "_batch_code": batch.batch_code,
                    "_batch_name": batch.batch_name,
                    "_batch_version": batch.version,
                    "_file_name": batch.file_name,
                    "_content_sha256": batch.content_sha256,
                    "_row_number": row_number,
                })
                record_key = f"{source_code}#{row_number}"
                batch_number[record_key] = record
                consumed.append(record)
            run.cursors[source_code] = cursor + len(chunk)
            receipt.consumed_rows = run.cursors[source_code]
            if run.cursors[source_code] < len(batch.rows):
                run.status = "部分取数"
            else:
                exhausted.append(source_code)

        self._run_records[job_id] = batch_number
        all_done = all(run.cursors[src] >= len(self._batches[rec.batch_id].rows)
                       for src, rec in run.receipts.items() if rec.batch_id in self._batches)
        if all_done and run.receipts:
            run.status = "取数完毕"

        return {
            "ok": True,
            "job_id": job_id,
            "run_status": run.status,
            "consumed_rows": len(consumed),
            "rows": consumed,
            "receipts": run.receipt_records(),
            "blocked_sources": list(run.blocked_sources),
            "exhausted_sources": exhausted,
        }

    def continue_fusion_job(self, job_id: int) -> dict[str, Any]:
        """继续取数并对本次已读明细执行映射、仲裁、门禁与快照。

        每次融合只消费回执所固定批次的一份完整明细（文件名 + 内容摘要一致），
        输出记录带批次与来源行号，可回溯到来源文件。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}
        run = self._runs.get(job_id)
        if run is None:
            started = self.start_fusion_run(job_id)
            if not started.get("ok") and not started.get("receipts"):
                return {"ok": False, "message": started.get("message", "无法开始取数")}
            run = self._runs[job_id]

        for source_code in list(run.blocked_sources):
            batch = self.get_effective_batch(source_code)
            if batch is None:
                continue
            run.blocked_sources.remove(source_code)
            receipt = SourceBatchReceipt(
                receipt_code=_generate_code(f"RECT-{job.job_code}", len(run.receipts) + 1),
                job_id=job_id,
                source_code=source_code,
                batch_id=batch.batch_id,
                batch_code=batch.batch_code,
                batch_name=batch.batch_name,
                batch_version=batch.version,
                file_name=batch.file_name,
                content_sha256=batch.content_sha256,
                valid_row_count=batch.valid_row_count,
                row_count=batch.row_count,
                pinned_at=_now_iso(),
            )
            run.receipts[source_code] = receipt
            run.cursors[source_code] = 0

        if run.status == "已完成":
            return {
                "ok": True,
                "already_completed": True,
                "message": f"融合作业 {job.job_code} 本次取数已完成，不会重复计入已融合的行",
                "job_code": job.job_code,
                "run_status": run.status,
                "raw_records": len(self._run_records.get(job_id, {})),
                "receipts": run.receipt_records(),
                "blocked_sources": list(run.blocked_sources),
            }

        read = self.read_fusion_batch(job_id)
        if not read.get("ok"):
            return {"ok": False, "message": read.get("message", "取数失败")}
        if not read.get("consumed_rows"):
            if run.blocked_sources:
                pending = "、".join(run.blocked_sources)
                message = (
                    f"融合作业 {job.job_code} 已取明细不再重复计入；"
                    f"来源 {pending} 仍缺少可受理批次，补齐后继续作业即可"
                )
            else:
                message = f"融合作业 {job.job_code} 没有新的待取明细，不会重复计入已融合的行"
            return {
                "ok": True,
                "already_completed": True,
                "message": message,
                "job_code": job.job_code,
                "run_status": run.status,
                "raw_records": len(self._run_records.get(job_id, {})),
                "receipts": run.receipt_records(),
                "blocked_sources": list(run.blocked_sources),
            }

        records = list(self._run_records.get(job_id, {}).values())
        mapped_records = self._apply_schema_mappings(records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "receipts": run.receipt_records(),
            "blocked_sources": list(run.blocked_sources),
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = {
            key: value for key, value in conflict_result.items() if key != "resolved"
        }
        resolved = conflict_result.get("resolved", mapped_records)
        result["resolved_records"] = resolved

        quality_result = self._apply_quality_gates(resolved, job.target_wide_table)
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(resolved)
            result["snapshot"] = {
                "partition_code": snap.partition_code,
                "record_count": snap.record_count,
            }
            for receipt in run.receipts.values():
                batch = self._batches.get(receipt.batch_id)
                if batch is not None:
                    batch.status = "已融合"
                    batch.fusion_confirmed_at = _now_iso()

        run.status = "已完成" if not run.blocked_sources else "部分完成"
        run.completed_at = _now_iso()
        job.run_status = "已完成" if run.status == "已完成" else "部分失败"
        job.run_completed_at = run.completed_at
        result["run_status"] = run.status
        result["confirmation"] = {
            "batches": [
                {
                    "来源编号": receipt.source_code,
                    "受理批次": receipt.batch_code,
                    "批次名称": receipt.batch_name,
                    "数据版本": f"v{receipt.batch_version}",
                    "来源文件": receipt.file_name,
                    "内容摘要": receipt.content_sha256,
                    "有效行数": receipt.valid_row_count,
                    "已取行数": receipt.consumed_rows,
                }
                for receipt in run.receipts.values()
            ],
            "blocked_sources": list(run.blocked_sources),
        }
        return result

    def get_fusion_run(self, job_id: int) -> dict[str, Any]:
        """查询作业当前取数状态、批次回执与逐行来源信息。"""
        run = self._runs.get(job_id)
        if run is None:
            return {"ok": True, "job_id": job_id, "run_status": "未开始",
                    "receipts": [], "blocked_sources": []}
        return {
            "ok": True,
            "job_id": job_id,
            "run_status": run.status,
            "created_at": run.created_at,
            "completed_at": run.completed_at,
            "receipts": run.receipt_records(),
            "blocked_sources": list(run.blocked_sources),
            "records": [
                {
                    "来源编号": record.get("_source"),
                    "受理批次": record.get("_batch_code"),
                    "来源文件": record.get("_file_name"),
                    "原数据行": str(record.get("_row_number", "")),
                    "内容摘要": record.get("_content_sha256"),
                    "数据": {k: v for k, v in record.items() if not k.startswith("_")},
                }
                for record in self._run_records.get(job_id, {}).values()
            ],
        }

    # ---- 复合融合操作 ----

    def execute_fusion_pipeline(self, job_id: int) -> dict[str, Any]:
        """执行完整融合流水线：数据源读取 -> 映射转换 -> 冲突仲裁 -> 质量检查 -> 产出快照。

        这是没有批次文件时的演示路径（按数据源模拟三条样例记录）。
        已登记来源的真实文件请走 import_source_batch + start_fusion_run + continue_fusion_job。
        返回值包含融合记录、冲突日志、质量报告和快照信息。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        raw_records: list[dict[str, Any]] = []
        for source_code in job.participant_sources:
            source = self._find_source_by_code(source_code)
            if source is None:
                continue
            source_records = self._simulate_source_read(source)
            for rec in source_records:
                rec["_source"] = source_code
                rec["_fusion_key"] = f"{source_code}#{rec.get(job.join_key, '')}"
                rec["_timestamp"] = _now_iso()
                rec["_version"] = 1
            raw_records.extend(source_records)

        mapped_records = self._apply_schema_mappings(raw_records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(raw_records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(raw_records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = conflict_result

        quality_result = self._apply_quality_gates(
            conflict_result.get("resolved", mapped_records),
            job.target_wide_table,
        )
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(conflict_result.get("resolved", mapped_records))
            result["snapshot"] = {"partition_code": snap.partition_code, "record_count": snap.record_count}

        return result

    def execute_delta_merge(self, job_id: int, delta_records: list[dict[str, Any]],
                            watermark_record_id: int = 0) -> dict[str, Any]:
        """执行增量归并：将增量数据合并到融合作业的输出中。"""
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        existing = {self._make_merge_key(r, job.join_key): r for r in job.merged_records}
        inserted = 0
        updated = 0
        deleted = 0

        for delta in delta_records:
            key = self._make_merge_key(delta, job.join_key)
            op = delta.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(delta)
                updated += 1
            else:
                existing[key] = dict(delta)
                inserted += 1

        job.merged_records = list(existing.values())

        if watermark_record_id > 0:
            wm = self._watermarks.get(watermark_record_id)
            if wm is not None:
                wm.previous_watermark = wm.current_watermark
                wm.current_watermark = _now_iso()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after_merge": len(job.merged_records),
        }

    def execute_backfill_with_checkpoint(self, backfill_id: int,
                                         source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行带断点续传的数据回填。

        从 checkpoint_position 处继续处理，失败时保留断点可恢复。
        """
        task = self._backfills.get(backfill_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}

        start_idx = 0
        if task.checkpoint_position:
            try:
                start_idx = int(task.checkpoint_position)
            except (ValueError, TypeError):
                start_idx = 0

        batch_size = max(task.parallel_partitions * 100, 100)
        end_idx = min(start_idx + batch_size, len(source_records))
        batch = source_records[start_idx:end_idx]

        processed = len(batch)
        task.processed_rows += processed
        task.remaining_rows = max(0, len(source_records) - end_idx)

        if end_idx < len(source_records):
            task.checkpoint_position = str(end_idx)
            complete = False
        else:
            task.checkpoint_position = ""
            complete = True

        return {
            "ok": True,
            "batch_processed": processed,
            "total_processed": task.processed_rows,
            "remaining": task.remaining_rows,
            "checkpoint": task.checkpoint_position,
            "complete": complete,
        }

    def migrate_to_cold_store(self, strategy_id: int,
                              hot_partition_cutoff: str) -> dict[str, Any]:
        """执行冷热分层迁移：将超过热数据保留期的分区迁移到冷存储。"""
        strategy = self._cold_stores.get(strategy_id)
        if strategy is None:
            return {"ok": False, "message": f"冷热分层策略 {strategy_id} 不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == strategy.target_wide_table
            and snap.partition_key_value < hot_partition_cutoff
        ]
        migrated = 0
        for snap in eligible_snaps:
            migrated += snap.record_count
            snap.status_mark = "已迁移至冷存储"

        strategy.migrated_rows += migrated
        strategy.last_migration = _now_iso()

        return {
            "ok": True,
            "migrated_partitions": len(eligible_snaps),
            "migrated_rows": migrated,
            "strategy_code": strategy.strategy_code,
        }

    def execute_export_from_snapshot(self, export_id: int,
                                     snapshot_id: int) -> dict[str, Any]:
        """从分区快照执行数据导出。"""
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}

        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        records = fusion_job.merged_records if fusion_job else []

        if plan.field_filter:
            records = [
                {k: v for k, v in rec.items() if k in plan.field_filter}
                for rec in records
            ]

        if plan.row_limit > 0 and len(records) > plan.row_limit:
            records = records[:plan.row_limit]

        plan.export_status = "已完成"

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "exported_rows": len(records),
            "format": plan.export_format,
            "path": plan.export_path,
            "records": records,
        }

    def execute_cross_validation(self, plan_id: int) -> dict[str, Any]:
        """执行跨宽表交叉校验，检测数据一致性偏差。"""
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_job = self._find_fusion_job_by_table(plan.primary_wide_table)
        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)

        primary_records = primary_job.merged_records if primary_job else []
        ref_records = ref_job.merged_records if ref_job else []

        primary_by_key = {str(r.get(plan.join_key, "")): r for r in primary_records}
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        drift_detected = False

        for key in set(primary_by_key.keys()) & set(ref_by_key.keys()):
            p = primary_by_key[key]
            r = ref_by_key[key]
            for field in plan.validate_fields:
                pv = str(p.get(field, ""))
                rv = str(r.get(field, ""))
                if pv != rv:
                    mismatches.append({
                        "key": key, "field": field,
                        "primary_value": pv, "reference_value": rv,
                    })
                    drift_detected = True

        plan.validate_status = "偏差告警" if mismatches else "一致"
        plan.last_result = json.dumps({
            "matched_keys": len(set(primary_by_key.keys()) & set(ref_by_key.keys())),
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
        }, ensure_ascii=False)

        result: dict[str, Any] = {
            "ok": True,
            "plan_code": plan.plan_code,
            "validate_status": plan.validate_status,
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
            "mismatches": mismatches[:20],
        }

        if drift_detected:
            drift_record = self.create_drift_record(
                wide_table_name=plan.primary_wide_table,
                drift_type="schema_mismatch",
                historical_baseline="baseline_" + _now_iso(),
                current_distribution="current_" + _now_iso(),
            )
            result["drift_record_code"] = drift_record.record_code

        return result

    def reconcile_partition_archive(self, partition_rule_id: int,
                                    cold_strategy_id: int) -> dict[str, Any]:
        """根据分区策略和冷热分层策略执行归档。

        将超出保留期的分区数据从冷存储转移到归档。
        """
        part_rule = self._partitions.get(partition_rule_id)
        cold_strat = self._cold_stores.get(cold_strategy_id)

        if part_rule is None or cold_strat is None:
            return {"ok": False, "message": "分区策略或冷热分层策略不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == part_rule.target_wide_table
            and snap.status_mark == "已迁移至冷存储"
        ]

        archived = 0
        for snap in eligible_snaps:
            snap.status_mark = "已归档"
            archived += snap.record_count

        return {
            "ok": True,
            "archived_partitions": len(eligible_snaps),
            "archived_rows": archived,
            "archive_policy": part_rule.archive_policy,
        }

    def reconcile_watermark_with_fusion(self, job_id: int) -> dict[str, Any]:
        """协调水位线与融合作业的推进状态。

        水位延迟时推进受限，融合作业未完成时增量归并暂停。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        related_watermarks = [
            wm for wm in self._watermarks.values()
            if wm.data_source in job.participant_sources
        ]

        stalled = [wm for wm in related_watermarks if wm.delay_minutes > 30]
        advancing = [wm for wm in related_watermarks if wm.delay_minutes <= 30]

        return {
            "ok": True,
            "job_code": job.job_code,
            "total_watermarks": len(related_watermarks),
            "advancing": len(advancing),
            "stalled": len(stalled),
            "can_proceed": len(stalled) == 0,
        }

    def build_wide_table_from_fusion(self, wide_table_id: int, fusion_job_id: int) -> dict[str, Any]:
        """从融合作业结果构建宽表定义，产生数据血缘边。"""
        wt = self._wide_tables.get(wide_table_id)
        job = self._fusion_jobs.get(fusion_job_id)

        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        if job is None:
            return {"ok": False, "message": f"融合作业 {fusion_job_id} 不存在"}

        wt.source_count = len(job.participant_sources)
        wt.last_updated = _now_iso()

        lineages: list[str] = []
        for source_code in job.participant_sources:
            edge = self.create_lineage_edge(
                upstream_node=source_code,
                downstream_node=wt.table_name,
                related_fields=wt.field_list,
                edge_type="fusion_map",
                dependency_level=1,
            )
            lineages.append(edge.edge_code)

        return {
            "ok": True,
            "table_code": wt.table_code,
            "source_count": wt.source_count,
            "lineage_edges": lineages,
            "last_updated": wt.last_updated,
        }

    def compute_metric_from_lineage(self, lineage_edge_id: int,
                                    target_wide_table: str,
                                    agg_function: str,
                                    group_dimensions: list[str]) -> dict[str, Any]:
        """基于数据血缘边计算指标聚合规则。"""
        edge = self._lineages.get(lineage_edge_id)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {lineage_edge_id} 不存在"}

        rule = self.create_metric_aggr_rule(
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
        )
        rule.last_computed = _now_iso()

        return {
            "ok": True,
            "metric_rule_code": rule.rule_code,
            "from_lineage": edge.edge_code,
            "agg_function": agg_function,
            "group_dimensions": group_dimensions,
        }

    # ---- 辅助方法 ----

    def _find_source_by_code(self, source_code: str) -> SourceDef | None:
        for source in self._sources.values():
            if source.source_code == source_code:
                return source
        return None

    def _find_fusion_job_by_table(self, wide_table_name: str) -> FusionJob | None:
        for job in self._fusion_jobs.values():
            if job.target_wide_table == wide_table_name:
                return job
        return None

    def _simulate_source_read(self, source: SourceDef) -> list[dict[str, Any]]:
        """模拟从数据源读取样本记录。"""
        return [
            {"id": f"{source.source_code}-rec-1", "name": f"{source.source_name}-rec1",
             "value": 100.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-2", "name": f"{source.source_name}-rec2",
             "value": 200.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-3", "name": f"{source.source_name}-rec3",
             "value": 300.0, "update_time": _now_iso()},
        ]

    def _apply_schema_mappings(self, records: list[dict[str, Any]],
                               target_wide_table: str) -> list[dict[str, Any]]:
        """应用 Schema 映射：对关联到目标宽表的映射方案进行字段转换。"""
        relevant = [
            m for m in self._schemas.values()
            if m.target_wide_table == target_wide_table
        ]
        if not relevant:
            return list(records)

        mapped: list[dict[str, Any]] = []
        for rec in records:
            source_table = rec.get("_source", "")
            applicable = [m for m in relevant if m.source_table == source_table]
            if applicable:
                new_rec = dict(rec)
                for mapping in applicable:
                    try:
                        field_map = json.loads(mapping.field_mapping_json)
                        for src_field, tgt_field in field_map.items():
                            if src_field in new_rec:
                                new_rec[tgt_field] = new_rec.pop(src_field)
                    except (json.JSONDecodeError, KeyError):
                        pass
                mapped.append(new_rec)
            else:
                mapped.append(rec)
        return mapped

    def _apply_conflict_resolution(self, records: list[dict[str, Any]],
                                   target_wide_table: str) -> dict[str, Any]:
        """应用冲突仲裁规则到融合记录。"""
        rules = [
            r for r in self._conflict_rules.values()
            if r.target_wide_table == target_wide_table and r.rule_status == "生效中"
        ]
        if not rules:
            return {"applied": False, "reason": "无生效的冲突规则", "resolved": records}

        resolved = list(records)
        all_conflicts: list[dict[str, Any]] = []
        for rule in rules:
            resolved, conflicts = rule.resolve(resolved)
            rule.conflict_count = len(conflicts)
            all_conflicts.extend(conflicts)

        return {
            "applied": True,
            "rules_used": len(rules),
            "conflicts_found": len(all_conflicts),
            "conflicts": all_conflicts[:50],
            "resolved": resolved,
        }

    def _apply_quality_gates(self, records: list[dict[str, Any]],
                             target_wide_table: str) -> dict[str, Any]:
        """应用质量门禁检查。"""
        gates = [
            g for g in self._quality_gates.values()
            if g.target_wide_table == target_wide_table
        ]
        if not gates:
            return {"passed": True, "gates_checked": 0, "violations": []}

        violations: list[dict[str, Any]] = []
        for gate in gates:
            gate.checked_rows = len(records)
            if gate.check_type == "row_count_min" and len(records) < int(gate.threshold_condition):
                violations.append({"rule_code": gate.rule_code, "check": "row_count_min",
                                   "actual": len(records), "threshold": gate.threshold_condition})
            elif gate.check_type == "null_check":
                null_count = sum(
                    1 for rec in records
                    if rec.get(gate.threshold_condition) is None
                )
                if null_count > 0:
                    violations.append({"rule_code": gate.rule_code, "check": "null_check",
                                       "null_count": null_count})

        passed = len(violations) == 0
        return {"passed": passed, "gates_checked": len(gates), "violations": violations}

    @staticmethod
    def _make_merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出引擎全局数据质量快照。"""
        return {
            "sources": len(self._sources),
            "schemas": len(self._schemas),
            "wide_tables": len(self._wide_tables),
            "fusion_jobs": len(self._fusion_jobs),
            "conflict_rules": len(self._conflict_rules),
            "quality_gates": len(self._quality_gates),
            "snapshots": len(self._snapshots),
            "backfills": len(self._backfills),
            "cold_stores": len(self._cold_stores),
            "lineages": len(self._lineages),
            "metrics": len(self._metrics),
            "partitions": len(self._partitions),
            "watermarks": len(self._watermarks),
            "exports": len(self._exports),
            "cross_validates": len(self._cross_validates),
            "drifts": len(self._drifts),
            "monitors": len(self._monitors),
            "source_batches": len(self._batches),
        }


# 全局单例供测试和跨模块消费
fusion_engine = FusionEngine()