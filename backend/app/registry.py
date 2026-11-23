"""登记簿适配：把管理页登记的来源/作业映射到融合引擎的领域对象。

管理页的增删改查落在内存数据仓库（store），融合引擎维护自己的领域对象。
上传批次或继续作业时，如果入参是登记簿里的编号，这里按编号把对象补齐到引擎，
并沿用同一编号，避免两套数据各说各话。
"""
from __future__ import annotations

from app.fusion_engine import FusionEngine
from app.models import FusionJob, SourceDef
from app.store import store


def resolve_source(engine: FusionEngine, source_id: int) -> SourceDef | None:
    """按登记簿优先的顺序解析数据源：登记簿 -> 引擎内部编号。"""
    row = store.find("source_def", source_id)
    if row is not None:
        code = str(row.get("源编号") or "").strip()
        if code:
            existing = engine._find_source_by_code(code)
            if existing is not None:
                return existing
            return engine.register_source(
                source_code=code,
                source_name=str(row.get("源名称") or code),
                source_type=str(row.get("源类型") or "文件"),
                connection_string=str(row.get("连接串") or ""),
                charset=str(row.get("字符编码") or "UTF-8"),
            )
    return engine.source_by_id(source_id)


def resolve_job(engine: FusionEngine, job_id: int) -> FusionJob | None:
    """按登记簿优先的顺序解析融合作业，必要时在引擎内补齐同名作业。"""
    existing = engine._fusion_jobs.get(job_id)
    row = store.find("fusion_job", job_id)
    if row is None:
        return existing

    job_code = str(row.get("作业编号") or "").strip()
    for job in engine._fusion_jobs.values():
        if job_code and job.job_code == job_code:
            return job

    participants = [
        item.strip() for item in str(row.get("参与数据源") or "").split(",") if item.strip()
    ]
    job = engine.create_fusion_job(
        target_wide_table=str(row.get("目标宽表") or ""),
        participant_sources=participants,
        join_key=str(row.get("关联键") or "id"),
        fusion_window=str(row.get("融合窗口") or "1d"),
        watermark_field=str(row.get("水位线字段") or "update_time"),
    )
    if job_code:
        job.job_code = job_code
    # 登记簿编号与引擎自增编号可能不同，按登记簿编号建一个别名，后续请求都能命中。
    engine._fusion_jobs[job_id] = job
    return job
