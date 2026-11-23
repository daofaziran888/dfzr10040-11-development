"""批次文件接入与融合取数接口：上传校验、版本确认、回执查询与继续作业。

上传接口接收原始文件字节（text/csv 或 application/octet-stream），
文件名与批次名称走查询参数，避免为 multipart 额外引入运行时依赖。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.fusion_engine import fusion_engine
from app.registry import resolve_job, resolve_source

router = APIRouter(prefix="/api/fusion", tags=["批次接入与融合取数"])

# 单次上传上限 8MB：批次文件是明细片段，超限应拆批而不是整文件灌内存。
MAX_UPLOAD_BYTES = 8 * 1024 * 1024


@router.post("/sources/{source_id}/batches")
async def upload_source_batch(
    source_id: int,
    request: Request,
    batch_name: str = Query(default=""),
    file_name: str = Query(default=""),
    identifier_fields: str = Query(default="id"),
) -> dict[str, Any]:
    """为已登记来源上传批次文件；同一批次名称的重复字节只确认一次。"""
    source = resolve_source(fusion_engine, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail=f"数据源 {source_id} 不存在")

    content = await request.body()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="文件超过 8MB，请拆分批次后上传")
    if not content:
        raise HTTPException(status_code=400, detail="上传内容为空，请携带文件字节")

    fields = [item.strip() for item in identifier_fields.split(",") if item.strip()]
    return fusion_engine.import_source_batch(
        source_code=source.source_code,
        batch_name=batch_name or file_name,
        file_name=file_name or batch_name,
        content=content,
        identifier_fields=fields,
    )


@router.get("/sources/{source_id}/batches")
def list_source_batches(source_id: int, batch_name: str = Query(default="")) -> dict[str, Any]:
    """列出某来源的批次版本，便于核对被替代版本与失败批次。"""
    source = resolve_source(fusion_engine, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail=f"数据源 {source_id} 不存在")
    items = fusion_engine.list_source_batches(source.source_code, batch_name)
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}")
def get_source_batch(batch_id: int) -> dict[str, Any]:
    """查看批次明细：校验错误、完整可消费行与来源行号。"""
    batch = fusion_engine.get_source_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"来源批次 {batch_id} 不存在")
    return batch.to_detail()


@router.post("/jobs/{job_id}/start")
def start_fusion_run(job_id: int) -> dict[str, Any]:
    """固定本作业的批次回执；未就绪来源会列入 blocked_sources。"""
    if resolve_job(fusion_engine, job_id) is None:
        raise HTTPException(status_code=404, detail=f"融合作业 {job_id} 不存在")
    result = fusion_engine.start_fusion_run(job_id)
    if result.get("message", "").endswith("不存在"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/jobs/{job_id}/run")
def get_fusion_run(job_id: int) -> dict[str, Any]:
    """查询取数状态、批次回执与逐行来源文件/行号。"""
    return fusion_engine.get_fusion_run(job_id)


@router.post("/jobs/{job_id}/read")
def read_fusion_batch(job_id: int, rows_per_source: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
    """按回执读取一批明细；用于先取一部分、再继续的续跑场景。"""
    if resolve_job(fusion_engine, job_id) is None:
        raise HTTPException(status_code=404, detail=f"融合作业 {job_id} 不存在")
    result = fusion_engine.read_fusion_batch(job_id, rows_per_source=rows_per_source)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message", "取数失败"))
    return result


@router.post("/jobs/{job_id}/continue")
def continue_fusion_job(job_id: int) -> dict[str, Any]:
    """继续作业：读取回执批次剩余明细并完成融合，输出关联受理批次。"""
    if resolve_job(fusion_engine, job_id) is None:
        raise HTTPException(status_code=404, detail=f"融合作业 {job_id} 不存在")
    return fusion_engine.continue_fusion_job(job_id)
