"""批次接入接口与登记簿适配测试：上传、回执与继续作业的 HTTP 侧行为。

用 ASGI 直接调用（不依赖额外测试客户端库），确认接口对原始字节上传、
登记簿编号解析和错误状态的契约。
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable

import pytest

from app.fusion_engine import FusionEngine, fusion_engine
from app.main import app
from app.store import store


def call(method: str, path: str, body: bytes = b"", query: str = "",
         content_type: str = "text/csv") -> tuple[int, Any]:
    """调用 ASGI 应用并返回 (状态码, 响应体)。"""
    target = f"{path}?{query}" if query else path
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query.encode(),
        "root_path": "",
        "headers": [
            (b"host", b"testserver"),
            (b"content-type", content_type.encode()),
            (b"content-length", str(len(body)).encode()),
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    messages = [{"type": "http.request", "body": body, "more_body": False}]
    received: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        if messages:
            return messages.pop(0)
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        received.append(message)

    asyncio.run(app(scope, receive, send))

    status = next((m["status"] for m in received if m["type"] == "http.response.start"), 500)
    payload = b"".join(m.get("body", b"") for m in received if m["type"] == "http.response.body")
    try:
        return status, __import__("json").loads(payload or b"null")
    except ValueError:
        return status, payload


@pytest.fixture(autouse=True)
def clean_engine():
    """每个测试用干净的引擎与新增登记行，避免全局单例互相污染。"""
    original = fusion_engine.__dict__.copy()
    fusion_engine.__init__()  # 重新初始化全局单例的动态状态
    source_rows = list(store.rows("source_def"))
    job_rows = list(store.rows("fusion_job"))
    yield
    fusion_engine.__dict__.clear()
    fusion_engine.__dict__.update(original)
    store.rows("source_def")[:] = source_rows
    store.rows("fusion_job")[:] = job_rows


def register_source(code: str, name: str = "") -> int:
    status, payload = call("POST", "/api/source_def", body=__import__("json").dumps(
        {"values": {"源编号": code, "源名称": name or code, "源类型": "文件"}}).encode(),
        content_type="application/json")
    assert status == 200 and payload["ok"] is True
    return int(payload["entry"]["id"])


def register_job(code: str, sources: str, join_key: str = "id") -> int:
    status, payload = call("POST", "/api/fusion_job", body=__import__("json").dumps({
        "values": {"作业编号": code, "目标宽表": "订单宽表",
                   "参与数据源": sources, "关联键": join_key},
    }).encode(), content_type="application/json")
    assert status == 200 and payload["ok"] is True
    return int(payload["entry"]["id"])


def upload(source_id: int, content: bytes, batch_name: str = "orders.csv") -> tuple[int, Any]:
    return call("POST", f"/api/fusion/sources/{source_id}/batches", body=content,
                query=f"batch_name={batch_name}&file_name={batch_name}")


ORDERS = b"id,amount\n1,10\n2,20\n3,30\n"


class TestIngestHttp:
    def test_upload_and_duplicate_over_http(self) -> None:
        source_id = register_source("orders")
        status, payload = upload(source_id, ORDERS)
        assert status == 200
        assert payload["accepted"] is True
        assert payload["batch"]["有效行数"] == "3"

        _, again = upload(source_id, ORDERS)
        assert again["duplicate"] is True
        assert again["batch"]["批次编号"] == payload["batch"]["批次编号"]

        _, listing = call("GET", f"/api/fusion/sources/{source_id}/batches")
        assert len(listing["items"]) == 1

    def test_full_scenario_over_http(self) -> None:
        orders_id = register_source("orders")
        supplier_id = register_source("supplier")
        job_id = register_job("FUSI-ORDERS", "orders,supplier")

        v1 = b"id,amount\n1,10.00\n2,20.00\n3,30.00\n"
        v2 = b"id,amount\n1,10.00\n2,30.00\n3,30.00\n"

        _, first = upload(orders_id, v1)
        pinned = first["batch"]["批次编号"]
        upload(orders_id, v1)
        upload(orders_id, v1)

        _, started = call("POST", f"/api/fusion/jobs/{job_id}/start")
        assert started["blocked_sources"] == ["supplier"]
        assert started["receipts"][0]["受理批次"] == pinned

        _, partial = call("POST", f"/api/fusion/jobs/{job_id}/read",
                          query="rows_per_source=1")
        assert partial["consumed_rows"] == 1

        _, changed = upload(orders_id, v2)
        assert changed["batch"]["数据版本"] == "v2"

        status, continued = call("POST", f"/api/fusion/jobs/{job_id}/continue")
        assert status == 200
        assert continued["raw_records"] == 3
        receipt = continued["receipts"][0]
        assert receipt["受理批次"] == pinned
        assert receipt["已取行数"] == "3"
        amount_by_id = {r["id"]: r["amount"] for r in continued["resolved_records"]
                        if r["_source"] == "orders"}
        assert amount_by_id["2"] == "20.00"

        bad = b"name,amount\na,1\n"
        _, rejected = upload(supplier_id, bad, batch_name="supplier.csv")
        assert rejected["accepted"] is False
        assert "缺少标识字段：id" in rejected["message"]

        _, still_partial = call("POST", f"/api/fusion/jobs/{job_id}/continue")
        assert still_partial["already_completed"] is True
        assert still_partial["raw_records"] == 3

        _, fixed = upload(supplier_id, b"id,amount\n1,100\n", batch_name="supplier.csv")
        assert fixed["accepted"] is True

        _, completed = call("POST", f"/api/fusion/jobs/{job_id}/continue")
        assert completed["raw_records"] == 4
        assert completed["blocked_sources"] == []
        assert len(completed["resolved_records"]) == 4

    def test_run_view_exposes_lineage_to_file_and_row(self) -> None:
        orders_id = register_source("orders")
        job_id = register_job("FUSI-TRACE", "orders")
        upload(orders_id, ORDERS)
        call("POST", f"/api/fusion/jobs/{job_id}/start")
        call("POST", f"/api/fusion/jobs/{job_id}/continue")

        _, run = call("GET", f"/api/fusion/jobs/{job_id}/run")
        assert run["run_status"] == "已完成"
        first = run["records"][0]
        assert first["来源文件"] == "orders.csv"
        assert first["原数据行"] == "2"
        assert first["受理批次"].startswith("BATC-")

    def test_unknown_source_returns_404(self) -> None:
        status, _ = upload(999, ORDERS)
        assert status == 404

    def test_empty_body_returns_400(self) -> None:
        source_id = register_source("orders")
        status, _ = call("POST", f"/api/fusion/sources/{source_id}/batches", body=b"")
        assert status == 400

    def test_read_before_start_conflicts(self) -> None:
        job_id = register_job("FUSI-EARLY", "orders")
        status, _ = call("POST", f"/api/fusion/jobs/{job_id}/read")
        assert status == 409
