"""单支路送电服务（FastAPI）。

业务规则：
1. 值班员申请合闸 -> 支路余量 >= 该服务器启动峰值才受理；
2. 受理即按峰值占用额度，后台等待 PDU 回执；
3. 回执超时/丢失 -> 合闸单置 UNKNOWN（结果未明），峰值额度继续保留；
4. 按本次合闸单查实设备已稳定 -> 占用从峰值改为稳定电流；
5. 查实合闸终止且设备断电 -> 释放额度，合闸单终结；
6. 同一服务器存在活动合闸单时重复申请 -> 200 返回原单（幂等）；
7. 等回执期间不阻塞，余量够的其他服务器可照常合闸。
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Protocol

from fastapi import FastAPI, HTTPException, Response

from .config import AppConfig, load_config
from .models import Order, OrderState
from .pdu_client import HttpxPDUClient
from .store import BranchStore

logger = logging.getLogger("feeder")


class PDUClient(Protocol):
    async def close(self, server_id: str, generation: int) -> dict: ...
    async def open(self, server_id: str, generation: int) -> dict: ...
    async def status(self, server_id: str) -> dict: ...


def order_view(order: Order, **extra: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "order_id": order.id,
        "server_id": order.server_id,
        "state": order.state.value,
        "occupied_amps": order.occupied_amps,
        "close_generation": order.close_generation,
        "open_generation": order.open_generation,
        "receipt": order.receipt,
        "error": order.error,
        "created_at": order.created_at,
        "updated_at": order.updated_at,
        "history": order.history,
    }
    data.update(extra)
    return data


def create_app(config: AppConfig, pdu: PDUClient) -> FastAPI:
    store = BranchStore(config)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        # 关闭时取消仍在等 PDU 回执的后台任务，避免悬挂
        tasks = list(receipt_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(title="单支路送电服务", version="1.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.config = config
    app.state.pdu = pdu
    # 持有等待回执的后台任务，避免被垃圾回收；close 接口不等待它们
    receipt_tasks: set[asyncio.Task] = set()
    app.state.receipt_tasks = receipt_tasks

    async def await_receipt(order_id: str, generation: int) -> None:
        """后台等待 PDU 回执。成功则记录回执；任何异常/超时都置 UNKNOWN，峰值保留。

        不持锁等待。结果返回后由 store 复核单据状态：
        若单据已被断电释放，迟到回执只留审计记录，不改状态与占额。
        """
        order = store.orders.get(order_id)
        if order is None:
            return
        try:
            receipt = await pdu.close(order.server_id, generation)  # type: ignore[union-attr]
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 超时、连接错误、PDU 5xx 均视为回执丢失
            async with store.lock:
                store.mark_unknown(order_id, f"{type(exc).__name__}: {exc}")
            logger.warning("合闸单 %s 回执丢失: %s", order_id, exc)
            return
        async with store.lock:
            store.mark_receipt(order_id, receipt)

    def server_config_or_404(server_id: str):
        cfg = config.servers.get(server_id)
        if cfg is None:
            raise HTTPException(status_code=404, detail=f"支路预设中没有该服务器: {server_id}")
        return cfg

    # ---------------- 合闸申请 ----------------
    @app.post("/api/servers/{server_id}/close", status_code=201)
    async def close_server(server_id: str, response: Response) -> dict[str, Any]:
        """申请合闸。余量够才受理；重复申请返回原单。"""
        cfg = server_config_or_404(server_id)

        async with store.lock:  # 临界区：查重 + 查余量 + 开单
            existing = store.active_order_for(server_id)
            if existing is not None:
                response.status_code = 200
                return order_view(
                    existing,
                    deduplicated=True,
                    detail="该服务器已有活动合闸单（启动中/结果未明/运行中），返回原单",
                )
            remaining = store.remaining_amps()
            if remaining + 1e-9 < cfg.peak_amps:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "支路余量不足，合闸申请不予受理",
                        "required_peak_amps": cfg.peak_amps,
                        "remaining_amps": remaining,
                        "limit_amps": config.limit_amps,
                        "occupied_amps": store.occupied_amps(),
                    },
                )
            order = store.create_order(server_id)

        # 回执等待放后台任务：不阻塞调用方，也不影响其他服务器申请合闸
        task = asyncio.create_task(await_receipt(order.id, order.close_generation))
        receipt_tasks.add(task)
        task.add_done_callback(receipt_tasks.discard)
        return order_view(order, deduplicated=False)

    # ---------------- PDU 断电指令 ----------------
    @app.post("/api/servers/{server_id}/open")
    async def open_server(server_id: str) -> dict[str, Any]:
        """指令 PDU 断开该服务器插座（值班员现场/远程断电操作）。

        断电代号先登记再下发；PDU 以此作废更早的在途合闸，因此本接口
        即使回执/后续查询迟到，也不会再让插座重新通电。
        """
        server_config_or_404(server_id)
        async with store.lock:
            order = store.active_order_for(server_id)
            token = store.begin_open(order, server_id)
        try:
            receipt = await pdu.open(server_id, token)  # type: ignore[union-attr]
        except Exception as exc:
            async with store.lock:
                store.fail_open(order, f"{type(exc).__name__}: {exc}")
            raise HTTPException(status_code=502, detail=f"PDU 断电指令失败: {type(exc).__name__}: {exc}")
        async with store.lock:
            store.finish_open(order, receipt, token)
        return receipt

    # ---------------- 查实稳定 ----------------
    @app.post("/api/orders/{order_id}/verify-stable")
    async def verify_stable(order_id: str) -> dict[str, Any]:
        """按本次合闸单向 PDU 查实：设备已通电且稳定 -> 改按稳定电流计。

        PDU 查询在锁外进行（不阻塞其他服务器申请合闸）；结果返回后再进锁复核
        单据是否仍是该服务器唯一活动单、PDU 围栏是否仍属于本单。
        迟到的旧查询结果不会改变新旧单状态或支路占额。
        """
        async with store.lock:
            order = store.orders.get(order_id)
            if order is None:
                raise HTTPException(status_code=404, detail="合闸单不存在")
            if order.state == OrderState.RELEASED:
                raise HTTPException(status_code=409, detail="合闸单已终结，额度已释放")
            if order.state == OrderState.RUNNING:
                return order_view(order, detail="该单已查实稳定，无需重复操作")
            server_id = order.server_id

        try:
            pdu_status = await pdu.status(server_id)  # type: ignore[union-attr]
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"查实失败，PDU 状态查询异常: {type(exc).__name__}: {exc}")

        async with store.lock:
            outcome, detail = store.apply_stable(order_id, pdu_status)
            order = store.orders[order_id]
            if outcome == "not_found":
                raise HTTPException(status_code=404, detail="合闸单不存在")
            if outcome in ("released", "superseded", "stale", "superseded_by_open"):
                raise HTTPException(status_code=409, detail=detail)
            if outcome == "not_stable":
                raise HTTPException(status_code=409, detail=detail)
            if outcome == "already_running":
                return order_view(order, detail="该单已查实稳定，无需重复操作")
            return order_view(order, **detail)

    # ---------------- 查实断电并释放 ----------------
    @app.post("/api/orders/{order_id}/release")
    async def release_order(order_id: str) -> dict[str, Any]:
        """查实合闸已终止且设备确实断电 -> 释放额度。

        只有 PDU 明确报告 powered=false 且断电围栏严格晚于本单合闸代号，
        即本单在途合闸已被作废、不可能再让插座通电时，才释放；
        否则 409 并继续保留占额。查询在锁外，复核在锁内。
        """
        async with store.lock:
            order = store.orders.get(order_id)
            if order is None:
                raise HTTPException(status_code=404, detail="合闸单不存在")
            if order.state == OrderState.RELEASED:
                return order_view(order, detail="该单额度已释放，操作幂等")
            server_id = order.server_id

        try:
            pdu_status = await pdu.status(server_id)  # type: ignore[union-attr]
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"查实失败，PDU 状态查询异常: {type(exc).__name__}: {exc}")

        async with store.lock:
            outcome, detail = store.apply_release(order_id, pdu_status)
            order = store.orders[order_id]
            if outcome == "not_found":
                raise HTTPException(status_code=404, detail="合闸单不存在")
            if outcome in ("powered", "unconfirmed", "superseded"):
                raise HTTPException(status_code=409, detail=detail)
            if outcome == "already_released":
                return order_view(order, detail="该单额度已释放，操作幂等")
            return order_view(order, **detail)

    # ---------------- 查询 ----------------
    @app.get("/api/orders")
    async def list_orders() -> list[dict[str, Any]]:
        return [order_view(o) for o in sorted(store.orders.values(), key=lambda o: o.created_at)]

    @app.get("/api/orders/{order_id}")
    async def get_order(order_id: str) -> dict[str, Any]:
        order = store.orders.get(order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="合闸单不存在")
        return order_view(order)

    @app.get("/api/branch")
    async def branch_status() -> dict[str, Any]:
        """支路总览：各机状态、占用电流与支路余量。"""
        servers = []
        for sid, cfg in config.servers.items():
            active = store.active_order_for(sid)
            servers.append(
                {
                    "server_id": sid,
                    "state": active.state.value if active else "OFF",
                    "occupied_amps": active.occupied_amps if active else 0.0,
                    "active_order_id": active.id if active else None,
                    "peak_amps": cfg.peak_amps,
                    "stable_amps": cfg.stable_amps,
                }
            )
        return {
            "branch": config.branch_name,
            "limit_amps": config.limit_amps,
            "occupied_amps": store.occupied_amps(),
            "remaining_amps": store.remaining_amps(),
            "servers": servers,
        }

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    async def root() -> dict[str, str]:
        return {"service": "single-feeder-power-service", "docs": "/docs", "branch": "/api/branch"}

    return app


# uvicorn app.main:app 使用的默认实例
config = load_config()
pdu_client = HttpxPDUClient(config.pdu_base_url, timeout=config.receipt_timeout_seconds)
app = create_app(config, pdu_client)
