"""PDU 模拟器（独立 FastAPI 服务）。

行为模式（/pdu/admin/mode/{server_id}?mode=...）：
- normal        : 正常合闸并在 PDU_RECEIPT_DELAY 秒后返回回执（默认）
- lost-receipt  : 实际合闸成功，但回执永不返回（客户端超时）——复现"合闸成功却丢回执"
- fail          : 直接返回 500，模拟 PDU/通信故障
- flaky         : 每次随机选上述三种行为
- delayed-close : 先等待 close_seconds 再真正执行合闸，用于复现"合闸指令迟到"

合闸/断电指令都携带单调递增的指令代号（generation，围栏令牌）：
- 每个插座记录最近一次生效的合闸代号 last_close_fence 与断电代号 last_open_fence；
- 若到达的合闸代号早于当前围栏（说明断电指令已先执行），PDU **拒绝合闸、绝不通电**，
  返回 409 stale_close；
- 状态查询返回这两个围栏，供送电服务确认"本单合闸已被更晚的断电作废"。

人为延迟（/pdu/admin/delays/{server_id}?status_seconds=&close_seconds=）：
- status_seconds > 0：状态查询在**收到请求时**先快照，再等待后返回快照，
  复现"断电前发出的稳定状态查询现在才返回"；
- close_seconds > 0：合闸请求先等待再生效，复现"迟到的合闸指令"。

合闸后持续通电 PDU_BOOT_SECONDS 秒，/pdu/status 才报 stable=true。
"""
from __future__ import annotations

import asyncio
import os
import random
import time
import uuid

from fastapi import FastAPI, HTTPException

BOOT_SECONDS = float(os.environ.get("PDU_BOOT_SECONDS", "5"))
RECEIPT_DELAY = float(os.environ.get("PDU_RECEIPT_DELAY", "0.2"))
DEFAULT_MODE = os.environ.get("PDU_DEFAULT_MODE", "normal")
VALID_MODES = {"normal", "lost-receipt", "fail", "flaky", "delayed-close"}

app = FastAPI(title="PDU 模拟器", version="1.1.0")

# server_id -> 插座状态（含单调围栏）
outlets: dict[str, dict] = {}
# server_id -> 行为模式覆盖
modes: dict[str, str] = {}
# server_id -> {"status_seconds": float, "close_seconds": float}
delays: dict[str, dict] = {}
# 保护单个插座的状态变更，避免并发合闸/断电交错
_locks: dict[str, asyncio.Lock] = {}


def _lock(server_id: str) -> asyncio.Lock:
    return _locks.setdefault(server_id, asyncio.Lock())


def _outlet(server_id: str) -> dict:
    return outlets.setdefault(
        server_id,
        {
            "powered": False,
            "on_since": None,
            "last_close_fence": None,
            "last_open_fence": None,
        },
    )


def _fence_floor(outlet: dict) -> int | None:
    fences = [f for f in (outlet["last_close_fence"], outlet["last_open_fence"]) if f is not None]
    return max(fences) if fences else None


def _delay(server_id: str, key: str) -> float:
    return float(delays.get(server_id, {}).get(key, 0.0))


def _status(server_id: str, outlet: dict | None = None) -> dict:
    outlet = outlet if outlet is not None else _outlet(server_id)
    powered = bool(outlet["powered"])
    uptime = round(time.time() - outlet["on_since"], 2) if powered and outlet["on_since"] else 0.0
    return {
        "server_id": server_id,
        "powered": powered,
        "stable": powered and uptime >= BOOT_SECONDS,
        "uptime_seconds": uptime,
        "boot_seconds_required": BOOT_SECONDS,
        "last_close_fence": outlet["last_close_fence"],
        "last_open_fence": outlet["last_open_fence"],
    }


@app.post("/pdu/close/{server_id}")
async def close_outlet(server_id: str, generation: int | None = None) -> dict:
    mode = modes.get(server_id, DEFAULT_MODE)
    if mode == "flaky":
        mode = random.choice(["normal", "lost-receipt", "fail"])

    if mode == "fail":
        raise HTTPException(status_code=500, detail="PDU 内部错误（模拟故障）")

    # 模拟"合闸指令在网络/队列里迟到"：先等一段时间再尝试生效
    close_delay = _delay(server_id, "close_seconds")
    if mode == "delayed-close" and close_delay <= 0:
        close_delay = 5.0
    if close_delay > 0:
        await asyncio.sleep(close_delay)

    outlet = _outlet(server_id)
    async with _lock(server_id):
        floor = _fence_floor(outlet)
        if generation is not None and floor is not None and generation < floor:
            # 关键安全行为：断电围栏已经高于本次合闸，说明断电先于本条迟到指令生效。
            # 绝不能重新通电；明确报告指令已过期。
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "stale_close: 合闸指令早于已生效的断电围栏，拒绝执行，插座保持断电",
                    "generation": generation,
                    "last_open_fence": outlet["last_open_fence"],
                    "last_close_fence": outlet["last_close_fence"],
                },
            )

        if not outlet["powered"]:  # 重复合闸不重置通电计时
            outlet["powered"] = True
            outlet["on_since"] = time.time()
        if generation is not None:
            outlet["last_close_fence"] = max(
                generation, outlet["last_close_fence"] or generation
            )

    if mode == "lost-receipt":
        # 关键行为：电已经送上，但回执丢弃，永远不响应本次请求
        await asyncio.sleep(3600)
        return {"unreachable": True}  # 实际不会到达

    await asyncio.sleep(RECEIPT_DELAY)
    return {
        "receipt_id": uuid.uuid4().hex[:8],
        "server_id": server_id,
        "outlet": server_id,
        "powered": True,
        "generation": generation,
        "last_close_fence": outlet["last_close_fence"],
        "ts": time.time(),
    }


@app.post("/pdu/open/{server_id}")
async def open_outlet(server_id: str, generation: int | None = None) -> dict:
    outlet = _outlet(server_id)
    async with _lock(server_id):
        # 断电永远立即生效；围栏只升不降，作废所有更早的在途合闸
        if generation is not None:
            outlet["last_open_fence"] = max(
                generation, outlet["last_open_fence"] or generation
            )
        outlet["powered"] = False
        outlet["on_since"] = None
        return {
            "receipt_id": uuid.uuid4().hex[:8],
            "server_id": server_id,
            "powered": False,
            "generation": generation,
            "last_open_fence": outlet["last_open_fence"],
            "ts": time.time(),
        }


@app.get("/pdu/status/{server_id}")
async def outlet_status(server_id: str) -> dict:
    # 收到请求先快照，再按配置延迟返回：模拟"查询结果迟到"。
    # 快照保证返回的是查询发出那一刻的旧状态，而不是延迟后的新状态。
    snapshot = _status(server_id)
    status_delay = _delay(server_id, "status_seconds")
    if status_delay > 0:
        await asyncio.sleep(status_delay)
    return snapshot


@app.get("/pdu/outlets")
async def all_outlets() -> dict:
    return {sid: _status(sid) for sid in outlets}


@app.post("/pdu/admin/mode/{server_id}")
async def set_mode(server_id: str, mode: str) -> dict:
    if mode not in VALID_MODES:
        raise HTTPException(status_code=400, detail=f"mode 必须是 {sorted(VALID_MODES)} 之一")
    modes[server_id] = mode
    return {"server_id": server_id, "mode": mode}


@app.get("/pdu/admin/mode")
async def get_modes() -> dict:
    return {"default": DEFAULT_MODE, "overrides": modes}


@app.post("/pdu/admin/delays/{server_id}")
async def set_delays(
    server_id: str,
    status_seconds: float = 0.0,
    close_seconds: float = 0.0,
) -> dict:
    if status_seconds < 0 or close_seconds < 0:
        raise HTTPException(status_code=400, detail="延迟不能为负")
    delays[server_id] = {"status_seconds": status_seconds, "close_seconds": close_seconds}
    return {"server_id": server_id, **delays[server_id]}


@app.get("/pdu/admin/delays/{server_id}")
async def get_delays(server_id: str) -> dict:
    return {
        "server_id": server_id,
        "status_seconds": _delay(server_id, "status_seconds"),
        "close_seconds": _delay(server_id, "close_seconds"),
    }


@app.get("/pdu/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
