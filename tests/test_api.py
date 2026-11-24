"""关键流程测试：受理占用、容量拒绝、幂等返回原单、丢回执保留峰值、查实稳定、查实断电释放。"""
from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import AppConfig, ServerConfig
from app.main import create_app


class FakePDU:
    """可编程 PDU 替身：记录通电状态，可注入合闸回执异常。

    实现与真实 PDU 相同的围栏语义：断电代号作废更早的在途合闸。
    """

    def __init__(self) -> None:
        self.powered: dict[str, bool] = {}
        self.stable: dict[str, bool] = {}
        self.close_errors: dict[str, Exception] = {}
        self.last_close: dict[str, int] = {}
        self.last_open: dict[str, int] = {}
        self.close_calls: list[tuple[str, int]] = []
        self.open_calls: list[tuple[str, int]] = []
        # 测试可挂钩子：在 close 执行前后插入并发操作
        self.on_close = None
        self.on_status = None

    async def close(self, server_id: str, generation: int) -> dict:
        self.close_calls.append((server_id, generation))
        if self.on_close is not None:
            await self.on_close(server_id)
        floor = max(
            [f for f in (self.last_close.get(server_id), self.last_open.get(server_id)) if f is not None],
            default=None,
        )
        if floor is not None and generation < floor:
            raise httpx.HTTPStatusError(
                "stale_close", request=httpx.Request("POST", "http://fake-pdu"),
                response=httpx.Response(409),
            )
        # 指令已生效、围栏已推进；回执可能在下面丢失（结果未明）
        self.powered[server_id] = True
        self.last_close[server_id] = max(generation, self.last_close.get(server_id, generation))
        if server_id in self.close_errors:
            raise self.close_errors[server_id]
        return {
            "receipt_id": f"rcpt-{server_id}",
            "server_id": server_id,
            "powered": True,
            "last_close_fence": self.last_close[server_id],
        }

    async def open(self, server_id: str, generation: int) -> dict:
        self.open_calls.append((server_id, generation))
        self.last_open[server_id] = max(generation, self.last_open.get(server_id, generation))
        self.powered[server_id] = False
        return {
            "server_id": server_id,
            "powered": False,
            "last_open_fence": self.last_open[server_id],
        }

    async def status(self, server_id: str) -> dict:
        # 与真实 PDU 一致：收到请求立刻快照，钩子（可等待）返回后交回旧快照，
        # 便于复现“断电前发出的查询现在才返回”。
        snapshot = {
            "server_id": server_id,
            "powered": self.powered.get(server_id, False),
            "stable": self.stable.get(server_id, False),
            "last_close_fence": self.last_close.get(server_id),
            "last_open_fence": self.last_open.get(server_id),
        }
        if self.on_status is not None:
            await self.on_status(server_id)
        return snapshot


CONFIG = AppConfig(
    branch_name="A-01",
    limit_amps=100.0,
    receipt_timeout_seconds=0.1,
    pdu_base_url="http://fake-pdu",
    servers={
        "srv-1": ServerConfig("srv-1", peak_amps=30.0, stable_amps=12.0),
        "srv-2": ServerConfig("srv-2", peak_amps=30.0, stable_amps=12.0),
        "srv-3": ServerConfig("srv-3", peak_amps=40.0, stable_amps=15.0),
        "srv-4": ServerConfig("srv-4", peak_amps=25.0, stable_amps=10.0),
    },
)


@pytest.fixture()
def env():
    pdu = FakePDU()
    client = TestClient(create_app(CONFIG, pdu))
    with client:  # 触发 lifespan（当前无额外逻辑，保持习惯）
        yield client, pdu


# ---------------- 受理与占用 ----------------

def test_close_accepted_occupies_peak(env):
    client, _ = env
    resp = client.post("/api/servers/srv-1/close")
    assert resp.status_code == 201
    body = resp.json()
    assert body["state"] == "STARTING"
    assert body["occupied_amps"] == 30.0
    assert body["deduplicated"] is False

    branch = client.get("/api/branch").json()
    assert branch["occupied_amps"] == 30.0
    assert branch["remaining_amps"] == 70.0

    # TestClient 会等后台任务结束，回执应已写入
    order = client.get(f"/api/orders/{body['order_id']}").json()
    assert order["receipt"]["receipt_id"] == "rcpt-srv-1"


def test_insufficient_capacity_rejected(env):
    client, _ = env
    for sid in ("srv-1", "srv-2", "srv-3"):  # 30+30+40 = 100A 正好占满
        assert client.post(f"/api/servers/{sid}/close").status_code == 201

    resp = client.post("/api/servers/srv-4/close")  # 还需 25A
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["remaining_amps"] == 0.0
    assert detail["required_peak_amps"] == 25.0

    # 被拒不产生占用
    branch = client.get("/api/branch").json()
    assert branch["occupied_amps"] == 100.0


# ---------------- 幂等：重复申请返回原单 ----------------

def test_duplicate_close_returns_original_order(env):
    client, _ = env
    first = client.post("/api/servers/srv-1/close").json()
    second = client.post("/api/servers/srv-1/close")
    assert second.status_code == 200
    body = second.json()
    assert body["order_id"] == first["order_id"]
    assert body["deduplicated"] is True
    assert client.get("/api/branch").json()["occupied_amps"] == 30.0  # 不重复占用


def test_duplicate_close_in_unknown_state_returns_original(env):
    client, pdu = env
    pdu.close_errors["srv-1"] = httpx.TimeoutException("simulated timeout")
    first = client.post("/api/servers/srv-1/close").json()
    stored = client.get(f"/api/orders/{first['order_id']}").json()
    assert stored["state"] == "UNKNOWN"

    second = client.post("/api/servers/srv-1/close").json()
    assert second["order_id"] == first["order_id"]
    assert second["deduplicated"] is True


# ---------------- 丢回执 / 结果未明 ----------------

def test_receipt_lost_marks_unknown_and_keeps_peak(env):
    client, pdu = env
    pdu.close_errors["srv-1"] = httpx.TimeoutException("simulated timeout")
    created = client.post("/api/servers/srv-1/close").json()

    order = client.get(f"/api/orders/{created['order_id']}").json()
    assert order["state"] == "UNKNOWN"
    assert order["occupied_amps"] == 30.0  # 峰值保留
    assert "TimeoutException" in order["error"]

    branch = client.get("/api/branch").json()
    assert branch["remaining_amps"] == 70.0


def test_other_servers_can_close_while_one_unknown(env):
    client, pdu = env
    pdu.close_errors["srv-1"] = httpx.TimeoutException("simulated timeout")
    client.post("/api/servers/srv-1/close")  # 30A，UNKNOWN

    resp = client.post("/api/servers/srv-2/close")  # 余量 70A，够 30A
    assert resp.status_code == 201
    assert client.get("/api/branch").json()["remaining_amps"] == 40.0


# ---------------- 查实稳定 ----------------

def test_verify_stable_switches_to_stable_current(env):
    client, pdu = env
    created = client.post("/api/servers/srv-1/close").json()
    pdu.stable["srv-1"] = True

    resp = client.post(f"/api/orders/{created['order_id']}/verify-stable")
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "RUNNING"
    assert body["occupied_amps"] == 12.0  # 30A 峰值 -> 12A 稳定

    branch = client.get("/api/branch").json()
    assert branch["occupied_amps"] == 12.0
    assert branch["remaining_amps"] == 88.0


def test_verify_stable_rejected_when_not_stable(env):
    client, _ = env
    created = client.post("/api/servers/srv-1/close").json()
    resp = client.post(f"/api/orders/{created['order_id']}/verify-stable")
    assert resp.status_code == 409
    assert "尚未稳定" in resp.json()["detail"]["error"]


# ---------------- 查实断电 + 释放 ----------------

def test_release_refused_while_powered_then_ok_after_open(env):
    client, _ = env
    created = client.post("/api/servers/srv-1/close").json()

    refused = client.post(f"/api/orders/{created['order_id']}/release")
    assert refused.status_code == 409
    assert "仍带电" in refused.json()["detail"]["error"]

    assert client.post("/api/servers/srv-1/open").status_code == 200
    released = client.post(f"/api/orders/{created['order_id']}/release")
    assert released.status_code == 200
    body = released.json()
    assert body["state"] == "RELEASED"
    assert body["occupied_amps"] == 0.0
    assert client.get("/api/branch").json()["remaining_amps"] == 100.0

    # 释放幂等
    again = client.post(f"/api/orders/{created['order_id']}/release")
    assert again.status_code == 200
    assert again.json()["state"] == "RELEASED"


def test_new_order_allowed_after_release(env):
    client, _ = env
    first = client.post("/api/servers/srv-1/close").json()
    client.post("/api/servers/srv-1/open")
    client.post(f"/api/orders/{first['order_id']}/release")

    second = client.post("/api/servers/srv-1/close")
    assert second.status_code == 201
    assert second.json()["order_id"] != first["order_id"]


# ---------------- 丢回执但实际合闸成功的完整链路 ----------------

def test_lost_receipt_but_actually_closed_full_flow(env):
    client, pdu = env
    # PDU 实际已合闸并稳定，但 close 回执超时
    pdu.close_errors["srv-1"] = httpx.TimeoutException("simulated timeout")
    pdu.powered["srv-1"] = True
    pdu.stable["srv-1"] = True

    created = client.post("/api/servers/srv-1/close").json()
    order = client.get(f"/api/orders/{created['order_id']}").json()
    assert order["state"] == "UNKNOWN"

    # 结果未明期间重复申请 -> 原单
    assert client.post("/api/servers/srv-1/close").json()["order_id"] == order["order_id"]

    # 按本次合闸单查实稳定 -> 转 RUNNING，按稳定电流计
    stable = client.post(f"/api/orders/{order['order_id']}/verify-stable").json()
    assert stable["state"] == "RUNNING"
    assert stable["occupied_amps"] == 12.0

    # 断电 -> 查实 -> 释放
    client.post("/api/servers/srv-1/open")
    released = client.post(f"/api/orders/{order['order_id']}/release").json()
    assert released["state"] == "RELEASED"
    assert client.get("/api/branch").json()["remaining_amps"] == 100.0


# ---------------- 查询与 404 ----------------

def test_branch_status_lists_all_servers(env):
    client, _ = env
    client.post("/api/servers/srv-1/close")
    branch = client.get("/api/branch").json()
    assert branch["branch"] == "A-01"
    assert branch["limit_amps"] == 100.0
    states = {s["server_id"]: s["state"] for s in branch["servers"]}
    assert states == {"srv-1": "STARTING", "srv-2": "OFF", "srv-3": "OFF", "srv-4": "OFF"}


def test_unknown_server_and_order_404(env):
    client, _ = env
    assert client.post("/api/servers/nope/close").status_code == 404
    assert client.post("/api/orders/nope/verify-stable").status_code == 404
    assert client.post("/api/orders/nope/release").status_code == 404
    assert client.get("/api/orders/nope").status_code == 404


# ================= 并发 / 迟到结果场景 =================
#
# PDU 查询在送出时就快照，返回的可能是“断电前”的旧状态。下面用同一个
# 事件循环里的 ASGITransport 精确卡住某次查询，在它返回前执行断电/释放/新单，
# 再放行迟到结果，验证它不会改变状态或占额。


def _blocking_status(pdu: FakePDU):
    """只卡住下一次 status 查询；返回 (放行事件, 已到达事件)。"""
    gate = asyncio.Event()
    reached = asyncio.Event()
    armed = {"on": True}

    async def hook(server_id: str) -> None:
        if not armed["on"]:
            return
        armed["on"] = False
        reached.set()
        await gate.wait()

    pdu.on_status = hook
    return gate, reached


async def _client_for(app):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    )


def test_late_stable_result_cannot_reoccupy_after_release():
    """断电前发出的稳定查询在释放后才返回：旧单不得复活或重新占额。"""

    async def scenario():
        pdu = FakePDU()
        app = create_app(CONFIG, pdu)
        async with await _client_for(app) as client:
            created = (await client.post("/api/servers/srv-1/close")).json()
            oid = created["order_id"]
            pdu.stable["srv-1"] = True
            gate, reached = _blocking_status(pdu)

            async def late_query():
                return await client.post(f"/api/orders/{oid}/verify-stable")

            task = asyncio.create_task(late_query())
            await asyncio.wait_for(reached.wait(), 2)

            # 查询已在 PDU 侧拿到“已稳定”快照，此时值班员断电并查实释放
            opened = await client.post("/api/servers/srv-1/open")
            assert opened.status_code == 200, opened.text
            released = await client.post(f"/api/orders/{oid}/release")
            assert released.status_code == 200, released.text
            assert released.json()["state"] == "RELEASED"

            gate.set()
            late = await asyncio.wait_for(task, 2)
            assert late.status_code == 409  # 迟到结果被丢弃

            order = (await client.get(f"/api/orders/{oid}")).json()
            assert order["state"] == "RELEASED"
            assert order["occupied_amps"] == 0.0
            branch = (await client.get("/api/branch")).json()
            assert branch["occupied_amps"] == 0.0
            assert branch["remaining_amps"] == 100.0

    asyncio.run(scenario())


def test_late_stable_result_does_not_touch_new_order_after_reopen():
    """旧单释放并按原规则开新单后，迟到的旧查询不得改变新旧单状态或占额。"""

    async def scenario():
        pdu = FakePDU()
        app = create_app(CONFIG, pdu)
        async with await _client_for(app) as client:
            first = (await client.post("/api/servers/srv-1/close")).json()
            first_id = first["order_id"]
            pdu.stable["srv-1"] = True
            gate, reached = _blocking_status(pdu)

            async def late_query():
                return await client.post(f"/api/orders/{first_id}/verify-stable")

            task = asyncio.create_task(late_query())
            await asyncio.wait_for(reached.wait(), 2)

            await client.post("/api/servers/srv-1/open")
            rel = await client.post(f"/api/orders/{first_id}/release")
            assert rel.status_code == 200, rel.text

            second = await client.post("/api/servers/srv-1/close")
            assert second.status_code == 201, second.text
            second_id = second.json()["order_id"]
            assert second_id != first_id

            gate.set()
            late = await asyncio.wait_for(task, 2)
            assert late.status_code == 409

            assert (await client.get(f"/api/orders/{first_id}")).json()["state"] == "RELEASED"
            second_now = (await client.get(f"/api/orders/{second_id}")).json()
            assert second_now["state"] == "STARTING"
            # 旧查询未把新单降为稳定电流，也未改变占额
            assert second_now["occupied_amps"] == 30.0
            assert (await client.get("/api/branch")).json()["occupied_amps"] == 30.0

    asyncio.run(scenario())


def test_release_refused_when_pdu_cannot_fence_pending_close():
    """PDU 未给出断电围栏 -> 无法确认在途合闸已作废，保留占额。"""

    async def scenario():
        pdu = FakePDU()
        pdu.close_errors["srv-1"] = httpx.TimeoutException("receipt lost")
        app = create_app(CONFIG, pdu)
        async with await _client_for(app) as client:
            created = (await client.post("/api/servers/srv-1/close")).json()
            oid = created["order_id"]
            await asyncio.sleep(0.05)  # 让后台回执任务落定

            pdu.powered["srv-1"] = False
            pdu.last_open.pop("srv-1", None)  # 无断电围栏
            resp = await client.post(f"/api/orders/{oid}/release")
            assert resp.status_code == 409, resp.text
            assert "无法确认" in resp.json()["detail"]["error"]

            branch = (await client.get("/api/branch")).json()
            assert branch["occupied_amps"] == 30.0
            assert (await client.get(f"/api/orders/{oid}")).json()["state"] == "UNKNOWN"

    asyncio.run(scenario())


def test_release_refused_when_open_fence_not_newer_than_close():
    """断电代号不晚于本单合闸代号 -> 迟到的合闸仍可能通电，保留占额。"""

    async def scenario():
        pdu = FakePDU()
        pdu.close_errors["srv-1"] = httpx.TimeoutException("receipt lost")
        app = create_app(CONFIG, pdu)
        async with await _client_for(app) as client:
            created = (await client.post("/api/servers/srv-1/close")).json()
            oid = created["order_id"]
            await asyncio.sleep(0.05)

            pdu.powered["srv-1"] = False
            pdu.last_open["srv-1"] = 1  # 早于本单合闸代号
            resp = await client.post(f"/api/orders/{oid}/release")
            assert resp.status_code == 409, resp.text
            assert (await client.get("/api/branch")).json()["occupied_amps"] == 30.0

    asyncio.run(scenario())


def test_open_while_receipt_pending_late_receipt_does_not_revive():
    """合闸回执在途时断电并释放；迟到回执不得让旧单复活或重新占额。"""

    async def scenario():
        pdu = FakePDU()
        gate = asyncio.Event()
        reached = asyncio.Event()

        async def on_close(server_id: str) -> None:
            reached.set()
            await gate.wait()

        pdu.on_close = on_close
        app = create_app(CONFIG, pdu)
        async with await _client_for(app) as client:
            # close 接口必须立即返回，不等待 PDU 回执
            created = (await client.post("/api/servers/srv-1/close")).json()
            oid = created["order_id"]
            await asyncio.wait_for(reached.wait(), 2)
            assert (await client.get(f"/api/orders/{oid}")).json()["state"] == "STARTING"

            # 回执在途，值班员仍可断电并查实释放
            opened = await client.post("/api/servers/srv-1/open")
            assert opened.status_code == 200, opened.text
            released = await client.post(f"/api/orders/{oid}/release")
            assert released.status_code == 200, released.text
            assert released.json()["state"] == "RELEASED"

            # 放行迟到的合闸回执：PDU 拒绝过期合闸，服务端只记审计
            gate.set()
            await asyncio.sleep(0.05)

            order = (await client.get(f"/api/orders/{oid}")).json()
            assert order["state"] == "RELEASED"
            assert order["occupied_amps"] == 0.0
            events = [h["event"] for h in order["history"]]
            assert "RECEIPT_LATE_IGNORED" in events
            assert pdu.powered["srv-1"] is False
            assert (await client.get("/api/branch")).json()["occupied_amps"] == 0.0

    asyncio.run(scenario())


def test_other_servers_can_close_while_waiting_for_pdu():
    """一次 PDU 查询卡住时，其他服务器仍能申请合闸（不持锁等 PDU）。"""

    async def scenario():
        pdu = FakePDU()
        app = create_app(CONFIG, pdu)
        async with await _client_for(app) as client:
            first = (await client.post("/api/servers/srv-1/close")).json()
            pdu.stable["srv-1"] = True
            gate, reached = _blocking_status(pdu)

            async def slow_verify():
                return await client.post(f"/api/orders/{first['order_id']}/verify-stable")

            task = asyncio.create_task(slow_verify())
            await asyncio.wait_for(reached.wait(), 2)

            other = await asyncio.wait_for(client.post("/api/servers/srv-2/close"), 2)
            assert other.status_code == 201, other.text

            gate.set()
            await asyncio.wait_for(task, 2)
            # srv-1 查实稳定降到 12A，srv-2 仍按峰值 30A
            assert (await client.get("/api/branch")).json()["occupied_amps"] == 42.0

    asyncio.run(scenario())


def test_open_fences_in_flight_close_so_late_close_cannot_power():
    """断电围栏晚于在途合闸：迟到的合闸指令被 PDU 拒绝，插座不会重新通电。"""

    async def scenario():
        pdu = FakePDU()
        gate = asyncio.Event()
        reached = asyncio.Event()

        async def on_close(server_id: str) -> None:
            # 模拟合闸指令在网络/队列中迟到：先卡住，等断电+释放完成再放行
            reached.set()
            await gate.wait()

        pdu.on_close = on_close
        app = create_app(CONFIG, pdu)
        async with await _client_for(app) as client:
            created = (await client.post("/api/servers/srv-1/close")).json()
            oid = created["order_id"]
            old_generation = created["close_generation"]
            await asyncio.wait_for(reached.wait(), 2)

            # 合闸指令尚未到达 PDU，值班员先断电并查实释放
            await client.post("/api/servers/srv-1/open")
            released = await client.post(f"/api/orders/{oid}/release")
            assert released.status_code == 200, released.text
            assert (await client.get("/api/branch")).json()["occupied_amps"] == 0.0

            # 放行迟到的合闸指令：PDU 侧围栏更高，拒绝执行
            gate.set()
            await asyncio.sleep(0.05)

            assert pdu.powered["srv-1"] is False
            assert pdu.last_close.get("srv-1") != old_generation
            assert (await client.get(f"/api/orders/{oid}")).json()["state"] == "RELEASED"
            assert (await client.get("/api/branch")).json()["occupied_amps"] == 0.0

    asyncio.run(scenario())



