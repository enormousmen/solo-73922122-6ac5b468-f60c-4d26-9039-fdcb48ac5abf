#!/usr/bin/env python3
"""端到端并发/迟到场景验收脚本（对真实运行中的 PDU + API 进程）。

用法：
    python3 scripts/verify_concurrency.py [API_URL] [PDU_URL]
默认 API_URL=http://localhost:8000, PDU_URL=http://localhost:9000。

脚本会改动 PDU 的模拟模式/延迟，建议对全新启动的服务运行。
每个场景结束时自行清理，可重复执行。
"""
from __future__ import annotations

import asyncio
import json
import sys
import time

import httpx

API = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
PDU = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:9000"

results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, info: object = "") -> None:
    results.append((name, bool(cond), str(info)))
    print(("PASS " if cond else "FAIL ") + name + (f" | {info}" if info != "" else ""))


async def main() -> int:
    async with (
        httpx.AsyncClient(base_url=API, timeout=30) as api,
        httpx.AsyncClient(base_url=PDU, timeout=30) as pdu,
    ):
        # 先清理所有服务器的活动单，保证脚本可重复运行
        for sid in ("srv-1", "srv-2", "srv-3", "srv-4"):
            await pdu.post(f"/pdu/admin/mode/{sid}", params={"mode": "normal"})
            await pdu.post(
                f"/pdu/admin/delays/{sid}", params={"status_seconds": 0, "close_seconds": 0}
            )
            await api.post(f"/api/servers/{sid}/open")
        for order in (await api.get("/api/orders")).json():
            if order["state"] != "RELEASED":
                await api.post(f"/api/orders/{order['order_id']}/release")
        leftover = (await api.get("/api/branch")).json()["occupied_amps"]
        check("起点支路占用为 0", leftover == 0.0, leftover)

        # ---------------- 场景一：合闸指令迟到 ----------------
        print("\n[场景一] 合闸指令迟到：断电释放后不得重新通电")
        await pdu.post("/pdu/admin/mode/srv-1", params={"mode": "delayed-close"})
        await pdu.post("/pdu/admin/delays/srv-1", params={"close_seconds": 3.0})

        t0 = time.time()
        r = await api.post("/api/servers/srv-1/close")
        close_secs = time.time() - t0
        check("close 立即受理 201", r.status_code == 201, r.status_code)
        check("close 未被迟到 PDU 阻塞 (<1s)", close_secs < 1.0, f"{close_secs:.3f}s")
        oid = r.json()["order_id"]
        old_gen = r.json()["close_generation"]

        await asyncio.sleep(0.3)  # 合闸指令仍在 PDU 侧排队
        opened = await api.post("/api/servers/srv-1/open")
        check("合闸在途时断电成功", opened.status_code == 200, opened.json().get("powered"))
        rel = await api.post(f"/api/orders/{oid}/release")
        check("断电围栏晚于合闸 -> 允许释放", rel.status_code == 200, rel.json().get("state"))
        check(
            "释放后占用归零",
            (await api.get("/api/branch")).json()["occupied_amps"] == 0.0,
            (await api.get("/api/branch")).json()["occupied_amps"],
        )

        await asyncio.sleep(3.5)  # 让迟到合闸真正到达 PDU
        st = (await pdu.get("/pdu/status/srv-1")).json()
        check("迟到合闸未使插座重新通电", st["powered"] is False, st["powered"])
        check("旧合闸代号从未成为生效围栏", st["last_close_fence"] != old_gen,
              (st["last_close_fence"], old_gen))
        old = (await api.get(f"/api/orders/{oid}")).json()
        check("旧单保持 RELEASED", old["state"] == "RELEASED", old["state"])

        await pdu.post("/pdu/admin/mode/srv-1", params={"mode": "normal"})
        await pdu.post("/pdu/admin/delays/srv-1", params={"close_seconds": 0})
        new = await api.post("/api/servers/srv-1/close")
        check("旧单终结后按原规则可开新单", new.status_code == 201, new.status_code)
        new_gen = new.json()["close_generation"]
        await asyncio.sleep(0.4)
        st2 = (await pdu.get("/pdu/status/srv-1")).json()
        check("新单以自己的围栏正常通电",
              st2["powered"] is True and st2["last_close_fence"] == new_gen,
              (st2["powered"], st2["last_close_fence"], new_gen))
        # 清理场景一
        await api.post("/api/servers/srv-1/open")
        await api.post(f"/api/orders/{new.json()['order_id']}/release")

        # ---------------- 场景二：稳定状态查询迟到 ----------------
        print("\n[场景二] 稳定查询迟到：不得复活旧单或影响新单")
        await pdu.post("/pdu/admin/mode/srv-2", params={"mode": "normal"})
        await pdu.post("/pdu/admin/delays/srv-2", params={"status_seconds": 0})
        r = await api.post("/api/servers/srv-2/close")
        oid2 = r.json()["order_id"]
        await asyncio.sleep(6)  # PDU_BOOT_SECONDS=5
        st = (await pdu.get("/pdu/status/srv-2")).json()
        check("PDU 报告已通电且稳定", st["powered"] and st["stable"], st)

        await pdu.post("/pdu/admin/delays/srv-2", params={"status_seconds": 1.5})
        verify_task = asyncio.create_task(api.post(f"/api/orders/{oid2}/verify-stable"))
        await asyncio.sleep(0.4)  # 查询已快照并挂起

        await api.post("/api/servers/srv-2/open")
        rel2 = await api.post(f"/api/orders/{oid2}/release")
        check("查询在途时释放旧单成功", rel2.status_code == 200, rel2.json().get("state"))
        new2 = await api.post("/api/servers/srv-2/close")
        check("释放后按原规则开新单 201", new2.status_code == 201, new2.status_code)
        oid2b = new2.json()["order_id"]
        check("新单号不同于旧单", oid2b != oid2, oid2b)

        late = await verify_task
        check("迟到稳定查询被 409 拒绝", late.status_code == 409,
              late.json().get("detail", {}).get("error") if late.status_code == 409 else late.status_code)
        old_now = (await api.get(f"/api/orders/{oid2}")).json()
        new_now = (await api.get(f"/api/orders/{oid2b}")).json()
        check("旧单保持 RELEASED", old_now["state"] == "RELEASED", old_now["state"])
        check("新单状态/占额未被改动 (STARTING/30A)",
              new_now["state"] == "STARTING" and new_now["occupied_amps"] == 30.0,
              (new_now["state"], new_now["occupied_amps"]))
        check("支路占用只含新单 30A",
              (await api.get("/api/branch")).json()["occupied_amps"] == 30.0,
              (await api.get("/api/branch")).json()["occupied_amps"])

        await pdu.post("/pdu/admin/delays/srv-2", params={"status_seconds": 0})
        await api.post("/api/servers/srv-2/open")
        await api.post(f"/api/orders/{oid2b}/release")

        # ---------------- 场景三：丢回执 + 围栏释放 ----------------
        print("\n[场景三] 丢回执 -> UNKNOWN 保留峰值；断电后按围栏可释放")
        await pdu.post("/pdu/admin/mode/srv-3", params={"mode": "lost-receipt"})
        r = await api.post("/api/servers/srv-3/close")
        oid3 = r.json()["order_id"]
        await asyncio.sleep(4)  # receipt_timeout_seconds=3
        o3 = (await api.get(f"/api/orders/{oid3}")).json()
        check("丢回执后 UNKNOWN 且保留峰值 40A",
              o3["state"] == "UNKNOWN" and o3["occupied_amps"] == 40.0,
              (o3["state"], o3["occupied_amps"]))
        await api.post("/api/servers/srv-3/open")
        rel3 = await api.post(f"/api/orders/{oid3}/release")
        check("真实断电后按围栏释放成功", rel3.status_code == 200, rel3.json().get("state"))
        await pdu.post("/pdu/admin/mode/srv-3", params={"mode": "normal"})

        # ---------------- 场景四：等 PDU 不阻塞他人 ----------------
        print("\n[场景四] 等 PDU 期间其他服务器仍可合闸")
        r = await api.post("/api/servers/srv-3/close")
        oid4 = r.json()["order_id"]
        await asyncio.sleep(6)
        await pdu.post("/pdu/admin/delays/srv-3", params={"status_seconds": 1.5})
        slow = asyncio.create_task(api.post(f"/api/orders/{oid4}/verify-stable"))
        await asyncio.sleep(0.3)
        t0 = time.time()
        other = await api.post("/api/servers/srv-4/close")
        dt = time.time() - t0
        check("一次 PDU 查询卡住时其他服务器可合闸", other.status_code == 201, other.status_code)
        check("其他服务器合闸未被阻塞 (<1s)", dt < 1.0, f"{dt:.3f}s")
        check("在途稳定查询最终成功", (await slow).status_code == 200, "")
        await pdu.post("/pdu/admin/delays/srv-3", params={"status_seconds": 0})

        print("\n最终支路：", json.dumps((await api.get("/api/branch")).json(), ensure_ascii=False))

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
