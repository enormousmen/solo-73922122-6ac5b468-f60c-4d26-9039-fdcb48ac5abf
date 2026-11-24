"""支路台账：额度占用与合闸单生命周期（内存实现，单实例）。

并发安全要点：
1. 查余量、开单、状态流转都在同一把 ``lock`` 内完成，临界区里绝不 ``await`` PDU；
2. 每个“合闸/断电”申请都绑定一个单调递增的代号（fencing token），
   释放额度时要求 PDU 报告的断电围栏严格晚于本单合闸代号；
3. 外部查询（稳定状态）返回后再进锁复核：单据必须仍是该服务器唯一的活动单，
   且 PDU 报告的合闸围栏仍属于本单，否则视为迟到/过期结果，丢弃不改状态。
"""
from __future__ import annotations

import asyncio
import time

from .config import AppConfig
from .models import ACTIVE_STATES, Order, OrderState


def _now_token(previous: int | None) -> int:
    """生成严格单调递增的指令代号。

    以微秒级时间戳为主：进程重启后仍能大于重启前发出的代号（PDU 的围栏
    会保留），避免新进程从 1 开始被 PDU 当成过期指令。同一微秒内用
    ``previous + 1`` 兜底。
    """
    now = int(time.time() * 1_000_000)
    return now if previous is None else max(now, previous + 1)


class BranchStore:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.orders: dict[str, Order] = {}
        # 保护“查余量 -> 开单占用”以及所有状态流转的临界区
        self.lock = asyncio.Lock()
        # 每台服务器最近一次发出的合闸/断电指令代号
        self._server_tokens: dict[str, int] = {}

    # ---------------- 指令代号 ----------------
    def allocate_token(self, server_id: str) -> int:
        """为一次合闸或断电申请分配严格递增的代号（调用方需持有 lock）。"""
        token = _now_token(self._server_tokens.get(server_id))
        self._server_tokens[server_id] = token
        return token

    # ---------------- 额度 ----------------
    def occupied_amps(self) -> float:
        return round(
            sum(o.occupied_amps for o in self.orders.values() if o.state in ACTIVE_STATES),
            3,
        )

    def remaining_amps(self) -> float:
        return round(self.config.limit_amps - self.occupied_amps(), 3)

    # ---------------- 合闸单 ----------------
    def active_order_for(self, server_id: str) -> Order | None:
        """该服务器当前未终结的合闸单（启动中 / 结果未明 / 运行中）。"""
        for order in self.orders.values():
            if order.server_id == server_id and order.state in ACTIVE_STATES:
                return order
        return None

    def create_order(self, server_id: str) -> Order:
        peak = self.config.servers[server_id].peak_amps
        token = self.allocate_token(server_id)
        order = Order(server_id=server_id, occupied_amps=peak, close_generation=token)
        order.log(
            "ACCEPTED",
            f"受理合闸申请，按启动峰值 {peak}A 占用支路额度（指令代号 {token}）",
        )
        self.orders[order.id] = order
        return order

    # 回执/超时是后台任务的结果，可能在本单已被断电释放之后才到达。
    # 此时只记录审计事件，绝不改变状态与占额。
    def mark_receipt(self, order_id: str, receipt: dict) -> None:
        order = self.orders[order_id]
        if order.state != OrderState.STARTING:
            order.log("RECEIPT_LATE_IGNORED", "回执到达时合闸单已不在启动中，忽略该结果")
            return
        order.receipt = receipt
        order.log("RECEIPT_OK", "收到 PDU 合闸回执，设备启动中，继续按峰值占用")

    def mark_unknown(self, order_id: str, error: str) -> None:
        order = self.orders[order_id]
        if order.state != OrderState.STARTING:
            order.log("RECEIPT_LATE_IGNORED", f"回执失败结果到达时单据已不在启动中: {error}")
            return
        order.state = OrderState.UNKNOWN
        order.error = error
        order.log("RECEIPT_TIMEOUT", f"回执超时/丢失，结果未明，峰值额度继续保留: {error}")

    # ---------------- 断电申请 ----------------
    def begin_open(self, order: Order | None, server_id: str) -> int:
        """在等待 PDU 之前先登记断电代号，保证其严格晚于本单合闸代号。"""
        token = self.allocate_token(server_id)
        if order is not None:
            order.open_generation = token
            order.log("OPEN_SUBMITTED", f"向 PDU 下发断电指令（代号 {token}）")
        return token

    def finish_open(self, order: Order | None, receipt: dict, token: int) -> None:
        if order is None:
            return
        if order.state in ACTIVE_STATES:
            order.log(
                "OPEN_CONFIRMED",
                f"PDU 已执行断电，代号 {token}，断电围栏 {receipt.get('last_open_fence')}",
            )
        else:
            order.log("OPEN_CONFIRMED_AFTER_RELEASE", f"断电完成时额度已释放（代号 {token}）")

    def fail_open(self, order: Order | None, error: str) -> None:
        if order is not None and order.state in ACTIVE_STATES:
            order.log("OPEN_FAILED", f"PDU 断电指令失败，需重新查实: {error}")

    # ---------------- 查实稳定 ----------------
    def apply_stable(self, order_id: str, pdu_status: dict) -> tuple[str, dict]:
        """在锁内复核并应用稳定查询结果。

        返回 (outcome, detail)；只有 outcome == "ok" 才会改变状态/占额。
        任何"无法确认这份快照属于本单当前设备状态"的情形都返回非 ok，调用方
        据此拒绝（409），从而保证迟到的旧查询不会改变状态或占额。
        """
        order = self.orders.get(order_id)
        if order is None:
            return "not_found", {}
        if order.state == OrderState.RELEASED:
            # 迟到的旧查询：旧单已终结，绝不能复活或重新占额
            return "released", {"error": "合闸单已终结并释放额度，迟到的查询结果被丢弃"}
        if order.state == OrderState.RUNNING:
            return "already_running", {}
        active = self.active_order_for(order.server_id)
        if active is not order:
            # 该服务器已有更新的合闸单，旧单查询结果不得影响新单
            return "superseded", {
                "error": "该服务器已有更新的活动合闸单，旧单的迟到查询结果被丢弃",
                "active_order_id": active.id if active else None,
            }

        if not (pdu_status.get("powered") and pdu_status.get("stable")):
            return "not_stable", {"error": "查实未通过：设备尚未通电或尚未稳定", "pdu_status": pdu_status}

        # 查询期间值班员已下发断电：这份“已稳定”快照是断电前的旧状态，
        # 不能据此把占额从峰值降到稳定电流（否则等于提前释放额度）。
        # 无论 PDU 是否已执行断电，稳定结果都已过期，占用维持峰值，
        # 等待 release 按断电围栏查实。
        if order.open_generation is not None:
            return "superseded_by_open", {
                "error": "该单已下发断电指令，断电前发出的稳定查询结果已过期，占用维持峰值",
                "open_generation": order.open_generation,
                "pdu_status": pdu_status,
            }

        # PDU 报告的合闸围栏必须正是本单的合闸代号；若已被后续断电/合闸覆盖，
        # 这份查询结果描述的不是本单设备状态，不能据此改占额。
        close_fence = pdu_status.get("last_close_fence", pdu_status.get("fence"))
        if close_fence != order.close_generation:
            return "stale", {
                "error": "PDU 状态已不属于本合闸单（存在更新的合闸/断电指令），查询结果被丢弃",
                "expected_close_generation": order.close_generation,
                "pdu_last_close_fence": close_fence,
            }

        stable = self.config.servers[order.server_id].stable_amps
        order.state = OrderState.RUNNING
        order.occupied_amps = stable
        order.log(
            "VERIFIED_STABLE",
            f"按本单查实设备已稳定运行（围栏 {close_fence}），改按稳定电流 {stable}A 占用",
        )
        return "ok", {"pdu_status": pdu_status}

    # ---------------- 查实断电并释放 ----------------
    def apply_release(self, order_id: str, pdu_status: dict) -> tuple[str, dict]:
        """在锁内复核并释放额度。

        释放条件（全部满足）：
        - PDU 明确报告 ``powered == false``；
        - PDU 报告的断电围栏严格晚于本单合闸代号，即本单的在途合闸
          已经被更晚的断电指令作废，不可能再让插座通电；
        - 单据仍是该服务器唯一的活动单。
        任何一项无法确认 -> 保留占额。
        """
        order = self.orders.get(order_id)
        if order is None:
            return "not_found", {}
        if order.state == OrderState.RELEASED:
            return "already_released", {}

        active = self.active_order_for(order.server_id)
        if active is not order:
            return "superseded", {
                "error": "该服务器已有更新的活动合闸单，拒绝释放旧单额度",
                "active_order_id": active.id if active else None,
            }

        if pdu_status.get("powered"):
            return "powered", {
                "error": "查实未通过：设备仍带电，不能释放额度；请先断电",
                "pdu_status": pdu_status,
            }

        open_fence = pdu_status.get("last_open_fence")
        if open_fence is None:
            return "unconfirmed", {
                "error": "PDU 未提供断电围栏，无法确认在途合闸已被作废，额度继续保留",
                "pdu_status": pdu_status,
                "required_open_fence_after": order.close_generation,
            }
        if int(open_fence) <= order.close_generation:
            return "unconfirmed", {
                "error": "PDU 的断电指令不晚于本单合闸指令，无法确认迟到合闸不会再通电；额度继续保留",
                "pdu_status": pdu_status,
                "required_open_fence_after": order.close_generation,
            }

        order.state = OrderState.RELEASED
        order.occupied_amps = 0.0
        order.log(
            "RELEASED",
            f"查实插座已断电（断电围栏 {open_fence} > 合闸代号 {order.close_generation}），"
            "在途合闸已被作废，释放支路额度",
        )
        return "ok", {"pdu_status": pdu_status}
