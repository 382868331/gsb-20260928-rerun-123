"""虚拟业务服务：扣款、预留、提交。

所有动作按 tx_id 幂等：同一事务重复调用只返回首次结果，
不重复扣款、不重复释放。每个服务自带真实业务台账，
协调者重启后通过查询接口确认“到底生效没有”。
"""

from __future__ import annotations

from txcoordinator import UnknownResult


class WalletService:
    def __init__(self, balance: int = 1000) -> None:
        self.balance = balance
        self.debits: dict[str, int] = {}        # tx_id -> 金额（已扣款）
        self.refunds: dict[str, int] = {}      # tx_id -> 金额（已退款）

    def debit(self, tx_id: str, payload: dict) -> str:
        if tx_id in self.debits:               # 幂等：结果丢失后重放
            return f"debit:{self.debits[tx_id]}"
        amount = payload["amount"]
        if amount > self.balance:
            raise RuntimeError("余额不足")
        self.balance -= amount
        self.debits[tx_id] = amount
        if payload.get("lose_debit_response"):
            raise UnknownResult("扣款已执行但应答丢失")
        return f"debit:{amount}"

    def refund(self, tx_id: str, payload: dict) -> str:
        if tx_id in self.refunds:              # 补偿幂等
            return f"refund:{self.refunds[tx_id]}"
        if tx_id not in self.debits:
            return "refund:skipped"             # 实际未生效则不退款
        amount = self.debits[tx_id]
        if payload.get("fail_refund_once"):
            payload["fail_refund_once"] = False  # 模拟补偿瞬时失败，重试成功
            raise RuntimeError("退款通道抖动")
        self.balance += amount
        self.refunds[tx_id] = amount
        if payload.get("lose_refund_response"):
            raise UnknownResult("退款已执行但应答丢失")
        return f"refund:{amount}"

    def confirm_debit(self, tx_id: str, payload: dict) -> str:
        if tx_id in self.debits:
            return "COMMITTED"
        return "ABORTED"


class InventoryService:
    def __init__(self, stock: int = 100) -> None:
        self.stock = stock
        self.reserved: dict[str, int] = {}
        self.committed: set[str] = set()
        self.released: set[str] = set()

    def reserve(self, tx_id: str, payload: dict) -> str:
        if tx_id in self.reserved:
            return f"reserve:{self.reserved[tx_id]}"
        qty = payload["qty"]
        if qty > self.stock:
            raise RuntimeError("库存不足")
        self.stock -= qty
        self.reserved[tx_id] = qty
        if payload.get("lose_reserve_response"):
            raise UnknownResult("预留已执行但应答丢失")
        return f"reserve:{qty}"

    def confirm_reserve(self, tx_id: str, payload: dict) -> str:
        if tx_id in self.reserved:
            return "COMMITTED"
        return "ABORTED"

    def release(self, tx_id: str, payload: dict) -> str:
        if tx_id in self.released:             # 补偿幂等
            return "release:done"
        if tx_id not in self.reserved:
            return "release:skipped"
        if tx_id in self.committed:
            return "release:skipped-committed"  # 已提交占用真实库存，不能释放
        qty = self.reserved[tx_id]
        self.stock += qty
        self.released.add(tx_id)
        return f"release:{qty}"


class OrderService:
    def __init__(self, inventory: InventoryService) -> None:
        self.inventory = inventory
        self.orders: dict[str, str] = {}       # tx_id -> 状态

    def submit(self, tx_id: str, payload: dict) -> str:
        if tx_id in self.orders:
            return f"order:{self.orders[tx_id]}"
        if tx_id not in self.inventory.committed:
            # 业务约束：提交必须建立在预留已生效的真实关系上
            raise RuntimeError("预留不存在，无法提交订单")
        if payload.get("fail_order"):
            raise RuntimeError("订单中心拒绝")
        self.orders[tx_id] = "SUBMITTED"
        return "order:SUBMITTED"

    def cancel(self, tx_id: str, payload: dict) -> str:
        if self.orders.get(tx_id) == "CANCELLED":
            return "order:cancelled"
        if tx_id in self.orders:
            self.orders[tx_id] = "CANCELLED"
        return "order:cancelled"


def reserve_then_commit(inventory: InventoryService):
    """“预留并提交”合成动作：先预留，确认生效后再提交占用。"""

    def action(tx_id: str, payload: dict) -> str:
        if tx_id not in inventory.reserved:
            inventory.reserve(tx_id, payload)
        if tx_id not in inventory.committed:
            # 提交把预留转为真实占用
            inventory.committed.add(tx_id)
            if payload.get("lose_commit_response"):
                raise UnknownResult("提交已生效但应答丢失")
        return "reserve-commit:done"

    def compensate(tx_id: str, payload: dict) -> str:
        # 只有真实提交生效才需要处理：此处回退提交并释放预留
        if tx_id in inventory.committed:
            inventory.committed.discard(tx_id)
        return inventory.release(tx_id, payload)

    def confirm(tx_id: str, payload: dict) -> str:
        if payload.get("commit_query_unknown"):
            return "UNKNOWN"  # 对端暂时也无法判定提交是否生效
        if tx_id in inventory.committed:
            return "COMMITTED"
        return "ABORTED"

    return action, compensate, confirm
