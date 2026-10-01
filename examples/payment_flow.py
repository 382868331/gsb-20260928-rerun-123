"""Demo business flow: virtual debit -> reserve -> commit.

The "services" below are in-memory fakes that can be told to fail or
to drop responses, so the coordination behaviour is observable.
"""

from __future__ import annotations

from txcoord import OutcomeUnknown, Step, StepFailed


class AccountService:
    """Virtual debit / rollback on an account ledger."""

    def __init__(self):
        self.ledger = {}
        self.debit_calls = 0
        self.rollback_calls = 0
        self.fail_debit = False
        self.drop_debit_response = False
        self.fail_rollback = False
        self.drop_rollback_response = False

    def debit(self, account: str, amount: int) -> str:
        self.debit_calls += 1
        if self.fail_debit:
            raise StepFailed("insufficient funds")
        self.ledger[account] = self.ledger.get(account, 0) - amount
        tx = f"debit-{account}-{self.debit_calls}"
        if self.drop_debit_response:
            raise OutcomeUnknown("debit response lost")
        return tx

    def rollback(self, account: str, amount: int) -> None:
        self.rollback_calls += 1
        if self.fail_rollback:
            raise StepFailed("rollback rejected")
        self.ledger[account] = self.ledger.get(account, 0) + amount
        if self.drop_rollback_response:
            raise OutcomeUnknown("rollback response lost")

    def balance(self, account: str) -> int:
        return self.ledger.get(account, 0)


class InventoryService:
    """Reserve / release stock."""

    def __init__(self):
        self.reserved = {}
        self.reserve_calls = 0
        self.release_calls = 0
        self.fail_reserve = False
        self.drop_reserve_response = False

    def reserve(self, sku: str, qty: int) -> str:
        self.reserve_calls += 1
        if self.fail_reserve:
            raise StepFailed("out of stock")
        self.reserved[sku] = self.reserved.get(sku, 0) + qty
        if self.drop_reserve_response:
            raise OutcomeUnknown("reserve response lost")
        return f"rsv-{sku}-{self.reserve_calls}"

    def release(self, sku: str, qty: int) -> None:
        self.release_calls += 1
        self.reserved[sku] = self.reserved.get(sku, 0) - qty


class OrderService:
    """Final commit of the order."""

    def __init__(self):
        self.committed = []
        self.fail_commit = False
        self.drop_commit_response = False

    def commit(self, order_id: str) -> str:
        if self.fail_commit:
            raise StepFailed("commit rejected")
        self.committed.append(order_id)
        if self.drop_commit_response:
            raise OutcomeUnknown("commit response lost")
        return f"order-{order_id}-committed"


def build_steps(accounts: AccountService, inventory: InventoryService,
                orders: OrderService) -> list:
    """The canonical 虚拟扣款 -> 预留 -> 提交 flow."""

    def debit_query(ctx):
        return "confirmed" if accounts.balance(ctx["account"]) <= -ctx["amount"] else "failed"

    def rollback_query(ctx, _result):
        return "confirmed" if accounts.balance(ctx["account"]) >= 0 else "unknown"

    return [
        Step(
            name="virtual_debit",
            action=lambda ctx: accounts.debit(ctx["account"], ctx["amount"]),
            compensate=lambda ctx, _res: accounts.rollback(ctx["account"], ctx["amount"]),
            query=debit_query,
            query_compensation=rollback_query,
        ),
        Step(
            name="reserve",
            action=lambda ctx: inventory.reserve(ctx["sku"], ctx["qty"]),
            compensate=lambda ctx, _res: inventory.release(ctx["sku"], ctx["qty"]),
            query=lambda ctx: "confirmed"
            if inventory.reserved.get(ctx["sku"], 0) >= ctx["qty"]
            else "failed",
        ),
        Step(
            name="commit",
            action=lambda ctx: orders.commit(ctx["order_id"]),
            query=lambda ctx: "confirmed"
            if ctx["order_id"] in orders.committed
            else "failed",
        ),
    ]
