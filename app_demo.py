"""端到端演示：虚拟扣款 + 预留提交中的结果丢失、协调者退出与补偿重试。

运行：python app_demo.py
"""

from __future__ import annotations

import os
import tempfile

from services.virtual_business import (
    InventoryService,
    OrderService,
    WalletService,
    reserve_then_commit,
)
from txcoordinator import (
    Coordinator,
    CoordinatorExit,
    JsonRecordStore,
    Step,
    TxState,
)


def build_steps(wallet, inventory, order):
    rc_action, rc_compensate, rc_confirm = reserve_then_commit(inventory)
    return [
        Step("debit", wallet.debit, wallet.refund, wallet.confirm_debit),
        Step("reserve_commit", rc_action, rc_compensate, rc_confirm),
        Step("submit_order", order.submit, order.cancel),
    ]


def show(label, coord):
    print(f"--- {label}: tx={coord.record['tx_state']}")
    for item in coord.reconcile():
        print(f"    {item['step']:<14}{item['state']:<20}{item['result']}")


def main():
    tmpdir = tempfile.mkdtemp(prefix="txcoord-demo-")
    store = JsonRecordStore(tmpdir)
    tx_id = "ORDER-1001"

    wallet = WalletService(balance=1000)
    inventory = InventoryService(stock=100)
    order = OrderService(inventory)
    payload = {"amount": 300, "qty": 2, "lose_debit_response": True,
               "fail_order": True, "fail_refund_once": True}
    steps = build_steps(wallet, inventory, order)

    print("== 1. 首次运行：扣款应答丢失，保留 UNKNOWN 停点并退出 ==")
    coord = Coordinator(tx_id, steps, store, payload)
    try:
        coord.run()
    except CoordinatorExit as exc:
        print("   协调者退出:", exc)
    show("退出后磁盘记录", coord)
    print(f"   真实业务台账: 余额={wallet.balance}, 已扣款={wallet.debits}")

    print("\n== 2. 重新创建协调者：不重做扣款，先查询确认真实记录 ==")
    coord2 = Coordinator(tx_id, steps, store)  # 不传 payload，从磁盘接续
    assert coord2.confirm_unknown() is True
    show("查询确认后", coord2)

    print("\n== 3. 继续推进：预留提交成功，订单提交失败，进入补偿 ==")
    state = coord2.run()
    print("   run 返回:", state.value)
    show("补偿遇瞬时失败，保留停点", coord2)

    print("\n== 4. 再次创建协调者重试补偿（退款通道恢复）==")
    coord3 = Coordinator(tx_id, steps, store)
    state = coord3.compensate()
    print("   compensate 返回:", state.value)
    show("补偿完成", coord3)

    print("\n== 5. 对清业务台账（重复补偿不重复改变状态）==")
    coord3.compensate()  # 幂等：不应再退款/释放
    print(f"   余额={wallet.balance} (应恢复 1000)")
    print(f"   库存={inventory.stock} (应恢复 100)")
    print(f"   退款记录={wallet.refunds}, 释放记录={sorted(inventory.released)}")
    assert wallet.balance == 1000
    assert inventory.stock == 100
    assert state == TxState.ABORTED
    print("\n记录文件:", os.path.join(tmpdir, tx_id + ".json"))


if __name__ == "__main__":
    main()
