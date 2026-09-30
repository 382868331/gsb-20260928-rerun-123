"""覆盖：结果丢失查询确认、协调者退出重建接续、补偿按真实生效推进、
补偿失败保留停点与重试、重复确认/补偿幂等、对清。"""

import pytest

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
    StepState,
    TxState,
)


def make_env():
    wallet = WalletService(1000)
    inventory = InventoryService(100)
    order = OrderService(inventory)
    rc_action, rc_comp, rc_confirm = reserve_then_commit(inventory)
    steps = [
        Step("debit", wallet.debit, wallet.refund, wallet.confirm_debit),
        Step("reserve_commit", rc_action, rc_comp, rc_confirm),
        Step("submit_order", order.submit, order.cancel),
    ]
    return wallet, inventory, order, steps


def test_happy_path(tmp_path):
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))
    coord = Coordinator("t1", steps, store, {"amount": 100, "qty": 3})
    assert coord.run() == TxState.COMMITTED
    assert wallet.balance == 900
    assert inventory.stock == 97
    assert order.orders["t1"] == "SUBMITTED"


def test_lost_response_blocks_and_confirm_then_commit(tmp_path):
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))
    coord = Coordinator("t2", steps, store,
                        {"amount": 100, "qty": 3, "lose_debit_response": True})
    with pytest.raises(CoordinatorExit):
        coord.run()
    # 钱真实扣了，但协调记录只敢写 UNKNOWN
    assert wallet.balance == 900
    assert coord.record["steps"]["debit"] == StepState.UNKNOWN.value
    assert coord.record["tx_state"] == TxState.BLOCKED.value

    # 阻塞期间 run 不能盲目重做
    with pytest.raises(CoordinatorExit):
        Coordinator("t2", steps, store).run()

    # 查询确认消解未知点后继续提交
    coord2 = Coordinator("t2", steps, store)
    assert coord2.confirm_unknown() is True
    assert coord2.record["steps"]["debit"] == StepState.COMMITTED.value
    assert coord2.run() == TxState.COMMITTED
    assert wallet.balance == 900  # 没有重复扣款


def test_unknown_resolved_aborted_then_redrive(tmp_path):
    """超时但实际未生效：确认 ABORTED 后从该步骤重新执行，后续正常提交。"""
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))

    # debit 不真正生效但抛 UnknownResult：用自定义 confirm 报 ABORTED
    def debit_lost(tx_id, payload):
        raise type("UR", (Exception,), {})

    # 直接构造 UNKNOWN 记录模拟异常退出后磁盘状态，业务侧无扣款
    coord = Coordinator("t3", steps, store, {"amount": 50, "qty": 1})
    coord._set_step("debit", StepState.UNKNOWN)
    coord._set_tx(TxState.BLOCKED)

    def confirm_aborted(tx_id, payload):
        return "ABORTED"

    steps[0] = Step("debit", wallet.debit, wallet.refund, confirm_aborted)
    coord2 = Coordinator("t3", steps, store)
    assert coord2.confirm_unknown() is True
    assert coord2.record["steps"]["debit"] == StepState.PENDING.value
    # 确认未生效后允许重做该步骤（重做安全：业务侧按 tx_id 幂等）
    assert coord2.run() == TxState.COMMITTED
    assert wallet.balance == 950
    assert wallet.refunds == {}


def test_business_failure_compensates_committed_only(tmp_path):
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))
    coord = Coordinator("t4", steps, store,
                        {"amount": 200, "qty": 5, "fail_order": True})
    assert coord.run() == TxState.ABORTED
    assert wallet.balance == 1000           # 扣款被退回
    assert inventory.stock == 100          # 预留被释放
    assert "t4" in inventory.released
    assert order.orders.get("t4") is None


def test_compensation_failure_keeps_stop_point_and_retries(tmp_path):
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))
    coord = Coordinator("t5", steps, store,
                        {"amount": 200, "qty": 5, "fail_order": True,
                         "fail_refund_once": True})
    assert coord.run() == TxState.ABORTING
    assert coord.record["steps"]["debit"] == StepState.COMPENSATION_FAILED.value
    assert coord.record["steps"]["reserve_commit"] == StepState.COMPENSATED.value
    assert wallet.balance == 800  # 退款未成功，钱仍被扣

    # 新建协调者从停点重试补偿
    coord2 = Coordinator("t5", steps, store)
    assert coord2.compensate() == TxState.ABORTED
    assert wallet.balance == 1000

    # 重复补偿幂等：不重复退款
    coord2.compensate()
    assert wallet.balance == 1000
    assert list(wallet.refunds.values()) == [200]


def test_lost_compensation_response_retry_is_idempotent(tmp_path):
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))
    coord = Coordinator("t6", steps, store,
                        {"amount": 150, "qty": 2, "fail_order": True,
                         "lose_refund_response": True})
    with pytest.raises(CoordinatorExit):
        coord.run()
    assert coord.record["steps"]["debit"] == StepState.COMPENSATING.value
    assert wallet.balance == 1000  # 退款实际已执行

    coord2 = Coordinator("t6", steps, store)
    assert coord2.compensate() == TxState.ABORTED
    assert wallet.balance == 1000  # 重试命中幂等，未二次退款
    assert list(wallet.refunds.values()) == [150]


def test_reconcile_reports_every_step(tmp_path):
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))
    coord = Coordinator("t7", steps, store,
                        {"amount": 90, "qty": 1, "fail_order": True})
    coord.run()
    report = {item["step"]: item["state"] for item in coord.reconcile()}
    assert report == {
        "debit": StepState.COMPENSATED.value,
        "reserve_commit": StepState.COMPENSATED.value,
        "submit_order": StepState.FAILED.value,
    }


def test_unknown_commit_must_not_be_compensated_blindly(tmp_path):
    """预留生效但提交结果未知时，不能直接释放预留。"""
    wallet, inventory, order, steps = make_env()
    store = JsonRecordStore(str(tmp_path))
    coord = Coordinator("t8", steps, store,
                        {"amount": 50, "qty": 1, "lose_commit_response": True,
                         "commit_query_unknown": True})
    with pytest.raises(CoordinatorExit):
        coord.run()
    # 扣款 COMMITTED，预留已转为真实占用但提交应答丢失 -> UNKNOWN
    assert coord.record["steps"]["debit"] == StepState.COMMITTED.value
    assert coord.record["steps"]["reserve_commit"] == StepState.UNKNOWN.value
    assert inventory.stock == 99

    # 业务侧尚不能判定提交结果，UNKNOWN 持续，库存保持
    coord2 = Coordinator("t8", steps, store)
    assert coord2.confirm_unknown() is False
    assert inventory.stock == 99

    # 对端稍后可查：提交真实生效 -> COMMITTED，整体继续完成订单
    coord2.record["payload"]["commit_query_unknown"] = False
    coord2.store.save(coord2.tx_id, coord2.record)
    coord3 = Coordinator("t8", steps, store)
    assert coord3.confirm_unknown() is True
    assert coord3.run() == TxState.COMMITTED
    assert inventory.stock == 99
