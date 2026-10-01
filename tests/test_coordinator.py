import threading

import pytest

from txcoord import Coordinator, OutcomeUnknown, Step, StepFailed, StepStatus, Store, TaskStatus
from examples.payment_flow import (
    AccountService,
    InventoryService,
    OrderService,
    build_steps,
)

CTX = {"account": "alice", "amount": 100, "sku": "SKU-1", "qty": 2, "order_id": "O-1"}


def make(tmp_path=None):
    accounts = AccountService()
    inventory = InventoryService()
    orders = OrderService()
    path = str(tmp_path / "coord.db") if tmp_path else ":memory:"
    store = Store(path)
    coord = Coordinator(store)
    steps = build_steps(accounts, inventory, orders)
    return accounts, inventory, orders, store, coord, steps


def test_happy_path_all_confirmed():
    accounts, inventory, orders, store, coord, steps = make()
    task = coord.run("t1", steps, dict(CTX))
    assert task.status == TaskStatus.SUCCEEDED
    assert [s.status for s in task.steps] == [StepStatus.CONFIRMED] * 3
    assert accounts.balance("alice") == -100
    assert inventory.reserved["SKU-1"] == 2
    assert orders.committed == ["O-1"]


def test_failure_triggers_compensation():
    accounts, inventory, orders, store, coord, steps = make()
    inventory.fail_reserve = True
    task = coord.run("t2", steps, dict(CTX))
    assert task.status == TaskStatus.COMPENSATED
    by_name = {s.name: s for s in task.steps}
    assert by_name["virtual_debit"].status == StepStatus.COMPENSATED
    assert by_name["reserve"].status == StepStatus.FAILED
    assert by_name["commit"].status == StepStatus.PENDING
    assert accounts.balance("alice") == 0
    assert orders.committed == []


def test_unknown_outcome_is_not_retried_blindly():
    accounts, inventory, orders, store, coord, steps = make()
    accounts.drop_debit_response = True  # effect happens, response lost
    task = coord.run("t3", steps, dict(CTX))
    # query resolves the unknown debit as confirmed, flow continues
    assert task.status == TaskStatus.SUCCEEDED
    assert accounts.debit_calls == 1  # never re-executed
    assert accounts.balance("alice") == -100


def test_unresolvable_unknown_needs_attention_then_operator_confirms():
    accounts, inventory, orders, store, coord, steps = make()
    accounts.drop_debit_response = True
    steps[0].query = None  # no automatic query available
    task = coord.run("t4", steps, dict(CTX))
    assert task.status == TaskStatus.NEEDS_ATTENTION
    assert accounts.debit_calls == 1
    coord.confirm_step("t4", "virtual_debit", happened=True)
    task = coord.resume("t4", steps)
    assert task.status == TaskStatus.SUCCEEDED
    assert accounts.debit_calls == 1


def test_compensation_response_lost_then_query_continues():
    accounts, inventory, orders, store, coord, steps = make()
    inventory.fail_reserve = True
    accounts.drop_rollback_response = True  # rollback happens, response lost
    task = coord.run("t5", steps, dict(CTX))
    # query_compensation confirms the rollback landed
    assert task.status == TaskStatus.COMPENSATED
    assert accounts.rollback_calls == 1
    assert accounts.balance("alice") == 0


def test_compensation_unknown_without_query_allows_manual_confirm():
    accounts, inventory, orders, store, coord, steps = make()
    inventory.fail_reserve = True
    accounts.drop_rollback_response = True
    steps[0].query_compensation = None
    task = coord.run("t6", steps, dict(CTX))
    assert task.status == TaskStatus.NEEDS_ATTENTION
    coord.confirm_step("t6", "virtual_debit", happened=True)  # rollback confirmed
    task = coord.resume("t6", steps)
    assert task.status == TaskStatus.COMPENSATED
    assert accounts.rollback_calls == 1


def test_pause_and_resume_keeps_identity():
    accounts, inventory, orders, store, coord, steps = make()
    accounts.drop_debit_response = True
    steps[0].query = None
    task = coord.run("t7", steps, dict(CTX))
    assert task.status == TaskStatus.NEEDS_ATTENTION
    paused = coord.pause("t7")
    assert paused.status == TaskStatus.PAUSED
    # run() must not drive a paused task
    again = coord.run("t7", steps, dict(CTX))
    assert again.status == TaskStatus.PAUSED
    coord.confirm_step("t7", "virtual_debit", happened=True)
    resumed = coord.resume("t7", steps)
    assert resumed.id == task.id
    assert resumed.status == TaskStatus.SUCCEEDED


def test_recreated_coordinator_resumes_from_records(tmp_path):
    accounts, inventory, orders, store, coord, steps = make(tmp_path)
    accounts.drop_debit_response = True
    steps[0].query = None
    task = coord.run("t8", steps, dict(CTX))
    assert task.status == TaskStatus.NEEDS_ATTENTION
    assert accounts.debit_calls == 1

    # simulate restart: brand new coordinator over the same store file
    coord2 = Coordinator(Store(str(tmp_path / "coord.db")))
    steps2 = build_steps(accounts, inventory, orders)
    coord2.confirm_step("t8", "virtual_debit", happened=True)
    resumed = coord2.resume("t8", steps2)
    assert resumed.status == TaskStatus.SUCCEEDED
    assert accounts.debit_calls == 1  # did not restart from step one


def test_concurrent_duplicate_requests_share_effects():
    accounts, inventory, orders, store, coord, steps = make()
    results = []

    def worker():
        results.append(coord.run("t9", steps, dict(CTX)))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert {r.id for r in results} == {results[0].id}
    assert all(r.status == TaskStatus.SUCCEEDED for r in results)
    assert accounts.debit_calls == 1
    assert inventory.reserve_calls == 1
    assert orders.committed == ["O-1"]


def test_terminal_task_rerun_returns_same_record():
    accounts, inventory, orders, store, coord, steps = make()
    first = coord.run("t10", steps, dict(CTX))
    second = coord.run("t10", steps, dict(CTX))
    assert second.id == first.id
    assert second.status == TaskStatus.SUCCEEDED
    assert accounts.debit_calls == 1


def test_recover_all_after_restart(tmp_path):
    accounts, inventory, orders, store, coord, steps = make(tmp_path)
    accounts.drop_debit_response = True
    steps[0].query = None
    coord.run("t11", steps, dict(CTX))

    coord2 = Coordinator(Store(str(tmp_path / "coord.db")))
    steps2 = build_steps(accounts, inventory, orders)
    coord2.confirm_step("t11", "virtual_debit", happened=True)
    resumed = coord2.recover_all(lambda task: steps2)
    assert len(resumed) == 1
    assert resumed[0].status == TaskStatus.SUCCEEDED
    assert accounts.debit_calls == 1
