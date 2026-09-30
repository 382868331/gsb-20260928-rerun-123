"""事务协调者核心。

每个业务步骤的状态约定：

    PENDING              尚未执行
    UNKNOWN              调用超时/应答丢失，业务侧是否生效未知，保留停点
    COMMITTED            已确认生效
    FAILED               已确认未生效（业务报错或查询确认未生效）
    COMPENSATING         正在补偿；补偿应答未知时停留在此
    COMPENSATED          补偿已生效
    COMPENSATION_FAILED  补偿执行失败，保留停点，等待重试

关键原则：
1. 超时不等于失败：先记 UNKNOWN，恢复时必须走查询确认，禁止盲目重做或补偿。
2. 补偿按“实际生效关系”推进：只有确认 COMMITTED 的步骤才补偿。
3. 补偿自身失败保留停点（COMPENSATING / COMPENSATION_FAILED），重试不跳过。
4. 所有状态迁移先落盘再继续；重复确认/重复恢复不重复改变业务状态
   （业务动作本身按 tx_id 幂等）。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from .store import JsonRecordStore


class UnknownResult(Exception):
    """业务调用超时或应答丢失：结果未知，不代表成功也不代表失败。"""


class CoordinatorExit(Exception):
    """存在未知结果停点，协调者主动退出，等待后续重新创建并接续。"""


class StepState(str, Enum):
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"


class TxState(str, Enum):
    RUNNING = "RUNNING"
    COMMITTED = "COMMITTED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"
    BLOCKED = "BLOCKED"  # 存在 UNKNOWN 停点，必须先查询确认


StepFn = Callable[[str, dict[str, Any]], Any]
ConfirmFn = Callable[[str, dict[str, Any]], str]


@dataclass(frozen=True)
class Step:
    name: str
    action: StepFn
    compensate: StepFn | None = None
    # 查询确认：返回 COMMITTED / ABORTED / UNKNOWN，纯查询，不允许改变业务状态
    confirm: ConfirmFn | None = None


class Coordinator:
    """依据持久化记录驱动业务步骤与补偿的协调者。"""

    def __init__(self, tx_id: str, steps: list[Step], store: JsonRecordStore,
                 payload: dict[str, Any] | None = None) -> None:
        self.tx_id = tx_id
        self.steps = steps
        self.store = store
        record = store.load(tx_id)
        if record is None:
            record = {
                "tx_id": tx_id,
                "tx_state": TxState.RUNNING.value,
                "payload": payload or {},
                "steps": {s.name: StepState.PENDING.value for s in steps},
                "results": {},
                "events": [],
            }
            store.save(tx_id, record)
        self.record = record

    def _set_step(self, name: str, state: StepState, result: Any = None) -> None:
        self.record["steps"][name] = state.value
        if result is not None:
            self.record["results"][name] = result
        self.record["events"].append({"step": name, "state": state.value})
        self.store.save(self.tx_id, self.record)

    def _set_tx(self, state: TxState) -> None:
        self.record["tx_state"] = state.value
        self.store.save(self.tx_id, self.record)

    def confirm_unknown(self) -> bool:
        """对所有 UNKNOWN 步骤做查询确认。

        返回 True 表示未知点已消解；False 表示仍未知（继续保留停点）。
        确认是纯查询：重复调用不会重复改变业务状态。
        """
        settled = True
        for step in self.steps:
            if self.record["steps"][step.name] != StepState.UNKNOWN.value:
                continue
            if step.confirm is None:
                settled = False
                continue
            outcome = step.confirm(self.tx_id, self.record["payload"])
            if outcome == "COMMITTED":
                self._set_step(step.name, StepState.COMMITTED)
            elif outcome == "ABORTED":
                # 查询确认未生效：回到 PENDING，允许安全重做（事件留痕可对清）；
                # 与“同步调用直接报错”的 FAILED（立即补偿）区分开。
                self._set_step(step.name, StepState.PENDING)
                self.record["events"].append(
                    {"step": step.name, "state": "CONFIRMED_ABORTED"})
                self.store.save(self.tx_id, self.record)
            else:  # UNKNOWN：业务侧也查不到，保留停点
                settled = False
        if settled and self.record["tx_state"] == TxState.BLOCKED.value:
            self._set_tx(TxState.RUNNING)
        return settled

    def run(self) -> TxState:
        """正向推进事务；遇未知结果保留停点并退出，遇业务失败转补偿。"""
        if self.record["tx_state"] == TxState.BLOCKED.value:
            # 阻塞时不允许自动重做：调用方必须先显式查询确认
            raise CoordinatorExit(
                f"事务 {self.tx_id} 存在尚未消解的未知结果，需先 confirm_unknown()"
            )

        for step in self.steps:
            state = self.record["steps"][step.name]
            if state in (StepState.COMMITTED.value, StepState.FAILED.value):
                continue  # 重启接续：已生效步骤绝不重做
            if state in (StepState.COMPENSATING.value,
                         StepState.COMPENSATED.value,
                         StepState.COMPENSATION_FAILED.value):
                break  # 已进入补偿路径，交给 compensate()

            try:
                result = step.action(self.tx_id, self.record["payload"])
            except UnknownResult:
                # 超时可能已经生效：记 UNKNOWN 并整体阻塞，等待查询确认
                self._set_step(step.name, StepState.UNKNOWN)
                self._set_tx(TxState.BLOCKED)
                raise CoordinatorExit(
                    f"步骤 {step.name} 结果未知，事务挂起，需查询确认后接续"
                )
            except Exception as exc:  # 确认失败：该步骤未生效，进入补偿
                self._set_step(step.name, StepState.FAILED, str(exc))
                return self.compensate()
            else:
                self._set_step(step.name, StepState.COMMITTED, result)

        if all(self.record["steps"][s.name] == StepState.COMMITTED.value
               for s in self.steps):
            self._set_tx(TxState.COMMITTED)
        return TxState(self.record["tx_state"])

    def compensate(self) -> TxState:
        """按实际生效关系逆序补偿，并在补偿失败处保留停点。"""
        self._set_tx(TxState.ABORTING)
        for step in reversed(self.steps):
            state = self.record["steps"][step.name]
            if state in (StepState.PENDING.value, StepState.FAILED.value,
                         StepState.COMPENSATED.value):
                continue  # 未生效 / 已补偿：不重复补偿
            if state == StepState.UNKNOWN.value:
                # 不确定是否生效，绝不盲目补偿：先消解未知点
                self._set_tx(TxState.BLOCKED)
                raise CoordinatorExit(
                    f"步骤 {step.name} 是否生效未知，暂停补偿，需查询确认"
                )
            if state not in (StepState.COMMITTED.value,
                             StepState.COMPENSATION_FAILED.value,
                             StepState.COMPENSATING.value):
                continue
            if step.compensate is None:
                self._set_step(step.name, StepState.COMPENSATED)
                continue

            if state == StepState.COMMITTED.value:
                self._set_step(step.name, StepState.COMPENSATING)
            try:
                result = step.compensate(self.tx_id, self.record["payload"])
            except UnknownResult:
                # 补偿应答丢失：停在 COMPENSATING，恢复时重试；
                # 业务侧按 tx_id 幂等，重复补偿不会重复改变状态
                raise CoordinatorExit(
                    f"步骤 {step.name} 补偿结果未知，保留 COMPENSATING 停点"
                )
            except Exception as exc:
                self._set_step(step.name, StepState.COMPENSATION_FAILED, str(exc))
                return TxState.ABORTING  # 保留停点，等待重试
            else:
                self._set_step(step.name, StepState.COMPENSATED, result)

        self._set_tx(TxState.ABORTED)
        return TxState.ABORTED

    def reconcile(self) -> list[dict[str, str]]:
        """返回每一步实际做了什么，用于重启后对清。"""
        return [
            {
                "step": s.name,
                "state": self.record["steps"][s.name],
                "result": self.record["results"].get(s.name, ""),
            }
            for s in self.steps
        ]
