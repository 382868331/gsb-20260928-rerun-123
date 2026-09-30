"""应用事务协调库：持久化协调记录 + 不确定结果查询确认 + 失败补偿。"""

from .coordinator import (
    Coordinator,
    CoordinatorExit,
    Step,
    StepState,
    TxState,
    UnknownResult,
)
from .store import JsonRecordStore, RecordStore

__all__ = [
    "Coordinator",
    "CoordinatorExit",
    "Step",
    "StepState",
    "TxState",
    "UnknownResult",
    "JsonRecordStore",
    "RecordStore",
]
