"""Core data model for the transaction coordination library."""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class TaskStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"
    NEEDS_ATTENTION = "NEEDS_ATTENTION"
    PAUSED = "PAUSED"


class StepStatus(str, enum.Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    COMPENSATED = "COMPENSATED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"
    COMPENSATION_UNKNOWN = "COMPENSATION_UNKNOWN"


TERMINAL_TASK_STATES = {
    TaskStatus.SUCCEEDED,
    TaskStatus.COMPENSATED,
    TaskStatus.FAILED,
}


class StepFailed(Exception):
    """The step explicitly reported a business failure."""


class OutcomeUnknown(Exception):
    """The step's outcome is unknown (timeout / lost response).

    The effect may or may not have happened; the coordinator must
    query before deciding, and must never blindly re-execute.
    """


@dataclass
class Step:
    """One business step of a coordinated task."""

    name: str
    action: Callable[[dict], Any]
    compensate: Optional[Callable[[dict, Any], Any]] = None
    # query() -> "confirmed" | "failed" | "unknown"
    query: Optional[Callable[[dict], str]] = None
    # query_compensation() -> "confirmed" | "failed" | "unknown"
    query_compensation: Optional[Callable[[dict, Any], str]] = None


@dataclass
class StepRecord:
    task_id: int
    idx: int
    name: str
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    updated_at: float = field(default_factory=time.time)


@dataclass
class TaskRecord:
    id: int
    key: str
    status: TaskStatus
    ctx: dict = field(default_factory=dict)
    error: Optional[str] = None
    steps: list = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

