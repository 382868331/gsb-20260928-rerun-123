"""Transaction coordination library.

Provides a saga-style coordinator with durable records of confirmed
effects, explicit failures, and unknown outcomes, plus compensation,
operator pause/resume, restart recovery, and idempotent re-entry.
"""

from .models import (
    OutcomeUnknown,
    Step,
    StepFailed,
    StepRecord,
    StepStatus,
    TaskRecord,
    TaskStatus,
)
from .store import Store
from .coordinator import Coordinator

__all__ = [
    "Coordinator",
    "Store",
    "Step",
    "StepRecord",
    "StepStatus",
    "TaskRecord",
    "TaskStatus",
    "StepFailed",
    "OutcomeUnknown",
]
