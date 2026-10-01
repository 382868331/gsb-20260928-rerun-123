"""Saga-style transaction coordinator.

Executes an ordered flow of business steps (e.g. virtual debit ->
reserve -> commit), persists every confirmed effect / explicit
failure / unknown outcome, compensates confirmed steps when a later
step fails, and resumes from persisted facts after a restart.
"""

from __future__ import annotations

import threading
from typing import Optional

from .models import (
    Step,
    StepFailed,
    StepRecord,
    StepStatus,
    TaskRecord,
    TaskStatus,
    OutcomeUnknown,
    TERMINAL_TASK_STATES,
)
from .store import Store


class Coordinator:
    def __init__(self, store: Store):
        self.store = store
        self._locks: dict = {}
        self._locks_guard = threading.Lock()

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def run(self, key: str, steps: list, ctx: Optional[dict] = None) -> TaskRecord:
        """Run (or resume) the coordinated task identified by key.

        Concurrent or repeated calls with the same key map to the same
        task and therefore the same single set of business effects.
        """
        ctx = ctx or {}
        task = self.store.create_task(key, ctx)
        if task is None:
            task = self.store.get_task(key)
        with self._task_lock(task.id):
            task = self.store.get_task(key)
            if task.status in TERMINAL_TASK_STATES or task.status == TaskStatus.PAUSED:
                return task
            self._ensure_step_records(task, steps)
            return self._drive(task, steps)

    def resume(self, key: str, steps: list) -> TaskRecord:
        """Operator-triggered resume of a paused / attention task.

        The original task identity (key and records) is preserved;
        processing continues from persisted facts.
        """
        task = self.store.get_task(key)
        if task is None:
            raise KeyError(f"unknown task: {key}")
        with self._task_lock(task.id):
            task = self.store.get_task(key)
            if task.status in TERMINAL_TASK_STATES:
                return task
            if task.status == TaskStatus.PAUSED:
                self.store.update_task_status(task.id, TaskStatus.RUNNING)
                task = self.store.get_task(key)
            self._ensure_step_records(task, steps)
            return self._drive(task, steps)

    def pause(self, key: str) -> TaskRecord:
        """Operator choice to hold off finalization."""
        task = self.store.get_task(key)
        if task is None:
            raise KeyError(f"unknown task: {key}")
        if task.status in TERMINAL_TASK_STATES:
            return task
        self.store.update_task_status(task.id, TaskStatus.PAUSED)
        return self.store.get_task(key)

    def confirm_step(self, key: str, step_name: str, happened: bool) -> TaskRecord:
        """Operator confirms an UNKNOWN step outcome after querying.

        happened=True records the effect as CONFIRMED; False records
        it as FAILED (no effect took place).
        """
        task = self.store.get_task(key)
        if task is None:
            raise KeyError(f"unknown task: {key}")
        rec = self._step_record(task, step_name)
        if rec.status == StepStatus.UNKNOWN:
            rec.status = StepStatus.CONFIRMED if happened else StepStatus.FAILED
            self.store.upsert_step(rec)
        elif rec.status == StepStatus.COMPENSATION_UNKNOWN:
            rec.status = StepStatus.COMPENSATED if happened else StepStatus.CONFIRMED
            self.store.upsert_step(rec)
        else:
            raise ValueError(f"step {step_name} is not awaiting confirmation")
        return self.store.get_task(key)

    def recover_all(self, steps_for) -> list:
        """Resume every unfinished task from the persisted records.

        steps_for maps a TaskRecord to its step definitions (or None).
        """
        resumed = []
        for task in self.store.unfinished_tasks():
            if task.status == TaskStatus.PAUSED:
                continue
            steps = steps_for(task)
            if steps is None:
                continue
            resumed.append(self.resume(task.key, steps))
        return resumed

    # ------------------------------------------------------------------
    # execution engine
    # ------------------------------------------------------------------

    def _drive(self, task: TaskRecord, steps: list) -> TaskRecord:
        task = self.store.get_task(task.key)
        if task.status == TaskStatus.NEEDS_ATTENTION:
            # A previous run stalled on an unknown outcome; try to
            # resolve it via queries before doing anything else.
            if not self._resolve_unknowns(task, steps):
                return self.store.get_task(task.key)
            task = self.store.get_task(task.key)
            if task.status != TaskStatus.RUNNING:
                return task

        for idx, step in enumerate(steps):
            rec = self._step_record(task, step.name)
            if rec.status in (StepStatus.CONFIRMED, StepStatus.COMPENSATED):
                continue
            if rec.status == StepStatus.FAILED:
                return self._compensate(task, steps, idx)
            if rec.status != StepStatus.PENDING:
                # UNKNOWN etc. without a way to resolve automatically.
                self.store.update_task_status(
                    task.id, TaskStatus.NEEDS_ATTENTION, rec.error
                )
                return self.store.get_task(task.key)

            try:
                result = step.action(task.ctx)
            except StepFailed as exc:
                rec.status = StepStatus.FAILED
                rec.error = str(exc)
                self.store.upsert_step(rec)
                return self._compensate(task, steps, idx)
            except OutcomeUnknown as exc:
                rec.status = StepStatus.UNKNOWN
                rec.error = str(exc)
                self.store.upsert_step(rec)
                resolved = self._query_step(task, step, rec)
                if resolved == "confirmed":
                    continue
                if resolved == "failed":
                    return self._compensate(task, steps, idx)
                self.store.update_task_status(
                    task.id, TaskStatus.NEEDS_ATTENTION, rec.error
                )
                return self.store.get_task(task.key)
            else:
                rec.status = StepStatus.CONFIRMED
                rec.result = result
                rec.error = None
                self.store.upsert_step(rec)

        self.store.update_task_status(task.id, TaskStatus.SUCCEEDED)
        return self.store.get_task(task.key)

    def _compensate(self, task: TaskRecord, steps: list, failed_idx: int) -> TaskRecord:
        """Undo confirmed steps in reverse order."""
        for idx in range(failed_idx - 1, -1, -1):
            step = steps[idx]
            rec = self._step_record(task, step.name)
            if rec.status != StepStatus.CONFIRMED:
                continue
            if step.compensate is None:
                rec.status = StepStatus.COMPENSATION_FAILED
                rec.error = "no compensation defined"
                self.store.upsert_step(rec)
                self.store.update_task_status(
                    task.id, TaskStatus.NEEDS_ATTENTION, rec.error
                )
                return self.store.get_task(task.key)
            try:
                step.compensate(task.ctx, rec.result)
            except StepFailed as exc:
                rec.status = StepStatus.COMPENSATION_FAILED
                rec.error = str(exc)
                self.store.upsert_step(rec)
                self.store.update_task_status(
                    task.id, TaskStatus.NEEDS_ATTENTION, rec.error
                )
                return self.store.get_task(task.key)
            except OutcomeUnknown as exc:
                rec.status = StepStatus.COMPENSATION_UNKNOWN
                rec.error = str(exc)
                self.store.upsert_step(rec)
                resolved = self._query_compensation(task, step, rec)
                if resolved == "confirmed":
                    continue
                self.store.update_task_status(
                    task.id, TaskStatus.NEEDS_ATTENTION, rec.error
                )
                return self.store.get_task(task.key)
            else:
                rec.status = StepStatus.COMPENSATED
                self.store.upsert_step(rec)
        self.store.update_task_status(
            task.id, TaskStatus.COMPENSATED, "compensated after failure"
        )
        return self.store.get_task(task.key)

    # ------------------------------------------------------------------
    # unknown-outcome resolution
    # ------------------------------------------------------------------

    def _resolve_unknowns(self, task: TaskRecord, steps: list) -> bool:
        """Try to clear UNKNOWN states via queries. Returns True if the
        task can keep being driven."""
        by_name = {s.name: s for s in steps}
        compensating = False
        for rec in list(task.steps):
            step = by_name.get(rec.name)
            if step is None:
                continue
            if rec.status == StepStatus.COMPENSATION_UNKNOWN:
                compensating = True
                if self._query_compensation(task, step, rec) != "confirmed":
                    return False
            elif rec.status == StepStatus.UNKNOWN:
                if self._query_step(task, step, rec) == "unknown":
                    return False
        task = self.store.get_task(task.key)
        has_failed = any(r.status == StepStatus.FAILED for r in task.steps)
        if compensating or has_failed or any(
            r.status in (StepStatus.COMPENSATION_FAILED, StepStatus.COMPENSATED)
            for r in task.steps
        ):
            failed_idx = next(
                (
                    i
                    for i, s in enumerate(steps)
                    if self._step_record(task, s.name).status == StepStatus.FAILED
                ),
                len(steps),
            )
            self.store.update_task_status(task.id, TaskStatus.RUNNING)
            result = self._compensate(self.store.get_task(task.key), steps, failed_idx)
            return result.status == TaskStatus.RUNNING
        self.store.update_task_status(task.id, TaskStatus.RUNNING)
        return True

    def _query_step(self, task: TaskRecord, step: Step, rec: StepRecord) -> str:
        if step.query is None:
            return "unknown"
        outcome = step.query(task.ctx)
        if outcome == "confirmed":
            rec.status = StepStatus.CONFIRMED
            self.store.upsert_step(rec)
        elif outcome == "failed":
            rec.status = StepStatus.FAILED
            self.store.upsert_step(rec)
        return outcome if outcome in ("confirmed", "failed") else "unknown"

    def _query_compensation(self, task: TaskRecord, step: Step, rec: StepRecord) -> str:
        if step.query_compensation is None:
            return "unknown"
        outcome = step.query_compensation(task.ctx, rec.result)
        if outcome == "confirmed":
            rec.status = StepStatus.COMPENSATED
            self.store.upsert_step(rec)
        elif outcome == "failed":
            rec.status = StepStatus.CONFIRMED  # compensation did not happen
            self.store.upsert_step(rec)
        return outcome if outcome in ("confirmed", "failed") else "unknown"

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _task_lock(self, task_id: int) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(task_id, threading.Lock())

    def _ensure_step_records(self, task: TaskRecord, steps: list) -> None:
        existing = {r.name for r in self.store.get_steps(task.id)}
        for idx, step in enumerate(steps):
            if step.name not in existing:
                self.store.upsert_step(
                    StepRecord(task_id=task.id, idx=idx, name=step.name)
                )

    def _step_record(self, task: TaskRecord, name: str) -> StepRecord:
        for rec in self.store.get_steps(task.id):
            if rec.name == name:
                return rec
        raise KeyError(f"no step record {name!r} for task {task.key!r}")
