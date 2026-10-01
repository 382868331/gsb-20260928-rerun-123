"""SQLite-backed persistent coordination record store."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Optional

from .models import StepRecord, StepStatus, TaskRecord, TaskStatus


class Store:
    """Durable record of tasks and their step outcomes.

    Every effect transition is persisted before the coordinator moves
    on, so a recreated coordinator resumes from facts instead of
    re-executing from the first step.
    """

    def __init__(self, path: str = ":memory:"):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    ctx TEXT NOT NULL DEFAULT '{}',
                    error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS steps (
                    task_id INTEGER NOT NULL,
                    idx INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (task_id, name)
                );
                """
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- tasks ---------------------------------------------------------

    def create_task(self, key: str, ctx: dict) -> Optional[TaskRecord]:
        """Insert a new task; return None if the key already exists."""
        now = time.time()
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO tasks (key, status, ctx, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (key, TaskStatus.RUNNING.value, json.dumps(ctx), now, now),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                return None
            return self.get_task_by_id(cur.lastrowid)

    def get_task(self, key: str) -> Optional[TaskRecord]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE key = ?", (key,)
            ).fetchone()
        return self._to_task(row) if row else None

    def get_task_by_id(self, task_id: int) -> Optional[TaskRecord]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return self._to_task(row) if row else None

    def update_task_status(
        self, task_id: int, status: TaskStatus, error: Optional[str] = None
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (status.value, error, time.time(), task_id),
            )
            self._conn.commit()

    def unfinished_tasks(self) -> list:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE status NOT IN (?, ?, ?) ORDER BY id",
                (
                    TaskStatus.SUCCEEDED.value,
                    TaskStatus.COMPENSATED.value,
                    TaskStatus.FAILED.value,
                ),
            ).fetchall()
        return [self._to_task(r) for r in rows]

    # -- steps ---------------------------------------------------------

    def upsert_step(self, rec: StepRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO steps (task_id, idx, name, status, result, error, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(task_id, name) DO UPDATE SET"
                " status=excluded.status, result=excluded.result,"
                " error=excluded.error, updated_at=excluded.updated_at",
                (
                    rec.task_id,
                    rec.idx,
                    rec.name,
                    rec.status.value,
                    json.dumps(rec.result),
                    rec.error,
                    time.time(),
                ),
            )
            self._conn.commit()

    def get_steps(self, task_id: int) -> list:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM steps WHERE task_id = ? ORDER BY idx", (task_id,)
            ).fetchall()
        return [
            StepRecord(
                task_id=r["task_id"],
                idx=r["idx"],
                name=r["name"],
                status=StepStatus(r["status"]),
                result=json.loads(r["result"]) if r["result"] is not None else None,
                error=r["error"],
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    # -- helpers -------------------------------------------------------

    def _to_task(self, row: sqlite3.Row) -> TaskRecord:
        task = TaskRecord(
            id=row["id"],
            key=row["key"],
            status=TaskStatus(row["status"]),
            ctx=json.loads(row["ctx"]),
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        task.steps = self.get_steps(task.id)
        return task

