"""协调记录持久化。

每个事务一份 JSON 记录，采用“写临时文件 + os.replace”原子落盘。
协调者退出后，新协调者依据磁盘上的真实记录接续，而不是依据内存猜测。
"""

from __future__ import annotations

import json
import os
from typing import Any, Protocol


class RecordStore(Protocol):
    def load(self, tx_id: str) -> dict[str, Any] | None: ...

    def save(self, tx_id: str, record: dict[str, Any]) -> None: ...


class JsonRecordStore:
    """以 JSON 文件保存协调记录的简单存储。"""

    def __init__(self, directory: str) -> None:
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, tx_id: str) -> str:
        safe = "".join(c for c in tx_id if c.isalnum() or c in "-_")
        return os.path.join(self.directory, f"{safe}.json")

    def load(self, tx_id: str) -> dict[str, Any] | None:
        path = self._path(tx_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save(self, tx_id: str, record: dict[str, Any]) -> None:
        path = self._path(tx_id)
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
