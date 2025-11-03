from __future__ import annotations

from typing import Any, Optional, Tuple, Dict
import json
import logging


class KeyValueStore:
    def __init__(self, logger: logging.LoggerAdapter, node_id: str) -> None:
        self._db: Dict[str, Any] = {}
        self._processed_seq: int = 0
        self._logger = logger
        self._node_id = node_id

    def _next_seq(self) -> int:
        self._processed_seq += 1
        return self._processed_seq

    def handle_set(self, key: str, value: Any) -> int:
        order = self._next_seq()
        self._db[key] = value
        return order

    def handle_get(self, key: str) -> Tuple[int, Optional[Any]]:
        order = self._next_seq()
        value = self._db.get(key)
        return order, value

    def handle_print(self) -> Tuple[int, str]:
        order = self._next_seq()
        snapshot = json.dumps(self._db, ensure_ascii=False, sort_keys=True)
        return order, snapshot

    def get_value(self, key: str) -> Optional[Any]:
        return self._db.get(key)
