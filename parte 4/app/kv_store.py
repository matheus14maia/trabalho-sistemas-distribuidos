from __future__ import annotations

from typing import Any, Optional, Tuple, Dict
import json
import logging
import asyncio


class KeyValueStore:
    def __init__(self, logger: logging.LoggerAdapter, node_id: str) -> None:
        self._db: Dict[str, Any] = {}
        self._processed_seq: int = 0
        self._logger = logger
        self._node_id = node_id
        self._lock = asyncio.Lock()

    async def apply_with_seq(self, seq: int, cmd_type: str, key: Optional[str], value: Any) -> None:
        """
        Aplica comando com verificação de ordem local.
        """
        async with self._lock:
            if seq <= self._processed_seq:
                # duplicata ou atraso, ignora
                return
            if seq != self._processed_seq + 1:
                # fora de ordem; o chamador deve bufferizar, então aqui apenas log.
                self._logger.warning(
                    f"seq fora de ordem no apply direto (esperado {self._processed_seq + 1}, veio {seq})"
                )
                return
            self._processed_seq = seq
            if cmd_type == "SET":
                if key is None:
                    self._logger.warning("SET sem chave; ignorando")
                    return
                self._db[key] = value
            elif cmd_type == "GET":
                # GET não muda estado; apenas conta ordem
                pass
            elif cmd_type == "PRINT":
                pass

    async def handle_set(self, key: str, value: Any) -> int:
        async with self._lock:
            self._processed_seq += 1
            self._db[key] = value
            return self._processed_seq

    async def handle_get(self, key: str) -> Tuple[int, Optional[Any]]:
        async with self._lock:
            self._processed_seq += 1
            value = self._db.get(key)
            return self._processed_seq, value

    async def handle_print(self) -> Tuple[int, str]:
        async with self._lock:
            self._processed_seq += 1
            snapshot = json.dumps(self._db, ensure_ascii=False, sort_keys=True)
            return self._processed_seq, snapshot

    def get_value(self, key: str) -> Optional[Any]:
        return self._db.get(key)

    def snapshot(self) -> str:
        return json.dumps(self._db, ensure_ascii=False, sort_keys=True)

