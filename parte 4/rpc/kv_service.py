from __future__ import annotations

import json
import uuid
from typing import Any, Dict
import logging

import grpc

from models.messages import CommandMessage
from messaging.rabbitmq import UpdatePublisher
from app.kv_store import KeyValueStore


class KvService:
    """
    Serviço RPC. No líder, aplica e replica para seguidores.
    Em seguidores, aceita GET/PRINT locais; SET é rejeitado para manter um único escritor.
    """

    def __init__(
        self,
        node_id: str,
        role: str,
        store: KeyValueStore,
        publisher: UpdatePublisher | None,
        logger: logging.LoggerAdapter,
        seq_counter_ref: dict,
    ) -> None:
        self._node_id = node_id
        self._role = role
        self._store = store
        self._publisher = publisher
        self._logger = logger
        self._seq_counter_ref = seq_counter_ref  # {"seq": int}

    def _next_seq(self) -> int:
        self._seq_counter_ref["seq"] += 1
        return self._seq_counter_ref["seq"]

    async def set(self, request: Dict[str, Any], context: grpc.aio.ServicerContext) -> Dict[str, Any]:
        if self._role != "leader":
            return {"status": "error", "message": "SET permitido apenas no líder", "node": self._node_id}

        key = request.get("key")
        value = request.get("value")
        if not key:
            return {"status": "error", "message": "chave é obrigatória"}

        seq = self._next_seq()
        cmd = CommandMessage(
            command_type="SET",
            key=key,
            value=value,
            sender_id=self._node_id,
            message_uuid=str(uuid.uuid4()),
            seq=seq,
        )
        # Aplica localmente antes de replicar
        await self._store.apply_with_seq(seq, "SET", key, value)
        await self._publisher.publish(cmd)  # type: ignore[arg-type]
        return {"status": "ok", "message": f"SET {key} aplicado e replicado", "node": self._node_id, "seq": seq}

    async def get(self, request: Dict[str, Any], context: grpc.aio.ServicerContext) -> Dict[str, Any]:
        key = request.get("key")
        if not key:
            return {"status": "error", "message": "chave é obrigatória"}
        local_val = self._store.get_value(key)
        return {
            "status": "ok",
            "message": f"GET {key} local",
            "node": self._node_id,
            "local_value": json.dumps(local_val, ensure_ascii=False),
        }

    async def print_state(self, request: Dict[str, Any], context: grpc.aio.ServicerContext) -> Dict[str, Any]:
        return {
            "status": "ok",
            "message": "PRINT local",
            "node": self._node_id,
            "snapshot": self._store.snapshot(),
        }

