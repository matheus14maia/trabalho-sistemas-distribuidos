from __future__ import annotations

import json
import uuid
from typing import Any, Dict
import logging

import grpc

from models.messages import CommandMessage
from messaging.rabbitmq import ToSequencerPublisher
from app.kv_store import KeyValueStore


class KvService:
    """
    Serviço RPC que recebe comandos, publica para o sequenciador
    e dá feedback imediato do estado local (consistência vem da
    ordem total aplicada depois).
    """

    def __init__(
        self,
        node_id: str,
        store: KeyValueStore,
        publisher: ToSequencerPublisher,
        logger: logging.LoggerAdapter,
    ) -> None:
        self._node_id = node_id
        self._store = store
        self._publisher = publisher
        self._logger = logger

    async def set(self, request: Dict[str, Any], context: grpc.aio.ServicerContext) -> Dict[str, Any]:
        key = request.get("key")
        value = request.get("value")
        if not key:
            return {"status": "error", "message": "chave é obrigatória"}

        cmd = CommandMessage(
            command_type="SET",
            key=key,
            value=value,
            sender_id=self._node_id,
            message_uuid=str(uuid.uuid4()),
        )
        await self._publisher.publish(cmd)
        return {"status": "ok", "message": f"SET {key} enviado ao sequenciador", "node": self._node_id}

    async def get(self, request: Dict[str, Any], context: grpc.aio.ServicerContext) -> Dict[str, Any]:
        key = request.get("key")
        if not key:
            return {"status": "error", "message": "chave é obrigatória"}

        cmd = CommandMessage(
            command_type="GET",
            key=key,
            value=None,
            sender_id=self._node_id,
            message_uuid=str(uuid.uuid4()),
        )
        await self._publisher.publish(cmd)
        local_val = self._store.get_value(key)
        return {
            "status": "ok",
            "message": f"GET {key} enviado ao sequenciador",
            "node": self._node_id,
            "local_value": json.dumps(local_val, ensure_ascii=False),
        }

    async def print_state(self, request: Dict[str, Any], context: grpc.aio.ServicerContext) -> Dict[str, Any]:
        cmd = CommandMessage(
            command_type="PRINT",
            key=None,
            value=None,
            sender_id=self._node_id,
            message_uuid=str(uuid.uuid4()),
        )
        await self._publisher.publish(cmd)
        return {
            "status": "ok",
            "message": "PRINT enviado ao sequenciador",
            "node": self._node_id,
            "snapshot": self._store.snapshot(),
        }

