from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from typing import Dict

from app.kv_store import KeyValueStore
from models.messages import CommandMessage
from messaging.rabbitmq import ToSequencerPublisher, OrderedConsumer
from rpc.json_grpc import create_grpc_server
from rpc.kv_service import KvService


def setup_logger(node_id: str) -> logging.LoggerAdapter:
    base_logger = logging.getLogger("kvnode")
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    base_logger.setLevel(getattr(logging, level_name, logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s [%(levelname)s] [node=%(node_id)s] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    if not base_logger.handlers:
        base_logger.addHandler(handler)
    return logging.LoggerAdapter(base_logger, extra={"node_id": node_id})


class OrderedApplier:
    """
    Recebe comandos com seq e aplica na ordem, bufferizando se chegar fora de ordem.
    """

    def __init__(self, store: KeyValueStore, logger: logging.LoggerAdapter) -> None:
        self._store = store
        self._logger = logger
        self._next_seq = 1
        self._buffer: Dict[int, CommandMessage] = {}

    async def handle(self, cmd: CommandMessage) -> None:
        if cmd.seq is None:
            self._logger.warning("Mensagem sem seq recebida no consumidor ordenado; descartando")
            return

        if cmd.seq < self._next_seq:
            self._logger.debug(f"Seq {cmd.seq} já aplicada; ignorando duplicata.")
            return

        self._buffer[cmd.seq] = cmd
        await self._drain()

    async def _drain(self) -> None:
        while self._next_seq in self._buffer:
            cmd = self._buffer.pop(self._next_seq)
            await self._apply(cmd)
            self._next_seq += 1

    async def _apply(self, cmd: CommandMessage) -> None:
        if cmd.command_type == "SET":
            if not cmd.key:
                self._logger.warning("Descartando SET sem chave")
                return
            order = await self._store.handle_set(cmd.key, cmd.value)
            self._logger.info(
                f"[seq={cmd.seq}] [{cmd.message_uuid}] de [{cmd.sender_id}] -> SET({cmd.key}, {json.dumps(cmd.value, ensure_ascii=False)}) | ordem_local={order}"
            )
            return

        if cmd.command_type == "GET":
            if not cmd.key:
                self._logger.warning("Descartando GET sem chave")
                return
            order, value = await self._store.handle_get(cmd.key)
            self._logger.info(
                f"[seq={cmd.seq}] [{cmd.message_uuid}] de [{cmd.sender_id}] -> GET({cmd.key}) | ordem_local={order} | valor_local={json.dumps(value, ensure_ascii=False)}"
            )
            return

        if cmd.command_type == "PRINT":
            order, snapshot = await self._store.handle_print()
            self._logger.info(
                f"[seq={cmd.seq}] [{cmd.message_uuid}] de [{cmd.sender_id}] -> PRINT | ordem_local={order} | snapshot={snapshot}"
            )
            return

        self._logger.warning(f"Comando desconhecido: {cmd.command_type}")


async def main() -> None:
    node_id = os.getenv("NODE_ID", f"node-{uuid.uuid4().hex[:6]}")
    amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
    to_seq_queue = os.getenv("TO_SEQ_QUEUE", "kv3_to_sequencer")
    ordered_exchange = os.getenv("ORDERED_EXCHANGE", "kv3_ordered")
    grpc_port = int(os.getenv("RPC_PORT", "50061"))

    logger = setup_logger(node_id)
    logger.info(
        f"Iniciando nó com ID={node_id} AMQP={amqp_url} fila_in={to_seq_queue} exchange_out={ordered_exchange} porta_rpc={grpc_port}"
    )

    store = KeyValueStore(logger, node_id)
    publisher = ToSequencerPublisher(amqp_url, to_seq_queue, logger)
    await publisher.connect()

    applier = OrderedApplier(store, logger)

    consumer = OrderedConsumer(
        amqp_url=amqp_url,
        exchange_name=ordered_exchange,
        queue_name=f"{node_id}_ordered",
        on_command=applier.handle,
        logger=logger,
    )
    await consumer.start()

    service = KvService(node_id=node_id, store=store, publisher=publisher, logger=logger)
    server = create_grpc_server(
        service_name="kv.KvService",
        methods={
            "Set": service.set,
            "Get": service.get,
            "Print": service.print_state,
        },
        port=grpc_port,
        logger=logger,
    )
    await server.start()
    logger.info("Nó pronto. Use o client CLI ou qualquer cliente gRPC JSON.")

    try:
        await server.wait_for_termination()
    finally:
        await consumer.close()
        await publisher.close()
        await server.stop(5)
        logger.info("Encerrado.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

