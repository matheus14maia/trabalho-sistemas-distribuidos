from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid

from app.kv_store import KeyValueStore
from messaging.rabbitmq import UpdatePublisher, UpdateConsumer
from models.messages import CommandMessage
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


class FollowerApplier:
    """
    Aplica atualizações na ordem produzida pelo líder.
    Como há um único produtor, assume-se ordem por fila; ainda assim valida seq.
    """

    def __init__(self, store: KeyValueStore, logger: logging.LoggerAdapter) -> None:
        self._store = store
        self._logger = logger
        self._next_seq = 1

    async def handle(self, cmd: CommandMessage) -> None:
        if cmd.seq is None:
            self._logger.warning("Mensagem sem seq recebida no seguidor; descartando")
            return
        if cmd.seq < self._next_seq:
            self._logger.debug(f"Seq {cmd.seq} já aplicada; ignorando duplicata.")
            return
        if cmd.seq != self._next_seq:
            self._logger.warning(f"Seq fora de ordem (esperado {self._next_seq}, veio {cmd.seq}); aguardando")
            # Simplesmente não aplica; aguardará a próxima mensagem correta (assumindo ordem do broker)
            return

        if cmd.command_type == "SET":
            await self._store.apply_with_seq(cmd.seq, "SET", cmd.key, cmd.value)
            self._logger.info(
                f"[seq={cmd.seq}] [{cmd.message_uuid}] de [{cmd.sender_id}] -> SET({cmd.key}) aplicado no seguidor"
            )
        elif cmd.command_type == "GET":
            await self._store.apply_with_seq(cmd.seq, "GET", cmd.key, cmd.value)
            self._logger.info(
                f"[seq={cmd.seq}] [{cmd.message_uuid}] de [{cmd.sender_id}] -> GET({cmd.key}) contado no seguidor"
            )
        elif cmd.command_type == "PRINT":
            await self._store.apply_with_seq(cmd.seq, "PRINT", cmd.key, cmd.value)
            self._logger.info(
                f"[seq={cmd.seq}] [{cmd.message_uuid}] de [{cmd.sender_id}] -> PRINT registrado no seguidor"
            )
        else:
            self._logger.warning(f"Comando desconhecido: {cmd.command_type}")
            return

        self._next_seq += 1


async def main() -> None:
    node_id = os.getenv("NODE_ID", f"node-{uuid.uuid4().hex[:6]}")
    role = os.getenv("ROLE", "follower").lower()
    amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
    updates_exchange = os.getenv("UPDATES_EXCHANGE", "kv4_updates")
    grpc_port = int(os.getenv("RPC_PORT", "50071"))

    logger = setup_logger(node_id)
    logger.info(
        f"Iniciando nó com ID={node_id} role={role} AMQP={amqp_url} exchange_updates={updates_exchange} porta_rpc={grpc_port}"
    )

    store = KeyValueStore(logger, node_id)
    seq_counter = {"seq": 0}

    publisher: UpdatePublisher | None = None
    consumer: UpdateConsumer | None = None

    if role == "leader":
        publisher = UpdatePublisher(amqp_url, updates_exchange, logger)
        await publisher.connect()
    else:
        applier = FollowerApplier(store, logger)
        consumer = UpdateConsumer(
            amqp_url=amqp_url,
            exchange_name=updates_exchange,
            queue_name=f"{node_id}_updates",
            on_command=applier.handle,
            logger=logger,
        )
        await consumer.start()

    service = KvService(
        node_id=node_id,
        role=role,
        store=store,
        publisher=publisher,
        logger=logger,
        seq_counter_ref=seq_counter,
    )

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
    logger.info("Nó pronto. Use o client CLI (apontando para o líder para SET).")

    try:
        await server.wait_for_termination()
    finally:
        if consumer:
            await consumer.close()
        if publisher:
            await publisher.close()
        await server.stop(5)
        logger.info("Encerrado.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

