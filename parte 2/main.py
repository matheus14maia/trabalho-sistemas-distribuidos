from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid

from app.kv_store import KeyValueStore
from models.messages import CommandMessage
from messaging.rabbitmq import CommandPublisher, CommandConsumer
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


async def handle_command(store: KeyValueStore, logger: logging.LoggerAdapter, cmd: CommandMessage) -> None:
    if cmd.command_type == "SET":
        if not cmd.key:
            logger.warning("Descartando SET sem chave")
            return
        order = await store.handle_set(cmd.key, cmd.value)
        logger.info(
            f"Recebeu [{cmd.message_uuid}] de [{cmd.sender_id}] -> Processando SET({cmd.key}, {json.dumps(cmd.value, ensure_ascii=False)}) | ordem_local={order}"
        )
        return

    if cmd.command_type == "GET":
        if not cmd.key:
            logger.warning("Descartando GET sem chave")
            return
        order, value = await store.handle_get(cmd.key)
        logger.info(
            f"Recebeu [{cmd.message_uuid}] de [{cmd.sender_id}] -> Processando GET({cmd.key}) | ordem_local={order} | valor_local={json.dumps(value, ensure_ascii=False)}"
        )
        return

    if cmd.command_type == "PRINT":
        order, snapshot = await store.handle_print()
        logger.info(
            f"Recebeu [{cmd.message_uuid}] de [{cmd.sender_id}] -> Processando PRINT | ordem_local={order} | snapshot={snapshot}"
        )
        return

    logger.warning(f"Comando desconhecido: {cmd.command_type}")


async def main() -> None:
    node_id = os.getenv("NODE_ID", f"node-{uuid.uuid4().hex[:6]}")
    amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@localhost/")
    exchange_name = os.getenv("EXCHANGE_NAME", "kv_commands")
    grpc_port = int(os.getenv("RPC_PORT", "50051"))

    logger = setup_logger(node_id)
    logger.info(
        f"Iniciando nó com ID={node_id} AMQP={amqp_url} exchange={exchange_name} porta_rpc={grpc_port}"
    )

    store = KeyValueStore(logger, node_id)
    publisher = CommandPublisher(amqp_url, exchange_name, logger)
    await publisher.connect()

    consumer = CommandConsumer(
        amqp_url=amqp_url,
        exchange_name=exchange_name,
        queue_name=node_id,
        on_command=lambda cmd: handle_command(store, logger, cmd),
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
    logger.info("Sistema pronto. Use o client CLI ou qualquer cliente gRPC JSON.")

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

