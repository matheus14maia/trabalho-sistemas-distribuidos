from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from typing import Tuple

from app.kv_store import KeyValueStore
from models.messages import CommandMessage
from network.sender import MulticastSender
from network.receiver import criar_receiver_multicast


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


async def user_input_loop(node_id: str, sender: MulticastSender, store: KeyValueStore, logger: logging.LoggerAdapter) -> None:
    help_text = (
        "Comandos: \n"
        "  SET <chave> <valor>    -> envia um comando SET via multicast\n"
        "  GET <chave>            -> envia um comando GET via multicast e mostra valor local\n"
        "  PRINT                  -> envia comando PRINT via multicast (cada nó loga seu snapshot)\n"
        "  HELP                   -> mostra este texto\n"
        "  EXIT                   -> encerra o processo\n"
    )
    print(help_text, flush=True)

    while True:
        try:
            line = await asyncio.to_thread(input, "> ")
        except (EOFError, KeyboardInterrupt):
            logger.info("Saindo por interrupção do usuário...")
            break

        if not line:
            continue
        parts = line.strip().split()
        if not parts:
            continue

        cmd = parts[0].upper()
        if cmd == "HELP":
            print(help_text, flush=True)
            continue
        if cmd in ("EXIT", "QUIT"):  # não envia nada, apenas sai
            logger.info("Encerrando por comando do usuário...")
            break

        if cmd == "SET":
            if len(parts) < 3:
                print("Uso: SET <chave> <valor>", flush=True)
                continue
            key = parts[1]
            value = " ".join(parts[2:])
            msg = CommandMessage(
                command_type="SET",
                key=key,
                value=value,
                sender_id=node_id,
                message_uuid=str(uuid.uuid4()),
            )
            await sender.enviar_multicast(msg.to_json().encode("utf-8"))
            continue

        if cmd == "GET":
            if len(parts) != 2:
                print("Uso: GET <chave>", flush=True)
                continue
            key = parts[1]
            msg = CommandMessage(
                command_type="GET",
                key=key,
                value=None,
                sender_id=node_id,
                message_uuid=str(uuid.uuid4()),
            )
            await sender.enviar_multicast(msg.to_json().encode("utf-8"))
            # Feedback imediato local (apenas informativo)
            local_val = store.get_value(key)
            print(f"[LOCAL] GET {key} -> {json.dumps(local_val, ensure_ascii=False)}", flush=True)
            continue

        if cmd == "PRINT":
            msg = CommandMessage(
                command_type="PRINT",
                key=None,
                value=None,
                sender_id=node_id,
                message_uuid=str(uuid.uuid4()),
            )
            await sender.enviar_multicast(msg.to_json().encode("utf-8"))
            continue

        print("Comando desconhecido. Digite HELP para ajuda.", flush=True)


async def main() -> None:
    multicast_group = os.getenv("MULTICAST_GROUP", "239.0.0.1")
    multicast_port = int(os.getenv("MULTICAST_PORT", "5007"))
    multicast_ttl = int(os.getenv("MULTICAST_TTL", "1"))
    node_id = os.getenv("NODE_ID", f"node-{uuid.uuid4().hex[:6]}")

    logger = setup_logger(node_id)
    logger.info(
        f"Iniciando nó com ID={node_id} grupo={multicast_group} porta={multicast_port} ttl={multicast_ttl}"
    )

    store = KeyValueStore(logger, node_id)
    sender = MulticastSender(multicast_group, multicast_port, multicast_ttl, logger)

    def on_datagram(data: bytes, addr: Tuple[str, int]) -> None:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("Descartando datagrama: falha de decodificação UTF-8")
            return

        # Validação/parse com Pydantic
        try:
            msg = CommandMessage.from_json(text)
        except Exception:
            logger.warning("Descartando mensagem inválida: falha na validação JSON/Pydantic")
            return

        # Guard clauses: campos essenciais
        if not msg.command_type:
            logger.warning("Descartando mensagem: command_type ausente")
            return
        if not msg.sender_id:
            logger.warning("Descartando mensagem: sender_id ausente")
            return
        if not msg.message_uuid:
            logger.warning("Descartando mensagem: message_uuid ausente")
            return

        # Processamento imediato sem ordenação garantida (demonstra inconsistência)
        if msg.command_type == "SET":
            if not msg.key:
                logger.warning("Descartando SET sem chave")
                return
            order = store.handle_set(msg.key, msg.value)
            logger.info(
                f"Recebeu [{msg.message_uuid}] de [{msg.sender_id}] -> Processando SET({msg.key}, {json.dumps(msg.value, ensure_ascii=False)}) | ordem_local={order}"
            )
            return

        if msg.command_type == "GET":
            if not msg.key:
                logger.warning("Descartando GET sem chave")
                return
            order, value = store.handle_get(msg.key)
            logger.info(
                f"Recebeu [{msg.message_uuid}] de [{msg.sender_id}] -> Processando GET({msg.key}) | ordem_local={order} | valor_local={json.dumps(value, ensure_ascii=False)}"
            )
            return

        if msg.command_type == "PRINT":
            order, snapshot = store.handle_print()
            logger.info(
                f"Recebeu [{msg.message_uuid}] de [{msg.sender_id}] -> Processando PRINT | ordem_local={order} | snapshot={snapshot}"
            )
            return

        logger.warning(f"Comando desconhecido: {msg.command_type}")

    # Inicia receiver multicast
    transport, _protocol = await criar_receiver_multicast(
        multicast_group, multicast_port, logger, on_datagram
    )

    # Inicia loop de entrada do usuário
    try:
        await user_input_loop(node_id, sender, store, logger)
    finally:
        try:
            transport.close()
        except Exception:
            pass
        sender.close()
        logger.info("Encerrado.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
