from __future__ import annotations

import asyncio
import socket
import struct
import logging
from typing import Callable, Tuple


class _ReceiverProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_message: Callable[[bytes, Tuple[str, int]], None], logger: logging.LoggerAdapter) -> None:
        self._on_message = on_message
        self._logger = logger

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            self._on_message(data, addr)
        except Exception:
            self._logger.exception("Erro no callback de processamento de mensagem")

    def error_received(self, exc: Exception) -> None:
        self._logger.error(f"Erro no transporte UDP: {exc}")

    def connection_lost(self, exc: Exception | None) -> None:
        if exc is not None:
            self._logger.error(f"Conexão UDP perdida: {exc}")
        else:
            self._logger.debug("Conexão UDP fechada")


async def criar_receiver_multicast(
    multicast_group: str,
    port: int,
    logger: logging.LoggerAdapter,
    on_message: Callable[[bytes, Tuple[str, int]], None],
):
    loop = asyncio.get_running_loop()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # Reuso de endereço/porta para múltiplos nós
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except OSError:
        pass
    try:
        # Nem sempre disponível, mas ajuda em alguns SOs
        sock.setsockopt(socket.SOL_SOCKET, getattr(socket, 'SO_REUSEPORT', 15), 1)
    except OSError:
        pass

    # Bind em todas as interfaces
    try:
        sock.bind(("", port))
    except OSError as e:
        logger.error(f"Falha no bind do socket UDP na porta {port}: {e}")
        raise

    # Entrar no grupo multicast
    try:
        mreq = struct.pack("=4sl", socket.inet_aton(multicast_group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    except OSError as e:
        logger.error(f"Falha ao ingressar no grupo multicast {multicast_group}: {e}")
        raise

    sock.setblocking(False)

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: _ReceiverProtocol(on_message, logger), sock=sock
    )
    return transport, protocol
