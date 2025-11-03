from __future__ import annotations

import asyncio
import socket
import struct
import logging


class MulticastSender:
    def __init__(self, multicast_group: str, port: int, ttl: int, logger: logging.LoggerAdapter) -> None:
        self._group = multicast_group
        self._port = port
        self._logger = logger
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # TTL (escopo) do multicast
        ttl_bin = struct.pack('b', max(1, min(ttl, 255)))
        self._sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)
        # Permite receber as próprias mensagens (loopback) para que o nó aplique seu próprio comando via recepção
        try:
            self._sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        except OSError:
            pass
        self._sock.setblocking(False)

    async def enviar_multicast(self, payload: bytes) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.sock_sendto(self._sock, payload, (self._group, self._port))
            self._logger.debug(f"Enviado multicast {len(payload)} bytes para {self._group}:{self._port}")
        except Exception:
            self._logger.exception("Falha ao enviar mensagem multicast")

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass
