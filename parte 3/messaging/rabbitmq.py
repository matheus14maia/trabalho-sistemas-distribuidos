from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable
import aio_pika
import logging

from models.messages import CommandMessage


CommandHandler = Callable[[CommandMessage], Awaitable[None]]


class ToSequencerPublisher:
    """
    Publica comandos brutos na fila que o sequenciador consome.
    """

    def __init__(self, amqp_url: str, queue_name: str, logger: logging.LoggerAdapter) -> None:
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._logger = logger
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._queue: aio_pika.abc.AbstractRobustQueue | None = None

    async def connect(self) -> None:
        retries = 8
        delay = 2
        for attempt in range(1, retries + 1):
            try:
                self._connection = await aio_pika.connect_robust(self._amqp_url)
                self._channel = await self._connection.channel()
                self._queue = await self._channel.declare_queue(
                    self._queue_name, durable=False, auto_delete=False
                )
                self._logger.info(f"Publicador conectado ao RabbitMQ (fila {self._queue_name})")
                return
            except Exception:
                self._logger.warning(
                    f"Tentativa {attempt}/{retries} falhou ao conectar no RabbitMQ; aguardando {delay}s"
                )
                if attempt == retries:
                    raise
                await asyncio.sleep(delay)

    async def publish(self, message: CommandMessage) -> None:
        if not self._channel or not self._queue:
            raise RuntimeError("Publicador não inicializado")
        payload = message.to_json().encode("utf-8")
        try:
            await self._channel.default_exchange.publish(
                aio_pika.Message(body=payload, content_type="application/json"),
                routing_key=self._queue_name,
            )
        except Exception:
            self._logger.exception("Falha ao publicar mensagem para o sequenciador")
            raise

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
            self._logger.debug("Conexão RabbitMQ (publisher) fechada")


class OrderedConsumer:
    """
    Consome comandos já ordenados (fanout).
    """

    def __init__(
        self,
        amqp_url: str,
        exchange_name: str,
        queue_name: str,
        on_command: CommandHandler,
        logger: logging.LoggerAdapter,
    ) -> None:
        self._amqp_url = amqp_url
        self._exchange_name = exchange_name
        self._queue_name = queue_name
        self._on_command = on_command
        self._logger = logger
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._exchange: aio_pika.abc.AbstractRobustExchange | None = None
        self._queue: aio_pika.abc.AbstractRobustQueue | None = None
        self._consumer_tag: str | None = None

    async def start(self) -> None:
        retries = 8
        delay = 2
        for attempt in range(1, retries + 1):
            try:
                self._connection = await aio_pika.connect_robust(self._amqp_url)
                self._channel = await self._connection.channel()
                await self._channel.set_qos(prefetch_count=50)
                self._exchange = await self._channel.declare_exchange(
                    self._exchange_name, aio_pika.ExchangeType.FANOUT
                )
                self._queue = await self._channel.declare_queue(
                    self._queue_name,
                    durable=False,
                    auto_delete=True,
                )
                await self._queue.bind(self._exchange)
                self._consumer_tag = await self._queue.consume(self._handle_message)
                self._logger.info(
                    f"Consumidor conectado. Fila '{self._queue_name}' ligada ao exchange '{self._exchange_name}'."
                )
                return
            except Exception:
                self._logger.warning(
                    f"Tentativa {attempt}/{retries} falhou ao conectar/bind no RabbitMQ; aguardando {delay}s"
                )
                if attempt == retries:
                    raise
                await asyncio.sleep(delay)

    async def _handle_message(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process(ignore_processed=True):
            try:
                text = message.body.decode("utf-8")
            except UnicodeDecodeError:
                self._logger.warning("Descartando mensagem com payload inválido (UTF-8)")
                return
            try:
                cmd = CommandMessage.from_json(text)
            except Exception:
                self._logger.warning("Descartando mensagem inválida ao validar/parsear JSON/Pydantic")
                return

            try:
                await self._on_command(cmd)
            except Exception:
                self._logger.exception("Erro ao processar comando ordenado")
                await message.nack(requeue=False)
                return

    async def close(self) -> None:
        try:
            if self._queue and self._consumer_tag:
                await self._queue.cancel(self._consumer_tag)
        except Exception:
            pass

        if self._connection:
            await self._connection.close()
            self._logger.debug("Conexão RabbitMQ (ordered consumer) fechada")


class SequencerConsumerPublisher:
    """
    Consumidor do sequenciador: lê fila bruta, atribui seq e publica no exchange ordenado.
    """

    def __init__(
        self,
        amqp_url: str,
        in_queue: str,
        ordered_exchange: str,
        logger: logging.LoggerAdapter,
    ) -> None:
        self._amqp_url = amqp_url
        self._in_queue = in_queue
        self._ordered_exchange = ordered_exchange
        self._logger = logger
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._exchange: aio_pika.abc.AbstractRobustExchange | None = None
        self._queue: aio_pika.abc.AbstractRobustQueue | None = None
        self._consumer_tag: str | None = None
        self._seq_counter: int = 0

    async def start(self) -> None:
        retries = 12
        delay = 2
        for attempt in range(1, retries + 1):
            try:
                self._connection = await aio_pika.connect_robust(self._amqp_url)
                self._channel = await self._connection.channel()
                await self._channel.set_qos(prefetch_count=200)
                self._queue = await self._channel.declare_queue(
                    self._in_queue, durable=False, auto_delete=False
                )
                self._exchange = await self._channel.declare_exchange(
                    self._ordered_exchange, aio_pika.ExchangeType.FANOUT
                )
                self._consumer_tag = await self._queue.consume(self._handle_message)
                self._logger.info(
                    f"Sequenciador pronto: consumindo '{self._in_queue}' e publicando em '{self._ordered_exchange}'."
                )
                return
            except Exception:
                self._logger.warning(
                    f"Tentativa {attempt}/{retries} do sequenciador falhou ao conectar/bind no RabbitMQ; aguardando {delay}s"
                )
                if attempt == retries:
                    raise
                await asyncio.sleep(delay)

    async def _handle_message(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process(ignore_processed=True):
            try:
                text = message.body.decode("utf-8")
            except UnicodeDecodeError:
                self._logger.warning("Sequenciador descartou payload inválido (UTF-8)")
                return
            try:
                cmd = CommandMessage.from_json(text)
            except Exception:
                self._logger.warning("Sequenciador descartou mensagem inválida")
                return

            # Atribuir número de sequência global
            self._seq_counter += 1
            cmd.seq = self._seq_counter

            try:
                await self._exchange.publish(
                    aio_pika.Message(body=cmd.to_json().encode("utf-8"), content_type="application/json"),
                    routing_key="",
                )
                self._logger.debug(f"Sequência {cmd.seq} publicada")
            except Exception:
                self._logger.exception("Sequenciador falhou ao publicar mensagem ordenada")
                await message.nack(requeue=True)
                return

    async def close(self) -> None:
        try:
            if self._queue and self._consumer_tag:
                await self._queue.cancel(self._consumer_tag)
        except Exception:
            pass

        if self._connection:
            await self._connection.close()
            self._logger.debug("Conexão RabbitMQ (sequenciador) fechada")

