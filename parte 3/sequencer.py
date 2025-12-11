from __future__ import annotations

import asyncio
import logging
import os
import sys

from messaging.rabbitmq import SequencerConsumerPublisher


def setup_logger(name: str = "sequencer") -> logging.LoggerAdapter:
    base_logger = logging.getLogger(name)
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    base_logger.setLevel(getattr(logging, level_name, logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s [%(levelname)s] [comp=sequencer] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    if not base_logger.handlers:
        base_logger.addHandler(handler)
    return logging.LoggerAdapter(base_logger, extra={})


async def main() -> None:
    amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
    to_seq_queue = os.getenv("TO_SEQ_QUEUE", "kv3_to_sequencer")
    ordered_exchange = os.getenv("ORDERED_EXCHANGE", "kv3_ordered")

    logger = setup_logger()
    logger.info(
        f"Iniciando sequenciador com AMQP={amqp_url} in_queue={to_seq_queue} ordered_exchange={ordered_exchange}"
    )

    seq = SequencerConsumerPublisher(
        amqp_url=amqp_url,
        in_queue=to_seq_queue,
        ordered_exchange=ordered_exchange,
        logger=logger,
    )
    await seq.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await seq.close()
        logger.info("Sequenciador encerrado.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

