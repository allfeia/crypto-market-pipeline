from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer

from .archive import ArchiveSink, HourlyArchive, MinioParquetSink
from .models import NormalizedEvent

logger = logging.getLogger(__name__)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


class KafkaArchiveConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        archive: HourlyArchive,
        flush_rows: int,
        flush_interval_seconds: float,
    ) -> None:
        self.consumer = consumer
        self.archive = archive
        self.flush_rows = flush_rows
        self.flush_interval_seconds = flush_interval_seconds

    async def _flush_and_commit(self) -> int:
        written = await asyncio.to_thread(self.archive.flush)
        await self.consumer.commit()
        return written

    async def run(self) -> None:
        await self.consumer.start()
        message_task = asyncio.create_task(self.consumer.getone())
        timer_task = asyncio.create_task(asyncio.sleep(self.flush_interval_seconds))
        try:
            while True:
                done, _ = await asyncio.wait(
                    {message_task, timer_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if timer_task in done:
                    if self.archive.pending_rows:
                        written = await self._flush_and_commit()
                        logger.info("Archived %s event(s) on periodic timer", written)
                    timer_task = asyncio.create_task(asyncio.sleep(self.flush_interval_seconds))

                if message_task not in done:
                    continue

                message = message_task.result()
                message_task = asyncio.create_task(self.consumer.getone())

                event = self._parse_event(message.value)
                if event is None:
                    continue
                if self.archive.add(event):
                    if self.archive.pending_rows >= self.flush_rows:
                        written = await self._flush_and_commit()
                        logger.info("Archived %s event(s) after row limit", written)
        finally:
            message_task.cancel()
            timer_task.cancel()
            await asyncio.gather(message_task, timer_task, return_exceptions=True)
            if self.archive.pending_rows:
                written = await self._flush_and_commit()
                logger.info("Archived %s event(s) before shutdown", written)
            await self.consumer.stop()

    @staticmethod
    def _parse_event(value: bytes) -> NormalizedEvent | None:
        try:
            data: dict[str, Any] = json.loads(value)
            required = {
                "event_id",
                "exchange",
                "market_type",
                "symbol",
                "event_type",
                "exchange_event_id",
                "event_time",
                "ingest_time",
                "payload",
            }
            if not required.issubset(data):
                logger.warning("Skipping event with missing fields: %s", required - data.keys())
                return None
            return NormalizedEvent(**data)
        except (json.JSONDecodeError, TypeError, ValueError) as error:
            logger.warning("Skipping invalid Kafka event: %s", error)
            return None


async def consume(args: argparse.Namespace, sink: ArchiveSink) -> None:
    consumer = AIOKafkaConsumer(
        "market.trades",
        "market.orderbook",
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: value,
    )
    await KafkaArchiveConsumer(
        consumer,
        HourlyArchive(sink),
        args.flush_rows,
        args.flush_interval_seconds,
    ).run()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive Kafka market events as hourly Parquet")
    parser.add_argument("--sink", choices=("minio",), default="minio")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group-id", default="market-raw-archiver")
    parser.add_argument("--minio-endpoint", default="localhost:9000")
    parser.add_argument("--minio-access-key", default="minioadmin")
    parser.add_argument("--minio-secret-key", default="minioadmin")
    parser.add_argument("--minio-bucket", default="market-raw")
    parser.add_argument("--flush-rows", type=positive_int, default=10000)
    parser.add_argument("--flush-interval-seconds", type=positive_float, default=60.0)
    return parser


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args()
    sink: ArchiveSink = MinioParquetSink(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        bucket=args.minio_bucket,
    )

    try:
        asyncio.run(consume(args, sink))
    except KeyboardInterrupt:
        logger.info("Archiver stopped")


if __name__ == "__main__":
    run()
