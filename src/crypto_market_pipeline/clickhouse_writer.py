from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from .models import NormalizedEvent

logger = logging.getLogger(__name__)

COLUMNS = (
    "event_id",
    "exchange",
    "market_type",
    "symbol",
    "event_type",
    "exchange_event_id",
    "event_time",
    "ingest_time",
    "payload_json",
)


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


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def event_to_row(event: NormalizedEvent) -> tuple[Any, ...]:
    return (
        event.event_id,
        event.exchange,
        event.market_type,
        event.symbol,
        event.event_type,
        event.exchange_event_id,
        parse_timestamp(event.event_time),
        parse_timestamp(event.ingest_time),
        json.dumps(event.payload, ensure_ascii=False, separators=(",", ":")),
    )


class ClickHouseRawWriter:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        client: Any,
        flush_rows: int,
        flush_interval_seconds: float,
    ) -> None:
        self.consumer = consumer
        self.client = client
        self.flush_rows = flush_rows
        self.flush_interval_seconds = flush_interval_seconds
        self.rows: list[tuple[Any, ...]] = []

    async def flush_and_commit(self) -> None:
        if not self.rows:
            return
        rows = self.rows
        await asyncio.to_thread(
            self.client.insert,
            "raw.market_events",
            rows,
            column_names=list(COLUMNS),
        )
        await self.consumer.commit()
        self.rows = []
        logger.info("Inserted %s raw event(s) into ClickHouse", len(rows))

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
                    await self.flush_and_commit()
                    timer_task = asyncio.create_task(asyncio.sleep(self.flush_interval_seconds))

                if message_task not in done:
                    continue

                message = message_task.result()
                message_task = asyncio.create_task(self.consumer.getone())
                event = self.parse_event(message.value)
                if event is None:
                    continue
                self.rows.append(event_to_row(event))
                if len(self.rows) >= self.flush_rows:
                    await self.flush_and_commit()
        finally:
            message_task.cancel()
            timer_task.cancel()
            await asyncio.gather(message_task, timer_task, return_exceptions=True)
            await self.flush_and_commit()
            await self.consumer.stop()

    @staticmethod
    def parse_event(value: bytes) -> NormalizedEvent | None:
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


async def consume(args: argparse.Namespace) -> None:
    import clickhouse_connect

    client = await asyncio.to_thread(
        clickhouse_connect.get_client,
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        username=args.clickhouse_user,
        password=args.clickhouse_password,
        database="raw",
    )
    consumer = AIOKafkaConsumer(
        "market.trades",
        "market.orderbook",
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: value,
    )
    await ClickHouseRawWriter(
        consumer,
        client,
        args.flush_rows,
        args.flush_interval_seconds,
    ).run()
    await asyncio.to_thread(client.close)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive Kafka market events in ClickHouse raw")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group-id", default="market-raw-clickhouse")
    parser.add_argument("--clickhouse-host", default="localhost")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--flush-rows", type=positive_int, default=1000)
    parser.add_argument("--flush-interval-seconds", type=positive_float, default=1.0)
    return parser


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args()
    try:
        asyncio.run(consume(args))
    except KeyboardInterrupt:
        logger.info("ClickHouse raw writer stopped")


if __name__ == "__main__":
    run()
