from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from .clients import create_bybit_client, create_okx_client
from .normalizers import normalize_bybit_message, normalize_okx_message
from .sinks import EventSink, JsonlSink, KafkaSink


async def collect(symbol: str, sink: EventSink) -> None:
    await sink.start()

    async def handle_bybit(message: dict) -> None:
        events = normalize_bybit_message(message, symbol)
        await sink.write(events)

    async def handle_okx(message: dict) -> None:
        events = normalize_okx_message(message, symbol)
        await sink.write(events)

    bybit = create_bybit_client(symbol, handle_bybit)
    okx = create_okx_client(symbol, handle_okx)
    try:
        await asyncio.gather(bybit.run(), okx.run())
    finally:
        await sink.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect public Bybit and OKX market data")
    parser.add_argument("--symbol", default="BTCUSDT", help="Normalized spot symbol, for example BTCUSDT")
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--sink", choices=("jsonl", "kafka"), default="jsonl")
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--kafka-client-id", default="market-collector")
    return parser.parse_args()


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    sink: EventSink
    if args.sink == "kafka":
        sink = KafkaSink(args.kafka_bootstrap_servers, args.kafka_client_id)
    else:
        sink = JsonlSink(args.output_dir)
    try:
        asyncio.run(collect(args.symbol, sink))
    except KeyboardInterrupt:
        logging.info("Collector stopped")


if __name__ == "__main__":
    run()
