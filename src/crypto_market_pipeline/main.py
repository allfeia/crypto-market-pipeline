from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

from .clients import (
    RetryableEventError,
    create_bybit_orderbook_client,
    create_bybit_trade_client,
    create_okx_orderbook_client,
    create_okx_trade_client,
)
from .normalizers import normalize_bybit_message, normalize_okx_message
from .sinks import EventSink, JsonlSink, KafkaSink


async def write_normalized(
    message: dict,
    normalize: Callable[[dict, str], list],
    symbol: str,
    sink: EventSink,
) -> None:
    try:
        events = normalize(message, symbol)
    except (KeyError, TypeError, ValueError):
        logging.getLogger(__name__).exception("Failed to normalize exchange message")
        return

    try:
        await sink.write(events)
    except Exception as error:
        raise RetryableEventError from error


async def collect_exchange(exchange: str, symbol: str, sink: EventSink) -> None:
    try:
        await sink.start()

        if exchange == "bybit":
            async def handle_trade(message: dict) -> None:
                await write_normalized(message, normalize_bybit_message, symbol, sink)

            async def handle_orderbook(message: dict) -> None:
                await write_normalized(message, normalize_bybit_message, symbol, sink)

            clients = (
                create_bybit_trade_client(symbol, handle_trade),
                create_bybit_orderbook_client(symbol, handle_orderbook),
            )
        else:
            async def handle_trade(message: dict) -> None:
                await write_normalized(message, normalize_okx_message, symbol, sink)

            async def handle_orderbook(message: dict) -> None:
                await write_normalized(message, normalize_okx_message, symbol, sink)

            clients = (
                create_okx_trade_client(symbol, handle_trade),
                create_okx_orderbook_client(symbol, handle_orderbook),
            )

        await asyncio.gather(*(client.run() for client in clients))
    finally:
        await sink.close()


def build_parser(exchange: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect public market data from Bybit or OKX")
    if exchange is None:
        parser.add_argument("--exchange", choices=("bybit", "okx"), required=True)
    parser.add_argument("--symbol", default="BTCUSDT", help="Normalized spot symbol, for example BTCUSDT")
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--sink", choices=("jsonl", "kafka"), default="jsonl")
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--kafka-client-id", default=None)
    return parser


def run_exchange(exchange: str | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser(exchange).parse_args()
    selected_exchange = exchange or args.exchange
    client_id = args.kafka_client_id or f"{selected_exchange}-market-collector"

    if args.sink == "kafka":
        sink: EventSink = KafkaSink(args.kafka_bootstrap_servers, client_id)
    else:
        sink = JsonlSink(args.output_dir / selected_exchange)

    try:
        asyncio.run(collect_exchange(selected_exchange, args.symbol, sink))
    except KeyboardInterrupt:
        logging.info("%s collector stopped", selected_exchange)


def run() -> None:
    run_exchange()


def run_bybit() -> None:
    run_exchange("bybit")


def run_okx() -> None:
    run_exchange("okx")


if __name__ == "__main__":
    run()
