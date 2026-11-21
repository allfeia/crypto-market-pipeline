from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import NormalizedEvent


def milliseconds_to_datetime(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def normalize_bybit_message(message: dict[str, Any], symbol: str) -> list[NormalizedEvent]:
    topic = message.get("topic", "")
    data = message.get("data")
    if not isinstance(data, (list, dict)):
        return []

    if topic.startswith("publicTrade."):
        trades = data if isinstance(data, list) else [data]
        return [
            NormalizedEvent.create(
                exchange="bybit",
                market_type="spot",
                symbol=item.get("s", symbol),
                event_type="trade",
                exchange_event_id=str(item["i"]),
                event_time=milliseconds_to_datetime(item["T"]),
                payload={
                    "price": item["p"],
                    "quantity": item["v"],
                    "side": item["S"].lower(),
                    "sequence": item.get("seq"),
                    "raw": item,
                },
            )
            for item in trades
            if item.get("i") is not None and item.get("T") is not None
        ]

    if topic.startswith("orderbook.") and isinstance(data, dict):
        update_id = data.get("u")
        event_time = message.get("ts") or data.get("cts")
        if update_id is None or event_time is None:
            return []
        return [
            NormalizedEvent.create(
                exchange="bybit",
                market_type="spot",
                symbol=data.get("s", symbol),
                event_type="orderbook",
                exchange_event_id=str(update_id),
                event_time=milliseconds_to_datetime(event_time),
                payload={
                    "message_type": message.get("type", "unknown"),
                    "update_id": update_id,
                    "sequence": data.get("seq"),
                    "bids": data.get("b", []),
                    "asks": data.get("a", []),
                    "raw": message,
                },
            )
        ]

    return []


def normalize_okx_message(message: dict[str, Any], symbol: str) -> list[NormalizedEvent]:
    arg = message.get("arg", {})
    channel = arg.get("channel")
    data = message.get("data")
    if not isinstance(data, list):
        return []

    if channel == "trades":
        return [
            NormalizedEvent.create(
                exchange="okx",
                market_type="spot",
                symbol=symbol,
                event_type="trade",
                exchange_event_id=str(item["tradeId"]),
                event_time=milliseconds_to_datetime(item["ts"]),
                payload={
                    "price": item["px"],
                    "quantity": item["sz"],
                    "side": item["side"],
                    "exchange_symbol": item.get("instId", symbol),
                    "sequence": item.get("seqId"),
                    "raw": item,
                },
            )
            for item in data
            if item.get("tradeId") is not None and item.get("ts") is not None
        ]

    if channel in {"books5", "books"}:
        events: list[NormalizedEvent] = []
        for item in data:
            update_id = item.get("seqId") or item.get("checksum") or item.get("ts")
            if update_id is None or item.get("ts") is None:
                continue
            events.append(
                NormalizedEvent.create(
                    exchange="okx",
                    market_type="spot",
                    symbol=symbol,
                    event_type="orderbook",
                    exchange_event_id=str(update_id),
                    event_time=milliseconds_to_datetime(item["ts"]),
                    payload={
                        "message_type": "snapshot_or_update",
                        "update_id": update_id,
                        "sequence": item.get("seqId"),
                        "exchange_symbol": item.get("instId", symbol),
                        "bids": item.get("bids", []),
                        "asks": item.get("asks", []),
                        "raw": item,
                    },
                )
            )
        return events

    return []
