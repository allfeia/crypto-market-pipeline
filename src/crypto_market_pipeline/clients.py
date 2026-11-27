from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

logger = logging.getLogger(__name__)
EventHandler = Callable[[Any], Awaitable[None]]
GapHandler = Callable[[datetime, datetime], Awaitable[None]]
EventTimeExtractor = Callable[[dict[str, Any]], datetime | None]


class RetryableEventError(Exception):
    """Temporary sink failure that should pause delivery and be retried."""


class ReconnectingWebSocketClient:
    def __init__(
        self,
        name: str,
        url: str,
        subscription: dict[str, Any],
        handler: EventHandler,
        on_gap: GapHandler | None = None,
        event_time_extractor: EventTimeExtractor | None = None,
    ) -> None:
        self.name = name
        self.url = url
        self.subscription = subscription
        self.handler = handler
        self.on_gap = on_gap
        self.event_time_extractor = event_time_extractor

    async def run(self) -> None:
        delay = 1
        while True:
            last_message_at: datetime | None = None
            try:
                logger.info("Connecting to %s", self.name)
                async with connect(self.url, ping_interval=20, ping_timeout=20) as websocket:
                    await websocket.send(json.dumps(self.subscription))
                    logger.info("Subscribed to %s", self.name)
                    delay = 1
                    last_message_at = await self._read_messages(websocket)
                    if last_message_at is not None:
                        raise ConnectionError(f"{self.name} stream closed")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s connection failed", self.name)
                logger.info("Reconnecting to %s in %s seconds", self.name, delay)
                if self.on_gap and last_message_at is not None:
                    gap_start = last_message_at
                    gap_end = datetime.now(timezone.utc)
                    if gap_start < gap_end:
                        try:
                            await self.on_gap(gap_start, gap_end)
                        except Exception:
                            logger.exception("REST gap recovery failed for %s", self.name)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _read_messages(self, websocket: ClientConnection) -> datetime | None:
        last_event_time: datetime | None = None
        async for raw_message in websocket:
            if raw_message == "ping":
                await websocket.send("pong")
                continue

            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning("%s sent a non-JSON message", self.name)
                continue

            if not isinstance(message, dict):
                logger.warning("%s sent a JSON value instead of an object", self.name)
                continue

            if message.get("op") == "ping":
                await websocket.send(json.dumps({"op": "pong"}))
                continue

            delay = 1
            while True:
                try:
                    await self.handler(message)
                    if self.event_time_extractor is not None:
                        extracted = self.event_time_extractor(message)
                        if extracted is not None:
                            last_event_time = extracted
                    break
                except asyncio.CancelledError:
                    raise
                except RetryableEventError:
                    logger.exception("Failed to handle message from %s", self.name)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)
                except Exception:
                    logger.exception("Discarding invalid message from %s", self.name)
                    break
        return last_event_time


def _bybit_trade_event_time(message: dict[str, Any]) -> datetime | None:
    data = message.get("data")
    if not message.get("topic", "").startswith("publicTrade.") or not isinstance(data, list):
        return None
    values = [item.get("T") for item in data if item.get("T") is not None]
    if not values:
        return None
    return datetime.fromtimestamp(max(int(value) for value in values) / 1000, tz=timezone.utc)


def _okx_trade_event_time(message: dict[str, Any]) -> datetime | None:
    if message.get("arg", {}).get("channel") != "trades" or not isinstance(message.get("data"), list):
        return None
    values = [item.get("ts") for item in message["data"] if item.get("ts") is not None]
    if not values:
        return None
    return datetime.fromtimestamp(max(int(value) for value in values) / 1000, tz=timezone.utc)


def create_bybit_trade_client(symbol: str, handler: EventHandler, on_gap: GapHandler | None = None) -> ReconnectingWebSocketClient:
    return ReconnectingWebSocketClient(
        name="bybit trades",
        url="wss://stream.bybit.com/v5/public/spot",
        subscription={
            "op": "subscribe",
            "args": [f"publicTrade.{symbol}"],
        },
        handler=handler,
        on_gap=on_gap,
        event_time_extractor=_bybit_trade_event_time,
    )


def create_bybit_orderbook_client(symbol: str, handler: EventHandler) -> ReconnectingWebSocketClient:
    return ReconnectingWebSocketClient(
        name="bybit orderbook",
        url="wss://stream.bybit.com/v5/public/spot",
        subscription={
            "op": "subscribe",
            "args": [f"orderbook.50.{symbol}"],
        },
        handler=handler,
    )


def create_okx_trade_client(symbol: str, handler: EventHandler, on_gap: GapHandler | None = None) -> ReconnectingWebSocketClient:
    okx_symbol = symbol[:-4] + "-" + symbol[-4:] if symbol.endswith("USDT") else symbol
    return ReconnectingWebSocketClient(
        name="okx trades",
        url="wss://wsus.okx.com/ws/v5/public",
        subscription={
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": okx_symbol}],
        },
        handler=handler,
        on_gap=on_gap,
        event_time_extractor=_okx_trade_event_time,
    )


def create_okx_orderbook_client(symbol: str, handler: EventHandler) -> ReconnectingWebSocketClient:
    okx_symbol = symbol[:-4] + "-" + symbol[-4:] if symbol.endswith("USDT") else symbol
    return ReconnectingWebSocketClient(
        name="okx orderbook",
        url="wss://wsus.okx.com/ws/v5/public",
        subscription={
            "op": "subscribe",
            "args": [{"channel": "books5", "instId": okx_symbol}],
        },
        handler=handler,
    )
