from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

logger = logging.getLogger(__name__)
EventHandler = Callable[[Any], Awaitable[None]]


class RetryableEventError(Exception):
    """Temporary sink failure that should pause delivery and be retried."""


class ReconnectingWebSocketClient:
    def __init__(self, name: str, url: str, subscription: dict[str, Any], handler: EventHandler) -> None:
        self.name = name
        self.url = url
        self.subscription = subscription
        self.handler = handler

    async def run(self) -> None:
        delay = 1
        while True:
            try:
                logger.info("Connecting to %s", self.name)
                async with connect(self.url, ping_interval=20, ping_timeout=20) as websocket:
                    await websocket.send(json.dumps(self.subscription))
                    logger.info("Subscribed to %s", self.name)
                    delay = 1
                    await self._read_messages(websocket)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s connection failed", self.name)
                logger.info("Reconnecting to %s in %s seconds", self.name, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _read_messages(self, websocket: ClientConnection) -> None:
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


def create_bybit_trade_client(symbol: str, handler: EventHandler) -> ReconnectingWebSocketClient:
    return ReconnectingWebSocketClient(
        name="bybit trades",
        url="wss://stream.bybit.com/v5/public/spot",
        subscription={
            "op": "subscribe",
            "args": [f"publicTrade.{symbol}"],
        },
        handler=handler,
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


def create_okx_trade_client(symbol: str, handler: EventHandler) -> ReconnectingWebSocketClient:
    okx_symbol = symbol[:-4] + "-" + symbol[-4:] if symbol.endswith("USDT") else symbol
    return ReconnectingWebSocketClient(
        name="okx trades",
        url="wss://wsus.okx.com/ws/v5/public",
        subscription={
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": okx_symbol}],
        },
        handler=handler,
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
