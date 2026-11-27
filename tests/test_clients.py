import asyncio

from crypto_market_pipeline.clients import (
    ReconnectingWebSocketClient,
    create_bybit_orderbook_client,
    create_bybit_trade_client,
    create_okx_orderbook_client,
    create_okx_trade_client,
)


async def handle_message(_: dict) -> None:
    return None


class FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = messages
        self.sent: list[str] = []

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)

    async def send(self, message: str) -> None:
        self.sent.append(message)


def test_bybit_trades_and_orderbook_have_separate_subscriptions() -> None:
    trades = create_bybit_trade_client("BTCUSDT", handle_message)
    orderbook = create_bybit_orderbook_client("BTCUSDT", handle_message)

    assert trades.subscription["args"] == ["publicTrade.BTCUSDT"]
    assert orderbook.subscription["args"] == ["orderbook.50.BTCUSDT"]


def test_okx_trades_and_orderbook_have_separate_subscriptions() -> None:
    trades = create_okx_trade_client("BTCUSDT", handle_message)
    orderbook = create_okx_orderbook_client("BTCUSDT", handle_message)

    assert trades.subscription["args"] == [{"channel": "trades", "instId": "BTC-USDT"}]
    assert orderbook.subscription["args"] == [{"channel": "books5", "instId": "BTC-USDT"}]


def test_client_answers_application_level_ping() -> None:
    received: list[dict] = []

    async def handler(message: dict) -> None:
        received.append(message)

    client = ReconnectingWebSocketClient("test", "wss://example.test", {}, handler)
    websocket = FakeWebSocket(["ping", '{"topic":"ignored"}'])

    asyncio.run(client._read_messages(websocket))

    assert websocket.sent == ["pong"]
    assert received[0]["topic"] == "ignored"
