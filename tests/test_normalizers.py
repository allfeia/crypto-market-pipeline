from crypto_market_pipeline.normalizers import normalize_bybit_message, normalize_okx_message


def test_normalize_bybit_trade() -> None:
    events = normalize_bybit_message(
        {
            "topic": "publicTrade.BTCUSDT",
            "data": [
                {
                    "T": 1763633723421,
                    "s": "BTCUSDT",
                    "S": "Buy",
                    "v": "0.01",
                    "p": "67120.5",
                    "i": "bybit-trade-1",
                    "seq": 10,
                }
            ],
        },
        "BTCUSDT",
    )

    assert len(events) == 1
    assert events[0].exchange == "bybit"
    assert events[0].symbol == "BTCUSDT"
    assert events[0].event_type == "trade"
    assert events[0].payload["side"] == "buy"
    assert events[0].payload["price"] == "67120.5"


def test_normalize_okx_trade_uses_common_symbol() -> None:
    events = normalize_okx_message(
        {
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [
                {
                    "tradeId": "okx-trade-1",
                    "instId": "BTC-USDT",
                    "px": "67121.0",
                    "sz": "0.02",
                    "side": "sell",
                    "ts": "1763633723421",
                }
            ],
        },
        "BTCUSDT",
    )

    assert len(events) == 1
    assert events[0].symbol == "BTCUSDT"
    assert events[0].payload["exchange_symbol"] == "BTC-USDT"
    assert events[0].payload["side"] == "sell"


def test_normalize_bybit_orderbook_snapshot() -> None:
    events = normalize_bybit_message(
        {
            "topic": "orderbook.50.BTCUSDT",
            "ts": 1763633723421,
            "type": "snapshot",
            "data": {
                "s": "BTCUSDT",
                "u": 42,
                "seq": 100,
                "b": [["67120.0", "1.0"]],
                "a": [["67120.5", "2.0"]],
            },
        },
        "BTCUSDT",
    )

    assert len(events) == 1
    assert events[0].event_type == "orderbook"
    assert events[0].payload["message_type"] == "snapshot"
    assert events[0].payload["bids"] == [["67120.0", "1.0"]]
