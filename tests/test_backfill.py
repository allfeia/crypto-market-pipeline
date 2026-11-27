from datetime import datetime, timezone

import asyncio

from crypto_market_pipeline.backfill import (
    fetch_bybit_archive_trades,
    fetch_bybit_trades,
    fetch_okx_trades,
    recover_trade_gap,
)


def window() -> tuple[datetime, datetime]:
    return (
        datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 26, 10, 1, tzinfo=timezone.utc),
    )


def test_bybit_rest_trade_uses_same_event_identity(monkeypatch) -> None:
    monkeypatch.setattr(
        "crypto_market_pipeline.backfill._fetch_json",
        lambda *_: {
            "retCode": 0,
            "result": {"list": [{"execId": "trade-1", "time": "1795687200000", "symbol": "BTCUSDT", "price": "100", "size": "0.1", "side": "Buy"}]},
        },
    )

    events = fetch_bybit_trades("BTCUSDT", *window())

    assert len(events) == 1
    assert events[0].event_id
    assert events[0].exchange_event_id == "trade-1"


def test_okx_rest_trade_uses_common_symbol(monkeypatch) -> None:
    monkeypatch.setattr(
        "crypto_market_pipeline.backfill._fetch_json",
        lambda *_: {
            "code": "0",
            "data": [{"tradeId": "trade-1", "ts": "1795687200000", "instId": "BTC-USDT", "px": "100", "sz": "0.1", "side": "buy"}],
        },
    )

    events = fetch_okx_trades("BTCUSDT", *window())

    assert len(events) == 1
    assert events[0].symbol == "BTCUSDT"
    assert events[0].exchange_event_id == "trade-1"


def test_okx_history_trades_paginates_until_gap_start(monkeypatch) -> None:
    responses = iter([
        {
            "code": "0",
            "data": [{"tradeId": "20", "ts": "1795687260000", "instId": "BTC-USDT", "px": "101", "sz": "0.1", "side": "buy"}],
        },
        {
            "code": "0",
            "data": [{"tradeId": "19", "ts": "1795687200000", "instId": "BTC-USDT", "px": "100", "sz": "0.1", "side": "buy"}],
        },
    ])
    monkeypatch.setattr("crypto_market_pipeline.backfill._fetch_json", lambda *_: next(responses))

    events = fetch_okx_trades("BTCUSDT", *window())

    assert [event.exchange_event_id for event in events] == ["19", "20"]


def test_backfill_deduplicates_overlap_before_kafka_write(monkeypatch) -> None:
    from crypto_market_pipeline.models import NormalizedEvent

    event = NormalizedEvent.create(
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_event_id="trade-1",
        event_time=datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc),
        payload={"price": "100", "quantity": "0.1"},
    )
    monkeypatch.setattr("crypto_market_pipeline.backfill.fetch_bybit_trades", lambda *_: [event, event])
    monkeypatch.setattr("crypto_market_pipeline.backfill.fetch_bybit_archive_trades", lambda *_: [event, event])
    written: list[list[NormalizedEvent]] = []

    async def write(events):
        written.append(events)

    assert asyncio.run(recover_trade_gap("bybit", "BTCUSDT", *window(), write)) == 1
    assert len(written[0]) == 1


def test_bybit_archive_rows_use_public_trade_id(monkeypatch) -> None:
    monkeypatch.setattr(
        "crypto_market_pipeline.backfill._read_archive_rows",
        lambda _: [
            {
                "id": "archive-trade-1",
                "timestamp": "1795687200000",
                "symbol": "BTCUSDT",
                "price": "100",
                "volume": "0.1",
                "side": "Buy",
            }
        ],
    )
    monkeypatch.setattr(
        "crypto_market_pipeline.backfill._bybit_archive_urls",
        lambda *_: ("https://archive/day", "https://archive/month"),
    )

    events = fetch_bybit_archive_trades("BTCUSDT", *window())

    assert len(events) == 1
    assert events[0].exchange_event_id == "archive-trade-1"
    assert events[0].payload["quantity"] == "0.1"


def test_full_archive_backfill_reports_recovered(monkeypatch) -> None:
    from crypto_market_pipeline.models import NormalizedEvent

    event = NormalizedEvent.create(
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_event_id="archive-trade-1",
        event_time=datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc),
        payload={"price": "100", "quantity": "0.1"},
    )
    monkeypatch.setattr("crypto_market_pipeline.backfill.fetch_bybit_archive_trades", lambda *_: [event])
    reports = []

    async def report(coverage):
        reports.append(coverage)

    async def write(_events):
        return None

    asyncio.run(recover_trade_gap("bybit", "BTCUSDT", *window(), write, report))

    assert [coverage.status for coverage in reports] == ["STARTED", "RECOVERED"]
    assert reports[-1].source == "bybit_public_archive"


def test_recent_fallback_is_explicitly_partial(monkeypatch) -> None:
    from crypto_market_pipeline.models import NormalizedEvent

    event = NormalizedEvent.create(
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_event_id="recent-trade-1",
        event_time=datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc),
        payload={"price": "100", "quantity": "0.1"},
    )
    monkeypatch.setattr(
        "crypto_market_pipeline.backfill.fetch_bybit_archive_trades",
        lambda *_: (_ for _ in ()).throw(FileNotFoundError("today is not published")),
    )
    monkeypatch.setattr("crypto_market_pipeline.backfill.fetch_bybit_trades", lambda *_: [event])
    reports = []

    async def report(coverage):
        reports.append(coverage)

    async def write(_events):
        return None

    asyncio.run(recover_trade_gap("bybit", "BTCUSDT", *window(), write, report))

    assert reports[-1].status == "PARTIAL"
    assert reports[-1].source == "bybit_recent_trade"
