from datetime import datetime, timezone

from crypto_market_pipeline.clickhouse_writer import event_to_row
from crypto_market_pipeline.models import NormalizedEvent


def test_clickhouse_row_preserves_event_contract() -> None:
    event = NormalizedEvent.create(
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_event_id="trade-1",
        event_time=datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc),
        payload={"price": "100.5", "quantity": "0.1"},
    )

    row = event_to_row(event)

    assert row[0] == event.event_id
    assert row[1:6] == ("bybit", "spot", "BTCUSDT", "trade", "trade-1")
    assert row[6].tzinfo == timezone.utc
    assert row[7].tzinfo == timezone.utc
    assert row[8] == '{"price":"100.5","quantity":"0.1"}'
