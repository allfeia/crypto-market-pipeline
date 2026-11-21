from datetime import datetime, timezone

from crypto_market_pipeline.models import NormalizedEvent


def make_event(exchange_event_id: str) -> NormalizedEvent:
    return NormalizedEvent.create(
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        event_type="trade",
        exchange_event_id=exchange_event_id,
        event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        payload={"price": "100"},
    )


def test_event_id_is_deterministic() -> None:
    assert make_event("trade-1").event_id == make_event("trade-1").event_id


def test_event_id_changes_for_another_exchange_event() -> None:
    assert make_event("trade-1").event_id != make_event("trade-2").event_id
