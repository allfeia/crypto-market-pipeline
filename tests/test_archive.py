from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as parquet

from crypto_market_pipeline.archive import HourlyArchive, event_partition, rows_to_parquet
from crypto_market_pipeline.models import NormalizedEvent


def make_event(event_type: str = "trade") -> NormalizedEvent:
    return NormalizedEvent.create(
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        event_type=event_type,
        exchange_event_id="event-1",
        event_time=datetime(2026, 11, 22, 14, 30, tzinfo=timezone.utc),
        payload={"price": "67120.5"},
    )


class MemorySink:
    def __init__(self) -> None:
        self.parts: dict[str, bytes] = {}

    def write_part(self, object_key: str, rows: list[dict]) -> None:
        self.parts[object_key] = rows_to_parquet(rows)


def test_event_partition_uses_utc_hour() -> None:
    partition = event_partition(make_event())

    assert partition.prefix == "date=2026-11-22/hour=14"


def test_hourly_archive_writes_parquet() -> None:
    sink = MemorySink()
    archive = HourlyArchive(sink)
    archive.add(make_event())

    assert archive.flush() == 1

    assert len(sink.parts) == 1
    table = parquet.read_table(pa.BufferReader(next(iter(sink.parts.values()))))
    row = table.to_pylist()[0]
    assert row["event_id"]
    assert row["symbol"] == "BTCUSDT"
    assert row["payload_json"] == '{"price":"67120.5"}'


def test_hourly_archive_deduplicates_event_ids() -> None:
    sink = MemorySink()
    archive = HourlyArchive(sink)
    event = make_event()

    assert archive.add(event) is True
    assert archive.add(event) is False
    assert archive.flush() == 1

    assert len(sink.parts) == 1
    assert parquet.read_table(pa.BufferReader(next(iter(sink.parts.values())))).num_rows == 1


def test_hourly_archive_uses_stable_part_name_for_same_batch() -> None:
    first_sink = MemorySink()
    second_sink = MemorySink()
    first = HourlyArchive(first_sink)
    second = HourlyArchive(second_sink)
    event = make_event()

    first.add(event)
    second.add(event)
    first.flush()
    second.flush()

    assert set(first_sink.parts) == set(second_sink.parts)
