from crypto_market_pipeline.manifest import build_manifest, manifest_key


def rows() -> list[dict]:
    return [
        {
            "event_id": "b",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "event_type": "trade",
            "event_time": "2026-11-26T10:00:02.000Z",
            "ingest_time": "2026-11-26T10:00:02.100Z",
        },
        {
            "event_id": "a",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "event_type": "trade",
            "event_time": "2026-11-26T10:00:01.000Z",
            "ingest_time": "2026-11-26T10:00:01.100Z",
        },
    ]


def test_manifest_contains_batch_stats_and_stable_digest() -> None:
    first = build_manifest("date=2026-11-26/hour=10/part-abcd.parquet", rows())
    second = build_manifest("date=2026-11-26/hour=10/part-abcd.parquet", list(reversed(rows())))

    assert first == second
    assert first["row_count"] == 2
    assert first["unique_event_ids"] == 2
    assert first["min_event_time"] == "2026-11-26T10:00:01.000Z"
    assert first["max_event_time"] == "2026-11-26T10:00:02.000Z"
    assert first["groups"][0]["event_id_digest"]
    assert manifest_key(first["object_key"]).endswith(".manifest.json")
