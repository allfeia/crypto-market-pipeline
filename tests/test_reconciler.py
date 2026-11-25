from crypto_market_pipeline.reconciler import aggregate_groups, classify_result, digest_event_ids


def test_digest_is_order_independent() -> None:
    assert digest_event_ids(["b", "a"]) == digest_event_ids(["a", "b"])


def test_reconciliation_statuses() -> None:
    common = {
        "expected_rows": 2,
        "actual_rows": 2,
        "expected_unique_event_ids": 2,
        "actual_unique_event_ids": 2,
        "expected_digest": "digest",
        "actual_digest": "digest",
    }
    assert classify_result(**common) == "MATCH"
    assert classify_result(**{**common, "actual_unique_event_ids": 1}) == "MISSING_IN_CLICKHOUSE"
    assert classify_result(**{**common, "actual_unique_event_ids": 3}) == "EXTRA_IN_CLICKHOUSE"
    assert classify_result(**{**common, "actual_rows": 3}) == "DUPLICATES_IN_CLICKHOUSE"
    assert classify_result(**{**common, "actual_digest": "other"}) == "METADATA_MISMATCH"


def test_manifest_groups_are_aggregated_by_hour_and_source() -> None:
    manifests = [
        {
            "object_key": "date=2026-11-26/hour=10/part-a.parquet",
            "groups": [{
                "exchange": "bybit",
                "symbol": "BTCUSDT",
                "event_type": "trade",
                "row_count": 2,
                "unique_event_ids": 2,
                "event_id_xor": "01",
            }],
        },
        {
            "object_key": "date=2026-11-26/hour=10/part-b.parquet",
            "groups": [{
                "exchange": "bybit",
                "symbol": "BTCUSDT",
                "event_type": "trade",
                "row_count": 3,
                "unique_event_ids": 3,
                "event_id_xor": "10",
            }],
        },
    ]

    result = aggregate_groups(manifests)

    assert len(result) == 1
    assert result[0]["row_count"] == 5
    assert result[0]["unique_event_ids"] == 5
    assert result[0]["event_id_digest"].endswith("11")
