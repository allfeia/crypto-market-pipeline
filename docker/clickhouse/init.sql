CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS raw.market_events
(
    event_id UUID,
    exchange LowCardinality(String),
    market_type LowCardinality(String),
    symbol LowCardinality(String),
    event_type LowCardinality(String),
    exchange_event_id String,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    payload_json String
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (exchange, symbol, event_type, event_time, event_id)
TTL event_time + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS ops.raw_reconciliation
(
    checked_at DateTime64(3, 'UTC'),
    object_key String,
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    event_type LowCardinality(String),
    expected_rows UInt64,
    actual_rows UInt64,
    expected_unique_event_ids UInt64,
    actual_unique_event_ids UInt64,
    expected_event_id_digest String,
    actual_event_id_digest String,
    status LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(checked_at)
ORDER BY (checked_at, exchange, symbol, event_type, object_key);
