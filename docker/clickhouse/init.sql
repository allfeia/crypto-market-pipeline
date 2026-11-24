CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;

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
