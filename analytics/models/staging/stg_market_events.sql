{{ config(materialized='view') }}

with deduplicated as (
    select
        event_id,
        argMax(exchange, ingest_time) as exchange,
        argMax(market_type, ingest_time) as market_type,
        argMax(symbol, ingest_time) as symbol,
        argMax(event_type, ingest_time) as event_type,
        argMax(exchange_event_id, ingest_time) as exchange_event_id,
        argMax(event_time, ingest_time) as event_time,
        max(ingest_time) as latest_ingest_time,
        argMax(payload_json, ingest_time) as payload_json
    from {{ source('raw', 'market_events') }}
    group by event_id
)

select
    event_id,
    exchange,
    market_type,
    symbol,
    event_type,
    exchange_event_id,
    event_time,
    latest_ingest_time as ingest_time,
    JSONExtractFloat(payload_json, 'price') as price,
    JSONExtractFloat(payload_json, 'quantity') as quantity,
    payload_json
from deduplicated
