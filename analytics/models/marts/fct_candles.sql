{{ config(
    materialized='table',
    engine='ReplacingMergeTree(aggregated_at)',
    order_by=['exchange', 'symbol', 'minute'],
    partition_by='toYYYYMM(minute)'
) }}

select
    exchange,
    symbol,
    toStartOfMinute(event_time) as minute,
    argMin(price, event_time) as open,
    max(price) as high,
    min(price) as low,
    argMax(price, event_time) as close,
    sum(quantity) as volume,
    count() as trade_count,
    max(ingest_time) as aggregated_at
from {{ ref('stg_market_events') }}
where event_type = 'trade'
  and price > 0
  and quantity > 0
group by exchange, symbol, minute
