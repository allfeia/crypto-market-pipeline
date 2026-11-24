with latest as (
    select
        max(minute) - interval 10 minute as cutoff_minute,
        max(aggregated_at) as cutoff_ingest_time
    from {{ ref('fct_candles') }}
), deduplicated as (
    select
        event_id,
        argMax(exchange, ingest_time) as exchange,
        argMax(symbol, ingest_time) as symbol,
        argMax(event_type, ingest_time) as event_type,
        argMax(event_time, ingest_time) as event_time,
        max(ingest_time) as latest_ingest_time,
        argMax(payload_json, ingest_time) as payload_json
    from {{ source('raw', 'market_events') }} as source_events
    where source_events.ingest_time <= (select cutoff_ingest_time from latest)
    group by event_id
), raw_window as (
    select
        event_id,
        exchange,
        symbol,
        event_type,
        event_time,
        latest_ingest_time as ingest_time,
        payload_json
    from deduplicated
    where event_time >= (select cutoff_minute - interval 9 minute from latest)
      and event_time < (select cutoff_minute + interval 1 minute from latest)
), trades as (
    select
        exchange,
        symbol,
        event_time,
        JSONExtractFloat(payload_json, 'price') as price,
        JSONExtractFloat(payload_json, 'quantity') as quantity
    from raw_window
    where event_type = 'trade'
      and exchange in ('bybit', 'okx')
      and price > 0
      and quantity > 0
), expected as (
    select
        exchange,
        symbol,
        toStartOfMinute(event_time) as minute,
        argMin(price, event_time) as open,
        max(price) as high,
        min(price) as low,
        argMax(price, event_time) as close,
        sum(quantity) as volume,
        count() as trade_count
    from trades
    group by exchange, symbol, minute
), actual as (
    select
        exchange,
        symbol,
        minute,
        open,
        high,
        low,
        close,
        volume,
        trade_count
    from {{ ref('fct_candles') }}
    where minute >= (select cutoff_minute - interval 9 minute from latest)
      and minute <= (select cutoff_minute from latest)
)

select
    expected.exchange,
    expected.symbol,
    expected.minute,
    expected.trade_count as expected_trade_count,
    actual.trade_count as actual_trade_count
from expected
left join actual
    on actual.exchange = expected.exchange
   and actual.symbol = expected.symbol
   and actual.minute = expected.minute
where actual.minute is null
   or actual.trade_count != expected.trade_count
   or abs(actual.open - expected.open) > 0.000000001
   or abs(actual.high - expected.high) > 0.000000001
   or abs(actual.low - expected.low) > 0.000000001
   or abs(actual.close - expected.close) > 0.000000001
   or abs(actual.volume - expected.volume) > 0.000000001
