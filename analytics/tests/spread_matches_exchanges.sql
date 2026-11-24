with latest as (
    select
        max(minute) - interval 10 minute as cutoff_minute,
        max(generated_at) as cutoff_ingest_time
    from {{ ref('fct_exchange_spread') }}
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
), exchange_prices as (
    select
        symbol,
        toStartOfMinute(event_time) as minute,
        exchange,
        argMax(JSONExtractFloat(payload_json, 'price'), event_time) as last_price
    from raw_window
    where event_type = 'trade'
      and exchange in ('bybit', 'okx')
      and JSONExtractFloat(payload_json, 'price') > 0
    group by symbol, minute, exchange
), expected as (
    select
        symbol,
        minute,
        maxIf(last_price, exchange = 'bybit') as bybit_price,
        maxIf(last_price, exchange = 'okx') as okx_price
    from exchange_prices
    group by symbol, minute
    having bybit_price > 0 and okx_price > 0
), actual as (
    select
        symbol,
        minute,
        bybit_price,
        okx_price,
        spread_abs,
        spread_bps
    from {{ ref('fct_exchange_spread') }}
    where minute >= (select cutoff_minute - interval 9 minute from latest)
      and minute <= (select cutoff_minute from latest)
)

select
    expected.symbol,
    expected.minute,
    expected.bybit_price,
    expected.okx_price
from expected
left join actual
    on actual.symbol = expected.symbol
   and actual.minute = expected.minute
where actual.minute is null
   or abs(actual.bybit_price - expected.bybit_price) > 0.000000001
   or abs(actual.okx_price - expected.okx_price) > 0.000000001
   or abs(actual.spread_abs - abs(expected.bybit_price - expected.okx_price)) > 0.000000001
   or abs(actual.spread_bps - if(expected.okx_price = 0, 0, abs(expected.bybit_price - expected.okx_price) / expected.okx_price * 10000)) > 0.000000001
