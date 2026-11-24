{{ config(
    materialized='table',
    engine='ReplacingMergeTree(generated_at)',
    order_by=['symbol', 'minute'],
    partition_by='toYYYYMM(minute)'
) }}

with prices as (
    select
        symbol,
        toStartOfMinute(event_time) as minute,
        exchange,
        argMax(price, event_time) as last_price
    from {{ ref('stg_market_events') }}
    where event_type = 'trade'
      and price > 0
      and exchange in ('bybit', 'okx')
    group by symbol, minute, exchange
),
pivoted as (
    select
        symbol,
        minute,
        maxIf(last_price, exchange = 'bybit') as bybit_price,
        maxIf(last_price, exchange = 'okx') as okx_price
    from prices
    group by symbol, minute
)

select
    symbol,
    minute,
    bybit_price,
    okx_price,
    abs(bybit_price - okx_price) as spread_abs,
    if(okx_price = 0, 0, abs(bybit_price - okx_price) / okx_price * 10000) as spread_bps,
    now64(3) as generated_at
from pivoted
where bybit_price > 0
  and okx_price > 0
