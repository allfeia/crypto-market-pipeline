select
    exchange,
    symbol,
    minute,
    count(*) as row_count
from {{ ref('fct_candles') }}
group by exchange, symbol, minute
having count(*) > 1
