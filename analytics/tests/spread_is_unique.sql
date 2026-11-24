select
    symbol,
    minute,
    count(*) as row_count
from {{ ref('fct_exchange_spread') }}
group by symbol, minute
having count(*) > 1
