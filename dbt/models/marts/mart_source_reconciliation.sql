-- Cross-exchange sanity check: Coinbase vs Kraken closes for the bars both cover.
-- abs_pct_diff is expressed in percent; a bar is discrepant above 0.5%.

with coinbase as (

    select symbol, ts, close
    from {{ ref('stg_ohlcv_all_sources') }}
    where source = 'coinbase'

),

kraken as (

    select symbol, ts, close
    from {{ ref('stg_ohlcv_all_sources') }}
    where source = 'kraken'

),

joined as (

    select
        c.symbol,
        c.ts,
        c.close as coinbase_close,
        k.close as kraken_close,
        abs(c.close - k.close) / nullif(c.close, 0) * 100 as abs_pct_diff
    from coinbase c
    inner join kraken k
        on c.symbol = k.symbol
        and c.ts = k.ts

)

select
    symbol,
    ts,
    coinbase_close,
    kraken_close,
    abs_pct_diff,
    coalesce(abs_pct_diff > 0.5, false) as is_discrepant
from joined
