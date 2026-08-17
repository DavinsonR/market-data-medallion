-- Canonical candle series: exactly one row per (symbol, ts).
-- When several sources cover the same bar, keep the highest-priority one:
-- coinbase > kraken > tiingo > tiingo_fx. (tiingo_fx never actually competes —
-- FX pairs have no second feed — but the rank is spelled out so a future overlap
-- resolves deterministically instead of falling into the alphabetical fallback.)

with ranked as (

    select
        *,
        row_number() over (
            partition by symbol, granularity, ts
            order by
                case source
                    when 'coinbase'  then 1
                    when 'kraken'    then 2
                    when 'tiingo'    then 3
                    when 'tiingo_fx' then 4
                    else 5
                end,
                source
        ) as source_rank
    from {{ ref('stg_ohlcv_all_sources') }}

)

select
    source,
    symbol,
    granularity,
    ts,
    open,
    high,
    low,
    close,
    volume,
    adj_close,
    is_zero_volume,
    has_null_price,
    high_lt_low
from ranked
where source_rank = 1
