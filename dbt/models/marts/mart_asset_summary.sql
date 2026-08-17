-- One-row-per-symbol summary for dashboards and the exported JSON.
-- return_30d compares the latest close with the last close at or before 30
-- calendar days earlier; volatility_30d annualizes the standard deviation of
-- daily returns over the trailing 30 calendar days.
-- The annualization factor comes from the asset class in dim_assets — sqrt(365)
-- for crypto (24/7 markets), sqrt(252) for equities and FX — not from the old
-- `symbol like '%-USD'` heuristic, which read the FX pair USDCOP as crypto.
-- Same INNER-join rationale as mart_data_quality: an uncatalogued symbol is a
-- config bug caught by the relationships test, not a row with a NULL class.

with indicators as (

    select
        symbol,
        ts,
        close,
        daily_return
    from {{ ref('fct_ohlcv_indicators') }}

),

latest as (

    select distinct on (symbol)
        symbol,
        ts as last_candle_ts,
        close as latest_close
    from indicators
    order by symbol, ts desc

),

prior_30d as (

    select distinct on (i.symbol)
        i.symbol,
        i.close as close_30d_ago
    from indicators i
    inner join latest l on i.symbol = l.symbol
    where i.ts <= l.last_candle_ts - interval '30 days'
    order by i.symbol, i.ts desc

),

vol_30d as (

    select
        i.symbol,
        stddev_samp(i.daily_return) as daily_return_stddev_30d
    from indicators i
    inner join latest l on i.symbol = l.symbol
    where i.ts > l.last_candle_ts - interval '30 days'
    group by i.symbol

)

select
    l.symbol,
    a.asset_class,
    a.region,
    l.latest_close,
    case
        when p.close_30d_ago is not null and p.close_30d_ago <> 0
            then l.latest_close / p.close_30d_ago - 1
    end as return_30d,
    v.daily_return_stddev_30d
        * sqrt(case when a.asset_class = 'crypto' then 365 else 252 end) as volatility_30d,
    l.last_candle_ts,
    now() - l.last_candle_ts > interval '3 days' as is_stale
from latest l
inner join {{ ref('dim_assets') }} a on l.symbol = a.symbol
left join prior_30d p on l.symbol = p.symbol
left join vol_30d v on l.symbol = v.symbol
