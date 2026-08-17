-- Per-symbol completeness and quality report over the canonical series.
-- Asset class and region come from the dim_assets catalog (generated from
-- config.yaml), never from the symbol string: the old `symbol like '%-USD'`
-- heuristic classified the FX pair USDCOP as crypto.
-- expected_days counts calendar days for crypto and weekdays for everything
-- else (equities and FX both follow a Mon-Fri calendar; holidays are
-- intentionally not modeled, so a small missing_days residual is normal).
-- max_gap_days is the largest distance in days between consecutive candles
-- (1 = contiguous; a value of 3 means 2 days are missing in between).
-- The join to the catalog is INNER on purpose: a symbol ingested without a
-- catalog entry is a configuration bug, and the `relationships` test on
-- stg_ohlcv.symbol fails loudly for it instead of it appearing here unclassified.

with candles as (

    select
        symbol,
        ts,
        (ts at time zone 'UTC')::date as candle_date,
        has_null_price,
        is_zero_volume
    from {{ ref('stg_ohlcv') }}

),

per_symbol as (

    select
        c.symbol,
        a.asset_class,
        a.region,
        min(c.ts) as first_ts,
        max(c.ts) as last_ts,
        min(c.candle_date) as first_date,
        max(c.candle_date) as last_date,
        count(*) as actual_days,
        count(*) filter (where c.has_null_price) as n_null_price,
        count(*) filter (where c.is_zero_volume) as n_zero_volume
    from candles c
    inner join {{ ref('dim_assets') }} a on c.symbol = a.symbol
    group by 1, 2, 3

),

gaps as (

    select
        symbol,
        max(day_gap) as max_gap_days
    from (
        select
            symbol,
            candle_date - lag(candle_date) over (partition by symbol order by ts) as day_gap
        from candles
    ) consecutive
    group by symbol

),

expected as (

    select
        p.symbol,
        count(*)::int as expected_days
    from per_symbol p
    cross join lateral generate_series(
        p.first_date, p.last_date, interval '1 day'
    ) as d (day)
    where p.asset_class = 'crypto' or extract(isodow from d.day) <= 5
    group by p.symbol

)

select
    p.symbol,
    p.asset_class,
    p.region,
    p.first_ts,
    p.last_ts,
    e.expected_days,
    p.actual_days,
    e.expected_days - p.actual_days as missing_days,
    coalesce(g.max_gap_days, 0) as max_gap_days,
    p.n_null_price,
    p.n_zero_volume
from per_symbol p
inner join expected e on p.symbol = e.symbol
left join gaps g on p.symbol = g.symbol
