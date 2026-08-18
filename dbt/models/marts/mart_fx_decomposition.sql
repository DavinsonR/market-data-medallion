-- FX decomposition for the Latin American ADRs: how much of each ADR's USD return
-- was the company and how much was the currency. One row per (adr_symbol, window),
-- window in ('30d', '90d', '365d', 'full') — 10 ADRs x 4 windows = 40 rows.
--
-- Sign conventions (fixed — see BITACORA 9.8):
--   * Tiingo quotes USDCOP as COP per 1 USD. Rising USDCOP = peso DEPRECIATES.
--   * Synthetic local price: price_local = price_usd * fx_rate, so over any window
--       (1 + r_local) = (1 + r_usd) * (1 + r_fx)
--     where r_fx is the return of the USDXXX rate itself.
--   * fx_drag_pp = r_usd - r_local: what the currency did TO the USD investor.
--     Peso depreciates -> r_fx > 0 -> r_local > r_usd -> fx_drag NEGATIVE (the
--     currency subtracted from the USD return). Worked example: local +50%,
--     USDCOP +25% -> wait, careful: r_local = (1+r_usd)(1+r_fx)-1 means r_usd =
--     (1+r_local)/(1+r_fx)-1 = 1.5/1.25-1 = +20%. fx_drag = 20% - 50% = -30 pp.
--   * Returns are stored as decimal fractions (-0.30 above), like every other
--     return in this project; multiply by 100 to quote in percentage points.
--   * EURUSD is quoted the OTHER direction (USD per EUR), maps to no ADR, and
--     never enters this mart: only the fx_pair values of dim_assets are read.
--
-- Anchor mechanics: FX trades ~7 days a week while NYSE closes on weekends and
-- holidays, so the two series rarely share exact dates. Each series therefore
-- uses its OWN anchors: its latest close as the window end, and its last close
-- at or before (its own latest_ts - interval) as the window start. The 'full'
-- window starts at each series' first available close. All four anchor
-- timestamps are emitted so every number is auditable.
--
-- Metrics are NULL (never fabricated) when either series lacks the window —
-- e.g. history shorter than the lookback, or a pair not yet ingested.

with adrs as (

    select
        symbol,
        region,
        fx_pair
    from {{ ref('dim_assets') }}
    where fx_pair is not null

),

windows as (

    select window_label, lookback
    from (
        values
            ('30d', interval '30 days'),
            ('90d', interval '90 days'),
            ('365d', interval '365 days'),
            ('full', null::interval)
    ) as w (window_label, lookback)

),

-- Canonical closes of the ADRs and of the FX pairs they map to (nothing else).
closes as (

    select
        symbol,
        ts,
        close
    from {{ ref('stg_ohlcv') }}
    where symbol in (select symbol from adrs)
        or symbol in (select fx_pair from adrs)

),

latest as (

    select distinct on (symbol)
        symbol,
        ts as end_ts,
        close as end_close
    from closes
    order by symbol, ts desc

),

-- Lookback windows: each series' last close at or before (its own end - interval).
lookback_starts as (

    select distinct on (c.symbol, w.window_label)
        c.symbol,
        w.window_label,
        c.ts as start_ts,
        c.close as start_close
    from closes c
    inner join latest l on c.symbol = l.symbol
    cross join windows w
    where w.lookback is not null
        and c.ts <= l.end_ts - w.lookback
    order by c.symbol, w.window_label, c.ts desc

),

-- The 'full' window starts at each series' first available close.
full_starts as (

    select distinct on (symbol)
        symbol,
        'full' as window_label,
        ts as start_ts,
        close as start_close
    from closes
    order by symbol, ts asc

),

starts as (

    select * from lookback_starts
    union all
    select * from full_starts

),

returns as (

    select
        a.symbol,
        a.region,
        a.fx_pair,
        w.window_label,
        case
            when adr_s.start_close is not null and adr_s.start_close <> 0
                then adr_l.end_close / adr_s.start_close - 1
        end as usd_return,
        case
            when fx_s.start_close is not null and fx_s.start_close <> 0
                then fx_l.end_close / fx_s.start_close - 1
        end as fx_return,
        adr_s.start_ts as adr_start_ts,
        adr_l.end_ts as adr_end_ts,
        fx_s.start_ts as fx_start_ts,
        fx_l.end_ts as fx_end_ts
    from adrs a
    cross join windows w
    left join latest adr_l on adr_l.symbol = a.symbol
    left join starts adr_s
        on adr_s.symbol = a.symbol and adr_s.window_label = w.window_label
    left join latest fx_l on fx_l.symbol = a.fx_pair
    left join starts fx_s
        on fx_s.symbol = a.fx_pair and fx_s.window_label = w.window_label

)

select
    symbol,
    region,
    fx_pair,
    window_label,
    usd_return,
    fx_return,
    -- NULL propagates: either leg missing means no decomposition, not a zero.
    (1 + usd_return) * (1 + fx_return) - 1 as local_return,
    usd_return - ((1 + usd_return) * (1 + fx_return) - 1) as fx_drag_pp,
    adr_start_ts,
    adr_end_ts,
    fx_start_ts,
    fx_end_ts
from returns
