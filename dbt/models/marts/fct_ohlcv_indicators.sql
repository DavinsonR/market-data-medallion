-- Daily candles enriched with technical indicators, per symbol ordered by ts.
-- Every rolling metric uses an explicit ROWS frame and emits NULL until the
-- window is completely full — no partial averages leak into early rows.
-- rsi_14 is Cutler's RSI (simple moving averages of gains and losses), written
-- as 100 * avg_gain / (avg_gain + avg_loss), which equals 100 - 100 / (1 + RS)
-- and degrades to NULL when the last 14 deltas are all zero.

with base as (

    select
        symbol,
        ts,
        open,
        high,
        low,
        close,
        volume,
        row_number() over sym as rn,
        lag(close) over sym as prev_close
    from {{ ref('stg_ohlcv') }}
    window sym as (partition by symbol order by ts)

),

deltas as (

    select
        *,
        close / nullif(prev_close, 0) - 1 as daily_return,
        case when prev_close is not null then greatest(close - prev_close, 0) end as gain,
        case when prev_close is not null then greatest(prev_close - close, 0) end as loss
    from base

),

rolled as (

    select
        *,
        avg(close)         over w20  as avg_close_20,
        avg(close)         over w50  as avg_close_50,
        avg(close)         over w200 as avg_close_200,
        avg(volume)        over w20  as avg_volume_20,
        stddev_pop(close) over w20   as stddev_close_20,
        avg(gain)          over w14  as avg_gain_14,
        avg(loss)          over w14  as avg_loss_14
    from deltas
    window
        w14  as (partition by symbol order by ts rows between 13 preceding and current row),
        w20  as (partition by symbol order by ts rows between 19 preceding and current row),
        w50  as (partition by symbol order by ts rows between 49 preceding and current row),
        w200 as (partition by symbol order by ts rows between 199 preceding and current row)

)

select
    symbol,
    ts,
    open,
    high,
    low,
    close,
    volume,
    daily_return,
    case when rn >= 20  then avg_close_20  end as sma_20,
    case when rn >= 50  then avg_close_50  end as sma_50,
    case when rn >= 200 then avg_close_200 end as sma_200,
    case when rn >= 20  then avg_volume_20 end as vol_sma_20,
    -- 14 deltas need 15 rows: the first row has no previous close.
    case
        when rn >= 15
        then 100.0 * avg_gain_14 / nullif(avg_gain_14 + avg_loss_14, 0)
    end as rsi_14,
    case when rn >= 20 then avg_close_20 end as bb_mid_20,
    case when rn >= 20 then avg_close_20 + 2 * stddev_close_20 end as bb_upper_20,
    case when rn >= 20 then avg_close_20 - 2 * stddev_close_20 end as bb_lower_20
from rolled
