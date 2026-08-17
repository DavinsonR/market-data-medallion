-- Typed OHLCV per (source, symbol, ts), parsed from the bronze payload JSONB.
-- Payload shapes differ by source (all labeled dicts; kraken numbers arrive as JSON strings):
--   coinbase: {"time", "low", "high", "open", "close", "volume"}
--   kraken:   {"time", "open", "high", "low", "close", "vwap", "volume", "count"}
--   tiingo:   {"date", "open", "high", "low", "close", "volume", "adjClose", ...}
-- The bar timestamp is taken from the typed candle_ts column, not re-parsed from the payload.

with raw as (

    select
        source,
        symbol,
        granularity,
        candle_ts,
        payload
    from {{ source('bronze', 'raw_candles') }}

),

parsed as (

    select
        source,
        symbol,
        granularity,
        candle_ts as ts,
        (payload ->> 'open')::numeric   as open,
        (payload ->> 'high')::numeric   as high,
        (payload ->> 'low')::numeric    as low,
        (payload ->> 'close')::numeric  as close,
        (payload ->> 'volume')::numeric as volume,
        case
            when source = 'tiingo' then (payload ->> 'adjClose')::numeric
        end as adj_close
    from raw

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
    coalesce(volume = 0, false)                                     as is_zero_volume,
    (open is null or high is null or low is null or close is null)  as has_null_price,
    coalesce(high < low, false)                                     as high_lt_low
from parsed
