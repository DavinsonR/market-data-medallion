{% test ohlc_consistency(model) %}

-- A candle is inconsistent when high < low or the close falls outside [low, high].
-- Rows with NULL prices are handled by the has_null_price flag, not by this test.
select *
from {{ model }}
where high < low
    or close < low
    or close > high

{% endtest %}
