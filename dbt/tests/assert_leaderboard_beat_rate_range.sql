-- beat_rate is a share of backtests, so it must live in [0, 1]. A value outside that
-- range means the numerator/denominator pair in mart_strategy_leaderboard drifted apart
-- (e.g. the FILTER clause counting a different population than count(*)).
-- Written as a singular test because accepted_range only exists in dbt_utils, which this
-- project deliberately does not install.
select
    strategy,
    asset_class,
    region,
    n_backtests,
    n_beat_buy_hold,
    beat_rate
from {{ ref('mart_strategy_leaderboard') }}
where beat_rate is not null
    and (beat_rate < 0 or beat_rate > 1)
