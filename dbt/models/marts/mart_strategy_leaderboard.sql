-- Headline analytical mart: does any strategy actually beat buy-and-hold, and where?
--
-- GRAIN — two grouping sets in one relation, distinguished by is_grand_total:
--   is_grand_total = false -> one row per (strategy, asset_class, region)
--   is_grand_total = true  -> one row per strategy across every class and region,
--                             emitted with asset_class = 'ALL' and region = 'ALL'
-- Both levels are computed from the SAME population, so the totals are exact
-- aggregates of the detail rows and never drift from them.
-- strategy_kind and n_components are grouped on as well, but they are functionally
-- determined by the strategy name (a combination is its sorted components joined by '+'),
-- so the grain is unchanged — tests/assert_combination_naming_convention.sql is what
-- guarantees that dependency instead of leaving it to convention.
--
-- POPULATION — mart_combination_analysis: the latest backtest run per (symbol, strategy),
-- singles and AND-combinations alike, already joined to the dim_assets catalog. Reading it
-- rather than re-deriving the population is what keeps this mart, the per-variant mart and
-- mart_overfitting_summary in agreement. Empty (zero rows) before the first backtest.
--
-- HONESTY COLUMNS — n_beat_is / n_beat_is_and_oos / oos_survival_rate answer the only
-- question that matters once ~1,347 variants have been searched: of the variants that beat
-- buy and hold on the data used to pick them, how many kept beating it on the held-out
-- window? The rate is NULL, never zero, when nothing beat buy and hold in sample.

with variants as (

    select * from {{ ref('mart_combination_analysis') }}

),

aggregated as (

    select
        strategy,
        strategy_kind,
        n_components,
        grouping(asset_class) = 1 as is_grand_total,
        case when grouping(asset_class) = 1 then 'ALL' else asset_class end as asset_class,
        case when grouping(region) = 1 then 'ALL' else region end as region,
        count(*)::int as n_backtests,
        count(*) filter (where beat_bh_full)::int as n_beat_buy_hold,
        count(*) filter (where beat_bh_full)::numeric
            / nullif(count(*), 0) as beat_rate,
        avg(total_return) as avg_total_return,
        avg(buy_hold_return) as avg_buy_hold_return,
        avg(excess_return) as avg_excess_return,
        percentile_cont(0.5) within group (order by sharpe::double precision) as median_sharpe,
        avg(max_drawdown) as avg_max_drawdown,
        avg(n_trades) as avg_n_trades,
        avg(exposure) as avg_exposure,
        avg(is_excess_return) as avg_is_excess_return,
        avg(oos_excess_return) as avg_oos_excess_return,
        count(*) filter (where beat_bh_is)::int as n_beat_is,
        -- Denominator of the survival rate: in-sample winners that were actually scored out
        -- of sample. A window the engine refused to score is missing evidence, not a death.
        count(*) filter (where beat_bh_is and oos_excess_return is not null)::int
            as n_beat_is_scored_oos,
        count(*) filter (where beat_bh_is and beat_bh_oos)::int as n_beat_is_and_oos,
        count(*) filter (where beat_bh_is and beat_bh_oos)::numeric
            / nullif(count(*) filter (where beat_bh_is and oos_excess_return is not null), 0)
            as oos_survival_rate
    from variants
    group by grouping sets ((strategy, strategy_kind, n_components, asset_class, region),
                            (strategy, strategy_kind, n_components))

)

select
    strategy,
    strategy_kind,
    n_components,
    asset_class,
    region,
    is_grand_total,
    n_backtests,
    n_beat_buy_hold,
    beat_rate,
    avg_total_return,
    avg_buy_hold_return,
    avg_excess_return,
    median_sharpe,
    avg_max_drawdown,
    avg_n_trades,
    avg_exposure,
    avg_is_excess_return,
    avg_oos_excess_return,
    n_beat_is,
    n_beat_is_scored_oos,
    n_beat_is_and_oos,
    oos_survival_rate
from aggregated
order by strategy, is_grand_total, asset_class, region
