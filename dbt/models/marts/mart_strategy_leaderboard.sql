-- Headline analytical mart: does any strategy actually beat buy-and-hold, and where?
--
-- GRAIN — two grouping sets in one relation, distinguished by is_grand_total:
--   is_grand_total = false -> one row per (strategy, asset_class, region)
--   is_grand_total = true  -> one row per strategy across every class and region,
--                             emitted with asset_class = 'ALL' and region = 'ALL'
-- Both levels are computed from the SAME population, so the totals are exact
-- aggregates of the detail rows and never drift from them.
--
-- POPULATION — the LATEST backtest run per (symbol, strategy). gold.backtest_runs is
-- append-only (re-runs add rows), so without the distinct on the averages would be
-- weighted by how many times each symbol happened to be re-backtested.
-- The table is created by migration 001 and read here as a dbt source, so this model
-- builds fine (returning zero rows) before the first backtest has ever run.
--
-- Asset class and region come from the dim_assets catalog; the inner join means an
-- uncatalogued symbol is excluded rather than aggregated as NULL — the relationships
-- test on stg_ohlcv.symbol is what fails loudly for that case.

with latest_runs as (

    select distinct on (symbol, strategy)
        symbol,
        strategy,
        executed_at,
        total_return,
        buy_hold_return,
        max_drawdown,
        sharpe,
        n_trades
    from {{ source('gold_engine', 'backtest_runs') }}
    order by symbol, strategy, executed_at desc

),

classified as (

    select
        r.strategy,
        a.asset_class,
        a.region,
        r.total_return,
        r.buy_hold_return,
        r.total_return - r.buy_hold_return as excess_return,
        r.max_drawdown,
        r.sharpe,
        r.n_trades
    from latest_runs r
    inner join {{ ref('dim_assets') }} a on r.symbol = a.symbol

),

aggregated as (

    select
        strategy,
        grouping(asset_class) = 1 as is_grand_total,
        case when grouping(asset_class) = 1 then 'ALL' else asset_class end as asset_class,
        case when grouping(region) = 1 then 'ALL' else region end as region,
        count(*)::int as n_backtests,
        count(*) filter (where total_return > buy_hold_return)::int as n_beat_buy_hold,
        count(*) filter (where total_return > buy_hold_return)::numeric
            / nullif(count(*), 0) as beat_rate,
        avg(total_return) as avg_total_return,
        avg(buy_hold_return) as avg_buy_hold_return,
        avg(excess_return) as avg_excess_return,
        percentile_cont(0.5) within group (order by sharpe::double precision) as median_sharpe,
        avg(max_drawdown) as avg_max_drawdown,
        avg(n_trades) as avg_n_trades
    from classified
    group by grouping sets ((strategy, asset_class, region), (strategy))

)

select
    strategy,
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
    avg_n_trades
from aggregated
order by strategy, is_grand_total, asset_class, region
