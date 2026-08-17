-- Per-variant fact table behind every strategy analytic: the latest backtest of each
-- (symbol, strategy), single strategy or AND-combination alike, with its full-period,
-- in-sample and out-of-sample numbers side by side. This is what the site's per-asset
-- heatmap reads, and it is the single population that mart_strategy_leaderboard and
-- mart_overfitting_summary aggregate, so the three relations can never disagree.
--
-- GRAIN — one row per (symbol, strategy). gold.backtest_runs is append-only (re-runs add
-- rows), so the DISTINCT ON keeps a symbol that happened to be re-backtested from being
-- counted twice. Asset class and region come from the dim_assets catalog; the inner join
-- means an uncatalogued symbol is excluded rather than aggregated as NULL — the
-- relationships test on stg_ohlcv.symbol is what fails loudly for that case.
--
-- NULL SEMANTICS — the is_* / oos_* columns are NULL when the engine could not evaluate
-- that window (fewer than ~30 bars) and on runs written before migration 003. NULL
-- deliberately propagates into the excess returns and into beat_bh_is / beat_bh_oos:
-- "never measured out of sample" must not be readable as "failed out of sample".
-- exposure is NULL for the same reason on pre-003 runs.

with latest_runs as (

    select distinct on (symbol, strategy)
        symbol,
        strategy,
        strategy_kind,
        -- Runs written before migration 003 carry the column default '{}'. By the naming
        -- convention the strategy name IS the '+'-joined, alphabetically sorted component
        -- list, so splitting it back is a lossless fallback, not an invention.
        coalesce(nullif(components, '{}'::text[]), string_to_array(strategy, '+')) as components,
        n_components,
        has_curve,
        executed_at,
        n_bars,
        split_ts,
        exposure,
        total_return,
        buy_hold_return,
        sharpe,
        max_drawdown,
        n_trades,
        is_total_return,
        is_buy_hold_return,
        is_sharpe,
        is_max_drawdown,
        is_n_trades,
        is_exposure,
        oos_total_return,
        oos_buy_hold_return,
        oos_sharpe,
        oos_max_drawdown,
        oos_n_trades,
        oos_exposure
    from {{ source('gold_engine', 'backtest_runs') }}
    order by symbol, strategy, executed_at desc

),

joined as (

    select
        r.symbol,
        a.asset_class,
        a.region,
        r.strategy,
        r.strategy_kind,
        r.n_components,
        r.components,
        r.has_curve,
        r.executed_at,
        r.n_bars,
        r.split_ts,
        r.exposure,

        -- Full period.
        r.total_return,
        r.buy_hold_return,
        r.total_return - r.buy_hold_return as excess_return,
        r.sharpe,
        r.max_drawdown,
        r.n_trades,

        -- In sample (first train_fraction of the bars).
        r.is_total_return,
        r.is_buy_hold_return,
        r.is_total_return - r.is_buy_hold_return as is_excess_return,
        r.is_sharpe,
        r.is_max_drawdown,
        r.is_n_trades,

        -- Out of sample (the held-out remainder, never used to pick anything).
        r.oos_total_return,
        r.oos_buy_hold_return,
        r.oos_total_return - r.oos_buy_hold_return as oos_excess_return,
        r.oos_sharpe,
        r.oos_max_drawdown,
        r.oos_n_trades,

        -- Share of each window's bars actually held. An AND-combination that never
        -- opens a position is the signature failure mode of over-filtering.
        r.is_exposure,
        r.oos_exposure
    from latest_runs r
    inner join {{ ref('dim_assets') }} a on r.symbol = a.symbol

)

select
    *,
    -- How much of the in-sample edge evaporated out of sample. Positive = decayed.
    is_excess_return - oos_excess_return as oos_excess_drop,
    -- "Beating" buy & hold requires having been in the market at all. A variant
    -- that never trades returns exactly 0% and would otherwise be credited with a
    -- win every time buy & hold fell — flattering precisely the variants that are
    -- least real.
    total_return > buy_hold_return and coalesce(n_trades, 0) > 0 as beat_bh_full,
    is_total_return > is_buy_hold_return
        and coalesce(is_n_trades, 0) > 0 as beat_bh_is,
    oos_total_return > oos_buy_hold_return
        and coalesce(oos_n_trades, 0) > 0 as beat_bh_oos
from joined
order by symbol, n_components, strategy
