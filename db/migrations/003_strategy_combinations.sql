-- 003_strategy_combinations.sql
--
-- Backtests grew from 177 single-strategy runs to ~1,347 runs covering every
-- non-empty AND-combination of the five strategies. Two consequences:
--
-- 1. Runs must describe their own shape (single vs combination, which components)
--    so the analytics layer can compare like with like.
-- 2. Testing that many variants invites data dredging, so every run also carries
--    out-of-sample metrics: the series is split into a training and a validation
--    window, and a combination is only credible if it wins in both.
--
-- Equity curves stay optional: storing one per run would exceed the free tier,
-- so only single-strategy runs keep their curve (see gold.backtest_runs.has_curve).

ALTER TABLE gold.backtest_runs
    ADD COLUMN IF NOT EXISTS strategy_kind  TEXT NOT NULL DEFAULT 'single'
        CHECK (strategy_kind IN ('single', 'combo')),
    ADD COLUMN IF NOT EXISTS components     TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS n_components   INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS has_curve      BOOLEAN NOT NULL DEFAULT true,
    -- Fraction of bars the strategy was actually invested; near-zero exposure is
    -- how over-filtered combinations reveal themselves.
    ADD COLUMN IF NOT EXISTS exposure       NUMERIC,
    -- In-sample window (first train_fraction of the bars).
    ADD COLUMN IF NOT EXISTS is_total_return    NUMERIC,
    ADD COLUMN IF NOT EXISTS is_buy_hold_return NUMERIC,
    ADD COLUMN IF NOT EXISTS is_sharpe          NUMERIC,
    ADD COLUMN IF NOT EXISTS is_max_drawdown    NUMERIC,
    ADD COLUMN IF NOT EXISTS is_n_trades        INTEGER,
    -- Out-of-sample window (the remainder), never used to pick anything.
    ADD COLUMN IF NOT EXISTS oos_total_return    NUMERIC,
    ADD COLUMN IF NOT EXISTS oos_buy_hold_return NUMERIC,
    ADD COLUMN IF NOT EXISTS oos_sharpe          NUMERIC,
    ADD COLUMN IF NOT EXISTS oos_max_drawdown    NUMERIC,
    ADD COLUMN IF NOT EXISTS oos_n_trades        INTEGER,
    ADD COLUMN IF NOT EXISTS split_ts            TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol_strategy
    ON gold.backtest_runs (symbol, strategy, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_kind
    ON gold.backtest_runs (strategy_kind, n_components);
