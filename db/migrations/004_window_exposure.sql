-- 004_window_exposure.sql
--
-- Two integrity gaps found by adversarial review of the v3 schema:
--
-- 1. Per-window exposure was computed by the engine and then dropped on the way
--    to the database. Without it, a window in which a variant never opened a
--    position is indistinguishable from one where it traded and broke even —
--    and it silently counts as "beat buy & hold" whenever buy & hold was
--    negative. Over-filtered AND-combinations are exactly the variants that sit
--    at zero exposure, so this is the column that keeps the headline
--    out-of-sample number honest.
--
-- 2. The free-tier invariant "only single-strategy runs store curves" lived
--    solely in Python. A CHECK makes the database enforce it, and a second one
--    stops `components` and `n_components` from drifting apart.

ALTER TABLE gold.backtest_runs
    ADD COLUMN IF NOT EXISTS is_exposure  NUMERIC,
    ADD COLUMN IF NOT EXISTS oos_exposure NUMERIC;

-- Idempotent: ADD CONSTRAINT has no IF NOT EXISTS, so drop first.
ALTER TABLE gold.backtest_runs
    DROP CONSTRAINT IF EXISTS backtest_runs_curve_only_for_singles;
ALTER TABLE gold.backtest_runs
    ADD CONSTRAINT backtest_runs_curve_only_for_singles
    CHECK (strategy_kind = 'single' OR NOT has_curve);

-- Runs written before migration 003 defaulted to an empty components array with
-- n_components = 1, which the CHECK below would reject. They are all single
-- strategies, so their component list is their own name.
UPDATE gold.backtest_runs
SET components = ARRAY[strategy], n_components = 1
WHERE cardinality(components) = 0;

ALTER TABLE gold.backtest_runs
    DROP CONSTRAINT IF EXISTS backtest_runs_components_match_count;
ALTER TABLE gold.backtest_runs
    ADD CONSTRAINT backtest_runs_components_match_count
    CHECK (cardinality(components) = n_components);

-- Never scanned by any query in the repo (idx_scan = 0 against 1.1M PK scans):
-- the marts filter by symbol/strategy, not by kind.
DROP INDEX IF EXISTS gold.idx_backtest_runs_kind;
