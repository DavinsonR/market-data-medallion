-- 002_enable_rls.sql — defense in depth for managed Postgres (Supabase).
--
-- These schemas are not exposed through Supabase's REST API and the pipeline
-- connects as the table owner (which bypasses RLS), so this changes nothing
-- operationally. It exists so that a future accidental exposure of the schema
-- does not turn into an open data endpoint, and to keep Supabase's security
-- advisor clean.
--
-- Harmless on a plain local Postgres: ENABLE ROW LEVEL SECURITY without any
-- policy still allows the owner full access.

ALTER TABLE meta.ingest_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze.raw_candles ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.backtest_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE gold.backtest_equity_curves ENABLE ROW LEVEL SECURITY;
