-- 001_init.sql — medallion foundation
-- Layers: bronze (raw, append-only) / silver (clean, dbt-owned) / gold (analytics, dbt + backtester)
-- meta holds operational audit tables.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

-- Every ingestion attempt is audited, success or failure.
CREATE TABLE IF NOT EXISTS meta.ingest_runs (
    ingest_run_id   UUID PRIMARY KEY,
    source          TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    granularity     TEXT NOT NULL,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    rows_fetched    INTEGER,
    rows_inserted   INTEGER,
    status          TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ
);

-- Raw candles exactly as the API returned them (payload untouched).
-- Natural key makes re-ingestion idempotent: ON CONFLICT DO NOTHING.
CREATE TABLE IF NOT EXISTS bronze.raw_candles (
    raw_candle_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source          TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    granularity     TEXT NOT NULL DEFAULT '1d',
    candle_ts       TIMESTAMPTZ NOT NULL,
    payload         JSONB NOT NULL,
    ingest_run_id   UUID NOT NULL REFERENCES meta.ingest_runs (ingest_run_id),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, symbol, granularity, candle_ts)
);

CREATE INDEX IF NOT EXISTS idx_raw_candles_symbol_ts
    ON bronze.raw_candles (symbol, candle_ts);

-- Backtest results are written by the Python engine (dbt owns the other gold relations).
CREATE TABLE IF NOT EXISTS gold.backtest_runs (
    backtest_run_id UUID PRIMARY KEY,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    symbol          TEXT NOT NULL,
    strategy        TEXT NOT NULL,
    params          JSONB NOT NULL,
    fee_bps         NUMERIC NOT NULL,
    slippage_bps    NUMERIC NOT NULL,
    start_ts        TIMESTAMPTZ NOT NULL,
    end_ts          TIMESTAMPTZ NOT NULL,
    n_bars          INTEGER NOT NULL,
    total_return    NUMERIC,
    cagr            NUMERIC,
    buy_hold_return NUMERIC,
    max_drawdown    NUMERIC,
    sharpe          NUMERIC,
    n_trades        INTEGER,
    win_rate        NUMERIC
);

CREATE TABLE IF NOT EXISTS gold.backtest_equity_curves (
    backtest_run_id UUID NOT NULL REFERENCES gold.backtest_runs (backtest_run_id) ON DELETE CASCADE,
    ts              TIMESTAMPTZ NOT NULL,
    equity          NUMERIC NOT NULL,
    buy_hold_equity NUMERIC NOT NULL,
    PRIMARY KEY (backtest_run_id, ts)
);
