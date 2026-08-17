"""Prefect 3 daily flow: ingest -> transform (dbt) -> validate -> backtest -> export.

Run locally or in CI with: python -m pipeline.flows
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import psycopg
from prefect import flow, get_run_logger, task

from pipeline import bronze, export
from pipeline.backtest.engine import run_backtest
from pipeline.backtest.strategies import build_strategy
from pipeline.config import (
    REPO_ROOT,
    AppConfig,
    AssetConfig,
    database_url,
    dbt_env,
    load_config,
    tiingo_api_key,
)
from pipeline.models import IngestResult
from pipeline.quality import validate_ohlcv


def _logger() -> logging.Logger:
    """Prefect run logger inside a flow/task, stdlib logger otherwise (module runs)."""
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger("pipeline")


INDICATORS_QUERY = """
SELECT symbol, ts,
       open::float8, high::float8, low::float8, close::float8, volume::float8,
       sma_20::float8, sma_50::float8, sma_200::float8,
       rsi_14::float8, vol_sma_20::float8,
       bb_upper_20::float8, bb_lower_20::float8, daily_return::float8
FROM gold.fct_ohlcv_indicators
WHERE symbol = %(symbol)s
ORDER BY ts
"""


@task(retries=2, retry_delay_seconds=30)
def ingest_asset(source_name: str, asset: AssetConfig, granularity: str) -> IngestResult:
    with psycopg.connect(database_url()) as conn:
        result = bronze.ingest(conn, source_name, asset, granularity)
    if result.status == "failed":
        raise RuntimeError(f"ingestion failed for {source_name}/{asset.symbol}: {result.error}")
    return result


@task
def run_dbt() -> None:
    logger = _logger()
    dbt_bin = Path(sys.executable).parent / "dbt"
    proc = subprocess.run(
        [str(dbt_bin), "build", "--profiles-dir", ".", "--no-use-colors"],
        cwd=REPO_ROOT / "dbt",
        env={**os.environ, **dbt_env()},
        capture_output=True,
        text=True,
    )
    logger.info(proc.stdout[-4000:])
    if proc.returncode != 0:
        logger.error(proc.stderr[-2000:])
        raise RuntimeError("dbt build failed")


@task
def backtest_symbol(asset: AssetConfig, cfg: AppConfig) -> int:
    logger = _logger()
    with psycopg.connect(database_url()) as conn:
        df = pd.read_sql_query(INDICATORS_QUERY, conn, params={"symbol": asset.symbol})
    if df.empty:
        logger.warning("no gold data for %s, skipping backtests", asset.symbol)
        return 0
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    validate_ohlcv(df)

    bt = cfg.backtest
    n = 0
    with psycopg.connect(database_url()) as conn:
        for strat_cfg in bt.strategies:
            strategy = build_strategy(strat_cfg)
            result = run_backtest(
                df,
                strategy,
                initial_cash=bt.initial_cash,
                fee_bps=bt.fee_bps,
                slippage_bps=bt.slippage_bps,
                periods_per_year=asset.periods_per_year,
            )
            export.write_backtest_result(
                conn,
                symbol=asset.symbol,
                strategy=strat_cfg.name,
                params=strat_cfg.params,
                fee_bps=bt.fee_bps,
                slippage_bps=bt.slippage_bps,
                result=result,
            )
            logger.info(
                "%s / %s: return=%.2f%% (buy&hold %.2f%%), trades=%s",
                asset.symbol,
                strat_cfg.name,
                100 * result.metrics["total_return"],
                100 * result.metrics["buy_hold_return"],
                result.metrics["n_trades"],
            )
            n += 1
    return n


@flow(name="daily-medallion-flow", log_prints=True)
def daily_flow() -> None:
    logger = _logger()
    cfg = load_config()

    for asset in cfg.assets:
        sources = [asset.sources.primary]
        if asset.sources.reconcile:
            sources.append(asset.sources.reconcile)
        for source_name in sources:
            if source_name == "tiingo" and not tiingo_api_key():
                logger.warning("TIINGO_API_KEY not set — skipping %s for %s",
                               source_name, asset.symbol)
                continue
            ingest_asset(source_name, asset, cfg.granularity)

    run_dbt()

    for asset in cfg.assets:
        backtest_symbol(asset, cfg)

    path = export.export_json(cfg)
    logger.info("export written: %s", path)


if __name__ == "__main__":
    daily_flow()
