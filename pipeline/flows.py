"""Prefect 3 daily flow: ingest -> transform (dbt) -> validate -> backtest -> export.

Run locally or in CI with: python -m pipeline.flows

Fault tolerance: with dozens of symbols behind rate-limited free APIs, a single
failing (source, symbol) pair must never abort the run. Failures are recorded in
meta.ingest_runs, reported at the end, and self-heal on the next run through the
bronze watermark. The flow only fails outright if nothing at all could be ingested.
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
from pipeline.sources import MissingApiKeyError

# Sources that need TIINGO_API_KEY; skipped with a warning when it is absent.
TIINGO_SOURCES = frozenset({"tiingo", "tiingo_fx"})

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


def _logger() -> logging.Logger:
    """Prefect run logger inside a flow/task, stdlib logger otherwise (module runs)."""
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger("pipeline")


@task(retries=2, retry_delay_seconds=30)
def ingest_asset(source_name: str, asset: AssetConfig, granularity: str) -> IngestResult:
    """Ingest one (source, symbol) pair.

    Transient failures raise so Prefect retries them. A rate-limit refusal is
    returned instead of raised: the quota is hourly, so retrying inside this run
    would only burn more of it — the caller trips a circuit breaker instead.
    """
    with psycopg.connect(database_url()) as conn:
        result = bronze.ingest(conn, source_name, asset, granularity)
    if result.status == "failed" and not result.rate_limited:
        raise RuntimeError(f"ingestion failed for {source_name}/{asset.symbol}: {result.error}")
    return result


def _run_dbt(args: list[str], *, required: bool) -> bool:
    """Invoke dbt in the project directory; return True on success."""
    logger = _logger()
    dbt_bin = Path(sys.executable).parent / "dbt"
    proc = subprocess.run(
        [str(dbt_bin), *args, "--profiles-dir", ".", "--no-use-colors"],
        cwd=REPO_ROOT / "dbt",
        env={**os.environ, **dbt_env()},
        capture_output=True,
        text=True,
    )
    logger.info(proc.stdout[-4000:])
    if proc.returncode != 0:
        logger.error(proc.stderr[-2000:])
        if required:
            raise RuntimeError(f"dbt {' '.join(args)} failed")
        logger.warning("non-critical dbt step failed: %s", " ".join(args))
        return False
    return True


@task
def run_dbt() -> None:
    """Build silver + gold and run every data-quality test."""
    _run_dbt(["build"], required=True)


@task
def refresh_leaderboard() -> None:
    """Rebuild the strategy leaderboard after backtests have written gold.backtest_runs."""
    _run_dbt(["run", "--select", "mart_strategy_leaderboard"], required=False)


@task
def backtest_symbol(asset: AssetConfig, cfg: AppConfig) -> int:
    logger = _logger()
    with psycopg.connect(database_url()) as conn:
        df = pd.read_sql_query(INDICATORS_QUERY, conn, params={"symbol": asset.symbol})
    if df.empty:
        logger.warning("no gold data for %s, skipping backtests", asset.symbol)
        return 0
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    # An all-NULL column (FX volume, or an indicator still in warm-up) arrives as
    # dtype object; force the numeric columns so downstream math and schema
    # validation see float64 everywhere.
    numeric = [c for c in df.columns if c not in ("symbol", "ts")]
    df[numeric] = df[numeric].astype("float64")
    validate_ohlcv(df, require_volume=asset.has_volume)

    bt = cfg.backtest
    n = 0
    with psycopg.connect(database_url()) as conn:
        for strat_cfg in cfg.strategies_for(asset):
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


def _ingest_all(cfg: AppConfig, logger: logging.Logger) -> tuple[int, list[str], list[str]]:
    """Ingest every (asset, source) pair, tolerating individual failures.

    Rate limits trip a per-source circuit breaker: once a source answers 429, the
    remaining symbols for that source are deferred without further calls. Their
    watermarks are untouched, so the next run resumes exactly where this one stopped.
    """
    succeeded = 0
    failures: list[str] = []
    deferred: list[str] = []
    limited_sources: set[str] = set()
    for asset in cfg.assets:
        for source_name in asset.sources.all:
            label = f"{source_name}/{asset.symbol}"
            if source_name in TIINGO_SOURCES and not tiingo_api_key():
                logger.warning("TIINGO_API_KEY not set — skipping %s", label)
                continue
            if source_name in limited_sources:
                deferred.append(label)
                continue
            try:
                result = ingest_asset(source_name, asset, cfg.granularity)
                if result.rate_limited:
                    limited_sources.add(source_name)
                    deferred.append(label)
                    logger.warning(
                        "%s is rate limited — deferring its remaining symbols to the next run",
                        source_name,
                    )
                else:
                    succeeded += 1
            except MissingApiKeyError as exc:
                logger.warning("skipping %s: %s", label, exc)
            except Exception as exc:  # transient API errors, bad symbols
                failures.append(label)
                logger.error("ingestion failed for %s: %s", label, exc)
    return succeeded, failures, deferred


@flow(name="daily-medallion-flow", log_prints=True)
def daily_flow() -> None:
    logger = _logger()
    cfg = load_config()

    succeeded, failures, deferred = _ingest_all(cfg, logger)
    if failures:
        logger.warning(
            "%d ingestions failed (recorded in meta.ingest_runs, will self-heal "
            "on the next run): %s",
            len(failures),
            ", ".join(failures),
        )
    if deferred:
        logger.warning(
            "%d ingestions deferred by rate limiting; they resume on the next run: %s",
            len(deferred),
            ", ".join(deferred),
        )
    if succeeded == 0 and not deferred:
        raise RuntimeError("every ingestion failed; aborting before dbt")

    run_dbt()

    total_backtests = 0
    backtest_failures: list[str] = []
    for asset in cfg.assets:
        # One malformed symbol must not cost the other 44 their backtests.
        try:
            total_backtests += backtest_symbol(asset, cfg)
        except Exception as exc:
            backtest_failures.append(asset.symbol)
            logger.error("backtests failed for %s: %s", asset.symbol, exc)
    if backtest_failures:
        logger.warning(
            "%d symbols produced no backtests: %s",
            len(backtest_failures),
            ", ".join(backtest_failures),
        )

    refresh_leaderboard()

    index_path = export.export_json(cfg)
    logger.info(
        "done: %d ingestions ok, %d failed, %d deferred (rate limit), %d backtests, export at %s",
        succeeded,
        len(failures),
        len(deferred),
        total_backtests,
        index_path,
    )


if __name__ == "__main__":
    daily_flow()
