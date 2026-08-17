"""Retention for derived data, so a daily schedule cannot outgrow a free tier.

Bronze is append-only and grows by one candle per asset per day (trivial).
Backtests are different: every run rewrites all 177 results and roughly 212k
equity-curve points. Left unbounded that fills a 500 MB database in under a
week, so only the most recent runs per (symbol, strategy) are kept.

Nothing of value is lost: the export reads the latest run per pair, and any
result can be reproduced exactly from bronze, which is never pruned.
"""

from __future__ import annotations

import logging

import psycopg

logger = logging.getLogger(__name__)

# Measured on the real warehouse: one full set of runs is ~40 MB of equity curves
# on top of ~25 MB of bronze. Two generations keep day-over-day comparison possible
# while staying near 140 MB — comfortably inside a 500 MB free tier.
DEFAULT_KEEP = 2

# Equity curves disappear with their run via ON DELETE CASCADE.
_PRUNE_SQL = """
WITH ranked AS (
    SELECT backtest_run_id,
           row_number() OVER (
               PARTITION BY symbol, strategy ORDER BY executed_at DESC
           ) AS recency
    FROM gold.backtest_runs
)
DELETE FROM gold.backtest_runs
WHERE backtest_run_id IN (SELECT backtest_run_id FROM ranked WHERE recency > %s)
"""


def prune_backtest_history(conn: psycopg.Connection, keep: int = DEFAULT_KEEP) -> int:
    """Delete all but the ``keep`` most recent runs per (symbol, strategy).

    The caller owns the transaction: psycopg's connection context manager commits
    on a clean exit, and callers already inside a transaction stay in control.
    """
    if keep < 1:
        raise ValueError("keep must be at least 1")
    cur = conn.execute(_PRUNE_SQL, (keep,))
    deleted = max(cur.rowcount, 0)
    if deleted:
        logger.info("pruned %d superseded backtest runs (keeping %d per pair)", deleted, keep)
    return deleted
