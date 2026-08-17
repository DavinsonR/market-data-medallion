"""Retention for derived data, so a daily schedule cannot outgrow a free tier.

Bronze is append-only and grows by one candle per asset per day (trivial).
Backtests are different: every run now rewrites ~1,347 results — 222 single
strategies plus 1,125 AND-combinations across the 45 assets. Left unbounded that
fills a 500 MB database in days, so only the most recent runs per
(symbol, strategy) are kept.

Combinations made the arithmetic survivable rather than worse: they store
metrics only (``gold.backtest_runs.has_curve`` is false), so the ~260k
equity-curve points per generation still come from the 222 single runs alone.
The metrics rows themselves are a rounding error next to their curves.

Nothing of value is lost: the export reads the latest run per pair, and any
result can be reproduced exactly from bronze, which is never pruned.
"""

from __future__ import annotations

import logging

import psycopg

logger = logging.getLogger(__name__)

# Measured on the real warehouse: one full set of runs is ~40 MB of equity curves
# on top of ~25 MB of bronze. Two generations keep day-over-day comparison possible
# while staying near 140 MB — comfortably inside a 500 MB free tier. The move to
# 1,347 variants does not change this, because the extra 1,125 runs per generation
# are curve-less metric rows.
DEFAULT_KEEP = 2

# Partitioning by (symbol, strategy) is already the right grain for combinations:
# a combination's `strategy` is its own name ("macd+sma_cross"), so each variant
# ages out independently of the singles it is built from.
#
# Equity curves disappear with their run via ON DELETE CASCADE.
#
# Measured at v3 scale on the local warehouse (4,423 runs, 1.27M curve points,
# 1,458 runs pruned): 25 ms for the ranking and the DELETE itself, 1.41 s for the
# cascade that removes ~270k curve rows — 1.44 s total, once a day.
#
# The plan is a seq scan plus a 15 ms quicksort, not an index scan on
# idx_backtest_runs_symbol_strategy: the runs table is ~170 pages, and the index
# does not carry backtest_run_id, so the planner correctly prefers the scan. That
# index earns its keep on the export's DISTINCT ON (symbol, strategy) query, and
# nothing here needs it to stay fast.
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
