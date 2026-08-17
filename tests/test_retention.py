"""Retention keeps the newest runs per (symbol, strategy) and nothing else."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pipeline.retention import prune_backtest_history

PG_URL = os.environ.get("DATABASE_URL", "postgresql://mdm@localhost:5433/mdm")

INSERT_RUN = """
INSERT INTO gold.backtest_runs (
    backtest_run_id, executed_at, symbol, strategy, params, fee_bps, slippage_bps,
    start_ts, end_ts, n_bars
) VALUES (%s, %s, %s, %s, '{}', 10, 5, now(), now(), 1)
"""


@pytest.fixture
def pg_conn() -> Any:
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(PG_URL, connect_timeout=5)
    except Exception:  # noqa: BLE001 - the suite must pass without a database
        pytest.skip("no reachable Postgres")
    with conn:
        yield conn


def _seed(conn: Any, symbol: str, strategy: str, n: int) -> list[str]:
    """Insert n runs for one pair, oldest first; returns ids newest-first."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    ids = []
    for i in range(n):
        run_id = str(uuid.uuid4())
        conn.execute(INSERT_RUN, (run_id, base + timedelta(days=i), symbol, strategy))
        ids.append(run_id)
    return list(reversed(ids))


def test_keeps_only_the_newest_runs_per_pair(pg_conn: Any) -> None:
    from psycopg import Rollback

    symbol = "RETENTION-TEST"
    # Rollback raised inside the block is swallowed by the transaction manager,
    # which is exactly what leaves the shared database untouched.
    with pg_conn.transaction():
        newest_a = _seed(pg_conn, symbol, "strat_a", 5)
        newest_b = _seed(pg_conn, symbol, "strat_b", 2)

        prune_backtest_history(pg_conn, keep=2)

        rows = pg_conn.execute(
            "SELECT strategy, backtest_run_id FROM gold.backtest_runs WHERE symbol = %s",
            (symbol,),
        ).fetchall()
        survivors = {str(r[1]) for r in rows}  # psycopg returns UUID objects

        # strat_a had 5 runs -> its 2 newest survive; strat_b had 2 -> both survive.
        assert survivors == set(newest_a[:2]) | set(newest_b)
        raise Rollback


def test_keep_must_be_positive(pg_conn: Any) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        prune_backtest_history(pg_conn, keep=0)
