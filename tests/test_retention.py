"""Retention keeps the newest runs per (symbol, strategy) and nothing else.

The same reachable-Postgres fixture also covers the persistence half of the
storage contract: with ~1,347 runs per execution, only single-strategy runs may
write an equity curve, and ``has_curve=False`` has to mean *no rows written* —
not "written and ignored later".
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pytest

from pipeline.backtest.engine import BacktestResult
from pipeline.export import write_backtest_result
from pipeline.retention import prune_backtest_history

# Never DATABASE_URL: that may point at the managed warehouse, and these tests
# write rows and run a table-wide prune. Tests get their own database or none.
PG_URL = os.environ.get("MDM_TEST_DATABASE_URL", "postgresql://mdm@localhost:5433/mdm")

INSERT_RUN = """
INSERT INTO gold.backtest_runs (
    backtest_run_id, executed_at, symbol, strategy, params, fee_bps, slippage_bps,
    start_ts, end_ts, n_bars, strategy_kind, components, n_components, has_curve
) VALUES (%s, %s, %s, %s, '{}', 10, 5, now(), now(), 1, %s, %s, %s, %s)
"""


def _shape(strategy: str) -> tuple[str, list[str], int]:
    """(kind, components, n) derived from the canonical 'a+b+c' strategy name.

    Migration 004 constrains cardinality(components) = n_components, so seeded rows
    have to be as well-formed as the ones the pipeline writes.
    """
    parts = strategy.split("+")
    return ("single" if len(parts) == 1 else "combo"), parts, len(parts)

# write_backtest_result commits, so its tests cannot hide inside a rolled-back
# transaction the way the prune tests do; they clean up by symbol instead.
PERSIST_SYMBOL = "PERSIST-TEST"


@pytest.fixture
def pg_conn() -> Any:
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(PG_URL, connect_timeout=5)
    except Exception:  # noqa: BLE001 - the suite must pass without a database
        pytest.skip("no reachable Postgres")
    with conn:
        yield conn


@pytest.fixture
def persist_conn(pg_conn: Any) -> Any:
    """A connection whose committed PERSIST_SYMBOL rows are removed afterwards."""
    try:
        yield pg_conn
    finally:
        pg_conn.execute("DELETE FROM gold.backtest_runs WHERE symbol = %s", (PERSIST_SYMBOL,))
        pg_conn.commit()


def _seed(conn: Any, symbol: str, strategy: str, n: int) -> list[str]:
    """Insert n runs for one pair, oldest first; returns ids newest-first."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    kind, parts, n_parts = _shape(strategy)
    ids = []
    for i in range(n):
        run_id = str(uuid.uuid4())
        conn.execute(
            INSERT_RUN,
            (
                run_id, base + timedelta(days=i), symbol, strategy,
                kind, parts, n_parts, kind == "single",
            ),
        )
        ids.append(run_id)
    return list(reversed(ids))


def _result(n_bars: int = 5) -> BacktestResult:
    """A minimal BacktestResult: the persistence path only reads metrics and the curve."""
    ts = pd.date_range("2024-01-01", periods=n_bars, freq="D", tz="UTC")
    curve = pd.DataFrame(
        {
            "ts": ts,
            "equity": [10_000.0 + i for i in range(n_bars)],
            "buy_hold_equity": [10_000.0 + 2 * i for i in range(n_bars)],
        }
    )
    metrics = {
        "total_return": 0.10,
        "cagr": 0.05,
        "buy_hold_return": 0.08,
        "max_drawdown": -0.02,
        "sharpe": 1.5,
        "n_trades": 3,
        "win_rate": 0.66,
        "exposure": 0.42,
    }
    return BacktestResult(metrics=metrics, equity_curve=curve, trades=[])


def _curve_rows(conn: Any, run_id: str) -> int:
    return conn.execute(
        "SELECT count(*) FROM gold.backtest_equity_curves WHERE backtest_run_id = %s",
        (run_id,),
    ).fetchone()[0]


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


def test_combinations_age_out_independently_of_their_components(pg_conn: Any) -> None:
    """A combination's `strategy` is its own name, so it is its own retention pair."""
    from psycopg import Rollback

    symbol = "RETENTION-COMBO"
    with pg_conn.transaction():
        newest_single = _seed(pg_conn, symbol, "macd", 4)
        newest_combo = _seed(pg_conn, symbol, "macd+sma_cross", 4)
        newest_wide = _seed(pg_conn, symbol, "fibonacci+macd+sma_cross", 1)

        prune_backtest_history(pg_conn, keep=2)

        rows = pg_conn.execute(
            "SELECT backtest_run_id FROM gold.backtest_runs WHERE symbol = %s", (symbol,)
        ).fetchall()
        survivors = {str(r[0]) for r in rows}

        # Pruning "macd" must not touch "macd+sma_cross", and a pair with a single
        # generation keeps it.
        assert survivors == set(newest_single[:2]) | set(newest_combo[:2]) | set(newest_wide)
        raise Rollback


def test_prune_scales_to_a_full_v3_generation(pg_conn: Any) -> None:
    """1,347 variants x 3 generations: the prune keeps exactly two of each pair."""
    from psycopg import Rollback

    symbol = "RETENTION-SCALE"
    base = datetime(2026, 1, 1, tzinfo=UTC)
    with pg_conn.transaction():
        with pg_conn.cursor() as cur:
            cur.executemany(
                INSERT_RUN,
                [
                    (
                        str(uuid.uuid4()),
                        base + timedelta(days=gen),
                        symbol,
                        f"variant_{i}",
                        "single",
                        [f"variant_{i}"],
                        1,
                        True,
                    )
                    for i in range(1_347)
                    for gen in range(3)
                ],
            )
        # The prune is table-wide, so `deleted` also counts whatever the shared
        # database already held; the assertions below stay scoped to this symbol.
        assert prune_backtest_history(pg_conn, keep=2) >= 1_347

        remaining, pairs, oldest = pg_conn.execute(
            "SELECT count(*), count(DISTINCT strategy), min(executed_at) "
            "FROM gold.backtest_runs WHERE symbol = %s",
            (symbol,),
        ).fetchone()
        assert (remaining, pairs) == (2 * 1_347, 1_347)  # two generations of every variant
        assert oldest == base + timedelta(days=1)  # generation 0 is the one that went
        raise Rollback


def test_keep_must_be_positive(pg_conn: Any) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        prune_backtest_history(pg_conn, keep=0)


def test_single_strategy_run_stores_its_curve(persist_conn: Any) -> None:
    run_id = write_backtest_result(
        persist_conn,
        symbol=PERSIST_SYMBOL,
        strategy="macd",
        params={"fast": 12, "slow": 26, "signal": 9},
        fee_bps=10,
        slippage_bps=5,
        result=_result(n_bars=7),
    )
    row = persist_conn.execute(
        "SELECT strategy_kind, components, n_components, has_curve, exposure "
        "FROM gold.backtest_runs WHERE backtest_run_id = %s",
        (run_id,),
    ).fetchone()

    # The pre-v3 call shape still writes the single-strategy row it always wrote.
    assert row[0] == "single"
    assert row[1] == ["macd"]
    assert row[2] == 1
    assert row[3] is True
    assert float(row[4]) == pytest.approx(0.42)
    assert _curve_rows(persist_conn, run_id) == 7


def test_combination_run_writes_no_curve_rows(persist_conn: Any) -> None:
    run_id = write_backtest_result(
        persist_conn,
        symbol=PERSIST_SYMBOL,
        strategy="macd+sma_cross",
        params={"components": ["macd", "sma_cross"]},
        fee_bps=10,
        slippage_bps=5,
        result=_result(n_bars=7),
        components=["macd", "sma_cross"],
        has_curve=False,
    )
    row = persist_conn.execute(
        "SELECT strategy_kind, components, n_components, has_curve, n_bars "
        "FROM gold.backtest_runs WHERE backtest_run_id = %s",
        (run_id,),
    ).fetchone()

    assert row[0] == "combo"  # derived from the component count
    assert row[1] == ["macd", "sma_cross"]
    assert row[2] == 2
    assert row[3] is False
    assert row[4] == 7  # the run still describes the bars it covered
    assert _curve_rows(persist_conn, run_id) == 0


def test_window_metrics_round_trip(persist_conn: Any) -> None:
    split = datetime(2024, 1, 5, tzinfo=UTC)
    run_id = write_backtest_result(
        persist_conn,
        symbol=PERSIST_SYMBOL,
        strategy="fibonacci",
        params={"window": 100, "ratio": 0.618},
        fee_bps=10,
        slippage_bps=5,
        result=_result(),
        is_metrics={
            "total_return": 0.30,
            "buy_hold_return": 0.10,
            "sharpe": 1.1,
            "max_drawdown": -0.05,
            "n_trades": 4,
        },
        oos_metrics={
            "total_return": -0.04,
            "buy_hold_return": 0.06,
            "sharpe": -0.3,
            "max_drawdown": -0.12,
            "n_trades": 2,
        },
        split_ts=pd.Timestamp(split),
    )
    row = persist_conn.execute(
        "SELECT is_total_return, is_buy_hold_return, is_sharpe, is_max_drawdown, is_n_trades,"
        "       oos_total_return, oos_buy_hold_return, oos_sharpe, oos_max_drawdown,"
        "       oos_n_trades, split_ts "
        "FROM gold.backtest_runs WHERE backtest_run_id = %s",
        (run_id,),
    ).fetchone()

    assert [float(v) for v in row[:4]] == [0.30, 0.10, 1.1, -0.05]
    assert row[4] == 4
    assert [float(v) for v in row[5:9]] == [-0.04, 0.06, -0.3, -0.12]
    assert row[9] == 2
    assert row[10] == split


def test_missing_window_metrics_are_stored_as_null(persist_conn: Any) -> None:
    """A window too short to measure is NULL, never a zero that reads like a result."""
    run_id = write_backtest_result(
        persist_conn,
        symbol=PERSIST_SYMBOL,
        strategy="rsi_reversion",
        params={},
        fee_bps=10,
        slippage_bps=5,
        result=_result(),
        is_metrics=None,
        oos_metrics=None,
        split_ts=None,
    )
    row = persist_conn.execute(
        "SELECT is_total_return, is_sharpe, is_n_trades, oos_total_return, oos_sharpe,"
        "       oos_n_trades, split_ts "
        "FROM gold.backtest_runs WHERE backtest_run_id = %s",
        (run_id,),
    ).fetchone()
    assert all(value is None for value in row)
