"""Gold persistence for backtest results and the two-tier JSON export for the portfolio site.

The export is split so the site can render fast and drill down on demand:

* ``exports/index.json``            — every configured asset with headline metrics, the strategy
                                      leaderboard and pipeline stats. No equity curves, so it stays
                                      small enough to load on first paint.
* ``exports/backtests/<SYMBOL>.json`` — the heavy per-symbol payload (params + downsampled equity
                                      curves), fetched only when a visitor opens that asset.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.backtest.engine import BacktestResult
from pipeline.config import AppConfig, database_url, load_config

logger = logging.getLogger(__name__)

# Numeric metrics carried verbatim from a gold.backtest_runs row.
_METRIC_KEYS = ("total_return", "cagr", "buy_hold_return", "max_drawdown", "sharpe", "win_rate")

_LEADERBOARD_RELATION = "gold.mart_strategy_leaderboard"
# Preferred sort of the leaderboard, applied only for columns the mart actually exposes, so the
# export stays deterministic without hard-coding a schema owned by another module (dbt).
_LEADERBOARD_ORDER = ("strategy", "asset_class", "region")

_LATEST_RUNS_SQL = """
SELECT DISTINCT ON (symbol, strategy) *
FROM gold.backtest_runs
ORDER BY symbol, strategy, executed_at DESC
"""

_CURVES_SQL = """
SELECT backtest_run_id, ts, equity, buy_hold_equity
FROM gold.backtest_equity_curves
WHERE backtest_run_id = ANY(%s)
ORDER BY backtest_run_id, ts
"""

_RECONCILIATION_SQL = """
SELECT symbol,
       count(*) FILTER (WHERE is_discrepant) AS n_discrepant,
       max(abs_pct_diff)                      AS max_abs_pct_diff
FROM gold.mart_source_reconciliation GROUP BY symbol
"""

_RECENT_INGEST_SQL = """
SELECT source, symbol, status, rows_fetched, rows_inserted, finished_at
FROM meta.ingest_runs ORDER BY started_at DESC LIMIT 40
"""

# Everything the pipeline has actually produced in the database (as opposed to what config.yaml
# asks for): assets carrying bronze data, stored backtest runs, raw rows and distinct sources.
_TOTALS_SQL = """
SELECT (SELECT count(DISTINCT symbol) FROM bronze.raw_candles) AS assets,
       (SELECT count(*)               FROM gold.backtest_runs) AS backtests,
       (SELECT count(*)               FROM bronze.raw_candles) AS bronze_rows,
       (SELECT count(DISTINCT source) FROM bronze.raw_candles) AS sources
"""


def write_backtest_result(
    conn: psycopg.Connection,
    *,
    symbol: str,
    strategy: str,
    params: dict[str, Any],
    fee_bps: float,
    slippage_bps: float,
    result: BacktestResult,
) -> str:
    run_id = str(uuid.uuid4())
    m = result.metrics
    curve = result.equity_curve
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.backtest_runs (
                backtest_run_id, symbol, strategy, params, fee_bps, slippage_bps,
                start_ts, end_ts, n_bars, total_return, cagr, buy_hold_return,
                max_drawdown, sharpe, n_trades, win_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id, symbol, strategy, json.dumps(params), fee_bps, slippage_bps,
                curve["ts"].iloc[0].to_pydatetime(), curve["ts"].iloc[-1].to_pydatetime(),
                len(curve), m.get("total_return"), m.get("cagr"), m.get("buy_hold_return"),
                m.get("max_drawdown"), m.get("sharpe"), m.get("n_trades"), m.get("win_rate"),
            ),
        )
        cur.executemany(
            """
            INSERT INTO gold.backtest_equity_curves (backtest_run_id, ts, equity, buy_hold_equity)
            VALUES (%s, %s, %s, %s)
            """,
            [
                (run_id, row.ts.to_pydatetime(), float(row.equity), float(row.buy_hold_equity))
                for row in curve.itertuples()
            ],
        )
    conn.commit()
    return run_id


def _downsample(points: list[list[Any]], max_points: int) -> list[list[Any]]:
    if len(points) <= max_points:
        return points
    step = (len(points) - 1) / (max_points - 1)
    idx = sorted({round(i * step) for i in range(max_points)} | {len(points) - 1})
    return [points[i] for i in idx]


def _rows(conn: psycopg.Connection, query: str, params: tuple = ()) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _json_default(o: Any) -> Any:
    if isinstance(o, datetime):
        return o.isoformat()
    return float(o)  # Decimal and numpy scalars


def _f(value: Any) -> float | None:
    """Decimal -> float, preserving NULL."""
    return float(value) if value is not None else None


def _excess_return(run: dict[str, Any]) -> float | None:
    total, buy_hold = run["total_return"], run["buy_hold_return"]
    if total is None or buy_hold is None:
        return None
    return float(total) - float(buy_hold)


def _fetch_leaderboard(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Read the dbt-owned leaderboard mart, tolerating its absence on a fresh database."""
    present = _rows(conn, "SELECT to_regclass(%s) IS NOT NULL AS present", (_LEADERBOARD_RELATION,))
    if not present[0]["present"]:
        logger.warning("%s not found — exporting an empty leaderboard", _LEADERBOARD_RELATION)
        return []
    schema, table = _LEADERBOARD_RELATION.split(".")
    columns = {
        r["column_name"]
        for r in _rows(
            conn,
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
    }
    order = [c for c in _LEADERBOARD_ORDER if c in columns]
    query = f"SELECT * FROM {_LEADERBOARD_RELATION}"  # constant relation name, not user input
    if order:
        query += " ORDER BY " + ", ".join(order)
    return _rows(conn, query)


def _fetch_curves(
    conn: psycopg.Connection, run_ids: list[Any]
) -> dict[Any, list[list[Any]]]:
    """All equity curves for the given runs in one round trip, grouped by run id.

    Points are ``[utc_date, equity, buy_hold_equity]``. The session is pinned to UTC by the
    caller, so ``.date()`` yields the UTC calendar date (BITACORA_TECNICA 6.2).
    """
    if not run_ids:
        return {}
    curves: dict[Any, list[list[Any]]] = defaultdict(list)
    for row in _rows(conn, _CURVES_SQL, (run_ids,)):
        curves[row["backtest_run_id"]].append(
            [
                row["ts"].date().isoformat(),
                round(float(row["equity"]), 2),
                round(float(row["buy_hold_equity"]), 2),
            ]
        )
    return curves


def _index_strategy(run: dict[str, Any]) -> dict[str, Any]:
    """Headline strategy figures for index.json — no curve, no params."""
    return {
        "strategy": run["strategy"],
        "total_return": _f(run["total_return"]),
        "buy_hold_return": _f(run["buy_hold_return"]),
        "excess_return": _excess_return(run),
        "max_drawdown": _f(run["max_drawdown"]),
        "sharpe": _f(run["sharpe"]),
        "n_trades": run["n_trades"],
        "win_rate": _f(run["win_rate"]),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, default=_json_default, separators=(",", ":")))
    return path


def _prune_stale(directory: Path, keep: set[Path]) -> list[Path]:
    """Drop per-symbol files that this export did not write (symbols dropped from config.yaml)."""
    if not directory.is_dir():
        return []
    removed = [p for p in sorted(directory.glob("*.json")) if p not in keep]
    for path in removed:
        path.unlink()
    if removed:
        logger.info("removed %d stale export file(s): %s", len(removed),
                    ", ".join(p.name for p in removed))
    return removed


def export_json(cfg: AppConfig) -> Path:
    """Write exports/index.json plus one exports/backtests/<SYMBOL>.json per backtested symbol.

    Returns the index path (the flow logs it).
    """
    generated_at = datetime.now(UTC).isoformat()
    with psycopg.connect(database_url()) as conn:
        # Date labels and isoformats below must not depend on the server's TimeZone: a
        # America/Bogota session used to shift every curve date one day back (BITACORA 6.2).
        conn.execute("SET TIME ZONE 'UTC'")

        summaries = {r["symbol"]: r for r in _rows(conn, "SELECT * FROM gold.mart_asset_summary")}
        quality = {r["symbol"]: r for r in _rows(conn, "SELECT * FROM gold.mart_data_quality")}
        recon = {r["symbol"]: r for r in _rows(conn, _RECONCILIATION_SQL)}

        # One query for the latest run per (symbol, strategy), one for all their curves.
        runs = [r for r in _rows(conn, _LATEST_RUNS_SQL) if cfg.asset(r["symbol"]) is not None]
        curves = _fetch_curves(conn, [r["backtest_run_id"] for r in runs])

        leaderboard = _fetch_leaderboard(conn)
        totals = _rows(conn, _TOTALS_SQL)[0]
        recent_ingest_runs = _rows(conn, _RECENT_INGEST_SQL)

    runs_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        runs_by_symbol[run["symbol"]].append(run)

    index = {
        "generated_at": generated_at,
        # Order follows config.yaml; assets with no data yet are emitted with nulls so the site
        # can render them as pending.
        "assets": [
            {
                "symbol": a.symbol,
                "name": a.name,
                "asset_class": a.asset_class,
                "region": a.region,
                "summary": summaries.get(a.symbol),
                "data_quality": quality.get(a.symbol),
                "reconciliation": recon.get(a.symbol),
                "strategies": [_index_strategy(r) for r in runs_by_symbol.get(a.symbol, [])],
            }
            for a in cfg.assets
        ],
        "leaderboard": leaderboard,
        "pipeline": {"recent_ingest_runs": recent_ingest_runs, "totals": totals},
    }

    written: set[Path] = set()
    for asset in cfg.assets:
        symbol_runs = runs_by_symbol.get(asset.symbol)
        if not symbol_runs:
            continue
        written.add(
            _write_json(
                cfg.export.symbol_path(asset.symbol),
                {
                    "symbol": asset.symbol,
                    "name": asset.name,
                    "asset_class": asset.asset_class,
                    "region": asset.region,
                    "generated_at": generated_at,
                    "backtests": [
                        {
                            "strategy": r["strategy"],
                            "params": r["params"],
                            "metrics": {k: _f(r[k]) for k in _METRIC_KEYS}
                            | {
                                "excess_return": _excess_return(r),
                                "n_trades": r["n_trades"],
                                "n_bars": r["n_bars"],
                            },
                            "equity_curve": _downsample(
                                curves.get(r["backtest_run_id"], []),
                                cfg.export.equity_curve_max_points,
                            ),
                        }
                        for r in symbol_runs
                    ],
                },
            )
        )

    _prune_stale(cfg.export.index_path.parent / cfg.export.per_symbol_dir, written)
    return _write_json(cfg.export.index_path, index)


if __name__ == "__main__":
    print(export_json(load_config()))
