"""Gold persistence for backtest results and the JSON export consumed by the portfolio site."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from pipeline.backtest.engine import BacktestResult
from pipeline.config import REPO_ROOT, AppConfig, database_url, load_config


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
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _json_default(o: Any) -> Any:
    if isinstance(o, datetime):
        return o.isoformat()
    return float(o)  # Decimal and numpy scalars


def export_json(cfg: AppConfig) -> Path:
    """Build exports/trading_sim.json from gold + meta. Assumes dbt marts exist."""
    out: dict[str, Any] = {"generated_at": datetime.now(UTC).isoformat()}
    with psycopg.connect(database_url()) as conn:
        # Date labels and isoformats below must not depend on the server's TimeZone.
        conn.execute("SET TIME ZONE 'UTC'")
        summaries = {r["symbol"]: r for r in _rows(conn, "SELECT * FROM gold.mart_asset_summary")}
        quality = {r["symbol"]: r for r in _rows(conn, "SELECT * FROM gold.mart_data_quality")}
        recon = {
            r["symbol"]: r
            for r in _rows(
                conn,
                """
                SELECT symbol,
                       count(*) FILTER (WHERE is_discrepant) AS n_discrepant,
                       max(abs_pct_diff)                      AS max_abs_pct_diff
                FROM gold.mart_source_reconciliation GROUP BY symbol
                """,
            )
        }
        out["assets"] = [
            {
                "symbol": a.symbol,
                "asset_class": a.asset_class,
                "summary": summaries.get(a.symbol),
                "data_quality": quality.get(a.symbol),
                "reconciliation": recon.get(a.symbol),
            }
            for a in cfg.assets
        ]

        runs = _rows(
            conn,
            """
            SELECT DISTINCT ON (symbol, strategy) *
            FROM gold.backtest_runs ORDER BY symbol, strategy, executed_at DESC
            """,
        )
        backtests = []
        for r in runs:
            curve = _rows(
                conn,
                """
                SELECT ts, equity, buy_hold_equity FROM gold.backtest_equity_curves
                WHERE backtest_run_id = %s ORDER BY ts
                """,
                (r["backtest_run_id"],),
            )
            points = [
                [c["ts"].date().isoformat(), round(float(c["equity"]), 2),
                 round(float(c["buy_hold_equity"]), 2)]
                for c in curve
            ]
            backtests.append(
                {
                    "symbol": r["symbol"],
                    "strategy": r["strategy"],
                    "params": r["params"],
                    "metrics": {
                        k: (float(r[k]) if r[k] is not None else None)
                        for k in (
                            "total_return", "cagr", "buy_hold_return",
                            "max_drawdown", "sharpe", "win_rate",
                        )
                    }
                    | {"n_trades": r["n_trades"]},
                    "equity_curve": _downsample(points, cfg.export.equity_curve_max_points),
                }
            )
        out["backtests"] = backtests

        out["pipeline"] = {
            "recent_ingest_runs": _rows(
                conn,
                """
                SELECT source, symbol, status, rows_fetched, rows_inserted, finished_at
                FROM meta.ingest_runs ORDER BY started_at DESC LIMIT 20
                """,
            )
        }

    path = REPO_ROOT / cfg.export.path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, default=_json_default, separators=(",", ":")))
    return path


if __name__ == "__main__":
    print(export_json(load_config()))
