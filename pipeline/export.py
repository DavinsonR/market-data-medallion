"""Gold persistence for backtest results and the two-tier JSON export for the portfolio site.

The export is split so the site can render fast and drill down on demand:

* ``exports/index.json``            — every configured asset with headline metrics, its evaluated
                                      strategy combinations (metrics only), the strategy
                                      leaderboard, the overfitting summary and pipeline stats.
                                      No equity curves, so it stays small enough for first paint.
* ``exports/backtests/<SYMBOL>.json`` — the heavy per-symbol payload (params + downsampled equity
                                      curves for the single strategies) plus the full combination
                                      array, fetched only when a visitor opens that asset.

Combinations never carry an equity curve: ~1,347 curves per run would not fit the database's free
tier (see db/migrations/003_strategy_combinations.sql), so ``gold.backtest_runs.has_curve`` is true
only for single strategies and the export asks for exactly those curves.

``index.json`` is kept under a hard byte budget. It is serialized, measured, and — only if the
combination arrays push it past the budget — rebuilt with each asset's top ``N`` combinations by
out-of-sample excess return. The complete arrays always remain in the per-symbol files.
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

# dbt-owned marts read by the combination export. Both are optional: on a fresh database (or
# before dbt has run) they simply do not exist, and the export degrades instead of crashing.
_COMBINATION_RELATION = "gold.mart_combination_analysis"
_OVERFITTING_RELATION = "gold.mart_overfitting_summary"

# ADR FX decomposition (10 ADRs x 4 windows = 40 rows), optional like the marts above.
# The window order mirrors the mart's accepted_values so the array reads shortest-first.
_FX_DECOMPOSITION_RELATION = "gold.mart_fx_decomposition"
_FX_DECOMPOSITION_SQL = f"""
SELECT * FROM {_FX_DECOMPOSITION_RELATION}
ORDER BY symbol, array_position(ARRAY['30d', '90d', '365d', 'full'], window_label)
"""

# index.json is the first-paint payload of a static site, so its size is a hard requirement rather
# than a hope: the payload is serialized, measured, and downgraded to the top combinations per
# asset if it exceeds the budget. 600 KB is roughly two seconds on a slow 3G connection.
_INDEX_BYTE_BUDGET = 600_000
_INDEX_TOP_COMBINATIONS = 5
# Combinations are ranked by out-of-sample excess return — the only figure that was not used to
# choose anything, and therefore the only honest way to say "top".
_COMBINATION_RANK_KEY = "oos_excess_return"

# Rounding applied to the combination arrays only. Six decimals on a return is a ten-thousandth of
# a basis point: far below the precision the numbers actually carry, and it removes ~40% of the
# bytes that full float repr would spend. The pre-existing `strategies` array is left untouched.
_RATIO_DIGITS = 6
_SHARPE_DIGITS = 4

# Marker values an aggregate ("all variants") row of the overfitting mart may carry. The house
# convention from mart_strategy_leaderboard is a boolean `is_grand_total`; a NULL grouping key or
# an explicit label are the other two shapes a grouping-sets aggregate usually takes.
_OVERALL_LABELS = frozenset({"overall", "all", "total"})

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


def _to_datetime(value: Any) -> Any:
    """pandas Timestamp -> datetime, passing through NULL and plain datetimes."""
    return value.to_pydatetime() if hasattr(value, "to_pydatetime") else value


def _window_columns(metrics: dict[str, Any] | None) -> tuple[Any, ...]:
    """The six columns a train/validation window contributes, or NULLs when it has none.

    A window below the engine's minimum length returns no metrics at all, and that absence is
    stored as NULL rather than as a zero, which would read like a measurement (BITACORA 5.3).

    ``exposure`` travels with the rest on purpose: without it, a window in which the variant
    never opened a position is indistinguishable from one where it traded and broke even, and
    it silently counts as beating buy & hold whenever buy & hold was negative.
    """
    if not metrics:
        return (None,) * 6
    return (
        metrics.get("total_return"),
        metrics.get("buy_hold_return"),
        metrics.get("sharpe"),
        metrics.get("max_drawdown"),
        metrics.get("n_trades"),
        metrics.get("exposure"),
    )


def write_backtest_result(
    conn: psycopg.Connection,
    *,
    symbol: str,
    strategy: str,
    params: dict[str, Any],
    fee_bps: float,
    slippage_bps: float,
    result: BacktestResult,
    strategy_kind: str | None = None,
    components: list[str] | tuple[str, ...] | None = None,
    n_components: int | None = None,
    has_curve: bool = True,
    exposure: float | None = None,
    is_metrics: dict[str, Any] | None = None,
    oos_metrics: dict[str, Any] | None = None,
    split_ts: Any = None,
) -> str:
    """Persist one backtest run, storing its equity curve only when ``has_curve``.

    Every v3 argument defaults to the single-strategy shape, so the original call signature still
    writes exactly the row it always wrote. Combinations pass their components and
    ``has_curve=False``: at ~174 KB per curve, 1,347 curves are 229 MB, and the two generations
    retention keeps would not fit a 500 MB free tier.
    """
    parts = list(components) if components is not None else [strategy]
    n_parts = n_components if n_components is not None else len(parts)
    kind = strategy_kind or ("single" if n_parts == 1 else "combo")
    run_id = str(uuid.uuid4())
    m = result.metrics
    curve = result.equity_curve
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.backtest_runs (
                backtest_run_id, symbol, strategy, params, fee_bps, slippage_bps,
                start_ts, end_ts, n_bars, total_return, cagr, buy_hold_return,
                max_drawdown, sharpe, n_trades, win_rate,
                strategy_kind, components, n_components, has_curve, exposure,
                is_total_return, is_buy_hold_return, is_sharpe, is_max_drawdown, is_n_trades,
                is_exposure,
                oos_total_return, oos_buy_hold_return, oos_sharpe, oos_max_drawdown,
                oos_n_trades, oos_exposure, split_ts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s)
            """,
            (
                run_id, symbol, strategy, json.dumps(params), fee_bps, slippage_bps,
                curve["ts"].iloc[0].to_pydatetime(), curve["ts"].iloc[-1].to_pydatetime(),
                len(curve), m.get("total_return"), m.get("cagr"), m.get("buy_hold_return"),
                m.get("max_drawdown"), m.get("sharpe"), m.get("n_trades"), m.get("win_rate"),
                kind, parts, n_parts, has_curve,
                exposure if exposure is not None else m.get("exposure"),
                *_window_columns(is_metrics), *_window_columns(oos_metrics),
                _to_datetime(split_ts),
            ),
        )
        if has_curve:
            cur.executemany(
                """
                INSERT INTO gold.backtest_equity_curves
                    (backtest_run_id, ts, equity, buy_hold_equity)
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


def _round(value: Any, digits: int = _RATIO_DIGITS) -> float | None:
    """Decimal/float -> rounded float, preserving NULL. Used only by the combination arrays."""
    return round(float(value), digits) if value is not None else None


def _first(row: dict[str, Any], *names: str) -> Any:
    """First non-NULL value among ``names``.

    The marts below are written by another module (dbt) and read here. Rather than assuming one
    exact column name — the mistake that silently NULLed every Kraken price in BITACORA 6.3 — each
    field accepts the plausible spellings and falls back to deriving the value.
    """
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return None


def _delta(minuend: Any, subtrahend: Any) -> float | None:
    if minuend is None or subtrahend is None:
        return None
    return float(minuend) - float(subtrahend)


def _relation_exists(conn: psycopg.Connection, relation: str) -> bool:
    """True when the relation is visible to this session (dbt-owned marts may not exist yet)."""
    rows = _rows(conn, "SELECT to_regclass(%s) IS NOT NULL AS present", (relation,))
    return bool(rows[0]["present"])


def _fetch_leaderboard(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Read the dbt-owned leaderboard mart, tolerating its absence on a fresh database."""
    if not _relation_exists(conn, _LEADERBOARD_RELATION):
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


def _excess_of(row: dict[str, Any], *names: str, prefix: str) -> float | None:
    """The row's own excess return if it carries one, else ``total_return − buy_hold_return``.

    ``prefix`` selects the window: ``""`` (full period), ``"is_"`` or ``"oos_"``.
    """
    value = _first(row, *names)
    if value is not None:
        return float(value)
    return _delta(
        _first(row, f"{prefix}total_return", f"full_{prefix}total_return"),
        _first(row, f"{prefix}buy_hold_return", f"full_{prefix}buy_hold_return"),
    )


def _combination_entry(row: dict[str, Any]) -> dict[str, Any]:
    """One evaluated variant, curve-free, for both index.json and the per-symbol file.

    Accepts a row of ``gold.mart_combination_analysis`` or, when that mart does not exist yet, a
    raw ``gold.backtest_runs`` row: the two share their column names, and anything the mart
    pre-computes (excess returns, the ``beat_bh_*`` booleans) is derived here when absent.

    ``n_components == 1`` marks a single strategy, so a per-asset heatmap can render the whole
    lattice — the five singles and their 26 AND-combinations — from this one array.
    """
    strategy = row["strategy"]
    total = _first(row, "total_return", "full_total_return")
    buy_hold = _first(row, "buy_hold_return", "full_buy_hold_return")

    excess = _excess_of(row, "excess_return", "full_excess_return", prefix="")
    is_excess = _excess_of(row, "is_excess_return", prefix="is_")
    oos_excess = _excess_of(row, "oos_excess_return", prefix="oos_")

    beat_full = _first(row, "beat_bh_full", "beat_buy_hold")
    if beat_full is None and excess is not None:
        beat_full = excess > 0
    beat_oos = _first(row, "beat_bh_oos")
    if beat_oos is None and oos_excess is not None:
        beat_oos = oos_excess > 0

    # The naming convention (components sorted and joined by '+') makes the component count
    # recoverable from the name alone, which keeps this correct if the column is ever missing.
    n_components = _first(row, "n_components")
    n_components = int(n_components) if n_components is not None else strategy.count("+") + 1
    n_trades = _first(row, "n_trades", "full_n_trades")
    return {
        "strategy": strategy,
        "n_components": n_components,
        "exposure": _round(_first(row, "exposure")),
        "total_return": _round(total),
        "buy_hold_return": _round(buy_hold),
        "excess_return": _round(excess),
        "is_excess_return": _round(is_excess),
        "oos_excess_return": _round(oos_excess),
        "beat_bh_full": beat_full,
        "beat_bh_oos": beat_oos,
        "sharpe": _round(_first(row, "sharpe", "full_sharpe"), _SHARPE_DIGITS),
        "max_drawdown": _round(_first(row, "max_drawdown", "full_max_drawdown")),
        "n_trades": int(n_trades) if n_trades is not None else None,
    }


def _combinations_by_symbol(
    rows: list[dict[str, Any]], cfg: AppConfig
) -> dict[str, list[dict[str, Any]]]:
    """Group variant rows per symbol, in the order variants are generated (size, then name)."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = row.get("symbol")
        if symbol is None or cfg.asset(symbol) is None:
            continue
        grouped[symbol].append(_combination_entry(row))
    for entries in grouped.values():
        entries.sort(key=lambda e: (e["n_components"], e["strategy"]))
    return grouped


def _fetch_combinations(
    conn: psycopg.Connection, cfg: AppConfig, runs: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    """Every evaluated variant per symbol, preferring the dbt mart, falling back to the runs.

    One query, whatever the number of assets or variants. When the mart is missing (fresh database,
    or dbt has not run since the migration) the latest-run rows already in memory carry the same
    columns, so the export still ships the combinations instead of an empty array.
    """
    if _relation_exists(conn, _COMBINATION_RELATION):
        # Constant relation name, not user input.
        rows = _rows(conn, f"SELECT * FROM {_COMBINATION_RELATION}")
        if rows and "symbol" in rows[0] and "strategy" in rows[0]:
            return _combinations_by_symbol(rows, cfg)
        logger.warning(
            "%s is empty or lacks the (symbol, strategy) grain — deriving combinations from "
            "gold.backtest_runs instead", _COMBINATION_RELATION,
        )
    else:
        logger.warning(
            "%s not found — deriving combinations from gold.backtest_runs", _COMBINATION_RELATION
        )
    return _combinations_by_symbol(runs, cfg)


def _shape_overfitting(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Split the overfitting mart into its per-size rows and its single aggregate row."""
    by_size, overall = [], None
    for row in rows:
        is_aggregate = (
            row.get("is_grand_total") is True
            or ("n_components" in row and row["n_components"] is None)
            or any(isinstance(v, str) and v.lower() in _OVERALL_LABELS for v in row.values())
        )
        if is_aggregate and overall is None:
            overall = row
        else:
            by_size.append(row)
    by_size.sort(key=lambda r: (r.get("n_components") is None, r.get("n_components") or 0))
    return {"by_n_components": by_size, "overall": overall}


def _fetch_fx_decomposition(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Read the ADR FX-decomposition mart, tolerating its absence like the other dbt marts."""
    if not _relation_exists(conn, _FX_DECOMPOSITION_RELATION):
        logger.warning("%s not found — exporting an empty fx_decomposition",
                       _FX_DECOMPOSITION_RELATION)
        return []
    return _rows(conn, _FX_DECOMPOSITION_SQL)  # constant relation name, not user input


def _fetch_overfitting(conn: psycopg.Connection) -> dict[str, Any] | None:
    """Read the dbt-owned overfitting mart, tolerating its absence exactly like the leaderboard."""
    if not _relation_exists(conn, _OVERFITTING_RELATION):
        logger.warning("%s not found — exporting a null overfitting summary", _OVERFITTING_RELATION)
        return None
    # Constant relation name, not user input.
    return _shape_overfitting(_rows(conn, f"SELECT * FROM {_OVERFITTING_RELATION}"))


def _rank(entry: dict[str, Any]) -> tuple[bool, float]:
    """Sort key: best out-of-sample excess first, variants without one last."""
    value = entry.get(_COMBINATION_RANK_KEY)
    return (value is None, -value if value is not None else 0.0)


def _dump_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=_json_default, separators=(",", ":"))


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return _write_text(path, _dump_json(payload))


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

        # One query for the latest run per (symbol, strategy), one for all their curves. Only
        # single strategies keep a curve, so only those run ids are asked for.
        runs = [r for r in _rows(conn, _LATEST_RUNS_SQL) if cfg.asset(r["symbol"]) is not None]
        curves = _fetch_curves(
            conn, [r["backtest_run_id"] for r in runs if r.get("has_curve", True)]
        )

        combinations = _fetch_combinations(conn, cfg, runs)
        overfitting = _fetch_overfitting(conn)
        leaderboard = _fetch_leaderboard(conn)
        fx_decomposition = _fetch_fx_decomposition(conn)
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
                # ADRs carry their home-currency pair (config.yaml `fx_pair`); null otherwise.
                "fx_pair": a.fx_pair,
                "summary": summaries.get(a.symbol),
                "data_quality": quality.get(a.symbol),
                "reconciliation": recon.get(a.symbol),
                # Unchanged shape: the single strategies with their headline metrics.
                # Filtered to singles on purpose — combinations belong in `combinations`
                # below, and emitting all 1,347 twice is what blew the byte budget.
                "strategies": [
                    _index_strategy(r)
                    for r in runs_by_symbol.get(a.symbol, [])
                    if r.get("strategy_kind", "single") == "single"
                ],
                # Every evaluated variant, curve-free. Possibly truncated to the top few below;
                # `n_combinations` always reports how many were actually evaluated.
                "combinations": list(combinations.get(a.symbol, [])),
                "n_combinations": len(combinations.get(a.symbol, [])),
            }
            for a in cfg.assets
        ],
        "combinations_index": {
            "mode": "full",
            "limit_per_asset": None,
            "ranked_by": _COMBINATION_RANK_KEY,
            "full_detail": f"{cfg.export.per_symbol_dir}/<SYMBOL>.json",
        },
        # The full FX-decomposition mart (10 ADRs x 4 windows = 40 rows, curve-free):
        # small enough to ship whole, so the site never joins windows client-side.
        "fx_decomposition": fx_decomposition,
        "overfitting": overfitting,
        "leaderboard": leaderboard,
        "pipeline": {"recent_ingest_runs": recent_ingest_runs, "totals": totals},
    }

    written: set[Path] = set()
    for asset in cfg.assets:
        symbol_runs = runs_by_symbol.get(asset.symbol)
        if not symbol_runs:
            continue
        # Curves exist for single strategies only, so `backtests` keeps exactly its former
        # contents and the combinations live in their own, curve-free array.
        singles = [r for r in symbol_runs if r.get("strategy_kind", "single") == "single"]
        split_ts = next((r["split_ts"] for r in symbol_runs if r.get("split_ts")), None)
        written.add(
            _write_json(
                cfg.export.symbol_path(asset.symbol),
                {
                    "symbol": asset.symbol,
                    "name": asset.name,
                    "asset_class": asset.asset_class,
                    "region": asset.region,
                    "generated_at": generated_at,
                    # First bar of the out-of-sample window: the boundary a chart draws.
                    "split_ts": split_ts,
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
                        for r in singles
                    ],
                    # Full detail, never truncated: one file renders the asset's whole heatmap.
                    "combinations": combinations.get(asset.symbol, []),
                },
            )
        )

    _prune_stale(cfg.export.index_path.parent / cfg.export.per_symbol_dir, written)

    # Measure, then decide. ~1,347 variants would put the index well past what a static site
    # should load on first paint, so the arrays collapse to the top few by out-of-sample excess
    # and the per-symbol files remain the source of full detail.
    payload = _dump_json(index)
    if len(payload.encode()) > _INDEX_BYTE_BUDGET:
        for entry in index["assets"]:
            entry["combinations"] = sorted(entry["combinations"], key=_rank)[
                :_INDEX_TOP_COMBINATIONS
            ]
        index["combinations_index"] |= {
            "mode": "top_n",
            "limit_per_asset": _INDEX_TOP_COMBINATIONS,
        }
        truncated = _dump_json(index)
        logger.info(
            "index.json %d B over the %d B budget — kept the top %d combinations per asset (%d B)",
            len(payload.encode()), _INDEX_BYTE_BUDGET, _INDEX_TOP_COMBINATIONS,
            len(truncated.encode()),
        )
        payload = truncated
        if len(payload.encode()) > _INDEX_BYTE_BUDGET:
            # Something other than the combination arrays is now the bulk of the file (the
            # leaderboard is the usual suspect). Say so instead of shipping a silent regression.
            logger.warning(
                "index.json is still %d B after trimming combinations — the %d B budget is being "
                "spent elsewhere in the payload", len(payload.encode()), _INDEX_BYTE_BUDGET,
            )

    path = _write_text(cfg.export.index_path, payload)
    logger.info("wrote %s (%d B) and %d per-symbol file(s)", path, len(payload.encode()),
                len(written))
    return path


if __name__ == "__main__":
    print(export_json(load_config()))
