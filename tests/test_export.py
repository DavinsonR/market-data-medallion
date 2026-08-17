"""Known-answer tests for the export's read/serialize path (no database).

These cover the parts that turn database rows into JSON: the combination entry, the overfitting
split, the ranking used by the index budget fallback, and the serializer itself.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pipeline.export import (
    _combination_entry,
    _dump_json,
    _rank,
    _shape_overfitting,
)


def _run_row(**overrides):
    """A gold.backtest_runs-shaped row: no excess returns, no beat_* booleans."""
    row = {
        "symbol": "SPY",
        "strategy": "macd+sma_cross",
        "strategy_kind": "combo",
        "n_components": 2,
        "exposure": Decimal("0.4736842105263158"),
        "total_return": Decimal("0.5"),
        "buy_hold_return": Decimal("0.2"),
        "sharpe": Decimal("0.123456789"),
        "max_drawdown": Decimal("-0.3"),
        "n_trades": 17,
        "is_total_return": Decimal("0.4"),
        "is_buy_hold_return": Decimal("0.1"),
        "oos_total_return": Decimal("0.05"),
        "oos_buy_hold_return": Decimal("0.15"),
    }
    return row | overrides


def test_combination_entry_derives_excess_returns_from_a_runs_row():
    entry = _combination_entry(_run_row())
    assert entry["excess_return"] == 0.3
    assert entry["is_excess_return"] == 0.3
    assert entry["oos_excess_return"] == -0.1
    # Derived, not fabricated: beating buy & hold in-sample and losing out-of-sample is exactly
    # the case the split exists to expose.
    assert entry["beat_bh_full"] is True
    assert entry["beat_bh_oos"] is False


def test_combination_entry_prefers_the_marts_own_columns():
    entry = _combination_entry(
        _run_row(
            excess_return=Decimal("0.31"),
            oos_excess_return=Decimal("-0.11"),
            beat_bh_full=False,
            beat_bh_oos=False,
        )
    )
    assert (entry["excess_return"], entry["oos_excess_return"]) == (0.31, -0.11)
    assert entry["beat_bh_full"] is False


def test_combination_entry_recovers_n_components_from_the_name():
    row = _run_row(strategy="fibonacci+macd+sma_cross")
    del row["n_components"]
    assert _combination_entry(row)["n_components"] == 3
    single = _run_row(strategy="fibonacci")
    del single["n_components"]
    assert _combination_entry(single)["n_components"] == 1


def test_combination_entry_keeps_missing_windows_null():
    """A window too short to measure is NULL, never a zero that reads like a measurement."""
    entry = _combination_entry(
        _run_row(oos_total_return=None, oos_buy_hold_return=None, exposure=None)
    )
    assert entry["oos_excess_return"] is None
    assert entry["beat_bh_oos"] is None
    assert entry["exposure"] is None


def test_combination_entry_is_curve_free_and_rounded():
    entry = _combination_entry(_run_row())
    assert "equity_curve" not in entry
    assert entry["exposure"] == 0.473684
    assert entry["sharpe"] == 0.1235


def test_shape_overfitting_splits_the_grand_total_row():
    rows = [
        {"is_grand_total": False, "n_components": 2, "n_variants": 10},
        {"is_grand_total": True, "n_components": None, "n_variants": 31},
        {"is_grand_total": False, "n_components": 1, "n_variants": 5},
    ]
    shaped = _shape_overfitting(rows)
    assert shaped["overall"]["n_variants"] == 31
    assert [r["n_components"] for r in shaped["by_n_components"]] == [1, 2]


def test_shape_overfitting_without_an_aggregate_row():
    shaped = _shape_overfitting([{"n_components": 1, "n_variants": 5}])
    assert shaped["overall"] is None
    assert len(shaped["by_n_components"]) == 1


def test_rank_puts_the_best_out_of_sample_first_and_unmeasured_last():
    entries = [
        {"strategy": "a", "oos_excess_return": -0.2},
        {"strategy": "b", "oos_excess_return": None},
        {"strategy": "c", "oos_excess_return": 0.4},
    ]
    assert [e["strategy"] for e in sorted(entries, key=_rank)] == ["c", "a", "b"]


def test_dump_json_serializes_decimals_and_datetimes():
    payload = _dump_json({"d": Decimal("1.5"), "ts": datetime(2024, 1, 2, tzinfo=UTC)})
    assert payload == '{"d":1.5,"ts":"2024-01-02T00:00:00+00:00"}'
