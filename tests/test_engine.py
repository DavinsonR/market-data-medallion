"""Execution-model and known-answer tests for the backtest engine (synthetic data only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
import pytest

from pipeline.backtest.engine import run_backtest
from pipeline.backtest.strategies import build_strategy
from pipeline.config import StrategyConfig

FEE_BPS = 10.0
SLIP_BPS = 5.0
FEE = FEE_BPS / 1e4
SLIP = SLIP_BPS / 1e4
CASH = 10_000.0


@dataclass
class StubStrategy:
    """Fixed signal sequence for exercising the execution model directly."""

    signals: list[int]
    name: str = "stub"
    params: dict[str, Any] = field(default_factory=dict)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        return pd.Series(self.signals[: len(df)], index=df.index, dtype=int)


def make_df(opens, closes) -> pd.DataFrame:
    opens = np.asarray(opens, dtype=float)
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {
            "ts": pd.date_range("2024-01-01", periods=len(opens), freq="D", tz="UTC"),
            "open": opens,
            "high": np.maximum(opens, closes) * 1.01,
            "low": np.minimum(opens, closes) * 0.99,
            "close": closes,
            "volume": np.full(len(opens), 1_000.0),
        }
    )


def run(df, strategy, *, fee_bps=FEE_BPS, slippage_bps=SLIP_BPS, ppy=365):
    return run_backtest(
        df,
        strategy,
        initial_cash=CASH,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        periods_per_year=ppy,
    )


def test_constant_price_produces_no_trades_and_zero_return():
    n = 30
    df = make_df([100.0] * n, [100.0] * n)
    strategy = build_strategy(StrategyConfig("sma_cross", {"fast": 3, "slow": 8}))
    result = run(df, strategy)

    assert result.metrics["n_trades"] == 0
    assert result.trades == []
    assert result.metrics["total_return"] == 0.0
    assert result.metrics["cagr"] == 0.0
    assert result.metrics["max_drawdown"] == 0.0
    assert result.metrics["sharpe"] is None
    assert result.metrics["win_rate"] is None
    assert (result.equity_curve["equity"] == CASH).all()
    # Buy & hold still pays entry fee + slippage on a flat market.
    expected_bh = 1.0 / ((1.0 + SLIP) * (1.0 + FEE)) - 1.0
    assert result.metrics["buy_hold_return"] == pytest.approx(expected_bh, rel=1e-12)


def test_uptrend_always_long_compounds_to_closed_form():
    n, g = 12, 1.01
    opens = [100.0 * g**t for t in range(n)]
    closes = [o * g for o in opens]
    result = run(make_df(opens, closes), StubStrategy([1] * n))

    # Entry at bar 1's open; equity_t = CASH * g**t / ((1 + slip)(1 + fee)) for t >= 1.
    cost_drag = (1.0 + SLIP) * (1.0 + FEE)
    expected = [CASH] + [CASH * g**t / cost_drag for t in range(1, n)]
    np.testing.assert_allclose(result.equity_curve["equity"].to_numpy(), expected, rtol=1e-12)
    expected_total = g ** (n - 1) / cost_drag - 1.0
    assert result.metrics["total_return"] == pytest.approx(expected_total, rel=1e-12)
    # CAGR closed form: ratio ** (periods_per_year / n_bars) - 1.
    expected_cagr = (1.0 + expected_total) ** (365.0 / n) - 1.0
    assert result.metrics["cagr"] == pytest.approx(expected_cagr, rel=1e-12)
    # The position never closes, so there are no completed round trips.
    assert result.metrics["n_trades"] == 0
    assert result.metrics["win_rate"] is None


def test_uptrend_without_costs_is_pure_compounding():
    n, g = 10, 1.02
    opens = [100.0 * g**t for t in range(n)]
    closes = [o * g for o in opens]
    result = run(make_df(opens, closes), StubStrategy([1] * n), fee_bps=0.0, slippage_bps=0.0)
    assert result.metrics["total_return"] == pytest.approx(g ** (n - 1) - 1.0, rel=1e-12)


def test_round_trip_fills_fees_and_trade_accounting():
    opens = [90.0, 100.0, 110.0, 120.0, 130.0]
    closes = [95.0, 105.0, 115.0, 125.0, 135.0]
    result = run(make_df(opens, closes), StubStrategy([1, 1, 0, 0, 0]))

    entry_fill = 100.0 * (1.0 + SLIP)  # bar 1's open, NOT bar 0's (next-open execution)
    units = CASH / (entry_fill * (1.0 + FEE))
    exit_fill = 120.0 * (1.0 - SLIP)  # flat signal at bar 2 fills at bar 3's open
    proceeds = units * exit_fill * (1.0 - FEE)

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.entry_ts == pd.Timestamp("2024-01-02", tz="UTC")
    assert trade.exit_ts == pd.Timestamp("2024-01-04", tz="UTC")
    assert trade.entry_fill == pytest.approx(entry_fill, rel=1e-15)
    assert trade.exit_fill == pytest.approx(exit_fill, rel=1e-15)
    assert trade.return_pct == pytest.approx(proceeds / CASH - 1.0, rel=1e-12)

    expected_equity = [CASH, units * 105.0, units * 115.0, proceeds, proceeds]
    np.testing.assert_allclose(
        result.equity_curve["equity"].to_numpy(), expected_equity, rtol=1e-12
    )
    assert result.metrics["n_trades"] == 1
    assert result.metrics["win_rate"] == 1.0

    bh_units = CASH / (90.0 * (1.0 + SLIP) * (1.0 + FEE))
    np.testing.assert_allclose(
        result.equity_curve["buy_hold_equity"].to_numpy(),
        [bh_units * c for c in closes],
        rtol=1e-12,
    )


def test_signal_on_final_bar_never_executes():
    df = make_df([100.0] * 5, [100.0] * 5)
    result = run(df, StubStrategy([0, 0, 0, 0, 1]))
    assert result.metrics["n_trades"] == 0
    assert result.trades == []
    assert (result.equity_curve["equity"] == CASH).all()


def test_higher_fees_strictly_lower_returns_for_active_strategy():
    n = 31
    opens = [100.0 * 1.01**t for t in range(n)]
    closes = [o * 1.005 for o in opens]
    df = make_df(opens, closes)
    strategy = StubStrategy([1, 0] * ((n + 1) // 2))

    totals = []
    for fee_bps in (0.0, 20.0, 50.0):
        result = run(df, strategy, fee_bps=fee_bps, slippage_bps=0.0)
        assert result.metrics["n_trades"] >= 5
        totals.append(result.metrics["total_return"])
    assert totals[0] > totals[1] > totals[2]


def test_higher_slippage_strictly_lowers_returns_for_active_strategy():
    n = 31
    opens = [100.0 * 1.01**t for t in range(n)]
    closes = [o * 1.005 for o in opens]
    df = make_df(opens, closes)
    strategy = StubStrategy([1, 0] * ((n + 1) // 2))

    totals = []
    for slippage_bps in (0.0, 20.0, 50.0):
        result = run(df, strategy, fee_bps=0.0, slippage_bps=slippage_bps)
        assert result.metrics["n_trades"] >= 5
        totals.append(result.metrics["total_return"])
    assert totals[0] > totals[1] > totals[2]


def test_buy_hold_enters_at_first_valid_open():
    opens = [np.nan, 100.0, 110.0, 120.0]
    closes = [100.0, 105.0, 115.0, 125.0]
    result = run(make_df(opens, closes), StubStrategy([0, 0, 0, 0]))
    buy_hold = result.equity_curve["buy_hold_equity"].to_numpy()
    assert buy_hold[0] == CASH
    units = CASH / (100.0 * (1.0 + SLIP) * (1.0 + FEE))
    np.testing.assert_allclose(buy_hold[1:], units * np.asarray(closes[1:]), rtol=1e-12)


def test_engine_sorts_rows_by_ts():
    n = 40
    closes = [100.0 + (t if t < 20 else 40.0 - t) for t in range(n)]
    df = make_df(closes, closes)
    strategy = build_strategy(StrategyConfig("sma_cross", {"fast": 3, "slow": 8}))

    ordered = run(df, strategy)
    reversed_rows = run(df.iloc[::-1].reset_index(drop=True), strategy)
    assert ordered.metrics["n_trades"] >= 1  # rise-then-fall forces at least one round trip
    assert ordered.metrics == reversed_rows.metrics
    pd.testing.assert_frame_equal(ordered.equity_curve, reversed_rows.equity_curve)


def test_empty_df_raises():
    with pytest.raises(ValueError, match="no rows"):
        run(make_df([], []), StubStrategy([]))


def test_missing_column_raises():
    df = make_df([100.0], [100.0]).drop(columns=["open"])
    with pytest.raises(ValueError, match="missing required columns"):
        run(df, StubStrategy([0]))


def test_invalid_signal_values_raise():
    df = make_df([100.0] * 3, [100.0] * 3)
    with pytest.raises(ValueError, match="signals"):
        run(df, StubStrategy([0, 2, 0]))
