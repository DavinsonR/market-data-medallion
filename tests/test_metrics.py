"""Known-answer tests for performance metrics (closed-form expectations)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from pipeline.backtest.engine import Trade
from pipeline.backtest.metrics import (
    cagr,
    exposure,
    max_drawdown,
    n_trades,
    sharpe,
    total_return,
    win_rate,
)


def _trade(return_pct: float) -> Trade:
    ts = pd.Timestamp("2024-01-01", tz="UTC")
    return Trade(entry_ts=ts, exit_ts=ts, entry_fill=100.0, exit_fill=100.0, return_pct=return_pct)


def test_total_return_exact():
    assert total_return(pd.Series([100.0, 130.0, 150.0]), 100.0) == 0.5


def test_total_return_flat_is_zero():
    assert total_return(pd.Series([100.0] * 10), 100.0) == 0.0


def test_max_drawdown_exact_closed_form():
    equity = pd.Series([100.0, 200.0, 50.0, 150.0, 120.0])
    # Peak 200 -> trough 50: 50 / 200 - 1 = -0.75, exactly representable.
    assert max_drawdown(equity) == -0.75


def test_max_drawdown_zero_for_monotonic_rise():
    assert max_drawdown(pd.Series([100.0, 101.0, 105.0, 110.0])) == 0.0


def test_max_drawdown_uses_running_peak_not_global_peak():
    equity = pd.Series([100.0, 80.0, 160.0, 120.0, 200.0])
    # Candidates: 80/100 - 1 = -0.2 and 120/160 - 1 = -0.25; the later one is worse.
    assert max_drawdown(equity) == -0.25


def test_cagr_two_years_closed_form():
    equity = pd.Series(np.linspace(100.0, 121.0, 730))
    # 730 bars at 365/year = 2 years; ratio 1.21 -> sqrt(1.21) - 1 = 0.10.
    assert cagr(equity, 100.0, 365) == pytest.approx(0.10, rel=1e-12)


def test_cagr_flat_is_zero():
    assert cagr(pd.Series([100.0] * 365), 100.0, 365) == 0.0


def test_cagr_none_when_equity_wiped_out():
    assert cagr(pd.Series([100.0, 0.0]), 100.0, 365) is None


def test_sharpe_closed_form():
    # Per-bar returns 1% then 3%: mean 0.02, sample std sqrt(2e-4).
    equity = pd.Series([100.0, 100.0 * 1.01, 100.0 * 1.01 * 1.03])
    expected = 0.02 / math.sqrt(2e-4) * math.sqrt(252)
    assert sharpe(equity, 252) == pytest.approx(expected, rel=1e-9)


def test_sharpe_none_when_flat():
    assert sharpe(pd.Series([100.0] * 20), 252) is None


def test_sharpe_none_with_fewer_than_two_returns():
    assert sharpe(pd.Series([100.0, 110.0]), 252) is None
    assert sharpe(pd.Series([100.0]), 252) is None


def test_exposure_fully_invested_is_one():
    assert exposure([1, 1, 1, 1]) == 1.0


def test_exposure_never_invested_is_zero():
    assert exposure([0] * 250) == 0.0


def test_exposure_counts_bars_not_trades():
    # Ten bars, three of them held — in two separate spells, which must not matter.
    assert exposure([0, 1, 1, 0, 0, 0, 1, 0, 0, 0]) == 0.3


def test_exposure_matches_an_over_filtered_combination():
    # The case the metric exists for: 2 invested bars out of 500.
    positions = [0] * 500
    positions[100] = positions[101] = 1
    assert exposure(positions) == pytest.approx(0.004, rel=1e-15)


def test_exposure_accepts_a_series_and_an_array():
    assert exposure(pd.Series([0, 1, 1, 1])) == 0.75
    assert exposure(np.array([0, 0, 1, 1], dtype=np.int8)) == 0.5


def test_exposure_counts_any_non_zero_position():
    # Fractional sizing is not used today, but a half position is still exposure.
    assert exposure([0.0, 0.5, 1.0, 0.0]) == 0.5


def test_exposure_treats_nan_as_flat():
    # A NaN is "no position recorded", not "invested"; np.count_nonzero would disagree.
    assert exposure([float("nan"), 1.0, 0.0, 1.0]) == 0.5


def test_exposure_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        exposure([])


def test_win_rate_and_n_trades():
    trades = [_trade(0.10), _trade(-0.05), _trade(0.20)]
    assert n_trades(trades) == 3
    assert win_rate(trades) == pytest.approx(2 / 3, rel=1e-15)


def test_win_rate_none_without_trades():
    assert n_trades([]) == 0
    assert win_rate([]) is None


def test_zero_return_trade_is_not_a_win():
    assert win_rate([_trade(0.0)]) == 0.0


def test_empty_equity_curve_raises():
    empty = pd.Series([], dtype=float)
    with pytest.raises(ValueError):
        total_return(empty, 100.0)
    with pytest.raises(ValueError):
        max_drawdown(empty)
    with pytest.raises(ValueError):
        cagr(empty, 100.0, 365)
