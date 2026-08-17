"""Execution-model and known-answer tests for the backtest engine (synthetic data only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
import pytest

from pipeline.backtest.engine import MIN_WINDOW_BARS, run_backtest, run_backtest_windows
from pipeline.backtest.strategies import build_strategy
from pipeline.config import StrategyConfig

FEE_BPS = 10.0
SLIP_BPS = 5.0
FEE = FEE_BPS / 1e4
SLIP = SLIP_BPS / 1e4
CASH = 10_000.0


@dataclass
class StubStrategy:
    """Fixed signal sequence for exercising the execution model directly.

    ``calls`` counts how often the engine asked for signals: with 1,347 variants a
    windowed run that regenerated them per window would triple the work.
    """

    signals: list[int]
    name: str = "stub"
    params: dict[str, Any] = field(default_factory=dict)
    calls: int = 0

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        self.calls += 1
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


def run_windows(df, strategy, *, train_fraction=0.7, fee_bps=FEE_BPS, slippage_bps=SLIP_BPS,
                ppy=365):
    return run_backtest_windows(
        df,
        strategy,
        initial_cash=CASH,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        periods_per_year=ppy,
        train_fraction=train_fraction,
    )


def uptrend(n: int, g: float = 1.01) -> pd.DataFrame:
    """Bars whose open and close both grow by exactly ``g`` per bar."""
    opens = [100.0 * g**t for t in range(n)]
    return make_df(opens, [o * g for o in opens])


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


# --- exposure -----------------------------------------------------------------


def test_exposure_counts_held_bars_not_signalled_bars():
    # Signals [0,1,1,0,0] execute one bar late, so bars 2 and 3 are the held ones.
    result = run(make_df([100.0] * 5, [100.0] * 5), StubStrategy([0, 1, 1, 0, 0]))
    assert result.metrics["exposure"] == 0.4


def test_exposure_of_an_always_long_run_misses_only_the_first_bar():
    n = 20
    result = run(make_df([100.0] * n, [100.0] * n), StubStrategy([1] * n))
    # Bar 0 has no preceding signal, so the entry lands on bar 1: 19 of 20 bars held.
    assert result.metrics["exposure"] == pytest.approx((n - 1) / n, rel=1e-15)


def test_exposure_is_zero_for_a_strategy_that_never_enters():
    result = run(make_df([100.0] * 10, [100.0] * 10), StubStrategy([0] * 10))
    assert result.metrics["exposure"] == 0.0


def test_exposure_falls_as_an_and_combination_adds_filters():
    # What the metric is for: ANDing filters can only remove exposure, never add it.
    n = 24
    df = make_df([100.0] * n, [100.0] * n)
    wide = [1] * n
    narrow = [1 if t % 2 == 0 else 0 for t in range(n)]
    both = [a & b for a, b in zip(wide, narrow, strict=True)]
    assert (
        run(df, StubStrategy(wide)).metrics["exposure"]
        > run(df, StubStrategy(both)).metrics["exposure"]
        == run(df, StubStrategy(narrow)).metrics["exposure"]
    )


# --- windowed evaluation (train / validation split) ---------------------------


def test_windows_are_disjoint_and_cover_every_bar():
    n = 100
    df = uptrend(n)
    windowed = run_windows(df, StubStrategy([1] * n), train_fraction=0.7)

    assert (windowed.is_bars, windowed.oos_bars) == (70, 30)
    assert windowed.is_bars + windowed.oos_bars == n  # no gap, no overlap
    assert windowed.split_ts == df["ts"].iloc[70]
    # The last in-sample bar is strictly before the first out-of-sample bar.
    assert df["ts"].iloc[windowed.is_bars - 1] < windowed.split_ts


def test_split_position_follows_train_fraction():
    n = 200
    df = uptrend(n)
    for fraction, expected in ((0.5, 100), (0.7, 140), (0.9, 180)):
        windowed = run_windows(df, StubStrategy([1] * n), train_fraction=fraction)
        assert windowed.is_bars == expected
        assert windowed.oos_bars == n - expected
        assert windowed.split_ts == df["ts"].iloc[expected]


def test_each_window_is_an_independent_run_starting_flat():
    """Closed form: a window that starts flat must re-pay entry costs and skip bar 0."""
    n, g = 100, 1.01
    df = uptrend(n, g)
    windowed = run_windows(df, StubStrategy([1] * n), train_fraction=0.7)
    cost_drag = (1.0 + SLIP) * (1.0 + FEE)

    # Both windows enter at their OWN bar 1, so each earns growth over (bars - 1)
    # bars and pays the entry fee and slippage again.
    for metrics, bars in ((windowed.is_metrics, 70), (windowed.oos_metrics, 30)):
        expected = g ** (bars - 1) / cost_drag - 1.0
        assert metrics["total_return"] == pytest.approx(expected, rel=1e-12)
        assert metrics["exposure"] == pytest.approx((bars - 1) / bars, rel=1e-15)
        # Buy & hold inside the window enters at its bar 0's open — one bar earlier.
        assert metrics["buy_hold_return"] == pytest.approx(g**bars / cost_drag - 1.0, rel=1e-12)

    # And the out-of-sample window is genuinely poorer than the full run it sits in:
    # it starts from cash, not from the position the in-sample window ended holding.
    assert windowed.oos_metrics["total_return"] < windowed.metrics["total_return"]


def test_out_of_sample_window_does_not_inherit_in_sample_profit():
    n = 100
    df = uptrend(n, 1.02)
    windowed = run_windows(df, StubStrategy([1] * n), train_fraction=0.7)
    # Compounding the two windows must not reproduce the full period: each restarts
    # from initial_cash, and the boundary bar is sat out.
    chained = (1 + windowed.is_metrics["total_return"]) * (
        1 + windowed.oos_metrics["total_return"]
    ) - 1
    assert chained != pytest.approx(windowed.metrics["total_return"], rel=1e-6)


def test_in_sample_window_ignores_future_bars():
    """The in-sample window equals a standalone backtest of exactly its own bars.

    The window starts after the strategy's warm-up: those bars are forced flat by
    NaN indicators rather than by the strategy's judgement, and leaving them on one
    side of the split would make the two windows different regimes.
    """
    n = 120
    closes = 100.0 + 10.0 * np.sin(np.arange(n) / 5.0)
    df = make_df(closes, closes)
    strategy = build_strategy(StrategyConfig("sma_cross", {"fast": 3, "slow": 8}))
    warmup = strategy.warmup_bars

    windowed = run_windows(df, strategy, train_fraction=0.7)
    # Signals stay causal on the full frame, so the slice reproduces the window only
    # when it is handed the same already-warm signals rather than recomputing cold.
    signals = strategy.generate_signals(df).tolist()
    window = df.iloc[warmup : warmup + windowed.is_bars].reset_index(drop=True)
    truncated = run(window, StubStrategy(signals[warmup : warmup + windowed.is_bars]))

    assert windowed.is_bars == round((n - warmup) * 0.7)
    assert windowed.is_metrics["n_trades"] >= 2  # the window actually traded
    assert windowed.is_metrics == truncated.metrics


def test_out_of_sample_window_covers_exactly_the_tail():
    n = 120
    closes = 100.0 + 10.0 * np.sin(np.arange(n) / 5.0)
    df = make_df(closes, closes)
    windowed = run_windows(df, StubStrategy([1] * n), train_fraction=0.7)

    tail = df.iloc[windowed.is_bars :].reset_index(drop=True)
    assert len(tail) == windowed.oos_bars
    assert tail["ts"].iloc[0] == windowed.split_ts
    assert tail["ts"].iloc[-1] == df["ts"].iloc[-1]


def test_short_window_yields_none_rather_than_a_fabricated_number():
    n = 40
    df = uptrend(n)

    # 10% of 40 bars is 4 in-sample bars: too few to say anything.
    lopsided_early = run_windows(df, StubStrategy([1] * n), train_fraction=0.1)
    assert lopsided_early.is_metrics is None
    assert lopsided_early.oos_metrics is not None

    # 90% leaves 4 out-of-sample bars, the case that matters most.
    lopsided_late = run_windows(df, StubStrategy([1] * n), train_fraction=0.9)
    assert lopsided_late.oos_metrics is None
    assert lopsided_late.is_metrics is not None

    # A short series has nothing to validate on at all, in either window.
    both_short = run_windows(uptrend(40), StubStrategy([1] * 40), train_fraction=0.5)
    assert both_short.is_metrics is None and both_short.oos_metrics is None
    # The split itself is still reported: the windows exist, their metrics do not.
    assert both_short.split_ts is not None
    assert both_short.is_bars + both_short.oos_bars == 40


def test_series_too_short_to_split_leaves_no_out_of_sample_window():
    # 3 bars at 0.9: the split rounds onto the end of the series, so there is no
    # out-of-sample bar at all — and therefore no split timestamp to report.
    windowed = run_windows(uptrend(3), StubStrategy([1, 1, 1]), train_fraction=0.9)
    assert (windowed.is_bars, windowed.oos_bars) == (3, 0)
    assert windowed.split_ts is None
    assert windowed.is_metrics is None and windowed.oos_metrics is None
    assert windowed.metrics["total_return"] is not None  # the full run still stands


def test_numpy_train_fraction_still_indexes_cleanly():
    n = 100
    windowed = run_windows(uptrend(n), StubStrategy([1] * n), train_fraction=np.float64(0.7))
    assert (windowed.is_bars, windowed.oos_bars) == (70, 30)


def test_window_of_exactly_the_minimum_length_is_measured():
    n = 100
    windowed = run_windows(uptrend(n), StubStrategy([1] * n), train_fraction=0.7)
    assert windowed.oos_bars == MIN_WINDOW_BARS
    assert windowed.oos_metrics is not None
    assert windowed.oos_metrics["total_return"] is not None


def test_full_period_result_is_identical_to_run_backtest():
    n = 90
    closes = 100.0 + 10.0 * np.sin(np.arange(n) / 5.0)
    df = make_df(closes, closes)
    strategy = build_strategy(StrategyConfig("sma_cross", {"fast": 3, "slow": 8}))

    windowed = run_windows(df, strategy, train_fraction=0.7)
    plain = run(df, strategy)
    assert windowed.metrics == plain.metrics
    pd.testing.assert_frame_equal(windowed.equity_curve, plain.equity_curve)
    assert windowed.full.trades == plain.trades


def test_windowed_run_generates_signals_exactly_once():
    n = 100
    strategy = StubStrategy([1] * n)
    run_windows(uptrend(n), strategy, train_fraction=0.7)
    assert strategy.calls == 1


def test_windowed_run_sorts_before_splitting():
    n = 100
    df = uptrend(n)
    strategy = build_strategy(StrategyConfig("sma_cross", {"fast": 3, "slow": 8}))
    ordered = run_windows(df, strategy)
    shuffled = run_windows(df.iloc[::-1].reset_index(drop=True), strategy)

    assert ordered.split_ts == shuffled.split_ts
    assert ordered.is_metrics == shuffled.is_metrics
    assert ordered.oos_metrics == shuffled.oos_metrics


@pytest.mark.parametrize("fraction", [0.0, 1.0, -0.1, 1.5])
def test_train_fraction_must_be_a_proper_fraction(fraction):
    with pytest.raises(ValueError, match="train_fraction"):
        run_windows(uptrend(50), StubStrategy([1] * 50), train_fraction=fraction)


def test_windowed_run_keeps_the_engine_guards():
    with pytest.raises(ValueError, match="no rows"):
        run_windows(make_df([], []), StubStrategy([]))
    with pytest.raises(ValueError, match="missing required columns"):
        run_windows(make_df([100.0] * 50, [100.0] * 50).drop(columns=["open"]),
                    StubStrategy([0] * 50))
    with pytest.raises(ValueError, match="signals"):
        run_windows(make_df([100.0] * 50, [100.0] * 50), StubStrategy([2] * 50))
