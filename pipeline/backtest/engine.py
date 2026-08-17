"""Backtest engine: honest next-open execution with fees and slippage.

Execution model:
- A position change signaled at bar ``t`` fills at bar ``t + 1``'s open.
- Buys fill at ``open * (1 + slippage)``; sells fill at ``open * (1 - slippage)``.
- Fees apply to traded notional on each side; sizing is all-in.
- Equity is marked to market at every close.
- A signal on the final bar never executes (there is no next open).

``run_backtest`` simulates the whole series. ``run_backtest_windows`` adds the
train/validation split: the same bars, cut once into two disjoint windows that
together cover the series, each simulated from scratch. With ~1,347 variants
evaluated per run, a variant that only looks good in-sample has to say so.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from pipeline.backtest.metrics import (
    cagr,
    exposure,
    max_drawdown,
    n_trades,
    sharpe,
    total_return,
    win_rate,
)
from pipeline.backtest.strategies import Strategy

REQUIRED_COLUMNS = ("ts", "open", "close")

# Shortest window that gets metrics at all. Below roughly a trading month the
# Sharpe, the drawdown and the trade count describe noise, so the window reports
# None instead of a number a reader would take at face value.
MIN_WINDOW_BARS = 30


@dataclass(frozen=True)
class Trade:
    """One completed round trip; fills include slippage, the return includes fees."""

    entry_ts: pd.Timestamp
    exit_ts: pd.Timestamp
    entry_fill: float
    exit_fill: float
    return_pct: float


@dataclass
class BacktestResult:
    """Metrics, per-bar equity curve (vs buy & hold), and completed round trips."""

    metrics: dict[str, float | int | None]
    equity_curve: pd.DataFrame
    trades: list[Trade]


@dataclass(frozen=True)
class WindowedResult:
    """One variant scored over the full period and over a train/validation split.

    ``full`` is the only run that carries an equity curve. ``is_metrics`` and
    ``oos_metrics`` are None when their window holds fewer than
    ``MIN_WINDOW_BARS`` bars. ``split_ts`` is the first timestamp of the
    out-of-sample window, and is None only when that window is empty.
    """

    full: BacktestResult
    is_metrics: dict[str, float | int | None] | None
    oos_metrics: dict[str, float | int | None] | None
    split_ts: pd.Timestamp | None
    is_bars: int
    oos_bars: int

    @property
    def metrics(self) -> dict[str, float | int | None]:
        """Full-period metrics (the windows are deliberately curve-less)."""
        return self.full.metrics

    @property
    def equity_curve(self) -> pd.DataFrame:
        return self.full.equity_curve


def run_backtest(
    df: pd.DataFrame,
    strategy: Strategy,
    *,
    initial_cash: float,
    fee_bps: float,
    slippage_bps: float,
    periods_per_year: int,
) -> BacktestResult:
    """Simulate ``strategy`` over OHLCV bars ``df`` (sorted by ``ts`` internally).

    A position still open on the last bar stays marked to market in the equity
    curve but is not counted as a completed round trip.
    """
    data = _prepare(df)
    return _simulate(
        data,
        _signals(data, strategy),
        initial_cash=initial_cash,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        periods_per_year=periods_per_year,
    )


def run_backtest_windows(
    df: pd.DataFrame,
    strategy: Strategy,
    *,
    initial_cash: float,
    fee_bps: float,
    slippage_bps: float,
    periods_per_year: int,
    train_fraction: float,
) -> WindowedResult:
    """Full-period backtest plus independent in-sample and out-of-sample runs.

    The first ``train_fraction`` of the time-sorted bars is the in-sample window
    and the remainder is out-of-sample: the two are disjoint and together cover
    every bar, with no gap and no overlap. Each window is a genuine backtest of
    its own — it starts flat, with ``initial_cash``, and its metrics are computed
    from its own equity curve — so an out-of-sample figure can never inherit an
    in-sample position or an in-sample profit.

    Signals are generated once, on the whole frame, and then sliced positionally.
    Every indicator here is causal, so signal ``t`` still sees only bars up to
    ``t`` and the in-sample window is bit-for-bit what it would be if the frame
    ended at the split (``test_in_sample_window_ignores_future_bars`` pins this
    down). Recomputing signals inside each window would instead punch an
    artificial warm-up hole right after the split, and would triple the
    indicator work for every one of the ~1,347 variants a daily run evaluates.

    **The split skips the indicator warm-up.** Every indicator is NaN for its
    first ``warmup_bars`` bars and every strategy reads NaN as flat, so those
    bars are forced-flat by construction, not by the strategy's judgement.
    Leaving them inside the in-sample window would hand the whole dead zone to
    one side of the comparison and make the two windows different regimes — the
    in-sample side would look worse purely because it starts cold. The split is
    therefore taken over the bars from ``warmup_bars`` onward, so both windows
    start warm. The full-period run still covers every bar, because that is the
    honest answer to "what would this have returned end to end".

    Note on state: the two windows share one causally computed signal path, so
    the out-of-sample window inherits warm indicators and any latched strategy
    state (RSI's position latch, MACD's EMA state) from the bars before it. That
    is deliberate — it is the real deployment case, where a model does not forget
    everything at midnight on the split date. What the window never inherits is
    capital: each one starts flat with ``initial_cash`` and is measured only on
    its own equity curve.
    """
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("train_fraction must be between 0 and 1 (exclusive)")

    data = _prepare(df)
    sig = _signals(data, strategy)
    sim = {
        "initial_cash": initial_cash,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "periods_per_year": periods_per_year,
    }

    n = len(data)
    warmup = min(max(int(getattr(strategy, "warmup_bars", 0)), 0), n)
    evaluable = n - warmup
    # int() so a numpy-typed train_fraction cannot turn the split into a float index.
    split = warmup + min(max(int(round(evaluable * train_fraction)), 0), evaluable)
    return WindowedResult(
        full=_simulate(data, sig, **sim),
        is_metrics=_window_metrics(data, sig, warmup, split, sim),
        oos_metrics=_window_metrics(data, sig, split, n, sim),
        split_ts=data["ts"].iloc[split] if split < n else None,
        is_bars=split - warmup,
        oos_bars=n - split,
    )


def _window_metrics(
    data: pd.DataFrame, sig: np.ndarray, start: int, stop: int, sim: dict[str, float | int]
) -> dict[str, float | int | None] | None:
    """Metrics for bars ``[start, stop)`` as a standalone run, or None if too short."""
    if stop - start < MIN_WINDOW_BARS:
        return None
    window = data.iloc[start:stop].reset_index(drop=True)
    return _simulate(window, sig[start:stop], **sim).metrics


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Validate the frame and return it sorted by ``ts`` on a fresh 0..n-1 index."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"df is missing required columns: {missing}")
    if df.empty:
        raise ValueError("df has no rows")
    return df.sort_values("ts").reset_index(drop=True)


def _signals(data: pd.DataFrame, strategy: Strategy) -> np.ndarray:
    """Generate and validate the strategy's {0, 1} targets for ``data``."""
    signals = strategy.generate_signals(data)
    if len(signals) != len(data):
        raise ValueError("strategy returned a signal series of mismatched length")
    sig = signals.to_numpy()
    if not np.isin(sig, (0, 1)).all():
        raise ValueError("signals must contain only 0 or 1")
    return sig


def _simulate(
    data: pd.DataFrame,
    sig: np.ndarray,
    *,
    initial_cash: float,
    fee_bps: float,
    slippage_bps: float,
    periods_per_year: int,
) -> BacktestResult:
    """Run the execution model over ``data`` with the given per-bar targets.

    ``data`` must already be sorted with a 0..n-1 index and ``sig`` must be
    positionally aligned with it. The run always starts flat: bar 0 has no
    preceding signal, which is what makes each window of a split independent.
    """
    fee = fee_bps / 1e4
    slip = slippage_bps / 1e4
    opens = data["open"].to_numpy(dtype=float)
    closes = data["close"].to_numpy(dtype=float)
    ts = data["ts"]
    n = len(data)

    cash = float(initial_cash)
    units = 0.0
    entry_ts: pd.Timestamp | None = None
    entry_fill = 0.0
    entry_cost = 0.0
    trades: list[Trade] = []
    equity = np.empty(n, dtype=float)
    positions = np.zeros(n, dtype=np.int8)

    for t in range(n):
        target = int(sig[t - 1]) if t > 0 else 0
        if target == 1 and units == 0.0:
            entry_fill = opens[t] * (1.0 + slip)
            entry_cost = cash
            entry_ts = ts.iloc[t]
            units = cash / (entry_fill * (1.0 + fee))
            cash = 0.0
        elif target == 0 and units > 0.0:
            exit_fill = opens[t] * (1.0 - slip)
            cash = units * exit_fill * (1.0 - fee)
            trades.append(
                Trade(
                    entry_ts=entry_ts,
                    exit_ts=ts.iloc[t],
                    entry_fill=entry_fill,
                    exit_fill=exit_fill,
                    return_pct=cash / entry_cost - 1.0,
                )
            )
            units = 0.0
        positions[t] = 1 if units > 0.0 else 0
        equity[t] = cash + units * closes[t]

    buy_hold = _buy_hold_curve(opens, closes, initial_cash, fee=fee, slip=slip)
    curve = pd.DataFrame({"ts": ts, "equity": equity, "buy_hold_equity": buy_hold})

    equity_series = curve["equity"]
    metrics: dict[str, float | int | None] = {
        "total_return": total_return(equity_series, initial_cash),
        "cagr": cagr(equity_series, initial_cash, periods_per_year),
        "buy_hold_return": float(buy_hold[-1] / initial_cash - 1.0),
        "max_drawdown": max_drawdown(equity_series),
        "sharpe": sharpe(equity_series, periods_per_year),
        "n_trades": n_trades(trades),
        "win_rate": win_rate(trades),
        "exposure": exposure(positions),
    }
    return BacktestResult(metrics=metrics, equity_curve=curve, trades=trades)


def _buy_hold_curve(
    opens: np.ndarray, closes: np.ndarray, initial_cash: float, *, fee: float, slip: float
) -> np.ndarray:
    """All-in buy at the first valid open, paying the same fee and slippage, then hold."""
    curve = np.full(len(opens), float(initial_cash))
    valid = np.flatnonzero(np.isfinite(opens) & (opens > 0.0))
    if valid.size:
        first = int(valid[0])
        units = initial_cash / (opens[first] * (1.0 + slip) * (1.0 + fee))
        curve[first:] = units * closes[first:]
    return curve
