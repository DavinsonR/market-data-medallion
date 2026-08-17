"""Backtest engine: honest next-open execution with fees and slippage.

Execution model:
- A position change signaled at bar ``t`` fills at bar ``t + 1``'s open.
- Buys fill at ``open * (1 + slippage)``; sells fill at ``open * (1 - slippage)``.
- Fees apply to traded notional on each side; sizing is all-in.
- Equity is marked to market at every close.
- A signal on the final bar never executes (there is no next open).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from pipeline.backtest.metrics import cagr, max_drawdown, n_trades, sharpe, total_return, win_rate
from pipeline.backtest.strategies import Strategy

REQUIRED_COLUMNS = ("ts", "open", "close")


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
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"df is missing required columns: {missing}")
    if df.empty:
        raise ValueError("df has no rows")

    data = df.sort_values("ts").reset_index(drop=True)
    signals = strategy.generate_signals(data)
    if len(signals) != len(data):
        raise ValueError("strategy returned a signal series of mismatched length")
    sig = signals.to_numpy()
    if not np.isin(sig, (0, 1)).all():
        raise ValueError("signals must contain only 0 or 1")

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
