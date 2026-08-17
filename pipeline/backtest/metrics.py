"""Performance metrics for equity curves and completed trades."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pipeline.backtest.engine import Trade


def total_return(equity: pd.Series, initial_value: float) -> float:
    """Final equity over starting capital, minus one."""
    _require_equity(equity, initial_value)
    return float(equity.iloc[-1] / initial_value - 1.0)


def cagr(equity: pd.Series, initial_value: float, periods_per_year: int) -> float | None:
    """Compound annual growth rate; None when the growth ratio is non-positive."""
    _require_equity(equity, initial_value)
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    ratio = float(equity.iloc[-1] / initial_value)
    if ratio <= 0.0:
        return None
    years = len(equity) / periods_per_year
    return float(ratio ** (1.0 / years) - 1.0)


def max_drawdown(equity: pd.Series) -> float:
    """Worst peak-to-trough drop: min of equity / running max - 1 (always <= 0)."""
    if len(equity) == 0:
        raise ValueError("equity curve is empty")
    return float((equity / equity.cummax() - 1.0).min())


#: A long window that moved on only a handful of bars yields a Sharpe that is a
#: function of the window length, not of skill: with one moving bar out of N,
#: mean = r/N and std = r/sqrt(N) exactly, so r cancels and the ratio collapses to
#: sqrt(periods_per_year / N) whatever the size of the move. Past this many bars a
#: window must show at least MIN_NONZERO_RETURNS live returns to be scored.
SPARSE_WINDOW_BARS = 30
MIN_NONZERO_RETURNS = 5


def sharpe(equity: pd.Series, periods_per_year: int) -> float | None:
    """Annualized mean/std of per-bar equity returns.

    None when the sample cannot support the statistic: fewer than two returns, a
    zero or undefined standard deviation, or a long window whose returns are so
    sparse that the ratio would measure the window rather than the strategy.
    """
    returns = equity.pct_change().dropna()
    if len(returns) < 2:
        return None
    nonzero = int((returns != 0).sum())
    if nonzero < 2:
        return None
    if len(returns) >= SPARSE_WINDOW_BARS and nonzero < MIN_NONZERO_RETURNS:
        return None
    std = float(returns.std(ddof=1))
    if std == 0.0 or math.isnan(std):
        return None
    return float(returns.mean() / std) * math.sqrt(periods_per_year)


def exposure(positions: Sequence[float] | pd.Series | np.ndarray) -> float:
    """Fraction of bars held with a non-zero position, in [0, 1].

    This is how an over-filtered combination gives itself away. ANDing five
    signals can produce a spectacular return while being invested 2% of the
    time: that is a handful of lucky bars, not an edge, and no return figure
    says so on its own. Non-finite entries count as flat.
    """
    values = np.asarray(positions, dtype=float)
    if values.size == 0:
        raise ValueError("position series is empty")
    invested = np.isfinite(values) & (values != 0.0)
    return float(invested.sum() / values.size)


def n_trades(trades: Sequence[Trade]) -> int:
    """Number of completed round trips."""
    return len(trades)


def win_rate(trades: Sequence[Trade]) -> float | None:
    """Share of round trips with positive net return; None when there are no trades."""
    if not trades:
        return None
    wins = sum(1 for trade in trades if trade.return_pct > 0.0)
    return wins / len(trades)


def _require_equity(equity: pd.Series, initial_value: float) -> None:
    if len(equity) == 0:
        raise ValueError("equity curve is empty")
    if initial_value <= 0:
        raise ValueError("initial_value must be positive")
