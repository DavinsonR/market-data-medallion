"""Backtesting: strategy signals, honest next-open execution, and performance metrics."""

from pipeline.backtest.engine import BacktestResult, Trade, run_backtest
from pipeline.backtest.metrics import (
    cagr,
    max_drawdown,
    n_trades,
    sharpe,
    total_return,
    win_rate,
)
from pipeline.backtest.strategies import (
    Macd,
    RsiReversion,
    SmaCross,
    Strategy,
    VolumeBreakout,
    build_strategy,
    cutlers_rsi,
)

__all__ = [
    "BacktestResult",
    "Macd",
    "RsiReversion",
    "SmaCross",
    "Strategy",
    "Trade",
    "VolumeBreakout",
    "build_strategy",
    "cagr",
    "cutlers_rsi",
    "max_drawdown",
    "n_trades",
    "run_backtest",
    "sharpe",
    "total_return",
    "win_rate",
]
