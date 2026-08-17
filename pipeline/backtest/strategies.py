"""Trading strategies: vectorized {0, 1} target-position generators.

Each strategy emits, per bar, the exposure desired AFTER that bar's close.
The engine executes any position change at the NEXT bar's open, so a signal
can never act on information from its own execution bar. Signals are NaN-safe:
the target position is 0 wherever an indicator is not yet defined.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol

import numpy as np
import pandas as pd

from pipeline.config import StrategyConfig


class Strategy(Protocol):
    """Signal-generator contract used by the backtest engine."""

    name: str
    params: dict[str, Any]

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """Return the target position {0, 1} per bar, aligned with ``df``."""
        ...


def sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average; NaN until the window is full (no partial averages)."""
    return series.rolling(window, min_periods=window).mean()


def cutlers_rsi(close: pd.Series, period: int) -> pd.Series:
    """Cutler's RSI (SMA of gains/losses); NaN until ``period + 1`` bars exist.

    Uses RSI = 100 * avg_gain / (avg_gain + avg_loss): 100 when losses are
    zero, 0 when gains are zero, NaN when the window is completely flat.
    """
    delta = close.diff()
    avg_gain = sma(delta.clip(lower=0.0), period)
    avg_loss = sma((-delta).clip(lower=0.0), period)
    return 100.0 * avg_gain / (avg_gain + avg_loss)


def _sma_of_close(df: pd.DataFrame, window: int) -> pd.Series:
    """Reuse a gold ``sma_<window>`` column when the window matches; else compute from close."""
    column = f"sma_{window}"
    if column in df.columns:
        return df[column]
    return sma(df["close"], window)


@dataclass(frozen=True)
class SmaCross:
    """Long while SMA(fast) > SMA(slow); flat otherwise."""

    fast: int
    slow: int

    name = "sma_cross"

    def __post_init__(self) -> None:
        if not 0 < self.fast < self.slow:
            raise ValueError("sma_cross requires 0 < fast < slow")

    @property
    def params(self) -> dict[str, Any]:
        return asdict(self)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        return (_sma_of_close(df, self.fast) > _sma_of_close(df, self.slow)).astype(int)


@dataclass(frozen=True)
class Macd:
    """Long while the MACD line is above its signal line (EMAs with adjust=False)."""

    fast: int
    slow: int
    signal: int

    name = "macd"

    def __post_init__(self) -> None:
        if not 0 < self.fast < self.slow:
            raise ValueError("macd requires 0 < fast < slow")
        if self.signal < 1:
            raise ValueError("macd requires signal >= 1")

    @property
    def params(self) -> dict[str, Any]:
        return asdict(self)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        close = df["close"]
        macd_line = (
            close.ewm(span=self.fast, adjust=False).mean()
            - close.ewm(span=self.slow, adjust=False).mean()
        )
        signal_line = macd_line.ewm(span=self.signal, adjust=False).mean()
        return (macd_line > signal_line).astype(int)


@dataclass(frozen=True)
class RsiReversion:
    """Stateful long/flat: enter when RSI < entry_below, exit when RSI > exit_above."""

    period: int
    entry_below: float
    exit_above: float

    name = "rsi_reversion"

    def __post_init__(self) -> None:
        if self.period < 1:
            raise ValueError("rsi_reversion requires period >= 1")
        if not self.entry_below < self.exit_above:
            raise ValueError("rsi_reversion requires entry_below < exit_above")

    @property
    def params(self) -> dict[str, Any]:
        return asdict(self)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        rsi = cutlers_rsi(df["close"], self.period).to_numpy()
        out = np.zeros(len(rsi), dtype=np.int64)
        position = 0
        for i, value in enumerate(rsi):
            if position == 0 and value < self.entry_below:  # NaN compares False: stays flat
                position = 1
            elif position == 1 and value > self.exit_above:
                position = 0
            out[i] = position
        return pd.Series(out, index=df.index)


@dataclass(frozen=True)
class VolumeBreakout:
    """Long while close breaks its SMA on above-average volume; flat otherwise."""

    price_window: int
    volume_window: int
    volume_mult: float

    name = "volume_breakout"

    def __post_init__(self) -> None:
        if self.price_window < 1 or self.volume_window < 1:
            raise ValueError("volume_breakout windows must be >= 1")
        if self.volume_mult <= 0:
            raise ValueError("volume_breakout requires volume_mult > 0")

    @property
    def params(self) -> dict[str, Any]:
        return asdict(self)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        above_price = df["close"] > sma(df["close"], self.price_window)
        above_volume = df["volume"] > self.volume_mult * sma(df["volume"], self.volume_window)
        return (above_price & above_volume).astype(int)


_REGISTRY: dict[str, type] = {
    "sma_cross": SmaCross,
    "macd": Macd,
    "rsi_reversion": RsiReversion,
    "volume_breakout": VolumeBreakout,
}


def build_strategy(cfg: StrategyConfig) -> Strategy:
    """Instantiate a strategy from configuration; raise ValueError for unknown names."""
    try:
        cls = _REGISTRY[cfg.name]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"Unknown strategy {cfg.name!r}; known strategies: {known}") from None
    return cls(**cfg.params)
