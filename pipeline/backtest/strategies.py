"""Trading strategies: vectorized {0, 1} target-position generators.

Each strategy emits, per bar, the exposure desired AFTER that bar's close.
The engine executes any position change at the NEXT bar's open, so a signal
can never act on information from its own execution bar. Signals are NaN-safe:
the target position is 0 wherever an indicator is not yet defined.

Strategies also combine: ``build_all_variants`` evaluates every configured
strategy once per frame and then ANDs the resulting signal series into every
non-empty combination ("long only while every selected signal is green").
Computing the components once instead of once per combination is what keeps
~1,347 backtests affordable: five signal computations per asset, not eighty.
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
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

    @property
    def warmup_bars(self) -> int:
        return self.slow

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

    @property
    def warmup_bars(self) -> int:
        # adjust=False EMAs are defined from bar 0, but they are still dominated by
        # their seed until roughly one slow span plus one signal span has passed.
        return self.slow + self.signal

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

    @property
    def warmup_bars(self) -> int:
        # period deltas need period + 1 closes.
        return self.period + 1

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

    @property
    def warmup_bars(self) -> int:
        return max(self.price_window, self.volume_window)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        above_price = df["close"] > sma(df["close"], self.price_window)
        above_volume = df["volume"] > self.volume_mult * sma(df["volume"], self.volume_window)
        return (above_price & above_volume).astype(int)


@dataclass(frozen=True)
class FibonacciRetracement:
    """Long while close holds above the ``ratio`` retracement of its recent swing range.

    The swing range is the highest high and the lowest low of the trailing
    ``window`` bars (both known at the current bar's close, so no look-ahead).
    ``level = swing_high - (swing_high - swing_low) * ratio`` is the classic
    Fibonacci retracement of that range: with the default 0.618 the strategy is
    long while price has given back less than 61.8% of its own recent advance.
    """

    window: int
    ratio: float

    name = "fibonacci"

    def __post_init__(self) -> None:
        if self.window < 2:
            raise ValueError("fibonacci requires window >= 2")
        if not 0.0 < self.ratio < 1.0:
            raise ValueError("fibonacci requires 0 < ratio < 1")

    @property
    def params(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def warmup_bars(self) -> int:
        return self.window

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        swing_high = df["high"].rolling(self.window, min_periods=self.window).max()
        swing_low = df["low"].rolling(self.window, min_periods=self.window).min()
        level = swing_high - (swing_high - swing_low) * self.ratio
        # NaN during warm-up: the comparison is False, so the position is 0.
        return (df["close"] > level).astype(int)


_REGISTRY: dict[str, type] = {
    "sma_cross": SmaCross,
    "macd": Macd,
    "rsi_reversion": RsiReversion,
    "volume_breakout": VolumeBreakout,
    "fibonacci": FibonacciRetracement,
}


def build_strategy(cfg: StrategyConfig) -> Strategy:
    """Instantiate a strategy from configuration; raise ValueError for unknown names."""
    try:
        cls = _REGISTRY[cfg.name]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"Unknown strategy {cfg.name!r}; known strategies: {known}") from None
    return cls(**cfg.params)


# --- combinations ------------------------------------------------------------
#
# A combination is named by its components sorted alphabetically and joined by
# '+': "macd+volume_breakout", "fibonacci+macd+sma_cross". That string is the
# `strategy` value everywhere downstream (database, exports, docs), so the
# ordering must be deterministic rather than "whatever order config.yaml used".

COMBINATION_SEPARATOR = "+"


def combination_name(components: Sequence[str]) -> str:
    """Canonical strategy name for a set of components (alphabetical, '+'-joined)."""
    return COMBINATION_SEPARATOR.join(sorted(components))


def _alignment_key(df: pd.DataFrame) -> pd.Index:
    """The labels that identify ``df``'s bars: its timestamps when it has them.

    Precomputed signals are keyed by ``ts`` rather than by row position, because
    the windowed evaluation hands the engine re-indexed *slices* of the frame the
    signals were computed on. A timestamp survives slicing and re-indexing; a row
    number does not.
    """
    return pd.Index(df["ts"]) if "ts" in df.columns else df.index


def _aligned_values(
    component: str, signals: pd.Series, key: pd.Index, index: pd.Index
) -> np.ndarray:
    """Return ``signals`` as a {0, 1} int array positioned on the frame's bars.

    Accepts signals keyed by timestamp (what ``build_all_variants`` produces) or
    already carrying the frame's own index; anything that does not cover every
    bar is an error rather than a silently misaligned backtest. The lookup for a
    window is by timestamp only — ``key is index`` means the frame has no ``ts``
    to match on, and matching row numbers across a re-indexed slice would line up
    the wrong bars.
    """
    if signals.index.equals(key) or signals.index.equals(index):
        aligned = signals
    elif key is not index and len(key) and key.isin(signals.index).all():
        aligned = signals.reindex(key)
    else:
        raise ValueError(
            f"precomputed signals for {component!r} do not align with the frame "
            f"({len(signals)} values for {len(index)} bars)"
        )
    values = aligned.to_numpy()
    if not np.isin(values, (0, 1)).all():
        raise ValueError(f"precomputed signals for {component!r} must contain only 0 or 1")
    return values.astype(np.int64, copy=False)


# eq=False keeps identity semantics: these dataclasses hold pandas Series and
# dicts, for which a generated __eq__/__hash__ would raise instead of comparing.
@dataclass(frozen=True, eq=False)
class PrecomputedStrategy:
    """A single strategy whose signals were already computed for one frame.

    The engine never has to recompute an indicator: ``generate_signals`` only
    positions the stored series on the frame it is handed.
    """

    name: str
    params: dict[str, Any]
    signals: pd.Series
    warmup_bars: int = 0

    @property
    def components(self) -> tuple[str, ...]:
        return (self.name,)

    @property
    def n_components(self) -> int:
        return 1

    @property
    def strategy_kind(self) -> str:
        return "single"

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        values = _aligned_values(self.name, self.signals, _alignment_key(df), df.index)
        return pd.Series(values, index=df.index)


@dataclass(frozen=True, eq=False)
class CompositeStrategy:
    """AND of pre-computed component signals: long only while every one is green.

    It takes signal *series*, not strategies, on purpose. Across the 31 variants
    of five strategies each component appears in 16 of them; letting a composite
    own its strategy objects would recompute the same indicators 80 times per
    asset instead of 5.

    AND (not OR) because the combination is a filter: each extra component can
    only remove exposure, never add it, which is what makes ``exposure`` the
    honest tell for an over-filtered variant.
    """

    components: tuple[str, ...]
    component_signals: tuple[pd.Series, ...]
    params: dict[str, Any]
    name: str = ""
    warmup_bars: int = 0

    def __post_init__(self) -> None:
        if not self.components:
            raise ValueError("a composite strategy needs at least one component")
        if len(self.components) != len(self.component_signals):
            raise ValueError(
                f"{len(self.components)} components but "
                f"{len(self.component_signals)} signal series"
            )
        # Canonicalize the order (carrying the signals along) so `components` can
        # never disagree with `name`: the same set is always one row, one name.
        if list(self.components) != sorted(self.components):
            order = sorted(range(len(self.components)), key=lambda i: self.components[i])
            object.__setattr__(self, "components", tuple(self.components[i] for i in order))
            object.__setattr__(
                self, "component_signals", tuple(self.component_signals[i] for i in order)
            )
        if not self.name:
            object.__setattr__(self, "name", combination_name(self.components))

    @property
    def n_components(self) -> int:
        return len(self.components)

    @property
    def strategy_kind(self) -> str:
        return "single" if len(self.components) == 1 else "combo"

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        key = _alignment_key(df)
        out = np.ones(len(df.index), dtype=np.int64)
        for component, signals in zip(self.components, self.component_signals, strict=True):
            out &= _aligned_values(component, signals, key, df.index)
        return pd.Series(out, index=df.index)


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Order the frame the way ``run_backtest`` will, so the signals line up.

    The engine sorts by ``ts`` and resets the index before asking a strategy for
    its signals; precomputing against the same normalized frame is what makes the
    combinations plain, aligned AND operations.
    """
    if "ts" in df.columns:
        return df.sort_values("ts").reset_index(drop=True)
    return df


def _volume_is_absent(df: pd.DataFrame) -> bool:
    """True when the frame carries no usable volume at all.

    Spot FX is OTC and has no centralized tape, so its ``volume`` column arrives
    entirely NULL and every volume-based indicator would be NaN on every bar.
    This is detected from the data rather than read off ``requires_volume`` +
    asset class on purpose: the reason to drop ``volume_breakout`` is that its
    inputs do not exist in *this frame*, which stays true if a provider silently
    stops reporting volume for an asset that config.yaml still calls an equity.
    """
    if "volume" not in df.columns:
        return True
    values = pd.to_numeric(df["volume"], errors="coerce")
    # All-NaN is how FX arrives; all-zero is how a provider outage arrives. Both
    # mean the same thing here — there is no tape to threshold against.
    return bool(values.isna().all() or (values.fillna(0) == 0).all())


def build_all_variants(
    df: pd.DataFrame, strategy_configs: Sequence[StrategyConfig]
) -> list[Strategy]:
    """Every non-empty AND-combination of ``strategy_configs``, signals computed once.

    Each configured strategy generates its signals exactly once for ``df``; the
    2**k - 1 variants are then built from those series. Variants come back singles
    first, then by increasing component count, alphabetically within each size.

    Configs whose ``requires_volume`` is set are skipped when the frame has no
    volume (see ``_volume_is_absent``), which is how FX drops to 15 variants.
    """
    data = _normalize_frame(df)
    key = _alignment_key(data)
    volume_absent = _volume_is_absent(data)

    signals: dict[str, pd.Series] = {}
    params: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    warmups: dict[str, int] = {}
    for cfg in strategy_configs:
        # Deduplicate first: a repeated config is a configuration error whether or
        # not this frame happens to skip that strategy.
        if cfg.name in seen:
            raise ValueError(f"duplicate strategy configuration for {cfg.name!r}")
        seen.add(cfg.name)
        if cfg.requires_volume and volume_absent:
            continue
        strategy = build_strategy(cfg)
        series = strategy.generate_signals(data)  # the only call per strategy, per frame
        if len(series) != len(data):
            raise ValueError(f"{cfg.name} returned a signal series of mismatched length")
        values = series.to_numpy()
        if not np.isin(values, (0, 1)).all():
            raise ValueError(f"{cfg.name} produced signals outside {{0, 1}}")
        signals[cfg.name] = pd.Series(values.astype(np.int64, copy=False), index=key)
        params[cfg.name] = dict(strategy.params)
        warmups[cfg.name] = int(getattr(strategy, "warmup_bars", 0))

    names = sorted(signals)
    variants: list[Strategy] = []
    for size in range(1, len(names) + 1):
        for combo in itertools.combinations(names, size):
            if size == 1:
                variants.append(
                    PrecomputedStrategy(
                        name=combo[0],
                        params=params[combo[0]],
                        signals=signals[combo[0]],
                        warmup_bars=warmups[combo[0]],
                    )
                )
            else:
                variants.append(
                    CompositeStrategy(
                        components=combo,
                        component_signals=tuple(signals[n] for n in combo),
                        # The component parameters travel with the variant so a
                        # stored run is reproducible from its own row.
                        params={"components": list(combo), **{n: params[n] for n in combo}},
                        name=combination_name(combo),
                        # An AND is only meaningful once every component is warm.
                        warmup_bars=max(warmups[n] for n in combo),
                    )
                )
    return variants
