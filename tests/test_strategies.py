"""Known-answer and look-ahead-guard tests for signal generators (synthetic data only)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.backtest.strategies import (
    Macd,
    RsiReversion,
    SmaCross,
    VolumeBreakout,
    build_strategy,
    cutlers_rsi,
)
from pipeline.config import StrategyConfig, load_config

STRATEGY_CONFIGS = [
    StrategyConfig("sma_cross", {"fast": 3, "slow": 8}),
    StrategyConfig("macd", {"fast": 5, "slow": 13, "signal": 4}),
    StrategyConfig("rsi_reversion", {"period": 5, "entry_below": 40.0, "exit_above": 60.0}),
    StrategyConfig("volume_breakout", {"price_window": 5, "volume_window": 5, "volume_mult": 1.2}),
]
# MACD is excluded below: its EMAs are seeded from the first bar, so it has no warm-up NaNs.
WARMUP_CONFIGS = [c for c in STRATEGY_CONFIGS if c.name != "macd"]


def make_df(closes, volumes=None, **extra) -> pd.DataFrame:
    closes = np.asarray(closes, dtype=float)
    n = len(closes)
    volumes = np.full(n, 100.0) if volumes is None else np.asarray(volumes, dtype=float)
    opens = np.concatenate([closes[:1], closes[:-1]])
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2024-01-01", periods=n, freq="D", tz="UTC"),
            "open": opens,
            "high": closes * 1.01,
            "low": closes * 0.99,
            "close": closes,
            "volume": volumes,
        }
    )
    for column, values in extra.items():
        df[column] = values
    return df


def synthetic_df(n: int = 80, seed: int = 7) -> pd.DataFrame:
    """Deterministic regime-switching walk (20-bar up/down legs, periodic volume spikes)."""
    rng = np.random.default_rng(seed)
    drift = np.where((np.arange(n) // 20) % 2 == 0, 0.02, -0.02)
    rets = drift + rng.normal(0.0, 0.01, n)
    closes = 100.0 * np.cumprod(1.0 + rets)
    volumes = rng.uniform(100.0, 200.0, n)
    volumes[::7] *= 5.0
    return make_df(closes, volumes)


# --- sma_cross ---------------------------------------------------------------


def test_sma_cross_known_answer_uptrend():
    signals = SmaCross(fast=2, slow=3).generate_signals(make_df([1.0, 2.0, 3.0, 4.0, 5.0, 6.0]))
    # SMA(3) is first defined at index 2; a rising series keeps SMA(2) above SMA(3).
    assert signals.tolist() == [0, 0, 1, 1, 1, 1]


def test_sma_cross_downtrend_stays_flat():
    signals = SmaCross(fast=2, slow=3).generate_signals(make_df([6.0, 5.0, 4.0, 3.0, 2.0, 1.0]))
    assert signals.tolist() == [0, 0, 0, 0, 0, 0]


def test_sma_cross_uses_gold_columns_when_windows_match():
    df = make_df([100.0] * 6, sma_20=[2.0] * 6, sma_50=[1.0] * 6)
    signals = SmaCross(fast=20, slow=50).generate_signals(df)
    # Constant closes would give equal SMAs (all 0); the gold columns say fast > slow.
    assert signals.tolist() == [1] * 6


def test_sma_cross_ignores_gold_columns_for_other_windows():
    df = make_df([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], sma_20=[10.0] * 6, sma_50=[20.0] * 6)
    signals = SmaCross(fast=2, slow=3).generate_signals(df)
    # If the mismatched gold columns were used, every bar would be flat.
    assert signals.tolist() == [0, 0, 1, 1, 1, 1]


def test_sma_cross_rejects_inverted_windows():
    with pytest.raises(ValueError):
        SmaCross(fast=50, slow=20)


# --- macd --------------------------------------------------------------------


def _ema_reference(values: list[float], span: int) -> list[float]:
    alpha = 2.0 / (span + 1.0)
    out: list[float] = []
    prev: float | None = None
    for value in values:
        prev = value if prev is None else alpha * value + (1.0 - alpha) * prev
        out.append(prev)
    return out


def test_macd_matches_independent_recursion():
    closes = [10.0, 11.0, 10.5, 12.0, 13.0, 12.5, 14.0, 15.0, 13.0, 12.0]
    got = Macd(fast=2, slow=4, signal=3).generate_signals(make_df(closes)).tolist()

    ema_fast = _ema_reference(closes, 2)
    ema_slow = _ema_reference(closes, 4)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow, strict=True)]
    signal_line = _ema_reference(macd_line, 3)
    expected = [int(m > s) for m, s in zip(macd_line, signal_line, strict=True)]

    # Guard against float-tie flakiness and vacuous expectations.
    gaps = [abs(m - s) for m, s in zip(macd_line[1:], signal_line[1:], strict=True)]
    assert min(gaps) > 1e-6
    assert 0 < sum(expected) < len(expected)
    assert got == expected


def test_macd_constant_price_is_flat():
    signals = Macd(fast=12, slow=26, signal=9).generate_signals(make_df([100.0] * 40))
    assert signals.tolist() == [0] * 40


def test_macd_monotonic_uptrend_long_from_second_bar():
    closes = [100.0 + t for t in range(15)]
    signals = Macd(fast=3, slow=6, signal=4).generate_signals(make_df(closes))
    # Bar 0: MACD == signal == 0 (flat); afterwards MACD leads its own EMA upward.
    assert signals.tolist() == [0] + [1] * 14


# --- rsi_reversion -----------------------------------------------------------


def test_cutlers_rsi_known_values():
    closes = pd.Series([10.0, 9.0, 8.0, 7.0, 8.0, 9.0, 10.0, 10.0, 10.0])
    rsi = cutlers_rsi(closes, period=2)
    # Hand-computed: pure losses -> 0, mixed 1/1 -> 50, pure gains -> 100, flat window -> NaN.
    expected = [np.nan, np.nan, 0.0, 0.0, 50.0, 100.0, 100.0, 100.0, np.nan]
    np.testing.assert_array_equal(rsi.to_numpy(), expected)


def test_rsi_reversion_stateful_walk():
    df = make_df([10.0, 9.0, 8.0, 7.0, 8.0, 9.0, 10.0, 10.0, 10.0])
    strategy = RsiReversion(period=2, entry_below=30.0, exit_above=70.0)
    # RSI: [nan, nan, 0, 0, 50, 100, 100, 100, nan]
    # Enter at index 2 (0 < 30), hold through 50, exit at index 5 (100 > 70).
    assert strategy.generate_signals(df).tolist() == [0, 0, 1, 1, 1, 0, 0, 0, 0]


def test_rsi_reversion_constant_price_never_trades():
    strategy = RsiReversion(period=14, entry_below=30.0, exit_above=70.0)
    assert strategy.generate_signals(make_df([100.0] * 30)).tolist() == [0] * 30


# --- volume_breakout ---------------------------------------------------------


def test_volume_breakout_known_answer():
    df = make_df([10.0, 10.0, 12.0, 12.0, 10.0], volumes=[100.0, 100.0, 400.0, 100.0, 100.0])
    strategy = VolumeBreakout(price_window=2, volume_window=2, volume_mult=1.5)
    # Index 2 is the only long bar: close 12 > SMA2 11 and volume 400 > 1.5 * 250 = 375.
    assert strategy.generate_signals(df).tolist() == [0, 0, 1, 0, 0]


def test_volume_breakout_requires_both_conditions():
    df = make_df([10.0, 10.0, 12.0, 13.0, 14.0], volumes=[100.0] * 5)
    strategy = VolumeBreakout(price_window=2, volume_window=2, volume_mult=1.5)
    # Price keeps breaking out but volume never exceeds 1.5x its SMA.
    assert strategy.generate_signals(df).tolist() == [0] * 5


# --- NaN-safety and look-ahead guards ----------------------------------------


@pytest.mark.parametrize("cfg", WARMUP_CONFIGS, ids=lambda c: c.name)
def test_signals_zero_while_indicators_undefined(cfg):
    df = make_df([100.0, 101.0, 99.0])  # shorter than every window in use
    signals = build_strategy(cfg).generate_signals(df)
    assert signals.tolist() == [0, 0, 0]


@pytest.mark.parametrize("cfg", STRATEGY_CONFIGS, ids=lambda c: c.name)
def test_no_look_ahead_prefix_property(cfg):
    df = synthetic_df()
    strategy = build_strategy(cfg)
    full = strategy.generate_signals(df).to_numpy()
    assert 0 < full.sum() < len(full)  # both states present: the guard is non-vacuous
    for k in (20, 45, 79):
        prefix = strategy.generate_signals(df.head(k).copy()).to_numpy()
        np.testing.assert_array_equal(prefix, full[:k])


@pytest.mark.parametrize("cfg", STRATEGY_CONFIGS, ids=lambda c: c.name)
def test_future_data_cannot_change_past_signals(cfg):
    df = synthetic_df()
    strategy = build_strategy(cfg)
    base = strategy.generate_signals(df).to_numpy()
    k = 40
    mutated = df.copy()
    mutated.loc[k:, ["open", "high", "low", "close"]] *= 25.0
    mutated.loc[k:, "volume"] *= 100.0
    np.testing.assert_array_equal(strategy.generate_signals(mutated).to_numpy()[:k], base[:k])


# --- factory -----------------------------------------------------------------


@pytest.mark.parametrize("cfg", STRATEGY_CONFIGS, ids=lambda c: c.name)
def test_build_strategy_round_trips_name_and_params(cfg):
    strategy = build_strategy(cfg)
    assert strategy.name == cfg.name
    assert strategy.params == cfg.params


def test_build_strategy_rejects_unknown_name():
    with pytest.raises(ValueError, match="Unknown strategy"):
        build_strategy(StrategyConfig("momentum_ai", {}))


def test_default_config_strategies_build():
    for cfg in load_config().backtest.strategies:
        strategy = build_strategy(cfg)
        assert strategy.name == cfg.name
        assert strategy.params == cfg.params
