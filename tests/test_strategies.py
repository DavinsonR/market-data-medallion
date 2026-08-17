"""Known-answer and look-ahead-guard tests for signal generators (synthetic data only)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import pipeline.backtest.strategies as strategies_module
from pipeline.backtest.strategies import (
    CompositeStrategy,
    FibonacciRetracement,
    Macd,
    PrecomputedStrategy,
    RsiReversion,
    SmaCross,
    VolumeBreakout,
    build_all_variants,
    build_strategy,
    combination_name,
    cutlers_rsi,
)
from pipeline.config import StrategyConfig, load_config

STRATEGY_CONFIGS = [
    StrategyConfig("sma_cross", {"fast": 3, "slow": 8}),
    StrategyConfig("macd", {"fast": 5, "slow": 13, "signal": 4}),
    StrategyConfig("rsi_reversion", {"period": 5, "entry_below": 40.0, "exit_above": 60.0}),
    StrategyConfig("volume_breakout", {"price_window": 5, "volume_window": 5, "volume_mult": 1.2}),
    StrategyConfig("fibonacci", {"window": 10, "ratio": 0.618}),
]
# MACD is excluded below: its EMAs are seeded from the first bar, so it has no warm-up NaNs.
WARMUP_CONFIGS = [c for c in STRATEGY_CONFIGS if c.name != "macd"]

# The five configured strategies as the flow sees them: volume_breakout carries
# requires_volume, which is what an FX frame has to drop.
COMBINATION_CONFIGS = [
    StrategyConfig("sma_cross", {"fast": 3, "slow": 8}),
    StrategyConfig("macd", {"fast": 5, "slow": 13, "signal": 4}),
    StrategyConfig("rsi_reversion", {"period": 5, "entry_below": 40.0, "exit_above": 60.0}),
    StrategyConfig(
        "volume_breakout",
        {"price_window": 5, "volume_window": 5, "volume_mult": 1.2},
        requires_volume=True,
    ),
    StrategyConfig("fibonacci", {"window": 10, "ratio": 0.618}),
]
COMBINATION_NAMES = sorted(c.name for c in COMBINATION_CONFIGS)


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


class SpyStrategy:
    """Wraps a real strategy and counts how often its signals are recomputed."""

    def __init__(self, inner):
        self.inner = inner
        self.name = inner.name
        self.calls = 0

    @property
    def params(self):
        return self.inner.params

    def generate_signals(self, df):
        self.calls += 1
        return self.inner.generate_signals(df)


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


# --- fibonacci ---------------------------------------------------------------


def test_fibonacci_monotonic_uptrend_stays_long_once_warm():
    df = make_df([float(x) for x in range(1, 11)])
    signals = FibonacciRetracement(window=3, ratio=0.618).generate_signals(df)
    # The swing range needs 3 bars; from then on a rising close sits at the top
    # of its own range, far above the 61.8% retracement.
    assert signals.tolist() == [0, 0] + [1] * 8


def test_fibonacci_crash_through_the_level_goes_flat():
    df = make_df([10.0, 11.0, 12.0, 13.0, 14.0, 5.0])
    signals = FibonacciRetracement(window=3, ratio=0.618).generate_signals(df)
    # Last bar: swing_high 14.14, swing_low 4.95 -> level 8.46; close 5 is below it.
    assert signals.tolist() == [0, 0, 1, 1, 1, 0]


def test_fibonacci_warmup_bars_are_flat():
    df = make_df([100.0, 101.0, 102.0, 103.0])
    assert FibonacciRetracement(window=10, ratio=0.618).generate_signals(df).tolist() == [0] * 4


def test_fibonacci_ratio_changes_the_signal():
    df = make_df([10.0] * 4, high=[20.0] * 4, low=[5.0] * 4)
    shallow = FibonacciRetracement(window=3, ratio=0.236).generate_signals(df)
    deep = FibonacciRetracement(window=3, ratio=0.786).generate_signals(df)
    # Range 5..20, close 10. Level is 16.46 at 0.236 (flat) and 8.21 at 0.786 (long).
    assert shallow.tolist() == [0, 0, 0, 0]
    assert deep.tolist() == [0, 0, 1, 1]


def test_fibonacci_window_changes_the_signal():
    df = make_df([10.0] * 6, high=[12.0, 12.0, 20.0, 12.0, 12.0, 12.0], low=[8.0] * 6)
    short = FibonacciRetracement(window=3, ratio=0.618).generate_signals(df)
    long = FibonacciRetracement(window=5, ratio=0.618).generate_signals(df)
    # The 20.0 spike leaves the 3-bar window at the last bar but not the 5-bar one.
    assert short.tolist() == [0, 0, 0, 0, 0, 1]
    assert long.tolist() == [0] * 6


@pytest.mark.parametrize("window,ratio", [(1, 0.618), (0, 0.618), (10, 0.0), (10, 1.0), (10, 1.5)])
def test_fibonacci_rejects_invalid_params(window, ratio):
    with pytest.raises(ValueError):
        FibonacciRetracement(window=window, ratio=ratio)


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


# --- CompositeStrategy -------------------------------------------------------


def binary_series(values, index=None) -> pd.Series:
    return pd.Series(np.asarray(values, dtype=np.int64), index=index)


def test_combination_name_is_alphabetical_and_plus_joined():
    assert combination_name(["macd", "fibonacci", "sma_cross"]) == "fibonacci+macd+sma_cross"


def test_composite_ands_its_component_signals():
    df = make_df([10.0, 11.0, 12.0, 13.0])
    a = binary_series([1, 1, 0, 1], df.index)
    b = binary_series([1, 0, 1, 1], df.index)
    composite = CompositeStrategy(
        components=("macd", "sma_cross"), component_signals=(a, b), params={}
    )
    assert composite.generate_signals(df).tolist() == [1, 0, 0, 1]


def test_composite_name_defaults_to_the_canonical_join():
    df = make_df([10.0, 11.0])
    ones = binary_series([1, 1], df.index)
    composite = CompositeStrategy(
        components=("fibonacci", "macd"), component_signals=(ones, ones), params={}
    )
    assert composite.name == "fibonacci+macd"
    assert composite.strategy_kind == "combo"
    assert composite.n_components == 2


def test_composite_of_one_component_is_still_a_single():
    df = make_df([10.0, 11.0])
    composite = CompositeStrategy(
        components=("macd",), component_signals=(binary_series([0, 1], df.index),), params={}
    )
    assert composite.strategy_kind == "single"
    assert composite.name == "macd"


def test_composite_rejects_misaligned_component_series():
    df = make_df([10.0, 11.0, 12.0, 13.0])
    composite = CompositeStrategy(
        components=("macd",), component_signals=(binary_series([1, 1]),), params={}
    )
    with pytest.raises(ValueError, match="do not align"):
        composite.generate_signals(df)


def test_composite_rejects_non_binary_component_series():
    df = make_df([10.0, 11.0, 12.0])
    composite = CompositeStrategy(
        components=("macd",), component_signals=(binary_series([0, 1, 2], df.index),), params={}
    )
    with pytest.raises(ValueError, match="only 0 or 1"):
        composite.generate_signals(df)


def test_composite_rejects_inconsistent_construction():
    ones = binary_series([1, 1])
    with pytest.raises(ValueError):
        CompositeStrategy(components=(), component_signals=(), params={})
    with pytest.raises(ValueError):
        CompositeStrategy(components=("macd", "sma_cross"), component_signals=(ones,), params={})


# --- build_all_variants ------------------------------------------------------


def fx_df(n: int = 160) -> pd.DataFrame:
    """A frame shaped like spot FX: OHLC only, volume entirely NULL."""
    df = synthetic_df(n)
    df["volume"] = np.nan
    return df


def test_build_all_variants_returns_every_non_empty_combination():
    variants = build_all_variants(synthetic_df(160), COMBINATION_CONFIGS)
    assert len(variants) == 31  # 2**5 - 1
    subsets = {frozenset(v.components) for v in variants}
    assert len(subsets) == 31
    assert frozenset(COMBINATION_NAMES) in subsets


def test_build_all_variants_drops_volume_strategy_when_the_tape_is_empty():
    variants = build_all_variants(fx_df(), COMBINATION_CONFIGS)
    assert len(variants) == 15  # 2**4 - 1
    assert all("volume_breakout" not in v.components for v in variants)


def test_build_all_variants_drops_volume_strategy_when_the_column_is_absent():
    df = synthetic_df(160).drop(columns=["volume"])
    assert len(build_all_variants(df, COMBINATION_CONFIGS)) == 15


def test_build_all_variants_keeps_volume_strategy_when_volume_is_partially_present():
    df = synthetic_df(160)
    df.loc[: len(df) // 2, "volume"] = np.nan  # warm-up gaps are not "no tape"
    assert len(build_all_variants(df, COMBINATION_CONFIGS)) == 31


def test_build_all_variants_ordering_is_deterministic():
    variants = build_all_variants(synthetic_df(160), COMBINATION_CONFIGS)
    names = [v.name for v in variants]

    assert len(set(names)) == len(names)
    assert names[:5] == COMBINATION_NAMES  # singles first, alphabetically
    assert names[-1] == "+".join(COMBINATION_NAMES)  # the full AND last
    assert [v.n_components for v in variants] == sorted(v.n_components for v in variants)
    for v in variants:
        assert list(v.components) == sorted(v.components)
        assert v.name == "+".join(v.components)
        assert v.strategy_kind == ("single" if v.n_components == 1 else "combo")
        assert v.n_components == len(v.components)
    for size in range(1, 6):
        group = [v.name for v in variants if v.n_components == size]
        assert group == sorted(group)
    # Two builds over the same frame agree bar for bar.
    again = build_all_variants(synthetic_df(160), COMBINATION_CONFIGS)
    assert [v.name for v in again] == names


def test_build_all_variants_singles_match_the_strategies_they_wrap():
    df = synthetic_df(160)
    variants = build_all_variants(df, COMBINATION_CONFIGS)
    singles = {v.name: v for v in variants if v.n_components == 1}
    assert all(isinstance(v, PrecomputedStrategy) for v in singles.values())
    for cfg in COMBINATION_CONFIGS:
        expected = build_strategy(cfg).generate_signals(df)
        assert singles[cfg.name].generate_signals(df).tolist() == expected.tolist()
        assert singles[cfg.name].params == cfg.params


def test_build_all_variants_composite_equals_the_and_of_its_parts():
    df = synthetic_df(160)
    variants = build_all_variants(df, COMBINATION_CONFIGS)
    singles = {v.name: v.generate_signals(df).to_numpy() for v in variants if v.n_components == 1}
    checked = 0
    for v in variants:
        if v.n_components == 1:
            continue
        expected = np.ones(len(df), dtype=np.int64)
        for component in v.components:
            expected &= singles[component]
        np.testing.assert_array_equal(v.generate_signals(df).to_numpy(), expected)
        checked += 1
    assert checked == 26
    # A non-vacuous check: at least one combination is invested some of the time,
    # and adding components never adds exposure.
    pair = next(v for v in variants if v.name == "fibonacci+sma_cross")
    assert 0 < pair.generate_signals(df).sum() < len(df)


def test_build_all_variants_computes_each_strategy_exactly_once(monkeypatch):
    """The efficiency contract: 5 signal computations back 31 backtests, not 80."""
    df = synthetic_df(160)
    spies: dict[str, SpyStrategy] = {}
    real_build_strategy = strategies_module.build_strategy

    def spying_build_strategy(cfg):
        spy = SpyStrategy(real_build_strategy(cfg))
        spies[cfg.name] = spy
        return spy

    monkeypatch.setattr(strategies_module, "build_strategy", spying_build_strategy)
    variants = build_all_variants(df, COMBINATION_CONFIGS)

    assert len(variants) == 31
    assert sorted(spies) == COMBINATION_NAMES
    assert sum(spy.calls for spy in spies.values()) == 5

    # Running every variant must not reach back into any component strategy.
    for variant in variants:
        variant.generate_signals(df)
    assert {name: spy.calls for name, spy in spies.items()} == dict.fromkeys(COMBINATION_NAMES, 1)

    # What the precomputation avoids: one call per component per variant.
    assert sum(v.n_components for v in variants) == 80


def test_build_all_variants_survives_the_train_validation_split():
    """Windowed evaluation re-indexes slices of the frame; signals must follow."""
    df = synthetic_df(200)
    variants = build_all_variants(df, COMBINATION_CONFIGS)
    k = int(len(df) * 0.7)
    train = df.iloc[:k].sort_values("ts").reset_index(drop=True)
    validation = df.iloc[k:].sort_values("ts").reset_index(drop=True)
    for variant in variants:
        full = variant.generate_signals(df).tolist()
        assert variant.generate_signals(train).tolist() == full[:k]
        assert variant.generate_signals(validation).tolist() == full[k:]


def test_build_all_variants_normalizes_frame_order_like_the_engine():
    df = synthetic_df(160)
    reversed_df = df.iloc[::-1].reset_index(drop=True)
    normalized = df.sort_values("ts").reset_index(drop=True)
    for ordered, shuffled in zip(
        build_all_variants(df, COMBINATION_CONFIGS),
        build_all_variants(reversed_df, COMBINATION_CONFIGS),
        strict=True,
    ):
        assert ordered.name == shuffled.name
        assert (
            ordered.generate_signals(normalized).tolist()
            == shuffled.generate_signals(normalized).tolist()
        )


def test_build_all_variants_rejects_duplicate_configuration():
    with pytest.raises(ValueError, match="duplicate strategy"):
        build_all_variants(synthetic_df(40), [COMBINATION_CONFIGS[0], COMBINATION_CONFIGS[0]])


def test_build_all_variants_with_the_shipped_configuration():
    configs = load_config().backtest.strategies
    assert len(configs) == 5
    assert len(build_all_variants(synthetic_df(400), configs)) == 31
    assert len(build_all_variants(fx_df(400), configs)) == 15
