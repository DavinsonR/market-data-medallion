"""Pandera gate between the warehouse and the backtesting engine.

dbt tests guard the SQL side; this guards the exact frame the engine consumes.
"""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa

PRICE_COLS = ["open", "high", "low", "close"]

ohlcv_schema = pa.DataFrameSchema(
    columns={
        "symbol": pa.Column(str),
        "open": pa.Column(float, pa.Check.gt(0)),
        "high": pa.Column(float, pa.Check.gt(0)),
        "low": pa.Column(float, pa.Check.gt(0)),
        "close": pa.Column(float, pa.Check.gt(0)),
        "volume": pa.Column(float, pa.Check.ge(0)),
        # Indicator columns are nullable during their warm-up window.
        "sma_20": pa.Column(float, nullable=True),
        "sma_50": pa.Column(float, nullable=True),
        "sma_200": pa.Column(float, nullable=True),
        "rsi_14": pa.Column(float, pa.Check.in_range(0, 100), nullable=True),
        "vol_sma_20": pa.Column(float, nullable=True),
    },
    checks=[
        pa.Check(lambda df: df["high"] >= df["low"], error="high < low"),
        pa.Check(
            lambda df: df["high"] >= df[["open", "close"]].max(axis=1),
            error="high below body",
        ),
        pa.Check(
            lambda df: df["low"] <= df[["open", "close"]].min(axis=1),
            error="low above body",
        ),
        pa.Check(lambda df: df["ts"].is_monotonic_increasing, error="ts not sorted"),
        pa.Check(lambda df: ~df["ts"].duplicated().any(), error="duplicate ts"),
    ],
    strict=False,
)


def validate_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Validate a single-symbol indicator frame; raises pandera.errors.SchemaError."""
    return ohlcv_schema.validate(df)
