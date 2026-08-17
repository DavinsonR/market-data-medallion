"""Pandera gate between the warehouse and the backtesting engine.

dbt tests guard the SQL side; this guards the exact frame the engine consumes.
Volume is required for asset classes that have a consolidated tape (crypto,
equities) and absent by nature for spot FX, so the caller states which applies.
"""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa

_PRICE_CHECKS = [
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
]

_BASE_COLUMNS = {
    "symbol": pa.Column(str),
    "open": pa.Column(float, pa.Check.gt(0)),
    "high": pa.Column(float, pa.Check.gt(0)),
    "low": pa.Column(float, pa.Check.gt(0)),
    "close": pa.Column(float, pa.Check.gt(0)),
    # Indicator columns are nullable during their warm-up window.
    "sma_20": pa.Column(float, nullable=True),
    "sma_50": pa.Column(float, nullable=True),
    "sma_200": pa.Column(float, nullable=True),
    "rsi_14": pa.Column(float, pa.Check.in_range(0, 100), nullable=True),
}


def _schema(*, require_volume: bool) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        columns={
            **_BASE_COLUMNS,
            "volume": pa.Column(float, pa.Check.ge(0), nullable=not require_volume),
            "vol_sma_20": pa.Column(float, nullable=True),
        },
        checks=_PRICE_CHECKS,
        strict=False,
    )


_WITH_VOLUME = _schema(require_volume=True)
_WITHOUT_VOLUME = _schema(require_volume=False)


def validate_ohlcv(df: pd.DataFrame, *, require_volume: bool = True) -> pd.DataFrame:
    """Validate a single-symbol indicator frame; raises pandera.errors.SchemaError.

    Set ``require_volume=False`` for spot FX, where no volume tape exists.
    """
    schema = _WITH_VOLUME if require_volume else _WITHOUT_VOLUME
    return schema.validate(df)
