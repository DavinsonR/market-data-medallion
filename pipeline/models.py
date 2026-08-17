"""Shared data contracts for the pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Candle:
    """One OHLCV bar as fetched from a source, timezone-aware UTC.

    ``raw`` preserves the source record exactly as returned by the API;
    it is what lands in bronze.raw_candles.payload.
    """

    source: str
    symbol: str
    granularity: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestResult:
    """Outcome of one ingestion window for meta.ingest_runs."""

    source: str
    symbol: str
    granularity: str
    window_start: datetime | None
    window_end: datetime | None
    rows_fetched: int
    rows_inserted: int
    status: str  # 'success' | 'failed'
    error: str | None = None
