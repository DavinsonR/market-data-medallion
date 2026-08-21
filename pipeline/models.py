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
    # Spot FX trades OTC with no centralized tape, so volume is genuinely absent
    # there — None, never a fabricated zero.
    volume: float | None
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
    # True when the source refused with HTTP 429: not a transient fault, so the
    # caller must stop hitting that source for the rest of the run.
    rate_limited: bool = False
    # True when the source refused with HTTP 401/403. Also not transient, and
    # unlike a rate limit it will not clear by itself: the run must go red so
    # somebody fixes the credential instead of the site quietly publishing
    # yesterday's data under today's timestamp.
    auth_failed: bool = False
