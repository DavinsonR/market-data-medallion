"""Source client protocol, factory, and shared HTTP plumbing."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pipeline.models import Candle

USER_AGENT = "market-data-medallion"
DEFAULT_TIMEOUT = 30.0
MAX_TRIES = 3
BACKOFF_SECONDS = 1.0


class SourceError(RuntimeError):
    """A source API returned an error or an unusable response."""


class MissingApiKeyError(SourceError):
    """A required API key is not configured; the source should be skipped."""


class RateLimitError(SourceError):
    """The source refused the call with HTTP 429.

    Free tiers meter per hour, so retrying within a run cannot succeed and only
    burns more quota. Callers must fail fast and let the next scheduled run
    resume from the bronze watermark.
    """


@runtime_checkable
class SourceClient(Protocol):
    """Fetches daily candles for a canonical symbol within [start, end]."""

    def fetch_candles(self, symbol: str, start: datetime, end: datetime) -> list[Candle]:
        """Return candles with bar-open timestamps inside [start, end], ascending."""
        ...


def request_json(
    session: Any,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_tries: int = MAX_TRIES,
    backoff_seconds: float | None = None,
) -> Any:
    """GET ``url`` and return parsed JSON, retrying with backoff on 429/5xx."""
    last_status = 0
    for attempt in range(1, max_tries + 1):
        response = session.get(url, params=params, headers=headers, timeout=timeout)
        last_status = response.status_code
        if last_status == 429:
            # Hourly quota: retrying now cannot succeed and costs more quota.
            raise RateLimitError(f"GET {url} refused with HTTP 429 (rate limited)")
        if last_status >= 500:
            if attempt < max_tries:
                delay = BACKOFF_SECONDS if backoff_seconds is None else backoff_seconds
                time.sleep(delay * 2 ** (attempt - 1))
            continue
        response.raise_for_status()
        return response.json()
    raise SourceError(f"GET {url} failed with HTTP {last_status} after {max_tries} tries")


def get_client(name: str) -> SourceClient:
    """Return the client registered under ``name``: coinbase, kraken, tiingo, tiingo_fx."""
    from pipeline.sources.coinbase import CoinbaseClient
    from pipeline.sources.kraken import KrakenClient
    from pipeline.sources.tiingo import TiingoClient
    from pipeline.sources.tiingo_fx import TiingoFxClient

    if name == "coinbase":
        return CoinbaseClient()
    if name == "kraken":
        return KrakenClient()
    if name == "tiingo":
        return TiingoClient()
    if name == "tiingo_fx":
        return TiingoFxClient()
    raise ValueError(f"Unknown source: {name!r}")
