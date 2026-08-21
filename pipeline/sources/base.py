"""Source client protocol, factory, and shared HTTP plumbing."""

from __future__ import annotations

import re
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


class AuthError(SourceError):
    """The source refused the call with HTTP 401 or 403.

    A credential is missing, wrong, or revoked. Unlike a 5xx this is not
    transient and unlike a 429 it will not clear on the next window: every
    retry and every later run fails identically until a human fixes the
    secret. Retrying it cost this pipeline 46 minutes a night for four nights
    (see FALLO-25) while the site kept publishing as if all was well.
    """


# A query string carries the credential. Any message built from a URL therefore
# carries it too, and those messages are persisted to meta.ingest_runs and can
# reach logs and exports. Redact before the string exists, not before it ships.
_SECRET_PARAM = re.compile(
    r"(?i)\b(token|api[_-]?key|key|apikey|password|secret|access[_-]?token)=[^&\s'\"]+"
)


def redact(text: str) -> str:
    """Mask credential-bearing query parameters in ``text``."""
    return _SECRET_PARAM.sub(lambda m: f"{m.group(1)}=***", text)


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
            raise RateLimitError(f"GET {redact(url)} refused with HTTP 429 (rate limited)")
        if last_status in (401, 403):
            # A bad credential is not a transient fault: fail the run loudly
            # rather than retrying it 46 times against the same rejection.
            raise AuthError(
                f"GET {redact(url)} refused with HTTP {last_status}: "
                "the API credential is missing, wrong or revoked"
            )
        if last_status >= 500:
            if attempt < max_tries:
                delay = BACKOFF_SECONDS if backoff_seconds is None else backoff_seconds
                time.sleep(delay * 2 ** (attempt - 1))
            continue
        try:
            response.raise_for_status()
        except Exception as exc:  # requests.HTTPError puts the full URL in str(exc)
            raise SourceError(redact(str(exc))) from None
        return response.json()
    raise SourceError(
        f"GET {redact(url)} failed with HTTP {last_status} after {max_tries} tries"
    )


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
