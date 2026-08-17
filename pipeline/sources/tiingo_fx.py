"""Tiingo FX client (spot currency pairs).

Separate from the equities client: a different endpoint, lowercase tickers, and
OHLC rows with no volume field (spot FX is OTC, so there is no consolidated
volume tape). Candles therefore carry ``volume=None``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import requests

from pipeline.config import tiingo_api_key
from pipeline.models import Candle
from pipeline.sources.base import USER_AGENT, MissingApiKeyError, request_json

API_URL = "https://api.tiingo.com/tiingo/fx/{ticker}/prices"
RESAMPLE_FREQ = "1day"


class TiingoFxClient:
    """Daily spot FX rates from Tiingo; canonical symbols are uppercase pairs."""

    source_name = "tiingo_fx"

    def __init__(
        self, session: requests.Session | None = None, api_key: str | None = None
    ) -> None:
        self._session = session if session is not None else requests.Session()
        self._api_key = api_key if api_key is not None else tiingo_api_key()
        if not self._api_key:
            raise MissingApiKeyError(
                "TIINGO_API_KEY is not set; configure it to ingest FX from Tiingo"
            )

    def fetch_candles(self, symbol: str, start: datetime, end: datetime) -> list[Candle]:
        """Fetch daily FX candles in [start, end]; Tiingo expects lowercase tickers."""
        rows = request_json(
            self._session,
            API_URL.format(ticker=symbol.lower()),
            params={
                "startDate": start.date().isoformat(),
                "endDate": end.date().isoformat(),
                "resampleFreq": RESAMPLE_FREQ,
                "token": self._api_key,
            },
            headers={"User-Agent": USER_AGENT},
        )
        candles: list[Candle] = []
        for row in rows:
            ts = _parse_utc(row["date"])
            if start <= ts <= end:
                candles.append(
                    Candle(
                        source=self.source_name,
                        symbol=symbol,
                        granularity="1d",
                        ts=ts,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=None,
                        raw=dict(row),
                    )
                )
        candles.sort(key=lambda c: c.ts)
        return candles


def _parse_utc(value: str) -> datetime:
    """Parse a Tiingo timestamp like ``2026-08-14T00:00:00.000Z`` as aware UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
