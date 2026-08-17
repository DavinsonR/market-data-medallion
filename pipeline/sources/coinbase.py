"""Coinbase Exchange public candles client."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests

from pipeline.models import Candle
from pipeline.sources.base import USER_AGENT, request_json

API_URL = "https://api.exchange.coinbase.com/products/{product}/candles"
GRANULARITY_SECONDS = 86_400
MAX_CANDLES_PER_REQUEST = 300

# Coinbase returns bare arrays; this labels them for the bronze payload.
_FIELDS = ("time", "low", "high", "open", "close", "volume")


class CoinbaseClient:
    """Daily OHLCV from Coinbase Exchange; canonical symbols map 1:1 to product ids."""

    source_name = "coinbase"

    def __init__(self, session: requests.Session | None = None) -> None:
        self._session = session if session is not None else requests.Session()

    def fetch_candles(self, symbol: str, start: datetime, end: datetime) -> list[Candle]:
        """Fetch daily candles in [start, end], paginating in 300-candle windows."""
        step = timedelta(seconds=GRANULARITY_SECONDS)
        window = step * (MAX_CANDLES_PER_REQUEST - 1)
        by_ts: dict[datetime, Candle] = {}
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + window, end)
            rows = request_json(
                self._session,
                API_URL.format(product=symbol),
                params={
                    "granularity": GRANULARITY_SECONDS,
                    "start": cursor.isoformat(),
                    "end": chunk_end.isoformat(),
                },
                headers={"User-Agent": USER_AGENT},
            )
            for row in rows:
                raw = dict(zip(_FIELDS, row, strict=True))
                ts = datetime.fromtimestamp(int(raw["time"]), tz=UTC)
                if start <= ts <= end:
                    by_ts[ts] = Candle(
                        source=self.source_name,
                        symbol=symbol,
                        granularity="1d",
                        ts=ts,
                        open=float(raw["open"]),
                        high=float(raw["high"]),
                        low=float(raw["low"]),
                        close=float(raw["close"]),
                        volume=float(raw["volume"]),
                        raw=raw,
                    )
            cursor = chunk_end + step
        return [by_ts[ts] for ts in sorted(by_ts)]
