"""Kraken public OHLC client."""

from __future__ import annotations

from datetime import UTC, datetime

import requests

from pipeline.models import Candle
from pipeline.sources.base import USER_AGENT, SourceError, request_json

API_URL = "https://api.kraken.com/0/public/OHLC"
INTERVAL_MINUTES = 1440

# Canonical symbol -> Kraken pair name. The response key may differ (e.g. XXBTZUSD).
PAIR_BY_SYMBOL = {"BTC-USD": "XBTUSD", "ETH-USD": "ETHUSD"}

# Kraken returns bare arrays (numbers as strings); this labels them for the bronze payload.
_FIELDS = ("time", "open", "high", "low", "close", "vwap", "volume", "count")


class KrakenClient:
    """Daily OHLCV from Kraken; returns at most ~720 most-recent candles per pair."""

    source_name = "kraken"

    def __init__(self, session: requests.Session | None = None) -> None:
        self._session = session if session is not None else requests.Session()

    def fetch_candles(self, symbol: str, start: datetime, end: datetime) -> list[Candle]:
        """Fetch daily candles and filter to [start, end] (API window is fixed)."""
        pair = PAIR_BY_SYMBOL.get(symbol)
        if pair is None:
            raise ValueError(f"No Kraken pair mapping for symbol {symbol!r}")
        payload = request_json(
            self._session,
            API_URL,
            params={"pair": pair, "interval": INTERVAL_MINUTES},
            headers={"User-Agent": USER_AGENT},
        )
        if payload.get("error"):
            raise SourceError(f"Kraken error for {pair}: {payload['error']}")
        result = payload.get("result") or {}
        rows = next((v for k, v in result.items() if k != "last"), None)
        if rows is None:
            raise SourceError(f"Kraken response for {pair} has no OHLC rows")
        candles: list[Candle] = []
        for row in rows:
            raw = dict(zip(_FIELDS, row, strict=True))
            ts = datetime.fromtimestamp(int(raw["time"]), tz=UTC)
            if start <= ts <= end:
                candles.append(
                    Candle(
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
                )
        candles.sort(key=lambda c: c.ts)
        return candles
