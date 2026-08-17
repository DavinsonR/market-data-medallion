"""Tests for source clients (recorded fixtures, no live network) and bronze ingestion."""

from __future__ import annotations

import json
import os
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from pipeline import bronze
from pipeline.config import AssetConfig, SourcesConfig
from pipeline.models import Candle
from pipeline.sources import MissingApiKeyError, RateLimitError, SourceError, get_client
from pipeline.sources.coinbase import CoinbaseClient
from pipeline.sources.kraken import KrakenClient
from pipeline.sources.tiingo import TiingoClient
from pipeline.sources.tiingo_fx import TiingoFxClient

FIXTURES = Path(__file__).parent / "fixtures"
# Never DATABASE_URL: that may point at the managed warehouse, and these tests
# write rows and run a table-wide prune. Tests get their own database or none.
PG_URL = os.environ.get("MDM_TEST_DATABASE_URL", "postgresql://mdm@localhost:5433/mdm")

SEED_RUN_IDS = (
    "11111111-1111-4111-8111-111111111111",  # coinbase BTC-USD
    "22222222-2222-4222-8222-222222222222",  # kraken BTC-USD
    "33333333-3333-4333-8333-333333333333",  # coinbase ETH-USD
)


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


class FakeResponse:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Offline stand-in for requests.Session: a response queue or a per-call callback."""

    def __init__(self, responses: list[FakeResponse] | None = None, callback: Any = None) -> None:
        self._responses = list(responses or [])
        self._callback = callback
        self.calls: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "headers": headers})
        if self._callback is not None:
            return self._callback(url, params)
        return self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]


# --- Coinbase -------------------------------------------------------------------


def test_coinbase_parses_recorded_response() -> None:
    payload = load_fixture("coinbase_candles.json")
    session = FakeSession([FakeResponse(payload=payload)])
    client = CoinbaseClient(session=session)  # type: ignore[arg-type]
    start = datetime(2026, 8, 1, tzinfo=UTC)
    end = datetime(2026, 8, 10, tzinfo=UTC)

    candles = client.fetch_candles("BTC-USD", start, end)

    assert len(candles) == 10
    assert [c.ts for c in candles] == sorted(c.ts for c in candles)
    oldest = min(payload, key=lambda r: r[0])  # API returns newest-first
    first = candles[0]
    assert first.ts == datetime.fromtimestamp(oldest[0], tz=UTC)
    assert first.ts.tzinfo is not None
    assert (first.low, first.high, first.open, first.close, first.volume) == tuple(oldest[1:])
    assert first.raw == dict(
        zip(("time", "low", "high", "open", "close", "volume"), oldest, strict=True)
    )
    assert first.source == "coinbase" and first.symbol == "BTC-USD"
    call = session.calls[0]
    assert "products/BTC-USD/candles" in call["url"]
    assert call["headers"]["User-Agent"] == "market-data-medallion"
    assert call["params"]["granularity"] == 86400


def test_coinbase_paginates_windows_over_300_candles() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=399)

    def respond(url: str, params: dict[str, Any]) -> FakeResponse:
        chunk_start = datetime.fromisoformat(params["start"])
        epoch = int(chunk_start.timestamp())
        return FakeResponse(payload=[[epoch, 1.0, 3.0, 2.0, 2.5, 100.0]])

    session = FakeSession(callback=respond)
    client = CoinbaseClient(session=session)  # type: ignore[arg-type]

    candles = client.fetch_candles("ETH-USD", start, end)

    assert len(session.calls) == 2
    first, second = (c["params"] for c in session.calls)
    assert first["start"] == start.isoformat()
    assert first["end"] == (start + timedelta(days=299)).isoformat()
    assert second["start"] == (start + timedelta(days=300)).isoformat()
    assert second["end"] == end.isoformat()
    assert [c.ts for c in candles] == [start, start + timedelta(days=300)]


def test_retries_on_5xx_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pipeline.sources.base.BACKOFF_SECONDS", 0.0)
    session = FakeSession(
        [FakeResponse(500), FakeResponse(502), FakeResponse(200, payload=[])]
    )
    client = CoinbaseClient(session=session)  # type: ignore[arg-type]
    start = datetime(2026, 8, 1, tzinfo=UTC)

    assert client.fetch_candles("BTC-USD", start, start) == []
    assert len(session.calls) == 3


def test_429_fails_fast_without_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    """Free-tier quotas are hourly: retrying a 429 in-run cannot succeed, it only
    burns more quota. One call, one RateLimitError."""
    monkeypatch.setattr("pipeline.sources.base.BACKOFF_SECONDS", 0.0)
    session = FakeSession([FakeResponse(429), FakeResponse(200, payload=[])])
    client = CoinbaseClient(session=session)  # type: ignore[arg-type]
    start = datetime(2026, 8, 1, tzinfo=UTC)

    with pytest.raises(RateLimitError, match="429"):
        client.fetch_candles("BTC-USD", start, start)
    assert len(session.calls) == 1, "a rate-limited call must not be retried"


def test_coinbase_raises_after_retries_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pipeline.sources.base.BACKOFF_SECONDS", 0.0)
    session = FakeSession([FakeResponse(503)])
    client = CoinbaseClient(session=session)  # type: ignore[arg-type]
    start = datetime(2026, 8, 1, tzinfo=UTC)

    with pytest.raises(SourceError, match="HTTP 503"):
        client.fetch_candles("BTC-USD", start, start)
    assert len(session.calls) == 3


# --- Kraken ---------------------------------------------------------------------


def test_kraken_parses_recorded_response_and_maps_pair() -> None:
    payload = load_fixture("kraken_ohlc.json")
    session = FakeSession([FakeResponse(payload=payload)])
    client = KrakenClient(session=session)  # type: ignore[arg-type]
    start = datetime(2026, 8, 8, tzinfo=UTC)
    end = datetime(2026, 8, 16, tzinfo=UTC)

    candles = client.fetch_candles("BTC-USD", start, end)

    rows = payload["result"]["XXBTZUSD"]  # response key differs from the requested pair
    in_window = [r for r in rows if start.timestamp() <= int(r[0]) <= end.timestamp()]
    assert len(candles) == len(in_window) == 9  # the newest (in-progress) row is filtered out
    first = candles[0]
    row = in_window[0]
    assert first.ts == datetime.fromtimestamp(int(row[0]), tz=UTC)
    assert (first.open, first.high, first.low, first.close) == tuple(map(float, row[1:5]))
    assert first.volume == float(row[6])
    assert first.raw == dict(
        zip(("time", "open", "high", "low", "close", "vwap", "volume", "count"), row, strict=True)
    )
    assert isinstance(first.raw["close"], str)  # payload keeps Kraken's untouched string values
    call = session.calls[0]
    assert call["params"] == {"pair": "XBTUSD", "interval": 1440}
    assert call["headers"]["User-Agent"] == "market-data-medallion"


def test_kraken_maps_eth_pair() -> None:
    payload = {"error": [], "result": {"XETHZUSD": [], "last": 0}}
    session = FakeSession([FakeResponse(payload=payload)])
    client = KrakenClient(session=session)  # type: ignore[arg-type]
    start = datetime(2026, 8, 1, tzinfo=UTC)

    assert client.fetch_candles("ETH-USD", start, start) == []
    assert session.calls[0]["params"]["pair"] == "ETHUSD"


def test_kraken_raises_on_api_error() -> None:
    payload = {"error": ["EQuery:Unknown asset pair"], "result": {}}
    session = FakeSession([FakeResponse(payload=payload)])
    client = KrakenClient(session=session)  # type: ignore[arg-type]
    start = datetime(2026, 8, 1, tzinfo=UTC)

    with pytest.raises(SourceError, match="Unknown asset pair"):
        client.fetch_candles("BTC-USD", start, start)


def test_kraken_rejects_unmapped_symbol() -> None:
    client = KrakenClient(session=FakeSession([FakeResponse(payload={})]))  # type: ignore[arg-type]
    start = datetime(2026, 8, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="SPY"):
        client.fetch_candles("SPY", start, start)


# --- Tiingo ---------------------------------------------------------------------


def test_tiingo_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    with pytest.raises(MissingApiKeyError, match="TIINGO_API_KEY"):
        TiingoClient(session=FakeSession([FakeResponse(payload=[])]))  # type: ignore[arg-type]
    with pytest.raises(MissingApiKeyError):
        get_client("tiingo")


def test_tiingo_parses_recorded_shape() -> None:
    payload = load_fixture("tiingo_prices.json")
    session = FakeSession([FakeResponse(payload=payload)])
    client = TiingoClient(session=session, api_key="test-token")  # type: ignore[arg-type]
    start = datetime(2026, 8, 10, tzinfo=UTC)
    end = datetime(2026, 8, 14, tzinfo=UTC)

    candles = client.fetch_candles("SPY", start, end)

    assert len(candles) == 5
    first = candles[0]
    row = payload[0]
    assert first.ts == datetime(2026, 8, 10, tzinfo=UTC)
    assert (first.open, first.high, first.low, first.close, first.volume) == (
        row["open"], row["high"], row["low"], row["close"], float(row["volume"]),
    )
    assert first.raw == row  # untouched dict, adjusted fields preserved for silver
    assert first.raw["adjClose"] == row["adjClose"]
    call = session.calls[0]
    assert "tiingo/daily/SPY/prices" in call["url"]
    assert call["params"]["token"] == "test-token"
    assert call["params"]["startDate"] == "2026-08-10"
    assert call["params"]["endDate"] == "2026-08-14"


# --- Tiingo FX -------------------------------------------------------------------


def test_tiingo_fx_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    with pytest.raises(MissingApiKeyError, match="TIINGO_API_KEY"):
        TiingoFxClient(session=FakeSession([FakeResponse(payload=[])]))  # type: ignore[arg-type]
    with pytest.raises(MissingApiKeyError):
        get_client("tiingo_fx")


def test_tiingo_fx_parses_recorded_shape_with_null_volume() -> None:
    payload = load_fixture("tiingo_fx_prices.json")
    session = FakeSession([FakeResponse(payload=payload)])
    client = TiingoFxClient(session=session, api_key="test-token")  # type: ignore[arg-type]

    candles = client.fetch_candles(
        "USDCOP", datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 10, tzinfo=UTC)
    )

    assert candles, "fixture should yield candles inside the window"
    first = candles[0]
    row = payload[0]
    assert (first.open, first.high, first.low, first.close) == (
        row["open"], row["high"], row["low"], row["close"],
    )
    # Spot FX has no consolidated tape: volume must stay absent, never a fake zero.
    assert first.volume is None
    assert first.source == "tiingo_fx"
    assert first.symbol == "USDCOP"
    assert first.raw == row
    assert [c.ts for c in candles] == sorted(c.ts for c in candles)


def test_tiingo_fx_lowercases_ticker_in_url() -> None:
    session = FakeSession([FakeResponse(payload=[])])
    client = TiingoFxClient(session=session, api_key="test-token")  # type: ignore[arg-type]

    client.fetch_candles(
        "USDCOP", datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 2, tzinfo=UTC)
    )

    call = session.calls[0]
    assert "tiingo/fx/usdcop/prices" in call["url"]
    assert call["params"]["resampleFreq"] == "1day"
    assert call["params"]["token"] == "test-token"


# --- Factory --------------------------------------------------------------------


def test_get_client_factory() -> None:
    assert isinstance(get_client("coinbase"), CoinbaseClient)
    assert isinstance(get_client("kraken"), KrakenClient)
    with pytest.raises(ValueError, match="binance"):
        get_client("binance")


# --- Bronze ingestion (fake connection) -----------------------------------------


class FakeCursor:
    def __init__(self, rows: list[tuple] | None = None, rowcount: int = 0) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


class FakeConn:
    """Minimal psycopg-connection stand-in for the SQL bronze.ingest issues."""

    def __init__(self, watermark: datetime | None = None, existing: set | None = None) -> None:
        self.watermark = watermark
        self.existing = set(existing or set())
        self.executed: list[tuple[str, tuple | None]] = []
        self.run_rows: list[tuple] = []
        self.candle_rows: list[tuple] = []
        self.finish_updates: list[tuple] = []

    def transaction(self) -> Any:
        return nullcontext()

    def execute(self, sql: str, params: tuple | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        if "max(candle_ts)" in sql:
            return FakeCursor(rows=[(self.watermark,)])
        if "INSERT INTO meta.ingest_runs" in sql:
            self.run_rows.append(params)
            return FakeCursor(rowcount=1)
        if "INSERT INTO bronze.raw_candles" in sql:
            ts = params[3]
            if ts in self.existing:
                return FakeCursor(rowcount=0)
            self.existing.add(ts)
            self.candle_rows.append(params)
            return FakeCursor(rowcount=1)
        if sql.startswith("UPDATE meta.ingest_runs"):
            self.finish_updates.append(params)
            return FakeCursor(rowcount=1)
        return FakeCursor()


class StubClient:
    def __init__(self, candles: list[Candle] | None = None, exc: Exception | None = None) -> None:
        self.candles = candles or []
        self.exc = exc
        self.calls: list[tuple[str, datetime, datetime]] = []

    def fetch_candles(self, symbol: str, start: datetime, end: datetime) -> list[Candle]:
        self.calls.append((symbol, start, end))
        if self.exc is not None:
            raise self.exc
        return [c for c in self.candles if start <= c.ts <= end]


def make_asset(symbol: str = "BTC-USD") -> AssetConfig:
    return AssetConfig(
        symbol=symbol,
        asset_class="crypto",
        region="global",
        name=symbol,
        sources=SourcesConfig(primary="coinbase", reconcile="kraken"),
        backfill_start="2022-01-01",
    )


def make_candle(ts: datetime, symbol: str = "BTC-USD") -> Candle:
    return Candle(
        source="coinbase", symbol=symbol, granularity="1d", ts=ts,
        open=2.0, high=3.0, low=1.0, close=2.5, volume=10.0,
        raw={"time": int(ts.timestamp()), "close": 2.5},
    )


def _install_stub(monkeypatch: pytest.MonkeyPatch, stub: StubClient) -> None:
    monkeypatch.setattr(bronze, "get_client", lambda name: stub)


def test_ingest_starts_at_backfill_when_no_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn(watermark=None)
    days = [datetime(2022, 1, d, tzinfo=UTC) for d in (1, 2, 3)]
    stub = StubClient(candles=[make_candle(d) for d in days])
    _install_stub(monkeypatch, stub)

    result = bronze.ingest(conn, "coinbase", make_asset(), "1d")

    assert stub.calls[0][1] == datetime(2022, 1, 1, tzinfo=UTC)
    assert stub.calls[0][2] == bronze._last_closed_day(datetime.now(UTC))
    assert result.status == "success" and result.error is None
    assert result.rows_fetched == 3 and result.rows_inserted == 3
    assert len(conn.run_rows) == 1
    assert conn.run_rows[0][8] == "success"
    assert conn.finish_updates[0][0] == 3
    assert [p[3] for p in conn.candle_rows] == days


def test_ingest_resumes_one_day_after_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    watermark = datetime(2024, 5, 1, tzinfo=UTC)
    conn = FakeConn(watermark=watermark)
    days = [datetime(2024, 5, d, tzinfo=UTC) for d in (1, 2, 3)]
    stub = StubClient(candles=[make_candle(d) for d in days])
    _install_stub(monkeypatch, stub)

    result = bronze.ingest(conn, "coinbase", make_asset(), "1d")

    assert stub.calls[0][1] == watermark + timedelta(days=1)
    assert result.rows_fetched == 2 and result.rows_inserted == 2


def test_ingest_skips_fetch_when_up_to_date(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn(watermark=bronze._last_closed_day(datetime.now(UTC)))
    stub = StubClient(candles=[])
    _install_stub(monkeypatch, stub)

    result = bronze.ingest(conn, "coinbase", make_asset(), "1d")

    assert stub.calls == []  # nothing to fetch; the running bar is never ingested
    assert result.status == "success" and result.rows_fetched == 0
    assert len(conn.run_rows) == 1  # the attempt is still audited


def test_ingest_is_idempotent_via_on_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    days = [datetime(2022, 1, d, tzinfo=UTC) for d in (1, 2, 3)]
    conn = FakeConn(watermark=None, existing={days[0]})
    stub = StubClient(candles=[make_candle(d) for d in days])
    _install_stub(monkeypatch, stub)

    result = bronze.ingest(conn, "coinbase", make_asset(), "1d")

    assert result.rows_fetched == 3
    assert result.rows_inserted == 2  # the pre-existing candle hit ON CONFLICT DO NOTHING


def test_ingest_records_failed_run_on_fetch_error(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn(watermark=None)
    stub = StubClient(exc=RuntimeError("api down"))
    _install_stub(monkeypatch, stub)

    result = bronze.ingest(conn, "coinbase", make_asset(), "1d")

    assert result.status == "failed"
    assert result.error == "RuntimeError: api down"
    assert result.rows_fetched == 0 and result.rows_inserted == 0
    assert len(conn.run_rows) == 1 and conn.run_rows[0][8] == "failed"
    assert conn.candle_rows == []


def test_ingest_lets_missing_api_key_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()

    def raise_missing(name: str) -> Any:
        raise MissingApiKeyError("TIINGO_API_KEY is not set")

    monkeypatch.setattr(bronze, "get_client", raise_missing)

    with pytest.raises(MissingApiKeyError):
        bronze.ingest(conn, "tiingo", make_asset("SPY"), "1d")
    assert conn.executed == []  # skipped sources are not audited as failures


# --- Bronze ingestion (local Postgres, rolled back; skipped if unavailable) -----


@pytest.fixture
def pg_conn() -> Any:
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(PG_URL, connect_timeout=2)
    except Exception:
        pytest.skip("local Postgres unavailable")
    try:
        if conn.execute("SELECT to_regclass('bronze.raw_candles')").fetchone()[0] is None:
            pytest.skip("migrations not applied")
        yield conn
    finally:
        conn.rollback()
        conn.close()


def test_ingest_watermark_against_local_pg(
    pg_conn: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from psycopg import Rollback
    from psycopg.types.json import Jsonb

    days = [datetime(2024, 1, d, tzinfo=UTC) for d in (1, 2, 3)]
    with pg_conn.transaction():
        run_id = uuid4()
        pg_conn.execute(
            "INSERT INTO meta.ingest_runs (ingest_run_id, source, symbol, granularity,"
            " rows_fetched, rows_inserted, status, started_at, finished_at)"
            " VALUES (%s, 'coinbase', 'TEST-PG', '1d', 1, 1, 'success', now(), now())",
            (run_id,),
        )
        pg_conn.execute(
            "INSERT INTO bronze.raw_candles (source, symbol, granularity, candle_ts, payload,"
            " ingest_run_id) VALUES ('coinbase', 'TEST-PG', '1d', %s, %s, %s)",
            (days[0], Jsonb({"time": int(days[0].timestamp())}), run_id),
        )
        stub = StubClient(candles=[make_candle(d, symbol="TEST-PG") for d in days])
        _install_stub(monkeypatch, stub)

        result = bronze.ingest(pg_conn, "coinbase", make_asset("TEST-PG"), "1d")

        assert stub.calls[0][1] == days[0] + timedelta(days=1)  # real watermark read
        assert result.status == "success"
        assert result.rows_fetched == 2 and result.rows_inserted == 2
        total = pg_conn.execute(
            "SELECT count(*) FROM bronze.raw_candles WHERE symbol = 'TEST-PG'"
        ).fetchone()[0]
        assert total == 3
        raise Rollback  # leave the shared database untouched


# --- seed_bronze.sql fixture ----------------------------------------------------


def test_seed_bronze_sql_static_shape() -> None:
    text = (FIXTURES / "seed_bronze.sql").read_text()
    assert text.count("('coinbase', 'BTC-USD', '1d',") == 130
    assert text.count("('kraken', 'BTC-USD', '1d',") == 130
    assert text.count("('coinbase', 'ETH-USD', '1d',") == 130
    assert "'2025-05-10" not in text and "'2025-05-11" not in text  # the 2-day gap
    assert text.count("ON CONFLICT (") == 4  # 3 candle blocks + ingest_runs, all idempotent
    assert '"vwap"' in text  # kraken payload shape
    for run_id in SEED_RUN_IDS:
        assert run_id in text


def test_seed_bronze_sql_applies_to_local_pg(pg_conn: Any) -> None:
    from psycopg import Rollback

    seed = (FIXTURES / "seed_bronze.sql").read_text()
    run_ids = "', '".join(SEED_RUN_IDS)
    with pg_conn.transaction():
        # Real ingested candles may occupy the seed's natural keys; clear them so the
        # seed's ON CONFLICT DO NOTHING actually inserts. The trailing Rollback below
        # guarantees the shared database is left untouched.
        pg_conn.execute(
            "DELETE FROM bronze.raw_candles WHERE granularity = '1d'"
            " AND candle_ts BETWEEN '2025-03-01T00:00:00+00' AND '2025-07-10T00:00:00+00'"
            " AND (source, symbol) IN"
            " (('coinbase','BTC-USD'), ('kraken','BTC-USD'), ('coinbase','ETH-USD'))"
        )
        pg_conn.execute(seed)
        counts = dict(
            ((source, symbol), n)
            for source, symbol, n in pg_conn.execute(
                f"SELECT source, symbol, count(*) FROM bronze.raw_candles"
                f" WHERE ingest_run_id IN ('{run_ids}') GROUP BY 1, 2"
            ).fetchall()
        )
        assert counts == {
            ("coinbase", "BTC-USD"): 130,
            ("kraken", "BTC-USD"): 130,
            ("coinbase", "ETH-USD"): 130,
        }
        discrepant = pg_conn.execute(
            f"SELECT count(*) FILTER (WHERE abs(c.close - k.close) / c.close > 0.005)"
            f" FROM (SELECT candle_ts, (payload->>'close')::numeric AS close"
            f"       FROM bronze.raw_candles WHERE ingest_run_id = '{SEED_RUN_IDS[0]}') c"
            f" JOIN (SELECT candle_ts, (payload->>'close')::numeric AS close"
            f"       FROM bronze.raw_candles WHERE ingest_run_id = '{SEED_RUN_IDS[1]}') k"
            f" USING (candle_ts)"
        ).fetchone()[0]
        assert discrepant == 3  # exactly the three ~1% divergence days
        raise Rollback  # leave the shared database untouched
