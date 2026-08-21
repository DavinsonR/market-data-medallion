"""Bronze ingestion: fetch new candles from a source and append raw rows idempotently."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from pipeline.config import AssetConfig, database_url, load_config
from pipeline.models import Candle, IngestResult
from pipeline.sources import AuthError, MissingApiKeyError, RateLimitError, get_client

logger = logging.getLogger(__name__)

_WATERMARK_SQL = """\
SELECT max(candle_ts) FROM bronze.raw_candles
WHERE source = %s AND symbol = %s AND granularity = %s
"""

_INSERT_RUN_SQL = """\
INSERT INTO meta.ingest_runs (ingest_run_id, source, symbol, granularity, window_start,
    window_end, rows_fetched, rows_inserted, status, error, started_at, finished_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_INSERT_CANDLE_SQL = """\
INSERT INTO bronze.raw_candles (source, symbol, granularity, candle_ts, payload, ingest_run_id)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (source, symbol, granularity, candle_ts) DO NOTHING
"""

_FINISH_RUN_SQL = """\
UPDATE meta.ingest_runs SET rows_inserted = %s, finished_at = %s WHERE ingest_run_id = %s
"""


def ingest(conn: Any, source_name: str, asset: AssetConfig, granularity: str) -> IngestResult:
    """Fetch new candles for ``asset`` from ``source_name`` and append them to bronze.

    The window starts one day after the stored watermark (or at ``backfill_start``) and
    ends at the last fully closed UTC day, so a still-forming bar is never frozen into
    the append-only layer. Every attempt is audited in meta.ingest_runs, success or
    failure. A ``MissingApiKeyError`` propagates before any audit row is written so the
    flow can skip the source with a warning instead of recording a failure.
    """
    client = get_client(source_name)
    started_at = datetime.now(UTC)
    run_id = uuid4()
    window_start: datetime | None = None
    window_end: datetime | None = None
    candles: list[Candle] = []
    error: str | None = None
    status = "success"
    rate_limited = False
    auth_failed = False
    try:
        watermark = _watermark(conn, source_name, asset.symbol, granularity)
        window_start = (
            watermark + timedelta(days=1)
            if watermark is not None
            else _as_utc(asset.backfill_start)
        )
        window_end = _last_closed_day(started_at)
        if window_start <= window_end:
            candles = client.fetch_candles(asset.symbol, window_start, window_end)
    except RateLimitError as exc:
        status = "failed"
        rate_limited = True
        error = f"{type(exc).__name__}: {exc}"
        logger.warning("Rate limited on %s/%s", source_name, asset.symbol)
    except AuthError as exc:
        status = "failed"
        auth_failed = True
        error = f"{type(exc).__name__}: {exc}"
        logger.error("Credential rejected by %s (%s)", source_name, asset.symbol)
    except Exception as exc:
        status = "failed"
        error = f"{type(exc).__name__}: {exc}"
        logger.exception("Ingestion failed for %s/%s", source_name, asset.symbol)

    rows_inserted = 0
    try:
        with conn.transaction():
            conn.execute(
                _INSERT_RUN_SQL,
                (run_id, source_name, asset.symbol, granularity, window_start, window_end,
                 len(candles), 0, status, error, started_at, None),
            )
            for candle in candles:
                cursor = conn.execute(
                    _INSERT_CANDLE_SQL,
                    (source_name, asset.symbol, granularity, candle.ts, Jsonb(candle.raw), run_id),
                )
                rows_inserted += max(cursor.rowcount, 0)
            conn.execute(_FINISH_RUN_SQL, (rows_inserted, datetime.now(UTC), run_id))
    except Exception as exc:
        status = "failed"
        error = f"{type(exc).__name__}: {exc}"
        rows_inserted = 0
        logger.exception("Bronze write failed for %s/%s", source_name, asset.symbol)
        _record_failed_run(
            conn, run_id, source_name, asset.symbol, granularity,
            window_start, window_end, len(candles), error, started_at,
        )

    return IngestResult(
        source=source_name,
        symbol=asset.symbol,
        granularity=granularity,
        window_start=window_start,
        window_end=window_end,
        rows_fetched=len(candles),
        rows_inserted=rows_inserted,
        status=status,
        error=error,
        rate_limited=rate_limited,
        auth_failed=auth_failed,
    )


def _watermark(conn: Any, source: str, symbol: str, granularity: str) -> datetime | None:
    row = conn.execute(_WATERMARK_SQL, (source, symbol, granularity)).fetchone()
    return row[0] if row else None


def _as_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _last_closed_day(now: datetime) -> datetime:
    """Open time of the most recent fully closed daily bar."""
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)


def _record_failed_run(
    conn: Any,
    run_id: UUID,
    source: str,
    symbol: str,
    granularity: str,
    window_start: datetime | None,
    window_end: datetime | None,
    rows_fetched: int,
    error: str,
    started_at: datetime,
) -> None:
    with conn.transaction():
        conn.execute(
            _INSERT_RUN_SQL,
            (run_id, source, symbol, granularity, window_start, window_end,
             rows_fetched, 0, "failed", error, started_at, datetime.now(UTC)),
        )


def main() -> None:
    """Ingest every configured (asset, source) pair into DATABASE_URL."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    config = load_config()
    with psycopg.connect(database_url()) as conn:
        for asset in config.assets:
            names = [asset.sources.primary]
            if asset.sources.reconcile:
                names.append(asset.sources.reconcile)
            for source_name in names:
                try:
                    result = ingest(conn, source_name, asset, config.granularity)
                except MissingApiKeyError as exc:
                    logger.warning("Skipping %s/%s: %s", source_name, asset.symbol, exc)
                    continue
                logger.info(
                    "%s/%s: %s (fetched=%d inserted=%d)",
                    source_name, asset.symbol, result.status,
                    result.rows_fetched, result.rows_inserted,
                )


if __name__ == "__main__":
    main()
