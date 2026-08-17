"""Copy the raw layer from one Postgres to another (local -> managed, or back).

Only bronze and meta travel: they hold the API responses that cost quota to
fetch. Everything downstream (silver, gold, backtests) is derived and is rebuilt
at the destination by `dbt build` and the backtest engine, which also proves the
whole pipeline works there.

The copy streams through COPY ... TO/FROM STDOUT in text format, so it works
across server versions and never materializes the table in memory. It is
idempotent: rows land in a staging table first and are inserted with
ON CONFLICT DO NOTHING against the destination's natural keys.

Usage:
    python -m scripts.sync_bronze_to_remote --source "$LOCAL_URL" --target "$REMOTE_URL"
"""

from __future__ import annotations

import argparse
import sys

import psycopg

# Ordered by dependency: bronze.raw_candles references meta.ingest_runs.
TABLES = (
    ("meta.ingest_runs", "ingest_run_id"),
    ("bronze.raw_candles", "source, symbol, granularity, candle_ts"),
)


def _columns(conn: psycopg.Connection, qualified: str) -> list[str]:
    schema, table = qualified.split(".")
    # Skip generated and identity columns: they are surrogate keys the destination
    # assigns itself, and GENERATED ALWAYS refuses an explicit value.
    rows = conn.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
          AND is_generated = 'NEVER' AND is_identity = 'NO'
        ORDER BY ordinal_position
        """,
        (schema, table),
    ).fetchall()
    return [r[0] for r in rows]


def copy_table(
    source: psycopg.Connection, target: psycopg.Connection, qualified: str, conflict_key: str
) -> tuple[int, int]:
    """Stream one table across; returns (rows_sent, rows_inserted)."""
    cols = ", ".join(_columns(source, qualified))
    staging = f"_sync_{qualified.replace('.', '_')}"

    # Shape the staging table from the column list itself, not LIKE: LIKE would
    # carry the identity column's NOT NULL without its generator.
    target.execute(f"CREATE TEMP TABLE {staging} AS SELECT {cols} FROM {qualified} WHERE false")
    sent = 0
    with source.cursor().copy(f"COPY (SELECT {cols} FROM {qualified}) TO STDOUT") as reader:
        with target.cursor().copy(f"COPY {staging} ({cols}) FROM STDIN") as writer:
            for block in reader:
                writer.write(block)
                # psycopg yields memoryview blocks; count row terminators on bytes.
                sent += bytes(block).count(b"\n")

    cur = target.execute(
        f"INSERT INTO {qualified} ({cols}) SELECT {cols} FROM {staging} "
        f"ON CONFLICT ({conflict_key}) DO NOTHING"
    )
    inserted = max(cur.rowcount, 0)
    target.execute(f"DROP TABLE {staging}")
    return sent, inserted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="source DSN (read-only usage)")
    parser.add_argument("--target", required=True, help="destination DSN")
    args = parser.parse_args()

    with psycopg.connect(args.source) as source, psycopg.connect(args.target) as target:
        # Serialize timestamps in UTC on both ends regardless of server TimeZone.
        source.execute("SET TIME ZONE 'UTC'")
        target.execute("SET TIME ZONE 'UTC'")
        for qualified, conflict_key in TABLES:
            sent, inserted = copy_table(source, target, qualified, conflict_key)
            target.commit()
            print(f"{qualified}: streamed {sent} rows, inserted {inserted} new")
    return 0


if __name__ == "__main__":
    sys.exit(main())
