"""Stage 1: ingest landing-zone CSV files into the raw schema.

Four ideas to look for in this file:

1. DATA CONTRACT. Each source has an expected header. If the source renames or
   adds a column ("schema drift"), the file is rejected loudly instead of being
   loaded into the wrong columns without anyone noticing.

2. IDEMPOTENCY. raw.ingestion_log records every file's name and SHA-256
   fingerprint. Running ingest twice loads nothing new the second time. If a
   file is re-delivered with different contents, its old rows are deleted and
   replaced, so nothing is double-counted.

3. ATOMICITY. Each file is loaded inside one database transaction. A crash
   halfway through a file leaves zero rows from it, never half a file.

4. LOAD STRATEGIES. Transactions are loaded incrementally (one file per day,
   appended). Shops are a full refresh (the whole table is replaced by the
   latest snapshot).

It uses Postgres COPY rather than row-by-row INSERTs. COPY streams the whole
file in one command and is much faster as data grows.
"""
import csv
import hashlib
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .config import LANDING_DIR, DBConfig
from .db import transaction
from .generate import SHOP_HEADER, TX_HEADER

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceSpec:
    name: str
    pattern: str            # which files in the landing zone belong to this source
    table: str              # raw table they load into
    header_map: dict        # source header -> raw column (this IS the data contract)
    mode: str               # "append" (incremental) or "replace" (full refresh)
    daily_file: str | None = None  # the one file for a given day; None = not split by day


SOURCES = [
    # Shops first, so master data is in place before the transactions that reference it.
    SourceSpec(
        name="shops",
        pattern="shops.csv",
        table="raw.shops",
        header_map=dict(zip(SHOP_HEADER, [
            "till_number", "shop_name", "business_category", "town", "county", "opened_on",
        ])),
        mode="replace",
    ),
    SourceSpec(
        name="mpesa_transactions",
        pattern="mpesa_transactions_*.csv",
        table="raw.mpesa_transactions",
        header_map=dict(zip(TX_HEADER, [
            "receipt_no", "completion_time", "till_number", "shop_name", "customer_msisdn",
            "customer_name", "amount", "transaction_type", "linked_receipt_no",
        ])),
        mode="append",
        daily_file="mpesa_transactions_{day}.csv",
    ),
]


class SchemaDriftError(Exception):
    """The file's header does not match the agreed contract."""


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_header(path: Path) -> list[str]:
    with open(path, newline="", encoding="utf-8-sig") as f:  # utf-8-sig strips Excel's BOM
        return [h.strip() for h in next(csv.reader(f), [])]


def ingest_file(conn, path: Path, spec: SourceSpec) -> int | None:
    """Load one file. Returns the rows loaded, or None if the file was already loaded."""
    digest = file_sha256(path)
    with conn.cursor() as cur:
        cur.execute("SELECT file_sha256 FROM raw.ingestion_log WHERE file_name = %s", (path.name,))
        previous = cur.fetchone()
        if previous and previous[0] == digest:
            log.info("SKIP  %s (already loaded, unchanged)", path.name)
            return None

        header = read_header(path)
        expected = list(spec.header_map)
        if header != expected:
            missing = [c for c in expected if c not in header]
            extra = [c for c in header if c not in expected]
            raise SchemaDriftError(
                f"{path.name}: header does not match the contract for '{spec.name}'. "
                f"missing={missing} unexpected={extra}"
            )

        # Clear out whatever this load replaces (a re-delivered file, or the old snapshot).
        if spec.mode == "replace":
            cur.execute(f"TRUNCATE {spec.table}")
        else:
            cur.execute(f"DELETE FROM {spec.table} WHERE _source_file = %s", (path.name,))
            if cur.rowcount:
                log.warning("RELOAD %s: file changed since last load, replaced %d old rows",
                            path.name, cur.rowcount)

        # COPY into a temporary table. Its SERIAL column numbers the rows in file order,
        # which lets every raw row point back to its exact line in the source file.
        cols = list(spec.header_map.values())
        col_list = ", ".join(cols)
        cur.execute(
            f"CREATE TEMP TABLE _load (_row_id SERIAL, {', '.join(c + ' TEXT' for c in cols)}) "
            "ON COMMIT DROP"
        )
        with open(path, newline="", encoding="utf-8-sig") as f:
            cur.copy_expert(f"COPY _load ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)

        cur.execute(
            f"INSERT INTO {spec.table} ({col_list}, _source_file, _source_row) "
            f"SELECT {col_list}, %s, _row_id + 1 FROM _load ORDER BY _row_id",  # +1: header is line 1
            (path.name,),
        )
        n_rows = cur.rowcount

        cur.execute(
            """
            INSERT INTO raw.ingestion_log (file_name, target_table, file_sha256, row_count, ingested_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (file_name) DO UPDATE
               SET file_sha256 = EXCLUDED.file_sha256,
                   row_count   = EXCLUDED.row_count,
                   ingested_at = EXCLUDED.ingested_at
            """,
            (path.name, spec.table, digest, n_rows),
        )
    log.info("LOAD  %s -> %s (%d rows)", path.name, spec.table, n_rows)
    return n_rows


def daily_file(day: date, landing_dir: Path = LANDING_DIR) -> Path:
    """Where the transaction export for one business day is expected to land."""
    spec = next(s for s in SOURCES if s.daily_file)
    return Path(landing_dir) / spec.daily_file.format(day=day.isoformat())


def ingest_all(landing_dir: Path = LANDING_DIR, cfg: DBConfig | None = None, day: date | None = None) -> dict:
    """Ingest every file in the landing zone, one transaction per file.

    With `day`, only that day's transaction file is loaded (plus the shops
    snapshot). That's what one daily Airflow run does. A missing file for that
    day is an error, not "nothing to do".

    A bad file does not stop the good ones. The failures are collected and
    raised together at the end, so the run still shows up as failed (and an
    orchestrator like Airflow will alert on it) while every good file loads.
    """
    landing_dir = Path(landing_dir)
    if day is not None and not daily_file(day, landing_dir).exists():
        raise FileNotFoundError(f"No transaction file for {day}: expected {daily_file(day, landing_dir)}")

    summary = {"loaded": {}, "skipped": [], "failed": {}}
    for spec in SOURCES:
        if day is not None and spec.daily_file:
            paths = [daily_file(day, landing_dir)]
        else:
            paths = sorted(landing_dir.glob(spec.pattern))
        for path in paths:
            try:
                with transaction(cfg) as conn:
                    n = ingest_file(conn, path, spec)
                if n is None:
                    summary["skipped"].append(path.name)
                else:
                    summary["loaded"][path.name] = n
            except Exception as exc:  # noqa: BLE001 - we re-raise below
                log.error("FAIL  %s: %s", path.name, exc)
                summary["failed"][path.name] = str(exc)

    log.info("Ingest finished: %d loaded, %d skipped, %d failed",
             len(summary["loaded"]), len(summary["skipped"]), len(summary["failed"]))
    if summary["failed"]:
        raise RuntimeError(f"{len(summary['failed'])} file(s) failed to ingest: {list(summary['failed'])}")
    return summary
