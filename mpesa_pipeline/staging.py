"""Stage 2: build the staging layer from raw, then run data quality checks.

The cleaning logic itself is SQL (sql/staging/). This module only runs it.
That's the ELT split: Python orchestrates, the warehouse does the transforming.

Two ideas to look for:

1. FULL REBUILD. Every run truncates staging and rebuilds it from raw in one
   transaction. Re-running is safe by construction, and a failed build leaves
   the previous staging tables untouched. Raw is never modified, so a fixed
   cleaning rule only needs a rebuild, never a re-ingest.

2. CHECKS AS QUERIES. Each file in sql/checks/ is a SELECT that returns the rows
   breaking one rule, so zero rows means pass. They run after the build and fail the
   command if any rule is broken. They catch problems the per-row constraints
   can't see, such as rows going missing or a quarantine rate jumping.
"""
import logging

from .config import SQL_DIR, DBConfig
from .db import get_connection, run_sql_file, transaction

log = logging.getLogger(__name__)

STAGING_SQL_DIR = SQL_DIR / "staging"
CHECKS_DIR = SQL_DIR / "checks"


class DataQualityError(Exception):
    """One or more data quality checks found violations."""


def build_staging(cfg: DBConfig | None = None) -> dict:
    """Rebuild every staging table from raw, atomically. Returns row counts."""
    with transaction(cfg) as conn:
        for path in sorted(STAGING_SQL_DIR.glob("*.sql")):
            run_sql_file(conn, path)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM staging.shops")
            shops = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE is_orphan_till) FROM staging.mpesa_transactions")
            clean, orphans = cur.fetchone()
            cur.execute(
                "SELECT reason, COUNT(*) FROM staging.mpesa_transactions_quarantine, unnest(reasons) AS reason "
                "GROUP BY reason ORDER BY reason"
            )
            quarantine_reasons = dict(cur.fetchall())
            cur.execute("SELECT COUNT(*) FROM staging.mpesa_transactions_quarantine")
            quarantined = cur.fetchone()[0]

    log.info("Staging built: %d shops, %d clean transactions (%d on orphan tills), %d quarantined",
             shops, clean, orphans, quarantined)
    for reason, n in quarantine_reasons.items():
        log.warning("QUARANTINE %-28s %d rows", reason, n)
    return {"shops": shops, "clean": clean, "orphan_till": orphans,
            "quarantined": quarantined, "quarantine_reasons": quarantine_reasons}


def run_checks(cfg: DBConfig | None = None, raise_on_failure: bool = True) -> dict[str, int]:
    """Run every check in sql/checks/. Returns {check name: number of failing rows}."""
    results = {}
    conn = get_connection(cfg)
    try:
        with conn.cursor() as cur:
            for path in sorted(CHECKS_DIR.glob("*.sql")):
                query = path.read_text().strip().rstrip(";")
                cur.execute(f"SELECT COUNT(*) FROM (\n{query}\n) AS failures")
                n = cur.fetchone()[0]
                results[path.stem] = n
                if n:
                    log.error("FAIL  %s (%d rows). Run the file to see them.", path.stem, n)
                else:
                    log.info("PASS  %s", path.stem)
    finally:
        conn.close()

    failed = [name for name, n in results.items() if n]
    if failed and raise_on_failure:
        raise DataQualityError(f"{len(failed)} check(s) failed: {failed}")
    return results
