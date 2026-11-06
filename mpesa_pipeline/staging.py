"""Stage 2: build the staging layer from raw, then run data quality checks.

The cleaning logic itself is SQL (sql/staging/). This module only runs it.
That's the ELT split: Python orchestrates, the warehouse does the transforming.

Two ideas to look for:

1. FULL REBUILD. Every run truncates staging and rebuilds it from raw in one
   transaction. Re-running is safe by construction, and a failed build leaves
   the previous staging tables untouched. Raw is never modified, so a fixed
   cleaning rule only needs a rebuild, never a re-ingest.

2. CHECKS AS QUERIES. Each file in sql/checks/staging/ is a SELECT that returns the rows
   breaking one rule, so zero rows means pass. They run after the build and fail the
   command if any rule is broken. They catch problems the per-row constraints
   can't see, such as rows going missing or a quarantine rate jumping.
"""
import logging

from .config import SQL_DIR, DBConfig
from .db import run_sql_dir, transaction

log = logging.getLogger(__name__)

STAGING_SQL_DIR = SQL_DIR / "staging"


def build_staging(cfg: DBConfig | None = None) -> dict:
    """Rebuild every staging table from raw, atomically. Returns row counts."""
    with transaction(cfg) as conn:
        run_sql_dir(conn, STAGING_SQL_DIR)
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

