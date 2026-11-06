"""Data quality checks, one folder per layer: sql/checks/<layer>/*.sql.

Each file is a SELECT that returns the rows breaking one rule, so zero rows
means pass. Checks are grouped by layer because each layer is built by its own
command: right after `build-staging`, the warehouse is legitimately out of date
until `build-warehouse` runs, so its checks shouldn't run yet.
"""
import logging

from .config import SQL_DIR, DBConfig
from .db import get_connection

log = logging.getLogger(__name__)

CHECKS_DIR = SQL_DIR / "checks"
LAYERS = ("staging", "warehouse")


class DataQualityError(Exception):
    """One or more data quality checks found violations."""


def run_checks(layer: str, cfg: DBConfig | None = None, raise_on_failure: bool = True) -> dict[str, int]:
    """Run every check for one layer. Returns {check name: number of failing rows}."""
    results = {}
    conn = get_connection(cfg)
    try:
        with conn.cursor() as cur:
            for path in sorted((CHECKS_DIR / layer).glob("*.sql")):
                query = path.read_text().strip().rstrip(";")
                cur.execute(f"SELECT COUNT(*) FROM (\n{query}\n) AS failures")
                n = cur.fetchone()[0]
                results[path.stem] = n
                if n:
                    log.error("FAIL  %s/%s (%d rows). Run the file to see them.", layer, path.stem, n)
                else:
                    log.info("PASS  %s/%s", layer, path.stem)
    finally:
        conn.close()

    failed = [name for name, n in results.items() if n]
    if failed and raise_on_failure:
        raise DataQualityError(f"{len(failed)} {layer} check(s) failed: {failed}")
    return results
