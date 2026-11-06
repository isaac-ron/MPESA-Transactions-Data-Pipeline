"""Stage 3: build the star schema (warehouse layer) from staging.

Like Stage 2, the logic is SQL (sql/warehouse/) and this module only runs it,
in one transaction, dimensions first so the fact table can look up their keys.

Each table uses the load pattern that fits it:

  dim_date          generated calendar, insert missing days     (only grows)
  dim_shop          SCD Type 2 merge: close changed versions,   (keeps history, so it
                    open new ones                                is never truncated)
  dim_customer      SCD Type 1 upsert: overwrite attributes     (keys stay stable)
  fact_transactions full rebuild from staging                   (a pure function of
                                                                 staging + dims)
"""
import logging

from .config import SQL_DIR, DBConfig
from .db import run_sql_dir, transaction

log = logging.getLogger(__name__)

WAREHOUSE_SQL_DIR = SQL_DIR / "warehouse"


def build_warehouse(cfg: DBConfig | None = None) -> dict:
    """Build every warehouse table from staging, atomically. Returns row counts."""
    with transaction(cfg) as conn:
        run_sql_dir(conn, WAREHOUSE_SQL_DIR)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT (SELECT COUNT(*) FROM warehouse.dim_date),
                       (SELECT COUNT(*) FROM warehouse.dim_shop WHERE shop_key <> 0),
                       (SELECT COUNT(*) FROM warehouse.dim_shop WHERE shop_key <> 0 AND is_current),
                       (SELECT COUNT(*) FROM warehouse.dim_customer),
                       (SELECT COUNT(*) FROM warehouse.fact_transactions),
                       (SELECT COUNT(*) FROM warehouse.fact_transactions WHERE shop_key = 0)
            """)
            dates, shop_versions, shops, customers, facts, unknown_shop = cur.fetchone()

    log.info("Warehouse built: %d facts (%d on Unknown shop), %d customers, %d shops (%d versions), %d dates",
             facts, unknown_shop, customers, shops, shop_versions, dates)
    return {"dates": dates, "shops": shops, "shop_versions": shop_versions, "customers": customers,
            "facts": facts, "unknown_shop_facts": unknown_shop}
