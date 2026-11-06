"""Stage 3 tests: the star schema reconciles with staging and the answer key,
and the shop dimension handles change the way SCD Type 2 promises.

Shop-master changes are simulated the realistic way: edit shops.csv in the
landing zone, re-ingest (the full refresh replaces raw.shops), rebuild.
"""
import csv

import pytest

from mpesa_pipeline.checks import run_checks
from mpesa_pipeline.ingest import ingest_all
from mpesa_pipeline.staging import build_staging
from mpesa_pipeline.warehouse import build_warehouse

from .conftest import query_one

FACT = "warehouse.fact_transactions"


@pytest.fixture()
def built(clean_db, landing):
    out, report = landing
    ingest_all(out, clean_db)
    build_staging(clean_db)
    summary = build_warehouse(clean_db)
    return clean_db, out, report, summary


def _rebuild_with_shops(cfg, out, edit):
    """Apply edit(rows) to shops.csv (rows[0] is the header), then re-run the whole pipeline."""
    path = out / "shops.csv"
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    edit(rows)
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)
    ingest_all(out, cfg)
    build_staging(cfg)
    return build_warehouse(cfg)


def test_facts_reconcile_with_the_answer_key(built):
    cfg, _, report, summary = built
    issues = report["planted_issues"]
    unique = report["rows_written"] - issues["duplicate_same_file"] - issues["duplicate_next_file"]
    quarantined = query_one(cfg, "SELECT COUNT(DISTINCT receipt_no) FROM staging.mpesa_transactions_quarantine")
    assert summary["facts"] == unique - quarantined
    assert query_one(cfg, f"SELECT SUM(amount) FROM {FACT}") == \
        query_one(cfg, "SELECT SUM(amount) FROM staging.mpesa_transactions")


def test_orphans_land_on_the_unknown_shop(built):
    cfg, _, report, summary = built
    quarantined_orphans = query_one(
        cfg, "SELECT COUNT(*) FROM staging.mpesa_transactions_quarantine WHERE is_orphan_till")
    assert summary["unknown_shop_facts"] + quarantined_orphans == report["planted_issues"]["orphan_till"]
    assert query_one(cfg, "SELECT shop_name FROM warehouse.dim_shop WHERE shop_key = 0") == "Unknown shop"


def test_dimensions_have_one_row_per_natural_key(built):
    cfg, _, report, summary = built
    assert summary["shops"] == summary["shop_versions"] == report["shops"]
    assert summary["customers"] == query_one(
        cfg, "SELECT COUNT(DISTINCT customer_msisdn) FROM staging.mpesa_transactions")


def test_dim_date_covers_whole_months(built):
    cfg, _, _, summary = built
    # The fixture covers 1-3 Oct, so dim_date holds all 31 days of October, sales or not.
    assert summary["dates"] == 31
    assert query_one(cfg, "SELECT COUNT(*) FROM warehouse.dim_date WHERE date_key = 20261031") == 1


def test_warehouse_checks_pass(built):
    cfg, _, _, _ = built
    assert set(run_checks("warehouse", cfg).values()) == {0}


def test_rebuild_is_idempotent_and_keeps_keys_stable(built):
    cfg, _, _, first = built
    keys = "SELECT string_agg(customer_msisdn || ':' || customer_key, ',' ORDER BY customer_msisdn) " \
           "FROM warehouse.dim_customer"
    before = query_one(cfg, keys)
    assert build_warehouse(cfg) == first
    assert query_one(cfg, keys) == before


def test_shop_change_creates_a_new_version(built):
    cfg, out, _, first = built
    till = query_one(cfg, "SELECT till_number FROM staging.shops ORDER BY till_number LIMIT 1")

    def move_town(rows):
        for r in rows[1:]:
            if r[0] == till:
                r[3] = "New Town"

    summary = _rebuild_with_shops(cfg, out, move_town)
    assert summary["shops"] == first["shops"]               # still one current version per shop
    assert summary["shop_versions"] == first["shop_versions"] + 1
    assert summary["facts"] == first["facts"]

    old_to, new_from = query_one(cfg, f"""
        SELECT ARRAY[MAX(valid_to) FILTER (WHERE NOT is_current), MAX(valid_from) FILTER (WHERE is_current)]
        FROM warehouse.dim_shop WHERE till_number = '{till}'""")
    assert old_to == new_from                               # no gap, no overlap
    assert query_one(cfg, f"SELECT town FROM warehouse.dim_shop WHERE till_number = '{till}' AND is_current") \
        == "New Town"
    assert set(run_checks("warehouse", cfg).values()) == {0}

    # Unchanged snapshot -> no new version.
    assert build_warehouse(cfg)["shop_versions"] == summary["shop_versions"]


def test_late_arriving_shop_adopts_its_orphan_facts(built):
    cfg, out, _, first = built
    orphan = query_one(cfg, "SELECT till_number FROM staging.mpesa_transactions WHERE is_orphan_till "
                            "ORDER BY till_number LIMIT 1")
    orphan_facts = query_one(cfg, "SELECT COUNT(*) FROM staging.mpesa_transactions WHERE till_number = %s",
                             (orphan,))

    def add_missing_shop(rows):
        rows.append([orphan, "Late Listed Shop", "Grocery", "Nyali", "Mombasa", "2020-01-01"])

    summary = _rebuild_with_shops(cfg, out, add_missing_shop)
    assert summary["unknown_shop_facts"] == first["unknown_shop_facts"] - orphan_facts
    assert query_one(cfg, f"""
        SELECT COUNT(*) FROM {FACT} f JOIN warehouse.dim_shop s USING (shop_key)
        WHERE s.till_number = %s""", (orphan,)) == orphan_facts
    assert set(run_checks("warehouse", cfg).values()) == {0}
