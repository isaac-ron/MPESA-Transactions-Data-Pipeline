"""Stage 2 tests: the staging layer graded against the generator's answer key.

Every expected number comes from the report that generate() returns, never a
hard-coded count, so these pass for any seed or dataset size.
"""
import pytest

from mpesa_pipeline.checks import DataQualityError, run_checks
from mpesa_pipeline.ingest import ingest_all
from mpesa_pipeline.staging import build_staging

from .conftest import query_one

CLEAN = "staging.mpesa_transactions"
QUAR = "staging.mpesa_transactions_quarantine"
# The earliest delivered raw row: always the copy that survives de-duplication.
FIRST_ROW = "SELECT _source_file, _source_row FROM raw.mpesa_transactions ORDER BY 1, 2 LIMIT 1"


@pytest.fixture()
def staged(clean_db, landing):
    out, report = landing
    ingest_all(out, clean_db)
    summary = build_staging(clean_db)
    return clean_db, report["planted_issues"], report, summary


def _both(cfg, clean_where, quar_where):
    """Count matching rows across staging and quarantine (each receipt is in exactly one)."""
    return (query_one(cfg, f"SELECT COUNT(*) FROM {CLEAN} WHERE {clean_where}")
            + query_one(cfg, f"SELECT COUNT(*) FROM {QUAR} WHERE {quar_where}"))


def test_duplicates_removed_exactly(staged):
    cfg, issues, report, _ = staged
    unique = report["rows_written"] - issues["duplicate_same_file"] - issues["duplicate_next_file"]
    assert _both(cfg, "true", "true") == unique
    assert query_one(cfg, "SELECT COUNT(DISTINCT receipt_no) FROM raw.mpesa_transactions") == unique


def test_each_quarantine_reason_matches_the_answer_key(staged):
    cfg, issues, _, summary = staged
    expected = {"invalid_amount": issues["invalid_amount"]}
    # The generator plants no other kind of invalid row, so any other reason is a cleaning bug.
    assert summary["quarantine_reasons"] == {k: v for k, v in expected.items() if v}


def test_clean_is_unique_minus_quarantined(staged):
    cfg, issues, report, _ = staged
    unique = report["rows_written"] - issues["duplicate_same_file"] - issues["duplicate_next_file"]
    quarantined = query_one(cfg, f"SELECT COUNT(DISTINCT receipt_no) FROM {QUAR}")
    assert query_one(cfg, f"SELECT COUNT(*) FROM {CLEAN}") == unique - quarantined


def test_orphan_tills_are_flagged_not_dropped(staged):
    cfg, issues, _, _ = staged
    assert _both(cfg, "is_orphan_till", "is_orphan_till") == issues["orphan_till"]


def test_reversals_found_despite_messy_casing(staged):
    cfg, issues, _, _ = staged
    assert _both(cfg, "transaction_type = 'Reversal'",
                 "initcap(btrim(transaction_type)) = 'Reversal'") == issues["reversal"]


def test_blank_customer_names_become_null(staged):
    cfg, issues, _, _ = staged
    assert _both(cfg, "customer_name IS NULL",
                 "NULLIF(btrim(customer_name), '') IS NULL") == issues["missing_customer_name"]
    # No leading/trailing/double spaces and no ALL-CAPS left behind.
    assert query_one(cfg, f"SELECT COUNT(*) FROM {CLEAN} WHERE customer_name ~ '(^\\s|\\s$|\\s\\s)' "
                          "OR customer_name <> initcap(customer_name)") == 0


def test_every_phone_is_canonical(staged):
    cfg, _, _, _ = staged
    assert query_one(cfg, f"SELECT COUNT(*) FROM {CLEAN} "
                          r"WHERE customer_msisdn !~ '^254(7\d{8}|11\d{7})$'") == 0


def test_one_shop_name_per_till(staged):
    cfg, _, _, _ = staged
    assert query_one(cfg, f"SELECT COUNT(*) FROM (SELECT till_number FROM {CLEAN} "
                          "GROUP BY till_number HAVING COUNT(DISTINCT shop_name) > 1) x") == 0


def test_late_arrivals_flagged(staged):
    # At least the planted late rows. Reversals that follow a late original into
    # the next file are late too, so this can't be an exact match.
    cfg, issues, _, _ = staged
    assert _both(cfg, "is_late_arrival", "is_late_arrival") >= issues["late_arrival"]


def test_day_first_dates_without_seconds(staged):
    cfg, _, _, _ = staged
    # "01/10/2026 06:41" must become 1 October 06:41:00, not 10 January.
    day_first = r"r.completion_time ~ '^\d{2}/'"
    assert query_one(cfg, f"SELECT COUNT(*) FROM raw.mpesa_transactions r JOIN {CLEAN} t USING (receipt_no) "
                          f"WHERE {day_first}") > 0
    assert query_one(cfg, f"SELECT COUNT(*) FROM raw.mpesa_transactions r JOIN {CLEAN} t USING (receipt_no) "
                          f"WHERE {day_first} AND to_char(t.completion_time, 'DD/MM/YYYY HH24:MI:SS') "
                          "<> r.completion_time || ':00'") == 0


def test_checks_pass_on_generated_data(staged):
    cfg, _, _, _ = staged
    assert set(run_checks("staging", cfg).values()) == {0}


def test_rebuild_is_idempotent(staged):
    cfg, _, _, first = staged
    assert build_staging(cfg) == first


def test_new_phone_format_is_quarantined(clean_db, landing):
    out, _ = landing
    ingest_all(out, clean_db)
    # The source starts sending a foreign number. It must not slip into staging.
    receipt = query_one(clean_db, f"UPDATE raw.mpesa_transactions SET customer_msisdn = '+44 7700 900123' "
                                  f"WHERE (_source_file, _source_row) = ({FIRST_ROW}) RETURNING receipt_no")
    summary = build_staging(clean_db)
    assert summary["quarantine_reasons"]["invalid_msisdn"] == 1
    assert "invalid_msisdn" in query_one(clean_db, f"SELECT reasons FROM {QUAR} WHERE receipt_no = %s", (receipt,))


def test_quarantine_rate_check_fails_the_run(clean_db, landing):
    out, _ = landing
    ingest_all(out, clean_db)
    # Blank out 50 amounts: far more than the usual handful of invalid rows.
    query_one(clean_db, "UPDATE raw.mpesa_transactions SET amount = '' WHERE (_source_file, _source_row) IN "
                        "(SELECT _source_file, _source_row FROM raw.mpesa_transactions ORDER BY 1, 2 LIMIT 50) "
                        "RETURNING 1")
    build_staging(clean_db)
    with pytest.raises(DataQualityError, match="quarantine_rate"):
        run_checks("staging", clean_db)
