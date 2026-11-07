"""Ingestion tests: each one checks a guarantee from the docstring in ingest.py."""
import csv
from datetime import date, timedelta

import pytest

from mpesa_pipeline.ingest import ingest_all

from .conftest import query_one


def _total_rows(out, pattern):
    n = 0
    for p in out.glob(pattern):
        with open(p, newline="") as f:
            n += sum(1 for _ in csv.reader(f)) - 1  # minus header
    return n


def test_first_load_copies_every_row(clean_db, landing):
    out, _ = landing
    summary = ingest_all(out, clean_db)
    assert len(summary["loaded"]) == 4  # shops + 3 daily files
    assert query_one(clean_db, "SELECT COUNT(*) FROM raw.mpesa_transactions") == \
        _total_rows(out, "mpesa_transactions_*.csv")
    assert query_one(clean_db, "SELECT COUNT(*) FROM raw.shops") == 5


def test_rerun_is_idempotent(clean_db, landing):
    out, _ = landing
    ingest_all(out, clean_db)
    before = query_one(clean_db, "SELECT COUNT(*) FROM raw.mpesa_transactions")
    summary = ingest_all(out, clean_db)
    assert summary["loaded"] == {}
    assert len(summary["skipped"]) == 4
    assert query_one(clean_db, "SELECT COUNT(*) FROM raw.mpesa_transactions") == before


def test_redelivered_file_replaces_old_rows(clean_db, landing):
    out, report = landing
    ingest_all(out, clean_db)
    target = out / report["files"][0]
    rows = list(csv.reader(open(target, newline="")))
    with open(target, "w", newline="") as f:  # the source re-sends the file with 2 rows removed
        csv.writer(f).writerows(rows[:-2])

    summary = ingest_all(out, clean_db)
    assert list(summary["loaded"]) == [target.name]
    n = query_one(clean_db, "SELECT COUNT(*) FROM raw.mpesa_transactions WHERE _source_file = %s",
                  (target.name,))
    assert n == len(rows) - 3  # minus header, minus the 2 removed rows


def test_schema_drift_is_rejected_but_other_files_still_load(clean_db, landing):
    out, report = landing
    bad = out / report["files"][1]
    rows = list(csv.reader(open(bad, newline="")))
    rows[0][4] = "Phone Number"  # the source renamed a column without telling us
    with open(bad, "w", newline="") as f:
        csv.writer(f).writerows(rows)

    with pytest.raises(RuntimeError, match="1 file"):
        ingest_all(out, clean_db)
    assert query_one(clean_db, "SELECT COUNT(*) FROM raw.ingestion_log") == 3
    assert query_one(clean_db, "SELECT COUNT(*) FROM raw.mpesa_transactions WHERE _source_file = %s",
                     (bad.name,)) == 0


def test_corrupt_file_leaves_no_partial_rows(clean_db, landing):
    out, report = landing
    bad = out / report["files"][2]
    with open(bad, "a", newline="") as f:
        f.write('BROKEN,"unterminated quote\n')  # the file gets cut off mid-row

    with pytest.raises(RuntimeError):
        ingest_all(out, clean_db)
    assert query_one(clean_db, "SELECT COUNT(*) FROM raw.mpesa_transactions WHERE _source_file = %s",
                     (bad.name,)) == 0


def test_every_raw_row_traces_back_to_its_source_line(clean_db, landing):
    out, report = landing
    ingest_all(out, clean_db)
    name = report["files"][0]
    rows = list(csv.reader(open(out / name, newline="")))
    line_no = 5
    receipt = query_one(clean_db,
                        "SELECT receipt_no FROM raw.mpesa_transactions "
                        "WHERE _source_file = %s AND _source_row = %s", (name, line_no))
    assert receipt == rows[line_no - 1][0]


def test_single_day_loads_only_that_days_file(clean_db, landing):
    # What one daily Airflow run does: the shops snapshot plus one transaction file.
    out, report = landing
    day = date.fromisoformat(report["start"]) + timedelta(days=1)
    summary = ingest_all(out, clean_db, day=day)
    assert sorted(summary["loaded"]) == ["mpesa_transactions_" + day.isoformat() + ".csv", "shops.csv"]
    assert query_one(clean_db, "SELECT COUNT(DISTINCT _source_file) FROM raw.mpesa_transactions") == 1


def test_single_day_with_missing_file_is_an_error(clean_db, landing):
    out, _ = landing
    with pytest.raises(FileNotFoundError, match="2030-01-01"):
        ingest_all(out, clean_db, day=date(2030, 1, 1))
    assert query_one(clean_db, "SELECT COUNT(*) FROM raw.ingestion_log") == 0
