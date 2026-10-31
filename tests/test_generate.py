import csv
from datetime import date

from mpesa_pipeline.generate import SHOP_HEADER, TX_HEADER, MessRates, generate


def _read(path):
    with open(path, newline="") as f:
        return list(csv.reader(f))


def test_same_seed_gives_identical_files(tmp_path):
    a = generate(tmp_path / "a", date(2026, 10, 1), days=2, n_shops=4, n_customers=50, seed=1)
    b = generate(tmp_path / "b", date(2026, 10, 1), days=2, n_shops=4, n_customers=50, seed=1)
    assert a["planted_issues"] == b["planted_issues"]
    for name in a["files"]:
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()


def test_files_follow_the_contract(landing):
    out, report = landing
    assert _read(out / "shops.csv")[0] == SHOP_HEADER
    for name in report["files"]:
        rows = _read(out / name)
        assert rows[0] == TX_HEADER
        assert all(len(r) == len(TX_HEADER) for r in rows)


def test_clean_mode_plants_nothing(tmp_path):
    zero = MessRates(**{k: 0.0 for k in MessRates.__dataclass_fields__})
    report = generate(tmp_path / "l", date(2026, 10, 1), days=2, n_shops=4, n_customers=50,
                      seed=3, mess=zero)
    issues = {k: v for k, v in report["planted_issues"].items() if k != "clean_rows_generated"}
    assert set(issues.values()) == {0}
    receipts = [r[0] for name in report["files"] for r in _read(tmp_path / "l" / name)[1:]]
    assert len(receipts) == len(set(receipts)), "no duplicates expected when mess is off"


def test_reversals_point_to_a_real_receipt(tmp_path):
    report = generate(tmp_path / "l", date(2026, 10, 1), days=5, n_shops=8, n_customers=200,
                      seed=11, mess=MessRates(reversal=0.2))
    rows = [r for name in report["files"] for r in _read(tmp_path / "l" / name)[1:]]
    receipts = {r[0] for r in rows}
    reversals = [r for r in rows if r[7].lower() == "reversal"]
    assert reversals
    assert all(r[8] in receipts for r in reversals)
