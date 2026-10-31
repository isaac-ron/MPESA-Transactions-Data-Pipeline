"""Synthetic M-Pesa till data: the stand-in for a real source system.

What it writes into data/landing/:
  shops.csv                          shop master data (full snapshot)
  mpesa_transactions_YYYY-MM-DD.csv  one export file per day

Real exports are messy, so this one is too, on purpose. It plants:
  - duplicate rows (in the same file, and re-sent in the next file)
  - "late" rows: a transaction from day D that only shows up in day D+1's file
  - mixed date formats, amounts like "KES 1,200.00", and four phone formats
  - inconsistent name casing and whitespace, and missing names
  - reversals (negative amounts that point back to the original receipt)
  - unknown till numbers that are not in shops.csv (orphan rows)
  - invalid amounts (zero or blank)

It also writes data/generation_report.json, an answer key counting each
planted issue. In Stage 2 the cleaning code is checked against it.

The generator is seeded, so the same --seed always produces the same files.
"""
import csv
import json
import logging
import math
import random
import string
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

TX_HEADER = [
    "Receipt No.", "Completion Time", "Till Number", "Shop Name", "Customer MSISDN",
    "Customer Name", "Amount", "Transaction Type", "Linked Receipt No.",
]
SHOP_HEADER = ["Till Number", "Shop Name", "Business Category", "Town", "County", "Opened On"]

FIRST_NAMES = [
    "Wanjiru", "Kamau", "Achieng", "Otieno", "Njeri", "Kiprono", "Chebet", "Mwangi", "Akinyi",
    "Omondi", "Wambui", "Kiptoo", "Nafula", "Wekesa", "Moraa", "Nyaboke", "Mutua", "Mwikali",
    "Jeptoo", "Barasa", "Atieno", "Ouma", "Wairimu", "Kibet", "Auma", "Onyango", "Muthoni",
    "Kariuki", "Jelagat", "Makena", "Brian", "Faith", "Kevin", "Mercy", "Dennis", "Esther",
    "Ian", "Sharon", "Collins", "Purity", "Victor", "Grace", "Allan", "Joy", "Felix", "Ruth",
]
LAST_NAMES = [
    "Kamau", "Otieno", "Mwangi", "Ochieng", "Kiprop", "Wanjala", "Njoroge", "Odhiambo", "Cheruiyot",
    "Mutiso", "Nyamweya", "Kimani", "Owino", "Rotich", "Wafula", "Maina", "Ndungu", "Okoth",
    "Langat", "Simiyu", "Kiplagat", "Muriuki", "Onyango", "Kilonzo", "Nyambura", "Omondi", "Koech",
]

TOWNS = {
    "Nairobi": ["Westlands", "Kasarani", "Embakasi", "CBD", "Rongai"],
    "Mombasa": ["Nyali", "Likoni", "Bamburi"],
    "Kisumu": ["Kisumu Central", "Kondele"],
    "Nakuru": ["Nakuru Town", "Naivasha", "Njoro"],
    "Uasin Gishu": ["Eldoret"],
    "Kakamega": ["Kakamega Town", "Mumias"],
    "Machakos": ["Machakos Town", "Athi River"],
    "Nyeri": ["Nyeri Town", "Karatina"],
    "Kisii": ["Kisii Town"],
    "Bungoma": ["Bungoma Town", "Webuye"],
}
# Counties weighted roughly by commercial activity.
COUNTY_WEIGHTS = {
    "Nairobi": 6, "Mombasa": 3, "Kisumu": 2, "Nakuru": 3, "Uasin Gishu": 2,
    "Kakamega": 1, "Machakos": 1, "Nyeri": 1, "Kisii": 1, "Bungoma": 1,
}

# category: (shop-name suffix, median ticket in KES, spread, typical transactions per day)
CATEGORIES = {
    "Grocery":     ("Groceries",    450, 0.8, 55),
    "Pharmacy":    ("Chemist",      800, 0.7, 25),
    "Hardware":    ("Hardware",    2500, 0.9, 12),
    "Electronics": ("Electronics", 6000, 0.9, 6),
    "Butchery":    ("Butchery",     700, 0.6, 35),
    "Salon":       ("Beauty Salon", 600, 0.5, 14),
    "Restaurant":  ("Hotel",        350, 0.5, 45),
    "Agrovet":     ("Agrovet",     1500, 0.8, 12),
    "Boutique":    ("Boutique",    1800, 0.7, 10),
    "Cyber Cafe":  ("Cyber",        100, 0.6, 28),
}
SHOP_PREFIXES = [
    "Mama Njeri", "Baraka", "Neema", "Upendo", "Jirani", "Kwa Otieno", "Wanjiku", "Faraja",
    "Tumaini", "Imani", "Amani", "Bidii", "Mwangaza", "Kilele", "Shalom", "Zawadi", "Fahari",
]

# Relative transaction volume by hour of day: a morning rush, lunch, and an evening peak.
HOUR_WEIGHTS = {6: 1, 7: 4, 8: 7, 9: 6, 10: 5, 11: 6, 12: 8, 13: 9, 14: 6, 15: 5,
                16: 6, 17: 9, 18: 10, 19: 8, 20: 5, 21: 2}


@dataclass
class MessRates:
    """Probability of each planted data quality issue. All rates are per row."""
    duplicate_same_file: float = 0.010
    duplicate_next_file: float = 0.004
    late_arrival: float = 0.020
    reversal: float = 0.005
    alt_date_format: float = 0.06
    formatted_amount: float = 0.07
    alt_phone_format: float = 0.30
    messy_customer_name: float = 0.08
    missing_customer_name: float = 0.03
    messy_shop_name: float = 0.04
    messy_tx_type: float = 0.03
    orphan_till: float = 0.003
    invalid_amount: float = 0.002


def _poisson(rng: random.Random, lam: float) -> int:
    # Knuth's algorithm: fine for the small daily counts used here.
    l, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= l:
            return k
        k += 1


class _Generator:
    def __init__(self, seed: int, n_shops: int, n_customers: int, mess: MessRates):
        self.rng = random.Random(seed)
        self.mess = mess
        self.used_receipts: set[str] = set()
        self.report = {k: 0 for k in asdict(mess)}
        self.report["clean_rows_generated"] = 0
        self.shops = self._make_shops(n_shops)
        self.customers = self._make_customers(n_customers)
        self._assign_regulars()

    # ---------- master data ----------
    def _make_shops(self, n: int) -> list[dict]:
        rng, shops, used_names, used_tills = self.rng, [], set(), set()
        counties, weights = zip(*COUNTY_WEIGHTS.items())
        cats = list(CATEGORIES)
        while len(shops) < n:
            county = rng.choices(counties, weights)[0]
            cat = rng.choice(cats)
            name = f"{rng.choice(SHOP_PREFIXES)} {CATEGORIES[cat][0]}"
            till = str(rng.randint(100000, 9999999))
            if name in used_names or till in used_tills:
                continue
            used_names.add(name)
            used_tills.add(till)
            shops.append({
                "till_number": till,
                "shop_name": name,
                "category": cat,
                "town": rng.choice(TOWNS[county]),
                "county": county,
                "opened_on": date(2015, 1, 1) + timedelta(days=rng.randint(0, 3600)),
                # Some shops are simply busier than others.
                "size_factor": rng.uniform(0.5, 1.8),
            })
        return shops

    def _make_customers(self, n: int) -> list[dict]:
        rng, customers, used = self.rng, [], set()
        counties, weights = zip(*COUNTY_WEIGHTS.items())
        while len(customers) < n:
            prefix = rng.choice(["2547", "2547", "2547", "25411"])  # 07xx and 011x lines
            msisdn = prefix + "".join(rng.choices(string.digits, k=12 - len(prefix)))
            if msisdn in used:
                continue
            used.add(msisdn)
            customers.append({
                "msisdn": msisdn,
                "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "home_county": rng.choices(counties, weights)[0],
            })
        return customers

    def _assign_regulars(self) -> None:
        """Each customer has 1-3 favourite shops, usually in their home county."""
        self.regulars: dict[str, list[dict]] = {s["till_number"]: [] for s in self.shops}
        by_county: dict[str, list[dict]] = {}
        for s in self.shops:
            by_county.setdefault(s["county"], []).append(s)
        for c in self.customers:
            local = by_county.get(c["home_county"]) or self.shops
            for _ in range(self.rng.randint(1, 3)):
                pool = local if self.rng.random() < 0.85 else self.shops
                self.regulars[self.rng.choice(pool)["till_number"]].append(c)

    # ---------- transactions ----------
    def _receipt(self) -> str:
        alphabet = string.ascii_uppercase + string.digits
        while True:
            r = self.rng.choice(string.ascii_uppercase) + "".join(self.rng.choices(alphabet, k=9))
            if r not in self.used_receipts:
                self.used_receipts.add(r)
                return r

    def _day_factor(self, d: date, category: str) -> float:
        f = 1.0
        if d.weekday() == 5:  # Saturday
            f *= 1.2
        elif d.weekday() == 6:  # Sunday
            f *= 1.3 if category == "Restaurant" else 0.7
        if d.day >= 28 or d.day <= 3:  # month-end payday spending
            f *= 1.25
        return f

    def _amount(self, category: str) -> int:
        _, median, sigma, _ = CATEGORIES[category]
        amt = self.rng.lognormvariate(math.log(median), sigma)
        return max(10, int(round(amt, -1 if amt < 1000 else -2)))  # shops price in round figures

    def day_transactions(self, d: date) -> list[dict]:
        """Clean, canonical transactions that happened on day d."""
        rows, rng = [], self.rng
        hours, hweights = zip(*HOUR_WEIGHTS.items())
        for shop in self.shops:
            base = CATEGORIES[shop["category"]][3]
            n = _poisson(rng, base * shop["size_factor"] * self._day_factor(d, shop["category"]))
            pool = self.regulars[shop["till_number"]]
            for _ in range(n):
                cust = rng.choice(pool) if pool and rng.random() < 0.8 else rng.choice(self.customers)
                ts = datetime(d.year, d.month, d.day, rng.choices(hours, hweights)[0],
                              rng.randint(0, 59), rng.randint(0, 59))
                rows.append({
                    "receipt_no": self._receipt(), "time": ts, "shop": shop, "customer": cust,
                    "amount": self._amount(shop["category"]), "type": "Buy Goods", "linked": "",
                })
        self.report["clean_rows_generated"] += len(rows)
        return rows

    def reversal_for(self, tx: dict) -> dict:
        self.report["reversal"] += 1
        return {
            "receipt_no": self._receipt(),
            "time": tx["time"] + timedelta(minutes=self.rng.randint(5, 600)),
            "shop": tx["shop"], "customer": tx["customer"], "amount": -tx["amount"],
            "type": "Reversal", "linked": tx["receipt_no"],
        }

    # ---------- making it messy ----------
    def _hit(self, issue: str) -> bool:
        if self.rng.random() < getattr(self.mess, issue):
            self.report[issue] += 1
            return True
        return False

    def to_csv_row(self, tx: dict) -> list[str]:
        """Render one canonical transaction the way a messy source would."""
        rng = self.rng
        ts: datetime = tx["time"]
        time_s = ts.strftime("%Y-%m-%d %H:%M:%S")
        if self._hit("alt_date_format"):
            time_s = rng.choice([ts.strftime("%d/%m/%Y %H:%M"), ts.strftime("%Y-%m-%dT%H:%M:%S")])

        amt = tx["amount"]
        amount_s = f"{amt:.2f}"
        if self._hit("invalid_amount"):
            amount_s = rng.choice(["0.00", ""])
        elif self._hit("formatted_amount"):
            amount_s = rng.choice([f"KES {amt:,.2f}", f"{amt:,}"])

        m = tx["customer"]["msisdn"]  # canonical form: 2547XXXXXXXX
        if self._hit("alt_phone_format"):
            m = rng.choice(["0" + m[3:], "+" + m, m[3:]])

        name = tx["customer"]["name"]
        if self._hit("missing_customer_name"):
            name = ""
        elif self._hit("messy_customer_name"):
            name = rng.choice([name.upper(), name.lower(), f"  {name} ", name.replace(" ", "  ")])

        shop_name, till = tx["shop"]["shop_name"], tx["shop"]["till_number"]
        if self._hit("orphan_till"):
            till = str(rng.randint(10000000, 99999999))  # 8 digits: never a real till here
        elif self._hit("messy_shop_name"):
            shop_name = rng.choice([shop_name.lower(), shop_name + " ", shop_name.upper()])

        tx_type = tx["type"]
        if self._hit("messy_tx_type"):
            tx_type = rng.choice([tx_type.upper(), tx_type.lower()])

        return [tx["receipt_no"], time_s, till, shop_name, m, name, amount_s, tx_type, tx["linked"]]


def generate(out_dir: Path, start: date, days: int, n_shops: int = 25, n_customers: int = 2000,
             seed: int = 42, mess: MessRates | None = None) -> dict:
    """Write shops.csv plus one transaction file per day. Returns the answer-key report."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    g = _Generator(seed, n_shops, n_customers, mess or MessRates())

    with open(out_dir / "shops.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(SHOP_HEADER)
        for s in g.shops:
            w.writerow([s["till_number"], s["shop_name"], s["category"], s["town"], s["county"],
                        s["opened_on"].isoformat()])

    # Decide which file each row lands in. Late rows and reversals can spill into a later file.
    files: dict[date, list[dict]] = {start + timedelta(days=i): [] for i in range(days)}
    last_day = start + timedelta(days=days - 1)
    for i in range(days):
        d = start + timedelta(days=i)
        for tx in g.day_transactions(d):
            target = d
            if d < last_day and g._hit("late_arrival"):
                target = d + timedelta(days=1)
            files[target].append(tx)
            if g.rng.random() < g.mess.reversal:
                rev = g.reversal_for(tx)
                rev_day = min(rev["time"].date(), last_day)
                files[max(rev_day, target)].append(rev)

    # Render rows, then plant duplicates (exact copies, as when a source re-sends data).
    rendered: dict[date, list[list[str]]] = {d: [] for d in files}
    for d in sorted(files):
        for tx in sorted(files[d], key=lambda t: t["time"]):
            row = g.to_csv_row(tx)
            rendered[d].append(row)
            if g._hit("duplicate_same_file"):
                rendered[d].append(list(row))
            nxt = d + timedelta(days=1)
            if nxt in rendered and g._hit("duplicate_next_file"):
                rendered[nxt].append(list(row))

    paths = []
    for d, rows in rendered.items():
        p = out_dir / f"mpesa_transactions_{d.isoformat()}.csv"
        with open(p, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(TX_HEADER)
            w.writerows(rows)
        paths.append(p)

    report = {
        "seed": seed, "start": start.isoformat(), "days": days, "shops": len(g.shops),
        "customers": len(g.customers), "files": [p.name for p in paths],
        "rows_written": sum(len(r) for r in rendered.values()),
        "planted_issues": g.report,
    }
    report_path = out_dir.parent / "generation_report.json"
    report_path.write_text(json.dumps(report, indent=2))
    log.info("Wrote %d transaction files (%d rows) to %s", len(paths), report["rows_written"], out_dir)
    return report
