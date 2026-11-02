# M-Pesa Sales Pipeline: an end-to-end data engineering project

A batch pipeline that takes daily M-Pesa till exports from 25 (synthetic) Kenyan
shops, lands them in a Postgres warehouse, cleans them, models them into a star
schema, and runs on a schedule with Airflow. It is built one stage at a time,
so each concept can be seen working on its own before the next one is added.

```mermaid
flowchart LR
    G[generate.py<br/>source system] -->|daily CSVs| L[(data/landing)]
    L -->|Stage 1: ingest.py<br/>COPY, manifest| R[(raw schema<br/>all TEXT)]
    R -->|Stage 2: clean + dedupe<br/>quality checks| S[(staging schema<br/>typed, valid)]
    S -->|Stage 3: model| W[(warehouse schema<br/>star schema)]
    W --> Q[SQL analytics]
    A{{Stage 4: Airflow DAG}} -. schedules .-> L & R & S & W
```

## Roadmap

| Stage | What gets built | Concepts |
|---|---|---|
| **1. Ingest** (done) | Data generator, raw layer, idempotent loader | Landing zone, data contracts, idempotency, atomic loads, lineage, full refresh vs incremental |
| 2. Clean | `staging` tables, cleaning rules, quality checks graded against the answer key | Type casting, deduplication, normalisation, late-arriving data, quarantine, data quality tests |
| 3. Model | `warehouse` star schema: `fact_transactions` + date/shop/customer dims | Dimensional modelling, surrogate keys, grain, SCD, analytical SQL |
| 4. Orchestrate | Airflow in Docker Compose, a daily DAG | DAGs, task dependencies, retries, backfills, scheduling |
| 5. Harden | End-to-end tests, a dashboard, a write-up | Monitoring, documentation, portfolio presentation |

## Running Stage 1

You need Docker Desktop and [uv](https://docs.astral.sh/uv/) (uv installs Python 3.13 for you if it's missing).

```bash
# 1. Start Postgres and Adminer
cp .env.example .env              # set PG_PORT=5433 here if 5432 is taken
docker compose up -d
docker compose ps                 # wait until postgres shows "healthy"

# 2. Python environment (creates .venv from pyproject.toml + uv.lock)
uv sync

# 3. Run the pipeline
uv run mpesa-pipeline generate     # 30 days of data -> data/landing/
uv run mpesa-pipeline init-db      # create schemas + tables
uv run mpesa-pipeline ingest       # load everything into raw.*
uv run mpesa-pipeline ingest       # run it again: everything is SKIPPED

# 4. Tests
uv run pytest -v
```

`python -m mpesa_pipeline <command>` still works inside an activated `.venv`.
Without uv, `pip install -r requirements.txt` works too.

Then open **http://localhost:8080** (Adminer):
- System: `PostgreSQL`
- Server: `postgres` (the Compose service name; Adminer runs inside Docker)
- Username / password: `de_user` / `de_pass`
- Database: `mpesa_dw`

Open "SQL command" and paste in the queries from `sql/queries/stage1_profile_raw.sql` one at a time.

### Experiments worth trying
1. **Idempotency:** open one CSV in `data/landing/`, delete a few rows, and save. Run `ingest`. Only that file reloads (the log says `RELOAD`), and its row count in `raw.ingestion_log` changes.
2. **Schema drift:** rename the `Amount` header in one file and run `ingest`. That file is rejected, every other file still loads, and the command exits with an error.
3. **Atomicity:** add a broken line such as `X,"unclosed` to the end of a file. Not one row from that file reaches the database.
4. **Lineage:** pick any row in `raw.mpesa_transactions`, then open the file named in `_source_file` at line `_source_row`. It's the same row.

## Project layout

```
mpesa_pipeline/        Python package (each pipeline step is a module)
  config.py            settings from env vars, so it runs anywhere unchanged
  db.py                connections, transactions, migrations
  generate.py          synthetic source system with planted data issues
  ingest.py            Stage 1 loader
sql/                   numbered migrations (run in order by init-db)
  queries/             analysis / profiling queries
tests/                 pytest; DB tests use a separate mpesa_dw_test database
data/landing/          where source files arrive (git-ignored)
data/generation_report.json   answer key: how many of each issue were planted
```

## The data

`generate.py` imitates a daily till export. The data quality issues below are
planted on purpose, and `data/generation_report.json` counts each one:

| Issue | Example |
|---|---|
| Duplicates (same file and re-sent next day) | the same `Receipt No.` twice |
| Late-arriving rows | a 1 Oct sale in the 2 Oct file |
| Mixed date formats | `2026-10-01 14:03:22`, `01/10/2026 14:03`, `2026-10-01T14:03:22` |
| Formatted amounts | `KES 1,200.00`, `1,200` |
| Phone formats | `254712…`, `+254712…`, `0712…`, `712…` |
| Messy names | `WANJIRU KAMAU`, `  wanjiru kamau `, blank |
| Reversals | negative amount, `Linked Receipt No.` points to the original |
| Orphan tills | till number missing from `shops.csv` |
| Invalid amounts | `0.00` or blank |

The shops, customers and transactions are synthetic. Any resemblance to real
businesses or phone numbers is coincidental.
