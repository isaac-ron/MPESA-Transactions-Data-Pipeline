"""Shared test fixtures.

Database tests run against a separate database, mpesa_dw_test, so they can
wipe tables without touching your real data. If Postgres isn't reachable
(e.g. `docker compose up` hasn't been run), those tests are skipped rather
than failing.
"""
from datetime import date

import psycopg2
import pytest
from psycopg2 import sql

from mpesa_pipeline.config import db_config
from mpesa_pipeline.db import get_connection, init_db
from mpesa_pipeline.generate import MessRates, generate

TEST_DB = "mpesa_dw_test"


@pytest.fixture(scope="session")
def test_db_cfg():
    try:
        admin = psycopg2.connect(**db_config(dbname="postgres").as_kwargs(), connect_timeout=3)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Postgres not reachable, skipping DB tests ({exc.__class__.__name__})")
    admin.autocommit = True  # CREATE DATABASE cannot run inside a transaction
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,))
        if not cur.fetchone():
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB)))
    admin.close()
    return db_config(dbname=TEST_DB)


@pytest.fixture()
def clean_db(test_db_cfg):
    """A freshly created, empty warehouse for each test."""
    conn = get_connection(test_db_cfg)
    with conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS raw, staging, warehouse CASCADE")
    conn.close()
    init_db(test_db_cfg)
    return test_db_cfg


@pytest.fixture()
def landing(tmp_path):
    """A small generated landing zone: 3 days, 5 shops."""
    out = tmp_path / "landing"
    report = generate(out, date(2026, 10, 1), days=3, n_shops=5, n_customers=100, seed=7,
                      mess=MessRates())
    return out, report


def query_one(cfg, q, params=None):
    conn = get_connection(cfg)
    with conn, conn.cursor() as cur:
        cur.execute(q, params)
        row = cur.fetchone()
    conn.close()
    return row[0]
