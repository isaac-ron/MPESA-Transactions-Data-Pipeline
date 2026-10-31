"""Database helpers: connections, transactions, and running the SQL files in /sql."""
import logging
from contextlib import contextmanager
from pathlib import Path

import psycopg2

from .config import SQL_DIR, DBConfig, db_config

log = logging.getLogger(__name__)


def get_connection(cfg: DBConfig | None = None):
    return psycopg2.connect(**(cfg or db_config()).as_kwargs())


@contextmanager
def transaction(cfg: DBConfig | None = None):
    """Open a connection and commit on success or roll back on any error.

    This is how a pipeline step stays atomic: it either fully happens
    or leaves no trace.
    """
    conn = get_connection(cfg)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_sql_file(conn, path: Path) -> None:
    log.info("Running %s", path.name)
    with conn.cursor() as cur:
        cur.execute(path.read_text())


def init_db(cfg: DBConfig | None = None) -> None:
    """Run every sql/NN_*.sql file in numeric order.

    Every file uses CREATE ... IF NOT EXISTS, so running this twice is safe.
    That property is called idempotency, and it comes up all through the project.
    """
    with transaction(cfg) as conn:
        for path in sorted(SQL_DIR.glob("[0-9][0-9]_*.sql")):
            run_sql_file(conn, path)
    log.info("Database initialised")
