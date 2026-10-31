"""Central configuration.

Every setting comes from an environment variable and has a sensible default.
The same code then runs on your laptop, inside Docker, and (in Stage 4)
inside Airflow, with no edits. Only the environment changes.
"""
import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env if python-dotenv is installed. Real environment variables still win.
try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:  # pragma: no cover
    pass

LANDING_DIR = Path(os.getenv("LANDING_DIR", PROJECT_ROOT / "data" / "landing"))
SQL_DIR = PROJECT_ROOT / "sql"


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def as_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


def db_config(dbname: str | None = None) -> DBConfig:
    return DBConfig(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=dbname or os.getenv("PG_DB", "mpesa_dw"),
        user=os.getenv("PG_USER", "de_user"),
        password=os.getenv("PG_PASSWORD", "de_pass"),
    )
