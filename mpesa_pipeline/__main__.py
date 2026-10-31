"""Command-line entry point. From the project root:

    python -m mpesa_pipeline generate --days 30
    python -m mpesa_pipeline init-db
    python -m mpesa_pipeline ingest
"""
import argparse
import logging
from datetime import date

from .config import LANDING_DIR


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")

    parser = argparse.ArgumentParser(prog="mpesa_pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write synthetic source files into the landing zone")
    g.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 1))
    g.add_argument("--days", type=int, default=30)
    g.add_argument("--shops", type=int, default=25)
    g.add_argument("--customers", type=int, default=2000)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--out", default=str(LANDING_DIR))

    sub.add_parser("init-db", help="create schemas and tables (safe to re-run)")

    i = sub.add_parser("ingest", help="load landing-zone files into the raw schema")
    i.add_argument("--landing-dir", default=str(LANDING_DIR))

    args = parser.parse_args()

    if args.command == "generate":
        from .generate import generate
        generate(args.out, args.start, args.days, args.shops, args.customers, args.seed)
    elif args.command == "init-db":
        from .db import init_db
        init_db()
    elif args.command == "ingest":
        from .ingest import ingest_all
        ingest_all(args.landing_dir)


if __name__ == "__main__":
    main()
