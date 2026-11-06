"""Command-line entry point. From the project root:

    python -m mpesa_pipeline generate --days 30
    python -m mpesa_pipeline init-db
    python -m mpesa_pipeline ingest
    python -m mpesa_pipeline build-staging
    python -m mpesa_pipeline build-warehouse
    python -m mpesa_pipeline check [--layer staging|warehouse]
"""
import argparse
import logging
from datetime import date

from .checks import LAYERS, DataQualityError, run_checks
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

    sub.add_parser("build-staging", help="rebuild the staging schema from raw, then run the quality checks")
    sub.add_parser("build-warehouse", help="build the star schema from staging, then run its quality checks")
    c = sub.add_parser("check", help="run the data quality checks only")
    c.add_argument("--layer", choices=LAYERS, help="one layer only (default: all)")

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
    elif args.command == "build-staging":
        from .staging import build_staging
        build_staging()
        run_checks("staging")
    elif args.command == "build-warehouse":
        from .warehouse import build_warehouse
        build_warehouse()
        run_checks("warehouse")
    elif args.command == "check":
        failed = []
        for layer in [args.layer] if args.layer else LAYERS:
            try:
                run_checks(layer)
            except DataQualityError as exc:
                failed.append(str(exc))
        if failed:
            raise DataQualityError("; ".join(failed))


if __name__ == "__main__":
    main()
