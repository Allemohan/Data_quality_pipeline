"""Main command-line entrypoint for Ingestion, Replay, and Reconciliation operations."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.config.settings import get_settings
from src.database.connection import get_db_session, get_engine, init_db
from src.database.models import Customer, CustomerType, PipelineRun, QuarantineRecord, ReplayRun
from src.pipeline.runner import PipelineRunner
from src.replay.replay_service import ReplayService


def configure_logging(level: str = "INFO") -> None:
    """Configures structured application logging."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s (run=%(filename)s:%(lineno)d): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def handle_ingest(args: argparse.Namespace) -> int:
    """Handles the 'ingest' subcommand."""
    input_file = Path(args.input)
    if not input_file.exists():
        print(f"Error: Input file does not exist: {input_file}", file=sys.stderr)
        return 1

    engine = get_engine(args.db_url)
    runner = PipelineRunner(engine=engine, batch_size=args.batch_size)
    tracker = runner.run(input_file_path=input_file)

    print("\n" + tracker.generate_report())
    return 0 if tracker.status in ("SUCCESS", "PARTIAL_SUCCESS") else 1


def handle_replay(args: argparse.Namespace) -> int:
    """Handles the 'replay' subcommand."""
    engine = get_engine(args.db_url)
    init_db(engine)

    with get_db_session(engine) as session:
        service = ReplayService(session)
        result = service.replay_corrected_records(limit=args.limit)
        print("\n" + result.format_summary())

    return 0


def handle_reconcile(args: argparse.Namespace) -> int:
    """Executes database aggregate reconciliation checks."""
    engine = get_engine(args.db_url)
    init_db(engine)

    with get_db_session(engine) as session:
        from sqlalchemy import func

        total_customers = session.query(func.count(Customer.customer_id)).scalar() or 0
        total_quarantine = session.query(func.count(QuarantineRecord.quarantine_id)).scalar() or 0
        total_runs = session.query(func.count(PipelineRun.run_id)).scalar() or 0
        total_replays = session.query(func.count(ReplayRun.replay_run_id)).scalar() or 0

        # Quarantine status counts
        q_status_counts = (
            session.query(QuarantineRecord.status, func.count(QuarantineRecord.quarantine_id))
            .group_by(QuarantineRecord.status)
            .all()
        )

        # Orphan check
        orphan_cust = (
            session.query(func.count(Customer.customer_id))
            .filter(~Customer.customer_type_id.in_(
                session.query(CustomerType.customer_type_id)
            ))
            .scalar() or 0
        )

        print("=" * 60)
        print("DATABASE RECONCILIATION SUMMARY")
        print("=" * 60)
        print(f"Total Customers in Target  : {total_customers}")
        print(f"Total Quarantine Records   : {total_quarantine}")
        for status, cnt in q_status_counts:
            print(f"  - Status '{status:<15}' : {cnt}")
        print(f"Total Pipeline Runs        : {total_runs}")
        print(f"Total Replay Audit Records : {total_replays}")
        print(f"Orphan Referential Keys   : {orphan_cust} {'(PASS)' if orphan_cust == 0 else '(FAIL)'}")
        print("=" * 60)

    return 0


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    # Pre-extract --db-url if passed anywhere in command line
    db_url_cli = None
    for i, arg in enumerate(sys.argv):
        if arg == "--db-url" and i + 1 < len(sys.argv):
            db_url_cli = sys.argv[i + 1]
            break
        elif arg.startswith("--db-url="):
            db_url_cli = arg.split("=", 1)[1]
            break

    # Common options parent parser
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--db-url",
        type=str,
        default=argparse.SUPPRESS,
        help="Optional database connection string override",
    )

    parser = argparse.ArgumentParser(
        description="Data Quality Quarantine & Replay Pipeline CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[parent_parser],
    )

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Ingest subcommand
    ingest_parser = subparsers.add_parser("ingest", parents=[parent_parser], help="Ingest a CSV source file")
    ingest_parser.add_argument(
        "--input",
        type=str,
        default="data/input/customers.csv",
        help="Path to the input CSV file",
    )
    ingest_parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.batch_size,
        help="Number of records per streaming batch",
    )

    # Replay subcommand
    replay_parser = subparsers.add_parser(
        "replay", parents=[parent_parser], help="Replay corrected quarantine records"
    )
    replay_parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum number of corrected records to replay",
    )

    # Reconcile subcommand
    subparsers.add_parser("reconcile", parents=[parent_parser], help="Run database reconciliation checks")

    args = parser.parse_args()

    # Apply detected CLI db_url override
    if db_url_cli:
        args.db_url = db_url_cli
    elif not hasattr(args, "db_url"):
        args.db_url = None

    if args.command == "ingest":
        sys.exit(handle_ingest(args))
    elif args.command == "replay":
        sys.exit(handle_replay(args))
    elif args.command == "reconcile":
        sys.exit(handle_reconcile(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
