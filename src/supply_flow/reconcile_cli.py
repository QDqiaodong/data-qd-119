"""Command line interface for transport handover reconciliation."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .reconcile import ReconcileError, reconcile

DESCRIPTION = "Reconcile shipment and receipt CSVs against a repository revision"
_EXIT_ERROR = 2
_EXIT_REPO = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("--db", type=Path, required=True,
                        help="versioned SQLite repository to read (read-only)")
    parser.add_argument("--revision", type=int, required=True,
                        help="immutable revision whose link endpoints are used")
    parser.add_argument("--shipments", type=Path, required=True,
                        help="CSV with exactly id,link_id,time,quantity")
    parser.add_argument("--receipts", type=Path, required=True,
                        help="CSV with exactly id,link_id,time,quantity")
    parser.add_argument("--output", type=Path, required=True,
                        help="standalone SQLite reconciliation database to publish")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        summary = reconcile(
            args.db, args.revision, args.shipments, args.receipts, args.output
        )
    except (ReconcileError, ValueError, TypeError, KeyError, OSError,
            OverflowError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return _EXIT_ERROR
    print(json.dumps(summary, sort_keys=True, ensure_ascii=False, allow_nan=False,
                     separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
