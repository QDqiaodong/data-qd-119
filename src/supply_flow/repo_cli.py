"""Command line interface for the SQLite versioned network repository."""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .common import read as read_strict_json, save as atomic_save
from .model import ALLOCATION_MODES
from .repo import (
    ConflictError,
    RepoError,
    Repository,
)

DESCRIPTION = "Versioned SQLite repository for supply/demand networks"
_EXIT_ERROR = 2
_EXIT_CONFLICT = 3


def _emit(payload, output: Path | None) -> None:
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2)
    print(text)
    if output is not None:
        atomic_save(output, payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="import a strict JSON network as revision 0")
    p_init.add_argument("--db", type=Path, required=True)
    p_init.add_argument("--source", type=Path, required=True)

    p_apply = sub.add_parser("apply", help="commit an upsert/delete batch on the current head")
    p_apply.add_argument("--db", type=Path, required=True)
    p_apply.add_argument("--request", type=Path, required=True,
                         help="JSON file with base_revision, batch_id and upsert/delete lists")

    p_export = sub.add_parser("export", help="export a revision as strict network JSON")
    p_export.add_argument("--db", type=Path, required=True)
    p_export.add_argument("--revision", type=int, required=True)
    p_export.add_argument("--output", type=Path,
                          help="write the bare network JSON (readable by supply-flow) here")

    p_analyze = sub.add_parser("analyze", help="analyze one immutable revision snapshot")
    p_analyze.add_argument("--db", type=Path, required=True)
    p_analyze.add_argument("--revision", type=int, required=True)
    p_analyze.add_argument("--allocation", choices=ALLOCATION_MODES, default="max-flow")
    p_analyze.add_argument("--output", type=Path)

    p_head = sub.add_parser("head", help="show the current head pointer")
    p_head.add_argument("--db", type=Path, required=True)

    p_history = sub.add_parser("history", help="list revisions with parents and snapshots")
    p_history.add_argument("--db", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "init":
            with Repository(args.db) as repo:
                payload = repo.init(args.source)
            _emit(payload, None)
        elif args.command == "apply":
            request, _digest = read_strict_json(args.request)
            with Repository(args.db) as repo:
                payload = repo.apply(request)
            _emit(payload, None)
        elif args.command == "export":
            with Repository(args.db) as repo:
                network = repo.export(args.revision)
            # Bare network document: the output file is directly readable by
            # the original supply-flow entry point and analyze().
            text = json.dumps(network, sort_keys=True, ensure_ascii=False,
                              allow_nan=False, indent=2)
            print(text)
            if args.output is not None:
                if args.output.resolve() == args.db.resolve():
                    raise RepoError("export output must differ from the database file")
                atomic_save(args.output, network)
        elif args.command == "analyze":
            with Repository(args.db) as repo:
                payload = repo.analyze_revision(args.revision, allocation=args.allocation)
            _emit(payload, args.output)
        elif args.command == "head":
            with Repository(args.db) as repo:
                _emit(repo.head(), None)
        elif args.command == "history":
            with Repository(args.db) as repo:
                _emit(repo.history(), None)
        else:  # pragma: no cover - argparse enforces the choices
            raise RepoError("unknown command")
    except ConflictError as error:
        print(str(error), file=sys.stderr)
        return _EXIT_CONFLICT
    except (ValueError, TypeError, KeyError, OSError, OverflowError,
            RecursionError, RepoError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return _EXIT_ERROR
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
