"""Command line interface for link observability collection."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .observe import (
    InvalidatedError,
    ObserveError,
    iter_events,
    latest_generation,
    observe,
)

DESCRIPTION = "Collect link observability events from a paged HTTP feed into SQLite"
_EXIT_ERROR = 2
_EXIT_INVALIDATED = 4


def _dump(payload) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    sub = parser.add_subparsers(dest="command", required=True)

    p_collect = sub.add_parser(
        "collect", help="collect or resume one generation from an HTTP endpoint")
    p_collect.add_argument("--db", type=Path, required=True,
                           help="versioned SQLite repository to read (read-only)")
    p_collect.add_argument("--revision", type=int, required=True,
                           help="immutable revision whose links are used")
    p_collect.add_argument("--endpoint", required=True,
                           help="http(s) URL of the paged observability feed")
    p_collect.add_argument("--collect", type=Path, required=True,
                           help="SQLite collection database (created or resumed)")
    p_collect.add_argument("--restart", action="store_true",
                           help="discard any staged incomplete generation first")

    p_latest = sub.add_parser(
        "latest", help="show the most recent complete generation and its binding")
    p_latest.add_argument("--collect", type=Path, required=True,
                          help="SQLite collection database to query")

    p_events = sub.add_parser(
        "events", help="stream events of the most recent complete generation")
    p_events.add_argument("--collect", type=Path, required=True,
                          help="SQLite collection database to query")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "collect":
            summary = observe(args.db, args.revision, args.endpoint, args.collect,
                              restart=args.restart)
            print(_dump(summary))
        elif args.command == "latest":
            # json.dumps(None) is "null": no complete generation yet.
            print(_dump(latest_generation(args.collect)))
        elif args.command == "events":
            # One compact JSON event per line (NDJSON), streamed in (time, id)
            # order so millions of events never accumulate in memory.
            for event in iter_events(args.collect):
                sys.stdout.write(_dump(event) + "\n")
        else:  # pragma: no cover - argparse enforces the choices
            raise ObserveError("unknown command")
    except InvalidatedError as error:
        print(str(error), file=sys.stderr)
        return _EXIT_INVALIDATED
    except (ObserveError, ValueError, TypeError, KeyError, OSError,
            OverflowError, RecursionError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return _EXIT_ERROR
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
