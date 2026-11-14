"""Transport handover reconciliation against one immutable repository revision.

The pipeline reads a shipment CSV and a receipt CSV (both with the exact columns
``id,link_id,time,quantity``), resolves every ``link_id`` against a specific
repository revision, and builds a standalone SQLite reconciliation database.

Processing model
----------------
* Input order is irrelevant.  Rows are streamed into an on-disk staging
  database, deduplicated per source table by ``id`` and ordered per link by
  ``(time, id)``; at the same instant shipments sort before receipts.
* A receipt consumes the oldest still-unmatched shipment quantity on the same
  link (FIFO); partial quantities are allowed.  A receipt part that cannot be
  matched because no earlier shipment balance exists becomes an orphan receipt
  share.  It never borrows a future shipment, and the later shipment simply
  remains unmatched.
* Link endpoints are taken from the selected revision snapshot.

Bounded resources
-----------------
No input row, match row or pending-shipment list is accumulated in Python
memory: staging, the per-link FIFO tail and all detail rows live on disk.  The
merge uses two forward-only cursors over one disk index, so a receipt advances
through *shipment segments* in key order and a shipment is never rescanned.
Only bounded per-link/per-node counters (the network contract allows at most
500 links and 100 nodes), one partially consumed shipment per link and small
fixed-size write batches are held in memory.

Failure and publication
-----------------------
The source repository is opened read-only and is never modified.  The result is
built in a temporary file next to the destination and renamed into place only
after every check passed; a parse, link-resolution or publication failure
leaves the previous result untouched.

Only the Python standard library is used.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from .repo import snapshot_id

SCHEMA_VERSION = 1
CSV_FIELDS = ("id", "link_id", "time", "quantity")
MAX_QUANTITY = 10**9
MAX_IDENTIFIER = 128
MAX_LINKS = 500
MAX_NODES = 100
_QUANTITY_RE = re.compile(r"[0-9]+\Z")
_CONTROL_RE = re.compile(r"[\x00-\x1f]")
_TIME_RE = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,9}))?([Zz]|[+-][0-9]{2}:[0-9]{2})\Z"
)
_EPOCH_ORDINAL = date(1970, 1, 1).toordinal()
_EPOCH_NAIVE = datetime(1970, 1, 1)
_UTC_MIN_SECONDS = (date(1, 1, 1).toordinal() - _EPOCH_ORDINAL) * 86400
_UTC_MAX_SECONDS = (date(9999, 12, 31).toordinal() - _EPOCH_ORDINAL) * 86400 + 86399
_INSERT_BATCH = 1000

# Shipments must be consumed before receipts that share an instant.
_KIND_SHIPMENT = 0
_KIND_RECEIPT = 1
_KIND_NAMES = {_KIND_SHIPMENT: "shipment", _KIND_RECEIPT: "receipt"}

_OUTPUT_SCHEMA = """
CREATE TABLE reconciliation(
    schema_version INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL,
    shipments_sha256 TEXT NOT NULL,
    receipts_sha256 TEXT NOT NULL,
    shipments_rows INTEGER NOT NULL,
    shipments_unique INTEGER NOT NULL,
    shipments_duplicates INTEGER NOT NULL,
    receipts_rows INTEGER NOT NULL,
    receipts_unique INTEGER NOT NULL,
    receipts_duplicates INTEGER NOT NULL,
    shipment_quantity INTEGER NOT NULL,
    receipt_quantity INTEGER NOT NULL,
    matched_quantity INTEGER NOT NULL,
    orphan_quantity INTEGER NOT NULL,
    unmatched_quantity INTEGER NOT NULL,
    link_count INTEGER NOT NULL,
    node_count INTEGER NOT NULL
);
CREATE TABLE links(
    link_id TEXT PRIMARY KEY,
    from_node TEXT NOT NULL,
    to_node TEXT NOT NULL,
    shipment_count INTEGER NOT NULL,
    shipment_quantity INTEGER NOT NULL,
    receipt_count INTEGER NOT NULL,
    receipt_quantity INTEGER NOT NULL,
    matched_quantity INTEGER NOT NULL,
    orphan_quantity INTEGER NOT NULL,
    unmatched_quantity INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE matches(
    link_id TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    receipt_time_utc TEXT NOT NULL,
    shipment_time_utc TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    PRIMARY KEY(link_id, receipt_id, shipment_id)
);
CREATE INDEX matches_by_shipment ON matches(link_id, shipment_id);
CREATE TABLE unmatched_shipments(
    link_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    shipment_time_utc TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    PRIMARY KEY(link_id, shipment_id)
) WITHOUT ROWID;
CREATE TABLE orphan_receipts(
    link_id TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    receipt_time_utc TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY(link_id, receipt_id)
) WITHOUT ROWID;
CREATE TABLE node_summary(
    node TEXT PRIMARY KEY,
    sent_quantity INTEGER NOT NULL,
    received_quantity INTEGER NOT NULL,
    matched_receipt_quantity INTEGER NOT NULL,
    orphan_receipt_quantity INTEGER NOT NULL,
    unmatched_shipment_quantity INTEGER NOT NULL
) WITHOUT ROWID;
"""

_STAGING_SCHEMA = """
CREATE TABLE raw(
    kind INTEGER NOT NULL,
    id TEXT NOT NULL,
    link TEXT NOT NULL,
    epoch_s INTEGER NOT NULL,
    nanos INTEGER NOT NULL,
    qty INTEGER NOT NULL,
    utc TEXT NOT NULL,
    line INTEGER NOT NULL
);
"""


class ReconcileError(Exception):
    """Invalid input, unresolvable link or impossible publication."""


# ---------------------------------------------------------------------------
# Time handling
# ---------------------------------------------------------------------------

def parse_utc_time(text: str) -> tuple[int, int, str]:
    """Parse a timezone-bearing ISO-8601 instant into (epoch_s, nanos, UTC text).

    The canonical UTC text is ``YYYY-MM-DDThh:mm:ss`` plus an optional fraction
    with trailing zeroes removed and a trailing ``Z``.
    """
    match = _TIME_RE.fullmatch(text)
    if match is None:
        raise ValueError(
            "time must be ISO-8601 with an explicit timezone "
            "(YYYY-MM-DDThh:mm:ss[.fraction]Z, +hh:mm or -hh:mm)"
        )
    year, month, day, hour, minute, second = (
        int(match.group(index)) for index in range(1, 7)
    )
    fraction = match.group(7) or ""
    zone = match.group(8)
    try:
        moment = datetime(year, month, day, hour, minute, second)
    except ValueError as error:
        raise ValueError("invalid calendar time: " + str(error)) from None
    if zone in ("Z", "z"):
        offset = 0
    else:
        offset_hours = int(zone[1:3])
        offset_minutes = int(zone[4:6])
        if offset_hours > 23 or offset_minutes > 59:
            raise ValueError("timezone offset out of range")
        offset = (offset_hours * 3600 + offset_minutes * 60) * (
            1 if zone[0] == "+" else -1
        )
    seconds = (
        (moment.toordinal() - _EPOCH_ORDINAL) * 86400
        + hour * 3600
        + minute * 60
        + second
        - offset
    )
    if seconds < _UTC_MIN_SECONDS or seconds > _UTC_MAX_SECONDS:
        raise ValueError("time outside the supported UTC range")
    nanos = int(fraction.ljust(9, "0")) if fraction else 0
    return seconds, nanos, format_utc_time(seconds, nanos)


def format_utc_time(seconds: int, nanos: int) -> str:
    """Canonical UTC text for an epoch-second/nanosecond instant."""
    moment = _EPOCH_NAIVE + timedelta(seconds=seconds)
    text = (
        "%04d-%02d-%02dT%02d:%02d:%02d"
        % (moment.year, moment.month, moment.day,
           moment.hour, moment.minute, moment.second)
    )
    if nanos:
        text += "." + ("%09d" % nanos).rstrip("0")
    return text + "Z"


# ---------------------------------------------------------------------------
# Repository access (read-only)
# ---------------------------------------------------------------------------

def _open_repository_read_only(path: Path) -> sqlite3.Connection:
    """Open the repository for reading without ever changing its content.

    When no ``-wal`` sidecar exists the database file is self-contained, so
    ``immutable=1`` is used and SQLite creates no files beside the source.  A
    live WAL sidecar must be read through ordinary read-only mode so its
    committed frames are visible; that only maintains the shared-memory
    sidecar, never repository content.
    """
    if not path.is_file():
        raise ReconcileError("repository does not exist or is not a file: " + str(path))
    options = "?mode=ro"
    wal = Path(str(path) + "-wal")
    # A WAL file without frames carries no committed data beyond the main file,
    # so the database is self-contained and can be read immutably (no sidecar
    # files are created next to the source).
    if not wal.exists() or wal.stat().st_size == 0:
        options += "&immutable=1"
    uri = "file:" + quote(str(path.resolve())) + options
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=30.0)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
    except sqlite3.Error as error:
        raise ReconcileError(
            "cannot open repository read-only: %s (%s)" % (path, error)
        ) from None
    return connection


def _load_revision(path: Path, revision: int) -> tuple[str, dict, list[str]]:
    """Return (verified snapshot id, link map, sorted nodes) for a revision."""
    connection = _open_repository_read_only(path)
    try:
        try:
            row = connection.execute(
                "SELECT r.snapshot_id, s.content_json FROM revisions r"
                " JOIN snapshots s ON s.snapshot_id=r.snapshot_id"
                " WHERE r.revision=?",
                (revision,),
            ).fetchone()
        except sqlite3.Error as error:
            raise ReconcileError(
                "cannot read repository revision %d: %s" % (revision, error)
            ) from None
    finally:
        connection.close()
    if row is None:
        raise ReconcileError("unknown revision: %d" % revision)
    recorded_id, content = row
    try:
        network = json.loads(content)
    except (TypeError, ValueError) as error:
        raise ReconcileError(
            "revision %d snapshot is not valid JSON: %s" % (revision, error)
        ) from None
    if snapshot_id(network) != recorded_id:
        raise ReconcileError("snapshot integrity check failed at revision %d" % revision)
    try:
        nodes = network["nodes"]
        links = network["links"]
    except (TypeError, KeyError):
        raise ReconcileError("revision %d snapshot has no nodes/links" % revision) from None
    if not isinstance(nodes, list) or not isinstance(links, list):
        raise ReconcileError("revision %d snapshot has malformed nodes/links" % revision)
    if len(nodes) > MAX_NODES or len(links) > MAX_LINKS:
        raise ReconcileError(
            "revision %d exceeds the network bounds (nodes<=%d, links<=%d)"
            % (revision, MAX_NODES, MAX_LINKS)
        )
    node_set = set()
    for node in nodes:
        if (
            not isinstance(node, str) or not node or node.startswith("__")
            or node in node_set
        ):
            raise ReconcileError("revision %d has an invalid node" % revision)
        node_set.add(node)
    link_map = {}
    for link in links:
        if not isinstance(link, dict) or not {"id", "from", "to"} <= set(link):
            raise ReconcileError("revision %d has a malformed link" % revision)
        link_id = link["id"]
        from_node = link["from"]
        to_node = link["to"]
        if not isinstance(link_id, str) or not link_id:
            raise ReconcileError("revision %d has an invalid link id" % revision)
        if (
            link_id in link_map
            or from_node not in node_set
            or to_node not in node_set
            or from_node == to_node
        ):
            raise ReconcileError("revision %d has an invalid link" % revision)
        link_map[link_id] = (from_node, to_node)
    return recorded_id, link_map, sorted(node_set)


# ---------------------------------------------------------------------------
# CSV validation and staging
# ---------------------------------------------------------------------------

def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReconcileError("cannot read %s: %s" % (path, error)) from None
    return digest.hexdigest()


def _require_input_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise ReconcileError(
            "%s file does not exist or is not a file: %s" % (label, path)
        )


def _csv_identifier(value: str, label: str) -> str:
    """Identifier rule of the network contract, sliced for CSV hot loops."""
    if (
        not value
        or len(value) > MAX_IDENTIFIER
        or _CONTROL_RE.search(value) is not None
        or value[0].isspace()
        or value[-1].isspace()
    ):
        raise ValueError(
            label + " must be a nonblank identifier, <=%d characters" % MAX_IDENTIFIER
        )
    return value


def _parse_quantity(text: str) -> int:
    if _QUANTITY_RE.fullmatch(text) is None:
        raise ValueError("quantity must be an integer")
    value = int(text)
    if not 1 <= value <= MAX_QUANTITY:
        raise ValueError("quantity must be between 1 and %d" % MAX_QUANTITY)
    return value


def _stage_file(
    connection: sqlite3.Connection,
    path: Path,
    kind: int,
    link_map: dict,
    revision: int,
) -> int:
    """Stream one CSV into the raw staging table; return the row count."""
    total = 0
    batch = []
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        try:
            header = next(reader)
        except StopIteration:
            raise ReconcileError(
                "%s:1: missing CSV header %s" % (path, ",".join(CSV_FIELDS))
            )
        if header != list(CSV_FIELDS):
            raise ReconcileError(
                "%s:1: header must be exactly %s" % (path, ",".join(CSV_FIELDS))
            )
        for row in reader:
            line = reader.line_num
            if not row:
                raise ReconcileError("%s:%d: blank CSV row" % (path, line))
            if len(row) != len(CSV_FIELDS):
                raise ReconcileError(
                    "%s:%d: expected %d columns, got %d"
                    % (path, line, len(CSV_FIELDS), len(row))
                )
            try:
                row_id = _csv_identifier(row[0], "id")
                link_id = _csv_identifier(row[1], "link_id")
                seconds, nanos, utc = parse_utc_time(row[2])
                quantity = _parse_quantity(row[3])
            except ValueError as error:
                raise ReconcileError("%s:%d: %s" % (path, line, error)) from None
            if link_id not in link_map:
                raise ReconcileError(
                    "%s:%d: link_id %r is not defined in revision %d"
                    % (path, line, link_id, revision)
                )
            batch.append((kind, row_id, link_id, seconds, nanos, quantity, utc, line))
            total += 1
            if len(batch) >= _INSERT_BATCH:
                connection.executemany(
                    "INSERT INTO raw(kind,id,link,epoch_s,nanos,qty,utc,line)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    batch,
                )
                batch.clear()
        if batch:
            connection.executemany(
                "INSERT INTO raw(kind,id,link,epoch_s,nanos,qty,utc,line)"
                " VALUES(?,?,?,?,?,?,?,?)",
                batch,
            )
            batch.clear()
    return total


def _dedupe_file(
    connection: sqlite3.Connection,
    path: Path,
    kind: int,
    total: int,
) -> tuple[int, int]:
    """Collapse duplicate ids from one source table in place.

    Returns ``(unique_rows, duplicate_rows)``.  A repeated id with identical
    content (the normalized link, instant and quantity) is counted once; a
    repeated id with different content is rejected naming this file, the
    conflicting line and the line of the first occurrence.
    """
    label = _KIND_NAMES[kind]
    content = "link||char(31)||epoch_s||char(31)||nanos||char(31)||qty"
    conflict = connection.execute(
        "SELECT id FROM raw WHERE kind=?"
        " GROUP BY id HAVING COUNT(*)>1 AND COUNT(DISTINCT " + content + ")>1 LIMIT 1",
        (kind,),
    ).fetchone()
    if conflict is not None:
        conflict_id = conflict[0]
        first = connection.execute(
            "SELECT link,epoch_s,nanos,qty,line FROM raw"
            " WHERE kind=? AND id=? ORDER BY line LIMIT 1",
            (kind, conflict_id),
        ).fetchone()
        other = connection.execute(
            "SELECT MIN(line) FROM raw WHERE kind=?"
            " AND (link,epoch_s,nanos,qty)!=(?,?,?,?) AND id=?",
            (kind, first[0], first[1], first[2], first[3], conflict_id),
        ).fetchone()
        raise ReconcileError(
            "%s:%d: %s id %r repeats with conflicting content"
            " (first occurrence at %s:%d)"
            % (path, other[0], label, conflict_id, path, first[4])
        )
    connection.execute(
        "DELETE FROM raw WHERE kind=? AND rowid NOT IN"
        " (SELECT MIN(rowid) FROM raw WHERE kind=? GROUP BY id)",
        (kind, kind),
    )
    unique = connection.execute(
        "SELECT COUNT(*) FROM raw WHERE kind=?", (kind,)
    ).fetchone()[0]
    return unique, total - unique


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _new_link_stats(link_map: dict) -> dict:
    return {
        link_id: {
            "from": from_node,
            "to": to_node,
            "shipment_count": 0,
            "shipment_quantity": 0,
            "receipt_count": 0,
            "receipt_quantity": 0,
            "matched_quantity": 0,
            "orphan_quantity": 0,
            "unmatched_quantity": 0,
        }
        for link_id, (from_node, to_node) in link_map.items()
    }


def _new_node_stats(nodes: list[str]) -> dict:
    return {
        node: {
            "sent_quantity": 0,
            "received_quantity": 0,
            "matched_receipt_quantity": 0,
            "orphan_receipt_quantity": 0,
            "unmatched_shipment_quantity": 0,
        }
        for node in nodes
    }


def _match_link(
    staging: sqlite3.Connection,
    output: sqlite3.Connection,
    link_id: str,
    stats: dict,
    node_stats: dict,
) -> None:
    """Merge one link chronologically and stream match/orphan rows to disk.

    Two independent, lazily fetched cursors walk the same staging index: one
    over shipments and one over receipts.  Each advances only forward, so no
    shipment is ever rescanned, and the FIFO head is the single partially
    consumed shipment held in memory.  Within a kind the order is ``(time, id)``;
    across kinds every shipment at an instant sorts before every receipt at
    that instant, so a shipment is consumable when ``shipment time <=
    receipt time``.  A strictly later shipment is never borrowed.
    """
    shipments = staging.execute(
        "SELECT id,epoch_s,nanos,qty,utc FROM raw"
        " WHERE link=? AND kind=0 ORDER BY epoch_s,nanos,id",
        (link_id,),
    )
    receipts = staging.execute(
        "SELECT id,epoch_s,nanos,qty,utc FROM raw"
        " WHERE link=? AND kind=1 ORDER BY epoch_s,nanos,id",
        (link_id,),
    )
    shipment = _next_row(shipments)
    head = None  # [shipment_id, utc, remaining] — the only in-memory tail
    shipments_seen = 0

    def take_shipment(row):
        nonlocal shipments_seen
        shipments_seen += 1
        stats["shipment_count"] += 1
        stats["shipment_quantity"] += row[3]
        node_stats[stats["from"]]["sent_quantity"] += row[3]
        return [row[0], row[4], row[3]]

    for receipt in receipts:
        quantity = receipt[3]
        stats["receipt_count"] += 1
        stats["receipt_quantity"] += quantity
        node_stats[stats["to"]]["received_quantity"] += quantity
        unmatched = quantity
        while unmatched:
            if head is None:
                if shipment is None:
                    break
                if (shipment[1], shipment[2]) > (receipt[1], receipt[2]):
                    break  # future shipment: never borrowed by an earlier receipt
                head = take_shipment(shipment)
                shipment = _next_row(shipments)
            take = unmatched if unmatched < head[2] else head[2]
            output.execute(
                "INSERT INTO matches(link_id,receipt_id,shipment_id,"
                "receipt_time_utc,shipment_time_utc,quantity) VALUES(?,?,?,?,?,?)",
                (link_id, receipt[0], head[0], receipt[4], head[1], take),
            )
            unmatched -= take
            head[2] -= take
            stats["matched_quantity"] += take
            node_stats[stats["to"]]["matched_receipt_quantity"] += take
            if head[2] == 0:
                head = None
        if unmatched:
            reason = "early_arrival" if shipments_seen == 0 else "over_receipt"
            output.execute(
                "INSERT INTO orphan_receipts(link_id,receipt_id,receipt_time_utc,"
                "quantity,reason) VALUES(?,?,?,?,?)",
                (link_id, receipt[0], receipt[4], unmatched, reason),
            )
            stats["orphan_quantity"] += unmatched
            node_stats[stats["to"]]["orphan_receipt_quantity"] += unmatched

    # A partially consumed shipment is the first of the unmatched remainder.
    if head is not None:
        stats["unmatched_quantity"] += head[2]
        node_stats[stats["from"]]["unmatched_shipment_quantity"] += head[2]
        output.execute(
            "INSERT INTO unmatched_shipments(link_id,shipment_id,"
            "shipment_time_utc,quantity) VALUES(?,?,?,?)",
            (link_id, head[0], head[1], head[2]),
        )
    # Every shipment not yet pulled was either later than all receipts or came
    # after the last receipt, so it remains unmatched in full.
    while shipment is not None:
        take_shipment(shipment)
        stats["unmatched_quantity"] += shipment[3]
        node_stats[stats["from"]]["unmatched_shipment_quantity"] += shipment[3]
        output.execute(
            "INSERT INTO unmatched_shipments(link_id,shipment_id,"
            "shipment_time_utc,quantity) VALUES(?,?,?,?)",
            (link_id, shipment[0], shipment[4], shipment[3]),
        )
        shipment = _next_row(shipments)


def _next_row(cursor: sqlite3.Cursor):
    return cursor.fetchone()


# ---------------------------------------------------------------------------
# Publication
# ---------------------------------------------------------------------------

def _check_paths(repo: Path, shipments: Path, receipts: Path, output: Path) -> None:
    if output.is_symlink():
        raise ReconcileError("output cannot be a symlink: " + str(output))
    if output.exists() and not output.is_file():
        raise ReconcileError("output is not a regular file: " + str(output))
    output_resolved = output.resolve()
    for label, path in (
        ("repository", repo),
        ("shipments", shipments),
        ("receipts", receipts),
    ):
        if path.resolve() == output_resolved:
            raise ReconcileError("output must differ from the %s file" % label)


def _publish(temp_path: Path, destination: Path) -> None:
    descriptor = os.open(str(temp_path), os.O_RDWR)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.chmod(temp_path, 0o644)
    os.replace(temp_path, destination)
    try:
        directory = os.open(str(destination.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError:  # pragma: no cover - best effort on exotic filesystems
        pass


# ---------------------------------------------------------------------------
# Output database construction
# ---------------------------------------------------------------------------

def _build_database(
    staging: sqlite3.Connection,
    output_path: Path,
    revision: int,
    snap_id: str,
    nodes: list[str],
    link_map: dict,
    shipments_digest: str,
    receipts_digest: str,
    shipment_rows: tuple[int, int],
    receipt_rows: tuple[int, int],
    shipment_total_rows: int,
    receipt_total_rows: int,
) -> dict:
    output = sqlite3.connect(str(output_path), isolation_level=None)
    output.execute("PRAGMA journal_mode=OFF")
    output.execute("PRAGMA synchronous=OFF")
    output.execute("PRAGMA temp_store=FILE")
    output.execute("PRAGMA cache_size=-8192")
    try:
        output.executescript(_OUTPUT_SCHEMA)
        output.execute("BEGIN")
        staging.execute("PRAGMA temp_store=FILE")
        staging.execute("PRAGMA cache_size=-4096")
        staging.execute("BEGIN")

        link_stats = _new_link_stats(link_map)
        node_stats = _new_node_stats(nodes)
        staging.execute("CREATE INDEX raw_order ON raw(link,epoch_s,nanos,kind,id)")
        for link_id in sorted(link_map):
            if staging.execute(
                "SELECT 1 FROM raw WHERE link=? LIMIT 1", (link_id,)
            ).fetchone() is None:
                continue
            _match_link(staging, output, link_id, link_stats[link_id], node_stats)

        staged_shipments = staging.execute(
            "SELECT COUNT(*), COALESCE(SUM(qty),0) FROM raw WHERE kind=0"
        ).fetchone()
        staged_receipts = staging.execute(
            "SELECT COUNT(*), COALESCE(SUM(qty),0) FROM raw WHERE kind=1"
        ).fetchone()
        staging.execute("COMMIT")

        totals = {
            "shipment_count": sum(s["shipment_count"] for s in link_stats.values()),
            "shipment_quantity": sum(s["shipment_quantity"] for s in link_stats.values()),
            "receipt_count": sum(s["receipt_count"] for s in link_stats.values()),
            "receipt_quantity": sum(s["receipt_quantity"] for s in link_stats.values()),
            "matched_quantity": sum(s["matched_quantity"] for s in link_stats.values()),
            "orphan_quantity": sum(s["orphan_quantity"] for s in link_stats.values()),
            "unmatched_quantity": sum(s["unmatched_quantity"] for s in link_stats.values()),
        }
        if (staged_shipments[0], staged_shipments[1]) != (
            totals["shipment_count"], totals["shipment_quantity"]
        ):
            raise ReconcileError("internal shipment staging mismatch")
        if (staged_receipts[0], staged_receipts[1]) != (
            totals["receipt_count"], totals["receipt_quantity"]
        ):
            raise ReconcileError("internal receipt staging mismatch")
        if totals["matched_quantity"] + totals["orphan_quantity"] != totals["receipt_quantity"]:
            raise ReconcileError("internal receipt balance mismatch")
        if totals["matched_quantity"] + totals["unmatched_quantity"] != totals["shipment_quantity"]:
            raise ReconcileError("internal shipment balance mismatch")

        for link_id in sorted(link_stats):
            stats = link_stats[link_id]
            output.execute(
                "INSERT INTO links(link_id,from_node,to_node,shipment_count,"
                "shipment_quantity,receipt_count,receipt_quantity,matched_quantity,"
                "orphan_quantity,unmatched_quantity) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    link_id, stats["from"], stats["to"],
                    stats["shipment_count"], stats["shipment_quantity"],
                    stats["receipt_count"], stats["receipt_quantity"],
                    stats["matched_quantity"], stats["orphan_quantity"],
                    stats["unmatched_quantity"],
                ),
            )
        for node in nodes:
            stats = node_stats[node]
            output.execute(
                "INSERT INTO node_summary(node,sent_quantity,received_quantity,"
                "matched_receipt_quantity,orphan_receipt_quantity,"
                "unmatched_shipment_quantity) VALUES(?,?,?,?,?,?)",
                (
                    node, stats["sent_quantity"], stats["received_quantity"],
                    stats["matched_receipt_quantity"],
                    stats["orphan_receipt_quantity"],
                    stats["unmatched_shipment_quantity"],
                ),
            )
        output.execute(
            "INSERT INTO reconciliation(schema_version,revision,snapshot_id,"
            "shipments_sha256,receipts_sha256,shipments_rows,shipments_unique,"
            "shipments_duplicates,receipts_rows,receipts_unique,receipts_duplicates,"
            "shipment_quantity,receipt_quantity,matched_quantity,orphan_quantity,"
            "unmatched_quantity,link_count,node_count)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                SCHEMA_VERSION, revision, snap_id, shipments_digest, receipts_digest,
                shipment_total_rows, shipment_rows[0], shipment_rows[1],
                receipt_total_rows, receipt_rows[0], receipt_rows[1],
                totals["shipment_quantity"], totals["receipt_quantity"],
                totals["matched_quantity"], totals["orphan_quantity"],
                totals["unmatched_quantity"], len(link_stats), len(nodes),
            ),
        )

        # Re-derive the balance from the persisted tables only; the summary
        # above must agree with what a reader of the output sees.
        persisted = output.execute(
            "SELECT (SELECT COALESCE(SUM(quantity),0) FROM matches),"
            " (SELECT COALESCE(SUM(quantity),0) FROM orphan_receipts),"
            " (SELECT COALESCE(SUM(quantity),0) FROM unmatched_shipments),"
            " (SELECT COALESCE(SUM(matched_quantity),0) FROM links),"
            " (SELECT COALESCE(SUM(shipment_quantity),0) FROM links),"
            " (SELECT COALESCE(SUM(receipt_quantity),0) FROM links),"
            " (SELECT COALESCE(SUM(orphan_quantity),0) FROM links),"
            " (SELECT COALESCE(SUM(unmatched_quantity),0) FROM links)"
        ).fetchone()
        if (
            persisted[0] + persisted[1] != totals["receipt_quantity"]
            or persisted[0] + persisted[2] != totals["shipment_quantity"]
            or persisted[3] != totals["matched_quantity"]
            or persisted[4] != totals["shipment_quantity"]
            or persisted[5] != totals["receipt_quantity"]
            or persisted[6] != totals["orphan_quantity"]
            or persisted[7] != totals["unmatched_quantity"]
        ):
            raise ReconcileError("internal persisted balance mismatch")
        check = output.execute("PRAGMA quick_check").fetchone()
        if check is None or check[0] != "ok":
            raise ReconcileError("output database failed quick_check")

        output.execute("COMMIT")
        output.execute("PRAGMA journal_mode=DELETE")
    finally:
        output.close()

    return {
        "schema_version": SCHEMA_VERSION,
        "revision": revision,
        "snapshot_id": snap_id,
        "shipments_sha256": shipments_digest,
        "receipts_sha256": receipts_digest,
        "shipments_rows": shipment_total_rows,
        "shipments_unique": shipment_rows[0],
        "shipments_duplicates": shipment_rows[1],
        "receipts_rows": receipt_total_rows,
        "receipts_unique": receipt_rows[0],
        "receipts_duplicates": receipt_rows[1],
        "shipment_quantity": totals["shipment_quantity"],
        "receipt_quantity": totals["receipt_quantity"],
        "matched_quantity": totals["matched_quantity"],
        "orphan_quantity": totals["orphan_quantity"],
        "unmatched_quantity": totals["unmatched_quantity"],
        "link_count": len(link_stats),
        "node_count": len(nodes),
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def reconcile(
    repository: str | os.PathLike,
    revision: int,
    shipments: str | os.PathLike,
    receipts: str | os.PathLike,
    output: str | os.PathLike,
) -> dict:
    """Reconcile two CSVs at one immutable revision into a standalone SQLite file."""
    repo_path = Path(repository)
    shipments_path = Path(shipments)
    receipts_path = Path(receipts)
    output_path = Path(output)
    if type(revision) is not int or revision < 0:
        raise ReconcileError("revision must be a non-negative integer")

    _check_paths(repo_path, shipments_path, receipts_path, output_path)
    _require_input_file(shipments_path, "shipments")
    _require_input_file(receipts_path, "receipts")

    snap_id, link_map, nodes = _load_revision(repo_path, revision)
    shipments_digest = _hash_file(shipments_path)
    receipts_digest = _hash_file(receipts_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_handle, temp_name = tempfile.mkstemp(
        prefix="." + output_path.name + ".", suffix=".tmp", dir=output_path.parent
    )
    os.close(temp_handle)
    temp_output = Path(temp_name)
    try:
        with tempfile.TemporaryDirectory(prefix="supply-flow-reconcile-") as scratch:
            staging = sqlite3.connect(
                str(Path(scratch) / "staging.db"), isolation_level=None
            )
            staging.execute("PRAGMA journal_mode=OFF")
            staging.execute("PRAGMA synchronous=OFF")
            staging.execute("PRAGMA temp_store=FILE")
            staging.execute("PRAGMA cache_size=-4096")
            try:
                staging.executescript(_STAGING_SCHEMA)
                staging.execute("BEGIN")
                shipment_total = _stage_file(
                    staging, shipments_path, _KIND_SHIPMENT, link_map, revision
                )
                receipt_total = _stage_file(
                    staging, receipts_path, _KIND_RECEIPT, link_map, revision
                )
                staging.execute("CREATE INDEX raw_identity ON raw(kind,id)")
                shipment_rows = _dedupe_file(
                    staging, shipments_path, _KIND_SHIPMENT, shipment_total
                )
                receipt_rows = _dedupe_file(
                    staging, receipts_path, _KIND_RECEIPT, receipt_total
                )
                staging.execute("COMMIT")
            except BaseException:
                if staging.in_transaction:
                    staging.execute("ROLLBACK")
                raise
            try:
                summary = _build_database(
                    staging, temp_output, revision, snap_id, nodes, link_map,
                    shipments_digest, receipts_digest, shipment_rows, receipt_rows,
                    shipment_total, receipt_total,
                )
            finally:
                staging.close()
        _publish(temp_output, output_path)
    except BaseException:
        try:
            temp_output.unlink()
        except OSError:
            pass
        raise
    summary["output"] = str(output_path)
    return summary
