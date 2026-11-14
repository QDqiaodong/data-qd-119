"""SQLite versioned repository for supply/demand networks.

A repository stores an immutable, complete content-addressed network snapshot
per revision plus a single moving head pointer. Only the Python standard
library (sqlite3/json/hashlib) is used.

Transaction boundary
--------------------
Every state-changing operation (``init`` and ``apply``) performs *all* of its
writes inside one ``BEGIN IMMEDIATE`` ... ``COMMIT`` transaction on a single
connection. The new revision row, the snapshot row, batch bookkeeping and the
head update therefore become visible together or not at all. A statement
failure rolls the transaction back; an operating-system kill or power loss
before ``COMMIT`` leaves the previous head and all previously committed
history untouched (SQLite's rollback journal/WAL guarantees this; fullfsync
intent is expressed with ``PRAGMA synchronous=FULL``). Readers using separate
connections never observe a half-written head.

Content normalization
---------------------
A snapshot id is the SHA256 of the canonical JSON serialization of the full
network ``{nodes, links, supplies, demands}`` object. Canonicalization:

* object keys are emitted in a fixed order;
* every array is sorted by a stable key (node id, link id, supply/demand node)
  and each row object uses a fixed key order;
* serialization is UTF-8 ``json.dumps(..., sort_keys=True,
  ensure_ascii=False, separators=(",", ":"), allow_nan=False)``.

Rearranging any input array therefore leaves the snapshot id unchanged. Two
revisions with identical canonical content share one snapshot row, while
revision numbers still increment on every successful batch.

Replay detection uses the same canonicalization on the *request* (the
upsert/delete lists only; ``base_revision`` and ``batch_id`` are excluded), so
a replayed batch is recognized even when its base revision is stale.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

from .common import ident, number, read as read_strict_json
from .model import analyze

SCHEMA_VERSION = 1

# Environment test seam: when set, an apply sleeps this many seconds with the
# write transaction open immediately before COMMIT, simulating a slow commit
# window for real-process concurrency/kill tests. Not part of the public API.
_COMMIT_DELAY_ENV = "SUPPLY_FLOW_TEST_COMMIT_DELAY"


class RepoError(Exception):
    """Base class for repository failures (invalid input or bad state)."""


class ConflictError(RepoError):
    """Optimistic-concurrency mismatch or batch_id reuse with a different body."""


# ---------------------------------------------------------------------------
# Canonical serialization
# ---------------------------------------------------------------------------

def _canonical_bytes(network: dict) -> bytes:
    """Deterministic byte serialization of a full network object."""
    canonical = {
        "nodes": sorted(network["nodes"]),
        "links": sorted(
            (
                {
                    "id": link["id"],
                    "from": link["from"],
                    "to": link["to"],
                    "capacity": link["capacity"],
                }
                for link in network["links"]
            ),
            key=lambda row: row["id"],
        ),
        "supplies": sorted(
            ({"node": row["node"], "amount": row["amount"]} for row in network["supplies"]),
            key=lambda row: row["node"],
        ),
        "demands": sorted(
            ({"node": row["node"], "amount": row["amount"]} for row in network["demands"]),
            key=lambda row: row["node"],
        ),
    }
    return json.dumps(
        canonical, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def snapshot_id(network: dict) -> str:
    """SHA256 of the canonical full-network content (array-order independent)."""
    return hashlib.sha256(_canonical_bytes(network)).hexdigest()


def _canonical_request_bytes(request: dict) -> bytes:
    """Canonical bytes of a batch request, excluding base_revision/batch_id.

    Only the six upsert/delete lists participate, so an identical logical batch
    replayed from an outdated base still hashes to the same value.
    """
    keys = (
        "upsert_nodes", "delete_nodes",
        "upsert_links", "delete_links",
        "upsert_supplies", "delete_supplies",
        "upsert_demands", "delete_demands",
    )
    canonical = {}
    for key in keys:
        value = request.get(key, [])
        if key.endswith("_nodes"):
            canonical[key] = sorted(value)
        elif key == "upsert_links":
            canonical[key] = sorted(
                (
                    {
                        "id": row["id"],
                        "from": row["from"],
                        "to": row["to"],
                        "capacity": row["capacity"],
                    }
                    for row in value
                ),
                key=lambda row: row["id"],
            )
        elif key in ("upsert_supplies", "upsert_demands"):
            canonical[key] = sorted(
                ({"node": row["node"], "amount": row["amount"]} for row in value),
                key=lambda row: row["node"],
            )
        else:  # id-only delete lists
            canonical[key] = sorted(value)
    return json.dumps(
        canonical, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# Request validation and batch application (pure functions over in-memory data)
# ---------------------------------------------------------------------------

_SCALAR_LIST_KEYS = frozenset(
    {"upsert_nodes", "delete_nodes", "delete_links", "delete_supplies", "delete_demands"}
)
_ROW_LIST_KEYS = frozenset({"upsert_links", "upsert_supplies", "upsert_demands"})
_REQUEST_KEYS = _SCALAR_LIST_KEYS | _ROW_LIST_KEYS


def normalize_request(request: dict) -> dict:
    """Validate a batch request shape and return a normalized copy."""
    if not isinstance(request, dict):
        raise RepoError("batch request must be an object")
    unknown = set(request) - _REQUEST_KEYS - {"base_revision", "batch_id"}
    if unknown:
        raise RepoError("unknown request field(s): " + ", ".join(sorted(unknown)))
    base = request.get("base_revision", 0)
    if type(base) is not int or base < 0:
        raise RepoError("base_revision must be a non-negative integer")
    batch_id = request.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id:
        raise RepoError("batch_id must be a non-empty string")
    ident(batch_id)
    normalized = {"base_revision": base, "batch_id": batch_id}
    for key in sorted(_REQUEST_KEYS):
        value = request.get(key, [])
        if not isinstance(value, list) or len(value) > 20000:
            raise RepoError(key + " must be a bounded array")
        if key in _SCALAR_LIST_KEYS:
            for item in value:
                ident(item)
        elif key == "upsert_links":
            for item in value:
                if not isinstance(item, dict) or set(item) != {"id", "from", "to", "capacity"}:
                    raise RepoError(key + " rows need exactly id/from/to/capacity")
                ident(item["id"])
                ident(item["from"])
                ident(item["to"])
                number(item["capacity"])
                if item["from"] == item["to"]:
                    raise RepoError("self links are rejected")
                if item["from"].startswith("__") or item["to"].startswith("__"):
                    raise RepoError("reserved internal node prefix")
        else:  # upsert_supplies / upsert_demands
            for item in value:
                if not isinstance(item, dict) or set(item) != {"node", "amount"}:
                    raise RepoError(key + " rows need exactly node/amount")
                ident(item["node"])
                if item["node"].startswith("__"):
                    raise RepoError("reserved internal node prefix")
                number(item["amount"])
        normalized[key] = list(value)
    _assert_disjoint_groups(normalized)
    return normalized


def _assert_disjoint_groups(req: dict) -> None:
    """Reject duplicated ids within a single upsert or delete list.

    An id may appear in both the upsert and delete lists of one batch: the batch
    applies sequentially, so a delete followed by an upsert atomically replaces
    a row (the "swap" pattern). A duplicated id *within* one list, however, has
    no deterministic order and is rejected up front.
    """

    def scalars(key: str) -> list:
        return list(req[key])

    def row_ids(key: str, id_field: str) -> list:
        return [row[id_field] for row in req[key]]

    checks = [
        ("node", scalars("upsert_nodes"), scalars("delete_nodes")),
        ("link", row_ids("upsert_links", "id"), scalars("delete_links")),
        ("supply", row_ids("upsert_supplies", "node"), scalars("delete_supplies")),
        ("demand", row_ids("upsert_demands", "node"), scalars("delete_demands")),
    ]
    for label, upserts, deletes in checks:
        if len(set(upserts)) != len(upserts) or len(set(deletes)) != len(deletes):
            raise RepoError("duplicate " + label + " id inside one batch list")


def apply_batch(network: dict, request: dict) -> dict:
    """Apply a normalized request to an in-memory network and validate the result.

    The whole batch is applied first; only the *final* network is validated, so
    a node may be referenced by a link/supply/demand earlier in the same batch
    and created later. Deletes never cascade: removing a node while links or
    supply/demand rows still point at it leaves a dangling reference that fails
    final validation and rejects the entire batch.
    """
    result = {
        "nodes": list(network["nodes"]),
        "links": [dict(row) for row in network["links"]],
        "supplies": [dict(row) for row in network["supplies"]],
        "demands": [dict(row) for row in network["demands"]],
    }

    def upsert(collection: list, id_field: str, row: dict) -> None:
        for existing in collection:
            if existing[id_field] == row[id_field]:
                existing.update(row)
                return
        collection.append(dict(row))

    # Deletes run before upserts so a batch can atomically replace an existing
    # row (same id present in both lists): delete-then-upsert semantics.
    for node in request["delete_nodes"]:
        if node not in result["nodes"]:
            raise RepoError("cannot delete missing node: " + node)
        result["nodes"].remove(node)  # no cascade: references survive until validation
    for node in request["upsert_nodes"]:
        if node.startswith("__"):
            raise RepoError("reserved internal node prefix")
        if node not in result["nodes"]:
            result["nodes"].append(node)

    for link_id in request["delete_links"]:
        before = len(result["links"])
        result["links"] = [row for row in result["links"] if row["id"] != link_id]
        if len(result["links"]) == before:
            raise RepoError("cannot delete missing link: " + link_id)
    for row in request["upsert_links"]:
        upsert(result["links"], "id", row)

    for key, collection in (
        ("delete_supplies", result["supplies"]),
        ("delete_demands", result["demands"]),
    ):
        for node in request[key]:
            before = len(collection)
            collection[:] = [row for row in collection if row["node"] != node]
            if len(collection) == before:
                raise RepoError("cannot delete missing row for node: " + node)
    for key, collection, id_field in (
        ("upsert_supplies", result["supplies"], "node"),
        ("upsert_demands", result["demands"], "node"),
    ):
        for row in request[key]:
            upsert(collection, id_field, row)

    # Final whole-network validation against the strict analyze contract. This
    # rejects dangling links/supplies/demands, duplicate ids, supply/demand
    # overlap and every other invariant of the original JSON entry point.
    analyze(result)
    return result


# ---------------------------------------------------------------------------
# SQLite repository
# ---------------------------------------------------------------------------

class Repository:
    """Versioned network repository backed by one SQLite file."""

    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        parent = self.path.parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None => explicit transaction management only.
        self.conn = sqlite3.connect(str(self.path), isolation_level=None, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        # WAL allows long-lived readers to keep querying history while a writer
        # holds the commit lock; synchronous FULL is the strongest durability
        # mode available without platform-specific settings.
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._ensure_schema()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Repository":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshots(
                snapshot_id TEXT PRIMARY KEY,
                content_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TABLE IF NOT EXISTS revisions(
                revision INTEGER PRIMARY KEY,
                parent_revision INTEGER NOT NULL,
                snapshot_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                request_sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id)
            );
            CREATE TABLE IF NOT EXISTS batches(
                batch_id TEXT PRIMARY KEY,
                request_sha256 TEXT NOT NULL,
                revision INTEGER NOT NULL
            );
            """
        )

    def _is_initialized(self) -> bool:
        row = self.conn.execute("SELECT value FROM meta WHERE key='head_revision'").fetchone()
        return row is not None

    # -- init ---------------------------------------------------------------

    def init(self, source_path: str | os.PathLike) -> dict:
        """Import an existing strict JSON network as immutable revision 0.

        The source is parsed with the same strict 8 MB / duplicate-key /
        non-finite-number contract as the original CLI, then fully validated by
        ``analyze``. Fails if the repository already holds revisions.
        """
        if self._is_initialized():
            raise RepoError("repository already initialized: " + str(self.path))
        data, source_digest = read_strict_json(source_path)
        analyze(data)  # strict contract validation before any write
        snap_id = snapshot_id(data)
        # Store the natural-order document (a faithful, analyze-readable copy of
        # the imported JSON); only the snapshot id is derived from canonical form.
        content = json.dumps(
            data, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
            allow_nan=False,
        )
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self.conn.execute(
                "INSERT INTO meta(key,value) VALUES('schema_version',?)", (str(SCHEMA_VERSION),)
            )
            self.conn.execute(
                "INSERT INTO meta(key,value) VALUES('source_sha256',?)", (source_digest,)
            )
            self.conn.execute("INSERT INTO snapshots(snapshot_id,content_json) VALUES(?,?)",
                              (snap_id, content))
            self.conn.execute(
                "INSERT INTO revisions(revision,parent_revision,snapshot_id,batch_id,request_sha256)"
                " VALUES(0,-1,?,?,?)",
                (snap_id, "__init__", hashlib.sha256(b"").hexdigest()),
            )
            self.conn.execute("INSERT INTO meta(key,value) VALUES('head_revision','0')")
            self._commit_with_test_delay()
        except BaseException:
            self.conn.rollback()
            raise
        return {"revision": 0, "parent_revision": -1, "snapshot_id": snap_id,
                "batch_id": "__init__"}

    # -- apply --------------------------------------------------------------

    def apply(self, request: dict) -> dict:
        """Atomically commit a batch on top of the current head.

        Order of checks inside one write transaction:

        1. replay/conflict by ``batch_id`` (takes precedence over everything,
           including an expired base revision);
        2. optimistic concurrency: ``base_revision`` must equal the current head;
        3. whole-batch application and final network validation;
        4. revision increment, immutable snapshot insert and head update.
        """
        req = normalize_request(request)
        request_hash = hashlib.sha256(_canonical_request_bytes(req)).hexdigest()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            batch_row = self.conn.execute(
                "SELECT request_sha256,revision FROM batches WHERE batch_id=?",
                (req["batch_id"],),
            ).fetchone()
            if batch_row is not None:
                if batch_row["request_sha256"] == request_hash:
                    self.conn.rollback()
                    rev = self.get_revision(batch_row["revision"])
                    return {"revision": rev["revision"], "parent_revision": rev["parent_revision"],
                            "snapshot_id": rev["snapshot_id"], "batch_id": req["batch_id"],
                            "replayed": True}
                raise ConflictError(
                    "batch_id " + req["batch_id"] + " already used with a different request"
                )
            head_row = self.conn.execute(
                "SELECT value FROM meta WHERE key='head_revision'"
            ).fetchone()
            if head_row is None:
                raise RepoError("repository is not initialized; run init first")
            head_revision = int(head_row["value"])
            if req["base_revision"] != head_revision:
                raise ConflictError(
                    "base_revision %d is not the current head %d"
                    % (req["base_revision"], head_revision)
                )
            base_network = self._network_at(head_revision)
            new_network = apply_batch(base_network, req)
            snap_id = snapshot_id(new_network)
            # Canonical hashing fixes the snapshot id; the stored document keeps
            # insertion order so export round-trips the working network.
            content = json.dumps(
                new_network, sort_keys=True, ensure_ascii=False,
                separators=(",", ":"), allow_nan=False,
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO snapshots(snapshot_id,content_json) VALUES(?,?)",
                (snap_id, content),
            )
            new_revision = head_revision + 1
            self.conn.execute(
                "INSERT INTO revisions(revision,parent_revision,snapshot_id,batch_id,request_sha256)"
                " VALUES(?,?,?,?,?)",
                (new_revision, head_revision, snap_id, req["batch_id"], request_hash),
            )
            self.conn.execute(
                "INSERT INTO batches(batch_id,request_sha256,revision) VALUES(?,?,?)",
                (req["batch_id"], request_hash, new_revision),
            )
            self.conn.execute(
                "UPDATE meta SET value=? WHERE key='head_revision'", (str(new_revision),)
            )
            self._commit_with_test_delay()
        except BaseException:
            self.conn.rollback()
            raise
        return {"revision": new_revision, "parent_revision": head_revision,
                "snapshot_id": snap_id, "batch_id": req["batch_id"], "replayed": False}

    def _commit_with_test_delay(self) -> None:
        delay = os.environ.get(_COMMIT_DELAY_ENV)
        if delay:
            time.sleep(float(delay))
        self.conn.execute("COMMIT")

    # -- reads --------------------------------------------------------------

    def _network_at(self, revision: int) -> dict:
        row = self.conn.execute(
            "SELECT s.content_json FROM revisions r JOIN snapshots s"
            " ON s.snapshot_id=r.snapshot_id WHERE r.revision=?",
            (revision,),
        ).fetchone()
        if row is None:
            raise RepoError("unknown revision: %d" % revision)
        return json.loads(row["content_json"])

    def head(self) -> dict:
        """Current head pointer (empty for an uninitialized repository)."""
        row = self.conn.execute("SELECT value FROM meta WHERE key='head_revision'").fetchone()
        if row is None:
            return {}
        revision = int(row["value"])
        info = self.get_revision(revision)
        return {"revision": revision, "snapshot_id": info["snapshot_id"],
                "parent_revision": info["parent_revision"], "batch_id": info["batch_id"]}

    def history(self) -> list[dict]:
        """All revisions in order with parent linkage and snapshot ids."""
        rows = self.conn.execute(
            "SELECT revision,parent_revision,snapshot_id,batch_id,created_at"
            " FROM revisions ORDER BY revision"
        ).fetchall()
        return [dict(row) for row in rows]

    def get_revision(self, revision: int) -> dict:
        row = self.conn.execute(
            "SELECT revision,parent_revision,snapshot_id,batch_id,request_sha256,created_at"
            " FROM revisions WHERE revision=?",
            (revision,),
        ).fetchone()
        if row is None:
            raise RepoError("unknown revision: %d" % revision)
        return dict(row)

    def export(self, revision: int) -> dict:
        """Return the original strict-JSON-readable network stored at a revision."""
        info = self.get_revision(revision)
        network = self._network_at(revision)
        # Re-serialize through the canonicalizer to prove the bytes hash back to
        # the bound snapshot id before handing data to callers.
        if snapshot_id(network) != info["snapshot_id"]:
            raise RepoError("snapshot integrity check failed at revision %d" % revision)
        return network

    def analyze_revision(self, revision: int, allocation: str = "max-flow") -> dict:
        """Run an allocation against one immutable snapshot.

        The analysis is bound to the snapshot id stored on the revision row, not
        to the moving head: advancing the head never changes this result.
        """
        info = self.get_revision(revision)
        network = self._network_at(revision)
        if snapshot_id(network) != info["snapshot_id"]:
            raise RepoError("snapshot integrity check failed at revision %d" % revision)
        result = analyze(network, allocation=allocation)
        return {"revision": revision, "snapshot_id": info["snapshot_id"],
                "allocation": allocation, "result": result}


# ---------------------------------------------------------------------------
# Module-level convenience API (one operation per connection)
# ---------------------------------------------------------------------------

def init_repository(db_path: str | os.PathLike, source_path: str | os.PathLike) -> dict:
    with Repository(db_path) as repo:
        return repo.init(source_path)


def apply_batch_file(db_path: str | os.PathLike, request: dict) -> dict:
    with Repository(db_path) as repo:
        return repo.apply(request)


def export_revision(db_path: str | os.PathLike, revision: int) -> dict:
    with Repository(db_path) as repo:
        return repo.export(revision)


def analyze_at_revision(
    db_path: str | os.PathLike, revision: int, allocation: str = "max-flow"
) -> dict:
    with Repository(db_path) as repo:
        return repo.analyze_revision(revision, allocation=allocation)


def read_head(db_path: str | os.PathLike) -> dict:
    with Repository(db_path) as repo:
        return repo.head()


def read_history(db_path: str | os.PathLike) -> list[dict]:
    with Repository(db_path) as repo:
        return repo.history()
