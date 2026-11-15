"""Link observability collection from a paged HTTP feed into SQLite.

The collector resolves link ids against one immutable repository revision
(read-only, exactly like the reconciliation pipeline), walks a paged HTTP
endpoint and commits every page into a standalone SQLite *collection
database*.  Only the Python standard library is used.

Feed protocol
-------------
The first page is ``GET <endpoint>`` with no parameters.  Every later page
is ``GET <endpoint>?snapshot=<token>&cursor=<cursor>`` with the token and
cursor taken from the previous response.  A page is a JSON object with
exactly three fields:

* ``snapshot``: non-empty string naming the generation; constant for every
  page of one generation;
* ``next``: ``null`` on the last page, otherwise the non-empty cursor
  string used to fetch the next page;
* ``events``: an array of at most 1000 events.

An event is an object with exactly ``id``, ``link_id``, ``time`` and
``state``.  Identifiers follow the network contract (nonblank, <=128
characters, no control characters), ``time`` follows the timezone-bearing
ISO-8601 contract and is normalized to UTC, ``state`` is ``up`` or
``down``, and ``link_id`` must exist in the selected revision snapshot.

Generation semantics
--------------------
* The snapshot token is bound to the run when the first page is stored; a
  different token on any later page aborts the collection.
* Event ids are deduplicated by canonical value: a repeated id with
  identical normalized content (link, UTC instant, state) is counted once;
  a repeated id with different content rejects the page.
* A cursor that was already used to fetch a page of the same run is a
  cursor loop and rejects the collection.

Transaction boundary
--------------------
Each page is committed in one ``BEGIN IMMEDIATE`` transaction together with
its events, the SHA256 of the raw response bytes and the next cursor.  A
kill or crash between commits resumes from the last committed page; a kill
inside a commit rolls that page back and it is fetched again.  Only the
final page (``next = null``) flips the run to ``published`` inside the same
transaction, so a whole generation becomes visible atomically.  Queries
always see the most recent *complete* generation plus its source binding
(endpoint, revision, repository snapshot id, feed token); a failed run
keeps its committed checkpoints and every previously published generation.
An HTTP 410 response invalidates the staged generation: its rows are
deleted in one transaction and the next run re-collects from page one.

A staged (incomplete) run is bound to its endpoint, revision and repository
snapshot id; continuing with different parameters is rejected.
``restart=True`` (CLI ``--restart``) discards a staged run explicitly.

Bounded resources
-----------------
At most one page (<=1000 events, <=8 MB raw) is held in Python memory;
events stream into SQLite page by page and deduplication runs as set-based
SQL against a bounded temporary table.  A million events stay well below a
64 MiB peak-RSS budget (measured on the real CLI process).
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

from .common import ident
from .reconcile import ReconcileError, _load_revision, parse_utc_time

SCHEMA_VERSION = 1
MAX_PAGE_EVENTS = 1000
MAX_PAGE_BYTES = 8_000_000
MAX_TOKEN = 4096
MAX_ENDPOINT = 4096
_HTTP_TIMEOUT = 30.0
_STATES = ("up", "down")

# Environment test seam: when set, every page transaction sleeps this many
# seconds with the write transaction open immediately before COMMIT, so
# real-process tests can kill the collector inside the commit window.
# Not part of the public API.
_COMMIT_DELAY_ENV = "SUPPLY_FLOW_TEST_OBSERVE_COMMIT_DELAY"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs(
    run_id INTEGER PRIMARY KEY,
    endpoint TEXT NOT NULL,
    revision INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('collecting','published')),
    pages INTEGER NOT NULL DEFAULT 0,
    received INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    published_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_collecting_run
    ON runs(state) WHERE state='collecting';
CREATE TABLE IF NOT EXISTS pages(
    run_id INTEGER NOT NULL REFERENCES runs(run_id),
    page_seq INTEGER NOT NULL,
    request_cursor TEXT,
    page_sha256 TEXT NOT NULL,
    next_cursor TEXT,
    event_count INTEGER NOT NULL,
    PRIMARY KEY(run_id, page_seq)
);
CREATE TABLE IF NOT EXISTS events(
    run_id INTEGER NOT NULL,
    id TEXT NOT NULL,
    link_id TEXT NOT NULL,
    epoch_s INTEGER NOT NULL,
    nanos INTEGER NOT NULL,
    time_utc TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('up','down')),
    PRIMARY KEY(run_id, id)
) WITHOUT ROWID;
"""

_TEMP_TABLE = """
CREATE TEMP TABLE IF NOT EXISTS page_events(
    id TEXT NOT NULL,
    link_id TEXT NOT NULL,
    epoch_s INTEGER NOT NULL,
    nanos INTEGER NOT NULL,
    time_utc TEXT NOT NULL,
    state TEXT NOT NULL
);
"""


class ObserveError(Exception):
    """Invalid feed, binding mismatch, unresolvable link or bad state."""


class InvalidatedError(ObserveError):
    """HTTP 410: the staged generation is gone; rerun to recollect."""


# ---------------------------------------------------------------------------
# Collection database
# ---------------------------------------------------------------------------

def _connect_collect(path: Path) -> sqlite3.Connection:
    parent = path.parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=30.0)
    connection.row_factory = sqlite3.Row
    # WAL lets readers query the last complete generation while the writer
    # commits pages; FULL sync is the strongest standard durability mode.
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA cache_size=-8192")
    connection.executescript(_SCHEMA)
    connection.execute(
        "INSERT OR IGNORE INTO meta(key,value) VALUES('schema_version',?)",
        (str(SCHEMA_VERSION),),
    )
    connection.execute(_TEMP_TABLE)
    return connection


def _open_readonly(path: Path) -> sqlite3.Connection:
    """Open a collection database for querying without ever modifying it."""
    if not path.is_file():
        raise ObserveError("collection database does not exist: " + str(path))
    options = "?mode=ro"
    wal = Path(str(path) + "-wal")
    # Without a live WAL sidecar the file is self-contained and can be read
    # immutably, creating no files beside the database.
    if not wal.exists() or wal.stat().st_size == 0:
        options += "&immutable=1"
    uri = "file:" + quote(str(path.resolve())) + options
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
    except sqlite3.Error as error:
        raise ObserveError(
            "cannot open collection database read-only: %s (%s)" % (path, error)
        ) from None
    return connection


# ---------------------------------------------------------------------------
# HTTP fetching and page validation
# ---------------------------------------------------------------------------

def _validate_endpoint(endpoint) -> str:
    if not isinstance(endpoint, str) or not endpoint or len(endpoint) > MAX_ENDPOINT:
        raise ObserveError("endpoint must be a non-empty URL string")
    parts = urlsplit(endpoint)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ObserveError("endpoint must be an http:// or https:// URL")
    return endpoint


def _strict_loads(text: str):
    """JSON with the house strictness: duplicate keys and non-finite numbers fail."""

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ObserveError("duplicate JSON key: " + key)
            result[key] = value
        return result

    def invalid(value):
        raise ObserveError("nonfinite JSON number: " + value)

    try:
        return json.loads(text, object_pairs_hook=unique, parse_constant=invalid)
    except json.JSONDecodeError as error:
        raise ObserveError("page is not valid JSON: %s" % error) from None


def _fetch_page(endpoint: str, generation: str | None, cursor: str | None):
    """GET one page; return (raw bytes, parsed JSON)."""
    if cursor is None:
        url = endpoint
    else:
        separator = "&" if "?" in endpoint else "?"
        url = endpoint + separator + urlencode(
            {"snapshot": generation, "cursor": cursor}
        )
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=_HTTP_TIMEOUT) as response:
            raw = response.read(MAX_PAGE_BYTES + 1)
    except HTTPError as error:
        if error.code == 410:
            raise InvalidatedError(
                "endpoint returned HTTP 410 Gone: the staged generation is no"
                " longer valid; staged checkpoints were discarded, rerun to"
                " recollect"
            ) from None
        raise ObserveError(
            "endpoint returned HTTP %d for %s" % (error.code, url)
        ) from None
    except URLError as error:
        raise ObserveError("cannot fetch %s: %s" % (url, error.reason)) from None
    except OSError as error:
        raise ObserveError("cannot fetch %s: %s" % (url, error)) from None
    if len(raw) > MAX_PAGE_BYTES:
        raise ObserveError("page exceeds %d bytes" % MAX_PAGE_BYTES)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ObserveError("page is not valid UTF-8") from None
    return raw, _strict_loads(text)


def _check_token(value, field: str, page_seq: int) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_TOKEN:
        raise ObserveError(
            "page %d: %s must be a non-empty string (<=%d characters)"
            % (page_seq, field, MAX_TOKEN)
        )
    return value


def _parse_page(page, expected_generation: str | None, link_map: dict, page_seq: int):
    """Validate one page envelope and its events; return normalized parts."""
    if not isinstance(page, dict) or set(page) != {"snapshot", "next", "events"}:
        raise ObserveError(
            "page %d: expected an object with exactly snapshot/next/events" % page_seq
        )
    token = _check_token(page["snapshot"], "snapshot", page_seq)
    if expected_generation is not None and token != expected_generation:
        raise ObserveError(
            "page %d: snapshot token changed mid-generation (%r -> %r)"
            % (page_seq, expected_generation, token)
        )
    next_cursor = page["next"]
    if next_cursor is not None:
        _check_token(next_cursor, "next", page_seq)
    events = page["events"]
    if not isinstance(events, list) or len(events) > MAX_PAGE_EVENTS:
        raise ObserveError(
            "page %d: events must be an array of at most %d items"
            % (page_seq, MAX_PAGE_EVENTS)
        )
    normalized = []
    for index, event in enumerate(events):
        where = "page %d event %d" % (page_seq, index)
        if not isinstance(event, dict) or set(event) != {"id", "link_id", "time", "state"}:
            raise ObserveError(where + ": expected exactly id/link_id/time/state")
        try:
            event_id = ident(event["id"])
            link_id = ident(event["link_id"])
        except (ValueError, TypeError) as error:
            raise ObserveError("%s: %s" % (where, error)) from None
        state = event["state"]
        if state not in _STATES:
            raise ObserveError(where + ": state must be 'up' or 'down'")
        try:
            seconds, nanos, utc = parse_utc_time(event["time"])
        except (ValueError, TypeError) as error:
            raise ObserveError("%s: %s" % (where, error)) from None
        if link_id not in link_map:
            raise ObserveError(
                "%s: link_id %r is not defined in the bound revision"
                % (where, link_id)
            )
        normalized.append((event_id, link_id, seconds, nanos, utc, state))
    return token, next_cursor, normalized


# ---------------------------------------------------------------------------
# Page storage (one transaction per page)
# ---------------------------------------------------------------------------

def _commit_with_test_delay(connection: sqlite3.Connection) -> None:
    delay = os.environ.get(_COMMIT_DELAY_ENV)
    if delay:
        time.sleep(float(delay))
    connection.execute("COMMIT")


def _store_page(
    connection: sqlite3.Connection,
    run_id: int | None,
    page_seq: int,
    token: str,
    next_cursor: str | None,
    events: list,
    digest: str,
    request_cursor: str | None,
    binding: tuple | None,
) -> int:
    """Commit one page, its events and the next cursor in one transaction.

    When ``binding`` is given this is the first page of a new run: the run
    row is created inside the same transaction, so a kill can never leave a
    run without its first page.  When ``next_cursor`` is None the run is
    published inside the same transaction, so a generation becomes visible
    atomically.  Returns the run id.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        if binding is not None:
            endpoint, revision, snap_id = binding
            try:
                cursor = connection.execute(
                    "INSERT INTO runs(endpoint,revision,snapshot_id,generation,state)"
                    " VALUES(?,?,?,?,'collecting')",
                    (endpoint, revision, snap_id, token),
                )
            except sqlite3.IntegrityError:
                raise ObserveError(
                    "another collection run is already in progress for this"
                    " collection database"
                ) from None
            run_id = cursor.lastrowid
        connection.execute("DELETE FROM page_events")
        connection.executemany(
            "INSERT INTO page_events(id,link_id,epoch_s,nanos,time_utc,state)"
            " VALUES(?,?,?,?,?,?)",
            events,
        )
        # Same id with two different canonical values inside one page.
        conflict = connection.execute(
            "SELECT id FROM page_events GROUP BY id"
            " HAVING COUNT(*)>1 AND COUNT(DISTINCT"
            " link_id||char(31)||epoch_s||char(31)||nanos||char(31)||state)>1"
            " LIMIT 1"
        ).fetchone()
        if conflict is not None:
            raise ObserveError(
                "page %d: event id %r repeats with conflicting content"
                % (page_seq, conflict[0])
            )
        # Same id with a different canonical value on an earlier page.
        conflict = connection.execute(
            "SELECT p.id FROM page_events p JOIN events e"
            " ON e.run_id=? AND e.id=p.id"
            " WHERE (p.link_id,p.epoch_s,p.nanos,p.state)!="
            " (e.link_id,e.epoch_s,e.nanos,e.state) LIMIT 1",
            (run_id,),
        ).fetchone()
        if conflict is not None:
            raise ObserveError(
                "page %d: event id %r conflicts with an earlier page of this"
                " generation" % (page_seq, conflict[0])
            )
        distinct_ids = connection.execute(
            "SELECT COUNT(DISTINCT id) FROM page_events"
        ).fetchone()[0]
        existing = connection.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT id FROM page_events) p"
            " WHERE EXISTS(SELECT 1 FROM events e"
            " WHERE e.run_id=? AND e.id=p.id)",
            (run_id,),
        ).fetchone()[0]
        inserted = distinct_ids - existing
        connection.execute(
            "INSERT INTO events(run_id,id,link_id,epoch_s,nanos,time_utc,state)"
            " SELECT ?,p.id,p.link_id,p.epoch_s,p.nanos,p.time_utc,p.state FROM"
            " (SELECT DISTINCT id,link_id,epoch_s,nanos,time_utc,state"
            " FROM page_events) p"
            " WHERE NOT EXISTS(SELECT 1 FROM events e"
            " WHERE e.run_id=? AND e.id=p.id)",
            (run_id, run_id),
        )
        duplicates = len(events) - inserted
        connection.execute(
            "INSERT INTO pages(run_id,page_seq,request_cursor,page_sha256,"
            "next_cursor,event_count) VALUES(?,?,?,?,?,?)",
            (run_id, page_seq, request_cursor, digest, next_cursor, len(events)),
        )
        if next_cursor is None:
            connection.execute(
                "UPDATE runs SET state='published',"
                " published_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),"
                " pages=pages+1, received=received+?, duplicates=duplicates+?"
                " WHERE run_id=?",
                (len(events), duplicates, run_id),
            )
        else:
            connection.execute(
                "UPDATE runs SET pages=pages+1, received=received+?,"
                " duplicates=duplicates+? WHERE run_id=?",
                (len(events), duplicates, run_id),
            )
        _commit_with_test_delay(connection)
    except BaseException:
        connection.rollback()
        raise
    return run_id


def _assert_fresh_cursor(connection: sqlite3.Connection, run_id: int, cursor: str) -> None:
    row = connection.execute(
        "SELECT page_seq FROM pages WHERE run_id=? AND request_cursor=? LIMIT 1",
        (run_id, cursor),
    ).fetchone()
    if row is not None:
        raise ObserveError(
            "cursor loop detected: cursor %r was already used at page %d"
            % (cursor, row[0])
        )


def _staged_run(connection: sqlite3.Connection):
    return connection.execute(
        "SELECT run_id,endpoint,revision,snapshot_id,generation,pages,received,"
        "duplicates FROM runs WHERE state='collecting'"
    ).fetchone()


def _delete_run(connection: sqlite3.Connection, run_id: int) -> None:
    """Discard one staged run's rows in a single transaction."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("DELETE FROM events WHERE run_id=?", (run_id,))
        connection.execute("DELETE FROM pages WHERE run_id=?", (run_id,))
        connection.execute(
            "DELETE FROM runs WHERE run_id=? AND state='collecting'", (run_id,)
        )
        _commit_with_test_delay(connection)
    except BaseException:
        connection.rollback()
        raise


def _check_binding(run, endpoint: str, revision: int, snapshot_id: str) -> None:
    if (
        run["endpoint"] == endpoint
        and run["revision"] == revision
        and run["snapshot_id"] == snapshot_id
    ):
        return
    raise ObserveError(
        "a staged collection is bound to endpoint=%r revision=%d"
        " snapshot_id=%s; refusing to continue with endpoint=%r revision=%d"
        " snapshot_id=%s (use --restart to discard the staged generation)"
        % (
            run["endpoint"], run["revision"], run["snapshot_id"],
            endpoint, revision, snapshot_id,
        )
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def observe(
    repository: str | os.PathLike,
    revision: int,
    endpoint: str,
    collect: str | os.PathLike,
    restart: bool = False,
) -> dict:
    """Collect one complete generation from a paged HTTP feed.

    Resumes a staged run bound to the same endpoint/revision/snapshot, or
    starts a new run.  Returns the published-generation summary.
    """
    repo_path = Path(repository)
    collect_path = Path(collect)
    if type(revision) is not int or revision < 0:
        raise ObserveError("revision must be a non-negative integer")
    endpoint = _validate_endpoint(endpoint)
    if collect_path.is_symlink():
        raise ObserveError(
            "collection database cannot be a symlink: " + str(collect_path)
        )
    if repo_path.resolve() == collect_path.resolve():
        raise ObserveError("collection database must differ from the repository file")
    try:
        snap_id, link_map, _nodes = _load_revision(repo_path, revision)
    except ReconcileError as error:
        raise ObserveError(str(error)) from None

    connection = _connect_collect(collect_path)
    try:
        if restart:
            staged = _staged_run(connection)
            if staged is not None:
                _delete_run(connection, staged["run_id"])
        run = _staged_run(connection)
        resumed = run is not None
        if run is not None:
            _check_binding(run, endpoint, revision, snap_id)
            run_id = run["run_id"]
            generation = run["generation"]
            row = connection.execute(
                "SELECT next_cursor FROM pages WHERE run_id=?"
                " ORDER BY page_seq DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            # A collecting run always has at least one committed page whose
            # next cursor is non-null (a null next cursor publishes the run
            # in the same transaction).
            if row is None or row["next_cursor"] is None:
                raise ObserveError("staged run has no continuation cursor")
            cursor = row["next_cursor"]
            page_seq = run["pages"]
        else:
            run_id = None
            generation = None
            cursor = None
            page_seq = 0
        try:
            while True:
                if cursor is not None:
                    _assert_fresh_cursor(connection, run_id, cursor)
                raw, page = _fetch_page(endpoint, generation, cursor)
                digest = hashlib.sha256(raw).hexdigest()
                token, next_cursor, events = _parse_page(
                    page, generation, link_map, page_seq
                )
                binding = None
                if run_id is None:
                    generation = token  # the first page binds the generation
                    binding = (endpoint, revision, snap_id)
                run_id = _store_page(
                    connection, run_id, page_seq, token, next_cursor, events,
                    digest, cursor, binding,
                )
                if next_cursor is None:
                    break
                cursor = next_cursor
                page_seq += 1
        except InvalidatedError:
            if run_id is not None:
                _delete_run(connection, run_id)
            raise
        row = connection.execute(
            "SELECT run_id,endpoint,revision,snapshot_id,generation,pages,"
            "received,duplicates FROM runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        return {
            "collect": str(collect_path),
            "run_id": row["run_id"],
            "state": "published",
            "endpoint": row["endpoint"],
            "revision": row["revision"],
            "snapshot_id": row["snapshot_id"],
            "generation": row["generation"],
            "pages": row["pages"],
            "received": row["received"],
            "events": row["received"] - row["duplicates"],
            "duplicates": row["duplicates"],
            "resumed": resumed,
        }
    finally:
        connection.close()


def _latest_row(connection: sqlite3.Connection):
    return connection.execute(
        "SELECT run_id,endpoint,revision,snapshot_id,generation,pages,received,"
        "duplicates,started_at,published_at FROM runs WHERE state='published'"
        " ORDER BY run_id DESC LIMIT 1"
    ).fetchone()


def latest_generation(collect: str | os.PathLike) -> dict | None:
    """Most recent *complete* generation with its source binding, or None."""
    connection = _open_readonly(Path(collect))
    try:
        row = _latest_row(connection)
        if row is None:
            return None
        return {
            "run_id": row["run_id"],
            "endpoint": row["endpoint"],
            "revision": row["revision"],
            "snapshot_id": row["snapshot_id"],
            "generation": row["generation"],
            "pages": row["pages"],
            "received": row["received"],
            "events": row["received"] - row["duplicates"],
            "duplicates": row["duplicates"],
            "started_at": row["started_at"],
            "published_at": row["published_at"],
        }
    finally:
        connection.close()


def iter_events(collect: str | os.PathLike):
    """Yield events of the most recent complete generation as dicts.

    Events stream in deterministic ``(time, id)`` order with the canonical
    UTC time text; nothing is accumulated in memory.
    """
    connection = _open_readonly(Path(collect))
    try:
        row = _latest_row(connection)
        if row is None:
            return
        cursor = connection.execute(
            "SELECT id,link_id,time_utc,state FROM events WHERE run_id=?"
            " ORDER BY epoch_s,nanos,id",
            (row["run_id"],),
        )
        for event_id, link_id, time_utc, state in cursor:
            yield {"id": event_id, "link_id": link_id, "time": time_utc, "state": state}
    finally:
        connection.close()
