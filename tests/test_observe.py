"""Link observability collection tests.

In-process tests cover the page/event contract, canonical-value
deduplication, conflicts, cursor loops, snapshot-token changes, binding and
HTTP 410 invalidation against a local HTTP server.  ``RealProcessTests``
drive the actual CLI in fresh OS processes: SIGTERM before and after page
commits, invalidation, parameter-change rejection and reads during
collection, verifying that a recovered collection equals an uninterrupted
one.  ``MemoryBoundTests`` measures the peak RSS of the real CLI process
over a million events.  Only the standard library is used.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit

from supply_flow.observe import (
    InvalidatedError,
    ObserveError,
    iter_events,
    latest_generation,
    observe,
)
from supply_flow.repo import Repository, init_repository, snapshot_id

ROOT = Path(__file__).resolve().parents[1]
NETWORK = ROOT / "fixtures" / "reconcile_network.json"
DEMO_SERVER = ROOT / "fixtures" / "observe_server.py"
COMMIT_DELAY_ENV = "SUPPLY_FLOW_TEST_OBSERVE_COMMIT_DELAY"


def make_event(event_id, link_id="L1", moment="2024-03-01T00:00:00Z", state="up"):
    return {"id": event_id, "link_id": link_id, "time": moment, "state": state}


def make_feed(pages, token="gen-1", script=None):
    """Build a scripted feed from page bodies ("snapshot" defaults to token).

    Extra keys pass through and missing keys stay missing so malformed
    envelopes reach the collector verbatim.  ``script`` maps a page index to
    a behavior: {"delay": seconds}, {"drop": True}, {"status": code} or
    {"raw": bytes}.
    """
    full_pages = []
    cursors = {}
    for index, page in enumerate(pages):
        body = {"snapshot": token}
        body.update(page)
        full_pages.append(body)
        next_cursor = page.get("next")
        if next_cursor is not None and next_cursor not in cursors:
            cursors[next_cursor] = index + 1
    return {"token": token, "pages": full_pages, "cursors": cursors,
            "script": script or {}}


def dynamic_feed(page_count, per_page, links, token="gen-mem"):
    """A feed whose pages are generated on the fly (for the memory test)."""
    return {"token": token, "pages": [], "cursors": {}, "script": {},
            "dynamic": {"pages": page_count, "per_page": per_page, "links": links}}


def _dynamic_page(spec, index, token):
    per_page = spec["per_page"]
    links = spec["links"]
    start = index * per_page
    events = []
    for offset in range(per_page):
        n = start + offset
        events.append({
            "id": "E%07d" % n,
            "link_id": "L%02d" % (n % links),
            "time": "2024-03-01T%02d:%02d:%02dZ" % (n % 24, (n // 7) % 60, n % 60),
            "state": "up" if n % 2 else "down",
        })
    return {"snapshot": token,
            "next": None if index + 1 >= spec["pages"] else str(index + 1),
            "events": events}


class _FeedHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        feed = self.server.feed
        parsed = urlsplit(self.path)
        if parsed.path != "/feed":
            self.send_error(404)
            return
        params = parse_qs(parsed.query)
        dynamic = feed.get("dynamic")
        if not parsed.query:
            index = 0
        else:
            if params.get("snapshot", [None])[0] != feed["token"]:
                self.send_error(410)
                return
            cursor = params.get("cursor", [None])[0]
            if dynamic is not None:
                try:
                    index = int(cursor)
                except (TypeError, ValueError):
                    index = -1
                if not 1 <= index < dynamic["pages"]:
                    self.send_error(400)
                    return
            else:
                index = feed["cursors"].get(cursor)
                if index is None:
                    self.send_error(400)
                    return
        action = feed["script"].get(index, {})
        if action.get("delay"):
            time.sleep(action["delay"])
        if action.get("drop"):
            self.connection.close()
            return
        if "status" in action:
            self.send_error(action["status"])
            return
        body = action.get("raw")
        if body is None:
            if dynamic is not None:
                page = _dynamic_page(dynamic, index, feed["token"])
            else:
                page = feed["pages"][index]
            body = json.dumps(page).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _FeedServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass  # killed clients close sockets mid-response; stay silent


class FeedServer:
    def __init__(self, feed=None):
        if feed is None:
            feed = make_feed([{"next": None, "events": []}])
        self._httpd = _FeedServer(("127.0.0.1", 0), _FeedHandler)
        self._httpd.feed = feed
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        self.url = "http://127.0.0.1:%d/feed" % self._httpd.server_address[1]

    def set_feed(self, feed):
        self._httpd.feed = feed

    def close(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(10)


def rows_of(database, sql, args=()):
    connection = sqlite3.connect(str(database))
    try:
        return connection.execute(sql, args).fetchall()
    finally:
        connection.close()


def page_count(database):
    return rows_of(database, "SELECT COUNT(*) FROM pages")[0][0]


def dump_db(database):
    """Comparable content of a collection database (timestamps excluded)."""
    connection = sqlite3.connect(str(database))
    try:
        events = connection.execute(
            "SELECT id,link_id,epoch_s,nanos,time_utc,state FROM events"
            " ORDER BY run_id,id").fetchall()
        pages = connection.execute(
            "SELECT run_id,page_seq,request_cursor,page_sha256,next_cursor,"
            "event_count FROM pages ORDER BY run_id,page_seq").fetchall()
        runs = connection.execute(
            "SELECT run_id,endpoint,revision,snapshot_id,generation,state,pages,"
            "received,duplicates FROM runs ORDER BY run_id").fetchall()
        return events, pages, runs
    finally:
        connection.close()


def wait_for_page_count(database, count, timeout=30.0):
    """Poll the collection database (read-only) until it holds count pages."""
    deadline = time.monotonic() + timeout
    uri = "file:" + quote(str(Path(database).resolve())) + "?mode=ro"
    while time.monotonic() < deadline:
        try:
            connection = sqlite3.connect(uri, uri=True)
            try:
                row = connection.execute("SELECT COUNT(*) FROM pages").fetchone()
            finally:
                connection.close()
            if row and row[0] >= count:
                return True
        except sqlite3.Error:
            pass
        time.sleep(0.05)
    return False


class ObserveCase(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.dir = Path(self._temp.name)
        self.repo = self.dir / "repo.db"
        init_repository(self.repo, NETWORK)
        self.collect = self.dir / "obs.db"
        self.server = FeedServer()

    def tearDown(self):
        self.server.close()
        self._temp.cleanup()

    def collect_with(self, pages, **feed_kwargs):
        self.server.set_feed(make_feed(pages, **feed_kwargs))
        return observe(self.repo, 0, self.server.url, self.collect)


class FullCollectionTests(ObserveCase):
    def test_multi_page_collection_binds_sources_and_stores_pages(self):
        pages = [
            {"next": "cur/sor +=&%", "events": [
                make_event("E1"),
                make_event("E2", "L2", "2024-03-01T08:00:00+08:00", "down"),
            ]},
            {"next": "c2", "events": [make_event("E3", "L3", "2024-03-01T00:00:05Z")]},
            {"next": None, "events": [make_event("E4", "L2", "2024-03-01T00:00:09Z", "down")]},
        ]
        summary = self.collect_with(pages, token="gen/1 +x=")
        self.assertFalse(summary["resumed"])
        self.assertEqual(summary["pages"], 3)
        self.assertEqual(summary["events"], 4)
        self.assertEqual(summary["duplicates"], 0)
        self.assertEqual(summary["generation"], "gen/1 +x=")
        self.assertEqual(summary["endpoint"], self.server.url)
        self.assertEqual(summary["revision"], 0)
        with Repository(self.repo) as repo:
            self.assertEqual(summary["snapshot_id"], repo.head()["snapshot_id"])
        # Events are normalized to canonical UTC text.
        self.assertEqual(
            rows_of(self.collect,
                    "SELECT id,link_id,time_utc,state FROM events ORDER BY id"),
            [("E1", "L1", "2024-03-01T00:00:00Z", "up"),
             ("E2", "L2", "2024-03-01T00:00:00Z", "down"),
             ("E3", "L3", "2024-03-01T00:00:05Z", "up"),
             ("E4", "L2", "2024-03-01T00:00:09Z", "down")],
        )
        # Every page row carries the request cursor, the raw-page SHA256 and
        # the next cursor; the exotic cursor round-tripped through the URL.
        stored = rows_of(
            self.collect,
            "SELECT page_seq,request_cursor,next_cursor,event_count,"
            " length(page_sha256) FROM pages ORDER BY page_seq")
        self.assertEqual(
            stored,
            [(0, None, "cur/sor +=&%", 2, 64),
             (1, "cur/sor +=&%", "c2", 1, 64),
             (2, "c2", None, 1, 64)],
        )
        self.assertEqual(
            rows_of(self.collect, "SELECT state FROM runs"), [("published",)])

    def test_single_page_generation_publishes_atomically(self):
        summary = self.collect_with([{"next": None, "events": [make_event("E1")]}])
        self.assertEqual(summary["pages"], 1)
        self.assertEqual(summary["state"], "published")

    def test_repository_is_not_modified(self):
        before = self.repo.read_bytes()
        mtime = self.repo.stat().st_mtime_ns
        self.collect_with([{"next": None, "events": [make_event("E1")]}])
        self.assertEqual(self.repo.read_bytes(), before)
        self.assertEqual(self.repo.stat().st_mtime_ns, mtime)
        self.assertFalse(Path(str(self.repo) + "-wal").exists())

    def test_unknown_revision_and_bad_endpoint_are_rejected(self):
        with self.assertRaises(ObserveError):
            observe(self.repo, 9, self.server.url, self.collect)
        with self.assertRaises(ObserveError):
            observe(self.repo, 0, "ftp://example/feed", self.collect)
        with self.assertRaises(ObserveError):
            observe(self.repo, 0, "", self.collect)
        with self.assertRaises(ObserveError):
            observe(self.repo, -1, self.server.url, self.collect)
        self.assertFalse(self.collect.exists())


class PageContractTests(ObserveCase):
    def test_exactly_1000_events_per_page_is_the_limit(self):
        events = [make_event("E%04d" % i) for i in range(1000)]
        summary = self.collect_with([{"next": None, "events": events}])
        self.assertEqual(summary["events"], 1000)
        too_many = [make_event("E%04d" % i) for i in range(1001)]
        with self.assertRaises(ObserveError) as caught:
            self.collect_with([{"next": None, "events": too_many}])
        self.assertIn("1000", str(caught.exception))

    def test_state_must_be_up_or_down(self):
        for bad in ["UP", "flapping", "", 1, None]:
            with self.subTest(state=bad):
                with self.assertRaises(ObserveError) as caught:
                    self.collect_with([{"next": None, "events": [
                        make_event("E1", state=bad)]}])
                self.assertIn("state", str(caught.exception))

    def test_time_must_carry_an_explicit_timezone(self):
        for bad in ["2024-03-01T00:00:00", "2024-03-01 00:00:00Z",
                    "2024-02-30T00:00:00Z", "not-a-time", 0]:
            with self.subTest(time=bad):
                with self.assertRaises(ObserveError):
                    self.collect_with([{"next": None, "events": [
                        make_event("E1", moment=bad)]}])

    def test_link_must_exist_in_the_bound_revision(self):
        with self.assertRaises(ObserveError) as caught:
            self.collect_with([{"next": None, "events": [make_event("E1", "L9")]}])
        self.assertIn("L9", str(caught.exception))

    def test_event_fields_are_exact(self):
        extra = dict(make_event("E1"), quantity=3)
        missing = {"id": "E1", "link_id": "L1", "time": "2024-03-01T00:00:00Z"}
        for bad in [extra, missing, "not-an-object"]:
            with self.subTest(event=bad):
                with self.assertRaises(ObserveError):
                    self.collect_with([{"next": None, "events": [bad]}])

    def test_envelope_fields_and_tokens_are_validated(self):
        cases = [
            [{"next": None, "events": [], "snapshot": ""}],          # empty token
            [{"next": "", "events": []}],                            # empty cursor
            [{"next": None, "events": [], "unexpected": 1}],         # extra key
            [{"events": []}],                                        # missing keys
        ]
        for pages in cases:
            with self.subTest(pages=pages):
                with self.assertRaises(ObserveError):
                    self.collect_with(pages)

    def test_non_json_duplicate_key_and_oversized_pages_are_rejected(self):
        with self.assertRaises(ObserveError):
            self.collect_with([{"next": None, "events": []}],
                              script={0: {"raw": b"not json"}})
        with self.assertRaises(ObserveError) as caught:
            self.collect_with(
                [{"next": None, "events": []}],
                script={0: {"raw": b'{"snapshot":"g","snapshot":"g",'
                                   b'"next":null,"events":[]}'}})
        self.assertIn("duplicate JSON key", str(caught.exception))
        huge = (b'{"snapshot":"g","next":null,"events":[],'
                b'"pad":"' + b"x" * 9_000_000 + b'"}')
        with self.assertRaises(ObserveError) as caught:
            self.collect_with([{"next": None, "events": []}],
                              script={0: {"raw": huge}})
        self.assertIn("exceeds", str(caught.exception))

    def test_http_error_status_is_reported(self):
        with self.assertRaises(ObserveError) as caught:
            self.collect_with([{"next": None, "events": []}],
                              script={0: {"status": 500}})
        self.assertIn("HTTP 500", str(caught.exception))


class DedupeAndConflictTests(ObserveCase):
    def test_same_canonical_value_dedupes_within_and_across_pages(self):
        pages = [
            {"next": "c1", "events": [
                make_event("E1", moment="2024-03-01T00:00:00Z"),
                make_event("E2"),
            ]},
            {"next": "c2", "events": [
                # Same id, same instant written with another offset: one event.
                make_event("E1", moment="2024-03-01T08:00:00+08:00"),
                make_event("E3"),
            ]},
            {"next": None, "events": [make_event("E3"), make_event("E3")]},
        ]
        summary = self.collect_with(pages)
        self.assertEqual(summary["received"], 6)
        self.assertEqual(summary["events"], 3)
        self.assertEqual(summary["duplicates"], 3)
        self.assertEqual(
            rows_of(self.collect, "SELECT id,time_utc FROM events ORDER BY id"),
            [("E1", "2024-03-01T00:00:00Z"), ("E2", "2024-03-01T00:00:00Z"),
             ("E3", "2024-03-01T00:00:00Z")],
        )

    def test_conflicting_ids_on_one_page_are_rejected(self):
        pages = [{"next": None, "events": [
            make_event("E1", state="up"),
            make_event("E1", state="down"),
        ]}]
        with self.assertRaises(ObserveError) as caught:
            self.collect_with(pages)
        self.assertIn("E1", str(caught.exception))
        self.assertEqual(rows_of(self.collect, "SELECT COUNT(*) FROM events"), [(0,)])

    def test_conflicting_ids_across_pages_are_rejected_and_checkpoint_kept(self):
        pages = [
            {"next": "c1", "events": [make_event("E1", state="up")]},
            {"next": None, "events": [make_event("E1", moment="2024-03-01T00:00:01Z")]},
        ]
        with self.assertRaises(ObserveError) as caught:
            self.collect_with(pages)
        self.assertIn("conflict", str(caught.exception).lower())
        # The committed first page remains as a checkpoint; nothing published.
        self.assertEqual(page_count(self.collect), 1)
        self.assertIsNone(latest_generation(self.collect))
        # Fixing the feed lets the same staged run resume and finish.
        fixed = [
            {"next": "c1", "events": [make_event("E1", state="up")]},
            {"next": None, "events": [make_event("E9", moment="2024-03-01T00:00:01Z")]},
        ]
        summary = self.collect_with(fixed)
        self.assertTrue(summary["resumed"])
        self.assertEqual(summary["events"], 2)


class CursorAndTokenTests(ObserveCase):
    def test_cursor_loop_is_rejected(self):
        pages = [
            {"next": "c1", "events": [make_event("E1")]},
            {"next": "c2", "events": [make_event("E2")]},
            {"next": "c1", "events": [make_event("E3")]},  # loops back to c1
        ]
        with self.assertRaises(ObserveError) as caught:
            self.collect_with(pages)
        self.assertIn("cursor loop", str(caught.exception))
        self.assertEqual(page_count(self.collect), 3)
        self.assertIsNone(latest_generation(self.collect))

    def test_snapshot_token_change_is_rejected(self):
        pages = [
            {"next": "c1", "events": [make_event("E1")]},
            {"next": None, "events": [make_event("E2")], "snapshot": "gen-OTHER"},
        ]
        with self.assertRaises(ObserveError) as caught:
            self.collect_with(pages)
        self.assertIn("snapshot token changed", str(caught.exception))
        self.assertEqual(page_count(self.collect), 1)
        self.assertIsNone(latest_generation(self.collect))


class ResumeAndInvalidationTests(ObserveCase):
    def test_interrupted_collection_resumes_and_matches_full_collection(self):
        pages = [
            {"next": "c1", "events": [make_event("E1"), make_event("E2", "L2")]},
            {"next": "c2", "events": [make_event("E3", "L3", state="down")]},
            {"next": "c3", "events": [make_event("E4", moment="2024-03-01T08:00:00+08:00")]},
            {"next": None, "events": [make_event("E5", "L2", "2024-03-01T00:00:09Z", "down")]},
        ]
        # First attempt: the connection drops while fetching page 2.
        self.server.set_feed(make_feed(pages, script={2: {"drop": True}}))
        with self.assertRaises(ObserveError):
            observe(self.repo, 0, self.server.url, self.collect)
        self.assertEqual(page_count(self.collect), 2)
        self.assertIsNone(latest_generation(self.collect))
        # Second attempt resumes from the committed checkpoint.
        self.server.set_feed(make_feed(pages))
        summary = observe(self.repo, 0, self.server.url, self.collect)
        self.assertTrue(summary["resumed"])
        self.assertEqual(summary["pages"], 4)
        # Recovery equals an uninterrupted full collection.
        full = self.dir / "full.db"
        observe(self.repo, 0, self.server.url, full)
        self.assertEqual(dump_db(self.collect), dump_db(full))

    def test_410_invalidates_the_staged_generation_and_recollects(self):
        pages = [
            {"next": "c1", "events": [make_event("E1")]},
            {"next": None, "events": [make_event("E2")]},
        ]
        self.server.set_feed(make_feed(pages, script={1: {"drop": True}}))
        with self.assertRaises(ObserveError):
            observe(self.repo, 0, self.server.url, self.collect)
        self.assertEqual(page_count(self.collect), 1)
        # The endpoint now reports the staged generation as gone.
        self.server.set_feed(make_feed(pages, script={1: {"status": 410}}))
        with self.assertRaises(InvalidatedError):
            observe(self.repo, 0, self.server.url, self.collect)
        self.assertEqual(rows_of(self.collect, "SELECT COUNT(*) FROM runs"), [(0,)])
        self.assertEqual(rows_of(self.collect, "SELECT COUNT(*) FROM events"), [(0,)])
        # The next run re-collects from the first page.
        self.server.set_feed(make_feed(pages))
        summary = observe(self.repo, 0, self.server.url, self.collect)
        self.assertFalse(summary["resumed"])
        self.assertEqual(summary["events"], 2)

    def test_410_keeps_previously_published_generations(self):
        self.collect_with([{"next": None, "events": [make_event("E0")]}], token="gen-old")
        pages = [{"next": "c1", "events": [make_event("E1")]},
                 {"next": None, "events": [make_event("E2")]}]
        self.server.set_feed(make_feed(pages, token="gen-new", script={1: {"status": 410}}))
        with self.assertRaises(InvalidatedError):
            observe(self.repo, 0, self.server.url, self.collect)
        latest = latest_generation(self.collect)
        self.assertEqual(latest["generation"], "gen-old")
        self.assertEqual([event["id"] for event in iter_events(self.collect)], ["E0"])

    def test_parameter_change_rejects_continuation_and_restart_discards(self):
        pages = [{"next": "c1", "events": [make_event("E1")]},
                 {"next": None, "events": [make_event("E2")]}]
        self.server.set_feed(make_feed(pages, script={1: {"drop": True}}))
        with self.assertRaises(ObserveError):
            observe(self.repo, 0, self.server.url, self.collect)
        # A different endpoint must not continue the staged run.
        with self.assertRaises(ObserveError) as caught:
            observe(self.repo, 0, "http://127.0.0.1:1/other", self.collect)
        self.assertIn("bound", str(caught.exception))
        # The staged run survives rejected attempts.
        self.assertEqual(page_count(self.collect), 1)
        # Same parameters resume; --restart discards and starts over.
        self.server.set_feed(make_feed(pages))
        summary = observe(self.repo, 0, self.server.url, self.collect)
        self.assertTrue(summary["resumed"])
        summary = observe(self.repo, 0, self.server.url, self.collect, restart=True)
        self.assertFalse(summary["resumed"])
        self.assertEqual(summary["events"], 2)

    def test_revision_binding_uses_the_verified_snapshot_id(self):
        # A second revision exists; the staged run stays bound to revision 0.
        pages = [{"next": "c1", "events": [make_event("E1")]},
                 {"next": None, "events": [make_event("E2")]}]
        self.server.set_feed(make_feed(pages, script={1: {"drop": True}}))
        with self.assertRaises(ObserveError):
            observe(self.repo, 0, self.server.url, self.collect)
        with Repository(self.repo) as repo:
            repo.apply({"base_revision": 0, "batch_id": "b1",
                        "upsert_nodes": ["extra"]})
        with self.assertRaises(ObserveError) as caught:
            observe(self.repo, 1, self.server.url, self.collect)
        self.assertIn("bound", str(caught.exception))


class QueryTests(ObserveCase):
    def test_latest_returns_newest_complete_generation_with_binding(self):
        self.collect_with([{"next": None, "events": [make_event("E1")]}], token="gen-1")
        self.collect_with(
            [{"next": "c1", "events": [make_event("E2", moment="2024-03-02T00:00:00Z")]},
             {"next": None, "events": [make_event("E3", "L2", "2024-03-01T00:00:00Z", "down")]}],
            token="gen-2")
        latest = latest_generation(self.collect)
        self.assertEqual(latest["generation"], "gen-2")
        self.assertEqual(latest["endpoint"], self.server.url)
        self.assertEqual(latest["revision"], 0)
        self.assertEqual(latest["snapshot_id"],
                         snapshot_id(json.loads(NETWORK.read_text())))
        self.assertEqual(latest["events"], 2)
        self.assertEqual(latest["pages"], 2)
        # Events stream in (time, id) order with canonical UTC text.
        self.assertEqual(
            list(iter_events(self.collect)),
            [{"id": "E3", "link_id": "L2", "time": "2024-03-01T00:00:00Z", "state": "down"},
             {"id": "E2", "link_id": "L1", "time": "2024-03-02T00:00:00Z", "state": "up"}],
        )

    def test_queries_on_missing_or_empty_database(self):
        with self.assertRaises(ObserveError):
            latest_generation(self.collect)
        # A database with only a staged (incomplete) run has no generation.
        pages = [{"next": "c1", "events": [make_event("E1")]},
                 {"next": None, "events": []}]
        self.server.set_feed(make_feed(pages, script={1: {"drop": True}}))
        with self.assertRaises(ObserveError):
            observe(self.repo, 0, self.server.url, self.collect)
        self.assertIsNone(latest_generation(self.collect))
        self.assertEqual(list(iter_events(self.collect)), [])

    def test_reads_during_collection_see_the_last_complete_generation(self):
        self.collect_with([{"next": None, "events": [make_event("E1")]}], token="gen-1")
        pages = [{"next": "c1", "events": [make_event("E2")]},
                 {"next": None, "events": [make_event("E3")]}]
        self.server.set_feed(make_feed(pages, token="gen-2", script={1: {"delay": 2.0}}))
        errors = []
        observed = []

        def collect():
            try:
                observe(self.repo, 0, self.server.url, self.collect)
            except Exception as error:  # pragma: no cover - surfaced below
                errors.append(error)

        worker = threading.Thread(target=collect)
        worker.start()
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and page_count(self.collect) < 2:
                time.sleep(0.05)
            observed.append(latest_generation(self.collect)["generation"])
            observed.append([event["id"] for event in iter_events(self.collect)])
        finally:
            worker.join(10)
        self.assertEqual(errors, [])
        self.assertEqual(observed, ["gen-1", ["E1"]])
        self.assertEqual(latest_generation(self.collect)["generation"], "gen-2")


class RealProcessTests(unittest.TestCase):
    """Each case drives the real CLI in fresh OS processes."""

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.dir = Path(self._temp.name)
        self.repo = self.dir / "repo.db"
        init_repository(self.repo, NETWORK)
        self.collect = self.dir / "obs.db"
        self.server = FeedServer()

    def tearDown(self):
        self.server.close()
        self._temp.cleanup()

    def run_cli(self, *arguments, env=None):
        full_env = dict(os.environ)
        if env:
            full_env.update(env)
        return subprocess.run(
            [sys.executable, "-m", "supply_flow.observe_cli", *map(str, arguments)],
            capture_output=True, text=True, env=full_env)

    def start_collect(self, env=None, **overrides):
        args = dict(db=self.repo, revision=0, endpoint=self.server.url,
                    collect=self.collect)
        args.update(overrides)
        full_env = dict(os.environ)
        if env:
            full_env.update(env)
        return subprocess.Popen(
            [sys.executable, "-m", "supply_flow.observe_cli", "collect",
             "--db", str(args["db"]), "--revision", str(args["revision"]),
             "--endpoint", str(args["endpoint"]), "--collect", str(args["collect"])],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=full_env)

    def kill(self, proc):
        proc.terminate()
        try:
            proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            proc.kill()
            proc.communicate()
        self.assertNotIn(proc.returncode, (0, None))

    def four_page_feed(self, script=None):
        pages = [
            {"next": "c1", "events": [make_event("E1"), make_event("E2", "L2")]},
            {"next": "c2", "events": [make_event("E3", "L3", state="down")]},
            {"next": "c3", "events": [make_event("E4", moment="2024-03-01T08:00:00+08:00")]},
            {"next": None, "events": [make_event("E5", "L2", "2024-03-01T00:00:09Z", "down")]},
        ]
        return make_feed(pages, script=script)

    def assert_matches_full_collection(self):
        full = self.dir / "full.db"
        self.server.set_feed(self.four_page_feed())
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", full)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(dump_db(self.collect), dump_db(full))

    def test_cli_collect_latest_events_lifecycle(self):
        self.server.set_feed(self.four_page_feed())
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["pages"], 4)
        self.assertEqual(summary["events"], 5)
        self.assertEqual(summary["state"], "published")

        result = self.run_cli("latest", "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)
        latest = json.loads(result.stdout)
        self.assertEqual(latest["generation"], "gen-1")
        self.assertEqual(latest["endpoint"], self.server.url)
        self.assertEqual(latest["revision"], 0)
        self.assertEqual(latest["snapshot_id"],
                         snapshot_id(json.loads(NETWORK.read_text())))
        self.assertEqual(latest["events"], 5)

        result = self.run_cli("events", "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)
        events = [json.loads(line) for line in result.stdout.splitlines()]
        # E1..E4 share the instant 00:00:00Z (E4 was written as 08:00+08:00)
        # and sort by id; E5 follows at 00:00:09Z.
        self.assertEqual([event["id"] for event in events],
                         ["E1", "E2", "E3", "E4", "E5"])
        self.assertEqual(events[3], {"id": "E4", "link_id": "L1",
                                     "time": "2024-03-01T00:00:00Z", "state": "up"})

    def test_cli_latest_on_fresh_and_missing_database(self):
        empty = self.dir / "empty.db"
        self.server.set_feed(make_feed([{"next": "c1", "events": [make_event("E1")]},
                                        {"next": None, "events": []}],
                                       script={1: {"drop": True}}))
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", empty)
        self.assertEqual(result.returncode, 2)
        result = self.run_cli("latest", "--collect", empty)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout), None)
        result = self.run_cli("events", "--collect", empty)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        result = self.run_cli("latest", "--collect", self.dir / "absent.db")
        self.assertEqual(result.returncode, 2)

    def test_kill_before_page_commit_resumes_and_matches_full_collection(self):
        self.server.set_feed(self.four_page_feed())
        proc = self.start_collect(env={COMMIT_DELAY_ENV: "2.0"})
        try:
            self.assertTrue(wait_for_page_count(self.collect, 2),
                            "collector did not commit two pages")
            self.kill(proc)  # SIGTERM lands inside page 2's commit window
        finally:
            if proc.poll() is None:  # pragma: no cover - defensive
                proc.kill()
                proc.communicate()
        self.assertEqual(page_count(self.collect), 2)
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertTrue(summary["resumed"])
        self.assertEqual(summary["pages"], 4)
        self.assert_matches_full_collection()

    def test_kill_after_page_commit_resumes_and_matches_full_collection(self):
        self.server.set_feed(self.four_page_feed(script={3: {"delay": 4.0}}))
        proc = self.start_collect()
        try:
            self.assertTrue(wait_for_page_count(self.collect, 3),
                            "collector did not commit three pages")
            self.kill(proc)  # SIGTERM lands while waiting for page 3
        finally:
            if proc.poll() is None:  # pragma: no cover - defensive
                proc.kill()
                proc.communicate()
        self.assertEqual(page_count(self.collect), 3)
        self.server.set_feed(self.four_page_feed())
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["resumed"])
        self.assert_matches_full_collection()

    def test_cli_410_invalidates_staged_and_next_run_recollects(self):
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)  # default feed: gen-1
        pages = [{"next": "c1", "events": [make_event("E1")]},
                 {"next": "c2", "events": [make_event("E2")]},
                 {"next": None, "events": [make_event("E3")]}]
        self.server.set_feed(make_feed(pages, token="gen-new", script={1: {"delay": 4.0}}))
        proc = self.start_collect()
        try:
            self.assertTrue(wait_for_page_count(self.collect, 2))
            self.kill(proc)
        finally:
            if proc.poll() is None:  # pragma: no cover - defensive
                proc.kill()
                proc.communicate()
        # The endpoint invalidates the staged generation with HTTP 410.
        self.server.set_feed(make_feed(pages, token="gen-new", script={1: {"status": 410}}))
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 4)
        self.assertIn("410", result.stderr)
        self.assertEqual(rows_of(self.collect, "SELECT COUNT(*) FROM runs"
                                 " WHERE state='collecting'"), [(0,)])
        # The previously published generation survived the invalidation.
        result = self.run_cli("latest", "--collect", self.collect)
        self.assertEqual(json.loads(result.stdout)["generation"], "gen-1")
        # The next run re-collects the new generation from the first page.
        self.server.set_feed(make_feed(pages, token="gen-new"))
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertFalse(summary["resumed"])
        self.assertEqual(summary["pages"], 3)
        result = self.run_cli("latest", "--collect", self.collect)
        self.assertEqual(json.loads(result.stdout)["generation"], "gen-new")

    def test_cli_cursor_loop_snapshot_change_and_conflict_exit_2(self):
        loop = [{"next": "c1", "events": [make_event("E1")]},
                {"next": "c1", "events": [make_event("E2")]}]
        self.server.set_feed(make_feed(loop))
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 2)
        self.assertIn("cursor loop", result.stderr)

        change = [{"next": "c1", "events": [make_event("E1")]},
                  {"next": None, "events": [make_event("E2")], "snapshot": "gen-X"}]
        self.server.set_feed(make_feed(change))
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.dir / "b.db")
        self.assertEqual(result.returncode, 2)
        self.assertIn("snapshot token changed", result.stderr)

        conflict = [{"next": "c1", "events": [make_event("E1", state="up")]},
                    {"next": None, "events": [make_event("E1", state="down")]}]
        self.server.set_feed(make_feed(conflict))
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.dir / "c.db")
        self.assertEqual(result.returncode, 2)
        self.assertIn("conflict", result.stderr)
        result = self.run_cli("latest", "--collect", self.dir / "c.db")
        self.assertEqual(json.loads(result.stdout), None)

    def test_cli_parameter_change_rejects_continuation(self):
        pages = [{"next": "c1", "events": [make_event("E1")]},
                 {"next": None, "events": [make_event("E2")]}]
        self.server.set_feed(make_feed(pages, script={1: {"delay": 4.0}}))
        proc = self.start_collect()
        try:
            self.assertTrue(wait_for_page_count(self.collect, 1))
            self.kill(proc)
        finally:
            if proc.poll() is None:  # pragma: no cover - defensive
                proc.kill()
                proc.communicate()
        self.server.set_feed(make_feed(pages))
        # A different endpoint must not continue the staged run.
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", "http://127.0.0.1:1/other",
                              "--collect", self.collect)
        self.assertEqual(result.returncode, 2)
        self.assertIn("bound", result.stderr)
        # Neither may a different existing revision.
        with Repository(self.repo) as repo:
            repo.apply({"base_revision": 0, "batch_id": "b1",
                        "upsert_nodes": ["extra"]})
        result = self.run_cli("collect", "--db", self.repo, "--revision", 1,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 2)
        self.assertIn("bound", result.stderr)
        # The original parameters resume the staged run to completion.
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["resumed"])

    def test_reads_during_collection_return_last_complete_generation(self):
        self.server.set_feed(make_feed(
            [{"next": None, "events": [make_event("E1")]}], token="gen-1"))
        result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                              "--endpoint", self.server.url, "--collect", self.collect)
        self.assertEqual(result.returncode, 0, result.stderr)  # gen-1 published
        pages = [{"next": "c1", "events": [make_event("E2")]},
                 {"next": "c2", "events": [make_event("E3")]},
                 {"next": None, "events": [make_event("E4")]}]
        self.server.set_feed(make_feed(pages, token="gen-2", script={1: {"delay": 4.0}}))
        proc = self.start_collect()
        try:
            # gen-1 committed one page; the staged run adds its first page.
            self.assertTrue(wait_for_page_count(self.collect, 2),
                            "staged run did not commit its first page")
            result = self.run_cli("latest", "--collect", self.collect)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["generation"], "gen-1")
            result = self.run_cli("events", "--collect", self.collect)
            self.assertEqual([json.loads(line)["id"]
                              for line in result.stdout.splitlines()], ["E1"])
        finally:
            stdout, stderr = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 0, stderr)
        result = self.run_cli("latest", "--collect", self.collect)
        self.assertEqual(json.loads(result.stdout)["generation"], "gen-2")
        result = self.run_cli("events", "--collect", self.collect)
        self.assertEqual([json.loads(line)["id"]
                          for line in result.stdout.splitlines()], ["E2", "E3", "E4"])

    def test_demo_fixture_server_matches_documented_example(self):
        server = subprocess.Popen(
            [sys.executable, str(DEMO_SERVER), "0"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            line = server.stdout.readline().strip()
            self.assertTrue(line.startswith("serving http://"), line)
            endpoint = line.split(" ", 1)[1]
            result = self.run_cli("collect", "--db", self.repo, "--revision", 0,
                                  "--endpoint", endpoint, "--collect", self.collect)
            self.assertEqual(result.returncode, 0, result.stderr)
            summary = json.loads(result.stdout)
            self.assertEqual(summary["received"], 6)
            self.assertEqual(summary["events"], 5)  # E2 deduplicated
            self.assertEqual(summary["duplicates"], 1)
        finally:
            server.terminate()
            server.communicate(timeout=10)


@unittest.skipUnless(
    os.environ.get("SUPPLY_FLOW_MEMORY_TEST", "1") == "1",
    "memory measurement disabled",
)
class MemoryBoundTests(unittest.TestCase):
    """Real CLI process over a 1,000,000-event generation."""

    EVENTS = 1_000_000
    PER_PAGE = 1000
    LINK_COUNT = 16
    LIMIT_MIB = 64

    @classmethod
    def setUpClass(cls):
        cls._temp = tempfile.TemporaryDirectory()
        cls.dir = Path(cls._temp.name)
        cls.repo = cls.dir / "repo.db"
        network = {
            "nodes": ["n%d" % index for index in range(cls.LINK_COUNT + 1)],
            "links": [
                {"id": "L%02d" % index, "from": "n%d" % index,
                 "to": "n%d" % (index + 1), "capacity": 10**9}
                for index in range(cls.LINK_COUNT)
            ],
            "supplies": [],
            "demands": [],
        }
        network_path = cls.dir / "network.json"
        network_path.write_text(json.dumps(network), encoding="utf-8")
        init_repository(cls.repo, network_path)
        cls.server = FeedServer(dynamic_feed(
            cls.EVENTS // cls.PER_PAGE, cls.PER_PAGE, cls.LINK_COUNT))

    @classmethod
    def tearDownClass(cls):
        cls.server.close()
        cls._temp.cleanup()

    def test_peak_rss_stays_under_64_mib_for_a_million_events(self):
        collect = self.dir / "memory.db"
        wrapper = (
            "import resource,subprocess,sys\n"
            "result=subprocess.run(sys.argv[1:],capture_output=True,text=True)\n"
            "sys.stdout.write(result.stdout)\n"
            "sys.stderr.write(result.stderr)\n"
            "sys.stderr.write('PEAK_RSS_KIB=%d\\n'%resource.getrusage("
            "resource.RUSAGE_CHILDREN).ru_maxrss)\n"
            "raise SystemExit(result.returncode)\n"
        )
        result = subprocess.run(
            [
                sys.executable, "-c", wrapper,
                sys.executable, "-m", "supply_flow.observe_cli", "collect",
                "--db", str(self.repo), "--revision", "0",
                "--endpoint", self.server.url,
                "--collect", str(collect),
            ],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        peak_line = [line for line in result.stderr.splitlines()
                     if line.startswith("PEAK_RSS_KIB=")]
        self.assertEqual(len(peak_line), 1, result.stderr)
        peak_mib = int(peak_line[0].split("=", 1)[1]) / 1024.0
        self.assertLess(
            peak_mib, self.LIMIT_MIB,
            "peak RSS %.1f MiB exceeded %d MiB" % (peak_mib, self.LIMIT_MIB))
        summary = json.loads(result.stdout)
        self.assertEqual(summary["received"], self.EVENTS)
        self.assertEqual(summary["events"], self.EVENTS)
        self.assertEqual(summary["duplicates"], 0)
        self.assertEqual(summary["pages"], self.EVENTS // self.PER_PAGE)
        connection = sqlite3.connect(str(collect))
        try:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                self.EVENTS)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
