"""Tests for the SQLite versioned repository.

The in-process cases cover the Python API; the multiprocessing cases exercise
real OS processes (independent connections, SQLite file locking and SIGTERM
before COMMIT) rather than threads or mocks.
"""
import hashlib
import json
import multiprocessing as mp
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from supply_flow import analyze
from supply_flow.repo import (
    ConflictError,
    RepoError,
    Repository,
    snapshot_id,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures" / "input.json"
COMMIT_DELAY_ENV = "SUPPLY_FLOW_TEST_COMMIT_DELAY"


def worker_apply(db_path, request, conn=None):
    """Top-level worker: apply one batch from a fresh process/connection."""
    try:
        with Repository(db_path) as repo:
            result = repo.apply(request)
        if conn is not None:
            conn.send(("ok", result))
    except ConflictError as error:
        if conn is not None:
            conn.send(("conflict", str(error)))
    except Exception as error:  # pragma: no cover - surfaced by the parent test
        if conn is not None:
            conn.send(("error", repr(error)))
    finally:
        if conn is not None:
            conn.close()


def worker_barrier(db_path, request, barrier, slot):
    """Two workers rendezvous at a barrier, then race the same head commit."""
    barrier.wait()
    try:
        with Repository(db_path) as repo:
            repo.apply(request)
        slot.value = 1
    except ConflictError:
        slot.value = 3
    except Exception:  # pragma: no cover - surfaced by the parent test
        slot.value = 2


class RepositoryAPITests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = Path(self.td.name) / "repo.db"
        with Repository(self.db) as repo:
            self.rev0 = repo.init(FIXTURE)
        self.base = json.loads(FIXTURE.read_text())

    def tearDown(self):
        self.td.cleanup()

    def test_init_imports_strict_json_and_rejects_reinit(self):
        with Repository(self.db) as repo:
            head = repo.head()
            self.assertEqual(head["revision"], 0)
            self.assertEqual(head["snapshot_id"], snapshot_id(self.base))
            network = repo.export(0)
            self.assertEqual(network, self.base)
            with self.assertRaises(RepoError):
                repo.init(FIXTURE)

    def test_snapshot_id_is_canonical_and_array_order_invariant(self):
        reordered = {
            "nodes": list(reversed(self.base["nodes"])),
            "links": list(reversed(self.base["links"])),
            "supplies": list(reversed(self.base["supplies"])),
            "demands": list(reversed(self.base["demands"])),
        }
        self.assertEqual(snapshot_id(reordered), snapshot_id(self.base))
        changed = json.loads(FIXTURE.read_text())
        changed["links"][0]["capacity"] = 6
        self.assertNotEqual(snapshot_id(changed), snapshot_id(self.base))

    def test_revision_increments_with_immutable_parents_and_full_snapshots(self):
        with Repository(self.db) as repo:
            r1 = repo.apply({"base_revision": 0, "batch_id": "b1",
                             "upsert_nodes": ["extra"]})
            r2 = repo.apply({"base_revision": 1, "batch_id": "b2",
                             "delete_nodes": ["extra"]})
            self.assertEqual((r1["revision"], r1["parent_revision"]), (1, 0))
            self.assertEqual((r2["revision"], r2["parent_revision"]), (2, 1))
            # revision 0 stays byte-identical and still hashes to its snapshot
            self.assertEqual(repo.export(0), self.base)
            history = repo.history()
            self.assertEqual([row["revision"] for row in history], [0, 1, 2])
            self.assertEqual([row["parent_revision"] for row in history], [-1, 0, 1])
            # deleting extra restores identical content: snapshots deduplicate,
            # while revision numbers still advanced.
            self.assertEqual(r2["snapshot_id"], self.rev0["snapshot_id"])
            stored = repo.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            self.assertEqual(stored, 2)

    def test_atomic_batch_swaps_node_and_edge_sets_then_validates_final_network(self):
        with Repository(self.db) as repo:
            # Reference-before-create inside one batch: links mention the new
            # nodes before the nodes are inserted; final validation succeeds.
            result = repo.apply({
                "base_revision": 0,
                "batch_id": "swap",
                "delete_links": ["L2", "L3"],
                "upsert_links": [
                    {"id": "L2", "from": "pump", "to": "east", "capacity": 9},
                    {"id": "LN", "from": "pump", "to": "north", "capacity": 3},
                ],
                "upsert_nodes": ["north"],
                "upsert_demands": [{"node": "north", "amount": 3}],
            })
            self.assertEqual(result["revision"], 1)
            network = repo.export(1)
            self.assertIn("north", network["nodes"])
            self.assertEqual({row["id"] for row in network["links"]},
                             {"L1", "L2", "LN"})
            # The batch is atomic: no intermediate state was ever visible.
            self.assertEqual(repo.head()["revision"], 1)

    def test_dangling_reference_after_batch_is_rejected_without_cascade(self):
        with Repository(self.db) as repo:
            # Deleting 'pump' does not cascade to its links; the final network
            # still references pump and must be rejected as a whole.
            with self.assertRaises(ValueError):
                repo.apply({"base_revision": 0, "batch_id": "bad",
                            "delete_nodes": ["pump"]})
            self.assertEqual(repo.head()["revision"], 0)
            # same for deleting a node still carrying supply/demand rows
            with self.assertRaises(ValueError):
                repo.apply({"base_revision": 0, "batch_id": "bad2",
                            "delete_nodes": ["east"]})
            self.assertEqual(repo.head()["revision"], 0)
            # no orphan batch/history rows were written by the failed attempt
            count = repo.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
            self.assertEqual(count, 0)
            self.assertEqual([row["revision"] for row in repo.history()], [0])

    def test_missing_delete_targets_and_malformed_rows_are_rejected(self):
        with Repository(self.db) as repo:
            with self.assertRaises(RepoError):
                repo.apply({"base_revision": 0, "batch_id": "x",
                            "delete_nodes": ["ghost"]})
            with self.assertRaises(ValueError):
                repo.apply({"base_revision": 0, "batch_id": "x",
                            "upsert_links": [{"id": "L", "from": "a", "to": "b",
                                              "capacity": -1}]})
            with self.assertRaises(RepoError):
                repo.apply({"base_revision": 0, "batch_id": "x",
                            "upsert_nodes": ["a", "a"]})
            with self.assertRaises(RepoError):
                repo.apply({"base_revision": 0, "batch_id": "x", "bogus": []})

    def test_same_id_delete_and_upsert_in_one_batch_atomically_replaces(self):
        with Repository(self.db) as repo:
            # L2 is deleted and re-created with a different capacity inside one
            # atomic batch; the intermediate (missing-L2) state is never visible.
            result = repo.apply({
                "base_revision": 0, "batch_id": "replace",
                "delete_links": ["L2"],
                "upsert_links": [{"id": "L2", "from": "pump", "to": "east",
                                  "capacity": 1}],
            })
            self.assertEqual(result["revision"], 1)
            network = repo.export(1)
            l2 = next(row for row in network["links"] if row["id"] == "L2")
            self.assertEqual(l2["capacity"], 1)
            # node replacement: delete then re-create referenced nodes is fine
            result2 = repo.apply({
                "base_revision": 1, "batch_id": "replace-node",
                "delete_nodes": ["west"],
                "upsert_nodes": ["west"],
            })
            self.assertEqual(result2["revision"], 2)

    def test_optimistic_concurrency_conflict_and_replay_precedence(self):
        request = {"base_revision": 0, "batch_id": "b1", "upsert_nodes": ["n1"]}
        with Repository(self.db) as repo:
            repo.apply(request)
            # stale base, new batch id -> conflict
            with self.assertRaises(ConflictError):
                repo.apply({"base_revision": 0, "batch_id": "b2",
                            "upsert_nodes": ["n2"]})
            # same batch_id with a different body conflicts even with a stale base
            with self.assertRaises(ConflictError):
                repo.apply({"base_revision": 0, "batch_id": "b1",
                            "upsert_nodes": ["different"]})
            # identical replay succeeds from any (even expired) base revision
            replay = repo.apply(dict(request, base_revision=999))
            self.assertTrue(replay["replayed"])
            self.assertEqual(replay["revision"], 1)
            # reordered request arrays canonicalize to the same replay key
            replay2 = repo.apply({
                "base_revision": 999, "batch_id": "b1",
                "upsert_nodes": list(reversed(request["upsert_nodes"])),
            })
            self.assertTrue(replay2["replayed"])
            self.assertEqual(repo.head()["revision"], 1)

    def test_analyze_is_bound_to_snapshot_not_head_and_supports_both_modes(self):
        with Repository(self.db) as repo:
            rev0 = repo.analyze_revision(0)
            rev0_fair = repo.analyze_revision(0, allocation="fair")
            self.assertEqual(rev0["snapshot_id"], self.rev0["snapshot_id"])
            self.assertEqual(rev0["result"], analyze(self.base))
            self.assertEqual(rev0_fair["result"], analyze(self.base, allocation="fair"))
            self.assertEqual(rev0_fair["result"]["min_service_ratio"],
                             {"numerator": 3, "denominator": 4})
            # advance the head; the revision-0 analysis remains unchanged
            repo.apply({"base_revision": 0, "batch_id": "move",
                        "upsert_links": [{"id": "L1", "from": "reservoir",
                                          "to": "pump", "capacity": 1}]})
            again = repo.analyze_revision(0)
            self.assertEqual(again["result"], rev0["result"])
            self.assertEqual(again["snapshot_id"], rev0["snapshot_id"])
            with self.assertRaises(ValueError):
                repo.analyze_revision(0, allocation="greedy")

    def test_export_round_trips_through_original_analyze_contract(self):
        with Repository(self.db) as repo:
            exported = repo.export(0)
        # The exported document is accepted unchanged by the original entry API.
        self.assertEqual(analyze(exported), analyze(self.base))


class RealProcessTests(unittest.TestCase):
    """Each case spawns independent OS processes against one SQLite file."""

    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / "repo.db")
        with Repository(self.db) as repo:
            repo.init(FIXTURE)
        ctx = mp.get_context("spawn")
        self.ctx = ctx

    def tearDown(self):
        self.td.cleanup()

    def _head(self):
        with Repository(self.db) as repo:
            return repo.head()

    def test_competing_commits_from_same_head_only_one_wins(self):
        # Rerun the race a few times: scheduling must never allow two winners.
        for round_no in range(5):
            with Repository(self.db) as repo:
                start_head = repo.head()["revision"]
            barrier = self.ctx.Barrier(2)
            slots = [self.ctx.Value("i", 0), self.ctx.Value("i", 0)]
            procs = []
            for i in range(2):
                request = {"base_revision": start_head,
                           "batch_id": "race-%d-%d" % (round_no, i),
                           "upsert_nodes": ["node-%d-%d" % (round_no, i)]}
                proc = self.ctx.Process(
                    target=worker_barrier, args=(self.db, request, barrier, slots[i]))
                procs.append(proc)
            for proc in procs:
                proc.start()
            for proc in procs:
                proc.join(20)
                self.assertFalse(proc.is_alive(), "worker hung")
            self.assertEqual(sorted(s.value for s in slots), [1, 3])
            head = self._head()
            self.assertEqual(head["revision"], start_head + 1)
            with Repository(self.db) as repo:
                self.assertEqual(len(repo.history()), start_head + 2)

    def test_kill_before_commit_leaves_head_and_history_untouched(self):
        parent_conn, child_conn = self.ctx.Pipe(duplex=False)
        request = {"base_revision": 0, "batch_id": "slow",
                   "upsert_nodes": ["slow-node"]}
        env_path = self.db
        proc = self.ctx.Process(target=_worker_slow_apply,
                                args=(env_path, request, child_conn, 2.0))
        proc.start()
        # Wait until the worker holds the open write transaction, then SIGTERM.
        self.assertTrue(parent_conn.poll(20))
        self.assertEqual(parent_conn.recv(), "started")
        time.sleep(0.3)
        proc.terminate()
        proc.join(10)
        self.assertNotIn(proc.exitcode, (0, None))
        # A fresh process/connection sees no half-written revision or head move.
        head = self._head()
        self.assertEqual(head["revision"], 0)
        self.assertEqual(head["snapshot_id"], snapshot_id(json.loads(FIXTURE.read_text())))
        with Repository(self.db) as repo:
            self.assertEqual([row["revision"] for row in repo.history()], [0])
            self.assertEqual(repo.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0], 0)
            self.assertEqual(repo.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0], 1)
        # SQLite recovers automatically and the next valid commit succeeds.
        with Repository(self.db) as repo:
            after = repo.apply({"base_revision": 0, "batch_id": "after-kill",
                                "upsert_nodes": ["recovered"]})
        self.assertEqual(after["revision"], 1)

    def test_replay_is_idempotent_across_processes(self):
        request = {"base_revision": 0, "batch_id": "shared-batch",
                   "upsert_nodes": ["shared"]}
        parent_conn, child_conn = self.ctx.Pipe(duplex=False)
        first = self.ctx.Process(target=worker_apply,
                                 args=(self.db, dict(request), child_conn))
        first.start()
        self.assertTrue(parent_conn.poll(20))
        kind, payload = parent_conn.recv()
        first.join(10)
        self.assertEqual(kind, "ok")
        first_revision = payload["revision"]
        # A different process replays the identical (canonical) request from a
        # deliberately stale base; it must receive the original revision.
        parent_conn2, child_conn2 = self.ctx.Pipe(duplex=False)
        replay_request = dict(request, base_revision=999)
        second = self.ctx.Process(target=worker_apply,
                                  args=(self.db, replay_request, child_conn2))
        second.start()
        self.assertTrue(parent_conn2.poll(20))
        kind2, payload2 = parent_conn2.recv()
        second.join(10)
        self.assertEqual(kind2, "ok")
        self.assertTrue(payload2["replayed"])
        self.assertEqual(payload2["revision"], first_revision)
        self.assertEqual(self._head()["revision"], first_revision)

    def test_reader_keeps_querying_history_while_head_advances(self):
        # Writer holds its transaction open (commit delay) while a reader on a
        # separate connection repeatedly reads an old revision and the head.
        parent_conn, child_conn = self.ctx.Pipe(duplex=False)
        request = {"base_revision": 0, "batch_id": "long",
                   "upsert_nodes": ["long-node"]}
        writer = self.ctx.Process(target=_worker_slow_apply,
                                  args=(self.db, request, child_conn, 2.5))
        writer.start()
        self.assertTrue(parent_conn.poll(20))
        self.assertEqual(parent_conn.recv(), "started")
        original = json.loads(FIXTURE.read_text())
        # One long-lived read connection opened BEFORE the writer commits: under
        # WAL its snapshot stays valid while the head moves underneath it.
        reader = Repository(self.db)
        try:
            pre_head = int(reader.conn.execute(
                "SELECT value FROM meta WHERE key='head_revision'").fetchone()["value"])
            self.assertEqual(pre_head, 0)
            seen_old = []
            deadline = time.time() + 10
            while time.time() < deadline:
                rev0 = reader.export(0)
                seen_old.append(snapshot_id(rev0) == snapshot_id(original))
                if writer.exitcode is not None:
                    break
                time.sleep(0.1)
        finally:
            reader.close()
        writer.join(15)
        self.assertEqual(writer.exitcode, 0)
        self.assertTrue(seen_old and all(seen_old))
        with Repository(self.db) as repo:
            self.assertEqual(repo.head()["revision"], 1)
            # Old revision remains readable and bound to its original snapshot.
            old = repo.analyze_revision(0)
            self.assertEqual(old["snapshot_id"], snapshot_id(original))
            self.assertEqual(old["result"]["total_served"], 7)
            new = repo.analyze_revision(1)
            self.assertNotEqual(new["snapshot_id"], old["snapshot_id"])

    def test_sqlite_file_is_standard_database_with_integrity_invariant(self):
        with Repository(self.db) as repo:
            repo.apply({"base_revision": 0, "batch_id": "one",
                        "upsert_nodes": ["x"]})
        raw = sqlite3.connect(self.db)
        try:
            self.assertEqual(raw.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            rows = raw.execute(
                "SELECT r.revision, r.parent_revision, r.snapshot_id, s.content_json"
                " FROM revisions r JOIN snapshots s ON s.snapshot_id=r.snapshot_id"
                " ORDER BY r.revision").fetchall()
        finally:
            raw.close()
        for revision, _parent, snap_id, content in rows:
            self.assertEqual(snapshot_id(json.loads(content)), snap_id)


def _worker_slow_apply(db_path, request, conn, delay_seconds):
    """Apply with SUPPLY_FLOW_TEST_COMMIT_DELAY set; signal once txn is open."""
    os.environ[COMMIT_DELAY_ENV] = str(delay_seconds)
    try:
        conn.send("started")
        with Repository(db_path) as repo:
            repo.apply(request)
        conn.send(("ok", None))
    except BaseException as error:  # pragma: no cover - surfaced by parent
        conn.send(("error", repr(error)))
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
