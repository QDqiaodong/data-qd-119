"""Subprocess tests for the supply-flow-repo CLI and legacy JSON entry."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from supply_flow.model import analyze

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures" / "input.json"


class RepoCLITests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.p = Path(self.td.name)
        self.db = self.p / "repo.db"

    def tearDown(self):
        self.td.cleanup()

    def _run(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "supply_flow.repo_cli", *map(str, args)],
            capture_output=True, text=True)

    def test_full_cli_lifecycle_and_legacy_entry_on_export(self):
        result = self._run("init", "--db", self.db, "--source", FIXTURE)
        self.assertEqual(result.returncode, 0, result.stderr)
        init_payload = json.loads(result.stdout)
        self.assertEqual(init_payload["revision"], 0)

        request = {
            "base_revision": 0, "batch_id": "batch-1",
            "upsert_nodes": ["north"],
            "upsert_links": [{"id": "LN", "from": "pump", "to": "north", "capacity": 3}],
            "upsert_demands": [{"node": "north", "amount": 3}],
        }
        req_path = self.p / "request.json"
        req_path.write_text(json.dumps(request))
        result = self._run("apply", "--db", self.db, "--request", req_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(json.loads(result.stdout)["replayed"])

        # head and history
        head = json.loads(self._run("head", "--db", self.db).stdout)
        self.assertEqual(head["revision"], 1)
        history = json.loads(self._run("history", "--db", self.db).stdout)
        self.assertEqual([row["revision"] for row in history], [0, 1])
        self.assertEqual(history[1]["parent_revision"], 0)

        # export revision 0 and feed it through the ORIGINAL supply-flow CLI
        exported = self.p / "rev0.json"
        result = self._run("export", "--db", self.db, "--revision", 0,
                           "--output", exported)
        self.assertEqual(result.returncode, 0, result.stderr)
        network = json.loads(exported.read_text())
        self.assertEqual(network, json.loads(FIXTURE.read_text()))
        report = self.p / "report.json"
        legacy = subprocess.run(
            [sys.executable, "-m", "supply_flow.cli", "--source", str(exported),
             "--output", str(report)], capture_output=True, text=True)
        self.assertEqual(legacy.returncode, 0, legacy.stderr)
        self.assertEqual(json.loads(report.read_text())["result"], analyze(network))

        # analyze subcommand with both allocation modes, bound to the snapshot
        plain = json.loads(self._run("analyze", "--db", self.db,
                                     "--revision", 0).stdout)
        fair = json.loads(self._run("analyze", "--db", self.db, "--revision", 0,
                                    "--allocation", "fair").stdout)
        self.assertEqual(plain["snapshot_id"], init_payload["snapshot_id"])
        self.assertEqual(plain["result"]["total_served"], 7)
        self.assertEqual(fair["result"]["allocation_mode"], "fair")
        self.assertEqual(fair["result"]["min_service_ratio"],
                         {"numerator": 3, "denominator": 4})

        # analyze output can be published atomically too
        out = self.p / "analysis.json"
        result = self._run("analyze", "--db", self.db, "--revision", 1,
                           "--output", out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(out.exists())

    def test_conflict_exit_code_and_replay_from_stale_base(self):
        self._run("init", "--db", self.db, "--source", FIXTURE)
        body = {"base_revision": 0, "batch_id": "b", "upsert_nodes": ["q"]}
        req = self.p / "req.json"
        req.write_text(json.dumps(body))
        self.assertEqual(self._run("apply", "--db", self.db,
                                   "--request", req).returncode, 0)
        # optimistic concurrency conflict -> exit 3
        conflict_req = self.p / "conflict.json"
        conflict_req.write_text(json.dumps(
            {"base_revision": 0, "batch_id": "other", "upsert_nodes": ["z"]}))
        result = self._run("apply", "--db", self.db, "--request", conflict_req)
        self.assertEqual(result.returncode, 3)
        # same batch id, different body -> conflict even with stale base
        diff_req = self.p / "diff.json"
        diff_req.write_text(json.dumps(
            {"base_revision": 9, "batch_id": "b", "upsert_nodes": ["z"]}))
        result = self._run("apply", "--db", self.db, "--request", diff_req)
        self.assertEqual(result.returncode, 3)
        # identical body with stale base -> replay success, exit 0
        replay_req = self.p / "replay.json"
        replay_req.write_text(json.dumps(dict(body, base_revision=9)))
        result = self._run("apply", "--db", self.db, "--request", replay_req)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertTrue(payload["replayed"])
        self.assertEqual(payload["revision"], 1)

    def test_invalid_batch_and_uninitialized_db_fail_with_exit_2(self):
        # operations on an uninitialized database
        req = self.p / "req.json"
        req.write_text(json.dumps({"base_revision": 0, "batch_id": "b"}))
        result = self._run("apply", "--db", self.db, "--request", req)
        self.assertEqual(result.returncode, 2)
        # strict JSON rules still apply to request files (NaN rejected)
        self._run("init", "--db", self.db, "--source", FIXTURE)
        bad = self.p / "bad.json"
        bad.write_text('{"base_revision":0,"batch_id":"b","upsert_nodes":[NaN]}')
        result = self._run("apply", "--db", self.db, "--request", bad)
        self.assertEqual(result.returncode, 2)
        # a batch producing a dangling reference fails and leaves head at 0
        dangling = self.p / "dangling.json"
        dangling.write_text(json.dumps(
            {"base_revision": 0, "batch_id": "d", "delete_nodes": ["pump"]}))
        result = self._run("apply", "--db", self.db, "--request", dangling)
        self.assertEqual(result.returncode, 2)
        head = json.loads(self._run("head", "--db", self.db).stdout)
        self.assertEqual(head["revision"], 0)
        # unknown revision on export/analyze
        self.assertEqual(self._run("export", "--db", self.db,
                                   "--revision", 99).returncode, 2)
        self.assertEqual(self._run("analyze", "--db", self.db,
                                   "--revision", 99).returncode, 2)

    def test_failed_publication_retains_prior_report_and_rejects_symlink(self):
        self._run("init", "--db", self.db, "--source", FIXTURE)
        out = self.p / "analysis.json"
        out.write_text("prior report")
        # Failing analysis (unknown revision) must not touch the old report.
        result = self._run("analyze", "--db", self.db, "--revision", 42,
                           "--output", out)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(out.read_text(), "prior report")
        # A successful run replaces the report atomically.
        result = self._run("analyze", "--db", self.db, "--revision", 0,
                           "--output", out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotEqual(out.read_text(), "prior report")
        # Symlink outputs are refused and the link target stays untouched.
        target = self.p / "target.json"
        target.write_text("keep me")
        link = self.p / "link.json"
        link.symlink_to(target)
        result = self._run("analyze", "--db", self.db, "--revision", 0,
                           "--output", link)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(target.read_text(), "keep me")
        # Export output cannot overwrite the database itself.
        result = self._run("export", "--db", self.db, "--revision", 0,
                           "--output", self.db)
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
