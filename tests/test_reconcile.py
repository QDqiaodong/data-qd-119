"""Transport handover reconciliation tests.

In-process tests cover the matching semantics, CSV contract, provenance and
atomic publication.  ``RealCLITests`` invokes the actual command-line entry
point in a fresh process; ``MemoryBoundTests`` measures the peak resident set
of that process on a million-row backlog.  Only the standard library is used.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from supply_flow.reconcile import (
    MAX_QUANTITY,
    ReconcileError,
    format_utc_time,
    parse_utc_time,
    reconcile,
)
from supply_flow.repo import Repository, init_repository

ROOT = Path(__file__).resolve().parents[1]
NETWORK = ROOT / "fixtures" / "reconcile_network.json"
SHIPMENTS = ROOT / "fixtures" / "reconcile_shipments.csv"
RECEIPTS = ROOT / "fixtures" / "reconcile_receipts.csv"

SHIPMENT_HEADER = "id,link_id,time,quantity\n"
RECEIPT_HEADER = "id,link_id,time,quantity\n"


def write_csv(path: Path, header: str, rows) -> Path:
    path.write_text(header + "".join(row + "\n" for row in rows), encoding="utf-8")
    return path


def rows_of(database: Path, sql: str):
    connection = sqlite3.connect(str(database))
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


class ReconcileCase(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.dir = Path(self._temp.name)
        self.db = self.dir / "repo.db"
        init_repository(self.db, NETWORK)
        self.out = self.dir / "reconcile.db"

    def tearDown(self):
        self._temp.cleanup()

    def shipments(self, rows, name="s.csv"):
        return write_csv(self.dir / name, SHIPMENT_HEADER, rows)

    def receipts(self, rows, name="r.csv"):
        return write_csv(self.dir / name, RECEIPT_HEADER, rows)


class TimeParsingTests(unittest.TestCase):
    def test_offset_instants_normalize_to_the_same_utc(self):
        instants = [
            "2024-03-01T00:00:00Z",
            "2024-03-01T08:00:00+08:00",
            "2024-02-29T19:00:00-05:00",
            "2024-03-01T00:00:00.000000000+00:00",
        ]
        normalized = {parse_utc_time(text) for text in instants}
        self.assertEqual(normalized, {(1709251200, 0, "2024-03-01T00:00:00Z")})

    def test_fractional_seconds_are_kept_at_nanosecond_resolution(self):
        self.assertEqual(parse_utc_time("2024-03-01T00:00:00.5Z")[1], 500_000_000)
        self.assertEqual(
            format_utc_time(*parse_utc_time("2024-03-01T00:00:00.5Z")[:2]),
            "2024-03-01T00:00:00.5Z",
        )
        self.assertEqual(
            parse_utc_time("2024-03-01T00:00:00.000000001Z")[1], 1
        )

    def test_missing_timezone_or_bad_calendar_value_is_rejected(self):
        for text in [
            "2024-03-01T00:00:00",
            "2024-03-01 00:00:00Z",
            "2024-02-30T00:00:00Z",
            "2024-03-01T24:00:00Z",
            "2024-03-01T00:00:00+24:00",
            "not-a-time",
        ]:
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    parse_utc_time(text)


class MatchingSemanticsTests(ReconcileCase):
    def test_same_instant_shipment_is_consumed_before_receipt(self):
        # The shipment id ("S9") sorts after the receipt id ("R1"); a naive
        # single (time, id) merge would place the receipt first.  The contract
        # is "at one instant, shipments first", so the match must still happen.
        shipments = self.shipments(["S9,L1,2024-03-01T00:00:00Z,4"])
        receipts = self.receipts(["R1,L1,2024-03-01T00:00:00Z,4"])
        reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(
            rows_of(self.out, "SELECT receipt_id,shipment_id,quantity FROM matches"),
            [("R1", "S9", 4)],
        )
        self.assertEqual(rows_of(self.out, "SELECT COUNT(*) FROM orphan_receipts"), [(0,)])

    def test_fifo_order_is_time_then_id_not_file_order(self):
        # Input is deliberately shuffled; ids decide the tie at one instant.
        shipments = self.shipments(
            [
                "S3,L1,2024-03-01T00:00:00Z,3",
                "S1,L1,2024-03-01T00:00:00Z,3",
                "S2,L1,2024-03-01T00:00:00Z,3",
            ]
        )
        receipts = self.receipts(
            [
                "R2,L1,2024-03-01T00:00:01Z,4",
                "R1,L1,2024-03-01T00:00:01Z,4",
            ]
        )
        reconcile(self.db, 0, shipments, receipts, self.out)
        matches = rows_of(
            self.out, "SELECT receipt_id,shipment_id,quantity FROM matches"
            " ORDER BY receipt_id,shipment_id"
        )
        self.assertEqual(
            matches,
            [
                ("R1", "S1", 3),
                ("R1", "S2", 1),
                ("R2", "S2", 2),
                ("R2", "S3", 2),
            ],
        )

    def test_partial_receipt_splits_across_shipments(self):
        shipments = self.shipments(
            ["S1,L1,2024-03-01T00:00:00Z,10", "S2,L1,2024-03-01T01:00:00Z,10"]
        )
        receipts = self.receipts(["R1,L1,2024-03-01T02:00:00Z,15"])
        reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(
            rows_of(self.out, "SELECT shipment_id,quantity FROM matches"
                     " ORDER BY shipment_id"),
            [("S1", 10), ("S2", 5)],
        )
        self.assertEqual(
            rows_of(self.out, "SELECT shipment_id,quantity FROM unmatched_shipments"),
            [("S2", 5)],
        )

    def test_receipt_before_any_shipment_is_orphaned_and_never_borrows_later_goods(self):
        shipments = self.shipments(["S1,L1,2024-03-01T10:00:00Z,5"])
        receipts = self.receipts(
            ["R1,L1,2024-03-01T09:00:00Z,3", "R2,L1,2024-03-01T11:00:00Z,5"]
        )
        reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(
            rows_of(self.out, "SELECT receipt_id,quantity,reason FROM orphan_receipts"),
            [("R1", 3, "early_arrival")],
        )
        self.assertEqual(
            rows_of(self.out, "SELECT receipt_id,shipment_id,quantity FROM matches"),
            [("R2", "S1", 5)],
        )

    def test_over_receipt_is_orphaned_and_split_receipt_keeps_matched_part(self):
        shipments = self.shipments(["S1,L1,2024-03-01T00:00:00Z,4"])
        receipts = self.receipts(["R1,L1,2024-03-01T01:00:00Z,7"])
        summary = reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(summary["matched_quantity"], 4)
        self.assertEqual(summary["orphan_quantity"], 3)
        self.assertEqual(
            rows_of(self.out, "SELECT receipt_id,quantity,reason FROM orphan_receipts"),
            [("R1", 3, "over_receipt")],
        )

    def test_cross_timezone_same_instant_and_split_receipt(self):
        # S1 at 00:00Z and S2 at 08:00+08:00 are the same instant and, by id,
        # S1 sorts first.  The receipt at 19:00-05:00 is 00:00Z as well.
        shipments = self.shipments(
            [
                "S2,L1,2024-03-01T08:00:00+08:00,6",
                "S1,L1,2024-03-01T00:00:00Z,6",
            ]
        )
        receipts = self.receipts(["R1,L1,2024-02-29T19:00:00-05:00,9"])
        reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(
            rows_of(self.out, "SELECT shipment_id,quantity FROM matches"
                     " ORDER BY shipment_id"),
            [("S1", 6), ("S2", 3)],
        )
        self.assertEqual(
            rows_of(self.out, "SELECT shipment_time_utc FROM matches"
                     " WHERE shipment_id='S2'"),
            [("2024-03-01T00:00:00Z",)],
        )

    def test_links_are_independent_and_endpoints_come_from_the_revision(self):
        shipments = self.shipments(
            ["S1,L1,2024-03-01T00:00:00Z,5", "S2,L2,2024-03-01T00:00:00Z,5"]
        )
        receipts = self.receipts(
            ["R1,L1,2024-03-01T01:00:00Z,5", "R2,L2,2024-03-01T00:00:00Z,2"]
        )
        reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(
            rows_of(self.out, "SELECT link_id,from_node,to_node FROM links"
                     " ORDER BY link_id"),
            [("L1", "depot", "hub"), ("L2", "hub", "port"), ("L3", "port", "yard")],
        )
        self.assertEqual(
            rows_of(self.out, "SELECT link_id,shipment_id,quantity FROM matches"
                     " ORDER BY link_id"),
            [("L1", "S1", 5), ("L2", "S2", 2)],
        )

    def test_input_row_order_does_not_change_the_result(self):
        shipment_rows = [
            "S1,L1,2024-03-01T00:00:00Z,7",
            "S2,L1,2024-03-01T00:00:00Z,3",
            "S3,L2,2024-03-01T02:00:00Z,4",
        ]
        receipt_rows = [
            "R1,L1,2024-03-01T01:00:00Z,9",
            "R2,L1,2024-03-01T03:00:00Z,4",
            "R3,L2,2024-03-01T01:00:00Z,2",
        ]
        outputs = []
        for index, (shipment_order, receipt_order) in enumerate(
            [
                (shipment_rows, receipt_rows),
                (list(reversed(shipment_rows)), receipt_rows[::-1]),
                (shipment_rows[1:] + shipment_rows[:1], [receipt_rows[1], receipt_rows[2], receipt_rows[0]]),
            ]
        ):
            shipments = self.shipments(shipment_order, "s%d.csv" % index)
            receipts = self.receipts(receipt_order, "r%d.csv" % index)
            out = self.dir / ("out%d.db" % index)
            reconcile(self.db, 0, shipments, receipts, out)
            outputs.append(
                rows_of(out, "SELECT * FROM matches ORDER BY 1,2,3")
                + rows_of(out, "SELECT * FROM orphan_receipts ORDER BY 1,2")
                + rows_of(out, "SELECT * FROM unmatched_shipments ORDER BY 1,2")
            )
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(outputs[0], outputs[2])

    def test_million_unit_backlog_without_unit_walks(self):
        # Quantities near the 10^9 cap with few rows: memory and runtime must
        # not scale with units.  A 10^9 shipment and a 10^9 receipt become one
        # match row, not 10^9 rows.
        big = str(MAX_QUANTITY)
        shipments = self.shipments(["S1,L1,2024-03-01T00:00:00Z," + big])
        receipts = self.receipts(["R1,L1,2024-03-01T01:00:00Z," + big])
        started = time.monotonic()
        summary = reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertLess(time.monotonic() - started, 5.0)
        self.assertEqual(summary["matched_quantity"], MAX_QUANTITY)
        self.assertEqual(rows_of(self.out, "SELECT COUNT(*) FROM matches"), [(1,)])


class CsvContractTests(ReconcileCase):
    def run_with(self, shipments_path, receipts_path):
        return reconcile(self.db, 0, shipments_path, receipts_path, self.out)

    def test_identical_duplicate_is_counted_once(self):
        shipments = self.shipments(
            [
                "S1,L1,2024-03-01T00:00:00Z,5",
                "S1,L1,2024-03-01T00:00:00Z,5",
                "S1,L1,2024-03-01T08:00:00+08:00,5",
            ]
        )
        receipts = self.receipts(["R1,L1,2024-03-01T01:00:00Z,5"])
        summary = self.run_with(shipments, receipts)
        self.assertEqual(summary["shipments_rows"], 3)
        self.assertEqual(summary["shipments_unique"], 1)
        self.assertEqual(summary["shipments_duplicates"], 2)
        self.assertEqual(summary["shipment_quantity"], 5)
        self.assertEqual(rows_of(self.out, "SELECT COUNT(*) FROM matches"), [(1,)])

    def test_conflicting_duplicate_reports_file_and_line(self):
        shipments = self.shipments(
            [
                "S1,L1,2024-03-01T00:00:00Z,5",
                "S2,L1,2024-03-01T00:00:00Z,6",
                "S1,L1,2024-03-01T00:00:00Z,9",
            ]
        )
        receipts = self.receipts([])
        with self.assertRaises(ReconcileError) as caught:
            self.run_with(shipments, receipts)
        message = str(caught.exception)
        self.assertIn(str(shipments) + ":4", message)
        self.assertIn("S1", message)
        self.assertIn(str(shipments) + ":2", message)
        self.assertFalse(self.out.exists())

    def test_conflicting_duplicate_in_receipts_is_rejected(self):
        shipments = self.shipments(["S1,L1,2024-03-01T00:00:00Z,5"])
        receipts = self.receipts(
            [
                "R1,L1,2024-03-01T01:00:00Z,2",
                "R1,L1,2024-03-01T01:00:00+00:00,2",
                "R2,L1,2024-03-01T02:00:00Z,2",
            ]
        )
        # The first two rows are the same instant and the same content: deduped.
        summary = self.run_with(shipments, receipts)
        self.assertEqual(summary["receipts_unique"], 2)
        self.assertEqual(summary["receipts_duplicates"], 1)
        receipts.write_text(
            RECEIPT_HEADER + "R1,L1,2024-03-01T01:00:00Z,2\nR1,L1,2024-03-01T02:00:00Z,2\n"
        )
        with self.assertRaises(ReconcileError) as caught:
            self.run_with(shipments, receipts)
        self.assertIn(str(receipts) + ":3", str(caught.exception))
        self.assertIn(str(receipts) + ":2", str(caught.exception))

    def test_header_columns_and_value_bounds_are_enforced(self):
        receipts = self.receipts([])
        cases = {
            "bad-header": ("id,link,time,quantity\nS1,L1,2024-03-01T00:00:00Z,1\n", True),
            "no-header": ("", True),
            "short-row": (SHIPMENT_HEADER + "S1,L1,2024-03-01T00:00:00Z\n", True),
            "zero-qty": (SHIPMENT_HEADER + "S1,L1,2024-03-01T00:00:00Z,0\n", True),
            "negative-qty": (SHIPMENT_HEADER + "S1,L1,2024-03-01T00:00:00Z,-1\n", True),
            "fraction-qty": (SHIPMENT_HEADER + "S1,L1,2024-03-01T00:00:00Z,1.5\n", True),
            "too-big-qty": (SHIPMENT_HEADER + "S1,L1,2024-03-01T00:00:00Z,1000000001\n", True),
            "naive-time": (SHIPMENT_HEADER + "S1,L1,2024-03-01T00:00:00,1\n", True),
            "unknown-link": (SHIPMENT_HEADER + "S1,L9,2024-03-01T00:00:00Z,1\n", True),
            "blank-id": (SHIPMENT_HEADER + ",L1,2024-03-01T00:00:00Z,1\n", True),
            "valid-max": (
                SHIPMENT_HEADER + "S1,L1,2024-03-01T00:00:00Z,%d\n" % MAX_QUANTITY,
                False,
            ),
        }
        for name, (content, should_fail) in cases.items():
            with self.subTest(case=name):
                path = self.dir / (name + ".csv")
                path.write_text(content, encoding="utf-8")
                if should_fail:
                    with self.assertRaises(ReconcileError):
                        reconcile(self.db, 0, path, receipts, self.out)
                    self.assertFalse(self.out.exists())
                else:
                    reconcile(self.db, 0, path, receipts, self.out)
                    self.assertTrue(self.out.exists())
                    self.out.unlink()

    def test_shipment_and_receipt_ids_are_separate_namespaces(self):
        shipments = self.shipments(["X1,L1,2024-03-01T00:00:00Z,3"])
        receipts = self.receipts(["X1,L1,2024-03-01T01:00:00Z,3"])
        summary = reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(summary["shipments_unique"], 1)
        self.assertEqual(summary["receipts_unique"], 1)
        self.assertEqual(
            rows_of(self.out, "SELECT receipt_id,shipment_id,quantity FROM matches"),
            [("X1", "X1", 3)],
        )

    def test_utf8_bom_is_accepted(self):
        shipments = self.dir / "bom.csv"
        shipments.write_bytes(
            b"\xef\xbb\xbf" + SHIPMENT_HEADER.encode() + b"S1,L1,2024-03-01T00:00:00Z,5\n"
        )
        receipts = self.receipts(["R1,L1,2024-03-01T01:00:00Z,5"])
        summary = reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(summary["shipments_unique"], 1)


class ProvenanceAndSummaryTests(ReconcileCase):
    def test_revision_snapshot_and_source_hashes_are_bound(self):
        from supply_flow.repo import apply_batch_file, snapshot_id

        apply_batch_file(
            self.db,
            {
                "base_revision": 0,
                "batch_id": "add-node",
                "upsert_nodes": ["depot"],
                "upsert_links": [{"id": "L4", "from": "hub", "to": "yard", "capacity": 5}],
            },
        )
        shipments = self.shipments(["S1,L4,2024-03-01T00:00:00Z,2"])
        receipts = self.receipts(["R1,L4,2024-03-01T01:00:00Z,2"])
        # Revision 1 knows L4; revision 0 does not.
        with self.assertRaises(ReconcileError):
            reconcile(self.db, 0, shipments, receipts, self.out)
        summary = reconcile(self.db, 1, shipments, receipts, self.out)
        head_snapshot = Repository(self.db).head()["snapshot_id"]
        self.assertEqual(summary["revision"], 1)
        self.assertEqual(summary["snapshot_id"], head_snapshot)
        self.assertEqual(
            summary["shipments_sha256"],
            __import__("hashlib").sha256(shipments.read_bytes()).hexdigest(),
        )
        self.assertEqual(
            summary["receipts_sha256"],
            __import__("hashlib").sha256(receipts.read_bytes()).hexdigest(),
        )
        row = rows_of(self.out, "SELECT revision,snapshot_id,shipments_sha256,"
                                "receipts_sha256 FROM reconciliation")
        self.assertEqual(
            row, [(1, head_snapshot, summary["shipments_sha256"], summary["receipts_sha256"])]
        )

    def test_unknown_revision_and_missing_source_do_not_publish(self):
        shipments = self.shipments(["S1,L1,2024-03-01T00:00:00Z,1"])
        receipts = self.receipts([])
        with self.assertRaises(ReconcileError):
            reconcile(self.db, 7, shipments, receipts, self.out)
        with self.assertRaises(ReconcileError):
            reconcile(self.db, 0, self.dir / "absent.csv", receipts, self.out)
        with self.assertRaises(ReconcileError):
            reconcile(self.dir / "absent.db", 0, shipments, receipts, self.out)
        self.assertFalse(self.out.exists())

    def test_repository_with_live_wal_writer_is_still_readable(self):
        # A concurrent writer holding a WAL sidecar must not distract the
        # read-only reader; committed revisions remain visible.
        writer = Repository(self.db)
        try:
            writer.conn.execute("PRAGMA journal_mode=WAL")
            writer.conn.execute("BEGIN IMMEDIATE")
            writer.conn.execute(
                "INSERT INTO meta(key,value) VALUES('scratch','pending')"
            )
            shipments = self.shipments(["S1,L1,2024-03-01T00:00:00Z,5"])
            receipts = self.receipts(["R1,L1,2024-03-01T01:00:00Z,5"])
            summary = reconcile(self.db, 0, shipments, receipts, self.out)
            self.assertEqual(summary["matched_quantity"], 5)
        finally:
            writer.conn.rollback()
            writer.close()

    def test_node_summary_balances_each_endpoint(self):
        shipments = self.shipments(
            ["S1,L1,2024-03-01T00:00:00Z,4", "S2,L2,2024-03-01T00:00:00Z,6"]
        )
        receipts = self.receipts(
            ["R1,L1,2024-03-01T01:00:00Z,3", "R2,L2,2024-03-01T00:00:00Z,9"]
        )
        reconcile(self.db, 0, shipments, receipts, self.out)
        summary = dict(
            (row[0], row[1:])
            for row in rows_of(
                self.out,
                "SELECT node,sent_quantity,received_quantity,matched_receipt_quantity,"
                "orphan_receipt_quantity,unmatched_shipment_quantity FROM node_summary",
            )
        )
        self.assertEqual(summary["depot"], (4, 0, 0, 0, 1))
        self.assertEqual(summary["hub"], (6, 3, 3, 0, 0))
        self.assertEqual(summary["port"], (0, 9, 6, 3, 0))
        self.assertEqual(summary["yard"], (0, 0, 0, 0, 0))

    def test_output_is_standalone_and_self_consistent(self):
        summary = reconcile(self.db, 0, SHIPMENTS, RECEIPTS, self.out)
        connection = sqlite3.connect(str(self.out))
        try:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )
            totals = connection.execute(
                "SELECT shipments_rows, shipments_unique, receipts_rows,"
                " receipts_unique, matched_quantity, orphan_quantity,"
                " unmatched_quantity FROM reconciliation"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(totals[0], totals[1])
        self.assertEqual(totals[2], totals[3])
        self.assertEqual(totals[4] + totals[5], summary["receipt_quantity"])
        self.assertEqual(totals[4] + totals[6], summary["shipment_quantity"])
        self.assertNotEqual(self.out.resolve(), self.db.resolve())


class PublicationTests(ReconcileCase):
    def test_failure_leaves_previous_result_untouched(self):
        shipments = self.shipments(["S1,L1,2024-03-01T00:00:00Z,5"])
        receipts = self.receipts(["R1,L1,2024-03-01T01:00:00Z,5"])
        reconcile(self.db, 0, shipments, receipts, self.out)
        before = self.out.read_bytes()
        broken = self.receipts(
            ["R1,L1,2024-03-01T01:00:00Z,5", "R1,L1,2024-03-01T01:00:00Z,6"], "broken.csv"
        )
        with self.assertRaises(ReconcileError):
            reconcile(self.db, 0, shipments, broken, self.out)
        self.assertEqual(self.out.read_bytes(), before)
        leftovers = [p.name for p in self.dir.iterdir() if p.name.startswith(".")]
        self.assertEqual(leftovers, [])

    def test_output_cannot_be_a_symlink_or_repository(self):
        shipments = self.shipments([])
        receipts = self.receipts([])
        link = self.dir / "link.db"
        link.symlink_to(self.out)
        with self.assertRaises(ReconcileError):
            reconcile(self.db, 0, shipments, receipts, link)
        with self.assertRaises(ReconcileError):
            reconcile(self.db, 0, shipments, receipts, self.db)
        with self.assertRaises(ReconcileError):
            reconcile(self.db, 0, shipments, receipts, shipments)
        self.assertFalse(self.out.exists())

    def test_repository_is_not_modified(self):
        shipments = self.shipments(["S1,L1,2024-03-01T00:00:00Z,5"])
        receipts = self.receipts(["R1,L1,2024-03-01T01:00:00Z,5"])
        before = self.db.read_bytes()
        mtime = self.db.stat().st_mtime_ns
        reconcile(self.db, 0, shipments, receipts, self.out)
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(self.db.stat().st_mtime_ns, mtime)
        # No WAL sidecar was created by the read-only connection.
        self.assertFalse(Path(str(self.db) + "-wal").exists())


class RealCLITests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.dir = Path(self._temp.name)
        self.db = self.dir / "repo.db"
        init_repository(self.db, NETWORK)

    def tearDown(self):
        self._temp.cleanup()

    def run_cli(self, *arguments):
        return subprocess.run(
            [sys.executable, "-m", "supply_flow.reconcile_cli", *map(str, arguments)],
            capture_output=True,
            text=True,
        )

    def test_cli_handles_cross_timezone_split_and_early_arrival(self):
        shipments = write_csv(
            self.dir / "s.csv",
            SHIPMENT_HEADER,
            [
                "S2,L1,2024-03-01T08:00:00+08:00,6",
                "S1,L1,2024-03-01T00:00:00Z,6",
                "S3,L2,2024-03-01T00:10:00+02:00,3",
            ],
        )
        receipts = write_csv(
            self.dir / "r.csv",
            RECEIPT_HEADER,
            [
                "R1,L1,2024-02-29T19:00:00-05:00,9",
                "R2,L2,2024-02-29T20:00:00Z,1",
                "R3,L2,2024-03-01T00:10:00+02:00,3",
            ],
        )
        out = self.dir / "out.db"
        result = self.run_cli(
            "--db", self.db, "--revision", 0,
            "--shipments", shipments, "--receipts", receipts, "--output", out,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        # Same instant sorted by id: S1 then S2 for R1; L2's early receipt at
        # 20:00Z precedes S3 at 22:10Z, so it is orphaned and cannot borrow it.
        self.assertEqual(
            rows_of(out, "SELECT receipt_id,shipment_id,quantity FROM matches"
                     " ORDER BY 1,2"),
            [("R1", "S1", 6), ("R1", "S2", 3), ("R3", "S3", 3)],
        )
        self.assertEqual(
            rows_of(out, "SELECT receipt_id,quantity,reason FROM orphan_receipts"
                     " ORDER BY 1"),
            [("R2", 1, "early_arrival")],
        )
        self.assertEqual(
            rows_of(out, "SELECT shipment_id,quantity FROM unmatched_shipments"),
            [("S2", 3)],
        )
        # UTC normalization is visible in the published detail rows.
        self.assertEqual(
            rows_of(out, "SELECT DISTINCT shipment_time_utc FROM matches"
                     " WHERE link_id='L1'"),
            [("2024-03-01T00:00:00Z",)],
        )

    def test_cli_rejects_naive_time_and_unknown_link_without_publishing(self):
        out = self.dir / "out.db"
        naive = write_csv(
            self.dir / "s.csv", SHIPMENT_HEADER, ["S1,L1,2024-03-01T00:00:00,1"]
        )
        empty = write_csv(self.dir / "r.csv", RECEIPT_HEADER, [])
        result = self.run_cli(
            "--db", self.db, "--revision", 0,
            "--shipments", naive, "--receipts", empty, "--output", out,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("s.csv:2", result.stderr)
        self.assertFalse(out.exists())
        unknown = write_csv(
            self.dir / "s2.csv", SHIPMENT_HEADER, ["S1,L9,2024-03-01T00:00:00Z,1"]
        )
        result = self.run_cli(
            "--db", self.db, "--revision", 0,
            "--shipments", unknown, "--receipts", empty, "--output", out,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("L9", result.stderr)
        self.assertFalse(out.exists())

    def test_cli_publishes_summary_database_and_reports_conflicts(self):
        out = self.dir / "out.db"
        result = self.run_cli(
            "--db", self.db, "--revision", 0,
            "--shipments", SHIPMENTS, "--receipts", RECEIPTS, "--output", out,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["matched_quantity"], 17)
        self.assertEqual(payload["orphan_quantity"], 3)
        self.assertTrue(out.is_file())
        self.assertEqual(
            rows_of(out, "SELECT COUNT(*) FROM orphan_receipts"), [(2,)]
        )

        # A conflicting duplicate fails with exit 2 and keeps the old result.
        previous = out.read_bytes()
        bad = self.dir / "bad.csv"
        bad.write_text(
            SHIPMENT_HEADER
            + "S1,L1,2024-03-01T00:00:00Z,5\nS1,L1,2024-03-01T00:00:00Z,6\n"
        )
        result = self.run_cli(
            "--db", self.db, "--revision", 0,
            "--shipments", bad, "--receipts", RECEIPTS, "--output", out,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("bad.csv:3", result.stderr)
        self.assertEqual(out.read_bytes(), previous)

        # Unknown revision also fails with exit 2 and publishes nothing new.
        new_out = self.dir / "absent.db"
        result = self.run_cli(
            "--db", self.db, "--revision", 9,
            "--shipments", SHIPMENTS, "--receipts", RECEIPTS, "--output", new_out,
        )
        self.assertEqual(result.returncode, 2)
        self.assertFalse(new_out.exists())

    def test_cli_is_order_and_repeat_independent(self):
        shuffled_shipments = self.dir / "s.csv"
        shuffled_receipts = self.dir / "r.csv"
        write_csv(
            shuffled_shipments,
            SHIPMENT_HEADER,
            list(reversed(SHIPMENTS.read_text().splitlines()[1:])),
        )
        write_csv(
            shuffled_receipts,
            RECEIPT_HEADER,
            list(reversed(RECEIPTS.read_text().splitlines()[1:])),
        )
        first = self.dir / "a.db"
        second = self.dir / "b.db"
        for source, receipts, out in (
            (SHIPMENTS, RECEIPTS, first),
            (shuffled_shipments, shuffled_receipts, second),
        ):
            result = self.run_cli(
                "--db", self.db, "--revision", 0,
                "--shipments", source, "--receipts", receipts, "--output", out,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            rows_of(first, "SELECT * FROM matches ORDER BY 1,2,3"),
            rows_of(second, "SELECT * FROM matches ORDER BY 1,2,3"),
        )
        # Re-running against an existing output replaces it atomically with the
        # same logical result (no accumulation across runs).
        first_again = self.dir / "c.db"
        result = self.run_cli(
            "--db", self.db, "--revision", 0,
            "--shipments", SHIPMENTS, "--receipts", RECEIPTS, "--output", first_again,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            rows_of(first_again, "SELECT * FROM matches ORDER BY 1,2,3"),
            rows_of(second, "SELECT * FROM matches ORDER BY 1,2,3"),
        )
        # Overwriting an existing output keeps it a single complete database.
        result = self.run_cli(
            "--db", self.db, "--revision", 0,
            "--shipments", SHIPMENTS, "--receipts", RECEIPTS, "--output", first,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            rows_of(first, "SELECT * FROM reconciliation"),
            rows_of(first_again, "SELECT * FROM reconciliation"),
        )


@unittest.skipUnless(
    os.environ.get("SUPPLY_FLOW_MEMORY_TEST", "1") == "1",
    "memory measurement disabled",
)
class MemoryBoundTests(unittest.TestCase):
    """Real CLI process over a 1,000,000-row backlog per source file."""

    ROWS = 1_000_000
    LINK_COUNT = 8
    LIMIT_MIB = 64

    @classmethod
    def setUpClass(cls):
        cls._temp = tempfile.TemporaryDirectory()
        cls.dir = Path(cls._temp.name)
        cls.db = cls.dir / "repo.db"
        network = {
            "nodes": ["n%d" % index for index in range(cls.LINK_COUNT + 1)],
            "links": [
                {
                    "id": "L%d" % index,
                    "from": "n%d" % index,
                    "to": "n%d" % (index + 1),
                    "capacity": 10**9,
                }
                for index in range(cls.LINK_COUNT)
            ],
            "supplies": [],
            "demands": [],
        }
        network_path = cls.dir / "network.json"
        network_path.write_text(json.dumps(network), encoding="utf-8")
        init_repository(cls.db, network_path)
        # Shipment i on link i%N; every receipt arrives a day later, so each link
        # carries a large pending backlog (all shipments) before any receipt is
        # merged.  Varying quantities force partial shipment splits.
        cls.shipments = cls.dir / "s.csv"
        cls.receipts = cls.dir / "r.csv"
        with cls.shipments.open("w", encoding="utf-8") as stream:
            stream.write(SHIPMENT_HEADER)
            for index in range(cls.ROWS):
                stream.write(
                    "S%07d,L%d,2024-03-01T00:00:00Z,%d\n"
                    % (index, index % cls.LINK_COUNT, 1 + index % 5)
                )
        # Receipts are interleaved by id, which is an arbitrary input order.
        with cls.receipts.open("w", encoding="utf-8") as stream:
            stream.write(RECEIPT_HEADER)
            for index in range(cls.ROWS):
                stream.write(
                    "R%07d,L%d,2024-03-02T00:00:00Z,%d\n"
                    % (index, index % cls.LINK_COUNT, 1 + index % 7)
                )

    @classmethod
    def tearDownClass(cls):
        cls._temp.cleanup()

    def test_peak_rss_stays_under_64_mib_for_two_million_rows(self):
        out = self.dir / "memory.db"
        # A tiny outer process runs the real CLI as its only child and reports
        # that child's peak RSS, keeping the measurement isolated from other
        # tests that may also have spawned processes.
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
                sys.executable, "-m", "supply_flow.reconcile_cli",
                "--db", str(self.db), "--revision", "0",
                "--shipments", str(self.shipments),
                "--receipts", str(self.receipts),
                "--output", str(out),
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        peak_line = [
            line for line in result.stderr.splitlines() if line.startswith("PEAK_RSS_KIB=")
        ]
        self.assertEqual(len(peak_line), 1, result.stderr)
        peak_mib = int(peak_line[0].split("=", 1)[1]) / 1024.0
        self.assertLess(
            peak_mib,
            self.LIMIT_MIB,
            "peak RSS %.1f MiB exceeded %d MiB" % (peak_mib, self.LIMIT_MIB),
        )
        summary = json.loads(result.stdout)
        self.assertEqual(summary["shipments_unique"], self.ROWS)
        self.assertEqual(summary["receipts_unique"], self.ROWS)
        self.assertEqual(summary["matched_quantity"] + summary["orphan_quantity"],
                         summary["receipt_quantity"])
        self.assertGreater(summary["matched_quantity"], 0)
        connection = sqlite3.connect(str(out))
        try:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
