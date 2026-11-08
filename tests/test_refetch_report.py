"""Tests of the "rewritten vs moved to a neighbouring date" report of
scripts/refetch_daily_revenue.py, on a tiny in-memory database.

Run: .venv\\Scripts\\python.exe -m unittest tests.test_refetch_report
"""
from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import refetch_daily_revenue as rep  # noqa: E402

FETCHED_AT = "2026-11-08T12:00:00+00:00"


def _db(ours: dict[str, float], src: dict[str, float]) -> sqlite3.Connection:
    """In-memory db with the two tables the report reads, one protocol 'p'."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE defillama_daily_revenue (protocol_slug TEXT, date TEXT, revenue_usd REAL)")
    conn.execute(
        "CREATE TABLE defillama_daily_revenue_restated "
        "(protocol_slug TEXT, date TEXT, revenue_usd REAL, fetched_at TEXT)"
    )
    conn.executemany("INSERT INTO defillama_daily_revenue VALUES ('p', ?, ?)", ours.items())
    conn.executemany(
        "INSERT INTO defillama_daily_revenue_restated VALUES ('p', ?, ?, ?)",
        [(d, v, FETCHED_AT) for d, v in src.items()],
    )
    return conn


class TestRestatementReport(unittest.TestCase):
    def test_rewritten_not_shift(self):
        # ens-like: the source filled a gap (0 -> real values) with no match
        # for these numbers on any neighbouring date of ours.
        # (neighbouring dates of both series are present: 26 and 30 Oct)
        ours = {"2026-10-26": 7000.0, "2026-10-27": 503.0, "2026-10-28": 0.0, "2026-10-29": 0.0, "2026-10-30": 0.0}
        src = {"2026-10-26": 7000.0, "2026-10-27": 15459.0, "2026-10-28": 20448.0, "2026-10-29": 8187.0,
               "2026-10-30": 0.0}
        days = rep.restated_days(ours, src)
        self.assertEqual([c for *_, c in days], ["rewritten"] * 3)
        text = rep.build_restatement_report(_db(ours, src), ["p"])[1]
        self.assertIn("сдвиг на день 0, переписано 3, не ясно 0", text)

    def test_shift_by_one_day(self):
        # safe-like: our 18 Sep value sits at the source on 17 Sep.
        ours = {"2026-09-17": 0.0, "2026-09-18": 10066.0}
        src = {"2026-09-17": 9954.0, "2026-09-18": 0.0}
        days = rep.restated_days(ours, src)
        self.assertEqual([c for *_, c in days], ["shift_earlier", "shift_earlier"])
        text = rep.build_restatement_report(_db(ours, src), ["p"])[1]
        self.assertIn("сдвиг на день 2, переписано 0, не ясно 0", text)
        self.assertIn("2026-09-18: 10 066 $ -> 0 $", text)

    def test_shift_later(self):
        # our 18 Sep value sits at the source on 19 Sep (mirror of the safe case)
        ours = {"2026-09-17": 9000.0, "2026-09-18": 10066.0, "2026-09-19": 0.0, "2026-09-20": 9000.0}
        src = {"2026-09-17": 9000.0, "2026-09-18": 0.0, "2026-09-19": 9954.0, "2026-09-20": 9000.0}
        days = rep.restated_days(ours, src)
        self.assertEqual([(d, c) for d, _, _, c in days], [("2026-09-18", "shift_later"), ("2026-09-19", "shift_later")])

    def test_unclear_at_the_edge_of_the_history(self):
        # a differing first day has no d-1: we cannot rule a shift out
        ours = {"2026-09-17": 5000.0, "2026-09-18": 6000.0}
        src = {"2026-09-17": 9000.0, "2026-09-18": 6000.0}
        self.assertEqual(rep.classify_day(ours, src, "2026-09-17"), "unclear")

    def test_unclear_next_to_our_gap(self):
        # d+1 is missing in our series: no pair to compare with
        ours = {"2026-09-16": 5000.0, "2026-09-17": 5000.0}
        src = {"2026-09-16": 5000.0, "2026-09-17": 9000.0, "2026-09-18": 7000.0}
        self.assertEqual(rep.classify_day(ours, src, "2026-09-17"), "unclear")

    def test_rewritten_needs_both_neighbours_in_both_series(self):
        ours = {"2026-09-16": 3000.0, "2026-09-17": 5000.0, "2026-09-18": 3500.0}
        src = {"2026-09-16": 3000.0, "2026-09-17": 9000.0, "2026-09-18": 3500.0}
        self.assertEqual(rep.classify_day(ours, src, "2026-09-17"), "rewritten")

    def test_match_with_barely_changed_receiving_cell_is_unclear(self):
        # the reviewer's example: 10000, 10400, 13000, 13200 moved by a day.
        # 10 Sep: ours 10000 -> src 13000? Here day 12 (13000 -> 10400) matches
        # day 11 of ours (10400), but day 11 changed only 10400 -> 10400 (0%).
        ours = {"2026-09-10": 10000.0, "2026-09-11": 10400.0, "2026-09-12": 13000.0, "2026-09-13": 13200.0}
        src = {"2026-09-10": 10000.0, "2026-09-11": 10400.0, "2026-09-12": 10400.0, "2026-09-13": 13200.0}
        self.assertEqual(rep.classify_day(ours, src, "2026-09-12"), "unclear")

    def test_receiving_cell_rule(self):
        # same number found next door, but that cell did not change: not a shift
        ours = {"2026-09-16": 5000.0, "2026-09-17": 5000.0, "2026-09-18": 9000.0}
        src = {"2026-09-16": 5000.0, "2026-09-17": 9000.0, "2026-09-18": 9000.0}
        self.assertEqual(rep.classify_day(ours, src, "2026-09-17"), "unclear")

    def test_no_complete_refetch_gives_no_report(self):
        conn = _db({"2026-09-17": 1.0}, {})
        self.assertIsNone(rep.build_restatement_report(conn, ["p"]))

    def test_incomplete_run_note(self):
        ours = {"2026-09-16": 3000.0, "2026-09-17": 5000.0, "2026-09-18": 3500.0}
        src = {"2026-09-16": 3000.0, "2026-09-17": 9000.0, "2026-09-18": 3500.0}
        conn = _db(ours, src)
        self.assertIn("запуск неполный", rep.build_restatement_report(conn, ["p"], run_incomplete=True)[0])
        self.assertNotIn("запуск неполный", rep.build_restatement_report(conn, ["p"])[0])


if __name__ == "__main__":
    unittest.main()
