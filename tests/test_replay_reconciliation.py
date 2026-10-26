"""Tests of the replay-vs-journal difference classifier
(replay_signals.is_discrepancy_explained), including the rule boundaries.

Run: .venv\\Scripts\\python.exe -m unittest tests.test_replay_reconciliation
"""
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from replay_signals import is_discrepancy_explained as explained  # noqa: E402

BEFORE = date(2026, 10, 10)


class TestDiscrepancyClassifier(unittest.TestCase):
    def test_extra_before_change_high_share_explained(self):
        self.assertTrue(explained(BEFORE, 60.0, "extra"))

    def test_extra_before_change_low_share_unexplained(self):
        self.assertFalse(explained(BEFORE, 30.0, "extra"))

    def test_extra_after_change_unexplained(self):
        self.assertFalse(explained(date(2026, 10, 25), 90.0, "extra"))

    def test_extra_without_share_unexplained(self):
        self.assertFalse(explained(BEFORE, None, "extra"))

    def test_missing_always_unexplained(self):
        self.assertFalse(explained(BEFORE, 60.0, "missing"))
        self.assertFalse(explained(date(2026, 10, 25), 60.0, "missing"))
        self.assertFalse(explained(BEFORE, None, "missing"))

    def test_boundary_exactly_19_oct_unexplained(self):
        self.assertFalse(explained(date(2026, 10, 19), 60.0, "extra"))

    def test_boundary_18_oct_explained(self):
        self.assertTrue(explained(date(2026, 10, 18), 60.0, "extra"))

    def test_boundary_exactly_50_pct_unexplained(self):
        self.assertFalse(explained(BEFORE, 50.0, "extra"))

    def test_boundary_50_01_pct_explained(self):
        self.assertTrue(explained(BEFORE, 50.01, "extra"))


if __name__ == "__main__":
    unittest.main()
