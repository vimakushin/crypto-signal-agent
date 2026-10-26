"""Tests of scripts/backup_db.py's rotation decision (select_backups_to_delete):
which backup files to delete, as a pure function of file names, today's date
and keep_days. No files and no clock are touched.

Run: .venv\\Scripts\\python.exe -m unittest tests.test_backup_rotation
"""
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from backup_db import select_backups_to_delete  # noqa: E402

TODAY = date(2026, 10, 26)


def _name(ymd: str, hms: str = "120000") -> str:
    return f"db_{ymd}_{hms}.sqlite"


class SelectBackupsToDeleteTest(unittest.TestCase):
    def test_older_than_keep_days_deleted_boundary_kept(self) -> None:
        # 26 Oct minus 14 days = 12 Oct (kept); 11 Oct is 15 days old (deleted).
        # 10 Oct is the earliest of the month in this list, so it is protected;
        # the 11 Oct copy is therefore the one that shows the age rule.
        names = [_name("20261010"), _name("20261011"), _name("20261012"), _name("20261026")]
        self.assertEqual(select_backups_to_delete(names, TODAY, 14), [_name("20261011")])

    def test_exactly_keep_days_and_younger_stay(self) -> None:
        names = [_name("20261001"), _name("20261012"), _name("20261013"), _name("20261026")]
        self.assertEqual(select_backups_to_delete(names, TODAY, 14), [])

    def test_earliest_of_each_month_kept_forever(self) -> None:
        names = [
            _name("20260803", "090000"), _name("20260805"),
            _name("20260901"), _name("20260930"),
            _name("20261001"), _name("20261026"),
        ]
        self.assertEqual(
            select_backups_to_delete(names, TODAY, 14),
            [_name("20260805"), _name("20260930")],
        )

    def test_earliest_of_month_decided_by_time_within_one_day(self) -> None:
        names = [_name("20260801", "235900"), _name("20260801", "010000"), _name("20260802")]
        self.assertEqual(
            select_backups_to_delete(names, TODAY, 14),
            [_name("20260801", "235900"), _name("20260802")],
        )

    def test_several_manual_copies_in_one_day_push_nothing_out(self) -> None:
        manual = [_name("20261025", f"1{i}0000") for i in range(6)]
        daily = [_name(f"202610{d:02d}") for d in range(13, 25)]
        self.assertEqual(select_backups_to_delete(manual + daily, TODAY, 14), [])

    def test_foreign_and_malformed_names_never_returned(self) -> None:
        names = [
            "notes.txt", "db_20200101_000000.sqlite.tmp", "db_2020_01.sqlite",
            "xdb_20200101_000000.sqlite", "db_20261345_000000.sqlite",
            "db_20200101_000000.sqlite",
        ]
        self.assertEqual(select_backups_to_delete(names, TODAY, 14), [])

    def test_empty_folder(self) -> None:
        self.assertEqual(select_backups_to_delete([], TODAY, 14), [])

    def test_day_exactly_keep_days_ago_kept_next_day_deleted(self) -> None:
        names = [_name("20261001"), _name("20261012"), _name("20261011")]
        self.assertEqual(select_backups_to_delete(names, TODAY, 14), [_name("20261011")])
        names2 = [_name("20261001"), _name("20261012")]
        self.assertEqual(select_backups_to_delete(names2, date(2026, 10, 27), 14), [_name("20261012")])


if __name__ == "__main__":
    unittest.main()
