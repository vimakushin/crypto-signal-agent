"""Tests of the "an unfinished day is never stored" protection (25 Oct 2026):
DeFiLlama's current-day revenue, CoinGecko's trailing "current" point, and the
revenue_price_gap window that ends on the last FINISHED UTC day. stdlib unittest,
network replaced by mocks, in-memory SQLite with the real schema.

Run: .venv\\Scripts\\python.exe -m unittest tests.test_unfinished_day
"""
from __future__ import annotations

import sqlite3
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import coingecko, defillama  # noqa: E402
from signals import revenue_price_gap  # noqa: E402
from storage.db import SCHEMA  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import replay_signals  # noqa: E402

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()
MS_PER_DAY = 86_400_000


class DefiLlamaTodayTests(unittest.TestCase):
    def test_today_and_later_days_are_not_returned(self):
        days = [TODAY - timedelta(days=2), TODAY - timedelta(days=1), TODAY, TODAY + timedelta(days=1)]
        history = {"daily_revenue": [(d.isoformat(), 100.0) for d in days]}
        with mock.patch.object(defillama, "fetch_protocol_daily_revenue_history", return_value=history):
            records = defillama.collect_daily_revenue(["proto"])
        self.assertEqual(
            [r["date"] for r in records],
            [(TODAY - timedelta(days=2)).isoformat(), (TODAY - timedelta(days=1)).isoformat()],
        )

    def test_window_is_filled_with_finished_days(self):
        # more finished days than the window: today's row must not take a place in it
        count = defillama.LIVE_DAILY_REVENUE_WINDOW_DAYS + 5
        days = [TODAY - timedelta(days=k) for k in range(count, -1, -1)]  # ends with today
        history = {"daily_revenue": [(d.isoformat(), 1.0) for d in days]}
        with mock.patch.object(defillama, "fetch_protocol_daily_revenue_history", return_value=history):
            records = defillama.collect_daily_revenue(["proto"])
        self.assertEqual(len(records), defillama.LIVE_DAILY_REVENUE_WINDOW_DAYS)
        self.assertEqual(records[-1]["date"], (TODAY - timedelta(days=1)).isoformat())


class CoinGeckoCurrentPointTests(unittest.TestCase):
    @staticmethod
    def midnight_ms(day) -> int:
        return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)

    def fetch(self, points: list[int]) -> list[dict]:
        data = {
            "prices": [[ts, 1.0] for ts in points],
            "market_caps": [[ts, 2.0] for ts in points],
            "total_volumes": [[ts, 3.0] for ts in points],
        }
        with mock.patch.object(coingecko, "_get_with_retry", return_value=data):
            return coingecko.fetch_market_chart("coin")

    def test_current_point_is_dropped_next_to_todays_midnight_point(self):
        yesterday = self.midnight_ms(TODAY - timedelta(days=1))
        today = self.midnight_ms(TODAY)
        records = self.fetch([yesterday, today, today + 9 * 3_600_000 + 123])
        self.assertEqual([r["date"] for r in records], [(TODAY - timedelta(days=1)).isoformat(), TODAY.isoformat()])
        self.assertTrue(all(r["timestamp"].endswith("T00:00:00+00:00") for r in records))

    def test_current_point_is_dropped_even_without_todays_midnight_point(self):
        yesterday = self.midnight_ms(TODAY - timedelta(days=1))
        records = self.fetch([yesterday, yesterday + MS_PER_DAY + 9 * 3_600_000])
        self.assertEqual([r["date"] for r in records], [(TODAY - timedelta(days=1)).isoformat()])


class RevenueWindowEndsYesterdayTests(unittest.TestCase):
    def test_window_ends_on_the_day_before_the_snapshot_and_ignores_today(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        snapshot_day = datetime(2026, 10, 26, 9, 0, tzinfo=timezone.utc)
        for days_ago, mcap in ((7, 100.0), (0, 101.0)):
            moment = (snapshot_day - timedelta(days=days_ago)).isoformat()
            conn.execute(
                "INSERT INTO defillama_snapshots (protocol_slug, symbol, name, fetched_at, mcap) "
                "VALUES ('p', 'P', 'P', ?, ?)", (moment, mcap),
            )
        # 83 ordinary days of 1000, then the last 7 finished days (19-25 Oct) at 3000,
        # then a stray row for "today" (26 Oct) that must stay outside the window
        for k in range(90, -1, -1):
            day = snapshot_day.date() - timedelta(days=k)
            value = 1_000_000.0 if k > 7 else 3_000_000.0
            if k == 0:
                value = 777_000_000.0
            conn.execute(
                "INSERT INTO defillama_daily_revenue (protocol_slug, date, revenue_usd, fetched_at) "
                "VALUES ('p', ?, ?, ?)", (day.isoformat(), value, snapshot_day.isoformat()),
            )
        result = revenue_price_gap.scan_watchlist(
            conn, ["p"],
            revenue_growth_threshold_pct=50, mcap_reaction_threshold_pct=10,
            lookback_days=7, baseline_window_days=90,
            shape_mark_ratio=2.0, single_day_mark_ratio=2.0,
        )
        self.assertEqual(len(result.signals), 1, result)
        signal = result.signals[0]
        self.assertEqual(signal.revenue_window_last, "2026-10-25")
        self.assertEqual(signal.revenue_window_first, "2026-10-19")
        self.assertEqual(signal.revenue_window_count, 7)
        self.assertEqual(signal.baseline_window_last, "2026-10-25")
        self.assertEqual(signal.recent_median_daily_revenue, 3_000_000.0)


class ReplayWindowMatchesLiveTests(unittest.TestCase):
    """The replay (scripts/replay_signals.py, both revenue sources) and the live
    signal must end the revenue window on the same day: the shared rule
    revenue_price_gap.revenue_window_end_date."""

    SNAPSHOT = datetime(2026, 10, 26, 9, 0, tzinfo=timezone.utc)
    CFG = {
        "lookback_days": 7, "baseline_window_days": 90,
        "revenue_growth_threshold_pct": 50, "mcap_reaction_threshold_pct": 10,
        "single_day_mark_ratio": 2.0, "shape_mark_ratio": 2.0,
    }

    def build_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        for days_ago, mcap in ((7, 100.0), (0, 101.0)):
            moment = (self.SNAPSHOT - timedelta(days=days_ago)).isoformat()
            conn.execute(
                "INSERT INTO defillama_snapshots (protocol_slug, symbol, name, fetched_at, mcap) "
                "VALUES ('p', 'P', 'P', ?, ?)", (moment, mcap),
            )
        for k in range(90, -1, -1):
            day = (self.SNAPSHOT.date() - timedelta(days=k)).isoformat()
            value = 1_000_000.0 if k > 7 else 3_000_000.0
            if k == 0:
                value = 777_000_000.0  # "today": outside the window in every mode
            for table in ("defillama_daily_revenue", "defillama_daily_revenue_restated"):
                conn.execute(
                    f"INSERT INTO {table} (protocol_slug, date, revenue_usd, fetched_at) "
                    "VALUES ('p', ?, ?, ?)", (day, value, self.SNAPSHOT.isoformat()),
                )
        return conn

    def test_rule_itself(self):
        self.assertEqual(revenue_price_gap.revenue_window_end_date("2026-10-26T09:00:00+00:00"), "2026-10-25")

    def test_rule_on_the_border(self):
        # before the change (incl. 25 Oct, when the live run still had the old
        # code): window ends on the snapshot's own day; from 26 Oct: the day before
        f = revenue_price_gap.revenue_window_end_date
        self.assertEqual(revenue_price_gap.WINDOW_RULE_CHANGE_DATE.isoformat(), "2026-10-26")
        self.assertEqual(f("2026-10-24T09:00:00+00:00"), "2026-10-24")
        self.assertEqual(f("2026-10-25T00:00:00+00:00"), "2026-10-25")
        self.assertEqual(f("2026-10-25T23:59:00+00:00"), "2026-10-25")
        self.assertEqual(f("2026-10-26T00:00:00+00:00"), "2026-10-25")
        self.assertEqual(f("2026-10-27T09:00:00+00:00"), "2026-10-26")

    def test_live_and_replay_on_both_sides_of_the_border(self):
        # same data shifted so the snapshot is 25 Oct (old rule: window includes
        # the snapshot's own day) or 26 Oct (new rule: ends the day before);
        # revenue of the snapshot's own day is an outlier that only the old rule includes
        for snapshot, expected_last, own_day_in_window in (
                (datetime(2026, 10, 25, 9, 0, tzinfo=timezone.utc), "2026-10-25", True),
                (datetime(2026, 10, 26, 9, 0, tzinfo=timezone.utc), "2026-10-25", False)):
            with self.subTest(snapshot=snapshot.date().isoformat()):
                case = type(self)()
                case.SNAPSHOT = snapshot
                conn = case.build_db()
                live = revenue_price_gap.scan_watchlist(
                    conn, ["p"],
                    revenue_growth_threshold_pct=50, mcap_reaction_threshold_pct=10,
                    lookback_days=7, baseline_window_days=90,
                    shape_mark_ratio=2.0, single_day_mark_ratio=2.0,
                ).signals
                self.assertEqual(len(live), 1)
                self.assertEqual(live[0].revenue_window_last, expected_last)
                self.assertEqual(live[0].revenue_window_last == snapshot.date().isoformat(), own_day_in_window)
                for restated_at in (None, snapshot.isoformat()):
                    result = replay_signals.replay_revenue_price_gap(conn, ["p"], self.CFG, restated_at)
                    fired = [s for _, s in result["per_protocol_entries"]["p"] if s]
                    self.assertEqual(len(fired), 1)
                    self.assertEqual(fired[0].revenue_window_last, expected_last)
                    self.assertEqual(fired[0].revenue_window_last == snapshot.date().isoformat(), own_day_in_window)

    def test_live_and_replay_windows_end_on_the_same_day(self):
        conn = self.build_db()
        live = revenue_price_gap.scan_watchlist(
            conn, ["p"],
            revenue_growth_threshold_pct=50, mcap_reaction_threshold_pct=10,
            lookback_days=7, baseline_window_days=90,
            shape_mark_ratio=2.0, single_day_mark_ratio=2.0,
        ).signals
        self.assertEqual(len(live), 1)
        for restated_at in (None, self.SNAPSHOT.isoformat()):
            with self.subTest(source="stored" if restated_at is None else "restated"):
                result = replay_signals.replay_revenue_price_gap(conn, ["p"], self.CFG, restated_at)
                fired = [s for _, s in result["per_protocol_entries"]["p"] if s]
                self.assertEqual(len(fired), 1)
                self.assertEqual(fired[0].revenue_window_last, live[0].revenue_window_last)
                self.assertEqual(fired[0].baseline_window_last, live[0].baseline_window_last)
                self.assertEqual(fired[0].revenue_window_last, "2026-10-25")


class ReplayHidesRevenueReceivedLaterTests(unittest.TestCase):
    """Two neighbouring snapshots D and D+1. The revenue of D-1 (the last day of
    D's window) reaches us for three coins at different moments: before snapshot
    D, between D and D+1, after D+1. The replay of D shows what arrived before
    the NEXT snapshot (D+1): the first two coins are seen, the third is hidden."""

    CFG = ReplayWindowMatchesLiveTests.CFG
    D = datetime(2026, 10, 26, 9, 0, tzinfo=timezone.utc)  # new rule (from 26 Oct): window ends D-1
    RESTATED_AT = "2026-11-02T09:00:00+00:00"

    def build_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        # when D-1's revenue reached us, per coin
        received = {
            "before_d": self.D - timedelta(hours=1),
            "between": self.D + timedelta(hours=12),
            "after_d1": self.D + timedelta(days=1, hours=12),
        }
        old = (self.D - timedelta(days=100)).isoformat()
        for slug, got in received.items():
            for moment, mcap in ((self.D - timedelta(days=7), 100.0), (self.D, 101.0), (self.D + timedelta(days=1), 102.0)):
                conn.execute(
                    "INSERT INTO defillama_snapshots (protocol_slug, symbol, name, fetched_at, mcap) "
                    "VALUES (?, 'P', 'P', ?, ?)", (slug, moment.isoformat(), mcap),
                )
            for k in range(97, -1, -1):
                day = (self.D.date() - timedelta(days=k)).isoformat()
                value = 1_000_000.0 if k > 7 else 3_000_000.0  # k=0 is D itself (D+1's window)
                fetched = got.isoformat() if k == 1 else old
                conn.execute(
                    "INSERT INTO defillama_daily_revenue (protocol_slug, date, revenue_usd, fetched_at) "
                    "VALUES (?, ?, ?, ?)", (slug, day, value, fetched),
                )
                conn.execute(
                    "INSERT INTO defillama_daily_revenue_restated (protocol_slug, date, revenue_usd, fetched_at) "
                    "VALUES (?, ?, ?, ?)", (slug, day, value, self.RESTATED_AT),
                )
        return conn

    def window_counts(self, result: dict, slug: str, snapshot_day) -> int:
        signals = [s for d, s in result["per_protocol_entries"][slug] if d == snapshot_day and s]
        self.assertEqual(len(signals), 1, (slug, snapshot_day))
        return signals[0].revenue_window_count

    def test_row_received_after_the_next_snapshot_is_hidden(self):
        conn = self.build_db()
        day_d, day_d1 = self.D.date(), self.D.date() + timedelta(days=1)
        slugs = ["before_d", "between", "after_d1"]
        for restated_at in (None, self.RESTATED_AT):
            with self.subTest(source="stored" if restated_at is None else "restated"):
                result = replay_signals.replay_revenue_price_gap(conn, slugs, self.CFG, restated_at)
                # D's window is D-7..D-1 (7 days); the run for D sees what arrived before D+1
                self.assertEqual(self.window_counts(result, "before_d", day_d), 7)
                self.assertEqual(self.window_counts(result, "between", day_d), 7)
                self.assertEqual(self.window_counts(result, "after_d1", day_d), 6)
                # the last snapshot has no cutoff: everything is visible
                for slug in slugs:
                    self.assertEqual(self.window_counts(result, slug, day_d1), 7)


if __name__ == "__main__":
    unittest.main()
