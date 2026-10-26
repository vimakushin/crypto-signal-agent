"""Tests of the digest run's database side: the data date (storage.db.get_data_date,
on an in-memory database) and the order "send, then mark" in
notifications.telegram_bot.main (sending and the database are substituted, nothing
goes to Telegram and no real file is touched).
"""
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from notifications import telegram_bot  # noqa: E402
from storage.db import get_connection, get_data_date, get_digest_send_dates, init_db  # noqa: E402

TODAY = date(2026, 10, 26)
ENV = {"TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c"}


def memory_db() -> sqlite3.Connection:
    """The three tables get_data_date reads, with just the fetched_at column."""
    conn = sqlite3.connect(":memory:")
    for table in ("defillama_snapshots", "coingecko_price_history", "binance_oi_snapshots"):
        conn.execute(f"CREATE TABLE {table} (fetched_at TEXT)")
    return conn


def local_midnight_plus(hours: float) -> str:
    """UTC ISO moment that is `hours` after local midnight of TODAY."""
    local = datetime(2026, 10, 26).astimezone() + timedelta(hours=hours)
    return local.astimezone(timezone.utc).isoformat()


class GetDataDateTest(unittest.TestCase):
    def put(self, conn, table, iso):
        conn.execute(f"INSERT INTO {table} VALUES (?)", (iso,))

    def test_empty_database_gives_today(self):
        self.assertEqual(get_data_date(memory_db(), TODAY), TODAY)

    def test_source_without_data_is_skipped(self):
        conn = memory_db()
        self.put(conn, "defillama_snapshots", local_midnight_plus(5))
        self.assertEqual(get_data_date(conn, date(2026, 10, 30)), TODAY)

    def test_minimum_over_three_sources(self):
        conn = memory_db()
        self.put(conn, "defillama_snapshots", local_midnight_plus(5))
        self.put(conn, "coingecko_price_history", local_midnight_plus(5 - 24))
        self.put(conn, "binance_oi_snapshots", local_midnight_plus(5 + 24))
        self.assertEqual(get_data_date(conn, date(2026, 10, 30)), TODAY - timedelta(days=1))

    def test_midnight_boundary_uses_local_date_not_utc(self):
        # 30 minutes after local midnight: in UTC this may still be the day before
        # (any zone east of UTC), the local date must win; 30 minutes before local
        # midnight is the previous local date.
        conn = memory_db()
        self.put(conn, "defillama_snapshots", local_midnight_plus(0.5))
        self.assertEqual(get_data_date(conn, date(2026, 10, 30)), TODAY)
        conn = memory_db()
        self.put(conn, "defillama_snapshots", local_midnight_plus(-0.5))
        self.assertEqual(get_data_date(conn, date(2026, 10, 30)), TODAY - timedelta(days=1))


class MainSendThenMarkTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        init_db(self.db_path)  # empty database: data date = today, no digest sent yet
        config = {
            "storage": {"sqlite_path": str(self.db_path)},
            "schedule": {"telegram_run_time": "00:00"},
        }
        patches = [
            mock.patch.object(telegram_bot, "_read_env_file", return_value=ENV),
            mock.patch.object(telegram_bot, "load_config", return_value=config),
            mock.patch.object(telegram_bot, "signal_weights_from_config", return_value={}),
            mock.patch.object(telegram_bot, "default_since_by_signal", return_value={}),
            mock.patch.object(telegram_bot, "rank_candidates", return_value=[]),
            mock.patch.object(telegram_bot, "format_digest", return_value="text"),
            mock.patch("notifications.observations.append_new_observations"),
            mock.patch("notifications.observations.backfill_seven_day_outcomes"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def marks(self):
        conn = get_connection(self.db_path)
        try:
            return get_digest_send_dates(conn)
        finally:
            conn.close()

    def test_failed_send_leaves_no_mark(self):
        with mock.patch.object(telegram_bot, "send_telegram_message", return_value=False) as send:
            self.assertEqual(telegram_bot.main(), 1)
        send.assert_called_once()
        self.assertEqual(self.marks(), set())

    def test_successful_send_leaves_mark_and_names_the_digest(self):
        with mock.patch.object(telegram_bot, "send_telegram_message", return_value=True) as send:
            self.assertEqual(telegram_bot.main(), 0)
        self.assertEqual(self.marks(), {date.today()})
        self.assertIn("сводка за", send.call_args.kwargs["what"])


class WaitSkippedAfterNoticeTest(unittest.TestCase):
    """Late data, regular time passed: wait for fresh data - unless today's
    notice has already gone out."""

    def run_main(self, notices):
        today = date.today()
        yesterday = today - timedelta(days=1)
        state = (yesterday, {yesterday}, notices, {"binance": None})
        config = {"storage": {"sqlite_path": ":memory:"}, "schedule": {"telegram_run_time": "00:00"}}
        with mock.patch.object(telegram_bot, "_read_env_file", return_value=ENV), \
                mock.patch.object(telegram_bot, "load_config", return_value=config), \
                mock.patch.object(telegram_bot, "init_db"), \
                mock.patch.object(telegram_bot, "_read_state", return_value=state), \
                mock.patch.object(telegram_bot, "scheduled_tasks_line", return_value=None), \
                mock.patch.object(telegram_bot, "send_telegram_message", return_value=False), \
                mock.patch.object(telegram_bot, "wait_for_fresh_data") as wait:
            telegram_bot.main()
        return wait

    def test_waits_when_no_notice_yet(self):
        self.run_main(set()).assert_called_once()

    def test_does_not_wait_when_notice_already_sent_today(self):
        self.run_main({date.today()}).assert_not_called()


if __name__ == "__main__":
    unittest.main()
