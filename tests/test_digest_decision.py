"""Tests of the digest run's decision (notifications.telegram_bot.decide_action)
and of the "collection did not run" notice text.

The function is pure, so every branch is checked with plain dates and times.
Scenario of 26 Oct 2026: the night catch-up run at 01:28 saw data of the 25th,
the digest of the 25th had gone out - it must stay silent.
"""
import subprocess
import sys
import unittest
from datetime import date, datetime, time, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from notifications.telegram_bot import (  # noqa: E402
    NOTHING, SEND_DIGEST, SEND_NOTICE, Action, decide_action, format_collection_notice,
    scheduled_tasks_line, _stale_warning,
)

FIRST_RUN = time(12, 30)
D25, D26, D27, D28 = (date(2026, 10, d) for d in (25, 26, 27, 28))
NL = "\n"
NOON = time(14, 30)
NIGHT = time(1, 28)


class DecideActionTest(unittest.TestCase):
    def decide(self, data_date, today, now, sent=(), notices=()):
        return decide_action(data_date, today, now, FIRST_RUN, set(sent), set(notices))

    def test_1_normal_day_fresh_data_not_sent(self):
        self.assertEqual(self.decide(D26, D26, NOON), Action(SEND_DIGEST, day=D26))

    def test_2_fresh_data_already_sent(self):
        self.assertEqual(self.decide(D26, D26, NOON, sent=[D26]), Action(NOTHING))

    def test_3_night_catch_up_run_after_digest_of_yesterday(self):
        self.assertEqual(self.decide(D25, D26, NIGHT, sent=[D25]), Action(NOTHING))

    def test_4_night_run_digest_of_yesterday_never_sent(self):
        self.assertEqual(self.decide(D25, D26, NIGHT, sent=[]), Action(SEND_DIGEST, day=D25))

    def test_5_collection_failed_digest_of_data_date_was_sent(self):
        self.assertEqual(self.decide(D25, D26, NOON, sent=[D25]), Action(SEND_NOTICE, days=1))

    def test_6_notice_counts_days(self):
        self.assertEqual(self.decide(D25, D27, NOON, sent=[D25]), Action(SEND_NOTICE, days=2))
        self.assertEqual(self.decide(D25, D28, NOON, sent=[D25]), Action(SEND_NOTICE, days=3))

    def test_7_notice_already_sent_today(self):
        self.assertEqual(self.decide(D25, D27, NOON, sent=[D25], notices=[D27]), Action(NOTHING))

    def test_7b_notice_of_yesterday_does_not_block_today(self):
        self.assertEqual(self.decide(D25, D27, NOON, sent=[D25], notices=[D26]), Action(SEND_NOTICE, days=2))

    def test_8_fresh_data_after_notice_gives_digest_not_notice(self):
        self.assertEqual(
            self.decide(D27, D27, NOON, sent=[D25], notices=[D26, D27]), Action(SEND_DIGEST, day=D27)
        )

    def test_9_boundary_exactly_at_first_run_time(self):
        self.assertEqual(self.decide(D25, D26, FIRST_RUN, sent=[D25]), Action(SEND_NOTICE, days=1))

    def test_9b_boundary_one_minute_before_first_run_time(self):
        self.assertEqual(self.decide(D25, D26, time(12, 29), sent=[D25]), Action(NOTHING))

    def test_late_digest_of_old_data_date_is_allowed_after_work_hours_start(self):
        # data of the 25th, digest of the 25th never went out: send it under its own date
        self.assertEqual(self.decide(D25, D26, NOON, sent=[]), Action(SEND_DIGEST, day=D25))


def utc(day: int) -> str:
    """fetched_at ISO (UTC) at local noon of the given October 2026 day."""
    return datetime(2026, 10, day, 12, 0).astimezone().astimezone(timezone.utc).isoformat()


class NoticeTextTest(unittest.TestCase):
    LINE = "Планировщик Windows: сбор фьючерсов запускался 26 октября в 11:00, но свежих данных после него нет."

    def test_first_day_one_source(self):
        text = format_collection_notice(D25, 1, None, {"binance": utc(25)})
        self.assertEqual(
            text,
            "Сегодня не обновились фьючерсы — последний сбор 25 октября. Выручка и цены обновились.\n"
            "Новая сводка не уйдёт, пока не обновятся все три источника (выручка, цены, фьючерсы); "
            "сводка по прежним данным (за 25 октября) уже была.",
        )
        self.assertNotIn("Claude Code", text)

    CHECK = "Откройте Claude Code в папке проекта и попросите проверить, почему не идёт сбор данных."

    def test_check_line_last_from_second_day(self):
        for days in (2, 3):
            self.assertTrue(format_collection_notice(D25, days, self.LINE, {"binance": utc(25)}).endswith(NL + self.CHECK))
            self.assertTrue(format_collection_notice(D25, days, None, {}).endswith(NL + self.CHECK))
        self.assertNotIn("Claude Code", format_collection_notice(D25, 1, None, {}))

    def test_several_days_two_sources_same_date(self):
        text = format_collection_notice(D25, 3, self.LINE, {"defillama": utc(23), "coingecko": utc(23)})
        self.assertTrue(text.startswith(
            "Сбор не проходит 3-й день подряд: не обновились выручка и цены — последний сбор 23 октября. "
            "Фьючерсы обновились.\n"
        ))
        self.assertTrue(text.endswith("\n" + self.LINE + "\n" + self.CHECK))

    def test_sources_with_different_dates_each_named(self):
        text = format_collection_notice(D25, 2, None, {"defillama": utc(23), "binance": utc(24)})
        self.assertIn("выручка — последний сбор 23 октября; фьючерсы — последний сбор 24 октября.", text)
        self.assertIn("Цены обновились.", text)

    def test_no_reassuring_words(self):
        text = format_collection_notice(D25, 1, self.LINE, {"binance": utc(25)})
        self.assertNotIn("без сбоя", text)

    def test_without_line_and_sources_is_one_line(self):
        self.assertNotIn("\n", format_collection_notice(D25, 1, None, {}))

    def test_tasks_line_failure_gives_none(self):
        with mock.patch("notifications.telegram_bot.subprocess.run", side_effect=FileNotFoundError("powershell")):
            with self.assertLogs("notifications.telegram_bot", level="WARNING"):
                self.assertIsNone(scheduled_tasks_line({"binance": utc(25)}, D26))

    def test_tasks_line_powershell_failure_gives_none(self):
        error = subprocess.CalledProcessError(1, "powershell")
        with mock.patch("notifications.telegram_bot.subprocess.run", side_effect=error):
            with self.assertLogs("notifications.telegram_bot", level="WARNING"):
                self.assertIsNone(scheduled_tasks_line({"binance": utc(25)}, D26))

    STALE_ALL = {"defillama": utc(25), "coingecko": utc(25), "binance": utc(25)}
    TWO_FRESH = "2026-10-26 13:00|0\n2026-10-26 12:05|0\n"

    def line(self, stdout, stale=None):
        with mock.patch("notifications.telegram_bot.subprocess.run", return_value=mock.Mock(stdout=stdout)):
            return scheduled_tasks_line(self.STALE_ALL if stale is None else stale, D26)

    def test_tasks_line_ran_today_but_no_fresh_data(self):
        self.assertEqual(
            self.line(self.TWO_FRESH),
            "Планировщик Windows: сбор выручки и цен запускался 26 октября в 13:00, но свежих данных после него нет; "
            "сбор фьючерсов запускался 26 октября в 12:05, но свежих данных после него нет.",
        )

    def test_tasks_line_only_tasks_of_stale_sources(self):
        line = self.line(self.TWO_FRESH, stale={"binance": utc(25)})
        self.assertNotIn("выручки", line)
        self.assertIn("сбор фьючерсов", line)
        self.assertIsNone(self.line(self.TWO_FRESH, stale={}))

    def test_tasks_line_not_run_today(self):
        line = self.line("2026-10-25 13:00|0\n2026-10-25 12:05|0\n")
        self.assertIn("сбор выручки и цен: последний запуск 25 октября в 13:00, сегодня запуска не было", line)

    def test_tasks_line_never_ran_running_and_other_code(self):
        line = self.line("1999-11-30 00:00|267011\n2026-10-26 13:00|267009\n")
        self.assertIn("сбор выручки и цен ещё ни разу не запускался", line)
        self.assertIn("сбор фьючерсов выполняется сейчас (запущен 26 октября в 13:00)", line)
        line = self.line("2026-10-26 13:00|2147942402\n2026-10-26 13:00|0\n")
        self.assertIn("запускался 26 октября в 13:00 и завершился с кодом 2147942402", line)

    def test_tasks_line_unreadable_answer_gives_none(self):
        with self.assertLogs("notifications.telegram_bot", level="WARNING"):
            self.assertIsNone(self.line("2026-10-26 13:00|0\n"))  # one task of two

class StaleWarningTest(unittest.TestCase):
    def test_one_source_stale(self):
        self.assertEqual(
            _stale_warning({"binance": utc(25)}, D25, D26),
            "Эта сводка за 25 октября пришла с опозданием: "
            "за 26 октября сбор не завершён — не обновились фьючерсы.",
        )

    def test_never_collected_everywhere_is_new_install(self):
        text = _stale_warning({"defillama": None, "coingecko": None, "binance": None}, D25, D26)
        self.assertIn("новая установка", text)
        self.assertNotIn("с опозданием", text)

    def test_never_collected_one_source_is_not_new_install(self):
        text = _stale_warning({"binance": None}, D25, D26)
        self.assertNotIn("новая установка", text)
        self.assertIn("с опозданием", text)


if __name__ == "__main__":
    unittest.main()
