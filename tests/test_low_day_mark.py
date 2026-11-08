"""Tests of mark 3 of revenue_price_gap, "one day falls out downward" (8 Nov 2026):
signals/revenue_price_gap.py's _shape_check and the wording in
notifications/telegram_bot.py. stdlib unittest, no database, no network.

Run: .venv\\Scripts\\python.exe -m unittest tests.test_low_day_mark
"""
from __future__ import annotations

import json
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from notifications.telegram_bot import _revenue_mark_texts, _times_text  # noqa: E402
from signals.revenue_price_gap import _shape_check  # noqa: E402

LAST_DAY = date(2026, 10, 25)


def rows(values: list[float]) -> list[dict]:
    """Baseline rows, oldest first, the last value on LAST_DAY."""
    first = LAST_DAY - timedelta(days=len(values) - 1)
    return [{"date": (first + timedelta(days=i)).isoformat(), "revenue_usd": v} for i, v in enumerate(values)]


def check(values: list[float]) -> dict:
    return _shape_check(rows(values), shape_mark_ratio=2.0, single_day_mark_ratio=2.0, low_day_mark_ratio=5.0)


# 90 ordinary days: 100..140 in a five-day cycle, so the usual smallest day of a
# week is ~12% of its sum
ORDINARY = [100.0 + 10 * (i % 5) for i in range(90)]


class LowDayMarkTests(unittest.TestCase):
    def test_ordinary_week_is_not_marked(self):
        result = check(ORDINARY)
        self.assertFalse(result["shape_check_skipped"])
        self.assertFalse(result["low_day_marked"])
        self.assertLess(result["low_day_ratio"], 5)

    def test_one_collapsed_day_is_marked_with_a_finite_ratio(self):
        result = check(ORDINARY[:-3] + [2.0] + ORDINARY[-2:])
        self.assertTrue(result["low_day_marked"])
        self.assertGreater(result["low_day_ratio"], 5)
        json.dumps(result)  # no inf/NaN in what goes to details_json

    def test_zero_day_is_marked_without_dividing_by_zero(self):
        result = check(ORDINARY[:-3] + [0.0] + ORDINARY[-2:])
        self.assertTrue(result["low_day_marked"])
        self.assertIsNone(result["low_day_ratio"])
        self.assertEqual(result["window_min_day_share_pct"], 0.0)
        json.dumps(result)

    def test_zeros_are_normal_for_the_coin_so_no_mark(self):
        # every sixth day is zero, so every week has a zero day (the coin's usual
        # smallest day is 0) but few have two (the shape check still applies)
        values = [0.0 if i % 6 == 0 else 100.0 for i in range(90)]
        result = check(values)
        self.assertFalse(result["shape_check_skipped"])
        self.assertEqual(result["habitual_min_day_share_pct"], 0.0)
        self.assertFalse(result["low_day_marked"])

    def test_skipped_check_never_marks(self):
        result = check(ORDINARY[:30][:-3] + [0.0] + ORDINARY[:2])  # too little history
        self.assertTrue(result["shape_check_skipped"])
        self.assertFalse(result["low_day_marked"])


class LowDayTextTests(unittest.TestCase):
    def details(self, values: list[float]) -> dict:
        result = check(values)
        return result | {"shape_window_values": result["shape_window_values"]}

    def test_text_names_the_day_and_the_amount_without_a_verdict(self):
        texts = _revenue_mark_texts(self.details(ORDINARY[:-3] + [2.0] + ORDINARY[-2:]))
        self.assertEqual(len(texts.marks), 1)
        self.assertIn("день выбивается вниз: 23 октября — 2 $", texts.marks[0])
        self.assertIn("доля меньше в", texts.marks[0])

    def mark(self, **over) -> str:
        """The mark text for a stored low-day event with the given fields."""
        values = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 2.0]
        details = {
            "low_day_marked": True, "shape_window_values": values,
            "shape_window_first": "2026-10-19", "shape_window_last": "2026-10-25",
            "habitual_min_day_share_pct": 12.0, "window_min_day_share_pct": 0.3,
            "low_day_ratio": 40.0,
        } | over
        return _revenue_mark_texts(details).marks[0]

    def test_ratio_is_the_true_one_not_the_one_from_rounded_shares(self):
        # 28 Oct 2026 (RUNE): the one-decimal shares 3,9% and 0,2% would give
        # "в 20 раз"; the real ratio is 16.2 and that is what must be printed,
        # next to a share of 0,24% so that 3,9 / 0,24 = 16 agrees with it
        self.assertEqual(
            self.mark(habitual_min_day_share_pct=3.9, window_min_day_share_pct=0.2399, low_day_ratio=16.2),
            "день выбивается вниз: 25 октября — 2 $, это 0,24% суммы дней, показанных ниже; "
            "по прошлым неделям самый малый день этой монеты даёт 3,9% такой суммы, "
            "сейчас доля меньше в 16 раз",
        )

    def test_small_ratio_has_one_decimal_and_raza(self):
        self.assertEqual(
            self.mark(window_min_day_share_pct=2.6, low_day_ratio=4.6),
            "день выбивается вниз: 25 октября — 2 $, это 2,6% суммы дней, показанных ниже; "
            "по прошлым неделям самый малый день этой монеты даёт 12,0% такой суммы, "
            "сейчас доля меньше в 4,6 раза",
        )

    def test_times_declension(self):
        cases = {
            5.0: "в 5 раз", 4.96: "в 5 раз", 4.6: "в 4,6 раза", 9.96: "в 10 раз",
            10.4: "в 10 раз", 12.0: "в 12 раз", 13.2: "в 13 раз", 21.0: "в 21 раз",
            22.3: "в 22 раза", 24.0: "в 24 раза", 25.0: "в 25 раз", 100.0: "в 100 раз",
            11.0: "в 11 раз", 14.0: "в 14 раз", 111.0: "в 111 раз", 122.0: "в 122 раза",
            9.95: "в 9,9 раза",  # 9.95 is stored as 9.9499..., so 9,9 is the true rounding
             102.0: "в 102 раза", 112.0: "в 112 раз", 999.6: "в 1000 раз",
            1000.0: "более чем в 1000 раз", 12000.0: "более чем в 1000 раз",
        }
        for ratio, text in cases.items():
            self.assertEqual(_times_text(ratio), text, ratio)

    def test_share_below_one_percent_has_two_decimals(self):
        self.assertEqual(
            self.mark(),
            "день выбивается вниз: 25 октября — 2 $, это 0,30% суммы дней, показанных ниже; "
            "по прошлым неделям самый малый день этой монеты даёт 12,0% такой суммы, "
            "сейчас доля меньше в 40 раз",
        )

    def test_tiny_share_is_not_printed_as_zero(self):
        # 0,04% would print as 0,0% with one decimal; with two it is 0,04%
        self.assertEqual(
            self.mark(window_min_day_share_pct=0.04, low_day_ratio=300.0),
            "день выбивается вниз: 25 октября — 2 $, это 0,04% суммы дней, показанных ниже; "
            "по прошлым неделям самый малый день этой монеты даёт 12,0% такой суммы, "
            "сейчас доля меньше в 300 раз",
        )

    def test_huge_ratio_is_capped(self):
        self.assertEqual(
            self.mark(window_min_day_share_pct=0.001, low_day_ratio=12000.0),
            "день выбивается вниз: 25 октября — 2 $; по прошлым неделям самый малый день "
            "этой монеты даёт 12,0% суммы дней, показанных ниже",
        )
        self.assertEqual(
            self.mark(window_min_day_share_pct=0.01, low_day_ratio=1200.0),
            "день выбивается вниз: 25 октября — 2 $, это 0,01% суммы дней, показанных ниже; "
            "по прошлым неделям самый малый день этой монеты даёт 12,0% такой суммы, "
            "сейчас доля меньше более чем в 1000 раз",
        )

    def test_zero_day_text(self):
        texts = _revenue_mark_texts(self.details(ORDINARY[:-3] + [0.0] + ORDINARY[-2:]))
        self.assertIn("день выбивается вниз: 23 октября — 0 $", texts.marks[0])

    def test_old_event_without_the_new_keys_gives_no_mark(self):
        old = {"single_day_marked": False, "shape_marked": False}
        self.assertEqual(_revenue_mark_texts(old).marks, [])


if __name__ == "__main__":
    unittest.main()
