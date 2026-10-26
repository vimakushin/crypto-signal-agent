"""Tests of the digest's market background: the median price change
(storage.db.get_market_change), the median volume ratio
(storage.db.get_market_volume_ratio) and the cards built on them
(notifications.telegram_bot). One file, because all of them check one thing:
which coins enter the median (the card's own coin must not) and when the
honest answer is "not computed". stdlib unittest on an in-memory SQLite with
the real schema, no network, no extra dependencies.

Run: .venv\\Scripts\\python.exe -m unittest discover tests
"""
from __future__ import annotations

import sqlite3
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from notifications.telegram_bot import (  # noqa: E402
    _GAP_SENTENCE, _digest_text_revenue_price_gap, _freshness_label, _digest_text_volume_breakout, _market_background_line,
    _market_note, _volume_background_line,
)  # noqa: E402
from collectors import binance_futures  # noqa: E402
from storage.db import (  # noqa: E402
    DAILY_VOLUME_FIRST_RUN_DAYS, SCHEMA, daily_volume_fetch_limit, get_market_change, get_market_volume_ratio,
)

START = "2026-10-15T00:00:00+00:00"
END = "2026-10-22T00:00:00+00:00"
DAY = "2026-10-21"  # a Binance day
AVG_DAYS = 30


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def add_prices(conn, symbol: str, start_price: float, end_price: float) -> None:
    for moment, price in ((START, start_price), (END, end_price)):
        conn.execute(
            "INSERT INTO binance_oi_snapshots (symbol, fetched_at, oi_timestamp, oi, oi_value_usdt, price) "
            "VALUES (?, ?, ?, 1, 1, ?)", (symbol, END, moment, price),
        )


def add_volume(conn, symbol: str, ratio: float, prior_days: int = AVG_DAYS) -> None:
    """`prior_days` days of volume 100 before DAY, and DAY itself at 100*ratio."""
    day = date.fromisoformat(DAY)
    rows = [(symbol, (day - timedelta(days=i)).isoformat(), 100.0, END) for i in range(1, prior_days + 1)]
    rows.append((symbol, DAY, 100.0 * ratio, END))
    conn.executemany("INSERT INTO binance_daily_volume VALUES (?, ?, ?, ?)", rows)


class MarketChangeTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        # +10%, +20%, +30% and one outlier +1000%
        for symbol, end in (("AUSDT", 110), ("BUSDT", 120), ("CUSDT", 130), ("DUSDT", 1100)):
            add_prices(self.conn, symbol, 100, end)
        self.symbols = ["AUSDT", "BUSDT", "CUSDT", "DUSDT"]

    def test_without_exclude_all_coins_count(self):
        result = get_market_change(self.conn, self.symbols, START, END)
        self.assertEqual((result.n, result.total), (4, 4))
        self.assertAlmostEqual(result.median_pct, 25.0)

    def test_exclude_removes_coin_from_median_and_total(self):
        result = get_market_change(self.conn, self.symbols, START, END, exclude="DUSDT")
        self.assertEqual((result.n, result.total), (3, 3))
        self.assertAlmostEqual(result.median_pct, 20.0)

    def test_foreign_ticker_excludes_nothing(self):
        result = get_market_change(self.conn, self.symbols, START, END, exclude="ZZZUSDT")
        self.assertEqual((result.n, result.total), (4, 4))
        self.assertAlmostEqual(result.median_pct, 25.0)

    def test_half_threshold_counts_without_excluded_coin(self):
        conn = make_conn()
        add_prices(conn, "AUSDT", 100, 110)
        add_prices(conn, "BUSDT", 100, 120)
        symbols = ["AUSDT", "BUSDT", "CUSDT", "DUSDT"]  # prices for 2 of 4: exactly half, enough
        self.assertIsNotNone(get_market_change(conn, symbols, START, END).median_pct)
        # with A excluded 1 of 3 remain: less than half, refused
        result = get_market_change(conn, symbols, START, END, exclude="AUSDT")
        self.assertIsNone(result.median_pct)
        self.assertEqual((result.n, result.total), (1, 3))
        self.assertIn("1 из 3", result.reason)

    def test_no_points_at_all(self):
        result = get_market_change(make_conn(), self.symbols, START, END)
        self.assertIsNone(result.median_pct)
        self.assertEqual((result.n, result.total), (0, 4))
        self.assertIn("ни по одной из 4", result.reason)
        self.assertNotIn("только по 0", result.reason)


class MarketVolumeTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()
        # ratios 1, 2, 3 and an outlier 100
        for symbol, ratio in (("AUSDT", 1), ("BUSDT", 2), ("CUSDT", 3), ("DUSDT", 100)):
            add_volume(self.conn, symbol, ratio)
        self.symbols = ["AUSDT", "BUSDT", "CUSDT", "DUSDT"]

    def test_without_exclude(self):
        result = get_market_volume_ratio(self.conn, self.symbols, DAY, AVG_DAYS)
        self.assertEqual((result.n, result.total), (4, 4))
        self.assertAlmostEqual(result.median_ratio, 2.5)

    def test_exclude_removes_coin_from_median_and_total(self):
        result = get_market_volume_ratio(self.conn, self.symbols, DAY, AVG_DAYS, exclude="DUSDT")
        self.assertEqual((result.n, result.total), (3, 3))
        self.assertAlmostEqual(result.median_ratio, 2.0)

    def test_foreign_ticker_excludes_nothing(self):
        result = get_market_volume_ratio(self.conn, self.symbols, DAY, AVG_DAYS, exclude="ZZZUSDT")
        self.assertEqual((result.n, result.total), (4, 4))

    def test_average_excludes_the_day_itself(self):
        # prior days are 100 each; a huge day must not move its own average
        conn = make_conn()
        add_volume(conn, "AUSDT", 50)
        result = get_market_volume_ratio(conn, ["AUSDT"], DAY, AVG_DAYS)
        self.assertAlmostEqual(result.median_ratio, 50.0)

    def test_short_history_drops_the_coin(self):
        conn = make_conn()
        add_volume(conn, "AUSDT", 2)
        add_volume(conn, "BUSDT", 4)
        add_volume(conn, "CUSDT", 9, prior_days=AVG_DAYS - 1)  # one day short
        result = get_market_volume_ratio(conn, ["AUSDT", "BUSDT", "CUSDT"], DAY, AVG_DAYS)
        self.assertEqual((result.n, result.total), (2, 3))
        self.assertAlmostEqual(result.median_ratio, 3.0)

    def test_missing_day_and_half_threshold(self):
        conn = make_conn()
        add_volume(conn, "AUSDT", 2)
        conn.execute("DELETE FROM binance_daily_volume WHERE symbol = 'AUSDT' AND date = ?", (DAY,))
        add_volume(conn, "BUSDT", 4)
        symbols = ["AUSDT", "BUSDT", "CUSDT"]  # only B has the day: 1 of 3
        result = get_market_volume_ratio(conn, symbols, DAY, AVG_DAYS)
        self.assertIsNone(result.median_ratio)
        self.assertIn("1 из 3", result.reason)
        # excluding C leaves 1 of 2: exactly half, enough
        self.assertAlmostEqual(get_market_volume_ratio(conn, symbols, DAY, AVG_DAYS, exclude="CUSDT").median_ratio, 4.0)

    def test_no_data_at_all(self):
        result = get_market_volume_ratio(make_conn(), ["AUSDT"], DAY, AVG_DAYS)
        self.assertIsNone(result.median_ratio)
        self.assertEqual((result.n, result.total), (0, 1))
        self.assertIn("ни по одной из 1", result.reason)
        self.assertNotIn("только по 0", result.reason)


class DailyVolumeCollectionTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_conn()

    def store_last_day(self, symbol: str, days_ago: int) -> None:
        last = date.today() - timedelta(days=days_ago)
        self.conn.execute("INSERT INTO binance_daily_volume VALUES (?, ?, 1, ?)", (symbol, last.isoformat(), END))

    def test_no_rows_means_full_history(self):
        self.assertEqual(daily_volume_fetch_limit(self.conn, ["AUSDT"]), DAILY_VOLUME_FIRST_RUN_DAYS)

    def test_lagging_coin_sets_the_limit(self):
        self.store_last_day("AUSDT", 1)
        self.store_last_day("BUSDT", 5)
        self.assertEqual(daily_volume_fetch_limit(self.conn, ["AUSDT", "BUSDT"]), 7)

    def test_new_coin_in_watchlist_gets_full_history(self):
        self.store_last_day("AUSDT", 1)
        self.assertEqual(daily_volume_fetch_limit(self.conn, ["AUSDT", "NEWUSDT"]), DAILY_VOLUME_FIRST_RUN_DAYS)

    def test_removed_coin_is_ignored(self):
        self.store_last_day("AUSDT", 1)
        self.store_last_day("OLDUSDT", 40)  # no longer in the watchlist
        self.assertEqual(daily_volume_fetch_limit(self.conn, ["AUSDT"]), 3)

    def test_open_candle_is_dropped(self):
        now_ms = 1_800_000_000_000
        day_ms = 86_400_000
        closed = [now_ms - 2 * day_ms, 0, 0, 0, 0, 0, now_ms - day_ms - 1, "123.5"]
        still_open = [now_ms - day_ms, 0, 0, 0, 0, 0, now_ms + 1, "7"]
        with mock.patch.object(binance_futures, "fetch_klines", return_value=[closed, still_open]),                 mock.patch.object(binance_futures.time, "time", return_value=now_ms / 1000):
            records = binance_futures.collect_daily_volume(["AUSDT"], limit=2)
        self.assertEqual([(r["symbol"], r["quote_volume_usdt"]) for r in records], [("AUSDT", 123.5)])


class CardTests(unittest.TestCase):
    SYMBOLS = ["AAVEUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT"]
    CONFIG = {"watchlist": {"binance_futures_symbols": SYMBOLS}}

    def setUp(self):
        self.conn = make_conn()
        self.conn.execute(
            "INSERT INTO defillama_snapshots (protocol_slug, symbol, name, fetched_at) "
            "VALUES ('aave-v2', 'AAVE', 'Aave V2', ?)", (END,),
        )

    def test_price_card_coin_in_list_is_left_out(self):
        for date_, price in (("2026-10-15", 100.0), ("2026-10-22", 105.0)):
            self.conn.execute(
                "INSERT INTO coingecko_price_history (gecko_id, protocol_slug, date, fetched_at, price) "
                "VALUES ('aave', 'aave-v2', ?, ?, ?)", (date_, END, price),
            )
        # AAVE +900% would drag the median if it were counted; others +10, +20, +30
        for symbol, end in (("AAVEUSDT", 1000), ("BTCUSDT", 110), ("ETHUSDT", 120), ("SOLUSDT", 130)):
            add_prices(self.conn, symbol, 100, end)
        contribution = SimpleNamespace(
            signal_name="revenue_price_gap",
            details={"revenue_window_first": "2026-10-15", "revenue_window_last": "2026-10-21"},
        )
        text = _market_background_line(contribution, "aave-v2", self.conn, self.CONFIG)
        self.assertIn("монета AAVE +5,0%, рынок +20,0%.", text)
        self.assertIn("по 3 монетам Binance; AAVE в расчёт рынка не входит", text)
        self.assertIn("с 15 октября 00:00 по 22 октября 00:00", text)

    def test_price_window_follows_revenue_window_not_cap_dates(self):
        # prices exist only for the revenue window's moments (17 and 23 Oct);
        # the cap snapshot dates (16 and 23) must not be used
        for date_, price in (("2026-10-16", 50.0), ("2026-10-17", 100.0), ("2026-10-23", 110.0)):
            self.conn.execute(
                "INSERT INTO coingecko_price_history (gecko_id, protocol_slug, date, fetched_at, price) "
                "VALUES ('aave', 'aave-v2', ?, ?, ?)", (date_, END, price),
            )
        for symbol in self.SYMBOLS[1:]:
            for moment, price in (("2026-10-17T00:00:00+00:00", 100), ("2026-10-23T00:00:00+00:00", 120)):
                self.conn.execute(
                    "INSERT INTO binance_oi_snapshots (symbol, fetched_at, oi_timestamp, oi, oi_value_usdt, price) "
                    "VALUES (?, ?, ?, 1, 1, ?)", (symbol, END, moment, price),
                )
        contribution = SimpleNamespace(
            signal_name="revenue_price_gap",
            details={"revenue_window_first": "2026-10-17", "revenue_window_last": "2026-10-22",
                     "mcap_date_earlier": "2026-10-16", "mcap_date_latest": "2026-10-23"},
        )
        text = _market_background_line(contribution, "aave-v2", self.conn, self.CONFIG)
        self.assertIn("с 17 октября 00:00 по 23 октября 00:00", text)
        self.assertIn("монета AAVE +10,0%, рынок +20,0%.", text)

    def test_price_card_without_exact_coin_price_says_so(self):
        # price on the first day only: the nearby 16th must not stand in for the 23rd
        for date_ in ("2026-10-17", "2026-10-22"):
            self.conn.execute(
                "INSERT INTO coingecko_price_history (gecko_id, protocol_slug, date, fetched_at, price) "
                "VALUES ('aave', 'aave-v2', ?, ?, 100)", (date_, END),
            )
        contribution = SimpleNamespace(
            signal_name="revenue_price_gap",
            details={"revenue_window_first": "2026-10-17", "revenue_window_last": "2026-10-22"},
        )
        text = _market_background_line(contribution, "aave-v2", self.conn, self.CONFIG)
        self.assertIn("сравнить с рынком нельзя", text)

    def test_market_note_is_one_short_line(self):
        self.assertEqual(
            _market_note(25, 25, "RUNE"),
            "Рынок — серединное значение (половина монет выше, половина ниже) "
            "по 25 монетам Binance; RUNE в расчёт рынка не входит.",
        )
        self.assertIn("Данные нашлись у 20 из 25.", _market_note(20, 25, "RUNE"))
        self.assertIn("Эта монета в расчёт рынка не входит", _market_note(25, 25, "эта монета"))

    def test_revenue_card_says_what_was_noticed(self):
        details = {
            "mcap_date_latest": "2026-10-23", "mcap_date_earlier": "2026-10-16",
            "revenue_window_first": "2026-10-17", "revenue_window_last": "2026-10-23",
            "revenue_window_count": 7, "lookback_days": 7, "revenue_growth_pct": 40.0,
            "baseline_window_first": "2026-07-26", "baseline_window_last": "2026-10-23",
            "baseline_window_count": 90, "mcap_growth_pct": 2.0,
        }
        lines = _digest_text_revenue_price_gap(details, {}).splitlines()
        self.assertIn("Медиана дневной выручки за 17–23 октября", lines[0])
        self.assertEqual(lines[1], _GAP_SENTENCE)
        self.assertNotIn("снизилась", _GAP_SENTENCE)
        # the sentence states the signal's condition (cap grew LESS than revenue),
        # so it must not claim the cap "did not follow" when it is +9%
        self.assertEqual(_GAP_SENTENCE, "Выручка протокола выросла сильнее, чем капитализация его токена.")
        for mcap_growth in (9.0, -2.1):
            with self.subTest(mcap_growth=mcap_growth):
                text = _digest_text_revenue_price_gap({**details, "mcap_growth_pct": mcap_growth}, {})
                self.assertEqual(text.splitlines()[1], _GAP_SENTENCE)
        # the cap line (its own window) is the LAST line, after the daily
        # revenue, with no remark about the market comparison
        self.assertEqual(lines[-1], "Капитализация токена с 16 по 23 октября изменилась на +2,0%.")
        self.assertFalse(any("не участвует" in line for line in lines))

    def test_cap_line_is_not_next_to_price_market_block(self):
        details = {
            "mcap_date_latest": "2026-10-23", "mcap_date_earlier": "2026-10-16",
            "revenue_window_first": "2026-10-17", "revenue_window_last": "2026-10-23",
            "revenue_window_count": 7, "lookback_days": 7, "revenue_growth_pct": 40.0,
            "baseline_window_first": "2026-07-26", "baseline_window_last": "2026-10-23",
            "baseline_window_count": 90, "mcap_growth_pct": -2.1,
        }
        block = "Цена монеты и рынка, с 17 октября 00:00 по 24 октября 00:00 (время UTC): монета +1,0%, рынок +2,0%."
        lines = _digest_text_revenue_price_gap(details, {}, block).splitlines()
        self.assertEqual(lines[2:5], ["", block, ""])
        self.assertIn("изменилась на −2,1%", lines[-1])
        self.assertTrue(all("Капитализация" not in line for line in lines[:-1]))

    def test_freshness_label_is_about_firing_run_not_revenue(self):
        contribution = SimpleNamespace(signal_name="revenue_price_gap", triggered_at="2026-10-25T09:00:00+00:00")
        candidate = SimpleNamespace(protocol_slug="rune", contributions=[contribution])
        episode = SimpleNamespace(
            signal_name="revenue_price_gap", protocol_slug="rune",
            episode_start_date="2026-10-06", episode_end_date="2026-10-25",
        )
        self.assertEqual(_freshness_label(candidate, [episode]), "срабатывает с 6 октября, была и вчера")
        self.assertEqual(_freshness_label(candidate, []), "первый день срабатывания")

    def test_volume_card_coin_in_list_is_left_out(self):
        for symbol, ratio in (("AAVEUSDT", 90), ("BTCUSDT", 1), ("ETHUSDT", 2), ("SOLUSDT", 3)):
            add_volume(self.conn, symbol, ratio)
        contribution = SimpleNamespace(
            signal_name="volume_breakout",
            details={"date": "2026-10-22", "volume_ratio": 3.46, "volume_avg_lookback_days": AVG_DAYS},
        )
        text = _volume_background_line(contribution, "aave-v2", self.conn, self.CONFIG)
        self.assertIn("монета AAVE — в 3,5 раза, рынок — в 2,0 раза.", text)
        self.assertIn("во сколько раз к среднему", text)
        self.assertIn("с 21 октября 00:00 по 22 октября 00:00", text)
        self.assertIn("по 3 монетам Binance; AAVE в расчёт рынка не входит", text)

    def test_volume_card_without_market_data_says_so(self):
        contribution = SimpleNamespace(
            signal_name="volume_breakout",
            details={"date": "2026-10-22", "volume_ratio": 3.46, "volume_avg_lookback_days": AVG_DAYS},
        )
        text = _volume_background_line(contribution, "aave-v2", self.conn, self.CONFIG)
        self.assertIn("Рынок посчитать не удалось", text)
        self.assertIn("Сравнить с рынком нельзя", text)
        self.assertIn("ни по одной из 3 монет", text)  # AAVE left out of 4: 3 remain
        self.assertNotIn("только по 0", text)

    def test_volume_breakout_title_has_direction_and_decimal_comma(self):
        details = {"resistance_lookback_days": 180, "resistance_level": 2.484,
                   "volume_ratio": 3.46, "volume_avg_lookback_days": 30}
        text = _digest_text_volume_breakout(details, {})
        self.assertIn("Цена поднялась выше уровня, державшегося 180 дней", text)
        self.assertIn("на объёме в 3,5 раза выше среднего за 30 дней", text)
        self.assertNotIn("пробила", text)
        details["volume_ratio"] = 12.04
        self.assertIn("в 12,0 раза", _digest_text_volume_breakout(details, {}))
        # the decimal comma is for the ratio only: the level's price keeps its point
        self.assertIn("$2.48", text)

    def test_volume_card_has_no_explanatory_sentence(self):
        for symbol, ratio in (("BTCUSDT", 1), ("ETHUSDT", 2), ("SOLUSDT", 3)):
            add_volume(self.conn, symbol, ratio)
        contribution = SimpleNamespace(
            signal_name="volume_breakout",
            details={"date": "2026-10-22", "volume_ratio": 3.46, "volume_avg_lookback_days": AVG_DAYS},
        )
        text = _volume_background_line(contribution, "aave-v2", self.conn, self.CONFIG)
        self.assertNotIn("CoinGecko", text)

    def test_volume_card_does_not_raise_on_broken_details(self):
        contribution = SimpleNamespace(signal_name="volume_breakout", details={})
        with self.assertLogs("notifications.telegram_bot", level="ERROR"):
            text = _volume_background_line(contribution, "aave-v2", self.conn, self.CONFIG)
        self.assertIn("сравнить с рынком нельзя", text)


if __name__ == "__main__":
    unittest.main()
