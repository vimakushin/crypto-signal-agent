"""Daily Telegram digest of ranked candidates (MVP.md checklist item 5:
"Сводка приходит в Telegram. Раз в сутки, автоматически, с расшифровкой
скора.").

This module does three things, and nothing else:
  1. Reads the Telegram bot token/chat id from .env (see _read_env_file) -
     never from code, never from config.yaml (CLAUDE.md: secrets only in
     .env, never in git).
  2. Calls scoring/ranker.py's rank_candidates() (already-tested, already
     doing the actual aggregation - this module does not re-derive scores)
     and turns its output into a short, human-readable text.
  3. Sends that text to Telegram via a plain POST to the Bot API's
     sendMessage endpoint.

Like scoring/ranker.py, signals/*.py and every other module in this
project, this only ranks/formats already-collected data for a human to
review manually - it never recommends or executes a trade (TZ section 1/9).
No parse_mode is used (see send_telegram_message's docstring) - the text is
plain, unformatted Telegram text, deliberately, so a coin name or a number
from the database can never accidentally break Telegram's Markdown/HTML
parsing.
"""
from __future__ import annotations

import logging
import math
import re
import subprocess
import sys
import time
from datetime import date, datetime, time as time_of_day, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import NamedTuple

import requests
import yaml

# Same sys.path fix as scoring/ranker.py and scripts/backfill_history.py -
# this module lives one level below the project root, so `from scoring...`
# below would fail with ModuleNotFoundError without it when this file is run
# directly (`python notifications/telegram_bot.py`), not imported as a
# package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scoring.ranker import (  # noqa: E402
    CandidateScore,
    default_since_by_signal,
    rank_candidates,
    signal_weights_from_config,
)
from storage.db import (  # noqa: E402
    BackupProblem,
    backup_problem,
    get_collection_notice_dates,
    get_connection,
    get_data_date,
    get_digest_send_dates,
    get_market_change,
    get_market_volume_ratio,
    init_db,
    record_collection_notice_sent,
    record_digest_sent,
    sources_without_fetch_today,
)
from storage.episodes import _build_all_episodes  # noqa: E402

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"

TELEGRAM_API_URL_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"

# Telegram hard-rejects any sendMessage text longer than this (Bot API
# limit) - _truncate_for_telegram() below cuts a longer digest down to this
# length rather than letting the API call fail outright just because a busy
# day produced a long list of candidates.
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TRUNCATION_SUFFIX = "\n...(обрезано)"

# Same retry/backoff shape as collectors/defillama.py's _get_with_retry -
# 3 attempts, a growing pause between them, log-and-don't-swallow on final
# failure. Reused here even though this is an outbound notification, not a
# data collector, because it hits the same kind of unreliable network the
# rest of this project already assumes (CLAUDE.md: "Сеть ненадёжна: retry с
# backoff").
MAX_RETRIES = 3
BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 30

# How long to wait for a stale source to get today's collection before
# building the digest anyway. After a late PC start Task Scheduler launches the
# collection and the digest at the same moment; without waiting the digest
# would go out with "data are stale" minutes before the fresh data appears.
# Measured in logs/scheduler_defillama.log (28 Sep - 5 Oct 2026): the whole
# DeFiLlama + CoinGecko cycle takes 6.7-8.2 minutes (CoinGecko's rate limit is
# most of it); 30 minutes is ~3.5x that, and under the task's 1-hour limit.
FRESH_DATA_MAX_WAIT_MINUTES = 30
FRESH_DATA_POLL_SECONDS = 60

# Matches the bot token embedded in the request URL's path, e.g.
# "/bot123456789:AAExampleTokenText" - see _redact_token below.
_TOKEN_IN_URL_RE = re.compile(r"/bot\d+:[A-Za-z0-9_-]+")


def _redact_token(text: str) -> str:
    """Strip a Telegram bot token out of error text before it's logged.

    requests embeds the full request URL - including the token, which
    Telegram's Bot API puts directly in the URL path - in the string form
    of a RequestException/HTTPError. Logged as-is, that would put the
    token in plain text into logs/telegram.log on the very first failed
    send (confirmed live during code review) - logs/ isn't in git, but a
    secret sitting in an unencrypted file on disk that a non-technical
    user might paste somewhere while asking for help is still a real
    leak, not a hypothetical one.

    Args:
        text: error text (typically str(exc)) that may contain the full
            sendMessage URL with the token in it.

    Returns:
        The same text with any "/bot<token>" path segment replaced by
        "/bot***REDACTED***". Text without a token in it is returned
        unchanged.
    """
    return _TOKEN_IN_URL_RE.sub("/bot***REDACTED***", text)


def _read_env_file(path: Path | str = DEFAULT_ENV_PATH) -> dict[str, str]:
    """Minimal .env parser - the first (and so far only) place in this
    project that reads a .env file, so a full dependency (python-dotenv) for
    two lines of parsing was deliberately skipped (see this task's
    instructions).

    Format: one KEY=VALUE per line. Blank lines and lines starting with '#'
    are skipped. No quoting/escaping support - values are taken as-is after
    the first '='. This is intentionally simple; it only needs to handle a
    bot token and a chat id, neither of which contains '=' or needs quoting.

    Args:
        path: path to the .env file. Defaults to <project root>/.env.

    Returns:
        {KEY: VALUE} for every KEY=VALUE line found. Empty dict if the file
        doesn't exist or has no such lines - the caller decides whether a
        missing key is fatal (see main()), this function itself never
        raises just because the file or a key is absent.
    """
    env_path = Path(path)
    if not env_path.exists():
        logger.error(
            ".env not found at %s - TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID can't be read; "
            "copy .env.example to .env and fill in the real values",
            env_path,
        )
        return {}

    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            logger.warning(".env: ignoring malformed line (no '='): %r", raw_line)
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def _truncate_for_telegram(text: str) -> str:
    """Cut `text` down to Telegram's sendMessage length limit if needed,
    appending TRUNCATION_SUFFIX so a shortened digest is never mistaken for
    a complete one.

    Args:
        text: the full digest text.

    Returns:
        `text` unchanged if it already fits within
        TELEGRAM_MAX_MESSAGE_LENGTH, otherwise a truncated copy that fits
        (including the suffix) within that limit.
    """
    if len(text) <= TELEGRAM_MAX_MESSAGE_LENGTH:
        return text
    cutoff = TELEGRAM_MAX_MESSAGE_LENGTH - len(TRUNCATION_SUFFIX)
    return text[:cutoff] + TRUNCATION_SUFFIX


def send_telegram_message(token: str, chat_id: str, text: str, what: str = "digest") -> bool:
    """Send `text` to `chat_id` via the Telegram Bot API's sendMessage,
    with retry-with-backoff (same shape as
    collectors/defillama.py's _get_with_retry).

    Deliberately does NOT set parse_mode (neither Markdown nor HTML): a
    coin name or a number pulled from the database can contain characters
    (e.g. '_', '*', '<') that would otherwise be interpreted as formatting
    and either break the message or fail the API call outright. Plain text
    sidesteps that entirely.

    Args:
        token: Telegram bot token (from .env's TELEGRAM_BOT_TOKEN).
        chat_id: Telegram chat id to send to (from .env's TELEGRAM_CHAT_ID).
        text: message text. Truncated to Telegram's length limit if needed
            (see _truncate_for_telegram).
        what: what is being sent, for the log only ("сводка за 26 октября",
            "уведомление о сборе") - so the log does not call a notice a digest.

    Returns:
        True if the message was sent successfully, False if all
        MAX_RETRIES attempts failed (already logged at ERROR level in that
        case - the caller is expected to exit non-zero, not to log again).
    """
    url = TELEGRAM_API_URL_TEMPLATE.format(token=token)
    payload = {"chat_id": chat_id, "text": _truncate_for_telegram(text)}

    last_error_text: str | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, data=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            logger.info("Telegram: %s sent successfully (attempt %s/%s)", what, attempt, MAX_RETRIES)
            return True
        except requests.exceptions.HTTPError as exc:
            last_error_text = _redact_token(str(exc))
            status_code = exc.response.status_code if exc.response is not None else None
            # A 4xx (bad token, bad chat_id, etc.) is a permanent error -
            # retrying won't fix it, it only wastes time and logs the same
            # failure three times. 429 ("too many requests") is the one 4xx
            # where Telegram itself is asking for a retry, so it still goes
            # through the normal backoff loop below.
            if status_code is not None and 400 <= status_code < 500 and status_code != 429:
                logger.error(
                    "Telegram sendMessage failed with a non-retryable client error "
                    "(attempt %s/%s, HTTP %s): %s",
                    attempt, MAX_RETRIES, status_code, last_error_text,
                )
                return False
            logger.warning(
                "Telegram sendMessage failed (attempt %s/%s): %s",
                attempt, MAX_RETRIES, last_error_text,
            )
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)
        except requests.RequestException as exc:
            last_error_text = _redact_token(str(exc))
            logger.warning(
                "Telegram sendMessage failed (attempt %s/%s): %s",
                attempt, MAX_RETRIES, last_error_text,
            )
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)

    logger.error(
        "Telegram sendMessage permanently failed after %s attempts: %s",
        MAX_RETRIES, last_error_text,
    )
    return False


# Per-signal formatters: each turns one SignalContribution's `.details` dict
# (as saved by main.py's json.dumps(...) calls at the moment the signal
# fired - see signals/*.py and main.py) into one human-readable line. Kept
# as a dict of small functions, one per signal_name, rather than one long
# if/elif chain, so adding a new signal's formatter later doesn't require
# touching the others (CLAUDE.md: adding a signal shouldn't require editing
# unrelated code).
def _date_range_ru(first: date, last: date) -> str:
    """'12–18 октября' (same month) or '28 сентября – 4 октября'."""
    if (first.year, first.month) == (last.year, last.month):
        return f"{first.day}–{_date_ru(last)}"
    return f"{_date_ru(first)} – {_date_ru(last)}"


class RevenueMarkTexts(NamedTuple):
    """Human-readable pieces for one revenue_price_gap candidate (no final
    punctuation, so the callers join them as they need).

    marks: lower-case fragments, empty when nothing is marked.
    daily_line: the seven daily revenue numbers with their date range, or
        None for events saved before these fields existed.
    check_failed: 'Сравнить с обычным для этой монеты нельзя: <reason>'
        (the reason text says itself whose limit it is), or None.
    """
    marks: list[str]
    daily_line: str | None
    check_failed: str | None


# All Russian wording of the revenue_price_gap marks lives in the next few
# lines (reason texts for a skipped check live in signals/revenue_price_gap.py
# next to the data they are stored with) - so an editor changes them here.
_MARK_PREFIX = "Что видно в дневных цифрах ниже: "
_MARK_SPREAD = (
    "второй самый крупный день в {week} раза больше второго самого мелкого "
    "(обычно у этой монеты — в {habitual} раза)"
)
_MARK_ZERO_DAYS = "в двух или больше из этих дней выручка нулевая или отрицательная"
_MARK_ONE_DAY = (
    "самый крупный день дал {share:.0f}% выручки этих дней "
    "(обычно у этой монеты — {habitual:.0f}%)"
)
_DAILY_LINE = "Выручка по дням, {dates}{note}, тыс. $: {numbers}"
# Headline sentence of the digest. Every date and number in it is the one the
# signal was really computed from (details_json: revenue_window_*,
# baseline_window_*, mcap_date_*).
_HEADLINE = (
    "Медиана дневной выручки за {revenue_dates}{revenue_note} на {growth:.0f}% выше, чем за "
    "{baseline_count} {baseline_days_word} ({baseline_dates}, включая эти дни)."
)
# The market cap is a different quantity from the price the market is compared
# with (RUNE 15-22 Oct 2026: price +6.8%, cap +9.1%), and its window is another
# one too (dates of the two DeFiLlama snapshots, not the revenue days) - so the
# two cannot be compared and must not stand side by side. Since 25 Oct 2026 the
# line is the LAST one of the card, after the daily revenue, with its own dates
# and no remark (first-user: the old "(в сравнении с рынком выше она не
# участвует)" made readers re-read; away from the price/market pair it is not needed).
_MCAP_LINE = "Капитализация токена {mcap_dates} изменилась на {mcap}%"
_BACKGROUND_TITLE = "Цена монеты и рынка, "
# What the signal compares, in one sentence: revenue growth against market-cap
# growth (the cap, not the price). The signal fires only when the cap grew by
# less than revenue did (or fell). 25 Oct 2026: "…выросла слабее выручки или
# снизилась" dropped - it repeated the cap line and stopped being exact if the
# cap threshold ever exceeded the revenue one; the shorter text is true both when
# the cap fell (-2.1%) and when it grew less than revenue. No verdict.
# Same day, second change: "…а капитализация его токена за ней не пошла" argued
# with the number below it when the cap was +9% (it did "go", just less). The
# signal's condition is exactly "cap growth < revenue growth" (and below the cap
# threshold; signals/revenue_price_gap.py), so the sentence says that: true for
# +9% and for a fall alike.
_GAP_SENTENCE = "Выручка протокола выросла сильнее, чем капитализация его токена."
_CHECK_FAILED = "Сравнить дневные цифры с обычными для этой монеты не получается: {reason}"


def _daily_numbers(values: list) -> str:
    """Daily revenue numbers as one compact string, always in thousands of
    dollars with one decimal (comma, so units never jump between digests);
    zero is '0', negatives carry the real minus sign, a day without data is
    '—'.
    """
    parts = []
    for v in values:
        if v is None:
            parts.append("—")
        elif v == 0:
            parts.append("0")
        else:
            parts.append(f"{v / 1000:.1f}".replace("-", "−").replace(".", ","))
    return " · ".join(parts)


def _revenue_mark_texts(details: dict, daily_note: str = "") -> RevenueMarkTexts:
    """Texts for revenue_price_gap's marks and the daily-numbers line.

    Args:
        details: the saved details_json dict of a revenue_price_gap event.
        daily_note: text put right after the dates of the daily-numbers line
            (the digest uses it to say which of these days the median covers).

    Returns:
        RevenueMarkTexts. Old events saved before the marks existed lack the
        keys and give ([], None, None). A missing key that a present mark
        needs raises KeyError, which the callers (_format_contribution,
        _digest_signal_text) catch.
    """
    marks: list[str] = []
    if details.get("shape_marked"):
        if details.get("week_unevenness") is not None:
            marks.append(_MARK_SPREAD.format(
                week=f"{details['week_unevenness']:.1f}".replace(".", ","),
                habitual=f"{details['habitual_unevenness']:.1f}".replace(".", ","),
            ))
        else:
            marks.append(_MARK_ZERO_DAYS)
    if details.get("single_day_marked"):
        marks.append(_MARK_ONE_DAY.format(
            share=details["window_max_day_share_pct"],
            habitual=details["habitual_max_day_share_pct"],
        ))

    daily_line = None
    if "shape_window_values" in details:
        dates = _date_range_ru(
            date.fromisoformat(details["shape_window_first"]),
            date.fromisoformat(details["shape_window_last"]),
        )
        daily_line = _DAILY_LINE.format(
            dates=dates, note=daily_note, numbers=_daily_numbers(details["shape_window_values"])
        )

    check_failed = None
    if details.get("shape_check_skipped"):
        check_failed = _CHECK_FAILED.format(reason=details["shape_note"])
    return RevenueMarkTexts(marks, daily_line, check_failed)


def _format_revenue_price_gap(details: dict) -> str:
    text = (
        f"revenue_price_gap: выручка {details.get('revenue_growth_pct'):+.1f}% "
        f"за {details.get('lookback_days')}д (порог {details.get('revenue_growth_threshold_pct')}%), "
        f"капитализация {details.get('mcap_growth_pct'):+.1f}% "
        f"(порог {details.get('mcap_reaction_threshold_pct')}%)"
    )
    texts = _revenue_mark_texts(details)
    notes = texts.marks + [
        part[0].lower() + part[1:] for part in (texts.daily_line, texts.check_failed) if part
    ]
    return f"{text} [{'; '.join(notes)}]" if notes else text


def _format_oi_divergence(details: dict) -> str:
    return (
        f"oi_divergence: открытый интерес {details.get('oi_growth_pct'):+.1f}% "
        f"за {details.get('lookback_hours'):.1f}ч (порог {details.get('oi_growth_threshold_pct')}%), "
        f"цена {details.get('price_change_pct'):+.1f}% "
        f"(порог {details.get('price_change_threshold_pct')}%)"
    )


def _format_volume_breakout(details: dict) -> str:
    return (
        f"volume_breakout: цена пробила уровень сопротивления за "
        f"{details.get('resistance_lookback_days')}д ({details.get('resistance_level'):.4g}), "
        f"объём {details.get('volume_ratio'):.1f}x от среднего за "
        f"{details.get('volume_avg_lookback_days')}д (порог {details.get('volume_ratio_threshold')}x)"
    )


_SIGNAL_FORMATTERS = {
    "revenue_price_gap": _format_revenue_price_gap,
    "oi_divergence": _format_oi_divergence,
    "volume_breakout": _format_volume_breakout,
}


def _format_contribution(contribution) -> str:
    """One readable line for a single SignalContribution, falling back to a
    generic "signal fired, no formatter" line for any signal_name not in
    _SIGNAL_FORMATTERS - so a new signal added later still shows up in the
    digest (just without a nicely worded breakdown yet) instead of silently
    vanishing or crashing digest formatting.
    """
    formatter = _SIGNAL_FORMATTERS.get(contribution.signal_name)
    if formatter is None:
        return f"{contribution.signal_name}: metric_value={contribution.metric_value}"
    try:
        return formatter(contribution.details)
    except (TypeError, ValueError, KeyError) as exc:
        # A malformed/incomplete details dict for one contribution must not
        # break the whole digest - fall back to the generic line for just
        # this one, same "one bad row doesn't sink everything else"
        # principle as collectors/defillama.py's per-point guards.
        logger.warning(
            "Could not format details for %s (protocol/ticker context lost here, "
            "see scoring/ranker.py output): %s",
            contribution.signal_name, exc,
        )
        return f"{contribution.signal_name}: metric_value={contribution.metric_value}"


# --- Telegram-only wording -------------------------------------------------
# The _format_* functions above are NOT used for the Telegram text: their
# output goes into observations.md (and web/app.py), and
# observations.backfill_seven_day_outcomes finds the data source by the
# substring "oi_divergence" in that text, so they must stay as they are.
# The digest reads for a person who doesn't know signal names or thresholds,
# so it gets its own plain-language functions below (no thresholds, no
# signal identifiers - only the measured facts).

_MONTHS_GENITIVE = (
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)


def _date_ru(day: date) -> str:
    """'2026-09-26' as a date -> '26 сентября' (genitive month, no year)."""
    return f"{day.day} {_MONTHS_GENITIVE[day.month - 1]}"


def _signed(value: float, digits: int = 1) -> str:
    """Number with an explicit sign, using the real minus sign U+2212."""
    text = f"{abs(value):.{digits}f}"
    return f"−{text}" if value < 0 else f"+{text}"


def _price_level(value: float) -> str:
    """Price with a dollar sign and ~3 significant digits, never in
    exponent notation: 0.4481 -> '$0.448', 0.0123 -> '$0.0123',
    145.8 -> '$146', 1234.5 -> '$1234'.
    """
    if value >= 100:
        return f"${value:.0f}"
    decimals = 2 - math.floor(math.log10(value))
    return f"${value:.{decimals}f}"


def _days_word(n: int) -> str:
    """'день' / 'дня' / 'дней' for n (81 день, 82 дня, 85 дней, 11 дней)."""
    if n % 10 == 1 and n % 100 != 11:
        return "день"
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return "дня"
    return "дней"


def _revenue_headline(details: dict) -> str:
    """The digest's headline sentence, naming the real windows. An event
    saved before the window dates were stored raises KeyError, which
    _digest_signal_text catches.
    """
    latest_day = date.fromisoformat(details["mcap_date_latest"])
    revenue_last = date.fromisoformat(details["revenue_window_last"])
    revenue_note = ""
    if details["revenue_window_count"] < details["lookback_days"]:
        revenue_note = f" ({details['revenue_window_count']} {_days_word(details['revenue_window_count'])}"
        # The window is meant to end on the last finished day (the day before
        # the snapshot); today's figure is never expected.
        expected_last = latest_day - timedelta(days=1)
        if revenue_last < expected_last:
            revenue_note += f"; за {_date_ru(expected_last)} данных ещё нет"
        revenue_note += ")"
    baseline_first = date.fromisoformat(details["baseline_window_first"])
    baseline_last = date.fromisoformat(details["baseline_window_last"])
    baseline_count = details["baseline_window_count"]
    baseline_days_word = _days_word(baseline_count)
    if baseline_count < (baseline_last - baseline_first).days + 1:
        baseline_days_word += " с данными"
    return _HEADLINE.format(
        revenue_dates=_date_range_ru(date.fromisoformat(details["revenue_window_first"]), revenue_last),
        revenue_note=revenue_note,
        growth=details["revenue_growth_pct"],
        baseline_count=baseline_count,
        baseline_days_word=baseline_days_word,
        baseline_dates=_date_range_ru(baseline_first, baseline_last),
    )


def _revenue_mcap_line(details: dict) -> str:
    """The market-cap sentence with its own dates (the dates of the two
    DeFiLlama snapshots the signal compared).
    """
    latest_day = date.fromisoformat(details["mcap_date_latest"])
    earlier_day = date.fromisoformat(details["mcap_date_earlier"])
    if (earlier_day.year, earlier_day.month) == (latest_day.year, latest_day.month):
        mcap_dates = f"с {earlier_day.day} по {_date_ru(latest_day)}"
    else:
        mcap_dates = f"с {_date_ru(earlier_day)} по {_date_ru(latest_day)}"
    return _MCAP_LINE.format(
        mcap_dates=mcap_dates, mcap=_signed(details["mcap_growth_pct"]).replace(".", ",")
    ) + "."


def _digest_text_revenue_price_gap(details: dict, config: dict, background: str = "") -> str:
    """Headline, the sentence about the point, the price/market block
    (`background`, if any), what is visible in the daily numbers (marks), the
    'cannot compare' line if any, the daily numbers, and the market-cap line
    last (its window differs from the price/market one, so it stands apart).

    The median covers the last `lookback_days` calendar days ending on the last
    finished UTC day (the day before the latest snapshot; since 25 Oct 2026 the
    collector does not store the unfinished current day, so normally 7 of 7;
    6 of 7 when a protocol's last finished day is not published yet), while the
    seven daily numbers always end on the last day that has
    data - they are the days the marks above them were computed on, so they
    cannot be cut to the median's days without contradicting the marks. When
    the two ranges differ, the daily line says which of its days the median
    covers.
    """
    first = date.fromisoformat(details["revenue_window_first"])
    last = date.fromisoformat(details["revenue_window_last"])
    shape_first = date.fromisoformat(details["shape_window_first"]) if "shape_window_first" in details else None
    note = ""
    if shape_first is not None and (shape_first, date.fromisoformat(details["shape_window_last"])) != (first, last):
        note = f" (в медиану выше вошли {_date_range_ru(first, last)})"
    texts = _revenue_mark_texts(details, note)
    lines = [_revenue_headline(details), _GAP_SENTENCE]
    if background:
        lines += ["", background, ""]
    if texts.marks:
        lines.append(_MARK_PREFIX + "; ".join(texts.marks) + ".")
    if texts.check_failed:
        lines.append(texts.check_failed + ".")
    if texts.daily_line:
        lines.append(texts.daily_line)
    lines.append(_revenue_mcap_line(details))
    return "\n".join(lines)


def _digest_text_volume_breakout(details: dict, config: dict) -> str:
    ratio = f"{details['volume_ratio']:.1f}".replace(".", ",")
    return (
        f"Цена поднялась выше уровня, державшегося {details['resistance_lookback_days']} дней "
        f"({_price_level(details['resistance_level'])}), на объёме в "
        f"{ratio} раза выше среднего за "
        f"{details['volume_avg_lookback_days']} дней."
    )


def _digest_text_oi_divergence(details: dict, config: dict) -> str:
    return (
        f"Открытый интерес по фьючерсам Binance вырос на {details['oi_growth_pct']:.0f}% "
        f"за {details['lookback_hours']:.0f} ч, а цена за это время изменилась на "
        f"{_signed(details['price_change_pct']).replace('.', ',')}%. "
        f"С рынком ниже сравнивается цена, а не рост интереса."
    )


_DIGEST_TEXTS = {
    "revenue_price_gap": _digest_text_revenue_price_gap,
    "volume_breakout": _digest_text_volume_breakout,
    "oi_divergence": _digest_text_oi_divergence,
}


class _PriceWindow(NamedTuple):
    """The two moments of a card's price comparison. coin_pct is the coin's
    own PRICE change between them (None if the coin's price is not in the
    database); start/end are ISO moments of the coin's own prices, so the
    market points are looked up next to exactly these. text is the window as
    shown to the reader, in UTC.
    """
    start: str
    end: str
    coin_pct: float | None
    text: str


def _moment_ru(iso: str) -> str:
    """'2026-10-15T00:00:00+00:00' -> '15 октября 00:00' (UTC)."""
    moment = datetime.fromisoformat(iso).astimezone(timezone.utc)
    return f"{_date_ru(moment.date())} {moment:%H:%M}"


def _window_text(start: str, end: str) -> str:
    return f"с {_moment_ru(start)} по {_moment_ru(end)} (время UTC)"


def _revenue_price_window(details: dict, conn, slug: str) -> _PriceWindow:
    """Price window of a revenue_price_gap card: the SAME days as the card's
    revenue (revenue_window_first .. revenue_window_last), from 00:00 UTC of
    the first day to 00:00 UTC of the day after the last one - so the revenue
    and the price in one card describe the same period (before 25 Oct 2026 the
    price used the market-cap snapshot dates, a day or two off). A daily
    CoinGecko price is the point of 00:00 UTC of its date (checked 22 Oct 2026
    against the live market_chart answer: every stored value equals the 00:00
    point), so these moments are the dates `first` and `last + 1` of
    coingecko_price_history; Binance has an hourly point at every 00:00, so
    the market is looked up at exactly the same moments. A date with no price
    row (one of the 25 protocols is missing on many days) gives coin_pct None
    - never zero and never a neighbouring day. The market-cap line keeps the
    snapshot dates; it is a different quantity (see _MCAP_LINE).
    """
    first = date.fromisoformat(details["revenue_window_first"])
    after_last = date.fromisoformat(details["revenue_window_last"]) + timedelta(days=1)
    prices = []
    for day in (first, after_last):
        row = conn.execute(
            "SELECT price FROM coingecko_price_history WHERE protocol_slug = ? AND date = ? AND price > 0",
            (slug, day.isoformat()),
        ).fetchone()
        prices.append(row["price"] if row else None)
    start = f"{first.isoformat()}T00:00:00+00:00"
    end = f"{after_last.isoformat()}T00:00:00+00:00"
    coin_pct = None if None in prices else (prices[1] - prices[0]) / prices[0] * 100
    return _PriceWindow(start, end, coin_pct, _window_text(start, end))


def _oi_price_window(details: dict, conn, slug: str) -> _PriceWindow:
    """Price window of an oi_divergence card: the exact stored hourly points
    the signal's price_before / price_now come from (details has only the
    values, so they are found by value, as the digest always did). The coin's
    number is the signal's own price_change_pct. LookupError if a point is gone.
    """
    moments = []
    for oi_key, price_key in (("oi_before", "price_before"), ("oi_now", "price_now")):
        row = conn.execute(
            "SELECT oi_timestamp FROM binance_oi_snapshots WHERE symbol = ? AND oi = ? AND price = ? "
            "ORDER BY oi_timestamp DESC LIMIT 1",
            (slug, details[oi_key], details[price_key]),
        ).fetchone()
        if row is None:
            raise LookupError(f"no stored point for {slug} with {oi_key}/{price_key} from the signal")
        moments.append(row["oi_timestamp"])
    return _PriceWindow(moments[0], moments[1], details["price_change_pct"], _window_text(*moments))


# Signals that get a price background: the ones that show the coin's own % move.
# volume_breakout has its own background, of volume (_volume_background_line).
_PRICE_WINDOWS = {
    "revenue_price_gap": _revenue_price_window,
    "oi_divergence": _oi_price_window,
}


def _own_symbol(signal_name: str, slug: str, conn) -> str | None:
    """The card coin's Binance ticker, to leave it out of its own market
    background. oi_divergence: the slug is the ticker. Other signals: the
    protocol's token symbol + USDT, exact match only (of 25 protocols only
    AAVE matches the 26 tickers); None if the symbol is unknown.
    """
    if signal_name == "oi_divergence":
        return slug
    row = conn.execute(
        "SELECT symbol FROM defillama_snapshots WHERE protocol_slug = ? "
        "ORDER BY fetched_at DESC LIMIT 1", (slug,),
    ).fetchone()
    return f"{row['symbol']}USDT" if row and row["symbol"] else None


def _market_note(n: int, total: int, ticker: str) -> str:
    """One short line under a background block (price and volume alike): what
    'рынок' is, how many coins it is taken over, and that the card's own coin
    is not among them. `total` is already counted without the own coin; the
    sentence says so for a coin that is in the list and for one that is not.

    Args:
        n: coins that had data for this block.
        total: coins the market is taken over (the list without the own coin).
        ticker: the card coin's ticker, or 'эта монета' when it is unknown.
    """
    note = (
        f"Рынок — серединное значение (половина монет выше, половина ниже) "
        f"по {total} монетам Binance; {ticker[:1].upper() + ticker[1:]} в расчёт рынка не входит."
    )
    if n < total:
        note += f" Данные нашлись у {n} из {total}."
    return note


def _market_background_line(contribution, slug: str, conn, config: dict) -> str:
    """The block comparing the coin's PRICE change with the median price change
    of the watchlist coins over the same two moments. Both numbers are prices
    (the coin's market cap or open-interest growth is never compared). Facts
    only, no verdict. Any exception is logged and replaced by a plain line: the
    background must never cost the owner the digest.
    """
    try:
        symbols = config["watchlist"]["binance_futures_symbols"]
        window = _PRICE_WINDOWS[contribution.signal_name](contribution.details, conn, slug)
        if window.coin_pct is None:
            return "Цену монеты за эти даты взять не удалось, сравнить с рынком нельзя."
        # The card's own coin must not sit in its own background.
        own = _own_symbol(contribution.signal_name, slug, conn)
        ticker = own.removesuffix("USDT") if own else "эта монета"
        change = get_market_change(conn, symbols, window.start, window.end, exclude=own)
        if change.median_pct is None:
            return f"Рынок посчитать не удалось: {change.reason}. Сравнить с рынком нельзя."
        value = _signed(change.median_pct).replace(".", ",")
        coin = _signed(window.coin_pct).replace(".", ",")
        who = f"монета {ticker}" if own else "монета"
        return "\n".join([
            f"{_BACKGROUND_TITLE}{window.text}:",
            f"{who} {coin}%, рынок {value}%.",
            _market_note(change.n, change.total, ticker),
        ])
    except Exception:
        logger.exception("Market background failed for %s", contribution.signal_name)
        return "Рыночный фон посчитать не удалось, сравнить с рынком нельзя."


def _volume_background_line(contribution, slug: str, conn, config: dict) -> str:
    """The volume block of a volume_breakout card: the coin's volume ratio (the
    signal's own volume_ratio) next to the median of the same ratio over the
    watchlist coins, for the same day. A CoinGecko daily point dated D is the
    24 h that ended at 00:00 UTC of D (see storage/db.py, get_market_volume_ratio), so
    the market ratio is that of Binance day D-1, and the window is shown as
    such. Facts only, no verdict; any exception is logged and replaced by a
    plain line, as in _market_background_line.
    """
    try:
        symbols = config["watchlist"]["binance_futures_symbols"]
        details = contribution.details
        days = int(details["volume_avg_lookback_days"])
        ratio = details.get("volume_ratio")
        if ratio is None:
            return "Объём монеты за эти сутки взять не удалось, сравнить с рынком нельзя."
        signal_day = date.fromisoformat(details["date"])
        binance_day = (signal_day - timedelta(days=1)).isoformat()
        text = _window_text(f"{binance_day}T00:00:00+00:00", f"{signal_day.isoformat()}T00:00:00+00:00")
        own = _own_symbol(contribution.signal_name, slug, conn)
        ticker = own.removesuffix("USDT") if own else "эта монета"
        market = get_market_volume_ratio(conn, symbols, binance_day, days, exclude=own)
        if market.median_ratio is None:
            return f"Рынок посчитать не удалось: {market.reason}. Сравнить с рынком нельзя."
        who = f"монета {ticker}" if own else "монета"
        return "\n".join([
            f"Объём за сутки {text}, во сколько раз к среднему за {days} предыдущих суток:",
            "{} — в {} раза, рынок — в {} раза.".format(
                who, f"{ratio:.1f}".replace(".", ","), f"{market.median_ratio:.1f}".replace(".", ",")
            ),
            _market_note(market.n, market.total, ticker),
        ])
    except Exception:
        logger.exception("Volume background failed for %s", contribution.signal_name)
        return "Рыночный фон по объёму посчитать не удалось, сравнить с рынком нельзя."


def _digest_signal_text(contribution, config: dict, conn, slug: str) -> str:
    """Plain-language sentence for one SignalContribution, plus a market
    background line for signals that show the coin's own % move. Unknown
    signal_name or a broken details dict falls back to the generic line
    (and a warning) so one bad row can't sink the whole digest.
    """
    generic = f"{contribution.signal_name}: metric_value={contribution.metric_value}"
    text_fn = _DIGEST_TEXTS.get(contribution.signal_name)
    if text_fn is None:
        logger.warning("Digest: no plain-language text for signal %s", contribution.signal_name)
        return generic
    background = ""
    if contribution.signal_name in _PRICE_WINDOWS:
        background = _market_background_line(contribution, slug, conn, config)
    elif contribution.signal_name == "volume_breakout":
        background = _volume_background_line(contribution, slug, conn, config)
    try:
        if contribution.signal_name == "revenue_price_gap":
            return text_fn(contribution.details, config, background)
        text = text_fn(contribution.details, config)
    except (TypeError, ValueError, KeyError) as exc:
        logger.warning("Digest: could not format details for %s: %s", contribution.signal_name, exc)
        return generic
    if background:
        text += "\n\n" + background
    return text


def _coin_label(conn, candidate: CandidateScore) -> str:
    """'LDO (Lido)' from the latest defillama_snapshots row; only the ticker
    if the name is empty or equal to it; the Binance ticker without the USDT
    suffix when there is no DeFiLlama row but oi_divergence fired; otherwise
    the raw protocol_slug.
    """
    row = conn.execute(
        "SELECT symbol, name FROM defillama_snapshots WHERE protocol_slug = ? "
        "ORDER BY fetched_at DESC LIMIT 1",
        (candidate.protocol_slug,),
    ).fetchone()
    if row is not None and row["symbol"]:
        symbol, name = row["symbol"], row["name"]
        return f"{symbol} ({name})" if name and name != symbol else symbol
    if any(c.signal_name == "oi_divergence" for c in candidate.contributions):
        return candidate.protocol_slug.removesuffix("USDT")
    return candidate.protocol_slug


def _freshness_label(candidate: CandidateScore, episodes: list) -> str:
    """'первый день срабатывания' or 'срабатывает с <дата>, была и вчера',
    judged by the episode (run of consecutive days of the signal firing, from
    signal_events) each contribution belongs to. It says only what is counted:
    when the firing run began, NOT when the coin first appeared in a sent
    digest (digest_sends only starts on 5 Oct, signals on 10 Sep) and not how
    long the revenue has been high (the old "идёт с 6 октября" read as a
    revenue duration next to "6 дней").
    """
    starts = []
    for contribution in candidate.contributions:
        day = datetime.fromisoformat(contribution.triggered_at).date().isoformat()
        for episode in episodes:
            if (
                episode.signal_name == contribution.signal_name
                and episode.protocol_slug == candidate.protocol_slug
                and episode.episode_end_date == day
                and episode.episode_start_date < day
            ):
                starts.append(episode.episode_start_date)
    if not starts:
        return "первый день срабатывания"
    return f"срабатывает с {_date_ru(date.fromisoformat(min(starts)))}, была и вчера"


# Source key (storage.db.sources_without_fetch_today) -> the one word the owner
# sees for it, in the collection notice and in the stale-data warning alike.
_STALE_SOURCE_SHORT = {"defillama": "выручка", "coingecko": "цены", "binance": "фьючерсы"}


def _join_names(keys: list[str]) -> str:
    """'выручка', 'выручка и цены', 'выручка, цены и фьючерсы'."""
    if len(keys) <= 1:
        return "".join(keys)
    return ", ".join(keys[:-1]) + " и " + keys[-1]


def _stale_groups(stale_sources: dict[str, str | None]) -> list[tuple[str, str]]:
    """Stale sources grouped by their last collection day, so one date is not
    repeated for sources that share it.

    Returns:
        [(source names, 'последний сбор 23 октября' | 'ещё ни разу не собирались')].
    """
    by_when: dict[str, list[str]] = {}
    for source, last_iso in stale_sources.items():
        if last_iso is None:
            when = "ещё ни разу не собирались"
        else:
            when = f"последний сбор {_date_ru(datetime.fromisoformat(last_iso).astimezone().date())}"
        by_when.setdefault(when, []).append(_STALE_SOURCE_SHORT[source])
    return [(_join_names(names), when) for when, names in by_when.items()]


def _stale_warning(stale_sources: dict[str, str | None], data_date: date, today: date) -> str:
    """The warning paragraph that opens a late digest: one for a data date
    that is already behind the calendar because a source has no collection
    today. "No collection today" and not "older than 24 hours" on purpose:
    yesterday's collection ran late (12:44), today's failed, the digest runs
    at 12:30 - the data are 23.8 hours old and a 24-hour rule would stay
    silent about a source that has not updated today.

    Args:
        stale_sources: {source key: last fetched_at ISO (UTC) or None}.
        data_date: the date of the data the digest is named after.
        today: this run's local date, taken once at its start.

    Returns:
        One sentence saying what this digest is (late, for `data_date`) and
        which sources did not update; with no data at all - the short
        new-install explanation instead.
    """
    names = _join_names([_STALE_SOURCE_SHORT[s] for s in stale_sources])
    if len(stale_sources) == len(_STALE_SOURCE_SHORT) and all(v is None for v in stale_sources.values()):
        return (
            f"Внимание: {_date_ru(today)} не обновились {names}. "
            "Похоже, это новая установка: в базе ещё нет данных."
        )
    return (
        f"Эта сводка за {_date_ru(data_date)} пришла с опозданием: "
        f"за {_date_ru(today)} сбор не завершён — не обновились {names}."
    )


def _backup_warning(problem: BackupProblem) -> str:
    """Warning line for a backup that is not OK (incident 2026-10-06..22: the
    backup failed silently for two weeks, the owner sees only this digest).

    Args:
        problem: storage.db.backup_problem()'s non-None result.

    Returns:
        One paragraph: what is wrong with the backup copy and why it matters.
    """
    if problem.last_backup is not None:
        what = f"резервная копия данных не делалась с {_date_ru(problem.last_backup.date())} (обычно её делают каждый день)"
    else:
        what = "резервная копия данных не делается"
    # The reason stays in the log (first-user, 22 Oct 2026: a folder name in the
    # digest left the owner with nothing to do); here: what is at risk and what to do.
    return (
        f"Внимание: {what}. Если сломается компьютер, накопленная история пропадёт безвозвратно. "
        "Откройте Claude Code в папке проекта и попросите проверить резервную копию."
    )


def format_digest(
    results: list[CandidateScore],
    data_date: date,
    conn,
    config: dict,
    since_by_signal: dict[str, str],
    stale_sources: dict[str, str | None] | None = None,
    today: date | None = None,
) -> str:
    """Turn rank_candidates()'s output into the Telegram message text.

    Only facts, in plain language (TZ sections 1/9 - no advice). Coins with
    two or more different signals go first in a separate block, and only
    there the score is shown. An empty `results` still yields an explicit
    message - silence is indistinguishable from a broken pipeline.

    Args:
        results: rank_candidates()'s return value (order kept as is).
        data_date: the date of the data the digest is built on (not the
            clock at the moment of sending - see decide_action); it names
            the digest in the header.
        conn: open storage/db.py connection (coin names, episodes).
        config: parsed config.yaml (passed to every signal's wording function).
        since_by_signal: default_since_by_signal()'s cursor, for episodes.
        stale_sources: storage.db.sources_without_fetch_today()'s result at
            the moment of building. Non-empty -> the text starts with a
            warning paragraph, and an empty digest no longer claims the
            system works. Empty/None -> the text is exactly as without it.
        today: this run's local date, taken once at its start (the stale
            warning names it); required when `stale_sources` is non-empty.
            Independently, storage.db.backup_problem(config) is checked here:
            a bad backup puts its own warning first (before the stale one).

    Returns:
        The full text, not yet truncated (send_telegram_message() does that).
    """
    header = f"Сводка за {_date_ru(data_date)}"
    warning = f"{_stale_warning(stale_sources, data_date, today)}\n\n" if stale_sources else ""
    # The check must never cost the owner the digest itself (reviewer, 22 Oct
    # 2026): a broken config or a file vanishing mid-scan becomes a line saying
    # the check failed - not silence, which is how the backup failure of
    # 6-22 Oct went unnoticed.
    try:
        backup = backup_problem(config)
        backup_text = _backup_warning(backup) if backup else None
    except Exception:
        logger.exception("Backup check failed")
        backup_text = (
            "Внимание: не удалось проверить, делается ли резервная копия данных. Если так будет и завтра, "
            "откройте Claude Code в папке проекта и попросите проверить резервную копию."
        )
    if backup_text:
        warning = f"{backup_text}\n\n{warning}"
    if not results:
        if stale_sources:
            return (
                f"{warning}{header}: сигналов не видно, но по устаревшим данным "
                "судить об этом нельзя."
            )
        return f"{warning}{header}: сигналов нет. Система работает, данные собираются."

    episodes = _build_all_episodes(conn, since_by_signal)
    multi: list[str] = []
    single: list[str] = []
    for candidate in results:
        title = f"{_coin_label(conn, candidate)} · {_freshness_label(candidate, episodes)}"
        texts = [_digest_signal_text(c, config, conn, candidate.protocol_slug) for c in candidate.contributions]
        if len({c.signal_name for c in candidate.contributions}) >= 2:
            body = "\n".join(f"• {t}" for t in texts)
            multi.append(f"{title} · скор {candidate.total_score:.2f}\n{body}")
        else:
            single.append(f"{title}\n{texts[0]}")

    blocks = [warning + header]
    if multi:
        blocks.append(
            "Совпадение сигналов — несколько разных сигналов на одной монете\n"
            + "\n\n".join(multi)
        )
        if single:
            blocks.append("Остальные\n" + "\n\n".join(single))
    elif single:
        blocks.append("\n\n".join(single))
    return "\n\n".join(blocks)


def load_config(path: Path | str = PROJECT_ROOT / "config.yaml") -> dict:
    """Load config.yaml (duplicated from main.py/scoring/ranker.py's own
    load_config, not imported, for the same reason those scripts duplicate
    it: importing main.py would run its module-level logging setup as an
    import side effect).
    """
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _setup_logging() -> None:
    """Console + a rotating file in logs/telegram.log, same convention as
    scripts/backup_db.py's own _setup_logging - each distinct operation gets
    its own log file rather than sharing one.
    """
    log_dir = PROJECT_ROOT / "logs"
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        log_dir.mkdir(exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_dir / "telegram.log",
                maxBytes=5_000_000,
                backupCount=3,
                encoding="utf-8",
            )
        )
    except OSError as exc:
        print(
            f"WARNING: could not set up file logging at {log_dir} ({exc}); "
            "continuing with console-only logging.",
            file=sys.stderr,
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )


def wait_for_fresh_data(
    db_path: Path,
    today: date,
    poll_seconds: float = FRESH_DATA_POLL_SECONDS,
    max_wait_seconds: float = FRESH_DATA_MAX_WAIT_MINUTES * 60,
) -> dict[str, str | None]:
    """Wait until every source has a collection on `today`, but no longer
    than `max_wait_seconds`. Opens a short connection per check and closes it
    before sleeping, so the waiting digest never blocks the collector's writes.

    Args:
        db_path: the live database.
        today: the local date the digest is for.
        poll_seconds: pause between checks (a parameter so a test need not
            wait real minutes).
        max_wait_seconds: upper limit of the wait (same reason).

    Returns:
        sources_without_fetch_today()'s result at the last check: empty if
        everything became fresh, otherwise what is still stale.
    """
    deadline = time.monotonic() + max_wait_seconds
    announced = False
    while True:
        conn = get_connection(db_path)
        try:
            stale = sources_without_fetch_today(conn, today)
        finally:
            conn.close()
        remaining = deadline - time.monotonic()
        if not stale or remaining <= 0:
            return stale
        if not announced:
            logger.info(
                "Waiting up to %.0f s for fresh data; no collection today yet for: %s",
                max_wait_seconds, ", ".join(stale),
            )
            announced = True
        time.sleep(min(poll_seconds, remaining))


SEND_DIGEST = "send_digest"
SEND_NOTICE = "send_notice"
NOTHING = "nothing"


class Action(NamedTuple):
    """What one run must do. kind is SEND_DIGEST (day = the data date to name
    the digest after), SEND_NOTICE (days = how many days the collection has
    not run) or NOTHING."""
    kind: str
    day: date | None = None
    days: int | None = None


def decide_action(
    data_date: date,
    today: date,
    time_now: time_of_day,
    first_run_time: time_of_day,
    digests_sent: set[date],
    notices_sent: set[date],
) -> Action:
    """Pure decision of a digest run - no database, no clock inside.

    The rules (owner's decision of 26 Oct 2026, after the 01:58 run named a
    digest built on 25 Oct data "за 26 октября"):
      - a digest is named after the DATA date and goes out once per data date;
      - data from today, digest not sent yet -> send it;
      - data from an earlier day, that day's digest not sent yet -> send it
        under its own date (the caller adds the stale-data warning);
      - data from an earlier day and that day's digest already sent: before
        the first regular digest time of the day the collection has not had
        its chance yet, so stay silent (the night catch-up run at 01:28 must
        not raise a false alarm); from that time on send one short notice per
        calendar day that the collection did not run;
      - the notice is never sent for a date whose digest has not gone out, and
        fresh data (a new data date) always gives a normal digest, not a notice.

    Args:
        data_date: get_data_date()'s result.
        today: this run's local date, taken once at its start.
        time_now: local time of day of the run's start.
        first_run_time: schedule.telegram_run_time, the first regular digest
            time of the day.
        digests_sent: data dates whose digest already went out.
        notices_sent: calendar days whose notice already went out.

    Returns:
        The Action to perform.
    """
    if data_date not in digests_sent:
        return Action(SEND_DIGEST, day=data_date)
    if data_date >= today or time_now < first_run_time or today in notices_sent:
        return Action(NOTHING)
    return Action(SEND_NOTICE, days=(today - data_date).days)


# The task names in the Windows Task Scheduler and what each one collects. The
# DeFiLlama task also collects CoinGecko prices (main.py runs both), hence
# "выручка и цены".
_TASKS_FOR_NOTICE = (
    ("CryptoSignalAgent-DeFiLlama", "сбор выручки и цен", ("defillama", "coingecko")),
    ("CryptoSignalAgent-BinanceOI", "сбор фьючерсов", ("binance",)),
)

# Task Scheduler's own result codes (LastTaskResult) that are not a process exit
# code: 0x41301 "is running now", 0x41303 "has not run yet".
_TASK_RUNNING = "267009"
_TASK_NEVER_RAN = "267011"

def scheduled_tasks_line(stale_sources: dict[str, str | None], today: date) -> str | None:
    """One sentence about the collection tasks that answer for the stale
    sources, from the Windows Task Scheduler (Get-ScheduledTaskInfo). It
    states the fact and sets it against the data ("ran today, but no fresh
    data after it"); exit code 0 is never worded as "all fine": a collection
    with an unreachable source exits with 0 too. No guess about the reason.

    Args:
        stale_sources: sources_without_fetch_today()'s result; only the tasks
            of these sources are mentioned.
        today: this run's local date.

    Returns:
        E.g. 'Планировщик Windows: сбор выручки и цен запускался 26 октября
        в 13:00, но свежих данных после него нет.' None when no task is
        relevant, or on any failure (not Windows, no powershell, no such
        task, unreadable answer): the reason is logged and the caller simply
        leaves the line out.
    """
    names = ",".join(f"'{name}'" for name, _, _ in _TASKS_FOR_NOTICE)
    script = (
        f"foreach ($n in {names}) {{ $i = Get-ScheduledTaskInfo -TaskName $n -ErrorAction Stop; "
        "'{0:yyyy-MM-dd HH:mm}|{1}' -f $i.LastRunTime, $i.LastTaskResult }"
    )
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            capture_output=True, text=True, timeout=30, check=True,
        ).stdout.splitlines()
        lines = [line.strip() for line in out if line.strip()]
        if len(lines) != len(_TASKS_FOR_NOTICE):
            raise ValueError(f"unexpected answer: {out!r}")
        parts = []
        for (_, label, sources), line in zip(_TASKS_FOR_NOTICE, lines):
            if not any(s in stale_sources for s in sources):
                continue
            moment, _, code = line.partition("|")
            ran = datetime.strptime(moment, "%Y-%m-%d %H:%M")
            when = f"{_date_ru(ran.date())} в {ran:%H:%M}"
            # year < 2000: the scheduler's "never ran" placeholder date
            if code == _TASK_NEVER_RAN or ran.year < 2000:
                parts.append(f"{label} ещё ни разу не запускался")
            elif code == _TASK_RUNNING:
                parts.append(f"{label} выполняется сейчас (запущен {when})")
            elif code != "0":
                parts.append(f"{label} запускался {when} и завершился с кодом {code}")
            elif ran.date() == today:
                parts.append(f"{label} запускался {when}, но свежих данных после него нет")
            else:
                parts.append(f"{label}: последний запуск {when}, сегодня запуска не было")
    except Exception as exc:
        logger.warning("Could not read the scheduled tasks' last run, notice goes without that line: %s", exc)
        return None
    if not parts:
        return None
    return "Планировщик Windows: " + "; ".join(parts) + "."


_CHECK_COLLECTION = "Откройте Claude Code в папке проекта и попросите проверить, почему не идёт сбор данных."


def format_collection_notice(
    data_date: date, days: int, tasks_line: str | None, stale_sources: dict[str, str | None]
) -> str:
    """Short notice that the data collection did not run. Facts only: what did
    not update (with each source's own last collection date) and what did, why
    there is no new digest, and the scheduler line when it is known. No
    guess about the reason, no advice.

    Args:
        data_date: the date of the last data (the digest for it already went).
        days: today minus data_date, in days (>= 1).
        tasks_line: scheduled_tasks_line()'s result or None.
        stale_sources: sources_without_fetch_today()'s result; empty -> the
            notice cannot say which sources, and is a single line.

    Returns:
        The message text.
    """
    old = f"Последние данные — за {_date_ru(data_date)}, сводка по ним уже была."
    if not stale_sources:
        first = "Сегодня сбор данных пока не прошёл." if days == 1 else f"Сбор данных не проходит {days}-й день подряд."
        return "\n".join([f"{first} {old}"] + ([_CHECK_COLLECTION] if days >= 2 else []))
    # "Сегодня ... не обновились" on day one: the first notice goes out about
    # 13:00 and the DeFiLlama repeats run until 21:00, so it may still happen.
    stale = "; ".join(f"{names} — {when}" for names, when in _stale_groups(stale_sources))
    head = "Сегодня не обновились" if days == 1 else f"Сбор не проходит {days}-й день подряд: не обновились"
    fresh = [_STALE_SOURCE_SHORT[s] for s in _STALE_SOURCE_SHORT if s not in stale_sources]
    fresh_text = f" {_join_names(fresh).capitalize()} обновились." if fresh else ""
    lines = [
        f"{head} {stale}.{fresh_text}",
        "Новая сводка не уйдёт, пока не обновятся все три источника (выручка, цены, фьючерсы); "
        f"сводка по прежним данным (за {_date_ru(data_date)}) уже была.",
    ]
    if tasks_line:
        lines.append(tasks_line)
    if days >= 2:
        # A third day of failure must read differently from the first (owner, 26 Oct 2026).
        lines.append(_CHECK_COLLECTION)
    return "\n".join(lines)


def _first_run_time(config: dict) -> time_of_day:
    """schedule.telegram_run_time ('HH:MM') as a time of day."""
    return datetime.strptime(config["schedule"]["telegram_run_time"], "%H:%M").time()


def _read_state(db_path: Path, today: date) -> tuple[date, set[date], set[date], dict[str, str | None]]:
    """Everything the decision needs from the database, in one short
    connection: the data date, the dates whose digest went out, the days whose
    notice went out, and the sources with no collection today."""
    conn = get_connection(db_path)
    try:
        return (
            get_data_date(conn, today),
            get_digest_send_dates(conn),
            get_collection_notice_dates(conn),
            sources_without_fetch_today(conn, today),
        )
    finally:
        conn.close()


def _record(db_path: Path, record, day: date, what: str) -> bool:
    """Write the sent mark (`record` is record_digest_sent or
    record_collection_notice_sent) after a successful send. False (logged) if
    the mark could not be written - the next run may then send a duplicate."""
    try:
        conn = get_connection(db_path)
        try:
            record(conn, day, datetime.now().astimezone().isoformat(timespec="seconds"))
        finally:
            conn.close()
    except Exception:
        logger.exception(
            "The %s for %s WAS SENT to Telegram, but the mark could not be written to the "
            "database - the next scheduled run (in ~2 hours) may send a duplicate",
            what, day.isoformat(),
        )
        return False
    return True


def main() -> int:
    """One digest run: decide (decide_action), then send a digest, a short
    notice, or nothing.

    Steps: read .env and the database state; if the collection of today has
    not happened and the first regular digest time has passed, wait for fresh
    data; decide; for a digest: rank candidates, update observations.md,
    build the text (with a stale-data warning if needed), send, remember the
    data date. A failed send leaves no mark, so the next scheduled run (every
    2 hours, see config.yaml schedule) tries again.

    Returns:
        Process exit code: 0 - sent, or nothing to send; 1 - no credentials,
        the send failed, or the sent mark could not be written.
    """
    env_values = _read_env_file()
    token = env_values.get("TELEGRAM_BOT_TOKEN")
    chat_id = env_values.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.error(
            "TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID missing from .env - "
            "cannot send the Telegram digest. Copy .env.example to .env and "
            "fill in the real values (see README.md)."
        )
        return 1

    config = load_config()
    db_path = Path(config["storage"]["sqlite_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path

    # The clock is read ONCE: a run that crosses midnight (waiting for data)
    # must not change what "today" means halfway through.
    started = datetime.now()
    today = started.date()
    first_run = _first_run_time(config)
    # init_db: creates the tables of the marks if this run is the first after
    # an upgrade.
    init_db(db_path)
    data_date, digests_sent, notices_sent, stale_sources = _read_state(db_path, today)

    # Waiting makes sense only once the collection should have happened: before
    # the first regular digest time of the day (the night catch-up run) data
    # from yesterday are normal, not late.
    # If today's notice has already gone out there is nothing to wait for: every
    # later repeat would otherwise hang 30 minutes for nothing while the
    # collection stays dead.
    if data_date < today and started.time() >= first_run and today not in notices_sent:
        wait_for_fresh_data(db_path, today)
        data_date, digests_sent, notices_sent, stale_sources = _read_state(db_path, today)

    action = decide_action(data_date, today, started.time(), first_run, digests_sent, notices_sent)
    if action.kind == NOTHING:
        logger.info("Nothing to send (data date %s, today %s)", data_date.isoformat(), today.isoformat())
        return 0

    if action.kind == SEND_NOTICE:
        text = format_collection_notice(
            data_date, action.days, scheduled_tasks_line(stale_sources, today), stale_sources
        )
        logger.warning(
            "Collection did not run for %d day(s), last data %s - sending the notice",
            action.days, data_date.isoformat(),
        )
        if not send_telegram_message(token, chat_id, text, what="уведомление о сборе"):
            return 1
        return 0 if _record(db_path, record_collection_notice_sent, today, "collection notice") else 1

    if stale_sources:
        logger.warning(
            "Building the digest for %s on data without a collection today for: %s",
            data_date.isoformat(), ", ".join(stale_sources),
        )

    signal_weights = signal_weights_from_config(config)
    conn = get_connection(db_path)
    try:
        since_by_signal = default_since_by_signal(conn, config)
        results = rank_candidates(conn, signal_weights, since_by_signal)

        # Manual-labeling journal (TZ section 6, cut-down MVP version - see
        # notifications/observations.py's module docstring). Independent of
        # the Telegram send below: a failure here must not block the digest,
        # and a failed digest send below must not block this. It uses the
        # digest's DATA date, so the journal never disagrees with the digest.
        from notifications.observations import (
            append_new_observations,
            backfill_seven_day_outcomes,
        )

        try:
            append_new_observations(conn, results, data_date.isoformat())
        except Exception:
            logger.exception("observations.md: append_new_observations failed")
        try:
            backfill_seven_day_outcomes(conn, today=data_date)
        except Exception:
            logger.exception("observations.md: backfill_seven_day_outcomes failed")

        try:
            digest_text = format_digest(results, data_date, conn, config, since_by_signal, stale_sources, today)
        except Exception:
            # Without this a bug in the wording code would end the run before
            # the send below and the digest would silently not arrive.
            logger.exception("Digest text could not be built")
            digest_text = (
                f"Сводка за {_date_ru(data_date)} не построилась из-за ошибки. "
                "Данные собраны, подробности в logs\\telegram.log."
            )
    finally:
        conn.close()
    logger.info("Digest text built (%d candidate(s), %d char(s))", len(results), len(digest_text))

    if not send_telegram_message(token, chat_id, digest_text, what=f"сводка за {_date_ru(data_date)}"):
        return 1
    # The "не построилась" fallback is final too: repeating it every two hours
    # would only mean several identical error messages.
    return 0 if _record(db_path, record_digest_sent, data_date, "digest") else 1


if __name__ == "__main__":
    _setup_logging()
    # The task runs through cmd.exe with no output redirection, so an
    # unhandled exception would vanish; log it to telegram.log instead.
    try:
        exit_code = main()
    except Exception:
        logger.exception("Digest run crashed")
        exit_code = 1
    sys.exit(exit_code)
