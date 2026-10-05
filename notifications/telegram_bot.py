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
import sys
import time
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

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
    get_connection,
    get_digest_send_stale,
    init_db,
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


def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
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
            logger.info("Telegram: digest sent successfully (attempt %s/%s)", attempt, MAX_RETRIES)
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
def _format_revenue_price_gap(details: dict) -> str:
    return (
        f"revenue_price_gap: выручка {details.get('revenue_growth_pct'):+.1f}% "
        f"за {details.get('lookback_days')}д (порог {details.get('revenue_growth_threshold_pct')}%), "
        f"капитализация {details.get('mcap_growth_pct'):+.1f}% "
        f"(порог {details.get('mcap_reaction_threshold_pct')}%)"
    )


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


def _digest_text_revenue_price_gap(details: dict, config: dict) -> str:
    baseline_days = config["signals"]["revenue_price_gap"]["baseline_window_days"]
    lookback_days = details["lookback_days"]
    return (
        f"Медианная дневная выручка за {lookback_days} дней на "
        f"{details['revenue_growth_pct']:.0f}% выше, чем за последние {baseline_days} дней, "
        f"а капитализация токена за эти {lookback_days} дней изменилась на "
        f"{_signed(details['mcap_growth_pct'])}%."
    )


def _digest_text_volume_breakout(details: dict, config: dict) -> str:
    return (
        f"Цена пробила уровень, державшийся {details['resistance_lookback_days']} дней "
        f"({_price_level(details['resistance_level'])}), на объёме в "
        f"{details['volume_ratio']:.1f} раза выше среднего за "
        f"{details['volume_avg_lookback_days']} дней."
    )


def _digest_text_oi_divergence(details: dict, config: dict) -> str:
    return (
        f"Открытый интерес по фьючерсам Binance вырос на {details['oi_growth_pct']:.0f}% "
        f"за {details['lookback_hours']:.0f} ч, а цена за это время изменилась на "
        f"{_signed(details['price_change_pct'])}%."
    )


_DIGEST_TEXTS = {
    "revenue_price_gap": _digest_text_revenue_price_gap,
    "volume_breakout": _digest_text_volume_breakout,
    "oi_divergence": _digest_text_oi_divergence,
}


def _digest_signal_text(contribution, config: dict) -> str:
    """Plain-language sentence for one SignalContribution. Unknown
    signal_name or a broken details dict falls back to the generic line
    (and a warning) so one bad row can't sink the whole digest.
    """
    generic = f"{contribution.signal_name}: metric_value={contribution.metric_value}"
    text_fn = _DIGEST_TEXTS.get(contribution.signal_name)
    if text_fn is None:
        logger.warning("Digest: no plain-language text for signal %s", contribution.signal_name)
        return generic
    try:
        return text_fn(contribution.details, config)
    except (TypeError, ValueError, KeyError) as exc:
        logger.warning("Digest: could not format details for %s: %s", contribution.signal_name, exc)
        return generic


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
    """'новая в сводке' or 'была и вчера, идёт с <дата>', judged by the
    episode (run of consecutive days) each contribution belongs to.
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
        return "новая в сводке"
    return f"была и вчера, идёт с {_date_ru(date.fromisoformat(min(starts)))}"


_STALE_SOURCE_LABELS = {
    "defillama": "выручка и капитализация протоколов (DeFiLlama)",
    "coingecko": "цены и объёмы (CoinGecko)",
    "binance": "фьючерсы (Binance)",
}


def _stale_warning(stale_sources: dict[str, str | None]) -> str:
    """The warning paragraph that opens the digest when a source has no
    collection today. "No collection today" and not "older than 24 hours" on
    purpose: yesterday's collection ran late (12:44), today's failed, the
    digest runs at 12:30 - the data are 23.8 hours old and a 24-hour rule
    would stay silent about a source that has not updated today.

    Args:
        stale_sources: {source key: last fetched_at ISO (UTC) or None}.

    Returns:
        Two lines: which sources did not update today (with their last
        collection date) and a note that the digest is built on that data.
    """
    parts = []
    for source, last_iso in stale_sources.items():
        if last_iso is None:
            when = "ещё ни разу не собирались"
        else:
            last_day = datetime.fromisoformat(last_iso).astimezone().date()
            when = f"последний сбор {_date_ru(last_day)}"
        parts.append(f"{_STALE_SOURCE_LABELS[source]} — {when}")
    return (
        f"Внимание: данные устарели. Сегодня не обновлялись: {'; '.join(parts)}.\n"
        "Сводка ниже построена на этих данных."
    )


def format_digest(
    results: list[CandidateScore],
    today: date,
    conn,
    config: dict,
    since_by_signal: dict[str, str],
    stale_sources: dict[str, str | None] | None = None,
) -> str:
    """Turn rank_candidates()'s output into the Telegram message text.

    Only facts, in plain language (TZ sections 1/9 - no advice). Coins with
    two or more different signals go first in a separate block, and only
    there the score is shown. An empty `results` still yields an explicit
    message - silence is indistinguishable from a broken pipeline.

    Args:
        results: rank_candidates()'s return value (order kept as is).
        today: date for the header.
        conn: open storage/db.py connection (coin names, episodes).
        config: parsed config.yaml (baseline window for the wording).
        since_by_signal: default_since_by_signal()'s cursor, for episodes.
        stale_sources: storage.db.sources_without_fetch_today()'s result at
            the moment of building. Non-empty -> the text starts with a
            warning paragraph, and an empty digest no longer claims the
            system works. Empty/None -> the text is exactly as without it.

    Returns:
        The full text, not yet truncated (send_telegram_message() does that).
    """
    header = f"Сводка за {_date_ru(today)}"
    warning = f"{_stale_warning(stale_sources)}\n\n" if stale_sources else ""
    if not results:
        if stale_sources:
            return (
                f"{warning}{header}: сигналов не видно, но по устаревшим данным "
                "судить об этом нельзя."
            )
        return f"{header}: сегодня сигналов нет. Система работает, данные собираются."

    episodes = _build_all_episodes(conn, since_by_signal)
    multi: list[str] = []
    single: list[str] = []
    for candidate in results:
        title = f"{_coin_label(conn, candidate)} · {_freshness_label(candidate, episodes)}"
        texts = [_digest_signal_text(c, config) for c in candidate.contributions]
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


def main() -> int:
    """Build today's digest and send it to Telegram: once per date, or twice
    if the first one had to go out on stale data and fresh data arrived later.

    Steps: read .env; exit quietly if the digest for today was already sent
    (this task is retried every few hours - see config.yaml schedule); wait
    for fresh data; rank candidates and update observations.md; build the
    text (with a stale-data warning if needed); send; on success remember it.
    A failed send leaves no mark, so the next scheduled run tries again.

    Returns:
        Process exit code: 0 - sent, or nothing left to send today; 1 - no
        credentials, the send failed, or the sent mark could not be written.
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

    today = date.today()
    # init_db: this script is the first to touch the digest_sends table, and
    # it may run before main.py ever did after an upgrade.
    init_db(db_path)
    conn = get_connection(db_path)
    try:
        sent_stale = get_digest_send_stale(conn, today)
    finally:
        conn.close()

    # At most two digests per date: the first one may go out on stale data
    # (after the wait below ran out); if so, a second one - on fresh data -
    # is allowed later the same day, without waiting, and it replaces the mark.
    is_resend = sent_stale is True
    if sent_stale is False:
        logger.info("Digest for %s was already sent today - nothing to do", today.isoformat())
        return 0
    if is_resend:
        conn = get_connection(db_path)
        try:
            stale_sources = sources_without_fetch_today(conn, today)
        finally:
            conn.close()
        if stale_sources:
            logger.info(
                "Digest for %s went out on stale data and %s still has no collection today - nothing to do",
                today.isoformat(), ", ".join(stale_sources),
            )
            return 0
        logger.info("Data are fresh now - sending the second digest for %s", today.isoformat())
    else:
        stale_sources = wait_for_fresh_data(db_path, today)
        if stale_sources:
            logger.warning(
                "Building the digest on stale data, no collection today for: %s", ", ".join(stale_sources)
            )

    signal_weights = signal_weights_from_config(config)
    conn = get_connection(db_path)
    try:
        since_by_signal = default_since_by_signal(conn, config)
        results = rank_candidates(conn, signal_weights, since_by_signal)

        # Manual-labeling journal (TZ section 6, cut-down MVP version - see
        # notifications/observations.py's module docstring). Independent of
        # the Telegram send below: a failure here must not block the digest,
        # and a failed digest send below must not block this.
        from notifications.observations import (
            append_new_observations,
            backfill_seven_day_outcomes,
        )

        try:
            append_new_observations(conn, results, today.isoformat())
        except Exception:
            logger.exception("observations.md: append_new_observations failed")
        try:
            backfill_seven_day_outcomes(conn)
        except Exception:
            logger.exception("observations.md: backfill_seven_day_outcomes failed")

        fallback_text = False
        try:
            digest_text = format_digest(results, today, conn, config, since_by_signal, stale_sources)
        except Exception:
            fallback_text = True
            # Without this a bug in the wording code would end the run before
            # the send below and the digest would silently not arrive.
            logger.exception("Digest text could not be built")
            digest_text = (
                f"Сводка за {_date_ru(today)} не построилась из-за ошибки. "
                "Данные собраны, подробности в logs\\telegram.log."
            )
    finally:
        conn.close()

    if is_resend:
        digest_text = (
            "Данные обновились. Это повторная сводка за сегодня, уже на свежих данных.\n\n"
            + digest_text
        )
    logger.info("Digest text built (%d candidate(s), %d char(s))", len(results), len(digest_text))

    if not send_telegram_message(token, chat_id, digest_text):
        return 1

    # The fallback "не построилась" text is final (stale=False): repeating it
    # every two hours would only mean several identical error messages.
    try:
        conn = get_connection(db_path)
        try:
            record_digest_sent(
                conn, today, datetime.now().astimezone().isoformat(timespec="seconds"),
                stale=bool(stale_sources) and not fallback_text,
            )
        finally:
            conn.close()
    except Exception:
        logger.exception(
            "The digest for %s WAS SENT to Telegram, but the mark could not be written to the "
            "database - the next scheduled run (in ~2 hours) may send a duplicate",
            today.isoformat(),
        )
        return 1
    return 0


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
