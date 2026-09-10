"""Manual-labeling journal (TZ section 6, cut-down MVP version): a plain
markdown table, `observations.md` at the project root, instead of a web UI.
The user fills in "через 14 дней" / "через 30 дней" / "Вывод" by hand in a
text editor - this module never writes those columns.

Two entry points, both called from notifications/telegram_bot.py's
`if __name__ == "__main__":` block, after rank_candidates() has already run:

  append_new_observations() - adds one row per NEW candidate (see its
    docstring for the "no more than once per 30 days per candidate" rule -
    rank_candidates() returns the same candidate every day its signal keeps
    firing, e.g. cowswap's revenue_price_gap holding for weeks, and this
    journal must not turn that into a near-duplicate row every single day).

  backfill_seven_day_outcomes() - fills in the "через 7 дней" column, once,
    for any row old enough (>=7 days) that still has that cell empty. Never
    touches "через 14 дней" / "через 30 дней" / "Вывод" - those are the
    user's, permanently.

Both are independent and each is wrapped in its own try/except by the
caller, so a failure in one never blocks the other or blocks the Telegram
digest itself.
"""
from __future__ import annotations

import logging
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from notifications.telegram_bot import _format_contribution  # noqa: E402
from scoring.ranker import CandidateScore  # noqa: E402
from storage.db import (  # noqa: E402
    get_binance_oi_snapshot_hours_before,
    get_coingecko_price_before,
    get_snapshot_days_before,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OBSERVATIONS_PATH = PROJECT_ROOT / "observations.md"

_HEADER_TEXT = (
    "# Журнал наблюдений\n"
    "\n"
    "Ранняя версия ручной разметки (раздел 6 ТЗ). Цену и капитализацию через "
    "7 дней система подставляет сама — данные уже есть в базе. Колонки "
    "\"через 14 дней\", \"через 30 дней\" и \"Вывод\" заполняются вручную.\n"
    "\n"
    "| Дата | Кандидат | Скор | Сработавшие сигналы | На момент срабатывания "
    "| Через 7 дней | Через 14 дней | Через 30 дней | Вывод |\n"
    "|---|---|---|---|---|---|---|---|---|\n"
)

# Number of elements produced by str.split("|") on a well-formed data row
# "| a | b | c | d | e | f | g | h | i |" (9 table columns => 10 "|"
# characters => 11 elements: one empty string before the first "|", one
# empty string after the last, and the 9 real cells in between).
_TABLE_COLUMN_COUNT = 9
_EXPECTED_SPLIT_COUNT = _TABLE_COLUMN_COUNT + 2

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _format_number(value: float) -> str:
    """Same "trim trailing zeros but don't overdo it" idea already used by
    telegram_bot.py's formatters (e.g. `:.4g` in _format_volume_breakout) -
    kept consistent between "на момент срабатывания" and "через 7 дней" per
    this task's instructions, rather than picking a different style for
    each.
    """
    if value >= 1000:
        return f"{value:,.2f}".replace(",", " ")
    if value >= 1:
        return f"{value:.4g}"
    return f"{value:.6g}"


def _get_price_and_mcap(
    conn, protocol_slug: str, source: str, on_or_before_date: str
) -> str:
    """Human-readable "цена: ..., mcap: ..." for `protocol_slug` at or
    before `on_or_before_date` (a plain "YYYY-MM-DD" string), never looking
    ahead of that date.

    Args:
        conn: open sqlite3 connection.
        protocol_slug: candidate.protocol_slug (a DeFiLlama slug for
            DeFiLlama/CoinGecko-sourced candidates, or a Binance Futures
            ticker like "BTCUSDT" for oi_divergence candidates).
        source: candidate.contributions[0].source - picks which tables to
            read (DeFiLlama snapshots + CoinGecko price history, vs a
            Binance OI snapshot).
        on_or_before_date: "YYYY-MM-DD"; the reference date, inclusive.

    Returns:
        A single line like "цена: $0.512, mcap: $73.7M" (or "цена: н/д,
        mcap: $73.7M" if only one half was found). "н/д" for both halves if
        nothing at all is found for this candidate/date.
    """
    if source == "Binance Futures":
        reference_iso = f"{on_or_before_date}T23:59:59+00:00"
        snapshot = get_binance_oi_snapshot_hours_before(conn, protocol_slug, reference_iso, 0)
        price_text = f"${_format_number(snapshot['price'])}" if snapshot is not None else "н/д"
        return f"цена: {price_text}, mcap: н/д"

    # DeFiLlama / CoinGecko candidate.
    reference_iso = f"{on_or_before_date}T23:59:59+00:00"
    mcap_snapshot = get_snapshot_days_before(conn, protocol_slug, reference_iso, 0)
    mcap_text = "н/д"
    if mcap_snapshot is not None and mcap_snapshot["mcap"] is not None:
        mcap_text = f"${_format_number(mcap_snapshot['mcap'])}"

    price_row = get_coingecko_price_before(conn, protocol_slug, on_or_before_date)
    price_text = "н/д"
    if price_row is not None and price_row["price"] is not None:
        price_text = f"${_format_number(price_row['price'])}"

    return f"цена: {price_text}, mcap: {mcap_text}"


def _ensure_file_with_header(observations_path: Path) -> None:
    """Create `observations_path` with the standard header if it doesn't
    exist yet. No-op if it already exists (existing content, including any
    manual edits, is left untouched).
    """
    if not observations_path.exists():
        observations_path.write_text(_HEADER_TEXT, encoding="utf-8")


def _read_lines(observations_path: Path) -> list[str]:
    return observations_path.read_text(encoding="utf-8").splitlines(keepends=True)


def _split_row_cells(line: str) -> list[str] | None:
    """Split one markdown table row "| a | b | c |" into cells
    (["a", "b", "c"]), or None if `line` doesn't look like a table data row
    at all (blank line, header text, separator row "|---|---|...").
    """
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return None
    raw_cells = stripped.split("|")
    if len(raw_cells) != _EXPECTED_SPLIT_COUNT:
        return None
    cells = [c.strip() for c in raw_cells[1:-1]]
    # Separator row, e.g. "|---|---|...|".
    if all(re.fullmatch(r"-+", c) for c in cells):
        return None
    return cells


def _existing_recent_candidates(observations_path: Path, today_str: str) -> set[str]:
    """protocol_slugs that already have a row in `observations_path` dated
    within the last 30 days of `today_str` - used by append_new_observations
    to avoid re-adding a candidate whose signal is still firing day after
    day (see this module's docstring).
    """
    if not observations_path.exists():
        return set()

    today = date.fromisoformat(today_str)
    recent: set[str] = set()
    for line in _read_lines(observations_path):
        cells = _split_row_cells(line)
        if cells is None:
            stripped = line.strip()
            # Same distinction as in backfill_seven_day_outcomes(): only warn
            # on lines that look like a broken table row, not on blank
            # lines, the heading, the intro paragraph or the "|---|"
            # separator - those are expected and should stay silent.
            if stripped.startswith("|") and not re.fullmatch(r"\|[-| ]+\|", stripped):
                logger.warning(
                    "observations.md: row does not look valid, leaving unchanged: %r",
                    stripped,
                )
            continue
        row_date_str, candidate = cells[0], cells[1]
        if not _DATE_RE.match(row_date_str):
            continue
        try:
            row_date = date.fromisoformat(row_date_str)
        except ValueError:
            continue
        if (today - row_date) <= timedelta(days=30):
            recent.add(candidate)
    return recent


def append_new_observations(
    conn,
    results: list[CandidateScore],
    today_str: str,
    observations_path: Path = DEFAULT_OBSERVATIONS_PATH,
) -> int:
    """Add one new row per candidate in `results` that doesn't already have
    a row dated within the last 30 days (see _existing_recent_candidates) -
    this is what keeps a signal that fires every day for weeks (e.g.
    cowswap's revenue_price_gap) from flooding the journal with a
    near-duplicate row every single run.

    Args:
        conn: open sqlite3 connection (read-only use here: only looks up
            price/mcap at today's date).
        results: rank_candidates()'s return value for today.
        today_str: today's date as "YYYY-MM-DD".
        observations_path: path to observations.md. Created with the
            standard header if missing.

    Returns:
        Number of rows actually appended (0 if every candidate already had
        a recent row, or if `results` is empty).
    """
    _ensure_file_with_header(observations_path)
    already_recent = _existing_recent_candidates(observations_path, today_str)

    new_lines: list[str] = []
    added = 0
    for candidate in results:
        if candidate.protocol_slug in already_recent:
            continue

        signals_text = "; ".join(
            _format_contribution(contribution) for contribution in candidate.contributions
        )
        source = candidate.contributions[0].source if candidate.contributions else ""
        at_trigger_text = _get_price_and_mcap(conn, candidate.protocol_slug, source, today_str)

        row = (
            f"| {today_str} | {candidate.protocol_slug} | {candidate.total_score:.2f} "
            f"| {signals_text} | {at_trigger_text} |  |  |  |  |\n"
        )
        new_lines.append(row)
        already_recent.add(candidate.protocol_slug)
        added += 1

    if added:
        with observations_path.open("a", encoding="utf-8") as f:
            f.writelines(new_lines)
        logger.info("observations.md: added %d new row(s) for %s", added, today_str)
    else:
        logger.info("observations.md: no new rows for %s (all candidates already recent)", today_str)

    return added


def backfill_seven_day_outcomes(
    conn, observations_path: Path = DEFAULT_OBSERVATIONS_PATH
) -> int:
    """Fill in the "через 7 дней" column for any row that is >=7 days old
    and still has that cell empty. Never touches "через 14 дней", "через 30
    дней" or "Вывод" - those are filled in by the user by hand, permanently.

    Rewrites `observations_path` in full, but every line that isn't actually
    changed (including the header, the separator, and any row not eligible
    for backfill) is written back byte-for-byte as read - manual edits to
    other columns are preserved. A line that doesn't parse as a valid table
    row (user may have broken it by hand) is logged as a warning and left
    untouched rather than raising.

    Reads the whole file into memory and rewrites it in full without any
    file locking: if the user happens to be editing and saving
    observations.md by hand in a text editor at the exact same moment this
    runs, their edit can be silently overwritten by the version this
    function held in memory when it read the file.

    Args:
        conn: open sqlite3 connection.
        observations_path: path to observations.md.

    Returns:
        Number of rows whose "через 7 дней" cell was filled in. 0 if the
        file doesn't exist yet.
    """
    if not observations_path.exists():
        return 0

    today = date.today()
    lines = _read_lines(observations_path)
    updated_count = 0
    out_lines: list[str] = []

    for line in lines:
        cells = _split_row_cells(line)
        if cells is None:
            stripped = line.strip()
            # A line starting with "|" that isn't a valid data row and isn't
            # obviously the separator ("|---|...|") is treated as a broken
            # row (user may have edited it by hand) and gets a warning.
            # Blank lines, the "# Журнал наблюдений" heading, the intro
            # paragraph and the "|---|" separator are all expected non-row
            # lines and stay silent.
            if stripped.startswith("|") and not re.fullmatch(r"\|[-| ]+\|", stripped):
                logger.warning(
                    "observations.md: row does not look valid, leaving unchanged: %r",
                    stripped,
                )
            out_lines.append(line)
            continue

        row_date_str, candidate, score, signals_text, at_trigger, seven_day, rest_14, rest_30, verdict = cells

        if seven_day != "" or not _DATE_RE.match(row_date_str):
            if seven_day == "" and not _DATE_RE.match(row_date_str):
                logger.warning(
                    "observations.md: row does not look valid (bad date %r), leaving unchanged: %r",
                    row_date_str, line.rstrip("\n"),
                )
            out_lines.append(line)
            continue

        try:
            row_date = date.fromisoformat(row_date_str)
        except ValueError:
            logger.warning(
                "observations.md: could not parse date %r, leaving row unchanged: %r",
                row_date_str, line.rstrip("\n"),
            )
            out_lines.append(line)
            continue

        if (today - row_date).days < 7:
            out_lines.append(line)
            continue

        target_date_str = (row_date + timedelta(days=7)).isoformat()

        # source isn't stored explicitly in the row - infer it from whether
        # "На момент срабатывания" already says "mcap: н/д" for a Binance
        # ticker, or by checking the signals text for "oi_divergence" (the
        # only signal_name sourced from Binance Futures - see
        # scoring/ranker.py / notifications/telegram_bot.py's
        # _SIGNAL_FORMATTERS).
        source = "Binance Futures" if "oi_divergence" in signals_text else "DeFiLlama"
        outcome_text = _get_price_and_mcap(conn, candidate, source, target_date_str)

        if outcome_text == "цена: н/д, mcap: н/д":
            # No data yet for that point in time (not "never will be") -
            # leave the cell empty and try again on a later run, per this
            # task's instructions.
            out_lines.append(line)
            continue

        new_line = (
            f"| {row_date_str} | {candidate} | {score} | {signals_text} | {at_trigger} "
            f"| {outcome_text} | {rest_14} | {rest_30} | {verdict} |\n"
        )
        out_lines.append(new_line)
        updated_count += 1

    if updated_count:
        observations_path.write_text("".join(out_lines), encoding="utf-8")
        logger.info("observations.md: backfilled 'через 7 дней' for %d row(s)", updated_count)

    return updated_count
