"""Re-fetch the FULL daily revenue history of every watchlist protocol from
DeFiLlama and append it to defillama_daily_revenue_restated, next to (never
over) the first-received values in defillama_daily_revenue. Reusable at any
time (e.g. after the 15 October freeze) and deliberately not part of the
schedule: DeFiLlama revises history after the fact (BACKLOG.md), and this is
how we keep both "what the signal saw then" and "what the source says now".

Prints and logs a per-protocol report of plain numbers (no verdicts), then a
second section listing the days the source rewrote by more than
RESTATED_THRESHOLD, split into "rewritten" and "moved to a neighbouring date"
(see classify_day). Exit code 1 if any protocol could not be fetched.

    .venv\\Scripts\\python.exe scripts\\refetch_daily_revenue.py
    .venv\\Scripts\\python.exe scripts\\refetch_daily_revenue.py --report-only
    .venv\\Scripts\\python.exe scripts\\refetch_daily_revenue.py --db-path <copy of db.sqlite>
"""
from __future__ import annotations

import argparse
import logging
import statistics
import sys
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

# Same sys.path fix as scripts/backfill_history.py: this script lives one
# level below the project root.
sys.path.insert(0, str(PROJECT_ROOT))

import yaml  # noqa: E402

from collectors import defillama  # noqa: E402
from storage.db import (  # noqa: E402
    get_connection,
    get_latest_complete_restated_fetch,
    init_db,
    save_defillama_daily_revenue_restated,
)

# A value counts as "differs" when it is off by more than this share of the
# old value. Only for the report; nothing is decided from it.
DIFF_THRESHOLD = 0.01

# The second report section lists only days whose value changed by more than
# 10% (abs relative change), chosen in advance on 8 Nov 2026 so that it is not
# tuned to the result. Smaller changes are normal rounding/late-trade noise.
RESTATED_THRESHOLD = 0.10
# Two values count as "the same number" (for the shift check) when they are
# within 5% of each other, and only if the value is above 100 USD: with tiny
# values (0 = 0, 3 ~ 3) a coincidence proves nothing.
# HONESTY NOTE. Only the 10% threshold above was fixed in advance (8 Nov 2026).
# The 5% tolerance, the 100 USD minimum and the rule "the receiving cell must
# itself have changed by more than 10%" were tuned on this same data AFTER
# looking at the numbers (the first variant found 492 shifted days, with the
# receiving-cell rule 391). So "shift" is a strong sign, not a proof, and
# "rewritten" is claimed only when nothing at all suggests a shift (see
# classify_day).
SHIFT_MATCH_TOLERANCE = 0.05
SHIFT_MIN_USD = 100.0
# Per protocol the console shows the days themselves only up to this many;
# above it only counters (the full list always goes to the log file).
MAX_DAYS_SHOWN = 6


def _setup_logging() -> None:
    """Console + rotating logs/refetch_daily_revenue.log (same pattern as
    scripts/backup_db.py)."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        LOG_DIR.mkdir(exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                LOG_DIR / "refetch_daily_revenue.log",
                maxBytes=5_000_000,
                backupCount=3,
                encoding="utf-8",
            )
        )
    except OSError as exc:
        print(f"WARNING: could not set up file logging at {LOG_DIR} ({exc})", file=sys.stderr)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )


_setup_logging()
logger = logging.getLogger("refetch_daily_revenue")
# DEBUG lines (the full day list) reach only the log file: the console handler
# is held at INFO.
logger.setLevel(logging.DEBUG)
for _h in logging.getLogger().handlers:
    if type(_h) is logging.StreamHandler:
        _h.setLevel(logging.INFO)


def compare_with_stored(conn, slug: str, fresh: dict[str, float]) -> str:
    """One report line for one protocol: fresh values vs. the first-received
    ones in defillama_daily_revenue.

    Args:
        conn: open storage connection.
        slug: protocol slug.
        fresh: date -> revenue_usd just received from DeFiLlama.

    Returns:
        Text line with counts only: dates received, how many of them we
        already had, on common dates how many differ by more than
        DIFF_THRESHOLD (with their date range and the median now/was ratio),
        dates only the source has, dates only we have.
    """
    old = {
        r["date"]: r["revenue_usd"]
        for r in conn.execute(
            "SELECT date, revenue_usd FROM defillama_daily_revenue WHERE protocol_slug = ?",
            (slug,),
        )
        if r["revenue_usd"] is not None
    }
    common = sorted(set(fresh) & set(old))
    differing = [
        d for d in common
        if abs(fresh[d] - old[d]) > DIFF_THRESHOLD * abs(old[d])
    ]
    # Ratio is undefined when the old value is 0; such dates still count as
    # differing above if the new value is not 0.
    ratios = [fresh[d] / old[d] for d in differing if old[d] != 0]
    diff_range = f"{differing[0]}..{differing[-1]}" if differing else "-"
    median_ratio = f"{statistics.median(ratios):.3f}" if ratios else "-"
    return (
        f"{slug}: received {len(fresh)}, in old table {len(common)}, "
        f"differ >{DIFF_THRESHOLD:.0%}: {len(differing)} (dates {diff_range}, "
        f"median now/was {median_ratio}), "
        f"only at source {len(set(fresh) - set(old))}, only ours {len(set(old) - set(fresh))}"
    )


def _same_number(a: float | None, b: float | None) -> bool:
    """True if a and b are both above SHIFT_MIN_USD and within
    SHIFT_MATCH_TOLERANCE of each other."""
    if a is None or b is None or min(abs(a), abs(b)) <= SHIFT_MIN_USD:
        return False
    return abs(a - b) <= SHIFT_MATCH_TOLERANCE * max(abs(a), abs(b))


def _is_restated(ours: dict[str, float], src: dict[str, float], d: str) -> bool:
    """True if d is in both series and the value changed by more than
    RESTATED_THRESHOLD of the old one."""
    return d in ours and d in src and abs(src[d] - ours[d]) > RESTATED_THRESHOLD * abs(ours[d])


def classify_day(ours: dict[str, float], src: dict[str, float], d: str) -> str:
    """Decide what happened on a day where ours[d] and src[d] differ a lot.

    Lesson of 8 Nov 2026: one value that moved to the neighbouring date was
    first read as "27 days of gaps". So before calling a day "rewritten" we
    check whether the number merely changed its date, and we say "rewritten"
    only when we could really check.

    Args:
        ours: date -> first-received revenue of one protocol.
        src: date -> revenue from the latest full re-fetch.
        d: the differing date, "YYYY-MM-DD".

    Returns:
        "shift_later" / "shift_earlier" - the same number now sits one day
        later / earlier at the source, and the receiving day itself changed
        by more than RESTATED_THRESHOLD;
        "rewritten" - both neighbouring dates (d-1 and d+1) exist in both
        series, no neighbour matches in any direction, and the values are not
        tiny;
        "unclear" - everything else: matches in both directions, a match whose
        receiving day barely changed, a neighbouring date missing in either
        series (edge of the history, next to a gap of ours), tiny values.
    """
    day = date.fromisoformat(d)
    prev_d = (day - timedelta(days=1)).isoformat()
    next_d = (day + timedelta(days=1)).isoformat()
    # Daily revenue is smooth, so a neighbour often matches by coincidence.
    # A number that really moved leaves a changed slot at the other end too:
    # a neighbour whose own value did not change cannot be its destination.
    def moved(a: float | None, b: float | None, slot: str) -> bool:
        return _same_number(a, b) and _is_restated(ours, src, slot)

    later = (moved(ours.get(d), src.get(next_d), next_d)
             or moved(src.get(d), ours.get(prev_d), prev_d))
    earlier = (moved(ours.get(d), src.get(prev_d), prev_d)
               or moved(src.get(d), ours.get(next_d), next_d))
    if later and earlier:
        return "unclear"
    if later:
        return "shift_later"
    if earlier:
        return "shift_earlier"
    # A match without a changed receiving cell is not a shift, but it is no
    # proof of a rewrite either.
    coincidence = (
        _same_number(ours.get(d), src.get(next_d)) or _same_number(src.get(d), ours.get(prev_d))
        or _same_number(ours.get(d), src.get(prev_d)) or _same_number(src.get(d), ours.get(next_d))
    )
    has_pairs = all(n in ours and n in src for n in (prev_d, next_d))
    if coincidence or not has_pairs or max(abs(ours[d]), abs(src[d])) <= SHIFT_MIN_USD:
        return "unclear"
    return "rewritten"


def restated_days(ours: dict[str, float], src: dict[str, float]) -> list[tuple[str, float, float, str]]:
    """Days present in both series whose value changed by more than
    RESTATED_THRESHOLD, with their class from classify_day.

    Returns:
        Sorted list of (date, was, now, class).
    """
    result = []
    for d in sorted(set(ours) & set(src)):
        if _is_restated(ours, src, d):
            result.append((d, ours[d], src[d], classify_day(ours, src, d)))
    return result


_CLASS_TITLES = {
    "shift_later": "сдвиг на день (число переехало на день ПОЗЖЕ)",
    "shift_earlier": "сдвиг на день (число переехало на день РАНЬШЕ)",
    "rewritten": "переписано (источник изменил число за ту же дату)",
    "unclear": "не ясно",
}


def _money(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ") + " $"


def build_restatement_report(conn, slugs: list[str], run_incomplete: bool = False) -> tuple[str, str] | None:
    """Section "which days did the source rewrite" for the latest COMPLETE
    re-fetch, compared with the first-received defillama_daily_revenue.
    Read-only. Only "was -> now" numbers, no verdicts on who is right.

    Args:
        conn: open storage connection.
        slugs: watchlist protocols (a re-fetch is complete if it covers all).
        run_incomplete: True if the run that just finished did not cover all
            protocols; the header then says the report is about an earlier run.

    Returns:
        None if there is no complete re-fetch yet; otherwise (short text for
        the console, full text with every day for the log file).
    """
    latest = get_latest_complete_restated_fetch(conn, len(slugs))
    if latest is None:
        return None
    fetched_at = latest[0]
    # class -> slug -> [(date, was, now)]
    groups: dict[str, dict[str, list[tuple[str, float, float]]]] = {c: {} for c in _CLASS_TITLES}
    for slug in slugs:
        ours = {r["date"]: r["revenue_usd"] for r in conn.execute(
            "SELECT date, revenue_usd FROM defillama_daily_revenue WHERE protocol_slug = ?", (slug,))
            if r["revenue_usd"] is not None}
        src = {r["date"]: r["revenue_usd"] for r in conn.execute(
            "SELECT date, revenue_usd FROM defillama_daily_revenue_restated "
            "WHERE protocol_slug = ? AND fetched_at = ?", (slug, fetched_at))
            if r["revenue_usd"] is not None}
        for d, was, now, cls in restated_days(ours, src):
            groups[cls].setdefault(slug, []).append((d, was, now))

    total = {c: sum(len(v) for v in g.values()) for c, g in groups.items()}
    head = (
        f"=== ПЕРЕПИСАННЫЕ ДНИ (перезабор от {fetched_at}) ===\n"
        + ("Этот запуск неполный, ниже - отчёт по предыдущему полному.\n" if run_incomplete else "")
        + f"Сравниваем то, что мы получили в первый раз, с тем, что источник отдаёт сейчас. "
        f"Берём дни, где число изменилось больше чем на {RESTATED_THRESHOLD:.0%}. "
        f"Кто прав - здесь не решается, только «было -> стало»."
    )
    tail = (
        f"Итого: из {sum(total.values())} расходящихся дней: "
        f"сдвиг на день {total['shift_later'] + total['shift_earlier']}, "
        f"переписано {total['rewritten']}, не ясно {total['unclear']}. "
        f"Считаются дни: один переехавший день даёт две ячейки (откуда ушло и куда пришло), "
        f"поэтому «сдвиг на день» обычно чётное число. «Сдвиг» - сильный признак, но не доказательство; "
        f"«переписано» мы говорим, только когда есть соседи для сравнения и ничто не похоже на сдвиг."
    )

    def render(limit: int | None) -> str:
        lines = [head]
        for cls, title in _CLASS_TITLES.items():
            lines.append(f"\n{title}: {total[cls]}")
            for slug, days in groups[cls].items():
                lines.append(f"  {slug}: {len(days)} дн.")
                if limit is None or len(days) <= limit:
                    lines.extend(f"    {d}: {_money(was)} -> {_money(now)}" for d, was, now in days)
        lines.append("\n" + tail)
        return "\n".join(lines)

    return render(MAX_DAYS_SHOWN), render(None)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path", type=Path, default=None,
        help="Database to write into (default: storage.sqlite_path from config.yaml).",
    )
    parser.add_argument(
        "--report-only", action="store_true",
        help="No network, no writes: only print the 'rewritten days' section "
             "from the latest complete re-fetch already in the database.",
    )
    args = parser.parse_args()

    with open(DEFAULT_CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    db_path = args.db_path
    if db_path is None:
        db_path = Path(cfg["storage"]["sqlite_path"])
        if not db_path.is_absolute():
            db_path = PROJECT_ROOT / db_path
    watchlist = cfg["watchlist"]["defillama_protocols"]
    if args.report_only:
        conn = get_connection(db_path)
        try:
            built = build_restatement_report(conn, watchlist)
        finally:
            conn.close()
        if built is None:
            sys.exit("Нет ни одного полного перезабора в базе - сначала запустите скрипт без --report-only.")
        print(built[0])
        logger.debug(built[1])
        return
    init_db(db_path)

    fetched_at = datetime.now(timezone.utc).isoformat()
    logger.info("Re-fetch into %s, fetched_at=%s, %d protocol(s)", db_path, fetched_at, len(watchlist))

    report: list[str] = []
    failed: list[str] = []
    conn = get_connection(db_path)
    try:
        for i, slug in enumerate(watchlist, 1):
            try:
                history = defillama.fetch_protocol_daily_revenue_history(slug)
            except RuntimeError:
                logger.error("[%d/%d] %s: could not fetch after retries - skipping", i, len(watchlist), slug)
                failed.append(slug)
                continue
            fresh = dict(history["daily_revenue"])
            save_defillama_daily_revenue_restated(conn, [
                {"protocol_slug": slug, "date": d, "revenue_usd": v, "fetched_at": fetched_at}
                for d, v in fresh.items()
            ])
            line = compare_with_stored(conn, slug, fresh)
            logger.info("[%d/%d] %s", i, len(watchlist), line)
            report.append(line)
        built = build_restatement_report(conn, watchlist)
    finally:
        conn.close()

    summary = "=== REPORT ===\n" + "\n".join(report)
    summary += f"\nNot fetched ({len(failed)}): {', '.join(failed) if failed else 'none'}"
    logger.info(summary)  # goes to console and to the log file
    if built is not None:
        logger.info(built[0])  # console: compact
        logger.debug(built[1])  # log file only: every day
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
