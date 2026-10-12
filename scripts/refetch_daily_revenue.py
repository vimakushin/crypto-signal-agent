"""Re-fetch the FULL daily revenue history of every watchlist protocol from
DeFiLlama and append it to defillama_daily_revenue_restated, next to (never
over) the first-received values in defillama_daily_revenue. Reusable at any
time (e.g. after the 15 October freeze) and deliberately not part of the
schedule: DeFiLlama revises history after the fact (BACKLOG.md), and this is
how we keep both "what the signal saw then" and "what the source says now".

Prints and logs a per-protocol report of plain numbers (no verdicts).
Exit code 1 if any protocol could not be fetched.

    .venv\\Scripts\\python.exe scripts\\refetch_daily_revenue.py
    .venv\\Scripts\\python.exe scripts\\refetch_daily_revenue.py --db-path <copy of db.sqlite>
"""
from __future__ import annotations

import argparse
import logging
import statistics
import sys
from datetime import datetime, timezone
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
    init_db,
    save_defillama_daily_revenue_restated,
)

# A value counts as "differs" when it is off by more than this share of the
# old value. Only for the report; nothing is decided from it.
DIFF_THRESHOLD = 0.01


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path", type=Path, default=None,
        help="Database to write into (default: storage.sqlite_path from config.yaml).",
    )
    args = parser.parse_args()

    with open(DEFAULT_CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    db_path = args.db_path
    if db_path is None:
        db_path = Path(cfg["storage"]["sqlite_path"])
        if not db_path.is_absolute():
            db_path = PROJECT_ROOT / db_path
    init_db(db_path)

    fetched_at = datetime.now(timezone.utc).isoformat()
    watchlist = cfg["watchlist"]["defillama_protocols"]
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
    finally:
        conn.close()

    summary = "=== REPORT ===\n" + "\n".join(report)
    summary += f"\nNot fetched ({len(failed)}): {', '.join(failed) if failed else 'none'}"
    logger.info(summary)  # goes to console and to the log file
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
