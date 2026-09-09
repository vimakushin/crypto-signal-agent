"""One-off backfill script (TZ section 6/7): populates storage/db.sqlite
with historical data that predates this project's first run, so signals -
especially the "volume breakout" signal (TZ 4.7, signals/volume_breakout.py)
- have real history to evaluate against from day one instead of starting
from an empty table.

Two independent sources, each safely re-runnable (idempotent) after a
partial failure such as CoinGecko rate-limiting mid-run:

  - DeFiLlama + CoinGecko, per config.yaml watchlist.defillama_protocols:
    pulls each protocol's full daily revenue history from DeFiLlama
    (years, not just the few days this project has been running) plus its
    CoinGecko id, then CoinGecko's own daily price/market-cap/volume
    history, and:
      - reconstructs the SAME revenue_total_7d/30d and
        revenue_change_7d/30d_pct fields collectors/defillama.py's live
        collect() writes on every daily run - see _compute_revenue_rollups
        for why the math must match DeFiLlama's own live /overview/fees
        rollup method exactly: signals/revenue_price_gap.py compares rows
        written by this script against rows written by the daily
        collector, and if the math differed, that comparison would
        silently be comparing two different things;
      - saves the same response's daily price/volume points to
        storage/db.py's coingecko_price_history table (TZ 4.7's
        volume_breakout signal - see _build_price_volume_records), so that
        signal also has real multi-month history to evaluate against from
        day one instead of starting from an empty table.

  - Binance Futures OI/price: history is physically capped at ~30 days by
    Binance itself, regardless of how large a `limit` is requested
    (verified live: period=1d returns 31 points, period=4h returns 186
    points, both exactly 30 days back) - so this half is just one
    collect() call via the existing collectors/binance_futures.py, not
    bespoke backfill math.

The DeFiLlama half deliberately stops BACKFILL_SKIP_RECENT_DAYS days before
today - the live daily collector (main.py --cycle defillama) already owns
that window, and its fetched_at (actual run time) doesn't share a format
with this script's (midnight UTC of the historical date), so a re-run that
tried to backfill all the way to today could otherwise slip a near-duplicate
row past the storage layer's own UNIQUE INDEX - see BACKFILL_SKIP_RECENT_DAYS.

Run manually, once (or again later, e.g. to extend history further, or to
resume a run that got interrupted):

    .venv\\Scripts\\python.exe scripts\\backfill_history.py

Deliberately not part of the daily/6-hourly main.py cycle - this is a slow,
rate-limit-bound bulk job with its own progress logging, meant to be run
and watched by a human, not scheduled.
"""
from __future__ import annotations

import logging
import math
import sys
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

# This script lives in scripts/, one level below the project root, but
# imports collectors/ and storage/ from the root - when Python runs a
# script by path (`python scripts\backfill_history.py`), it puts only the
# script's OWN directory on sys.path, not the project root, regardless of
# the current working directory. Without this, `from collectors import
# ...` below fails with ModuleNotFoundError no matter where this is run
# from. main.py doesn't need this because it already lives at the project
# root.
sys.path.insert(0, str(PROJECT_ROOT))

import yaml  # noqa: E402

from collectors import binance_futures, coingecko, defillama  # noqa: E402
from storage.db import (  # noqa: E402
    get_connection,
    init_db,
    save_binance_oi_snapshots,
    save_coingecko_price_history,
    save_defillama_snapshots,
)

# DeFiLlama's own live rollup (fetch_revenue_overview -> total7d/total30d/
# change_7d/change_30dover30d, relayed as-is by collectors/defillama.py)
# needs a full calendar window of daily revenue behind a date before its
# numbers mean anything - 60 days is the stricter of the two windows this
# script reconstructs (the 30d rollup's "60 days before" sub-window), so it
# gates both. Matches signals/revenue_price_gap.py's own "insufficient
# history" treatment of the same situation on the live/daily side.
MIN_HISTORY_DAYS_FOR_ROLLUPS = 60

# The live daily collector (collectors/defillama.py's collect(), run by
# main.py --cycle defillama) writes fetched_at as the actual wall-clock time
# of that run, while this script writes fetched_at as midnight UTC of the
# historical date it's backfilling - two different strings for what can be
# the same calendar day, so idx_defillama_snapshots_slug_fetched_unique's
# UNIQUE INDEX does NOT catch it as a duplicate. README.md tells users this
# script is safe to re-run later "to extend history further" - if it always
# backfilled all the way up to today, a re-run after the live collector has
# already run today would insert a second, near-duplicate row for the same
# protocol/day instead of being caught as already-saved. Skipping the most
# recent few days here - which the live collector already covers on its own
# daily schedule anyway - avoids that instead of requiring a fetched_at
# format change.
BACKFILL_SKIP_RECENT_DAYS = 3

# Binance's openInterestHist endpoint hard-caps `limit` at 500 regardless of
# what's asked (see collectors/binance_futures.py's `collect(... limit=)`
# docstring) - verified live to be enough for one call to cover the whole
# ~30-day archive Binance actually has at a 4h bucket size.
BINANCE_BACKFILL_LOOKBACK_HOURS = 720  # 30 days - matches Binance's real archive depth
BINANCE_BACKFILL_PERIOD = "4h"
BINANCE_BACKFILL_LIMIT = 500


def _setup_logging() -> None:
    """Console + a rotating file in logs/, separate from main.py's
    logs/scheduler.log - this is a distinct, occasional operation, and its
    (much longer, much chattier) run shouldn't compete for space in the
    daily cycle's log or be interleaved with it.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        LOG_DIR.mkdir(exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                LOG_DIR / "backfill.log",
                maxBytes=5_000_000,
                backupCount=3,
                encoding="utf-8",
            )
        )
    except OSError as exc:
        print(
            f"WARNING: could not set up file logging at {LOG_DIR} ({exc}); "
            "continuing with console-only logging.",
            file=sys.stderr,
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )


_setup_logging()
logger = logging.getLogger("backfill_history")


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> dict:
    """Load config.yaml (see main.py's load_config - duplicated here, not
    imported, so this script doesn't trigger main.py's own module-level
    logging setup as an import side effect).
    """
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _build_revenue_by_date(slug: str, daily_revenue: list[tuple[str, float]]) -> dict[str, float]:
    """Turn defillama.fetch_protocol_daily_revenue_history()'s daily_revenue
    list into a date -> revenue lookup for _sum_window, filtering out
    NaN/Infinity values first.

    JSON allows NaN/Infinity (Python's json module parses them by default,
    and requests/DeFiLlama's own response has been observed to occasionally
    carry one), and `float(nan)` doesn't raise - so an un-filtered NaN would
    silently flow straight through _sum_window's addition into
    revenue_total_7d/30d, turning every rollup that window touches into NaN
    without a single error or log line. A bad value is logged and dropped
    (the date is then treated the same as a day with no entry at all - see
    _sum_window's own "missing day = zero revenue" convention), rather than
    poisoning every rollup within `days` of it.

    Args:
        slug: protocol slug, only used for the log message.
        daily_revenue: (date, revenue) tuples from
            defillama.fetch_protocol_daily_revenue_history().

    Returns:
        "YYYY-MM-DD" -> revenue_usd, with any NaN/Infinity entries removed.
    """
    revenue_by_date: dict[str, float] = {}
    for date_str, revenue in daily_revenue:
        if math.isnan(revenue) or math.isinf(revenue):
            logger.warning(
                "DeFiLlama backfill: '%s' has a non-finite revenue value (%s) on %s - "
                "dropping this date's revenue (treated as no data, not zero) so it "
                "doesn't poison any 7d/30d/60d window that includes it",
                slug, revenue, date_str,
            )
            continue
        revenue_by_date[date_str] = revenue
    return revenue_by_date


def _sum_window(revenue_by_date: dict[str, float], as_of: date, days: int, offset: int) -> float:
    """Sum daily revenue over a `days`-day calendar window ending `offset`
    days before `as_of` (offset=0 means the window ends ON `as_of`).

    A calendar day with no entry in `revenue_by_date` counts as zero
    revenue rather than being skipped - DeFiLlama's own daily chart
    wouldn't have a gap-shaped hole for a protocol that simply made no
    revenue that day, so treating a missing key the same way is the
    faithful interpretation, not a data loss.
    """
    total = 0.0
    for i in range(days):
        day = as_of - timedelta(days=offset + i)
        total += revenue_by_date.get(day.isoformat(), 0.0)
    return total


def _compute_revenue_rollups(
    revenue_by_date: dict[str, float], as_of: date, first_date: date
) -> dict | None:
    """Reconstruct DeFiLlama's own 7d/30d revenue rollup math for one
    historical date, matching how DeFiLlama computes it live on
    /overview/fees (which collectors/defillama.py relays as-is):
    total7d = sum of the 7 days ending on `as_of` (inclusive);
    total14dto7d = sum of the 7 days immediately before that window
    (as_of-13..as_of-7); change_7d = (total7d - total14dto7d) /
    total14dto7d * 100. total30d/total60dto30d/change_30dover30d follow the
    same pattern with 30/60-day windows.

    Args:
        revenue_by_date: "YYYY-MM-DD" (UTC) -> daily revenue in USD, for
            one protocol.
        as_of: the historical date to compute rollups "as of" (inclusive).
        first_date: this protocol's earliest known revenue date - used to
            gate on having enough calendar history (see
            MIN_HISTORY_DAYS_FOR_ROLLUPS), the same way
            signals/revenue_price_gap.py treats a too-short history as
            "insufficient" rather than computing a misleading number off a
            partial window.

    Returns:
        None if `as_of` is less than MIN_HISTORY_DAYS_FOR_ROLLUPS calendar
        days after `first_date` - not enough history exists yet to trust
        the 30d/60d windows. Otherwise a dict with revenue_total_7d,
        revenue_total_30d, revenue_change_7d_pct, revenue_change_30d_pct
        (the pct fields are None, not a fabricated number, if their
        denominator window summed to exactly zero).
    """
    if (as_of - first_date).days < MIN_HISTORY_DAYS_FOR_ROLLUPS - 1:
        return None

    total7d = _sum_window(revenue_by_date, as_of, days=7, offset=0)
    total14dto7d = _sum_window(revenue_by_date, as_of, days=7, offset=7)
    total30d = _sum_window(revenue_by_date, as_of, days=30, offset=0)
    total60dto30d = _sum_window(revenue_by_date, as_of, days=30, offset=30)

    change_7d = (total7d - total14dto7d) / total14dto7d * 100 if total14dto7d else None
    change_30d = (total30d - total60dto30d) / total60dto30d * 100 if total60dto30d else None

    return {
        "revenue_total_7d": total7d,
        "revenue_total_30d": total30d,
        "revenue_change_7d_pct": change_7d,
        "revenue_change_30d_pct": change_30d,
    }


def _build_price_volume_records(
    slug: str, gecko_id: str, market_chart: list[dict], recent_cutoff: date
) -> list[dict]:
    """Turn coingecko.fetch_market_chart()'s points into rows for
    storage.db.save_coingecko_price_history() (TZ 4.7's volume_breakout
    signal), reusing the SAME market_chart response backfill_defillama_protocol
    already fetched for market cap - no second CoinGecko request needed.

    Two protections, mirroring what _build_revenue_by_date and
    BACKFILL_SKIP_RECENT_DAYS already do for the revenue side of this
    script, applied here to price/volume instead:
      - NaN/Infinity filtering: JSON allows NaN/Infinity, and
        `float('nan') < x` and friends don't raise - an unfiltered
        NaN/Infinity price or volume would otherwise flow silently into
        storage and poison every max()/average signals/volume_breakout.py
        later computes from it. The bad field is stored as NULL, not
        dropped as a whole date - see collectors/coingecko.py's
        collect_price_volume_history(), which applies the identical rule
        on the live daily-cycle side, for the matching reasoning.
      - recent_cutoff: dates on/after it are skipped (left to the live
        daily collector, main.py --cycle defillama) - see
        BACKFILL_SKIP_RECENT_DAYS for why a re-run could otherwise slip a
        near-duplicate row past the storage layer's UNIQUE(gecko_id, date)
        constraint (this script's midnight-UTC fetched_at vs. the live
        collector's actual run time - two different strings, but the SAME
        gecko_id/date pair, so it's the `date` column doing the real
        deduping here, unlike defillama_snapshots which dedupes on
        fetched_at).

    Args:
        slug: DeFiLlama protocol slug, only used for log messages.
        gecko_id: CoinGecko coin id `market_chart` was fetched for.
        market_chart: coingecko.fetch_market_chart()'s return value.
        recent_cutoff: dates on/after this are skipped.

    Returns:
        List of dicts ready for storage.db.save_coingecko_price_history().
    """
    records: list[dict] = []
    dropped_non_finite = 0
    skipped_recent = 0

    for rec in market_chart:
        as_of = date.fromisoformat(rec["date"])
        if as_of >= recent_cutoff:
            skipped_recent += 1
            continue

        price = rec.get("price")
        volume = rec.get("volume")
        if price is not None and (math.isnan(price) or math.isinf(price)):
            dropped_non_finite += 1
            price = None
        if volume is not None and (math.isnan(volume) or math.isinf(volume)):
            dropped_non_finite += 1
            volume = None

        records.append({
            "gecko_id": gecko_id,
            "protocol_slug": slug,
            "date": rec["date"],
            "fetched_at": datetime(
                as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc
            ).isoformat(),
            "price": price,
            "volume": volume,
        })

    if dropped_non_finite:
        logger.warning(
            "CoinGecko backfill: '%s' (%s) had %d non-finite price/volume value(s) - "
            "stored as NULL for those fields/dates rather than a fabricated or "
            "poisoned number",
            slug, gecko_id, dropped_non_finite,
        )
    logger.info(
        "CoinGecko backfill: '%s' (%s) -> %d day(s) of price/volume history to save "
        "(%d most-recent day(s) left to the live daily collector)",
        slug, gecko_id, len(records), skipped_recent,
    )
    return records


def backfill_defillama_protocol(conn, slug: str, protocol_entry: dict | None) -> None:
    """Backfill defillama_snapshots + coingecko_price_history history for
    one protocol.

    Never raises for a source outage on this one protocol (DeFiLlama or
    CoinGecko down after retries) - logs it and either degrades to
    revenue-only rows (mcap=NULL, same as collectors/defillama.py's own
    handling of a missing market cap) or skips the protocol entirely if
    there's no revenue history to work with at all, so one bad protocol
    doesn't stop the rest of the watchlist from being backfilled.

    Args:
        conn: open storage/db.py connection.
        slug: DeFiLlama protocol slug.
        protocol_entry: this slug's entry from defillama.fetch_protocols()
            (for `symbol`), or None if that lookup failed entirely.
    """
    try:
        history = defillama.fetch_protocol_daily_revenue_history(slug)
    except RuntimeError:
        logger.error("DeFiLlama backfill: could not fetch revenue history for '%s' - skipping", slug)
        return

    daily_revenue = history["daily_revenue"]
    if not daily_revenue:
        logger.warning("DeFiLlama backfill: no daily revenue history at all for '%s' - skipping", slug)
        return

    revenue_by_date = _build_revenue_by_date(slug, daily_revenue)
    first_date = date.fromisoformat(daily_revenue[0][0])
    last_date = date.fromisoformat(daily_revenue[-1][0])
    logger.info(
        "DeFiLlama backfill: '%s' has %d days of revenue history (%s to %s)",
        slug, len(daily_revenue), first_date, last_date,
    )

    # See BACKFILL_SKIP_RECENT_DAYS: the live daily collector already owns
    # the most recent few days, and its fetched_at format doesn't line up
    # with this script's for the UNIQUE INDEX to dedupe them itself.
    recent_cutoff = datetime.now(timezone.utc).date() - timedelta(days=BACKFILL_SKIP_RECENT_DAYS)

    gecko_id = history.get("gecko_id")
    mcap_by_date: dict[str, float] = {}
    price_volume_records: list[dict] = []
    if gecko_id:
        try:
            market_chart = coingecko.fetch_market_chart(gecko_id, days=365)
            mcap_by_date = {
                rec["date"]: rec["market_cap"]
                for rec in market_chart
                if rec.get("market_cap") is not None
            }
            logger.info(
                "CoinGecko backfill: '%s' (%s) -> %d days of market cap history",
                slug, gecko_id, len(mcap_by_date),
            )
            price_volume_records = _build_price_volume_records(
                slug, gecko_id, market_chart, recent_cutoff
            )
        except RuntimeError:
            logger.error(
                "CoinGecko backfill: could not fetch market cap/price/volume history for "
                "'%s' (%s) - saving revenue-only rows for this protocol (mcap=NULL), same "
                "as collectors/defillama.py's own handling of a missing market cap; no "
                "coingecko_price_history rows saved for it either",
                slug, gecko_id,
            )
    else:
        logger.warning(
            "DeFiLlama backfill: '%s' has no gecko_id on file at DeFiLlama - market cap "
            "and price/volume history can't be resolved, saving revenue-only rows (mcap=NULL)",
            slug,
        )

    if price_volume_records:
        save_coingecko_price_history(conn, price_volume_records)
        logger.info(
            "CoinGecko backfill: '%s' saved %d price/volume row(s)",
            slug, len(price_volume_records),
        )

    symbol = protocol_entry.get("symbol") if protocol_entry else None
    name = history.get("name")
    category = history.get("category")

    records: list[dict] = []
    skipped_insufficient = 0
    skipped_recent = 0
    for date_str, _ in daily_revenue:
        as_of = date.fromisoformat(date_str)
        if as_of >= recent_cutoff:
            skipped_recent += 1
            continue

        rollups = _compute_revenue_rollups(revenue_by_date, as_of, first_date)
        if rollups is None:
            skipped_insufficient += 1
            continue

        fetched_at = datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc).isoformat()
        records.append({
            "slug": slug,
            "symbol": symbol,
            "name": name,
            "category": category,
            "fetched_at": fetched_at,
            "mcap": mcap_by_date.get(date_str),
            **rollups,
        })

    if not records:
        logger.warning(
            "DeFiLlama backfill: '%s' produced 0 backfillable rows - %d day(s) within the "
            "first %d days of this protocol's history (not enough for the 30d/60d rollup "
            "window), %d day(s) within the last %d days (left to the live daily collector) "
            "- skipping",
            slug, skipped_insufficient, MIN_HISTORY_DAYS_FOR_ROLLUPS,
            skipped_recent, BACKFILL_SKIP_RECENT_DAYS,
        )
        return

    save_defillama_snapshots(conn, records)
    mcap_covered = sum(1 for r in records if r["mcap"] is not None)
    logger.info(
        "DeFiLlama backfill: '%s' saved %d row(s) (%d with market cap, %d day(s) skipped for "
        "insufficient revenue history, %d most-recent day(s) left to the live daily collector)",
        slug, len(records), mcap_covered, skipped_insufficient, skipped_recent,
    )


def backfill_binance(conn, symbols: list[str]) -> None:
    """Backfill binance_oi_snapshots for the whole watchlist in one call.

    Unlike the DeFiLlama half, no bespoke rollup math is needed here:
    collectors/binance_futures.py already returns timestamp-aligned OI +
    price points ready to save as-is. This just asks for the entire ~30-day
    archive Binance actually has (period=4h, limit=500 - see module
    docstring) instead of the ~36h window main.py's daily cycle uses.
    """
    try:
        records = binance_futures.collect(
            symbols,
            lookback_hours=BINANCE_BACKFILL_LOOKBACK_HOURS,
            period=BINANCE_BACKFILL_PERIOD,
            limit=BINANCE_BACKFILL_LIMIT,
        )
    except Exception:
        logger.exception("Binance Futures backfill: collect() failed for the whole watchlist")
        return

    if not records:
        logger.warning("Binance Futures backfill: 0 points collected for any symbol")
        return

    save_binance_oi_snapshots(conn, records)

    by_symbol: dict[str, list[dict]] = {}
    for record in records:
        by_symbol.setdefault(record["symbol"], []).append(record)

    missing = sorted(set(symbols) - set(by_symbol))
    if missing:
        logger.warning(
            "Binance Futures backfill: no data at all for %d/%d symbols: %s",
            len(missing), len(symbols), missing,
        )

    for symbol in sorted(by_symbol):
        points = by_symbol[symbol]
        oldest = min(p["oi_timestamp"] for p in points)
        newest = max(p["oi_timestamp"] for p in points)
        logger.info(
            "Binance Futures backfill: '%s' saved %d point(s) (%s to %s)",
            symbol, len(points), oldest, newest,
        )


def main() -> None:
    cfg = load_config()
    db_path = Path(cfg["storage"]["sqlite_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    init_db(db_path)

    conn = get_connection(db_path)
    try:
        logger.info("=== DeFiLlama + CoinGecko revenue/mcap history backfill ===")
        try:
            protocols_by_slug = defillama.fetch_protocols()
        except RuntimeError:
            logger.exception(
                "Could not fetch DeFiLlama /protocols (needed for token symbols) - "
                "continuing without symbols, rows will still be saved"
            )
            protocols_by_slug = {}

        watchlist = cfg["watchlist"]["defillama_protocols"]
        for i, slug in enumerate(watchlist, 1):
            logger.info("[%d/%d] %s", i, len(watchlist), slug)
            try:
                backfill_defillama_protocol(conn, slug, protocols_by_slug.get(slug))
            except Exception:
                logger.exception(
                    "Unexpected error backfilling '%s' - skipping, rest of the watchlist continues",
                    slug,
                )

        logger.info("=== Binance Futures OI/price history backfill ===")
        symbols = cfg["watchlist"]["binance_futures_symbols"]
        logger.info(
            "%d symbols, period=%s, ~30 days of history available",
            len(symbols), BINANCE_BACKFILL_PERIOD,
        )
        backfill_binance(conn, symbols)

        logger.info("Backfill run complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Backfill script failed")
        sys.exit(1)
