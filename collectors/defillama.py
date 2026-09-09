"""DeFiLlama collector (TZ section 3 / 4.5): fees, revenue and market cap.

Free API, no key required. Two endpoints are combined:
  - /overview/fees?dataType=dailyRevenue: revenue per protocol, with 7d/30d
    growth percentages already computed by DeFiLlama.
  - /protocols: token market cap per protocol.

A third function, fetch_protocol_daily_revenue_history(), is used only by
scripts/backfill_history.py to pull years of daily revenue for one protocol
at a time (a different endpoint - see its docstring) - not part of the
regular collect() cycle below.

Known MVP limitation: DeFiLlama tracks fees per protocol *version*
(e.g. "uniswap-v3", "aave-v3"), but a token's market cap is only attached to
a single parent entity that the free API doesn't expose in a joinable form.
So protocols whose fees-slug doesn't also carry its own market cap (checked
against /protocols) are logged and skipped rather than guessed at. The 25
protocols in config.yaml were chosen specifically because they resolve
cleanly on both endpoints.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

FEES_URL = "https://api.llama.fi/overview/fees?dataType=dailyRevenue"
PROTOCOLS_URL = "https://api.llama.fi/protocols"
DAILY_REVENUE_HISTORY_URL_TEMPLATE = "https://api.llama.fi/summary/fees/{slug}?dataType=dailyRevenue"

MAX_RETRIES = 3
BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 30


def _get_with_retry(url: str) -> dict | list:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning(
                "DeFiLlama request failed (attempt %s/%s) for %s: %s",
                attempt, MAX_RETRIES, url, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)
    logger.error("DeFiLlama request permanently failed for %s: %s", url, last_error)
    raise RuntimeError(f"DeFiLlama request failed for {url}") from last_error


def fetch_revenue_overview() -> dict[str, dict]:
    data = _get_with_retry(FEES_URL)
    return {p["slug"]: p for p in data.get("protocols", [])}


def fetch_protocols() -> dict[str, dict]:
    data = _get_with_retry(PROTOCOLS_URL)
    return {p["slug"]: p for p in data}


def fetch_protocol_daily_revenue_history(slug: str) -> dict:
    """Full daily revenue history for one protocol - used by
    scripts/backfill_history.py, NOT by the regular collect() cycle below
    (which only needs DeFiLlama's already-computed 7d/30d rollups from
    fetch_revenue_overview()).

    Hits a different DeFiLlama endpoint than the rest of this module
    (/summary/fees/{slug} instead of /overview/fees): a per-protocol page
    that serves years of daily granularity instead of one rolled-up
    snapshot for every protocol at once. It also conveniently carries the
    protocol's CoinGecko id (`gecko_id`) in the same response, so a
    historical backfill doesn't need a separate slug-to-gecko-id mapping
    step.

    Args:
        slug: DeFiLlama protocol slug (config.yaml watchlist.defillama_protocols).

    Returns:
        Dict with:
          - gecko_id: str | None - CoinGecko coin id for this protocol's
            token, as reported by DeFiLlama itself (None if DeFiLlama
            doesn't have one on file for this protocol - the caller then
            has no way to look up historical market cap for it).
          - name: str | None, category: str | None - same fields
            fetch_revenue_overview() entries carry, relayed as-is.
          - daily_revenue: list of (date, revenue_usd) tuples, sorted
            oldest-to-newest, where `date` is a "YYYY-MM-DD" UTC calendar
            date string and `revenue_usd` a float.
    """
    url = DAILY_REVENUE_HISTORY_URL_TEMPLATE.format(slug=slug)
    data = _get_with_retry(url)
    chart = data.get("totalDataChart", [])

    daily_revenue: list[tuple[str, float]] = []
    for point in chart:
        # A single malformed/non-numeric point (DeFiLlama has been observed
        # to carry a null revenue value on some protocols/dates) must drop
        # only that one date, not the whole protocol's history - same
        # principle as collectors/binance_futures.py's per-point guard.
        # Without this, a TypeError from float(None) would propagate out of
        # the sorted(...) generator, past this function, past
        # scripts/backfill_history.py's `except RuntimeError` (a TypeError
        # isn't one), and get caught only by main()'s broad `except
        # Exception` there - silently discarding every OTHER, perfectly
        # good year of history for this protocol along with the one bad day.
        try:
            ts, value = point
            date_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
            revenue = float(value)
        except (TypeError, ValueError, OSError, OverflowError) as exc:
            logger.warning(
                "DeFiLlama: malformed daily revenue point for '%s' (%r), dropping point: %s",
                slug, point, exc,
            )
            continue
        daily_revenue.append((date_str, revenue))

    daily_revenue.sort(key=lambda pair: pair[0])
    return {
        "gecko_id": data.get("gecko_id"),
        "name": data.get("name"),
        "category": data.get("category"),
        "daily_revenue": daily_revenue,
    }


def collect(watchlist_slugs: list[str]) -> list[dict]:
    """Fetch revenue/fees + market cap for each watchlist protocol.

    Never raises on a per-protocol miss (TZ section 7: log data-source
    failures, don't stay silent) - only a full request failure propagates.

    Each record also carries a "gecko_id" field, read from the same
    /protocols response already fetched here for `mcap` (DeFiLlama's public
    protocol listing includes a `gecko_id` field alongside `mcap`/`symbol`
    for protocols it can resolve to a CoinGecko coin - the same field
    fetch_protocol_daily_revenue_history() already relays from a different,
    heavier endpoint for scripts/backfill_history.py). Reading it off this
    already-fetched response costs no extra network call. main.py's daily
    DeFiLlama cycle uses this field to resolve each watchlist protocol's
    CoinGecko coin id for collectors/coingecko.py's
    collect_price_volume_history() (TZ 4.7's volume_breakout signal),
    instead of re-fetching a per-protocol page just for this one field.
    NOTE: this assumes DeFiLlama's live /protocols response actually
    carries `gecko_id` on each entry - if a live run logs "no gecko_id" for
    protocols that do have known CoinGecko coins, that assumption needs
    revisiting (e.g. falling back to fetch_protocol_daily_revenue_history's
    gecko_id, or a manual slug -> gecko_id mapping in config.yaml).
    """
    fees_by_slug = fetch_revenue_overview()
    protocols_by_slug = fetch_protocols()
    fetched_at = datetime.now(timezone.utc).isoformat()

    records = []
    for slug in watchlist_slugs:
        fee_entry = fees_by_slug.get(slug)
        if fee_entry is None:
            logger.error("DeFiLlama: no fees/revenue data for watchlist protocol '%s'", slug)
            continue

        protocol_entry = protocols_by_slug.get(slug)
        mcap = protocol_entry.get("mcap") if protocol_entry else None
        if not mcap:
            logger.warning(
                "DeFiLlama: no resolvable market cap for '%s' (likely tracked as a "
                "versioned sub-protocol) - snapshot saved without mcap, "
                "revenue_price_gap will skip it until fixed",
                slug,
            )

        gecko_id = protocol_entry.get("gecko_id") if protocol_entry else None
        if not gecko_id:
            logger.warning(
                "DeFiLlama: no gecko_id on file for '%s' - volume_breakout's CoinGecko "
                "price/volume history can't be collected for this protocol until it is",
                slug,
            )

        records.append({
            "slug": slug,
            "symbol": protocol_entry.get("symbol") if protocol_entry else None,
            "name": fee_entry.get("name"),
            "category": fee_entry.get("category"),
            "fetched_at": fetched_at,
            "revenue_total_7d": fee_entry.get("total7d"),
            "revenue_total_30d": fee_entry.get("total30d"),
            "revenue_change_7d_pct": fee_entry.get("change_7d"),
            "revenue_change_30d_pct": fee_entry.get("change_30dover30d"),
            "mcap": mcap,
            "gecko_id": gecko_id,
        })

    return records
