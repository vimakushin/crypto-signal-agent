"""CoinGecko collector (TZ section 3): historical price / market cap / volume
per coin, from the free, no-key `market_chart` endpoint.

Used by scripts/backfill_history.py (to get historical market cap so
backfilled DeFiLlama revenue rows can be paired with a market cap the way
collectors/defillama.py pairs its own live rows) and by main.py's daily
DeFiLlama cycle + collect_price_volume_history() below (TZ 4.7's
volume_breakout signal). This module is deliberately not shaped around
either of those two callers: signals/sector_rotation.py (TZ 4.6, not built
yet) will need the same daily price/volume history later too, and should be
able to call fetch_market_chart() (or collect_price_volume_history())
directly instead of this getting rewritten.

Two things this collector has to work around, both confirmed by live checks
before writing this file:
  - No API key on the free tier, but requests without a `User-Agent` header
    can get rejected - a fixed custom header is sent on every request.
  - The free tier is aggressively rate-limited by IP, not by API key
    (there is no key). `days=365` (daily granularity, the coarsest/cheapest
    query CoinGecko offers - anything <=90 days switches to hourly and
    returns far more points) works reliably; `days=max` hits HTTP 429
    almost immediately and is not used here. On top of picking a cheap
    query, every request (including retries) is paced through a
    module-level minimum interval so a caller that fetches many coins in a
    loop (as scripts/backfill_history.py does) can't outrun the rate limit
    just by calling this function back-to-back - callers don't have to
    remember to sleep() between coins themselves. A 429 response is also
    treated differently from a generic network error: it gets a much
    longer, dedicated backoff (honoring the `Retry-After` header when
    CoinGecko sends one) instead of the short backoff used for a plain
    timeout/connection error, since a rate limit doesn't clear in a couple
    of seconds.
"""
from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.coingecko.com/api/v3"

# CoinGecko has no free-tier API key to send, but an empty/absent User-Agent
# (e.g. requests' own default "python-requests/2.x") can get a request
# rejected - any descriptive, non-default value works.
HEADERS = {"User-Agent": "crypto-signal-agent/0.1 (personal research project)"}

MAX_RETRIES = 5
REQUEST_TIMEOUT = 30

# Backoff for a plain network error (timeout, connection reset, bad JSON) -
# short, same convention as collectors/defillama.py and
# collectors/binance_futures.py, since these usually clear in seconds.
BACKOFF_SECONDS = 5

# Backoff specifically for HTTP 429 (rate limited) - deliberately much
# longer than BACKOFF_SECONDS. CoinGecko's free-tier limit does not reset
# in a couple of seconds the way a transient network hiccup does; retrying
# on the same short schedule just burns through MAX_RETRIES against a limit
# that hasn't lifted yet. Used only when the response doesn't carry its own
# `Retry-After` header.
RATE_LIMIT_BACKOFF_SECONDS = 20

# Minimum gap enforced between the start of any two requests this module
# makes (including retries), regardless of caller. Keeps normal, non-429
# usage comfortably under the free-tier rate limit in the first place,
# instead of only reacting to 429s after they already happened.
MIN_REQUEST_INTERVAL_SECONDS = 1.5

# See module docstring: days=max was observed hitting 429 almost
# immediately in a live check; 365 (still daily granularity) is the largest
# window treated as safe here.
MAX_SAFE_DAYS = 365

_last_request_at: float = 0.0


def _throttle() -> None:
    """Sleep just long enough to keep this call at least
    MIN_REQUEST_INTERVAL_SECONDS after the previous one this process made.

    Module-level (not per-coin) on purpose - CoinGecko's free-tier rate
    limit is per source IP, not per coin, so pacing has to be global across
    every call this collector makes, not reset per gecko_id.
    """
    global _last_request_at
    elapsed = time.monotonic() - _last_request_at
    wait = MIN_REQUEST_INTERVAL_SECONDS - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()


def _get_with_retry(url: str, params: dict) -> dict:
    """GET with retry + backoff, with HTTP 429 handled separately from a
    generic network error (see RATE_LIMIT_BACKOFF_SECONDS).

    Args:
        url: endpoint to call.
        params: query parameters.

    Returns:
        Parsed JSON body.

    Raises:
        RuntimeError: if every retry attempt failed (network error or
            still-429 after MAX_RETRIES attempts).
    """
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        _throttle()
        try:
            response = requests.get(
                url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT
            )
            if response.status_code == 429:
                retry_after_header = response.headers.get("Retry-After")
                if retry_after_header and retry_after_header.strip().isdigit():
                    wait_seconds = float(retry_after_header)
                    wait_source = "Retry-After header"
                else:
                    wait_seconds = RATE_LIMIT_BACKOFF_SECONDS * attempt
                    wait_source = "default rate-limit backoff"
                last_error = RuntimeError(f"HTTP 429 from {url}")
                logger.warning(
                    "CoinGecko rate-limited (429) on attempt %s/%s for %s %s - "
                    "waiting %.1fs (%s) before retry",
                    attempt, MAX_RETRIES, url, params, wait_seconds, wait_source,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning(
                "CoinGecko request failed (attempt %s/%s) for %s %s: %s",
                attempt, MAX_RETRIES, url, params, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)

    logger.error("CoinGecko request permanently failed for %s %s: %s", url, params, last_error)
    raise RuntimeError(f"CoinGecko request failed for {url} {params}") from last_error


def fetch_market_chart(
    gecko_id: str, days: int = MAX_SAFE_DAYS, vs_currency: str = "usd"
) -> list[dict]:
    """Historical price/market-cap/volume for one coin.

    Args:
        gecko_id: CoinGecko coin id (e.g. "lido-dao") - not a ticker symbol.
        days: how many days of history to request, ending "now". Clamped to
            MAX_SAFE_DAYS with a warning if a caller passes more - see
            module docstring for why (days=max was observed to 429
            immediately). Values above 90 return daily granularity (one
            point/day), which is what this function assumes when it derives
            `date` below.
        vs_currency: fiat/crypto quote currency, "usd" for everything this
            project needs so far.

    Returns:
        List of dicts sorted oldest-to-newest, one per CoinGecko data point,
        each with:
          - timestamp: ISO 8601 UTC timestamp of the point, as reported by
            CoinGecko.
          - date: "YYYY-MM-DD" UTC calendar date of `timestamp` - convenient
            for matching against another daily-granularity series (e.g.
            DeFiLlama's daily revenue chart) by calendar day.
          - price: float.
          - market_cap: float, or None if CoinGecko didn't report one for
            this exact timestamp (rare, but its three series are looked up
            independently below rather than assumed to always be the same
            length, so a gap in one doesn't misalign the others).
          - volume: float, or None (same caveat as market_cap).
    """
    if days > MAX_SAFE_DAYS:
        logger.warning(
            "CoinGecko: requested days=%s for %s exceeds the %s-day cap this collector "
            "treats as safe (see module docstring) - clamping to %s",
            days, gecko_id, MAX_SAFE_DAYS, MAX_SAFE_DAYS,
        )
        days = MAX_SAFE_DAYS

    url = f"{BASE_URL}/coins/{gecko_id}/market_chart"
    data = _get_with_retry(url, {"vs_currency": vs_currency, "days": days})

    prices = data.get("prices", [])
    mcap_by_ts = {int(ts): value for ts, value in data.get("market_caps", [])}
    volume_by_ts = {int(ts): value for ts, value in data.get("total_volumes", [])}

    records: list[dict] = []
    for ts_ms, price in prices:
        ts_ms_int = int(ts_ms)
        point_dt = datetime.fromtimestamp(ts_ms_int / 1000, tz=timezone.utc)
        records.append({
            "timestamp": point_dt.isoformat(),
            "date": point_dt.date().isoformat(),
            "price": price,
            "market_cap": mcap_by_ts.get(ts_ms_int),
            "volume": volume_by_ts.get(ts_ms_int),
        })

    return records


def collect_price_volume_history(
    slug_to_gecko_id: dict[str, str | None], days: int = MAX_SAFE_DAYS
) -> list[dict]:
    """Daily price/volume history for a set of DeFiLlama protocols, ready to
    save via storage/db.py's save_coingecko_price_history() (TZ 4.7's
    volume_breakout signal).

    Used by main.py's daily DeFiLlama cycle and by scripts/backfill_history.py
    for the same table - both need the exact same "one row per calendar
    day, with a finite price/volume or NULL" shape. Never raises for one
    bad/missing gecko_id (rate limit exhausted after retries, or DeFiLlama
    has no gecko_id on file for a protocol) - logged and skipped, so one bad
    coin doesn't stop the rest of the watchlist, the same convention as
    every other collect()-style function in this project.

    Args:
        slug_to_gecko_id: DeFiLlama protocol slug -> CoinGecko coin id, e.g.
            from collectors/defillama.py's collect() (which resolves
            gecko_id from the same /protocols response it already fetches
            for market cap - no extra network call needed there). A slug
            mapped to None/"" is skipped with a warning.
        days: forwarded to fetch_market_chart on every call - see that
            function's docstring for why this must stay > 90 (anything <=90
            switches CoinGecko to hourly-granularity points instead of one
            per calendar day, which would silently break this function's
            "one row per day" contract, and would also let INSERT OR
            IGNORE's UNIQUE(gecko_id, date) constraint pick an arbitrary
            hour of the day as "the" row for that date instead of a full
            day's own point). Defaults to MAX_SAFE_DAYS (365), the same
            value scripts/backfill_history.py uses for its own initial
            history load. Re-fetching the full ~365-day window on every
            daily call is redundant bandwidth (most of those points already
            exist in storage from a previous run), but it is cheap and safe
            because of INSERT OR IGNORE, and it avoids inventing a second,
            smaller "top-up" query shape that would need its own case to
            stay above the 90-day granularity threshold.

    Returns:
        List of dicts, one per (gecko_id, historical calendar date) point
        CoinGecko returned, each with: gecko_id, protocol_slug, date
        ("YYYY-MM-DD"), fetched_at (ISO timestamp of THIS collection run -
        not the historical date), price (float or None), volume (float or
        None). A NaN/Infinity price or volume is stored as None for that
        field, not the raw non-finite value - JSON allows NaN/Infinity, and
        `float('nan') < x` and friends don't raise, so an unfiltered
        NaN/Infinity would otherwise flow silently into storage/db.py and
        then poison every max()/average signals/volume_breakout.py computes
        from it (same risk scripts/backfill_history.py's
        _build_revenue_by_date already guards DeFiLlama's revenue numbers
        against - see that function's docstring).
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []

    for slug, gecko_id in slug_to_gecko_id.items():
        if not gecko_id:
            logger.warning(
                "CoinGecko: no gecko_id for protocol '%s' - skipping price/volume history",
                slug,
            )
            continue

        try:
            market_chart = fetch_market_chart(gecko_id, days=days)
        except RuntimeError:
            logger.error(
                "CoinGecko: could not fetch price/volume history for '%s' (%s) - skipping",
                slug, gecko_id,
            )
            continue

        for point in market_chart:
            price = point["price"]
            volume = point["volume"]

            if price is not None and (math.isnan(price) or math.isinf(price)):
                logger.warning(
                    "CoinGecko: '%s' (%s) has a non-finite price value (%s) on %s - "
                    "storing NULL for price on this date instead of a poisoned number",
                    slug, gecko_id, price, point["date"],
                )
                price = None
            if volume is not None and (math.isnan(volume) or math.isinf(volume)):
                logger.warning(
                    "CoinGecko: '%s' (%s) has a non-finite volume value (%s) on %s - "
                    "storing NULL for volume on this date instead of a poisoned number",
                    slug, gecko_id, volume, point["date"],
                )
                volume = None

            records.append({
                "gecko_id": gecko_id,
                "protocol_slug": slug,
                "date": point["date"],
                "fetched_at": fetched_at,
                "price": price,
                "volume": volume,
            })

    return records
