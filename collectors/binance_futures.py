"""Binance Futures collector (TZ section 4.1): Open Interest history + price,
aligned on the same timestamps, for the OI/price divergence signal.

TZ section 3 originally named Coinglass as the source for this signal, but
Coinglass no longer has a free API tier at all - the cheapest paid plan is
$29/mo. Replaced with Binance Futures public endpoints, which need no API
key and are only rate-limited by IP:
  - GET /futures/data/openInterestHist: historical total Open Interest for a
    symbol at regular intervals, going back roughly a month depending on
    `period` - free, no key.
  - GET /fapi/v1/klines: historical candles, used to read the close price at
    the same timestamps as the OI history so both series line up point for
    point.

Unlike collectors/defillama.py (which has no historical market cap and must
wait for its own accumulated snapshot history to build up over several
days), Binance already serves 24-48h+ of OI and price history in a single
call. So this collector fetches the whole lookback window every run, not
just "now" - the OI-divergence signal (signals/oi_divergence.py, a future
task) can be computed from the very first run, and every point collected is
still saved to storage/db.py so history keeps accumulating for backtesting
(TZ section 6/7).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://fapi.binance.com"
OPEN_INTEREST_HIST_URL = f"{BASE_URL}/futures/data/openInterestHist"
KLINES_URL = f"{BASE_URL}/fapi/v1/klines"

MAX_RETRIES = 3
BACKOFF_SECONDS = 5
REQUEST_TIMEOUT = 15

# Bucket size for both the OI history and the klines call - must match
# between the two so their timestamps line up. One of Binance's accepted
# values for both endpoints: "5m","15m","30m","1h","2h","4h","6h","12h","1d".
DEFAULT_PERIOD = "1h"

_PERIOD_TO_MS = {
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}


def _period_to_ms(period: str) -> int:
    """Convert a Binance OI-history/klines period string to milliseconds.

    Used to size the "close enough" window in _closest_close_price - fails
    loudly on an unsupported period instead of silently comparing against a
    wrong/missing threshold.

    Args:
        period: bucket size, one of _PERIOD_TO_MS's keys.

    Returns:
        Bucket length in milliseconds.

    Raises:
        ValueError: if `period` isn't one of the values this collector (and
            the two Binance endpoints it calls) supports.
    """
    try:
        return _PERIOD_TO_MS[period]
    except KeyError:
        raise ValueError(
            f"Unsupported period '{period}' - must be one of {sorted(_PERIOD_TO_MS)}"
        ) from None


def _get_with_retry(url: str, params: dict) -> list:
    """GET with retry + backoff, same convention as collectors/defillama.py:
    log every failed attempt, only raise once retries are exhausted, so a
    down or rate-limited source is logged rather than silently skipped
    (project rule, TZ section 7).

    Args:
        url: endpoint to call.
        params: query parameters.

    Returns:
        Parsed JSON body (Binance returns a list for both endpoints used
        here).

    Raises:
        RuntimeError: if every retry attempt failed.
    """
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning(
                "Binance Futures request failed (attempt %s/%s) for %s %s: %s",
                attempt, MAX_RETRIES, url, params, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)
    logger.error(
        "Binance Futures request permanently failed for %s %s: %s",
        url, params, last_error,
    )
    raise RuntimeError(f"Binance Futures request failed for {url} {params}") from last_error


def fetch_open_interest_hist(
    symbol: str, period: str = DEFAULT_PERIOD, limit: int = 49
) -> list[dict]:
    """Historical total Open Interest for one futures symbol.

    Args:
        symbol: Binance Futures ticker, e.g. "BTCUSDT".
        period: bucket size Binance accepts for this endpoint.
        limit: number of buckets to fetch (Binance max is 500).

    Returns:
        List of dicts sorted oldest-to-newest, each with "symbol",
        "sumOpenInterest" (OI in base-asset units, string), "sumOpenInterestValue"
        (OI in USDT, string), "timestamp" (ms, int).
    """
    return _get_with_retry(
        OPEN_INTEREST_HIST_URL,
        {"symbol": symbol, "period": period, "limit": limit},
    )


def fetch_klines(symbol: str, interval: str = DEFAULT_PERIOD, limit: int = 49) -> list[list]:
    """Historical candles for one symbol, used to read the close price at
    the same timestamps as the OI history.

    Args:
        symbol: Binance Futures ticker, e.g. "BTCUSDT".
        interval: candle size - pass the same value used for `period` in
            fetch_open_interest_hist so the two series' timestamps line up.
        limit: number of candles to fetch (Binance max is 1500).

    Returns:
        List of raw Binance kline arrays, sorted oldest-to-newest:
        [openTime, open, high, low, close, volume, closeTime, ...].
    """
    return _get_with_retry(
        KLINES_URL,
        {"symbol": symbol, "interval": interval, "limit": limit},
    )


def _closest_close_price(
    klines: list[list], target_ms: int, period_ms: int, symbol: str
) -> float | None:
    """Close price of the kline whose open time is nearest to target_ms.

    The OI history and klines are fetched with the same bucket size, so in
    practice their timestamps match exactly - `min()` here is a safety net
    for the rare case the two endpoints are off by a bucket at the current
    edge. That safety net only holds if the "closest" candle is actually
    close: if `klines` came back short or gapped (e.g. Binance didn't have
    the full window for this symbol), `min()` would otherwise happily
    return some candle hours away and silently pair it with the wrong OI
    point. So anything further than half a bucket from `target_ms` is
    treated as no match at all, not a best-effort guess.

    Args:
        klines: raw kline list from fetch_klines.
        target_ms: OI history point's timestamp, in epoch milliseconds.
        period_ms: bucket length in milliseconds (see _period_to_ms) - the
            match must land within half a bucket of `target_ms`.
        symbol: only used for the log message if the match is rejected.

    Returns:
        Close price as float, or None if `klines` is empty or the nearest
        candle is too far from `target_ms` to trust as a real match.
    """
    if not klines:
        logger.warning(
            "Binance Futures: no klines at all for %s, can't price OI point %s",
            symbol, target_ms,
        )
        return None
    best = min(klines, key=lambda k: abs(k[0] - target_ms))
    diff_ms = abs(best[0] - target_ms)
    if diff_ms > period_ms / 2:
        logger.warning(
            "Binance Futures: closest candle for %s is %sms away from OI point "
            "%s (bucket=%sms) - too far to trust as a match, dropping point",
            symbol, diff_ms, target_ms, period_ms,
        )
        return None
    return float(best[4])


def collect(
    symbols: list[str],
    lookback_hours: int = 48,
    period: str = DEFAULT_PERIOD,
    limit: int | None = None,
) -> list[dict]:
    """Fetch aligned Open-Interest + price history for each watchlist symbol.

    Never raises for a single bad symbol (delisted ticker, source down after
    retries) - that symbol is logged and skipped so the rest of the
    watchlist still gets collected (TZ section 7: log data-source failures,
    don't stay silent, but don't let one bad ticker kill the whole run
    either).

    Args:
        symbols: Binance Futures USDT-M perpetual tickers, e.g. ["BTCUSDT"].
        lookback_hours: how far back to fetch, matching TZ 4.1's 24-48h
            comparison window. A couple of extra buckets are fetched on top
            as margin (unless `limit` overrides this - see below).
        period: OI-history bucket size; klines are fetched with the same
            value as `interval` so both series share timestamps.
        limit: overrides the auto-computed `lookback_hours + 2` bucket
            count sent to Binance. Needed because openInterestHist hard-caps
            `limit` at 500 regardless of what's requested (confirmed live) -
            a caller asking for a wide window that computes to more than
            500 buckets (e.g. lookback_hours=720 at period="4h" computes to
            722) must pass limit=500 explicitly here, or the request is
            rejected outright instead of just returning fewer points.
            Defaults to None, which keeps the existing lookback_hours + 2
            auto-sizing used by main.py's daily/6-hourly cycles (always
            comfortably under 500 there) - only scripts/backfill_history.py
            needs to pass this.

    Returns:
        One dict per (symbol, timestamp) point Binance actually returned,
        sorted oldest-to-newest within each symbol, each with:
          - symbol: str
          - fetched_at: ISO timestamp of when this collector run polled
          - oi_timestamp: ISO timestamp of this specific OI/price point
          - oi: float, sum Open Interest in base-asset units
          - oi_value_usdt: float, sum Open Interest in USDT
          - price: float, close price of the matching candle

        This is not the signal itself (TZ 4.1's oi_growth_pct / price_change_pct
        aren't computed here) - it's the raw, timestamp-aligned series that
        makes that calculation trivial: the caller picks the newest point
        and the point ~lookback_hours before it.
    """
    effective_limit = limit if limit is not None else lookback_hours + 2
    period_ms = _period_to_ms(period)
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []

    for symbol in symbols:
        try:
            oi_hist = fetch_open_interest_hist(symbol, period=period, limit=effective_limit)
        except RuntimeError:
            logger.error(
                "Binance Futures: skipping '%s' - Open Interest history unavailable",
                symbol,
            )
            continue
        if not oi_hist:
            logger.warning(
                "Binance Futures: empty Open Interest history for '%s' "
                "(symbol may not exist or may not be an active perpetual)",
                symbol,
            )
            continue

        try:
            klines = fetch_klines(symbol, interval=period, limit=effective_limit)
        except RuntimeError:
            logger.error("Binance Futures: skipping '%s' - klines unavailable", symbol)
            continue

        for point in oi_hist:
            # A single malformed point (missing/null field - happens at the
            # edges of a source's history) must drop only that point, not
            # the data already collected for this symbol or earlier ones -
            # this loop used to have no guard at all, so one bad point would
            # raise out of collect() and silently lose every prior symbol's
            # records too (they're only returned once, at the very end).
            try:
                timestamp_ms = point["timestamp"]
                oi = float(point["sumOpenInterest"])
                oi_value_usdt = float(point["sumOpenInterestValue"])
                oi_timestamp_iso = datetime.fromtimestamp(
                    timestamp_ms / 1000, tz=timezone.utc
                ).isoformat()
            except (KeyError, TypeError, ValueError, OSError, OverflowError) as exc:
                logger.warning(
                    "Binance Futures: malformed Open Interest point for %s (%r), "
                    "dropping point: %s",
                    symbol, point, exc,
                )
                continue

            # _closest_close_price already logs the specific reason (no
            # klines at all, or nearest candle too far to trust) - nothing
            # more to log here, just drop the point.
            price = _closest_close_price(klines, timestamp_ms, period_ms, symbol)
            if price is None:
                continue

            records.append({
                "symbol": symbol,
                "fetched_at": fetched_at,
                "oi_timestamp": oi_timestamp_iso,
                "oi": oi,
                "oi_value_usdt": oi_value_usdt,
                "price": price,
            })

    return records
