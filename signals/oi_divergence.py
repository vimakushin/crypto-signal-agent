"""Signal 4.1: Open Interest / price divergence.

Condition (TZ section 4.1): Open Interest grew by >= X% over
`lookback_hours` (24-48h range per the TZ), while price moved by LESS than
Y% over the same window - no direction requirement on price, unlike
revenue_price_gap's mcap-vs-revenue comparison: here it's just
|price_change_pct| < Y%. The idea is leveraged positioning building up
("someone is loading up") while the price chart hasn't "shown up" yet.

Uses the OI history collectors/binance_futures.py already fetches and
storage/db.py already stores (binance_oi_snapshots) - this module only
computes the comparison, it never talks to the network itself.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime

from storage.db import get_binance_oi_snapshot_hours_before, get_latest_binance_oi_snapshot


@dataclass
class OiDivergenceSignal:
    symbol: str
    # Actual elapsed time between the two compared snapshots, in hours - NOT
    # the configured lookback_hours from config.yaml. Storage can have gaps
    # (collectors/binance_futures.py legitimately drops a point when no
    # candle price matches closely enough), so the closest available
    # "earlier" snapshot can sit further back than requested. Reporting the
    # configured number here instead of what actually elapsed would silently
    # misstate the window a calibrated threshold (e.g. 30% over 36h) was
    # applied to - see _hours_between.
    lookback_hours: float
    oi_growth_pct: float
    price_change_pct: float
    oi_now: float
    oi_before: float
    price_now: float
    price_before: float


@dataclass
class OiDivergenceScanResult:
    """Result of scanning the whole watchlist - keeps "not enough data yet"
    separate from "evaluated, but below threshold" so main.py can log an
    accurate reason instead of a single ambiguous "no candidates" line.
    """
    signals: list[OiDivergenceSignal] = field(default_factory=list)
    # Symbols skipped because there's no stored snapshot yet, or no stored
    # snapshot old enough to compare against (first runs / short history).
    insufficient_history: list[str] = field(default_factory=list)


def _hours_between(later_iso: str, earlier_iso: str) -> float:
    """Actual elapsed time between two stored OI snapshot timestamps.

    collectors/binance_futures.py can legitimately drop individual OI
    points (e.g. no matching candle within half a bucket - see its
    `_closest_close_price`), so stored history can have gaps.
    `get_binance_oi_snapshot_hours_before` returns the closest point AT OR
    BEFORE the target time, not necessarily exactly at it - if a gap pushed
    it further back, the real elapsed window can be noticeably wider than
    `lookback_hours` from config.yaml. Callers must use this actual value
    when reporting the signal, not the configured one, so a threshold
    calibrated for e.g. 36h is never silently applied to, say, a 45h window
    without saying so.

    Args:
        later_iso: ISO 8601 timestamp of the more recent snapshot.
        earlier_iso: ISO 8601 timestamp of the older snapshot.

    Returns:
        Elapsed time between the two timestamps, in hours.
    """
    later = datetime.fromisoformat(later_iso)
    earlier = datetime.fromisoformat(earlier_iso)
    return (later - earlier).total_seconds() / 3600


def _evaluate_snapshots(
    latest: sqlite3.Row,
    earlier: sqlite3.Row,
    oi_growth_threshold_pct: float,
    price_change_threshold_pct: float,
    actual_lookback_hours: float,
) -> OiDivergenceSignal | None:
    """Compare one (latest, earlier) pair of stored OI/price points.

    Args:
        latest: most recent binance_oi_snapshots row for a symbol.
        earlier: the row closest to (latest - lookback_hours) for the same
            symbol.
        oi_growth_threshold_pct: minimum required OI growth over the window,
            in percent (config.yaml signals.oi_divergence.oi_growth_threshold_pct).
        price_change_threshold_pct: price must move by strictly less than
            this, in percent, in either direction
            (config.yaml signals.oi_divergence.price_change_threshold_pct).
        actual_lookback_hours: real elapsed time between `earlier` and
            `latest` (see _hours_between) - carried through to the returned
            signal so a stored gap never gets silently reported under the
            configured window instead of the real one.

    Returns:
        An OiDivergenceSignal if both conditions hold, otherwise None -
        including when either point has a zero/missing OI or price, which
        would make the growth-% math divide by zero.
    """
    oi_now = latest["oi"]
    oi_before = earlier["oi"]
    price_now = latest["price"]
    price_before = earlier["price"]

    if not oi_now or not oi_before or not price_now or not price_before:
        return None

    oi_growth_pct = (oi_now - oi_before) / oi_before * 100
    if oi_growth_pct < oi_growth_threshold_pct:
        return None

    price_change_pct = (price_now - price_before) / price_before * 100
    if abs(price_change_pct) >= price_change_threshold_pct:
        return None

    return OiDivergenceSignal(
        symbol=latest["symbol"],
        lookback_hours=actual_lookback_hours,
        oi_growth_pct=oi_growth_pct,
        price_change_pct=price_change_pct,
        oi_now=oi_now,
        oi_before=oi_before,
        price_now=price_now,
        price_before=price_before,
    )


def scan_watchlist(
    conn: sqlite3.Connection,
    symbols: list[str],
    oi_growth_threshold_pct: float,
    price_change_threshold_pct: float,
    lookback_hours: int,
) -> OiDivergenceScanResult:
    """Run the signal for every watchlist symbol using stored OI history.

    Args:
        conn: open storage/db.py connection.
        symbols: Binance Futures tickers to scan (config.yaml
            watchlist.binance_futures_symbols).
        oi_growth_threshold_pct: see _evaluate_snapshots.
        price_change_threshold_pct: see _evaluate_snapshots.
        lookback_hours: target comparison window, used to look up the
            "earlier" stored point (config.yaml
            signals.oi_divergence.lookback_hours) - the actual elapsed time
            between the two points used for evaluation and reporting may
            differ slightly if the stored history has gaps; see
            _hours_between.

    Returns:
        An OiDivergenceScanResult: `.signals` for every symbol that
        triggered, and `.insufficient_history` for symbols skipped because
        there's no stored snapshot yet, or none old enough to compare
        against `lookback_hours` - not expected to happen in practice since
        collectors/binance_futures.py fetches the whole lookback window on
        its very first run, but guarded anyway in case a symbol's history
        is short (new listing, source gap, or today's collection failed -
        see main.py's handling of that case).
    """
    signals: list[OiDivergenceSignal] = []
    insufficient_history: list[str] = []

    for symbol in symbols:
        latest = get_latest_binance_oi_snapshot(conn, symbol)
        if latest is None:
            insufficient_history.append(symbol)
            continue

        earlier = get_binance_oi_snapshot_hours_before(
            conn, symbol, latest["oi_timestamp"], lookback_hours
        )
        if earlier is None:
            insufficient_history.append(symbol)
            continue

        actual_lookback_hours = _hours_between(latest["oi_timestamp"], earlier["oi_timestamp"])
        signal = _evaluate_snapshots(
            latest, earlier,
            oi_growth_threshold_pct, price_change_threshold_pct, actual_lookback_hours,
        )
        if signal is not None:
            signals.append(signal)

    return OiDivergenceScanResult(signals=signals, insufficient_history=insufficient_history)
