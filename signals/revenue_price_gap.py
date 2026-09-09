"""Signal 4.5: protocol revenue grows while market cap doesn't react.

Condition (TZ section 4.5): fees/revenue grew by >= X% over `lookback_days`,
while the token's market cap grew by less than that (or fell), over the
same window.

DeFiLlama's fees endpoint already reports revenue growth directly. Market
cap growth is derived from our own snapshot history in storage/db.py,
since DeFiLlama's free API does not expose historical market cap - so this
signal only starts firing once the collector has run for at least
`lookback_days` (see main.py).
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from storage.db import get_latest_snapshot, get_snapshot_days_before


@dataclass
class RevenueGapSignal:
    protocol_slug: str
    symbol: str | None
    lookback_days: int
    revenue_growth_pct: float
    mcap_growth_pct: float
    revenue_total: float
    mcap_now: float
    mcap_before: float


@dataclass
class RevenueGapScanResult:
    """Result of scanning the whole watchlist - keeps "not enough data yet"
    separate from "evaluated, but below threshold" so main.py can log an
    accurate reason instead of a single ambiguous "no candidates" line.
    Same shape as signals/oi_divergence.py's OiDivergenceScanResult, so
    main.py can treat both signal modules the same way.
    """
    signals: list[RevenueGapSignal] = field(default_factory=list)
    # Protocol slugs skipped because there's no latest snapshot yet, no
    # market cap on the latest snapshot, or no snapshot far enough back to
    # compare against `lookback_days` (first runs / short history).
    insufficient_history: list[str] = field(default_factory=list)


def _evaluate_snapshots(
    latest: sqlite3.Row,
    earlier: sqlite3.Row,
    revenue_growth_threshold_pct: float,
    mcap_reaction_threshold_pct: float,
    lookback_days: int,
) -> RevenueGapSignal | None:
    revenue_growth_pct = (
        latest["revenue_change_7d_pct"] if lookback_days == 7 else latest["revenue_change_30d_pct"]
    )
    if revenue_growth_pct is None or revenue_growth_pct < revenue_growth_threshold_pct:
        return None

    mcap_now = latest["mcap"]
    mcap_before = earlier["mcap"]
    if not mcap_now or not mcap_before:
        return None

    mcap_growth_pct = (mcap_now - mcap_before) / mcap_before * 100

    # Cap must have reacted less than revenue AND stayed under its own
    # threshold - a token that's up 25% while revenue is up 30% is not
    # really an "unreacted" gap yet.
    if mcap_growth_pct >= revenue_growth_pct or mcap_growth_pct >= mcap_reaction_threshold_pct:
        return None

    return RevenueGapSignal(
        protocol_slug=latest["protocol_slug"],
        symbol=latest["symbol"],
        lookback_days=lookback_days,
        revenue_growth_pct=revenue_growth_pct,
        mcap_growth_pct=mcap_growth_pct,
        revenue_total=latest["revenue_total_7d"] if lookback_days == 7 else latest["revenue_total_30d"],
        mcap_now=mcap_now,
        mcap_before=mcap_before,
    )


def scan_watchlist(
    conn: sqlite3.Connection,
    watchlist_slugs: list[str],
    revenue_growth_threshold_pct: float,
    mcap_reaction_threshold_pct: float,
    lookback_days: int,
) -> RevenueGapScanResult:
    """Run the signal for every watchlist protocol using stored snapshot history.

    Returns a RevenueGapScanResult whose `.insufficient_history` lists every
    protocol skipped because there's no snapshot yet, no market cap on the
    latest snapshot, or no snapshot far enough back to compare against
    `lookback_days` - expected on the first few collector runs, not just an
    empty `.signals` list indistinguishable from "evaluated, but none
    crossed the threshold".

    Raises:
        ValueError: if `lookback_days` isn't 7 or 30. DeFiLlama (and
            therefore collectors/defillama.py's stored snapshots) only ever
            carries pre-computed revenue growth for a 7-day and a 30-day
            window (revenue_change_7d_pct / revenue_change_30d_pct) -
            _evaluate_snapshots picks between those two columns with
            `if lookback_days == 7 else ...30d`, so any other value (e.g. 14,
            which TZ section 4.5's stated 7-30 day range would otherwise
            suggest is fine) would silently compare the 30-day revenue
            column against a market-cap snapshot fetched from 14 days back -
            two different windows, with no error to say so. Config.yaml
            documents this constraint next to the setting; this check makes
            a misconfigured value fail loudly instead of producing a
            plausible-looking but wrong signal.
    """
    if lookback_days not in (7, 30):
        raise ValueError(
            f"signals.revenue_price_gap.lookback_days in config.yaml must be 7 or 30 "
            f"(DeFiLlama only pre-computes revenue growth for those two windows), got "
            f"{lookback_days!r} - see this function's docstring"
        )

    signals: list[RevenueGapSignal] = []
    insufficient_history: list[str] = []

    for slug in watchlist_slugs:
        latest = get_latest_snapshot(conn, slug)
        if latest is None or latest["mcap"] is None:
            insufficient_history.append(slug)
            continue

        earlier = get_snapshot_days_before(conn, slug, latest["fetched_at"], lookback_days)
        if earlier is None:
            insufficient_history.append(slug)
            continue

        signal = _evaluate_snapshots(
            latest, earlier,
            revenue_growth_threshold_pct, mcap_reaction_threshold_pct, lookback_days,
        )
        if signal is not None:
            signals.append(signal)

    return RevenueGapScanResult(signals=signals, insufficient_history=insufficient_history)
