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

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime

from storage.db import get_latest_snapshot, get_snapshot_days_before

logger = logging.getLogger(__name__)

# A pair is rejected if the real gap between `earlier` and `latest` exceeds
# this multiple of `lookback_days` - a collection gap (source outage) can
# push "earlier" further back than requested, comparing revenue growth over
# one window against an mcap change over a longer one. Duplicated (not
# imported) in scripts/replay_signals.py's own MAX_GAP_MULTIPLIER - keep both in sync.
MAX_GAP_MULTIPLIER = 1.5


class _DataQualityRejected(Exception):
    """Raised by _evaluate_snapshots's guards (gap too large, revenue or
    mcap missing/non-positive) - distinct from "evaluated, below threshold",
    which still returns None. Caught by scan_watchlist to route the protocol
    into insufficient_data_quality instead of vanishing from every bucket."""


@dataclass
class RevenueGapSignal:
    protocol_slug: str
    symbol: str | None
    lookback_days: int
    # Actual elapsed days between the two snapshots, NOT the configured
    # lookback_days - storage gaps can push "earlier" further back than
    # requested (see MAX_GAP_MULTIPLIER). Reporting the real value keeps the
    # threshold honest about what window it was actually applied to.
    actual_lookback_days: float
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
    # No latest snapshot, latest mcap missing/<=0, or no snapshot far
    # enough back to compare (short history). Bad EARLIER-side data is
    # handled separately - see insufficient_data_quality below.
    insufficient_history: list[str] = field(default_factory=list)
    # Latest OR earlier revenue below config.yaml's min_revenue_total_usd -
    # checked on both sides, since a tiny EARLIER revenue can inflate the
    # growth ratio just as much as a tiny latest one ("tiny base, giant
    # percent" artifact).
    insufficient_liquidity: list[str] = field(default_factory=list)
    # Reached _evaluate_snapshots but rejected by one of its own guards
    # (gap too large, revenue/mcap missing or non-positive) - "we had data
    # on both sides, but it was bad data", not "too small" (liquidity) or
    # "not enough yet" (history). Without this bucket a collector outage
    # hitting the whole watchlist would look identical to a quiet market.
    insufficient_data_quality: list[str] = field(default_factory=list)


def _days_between(later_iso: str, earlier_iso: str) -> float:
    """Actual elapsed time between two stored snapshot timestamps, in days.

    Mirrors signals/oi_divergence.py's `_hours_between` - see
    MAX_GAP_MULTIPLIER above for why this matters here too.

    Args:
        later_iso: ISO 8601 timestamp of the more recent snapshot.
        earlier_iso: ISO 8601 timestamp of the older snapshot.

    Returns:
        Elapsed time between the two timestamps, in days.
    """
    later = datetime.fromisoformat(later_iso)
    earlier = datetime.fromisoformat(earlier_iso)
    return (later - earlier).total_seconds() / 86400


def _evaluate_snapshots(
    latest: sqlite3.Row,
    earlier: sqlite3.Row,
    revenue_growth_threshold_pct: float,
    mcap_reaction_threshold_pct: float,
    lookback_days: int,
    actual_lookback_days: float | None = None,
) -> RevenueGapSignal | None:
    """Compare one (latest, earlier) pair of stored DeFiLlama snapshots.

    Args:
        latest: most recent defillama_snapshots row for a protocol.
        earlier: the row closest to (latest - lookback_days) for the same
            protocol.
        revenue_growth_threshold_pct: see config.yaml
            signals.revenue_price_gap.revenue_growth_threshold_pct.
        mcap_reaction_threshold_pct: see config.yaml
            signals.revenue_price_gap.mcap_reaction_threshold_pct.
        lookback_days: configured comparison window (7 or 30).
        actual_lookback_days: real elapsed time between `earlier` and
            `latest` (see _days_between), checked against
            MAX_GAP_MULTIPLIER. Defaults to None for
            scripts/replay_signals.py's callers, which apply their own gap
            filter before calling this and skip the guard here (reports
            actual_lookback_days=lookback_days, i.e. "assume no gap").
    """
    if actual_lookback_days is not None and actual_lookback_days > lookback_days * MAX_GAP_MULTIPLIER:
        logger.warning(
            "revenue_price_gap: '%s' - skipping, real gap between compared snapshots is "
            "%.1f days, more than %.1fx the configured lookback_days (%d) - a collector gap "
            "(e.g. a source outage) likely pushed the 'earlier' snapshot further back than "
            "requested, which would compare latest's revenue growth (computed by DeFiLlama "
            "over its own %d-day window) against a market-cap change measured over a longer "
            "window than that - not 'the same window' as TZ 4.5 requires",
            latest["protocol_slug"], actual_lookback_days, MAX_GAP_MULTIPLIER, lookback_days,
            lookback_days,
        )
        raise _DataQualityRejected("snapshot gap too large")

    revenue_growth_pct = (
        latest["revenue_change_7d_pct"] if lookback_days == 7 else latest["revenue_change_30d_pct"]
    )
    if revenue_growth_pct is None or revenue_growth_pct < revenue_growth_threshold_pct:
        return None

    # DeFiLlama's revenue_change_pct is (total - prior)/prior*100, meaningful
    # only if `prior` is positive - confirmed live for nexus-mutual (an
    # insurance protocol whose payouts can exceed premiums), a negative base
    # can flip a worsening loss into a large POSITIVE percent. earlier's own
    # revenue_total_{7,30}d is the closest proxy for that base period.
    revenue_total_col = "revenue_total_7d" if lookback_days == 7 else "revenue_total_30d"
    base_revenue = earlier[revenue_total_col]
    # `is None or <= 0`: the column can be absent entirely (DeFiLlama
    # omitted it), not just non-positive - both make revenue_growth_pct
    # untrustworthy.
    if base_revenue is None or base_revenue <= 0:
        logger.warning(
            "revenue_price_gap: '%s' - skipping, base-period revenue (%s, ~%dd window "
            "ending %s) is missing or <= 0, so this protocol's revenue_growth_pct (%.2f%%) "
            "is not a meaningful percentage (missing/negative/zero denominator makes the "
            "growth formula's result meaningless or its sign flip - see this function's "
            "comment)",
            latest["protocol_slug"], base_revenue, lookback_days, earlier["fetched_at"],
            revenue_growth_pct,
        )
        raise _DataQualityRejected("base-period revenue missing or non-positive")

    # Distinct from the base_revenue guard: here the % itself is
    # well-defined (base was positive), but a non-positive CURRENT revenue
    # makes "revenue grew by N%" self-contradictory - same nexus-mutual
    # insurance-payout case as above. Missing (None) is blocked too, not
    # just <= 0: there's simply no current number to point at.
    revenue_total_now = latest[revenue_total_col]
    if revenue_total_now is None or revenue_total_now <= 0:
        logger.warning(
            "revenue_price_gap: '%s' - skipping, latest %dd revenue total is missing, "
            "negative or zero (%s) - revenue_growth_pct (%.2f%%) is arithmetically valid "
            "here (base period was positive) but reporting that as '%% growth' for a "
            "protocol with no meaningful revenue right now is not meaningful",
            latest["protocol_slug"], lookback_days, revenue_total_now, revenue_growth_pct,
        )
        raise _DataQualityRejected("latest-period revenue missing or non-positive")

    mcap_now = latest["mcap"]
    mcap_before = earlier["mcap"]
    # `is None or <= 0`, not `not X`: `not X` lets a NEGATIVE mcap through.
    # Confirmed live: CoinGecko returns -1 as a placeholder mcap on some
    # dates (89 renzo rows) - an unguarded negative denominator produced an
    # absurd -3,053,635,664% instead of erroring, same class of bug as the
    # revenue guards above.
    if mcap_now is None or mcap_now <= 0 or mcap_before is None or mcap_before <= 0:
        logger.warning(
            "revenue_price_gap: '%s' - skipping, market cap is missing or <= 0 "
            "(latest=%s on %s, earlier=%s on %s) - mcap_growth_pct cannot be computed "
            "meaningfully from a missing/non-positive value on either side",
            latest["protocol_slug"], mcap_now, latest["fetched_at"], mcap_before,
            earlier["fetched_at"],
        )
        raise _DataQualityRejected("market cap missing or non-positive")

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
        actual_lookback_days=(
            actual_lookback_days if actual_lookback_days is not None else float(lookback_days)
        ),
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
    min_revenue_total_usd: float = 0,
) -> RevenueGapScanResult:
    """Run the signal for every watchlist protocol using stored snapshot history.

    Returns a RevenueGapScanResult - see that dataclass's docstring for what
    each of the four buckets (signals/insufficient_history/
    insufficient_liquidity/insufficient_data_quality) means and why they're
    kept separate.

    Args:
        min_revenue_total_usd: minimum revenue (USD) over `lookback_days`
            BOTH snapshots must have before a protocol is evaluated.
            Defaults to 0 (no filtering).

    Raises:
        ValueError: if `lookback_days` isn't 7 or 30 - DeFiLlama only
            pre-computes revenue growth for those two windows; any other
            value would silently compare mismatched windows. config.yaml
            documents this next to the setting.
    """
    if lookback_days not in (7, 30):
        raise ValueError(
            f"signals.revenue_price_gap.lookback_days in config.yaml must be 7 or 30 "
            f"(DeFiLlama only pre-computes revenue growth for those two windows), got "
            f"{lookback_days!r} - see this function's docstring"
        )

    revenue_total_col = "revenue_total_7d" if lookback_days == 7 else "revenue_total_30d"

    signals: list[RevenueGapSignal] = []
    insufficient_history: list[str] = []
    insufficient_liquidity: list[str] = []
    insufficient_data_quality: list[str] = []

    for slug in watchlist_slugs:
        latest = get_latest_snapshot(conn, slug)
        # `<= 0` not just `is None`: a garbage mcap (renzo's CoinGecko feed
        # uses -1 as a placeholder) is as unusable as a missing one - caught
        # here too so it's reported honestly as insufficient_history instead
        # of silently falling through with no bucket.
        if latest is None or latest["mcap"] is None or latest["mcap"] <= 0:
            insufficient_history.append(slug)
            continue

        revenue_total_now = latest[revenue_total_col]
        if revenue_total_now is None or revenue_total_now < min_revenue_total_usd:
            insufficient_liquidity.append(slug)
            continue

        earlier = get_snapshot_days_before(conn, slug, latest["fetched_at"], lookback_days)
        if earlier is None:
            insufficient_history.append(slug)
            continue

        # Same min_revenue_total_usd floor applied to `earlier`: since
        # revenue_growth_pct is a ratio, a tiny EARLIER revenue inflates it
        # even when latest clears the floor - confirmed live for
        # jupiter-aggregator (a ~$46 base week produced 3421097% growth, a
        # DeFiLlama data hole, not a real event).
        earlier_revenue = earlier[revenue_total_col]
        if earlier_revenue is None or earlier_revenue < min_revenue_total_usd:
            insufficient_liquidity.append(slug)
            continue

        actual_lookback_days = _days_between(latest["fetched_at"], earlier["fetched_at"])
        try:
            signal = _evaluate_snapshots(
                latest, earlier,
                revenue_growth_threshold_pct, mcap_reaction_threshold_pct, lookback_days,
                actual_lookback_days,
            )
        except _DataQualityRejected:
            # Reason already logged by _evaluate_snapshots - just route
            # into the right bucket (see RevenueGapScanResult docstring).
            insufficient_data_quality.append(slug)
            continue
        if signal is not None:
            signals.append(signal)

    return RevenueGapScanResult(
        signals=signals,
        insufficient_history=insufficient_history,
        insufficient_liquidity=insufficient_liquidity,
        insufficient_data_quality=insufficient_data_quality,
    )
