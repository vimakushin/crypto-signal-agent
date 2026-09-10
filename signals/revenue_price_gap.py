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

# A pair is rejected (not scored) if the real elapsed time between `earlier`
# and `latest` exceeds this multiple of the configured `lookback_days`.
# storage.db.get_snapshot_days_before returns the closest stored snapshot AT
# OR BEFORE the target date, not necessarily exactly at it - a gap in daily
# collection (e.g. a source outage - CoinGecko has already returned 429s
# during this project's own runs) can push "earlier" further back than
# requested, which would silently compare latest's ~7-day revenue growth
# (a number DeFiLlama itself computes over ITS OWN 7 days) against a
# market-cap change measured over a longer window - violating TZ 4.5's "same
# window" requirement without saying so. Same 1.5x ratio, and same rationale,
# as scripts/replay_signals.py's own MAX_GAP_MULTIPLIER (duplicated here
# rather than imported, since this module must not depend on a one-off
# analysis script, and the two constants are simple enough that keeping them
# in sync by inspection is not a burden).
MAX_GAP_MULTIPLIER = 1.5


class _DataQualityRejected(Exception):
    """Raised internally by `_evaluate_snapshots` when a protocol/pair is
    dropped by one of its own guards (snapshot gap too large, base or
    current revenue non-positive/missing, market cap non-positive/missing)
    rather than because revenue growth failed to clear the configured
    threshold.

    `scan_watchlist` catches this to route the protocol into
    `RevenueGapScanResult.insufficient_data_quality` instead of silently
    leaving it out of every bucket - see that dataclass's docstring for why
    this distinction matters. Not raised for the "evaluated fine, but below
    threshold" case, which still returns None as before.
    """


@dataclass
class RevenueGapSignal:
    protocol_slug: str
    symbol: str | None
    lookback_days: int
    # Actual elapsed time between the two compared snapshots, in days - NOT
    # the configured `lookback_days` above. See MAX_GAP_MULTIPLIER: storage
    # can have gaps, so the closest available "earlier" snapshot can sit
    # further back than requested. Reporting the configured number instead
    # of what actually elapsed would silently misstate the window a
    # calibrated threshold (e.g. 30% growth) was applied to - same
    # reasoning as signals/oi_divergence.py's own `lookback_hours` field on
    # OiDivergenceSignal, which stores the actual, not configured, value.
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
    # Protocol slugs skipped because there's no latest snapshot yet, the
    # latest snapshot's market cap is missing or <= 0 (checked up front in
    # scan_watchlist, before any pair comparison is attempted), or there's
    # no snapshot far enough back to compare against `lookback_days` (first
    # runs / short history). A market cap that's merely too old-vs-latest
    # for a meaningful comparison, or bad on the EARLIER side, is not here -
    # see insufficient_data_quality below.
    insufficient_history: list[str] = field(default_factory=list)
    # Protocol slugs skipped because EITHER the latest OR the earlier
    # snapshot's revenue for `lookback_days` is below config.yaml's
    # min_revenue_total_usd - checked on both sides of the comparison, not
    # just the latest one, since revenue_growth_pct is a ratio and a tiny
    # EARLIER revenue can inflate it just as much as a tiny latest one.
    # Kept separate from insufficient_history: this is not "not enough data
    # yet", it's "there IS a number, and it's too small to mean anything"
    # (the "tiny base, giant percent" artifact - see scan_watchlist).
    insufficient_liquidity: list[str] = field(default_factory=list)
    # Protocol slugs that reached _evaluate_snapshots but were rejected by
    # one of ITS OWN internal guards - snapshot gap too large (collector
    # outage pushed the 'earlier' snapshot further back than requested),
    # base or current-period revenue missing/non-positive, or market cap
    # missing/non-positive on either snapshot. Distinct from
    # insufficient_liquidity (which is about revenue being too SMALL to
    # trust, checked up front in scan_watchlist) and from
    # insufficient_history (no snapshot far enough back to compare at all):
    # this bucket means "we had data on both sides, but it was bad data",
    # not "not enough data yet". Without this bucket these protocols would
    # simply vanish from every result list, making a day where a collector
    # gap trips this guard for the whole watchlist indistinguishable from a
    # quiet market with zero real candidates - see scan_watchlist.
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
            `latest` (see _days_between) - carried through to the returned
            signal for transparency, and checked against
            MAX_GAP_MULTIPLIER below. Defaults to None for
            scripts/replay_signals.py's own callers, which already apply
            their own, separate gap exclusion (MAX_GAP_MULTIPLIER there)
            BEFORE calling this function and don't need a second check
            here; when None, this function skips the gap guard entirely and
            reports `actual_lookback_days=lookback_days` on the returned
            signal (i.e. "assume no gap"), matching replay's pre-existing
            behavior exactly.
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

    # Defends the live path against the same formula flaw backfill_history.py's
    # _compute_revenue_rollups guards against on the historical path (see that
    # function's docstring): (total - prior) / prior * 100 only means "percent
    # growth" if `prior` (the base-period revenue) is POSITIVE. DeFiLlama's own
    # /overview/fees endpoint computes revenue_change_7d_pct/change_30dover30d
    # itself - we don't control its math - and can hand back exactly this same
    # shape of number for a protocol with a negative base period (confirmed
    # live for nexus-mutual, an insurance protocol where payouts can exceed
    # premiums - a real business outcome, not a data bug). With a negative
    # base, a protocol whose losses got WORSE (more negative) can come out as
    # a large POSITIVE percentage - which would then look exactly like real
    # growth to the `revenue_growth_threshold_pct` check above.
    #
    # `earlier`'s own revenue_total_{7,30}d is the right proxy for that base
    # period: earlier's snapshot was fetched ~lookback_days before latest's,
    # so earlier's own trailing-N-day total covers (almost) exactly the same
    # calendar window DeFiLlama's growth formula would have used as its
    # denominator for latest's pct - see backfill_history.py's
    # _compute_revenue_rollups docstring for the exact window match.
    revenue_total_col = "revenue_total_7d" if lookback_days == 7 else "revenue_total_30d"
    base_revenue = earlier[revenue_total_col]
    # `is None or` (not just `<= 0`): `earlier` can legitimately have no
    # value at all for this column - collectors/defillama.py reads it via
    # fee_entry.get("total7d"), which is None whenever DeFiLlama's own
    # response omits that field for this protocol/day. A missing base is
    # exactly the situation this guard exists to block (see the guard's own
    # comment above and below): with no base value, `revenue_growth_pct`
    # (computed by DeFiLlama itself, not by us, from whatever base IT had)
    # cannot be trusted to describe a real percentage change either, so it
    # must be blocked here too, not just the base<=0 case.
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

    # Blocks the signal, distinct reason from the base_revenue guard above:
    # even when the arithmetic above is not broken (positive base, so the %
    # itself is well-defined), a NEGATIVE/ZERO revenue on the CURRENT period
    # makes the signal self-contradictory in practice - "revenue grew by
    # N%" while simultaneously reporting a negative/zero revenue right now
    # doesn't describe anything actionable, even though the number itself
    # is not mathematically wrong. Confirmed live for insurance protocols
    # like nexus-mutual, where payouts can exceed premiums in some windows.
    revenue_total_now = latest[revenue_total_col]
    # `is None or` (not just `<= 0`) for the same reason as the base_revenue
    # guard above: a missing current-period total (collectors/defillama.py's
    # fee_entry.get("total7d") returning None) is just as unable to support
    # "revenue grew by N%" as a negative/zero one - there's no current
    # revenue number to point at at all.
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
    # `is None or <= 0` (not just `not X`): `not X` would drop None and 0
    # but let a NEGATIVE mcap straight through into the division below.
    # Confirmed live: defillama_snapshots has 89 rows for renzo with
    # mcap <= 0 (CoinGecko's own /coins/renzo/market_chart, which
    # collectors/defillama.py reads market cap from, returns -1 as a
    # placeholder on dates it has no real figure for - not our collection
    # bug), and the same negative/garbage values can end up on either side
    # of this comparison. With a negative `mcap_before`,
    # (mcap_now - mcap_before) / mcap_before below produces an absurd
    # percentage (observed: -3,053,635,664%) instead of erroring - same
    # class of bug as the base_revenue/revenue_total_now guards above, just
    # for market cap instead of revenue.
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

    Returns a RevenueGapScanResult whose `.insufficient_history` lists every
    protocol skipped because there's no snapshot yet, no market cap on the
    latest snapshot, or no snapshot far enough back to compare against
    `lookback_days` - expected on the first few collector runs, not just an
    empty `.signals` list indistinguishable from "evaluated, but none
    crossed the threshold". `.insufficient_liquidity` (TZ section 9's
    low-liquidity noise risk) lists protocols skipped because EITHER the
    latest OR the earlier snapshot's revenue for `lookback_days` is below
    `min_revenue_total_usd` (config.yaml
    signals.revenue_price_gap.min_revenue_total_usd) - a protocol whose
    revenue on either side of the comparison is only a few hundred dollars
    can show a huge, technically-correct growth percentage off a base
    that's too small to be meaningful (the same "tiny base, giant percent"
    shape as the negative-revenue formula issue _evaluate_snapshots already
    guards, but for small POSITIVE bases instead of negative ones - checked
    on the earlier side too, since revenue_growth_pct is a ratio and a tiny
    denominator inflates it regardless of which side of the pair it's on).
    `.insufficient_data_quality` lists protocols that reached the per-pair
    comparison but were rejected by one of `_evaluate_snapshots`'s own
    guards (snapshot gap too large, base/current revenue missing or
    non-positive, market cap missing or non-positive) - "we had data on
    both sides, but it was bad data", as opposed to "not enough data yet"
    or "revenue too small to trust". See RevenueGapScanResult's docstring
    for why this bucket exists and _DataQualityRejected for the mechanism.

    Args:
        min_revenue_total_usd: minimum absolute revenue (USD) over
            `lookback_days` BOTH the latest and the earlier snapshot must
            have before a protocol is evaluated - see above. Defaults to 0
            (no filtering) so existing callers that don't pass it keep
            working unchanged.

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

    revenue_total_col = "revenue_total_7d" if lookback_days == 7 else "revenue_total_30d"

    signals: list[RevenueGapSignal] = []
    insufficient_history: list[str] = []
    insufficient_liquidity: list[str] = []
    insufficient_data_quality: list[str] = []

    for slug in watchlist_slugs:
        latest = get_latest_snapshot(conn, slug)
        # `<= 0`, not just `is None`: a stored mcap of 0 or negative (seen
        # live for renzo, where CoinGecko's own market_chart response uses
        # -1 as a placeholder for dates it has no real market cap for - see
        # _evaluate_snapshots's matching guard below) is just as unusable
        # for mcap_growth_pct as a missing one. Checked here too, not just
        # inside _evaluate_snapshots, so a protocol stuck with a garbage
        # mcap is reported as "insufficient_history" (an honest reason) up
        # front, instead of silently falling through the later per-pair
        # check with no entry in either result bucket.
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

        # Same min_revenue_total_usd floor as the `latest` check above,
        # applied to `earlier` too: revenue_growth_pct is a RATIO
        # (latest revenue / earlier revenue), so a tiny EARLIER revenue can
        # produce a huge, technically-correct-looking growth percentage even
        # when the LATEST revenue comfortably clears the floor by itself -
        # confirmed live for jupiter-aggregator (2024-12-23,
        # revenue_total_7d=$1,573,751, revenue_change_7d_pct=3421097%,
        # implying a ~$46 base week for an established multi-million-dollar
        # protocol - a data hole in DeFiLlama's totalDataChart for that
        # week, not a real business event). Checking only the numerator's
        # scale, as before, let exactly this kind of point through.
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
            # _evaluate_snapshots already logged the specific reason (gap,
            # base revenue, current revenue, or market cap) - here we just
            # need to route the slug into the right bucket so main.py can
            # tell "bad data" apart from "quiet market, nothing crossed the
            # threshold" instead of the slug silently vanishing from every
            # result list. See RevenueGapScanResult docstring.
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
