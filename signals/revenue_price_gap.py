"""Signal 4.5: protocol revenue grows while market cap doesn't react.

Condition (TZ section 4.5): fees/revenue grew by >= X% over `lookback_days`,
while the token's market cap grew by less than that (or fell), over the
same window.

Revenue growth is now computed from our own daily-granularity history
(storage/db.py's defillama_daily_revenue table), NOT from DeFiLlama's own
pre-computed weekly SUM (defillama_snapshots.revenue_change_7d_pct) the way
this module originally worked - see BACKLOG.md's "revenue_price_gap ловит
одиночные выбросы" entry for the live evidence (thorchain-dex, cowswap,
2026-09-12) that a SUM-based comparison lets one unusually large single day
masquerade as a whole week's trend. The fix compares MEDIAN daily revenue
over a recent window against the median over a longer baseline window -
medians are far less sensitive to a single outlier day than a sum is.
Market cap growth is still derived from our own snapshot history
(defillama_snapshots), since DeFiLlama's free API does not expose
historical market cap - so this signal only starts firing once the
collector has run for at least `lookback_days` (see main.py).
"""
from __future__ import annotations

import logging
import math
import sqlite3
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from storage.db import (
    get_defillama_daily_revenue_window,
    get_latest_snapshot,
    get_snapshot_days_before,
)

logger = logging.getLogger(__name__)

# A pair is rejected if the real gap between `earlier` and `latest` exceeds
# this multiple of `lookback_days` - a collection gap (source outage) can
# push "earlier" further back than requested, comparing revenue growth over
# one window against an mcap change over a longer one. Duplicated (not
# imported) in scripts/replay_signals.py's own MAX_GAP_MULTIPLIER - keep both in sync.
MAX_GAP_MULTIPLIER = 1.5

# Same coverage floor signals/volume_breakout.py's own
# MIN_HISTORY_COVERAGE_FRACTION already uses for its own trailing-window
# checks - reused as-is rather than inventing a new number: a window missing
# more than 10% of its calendar days is treated as "not enough history yet"
# (insufficient_history), not evaluated with gaps silently filled in.
# scan_watchlist below turns this fraction into a minimum count of valid
# days with math.floor, not math.ceil, deliberately: on a small window like
# lookback_days=7, ceil(7 * 0.9) = ceil(6.3) = 7 - i.e. zero missing days
# tolerated, which quietly turns the "about 90%" threshold into "100%".
# floor(6.3) = 6 instead allows exactly one gap day out of seven, which
# matches the stated intent of "not stricter than ~90%".
MIN_HISTORY_COVERAGE_FRACTION = 0.9

# The "shape" mark (see _shape_check) compares this week's unevenness with the
# protocol's own usual unevenness, taken as the median over all complete past
# 7-day windows. Fewer than 40 such windows and the "usual" value is itself
# too shaky to compare against, so the check is NOT applied (reported as
# "nothing to compare with", never silently as "all fine"). 40 is roughly
# half of the ~77 windows that fit into the 90-day baseline before the fresh
# week: a round, plain floor, not tuned to any protocol's history.
MIN_PAST_WINDOWS_FOR_SHAPE = 40

# Unevenness is defined for a 7-day window: median of the 3 largest days
# divided by median of the 3 smallest (= 2nd largest / 2nd smallest).
SHAPE_WINDOW_DAYS = 7


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
    # Median daily revenue over `lookback_days` ending on the latest
    # snapshot's date, and over `baseline_window_days` ending on the same
    # date - the two numbers revenue_growth_pct is computed from. Replaces
    # the old `revenue_total` (a weekly SUM) field - see this module's
    # docstring for why a sum was misleading.
    recent_median_daily_revenue: float
    baseline_median_daily_revenue: float
    # Sum of the recent lookback_days window's daily revenue - reported
    # alongside the median for transparency (this is the number
    # min_revenue_total_usd's liquidity floor is actually checked against),
    # not itself part of the growth-% formula.
    recent_week_sum: float
    # Share (%) of recent_week_sum contributed by its single largest day -
    # logged on every signal so the user can see at a glance how "spiky" vs.
    # "broad" the week behind a candidate was.
    max_day_share_pct: float
    # Mark 1 ("one outlier day"): max_day_share_pct is above config.yaml's
    # outlier_max_share_pct. Since 19 Oct 2026 this only marks the signal, it
    # no longer rejects the week (see config.yaml for the history).
    single_day_marked: bool
    # Mark 2 ("this week is more uneven than usual for this coin"), see
    # _shape_check. week_unevenness/habitual_unevenness are None when they
    # could not be computed; shape_ratio is None when it is not a finite
    # number (check skipped, or the window has zero-revenue days).
    week_unevenness: float | None
    habitual_unevenness: float | None
    shape_ratio: float | None
    shape_marked: bool
    # True when there was nothing to compare with (too little history, or the
    # usual unevenness is undefined); shape_note then says why, in Russian -
    # the only place with these reason texts (the Telegram digest inserts
    # them as they are). When the mark is put because the window has
    # zero-revenue days, shape_ratio is None and shape_note stays None: the
    # digest words that case itself.
    shape_check_skipped: bool
    shape_note: str | None
    # The 7 days mark 2 describes (first/last day, ISO dates; None if no
    # window could be formed) and the last day of the window the revenue
    # medians used - they differ by a day for protocols whose latest day
    # DeFiLlama publishes only the next morning. The reader must see which
    # days the mark is about.
    shape_window_first: str | None
    shape_window_last: str | None
    revenue_window_end: str
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
    # Recent-window revenue SUM, or baseline-window median scaled to a
    # weekly-equivalent, below config.yaml's min_revenue_total_usd - checked
    # on both sides, since a tiny BASELINE median can inflate the growth
    # ratio just as much as a tiny recent sum ("tiny base, giant percent"
    # artifact).
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


def _unevenness(values: list[float]) -> float:
    """Unevenness of a 7-day window: median of the 3 largest days divided by
    median of the 3 smallest days (= 2nd largest / 2nd smallest).

    Args:
        values: exactly SHAPE_WINDOW_DAYS daily revenue values.

    Returns:
        The ratio, or math.inf when the smaller median is <= 0 (two or more
        zero/negative days) - the ratio is then not a usable number.
    """
    ordered = sorted(values)
    low, high = ordered[1], ordered[-2]
    return high / low if low > 0 else math.inf


def _shape_check(
    baseline_daily_revenue: list[sqlite3.Row],
    revenue_window_end: date,
    shape_mark_ratio: float,
) -> dict:
    """Mark 2: is the shape of the latest 7 days more uneven than usual for
    this protocol?

    The window is the LAST 7 consecutive days that have data as of this
    evaluation (the last day with data and the 6 before it), not necessarily
    the window the signal's medians use: for ~10 protocols DeFiLlama gives a
    day's revenue only the next morning, so their window ends one day earlier
    than `revenue_window_end`. Its unevenness is compared with the median
    unevenness of all complete 7-day windows (7 consecutive dates, no missing
    values) lying fully inside the baseline window and ending BEFORE this
    window's first day. Uses only rows the caller already has (nothing from
    after the evaluation moment). The window never reaches back further than
    D-7 only because scan_watchlist's coverage check (>= 6 valid days in
    D-6..D, >= 81 in the baseline) runs before this function is called.

    Args:
        baseline_daily_revenue: the baseline window's rows (date, revenue_usd).
        revenue_window_end: last day of the window the signal's revenue
            medians were computed on (the latest snapshot's date).
        shape_mark_ratio: config.yaml signals.revenue_price_gap.shape_mark_ratio.

    Returns:
        Dict with the RevenueGapSignal fields week_unevenness,
        habitual_unevenness, shape_ratio, shape_marked, shape_check_skipped,
        shape_note, shape_window_first, shape_window_last, revenue_window_end.
        Never contains inf/NaN (the result goes into JSON).
    """
    result: dict = {
        "week_unevenness": None, "habitual_unevenness": None, "shape_ratio": None,
        "shape_marked": False, "shape_check_skipped": True, "shape_note": None,
        "shape_window_first": None, "shape_window_last": None,
        "revenue_window_end": revenue_window_end.isoformat(),
    }
    by_date = {
        date.fromisoformat(r["date"]): r["revenue_usd"]
        for r in baseline_daily_revenue if r["revenue_usd"] is not None
    }
    last_day = max(by_date)
    window_days = [last_day - timedelta(days=k) for k in range(SHAPE_WINDOW_DAYS)]
    first_day = window_days[-1]
    result["shape_window_first"] = first_day.isoformat()
    result["shape_window_last"] = last_day.isoformat()
    if not all(d in by_date for d in window_days):
        result["shape_note"] = "в последних семи днях есть пропуски в данных"
        return result
    week = [by_date[d] for d in window_days]

    past = []
    for end_day in by_date:
        if end_day >= first_day:
            continue
        days = [end_day - timedelta(days=k) for k in range(SHAPE_WINDOW_DAYS)]
        if all(d in by_date for d in days):
            past.append(_unevenness([by_date[d] for d in days]))
    if len(past) < MIN_PAST_WINDOWS_FOR_SHAPE:
        result["shape_note"] = (
            f"у монеты пока мало прошлых недель для сравнения (есть {len(past)}, нужно {MIN_PAST_WINDOWS_FOR_SHAPE})"
        )
        return result
    habitual = statistics.median(past)
    if not math.isfinite(habitual):
        result["shape_note"] = (
            "у этой монеты слишком часто бывают дни с нулевой выручкой, "
            "обычный разброс не посчитать"
        )
        return result

    result["habitual_unevenness"] = habitual
    result["shape_check_skipped"] = False
    week_unevenness = _unevenness(week)
    if math.isfinite(week_unevenness):
        result["week_unevenness"] = week_unevenness
        result["shape_ratio"] = week_unevenness / habitual
        result["shape_marked"] = result["shape_ratio"] > shape_mark_ratio
    else:
        result["shape_marked"] = True
    return result


def _evaluate_snapshots(
    latest: sqlite3.Row,
    earlier: sqlite3.Row,
    recent_daily_revenue: list[sqlite3.Row],
    baseline_daily_revenue: list[sqlite3.Row],
    revenue_growth_threshold_pct: float,
    mcap_reaction_threshold_pct: float,
    outlier_max_share_pct: float,
    shape_mark_ratio: float,
    lookback_days: int,
    actual_lookback_days: float | None = None,
) -> RevenueGapSignal | None:
    """Compare one (latest, earlier) pair of stored DeFiLlama snapshots,
    using daily-granularity revenue history for the growth-% math.

    Args:
        latest: most recent defillama_snapshots row for a protocol - only
            used here for its `mcap` (revenue now comes from
            `recent_daily_revenue`/`baseline_daily_revenue` instead).
        earlier: the defillama_snapshots row closest to
            (latest - lookback_days) for the same protocol - only used here
            for its `mcap`.
        recent_daily_revenue: defillama_daily_revenue rows for the
            `lookback_days`-day window ending on latest's date (see
            storage.db.get_defillama_daily_revenue_window).
        baseline_daily_revenue: defillama_daily_revenue rows for the
            `baseline_window_days`-day window ending on the SAME date.
        revenue_growth_threshold_pct: see config.yaml
            signals.revenue_price_gap.revenue_growth_threshold_pct.
        mcap_reaction_threshold_pct: see config.yaml
            signals.revenue_price_gap.mcap_reaction_threshold_pct.
        outlier_max_share_pct: see config.yaml
            signals.revenue_price_gap.outlier_max_share_pct (a mark
            threshold, not a rejection).
        shape_mark_ratio: see config.yaml
            signals.revenue_price_gap.shape_mark_ratio.
        lookback_days: configured "recent" comparison window in days.
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
            "requested, which would compare latest's revenue growth against a market-cap "
            "change measured over a longer window than that - not 'the same window' as TZ "
            "4.5 requires",
            latest["protocol_slug"], actual_lookback_days, MAX_GAP_MULTIPLIER, lookback_days,
        )
        raise _DataQualityRejected("snapshot gap too large")

    recent_values = [r["revenue_usd"] for r in recent_daily_revenue if r["revenue_usd"] is not None]
    baseline_values = [r["revenue_usd"] for r in baseline_daily_revenue if r["revenue_usd"] is not None]

    recent_sum = sum(recent_values)
    # A single day dominating the week used to REJECT the week here (see
    # BACKLOG.md: thorchain-dex, 12 Sep 2026). Since 19 Oct 2026 it is only a
    # mark on the result - nothing is dropped, the reader decides.
    max_day_share_pct = (max(recent_values) / recent_sum * 100) if recent_sum > 0 else 0.0

    # Empty lists should not reach this point: scan_watchlist's own coverage
    # guard (MIN_HISTORY_COVERAGE_FRACTION) already requires close to a full
    # window of valid daily values on both sides before calling this
    # function - statistics.median would raise StatisticsError on an empty
    # list, which is deliberately NOT caught here, so a coverage-guard bug
    # upstream fails loudly instead of silently miscounting.
    recent_median = statistics.median(recent_values)
    baseline_median = statistics.median(baseline_values)

    # Same "negative/zero denominator makes the growth formula meaningless
    # or sign-flipped" situation the old revenue_total_col guard existed
    # for (e.g. nexus-mutual, an insurance protocol whose payouts can
    # exceed premiums in a given window) - now checked against the BASELINE
    # MEDIAN instead of a weekly sum.
    if baseline_median <= 0:
        logger.warning(
            "revenue_price_gap: '%s' - skipping, baseline median daily revenue (%.2f over "
            "the trailing baseline window ending %s) is <= 0, so revenue_growth_pct cannot "
            "be computed meaningfully (missing/negative/zero denominator)",
            latest["protocol_slug"], baseline_median, earlier["fetched_at"],
        )
        raise _DataQualityRejected("baseline median revenue non-positive")

    # Distinct from the baseline_median guard: here the ratio's denominator
    # is fine, but a non-positive RECENT median makes "revenue grew by N%"
    # self-contradictory - same nexus-mutual-style situation as above, now
    # on the recent side.
    if recent_median <= 0:
        logger.warning(
            "revenue_price_gap: '%s' - skipping, recent median daily revenue (%.2f over the "
            "%dd window ending %s) is <= 0 - reporting that as '%% growth' for a protocol "
            "with no meaningful revenue right now is not meaningful",
            latest["protocol_slug"], recent_median, lookback_days, latest["fetched_at"],
        )
        raise _DataQualityRejected("recent median revenue non-positive")

    revenue_growth_pct = (recent_median - baseline_median) / baseline_median * 100
    if revenue_growth_pct < revenue_growth_threshold_pct:
        return None

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
        recent_median_daily_revenue=recent_median,
        baseline_median_daily_revenue=baseline_median,
        recent_week_sum=recent_sum,
        max_day_share_pct=max_day_share_pct,
        single_day_marked=max_day_share_pct > outlier_max_share_pct,
        **_shape_check(
            baseline_daily_revenue, datetime.fromisoformat(latest["fetched_at"]).date(),
            shape_mark_ratio,
        ),
        mcap_now=mcap_now,
        mcap_before=mcap_before,
    )


def scan_watchlist(
    conn: sqlite3.Connection,
    watchlist_slugs: list[str],
    revenue_growth_threshold_pct: float,
    mcap_reaction_threshold_pct: float,
    lookback_days: int,
    baseline_window_days: int,
    shape_mark_ratio: float,
    min_revenue_total_usd: float = 0,
    outlier_max_share_pct: float = 100,
) -> RevenueGapScanResult:
    """Run the signal for every watchlist protocol using stored snapshot and
    daily-revenue history.

    Returns a RevenueGapScanResult - see that dataclass's docstring for what
    each of the four buckets (signals/insufficient_history/
    insufficient_liquidity/insufficient_data_quality) means and why they're
    kept separate.

    Args:
        lookback_days: recent comparison window, in days - median daily
            revenue over this window is compared against the baseline
            median (see baseline_window_days). Any positive value works now
            (see Raises below for the one remaining constraint) - unlike
            this module's original DeFiLlama-sum-based version, which
            required exactly 7 or 30 because DeFiLlama only pre-computes
            growth for those two windows. This version computes its own
            medians from defillama_daily_revenue, so that constraint no
            longer applies.
        baseline_window_days: longer trailing window (config.yaml
            signals.revenue_price_gap.baseline_window_days) the recent
            window's median is compared against.
        min_revenue_total_usd: minimum revenue (USD) - checked against the
            recent window's SUM and the baseline window's median scaled to
            a weekly-equivalent - both snapshots must clear before a
            protocol is evaluated. Defaults to 0 (no filtering).
        outlier_max_share_pct: see config.yaml
            signals.revenue_price_gap.outlier_max_share_pct.
        shape_mark_ratio: see config.yaml
            signals.revenue_price_gap.shape_mark_ratio.

    Raises:
        ValueError: if `lookback_days` is not strictly less than
            `baseline_window_days` - otherwise the "recent" window is not
            actually narrower than the "baseline" it's being compared
            against, and the comparison loses its meaning.
    """
    if lookback_days >= baseline_window_days:
        raise ValueError(
            f"signals.revenue_price_gap.lookback_days ({lookback_days}) must be strictly "
            f"less than baseline_window_days ({baseline_window_days}) in config.yaml - "
            f"otherwise the 'recent' window isn't actually narrower than the 'baseline' "
            f"window it's compared against"
        )

    min_valid_recent = math.floor(lookback_days * MIN_HISTORY_COVERAGE_FRACTION)
    min_valid_baseline = math.floor(baseline_window_days * MIN_HISTORY_COVERAGE_FRACTION)

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

        earlier = get_snapshot_days_before(conn, slug, latest["fetched_at"], lookback_days)
        if earlier is None:
            insufficient_history.append(slug)
            continue

        end_date = datetime.fromisoformat(latest["fetched_at"]).date().isoformat()
        recent_rows = get_defillama_daily_revenue_window(conn, slug, end_date, lookback_days)
        baseline_rows = get_defillama_daily_revenue_window(conn, slug, end_date, baseline_window_days)

        valid_recent = [r["revenue_usd"] for r in recent_rows if r["revenue_usd"] is not None]
        valid_baseline = [r["revenue_usd"] for r in baseline_rows if r["revenue_usd"] is not None]

        # Coverage guard: not enough daily-revenue history stored yet to
        # trust a median from either window - "not enough yet", not "bad
        # data" (see insufficient_history's own docstring in
        # RevenueGapScanResult).
        if len(valid_recent) < min_valid_recent or len(valid_baseline) < min_valid_baseline:
            insufficient_history.append(slug)
            continue

        # Liquidity/noise floor (TZ section 9), now checked against the
        # daily-revenue windows directly instead of DeFiLlama's own weekly
        # rollup columns: recent window's SUM (the same shape as the old
        # revenue_total_7d check), and the baseline median scaled to a
        # weekly-equivalent so it's comparable to the same floor (a tiny
        # baseline can inflate revenue_growth_pct just as much as a tiny
        # recent sum - same "tiny base, giant percent" risk as before).
        recent_sum = sum(valid_recent)
        baseline_weekly_equivalent = statistics.median(valid_baseline) * lookback_days
        if recent_sum < min_revenue_total_usd or baseline_weekly_equivalent < min_revenue_total_usd:
            insufficient_liquidity.append(slug)
            continue

        actual_lookback_days = _days_between(latest["fetched_at"], earlier["fetched_at"])
        try:
            signal = _evaluate_snapshots(
                latest, earlier, recent_rows, baseline_rows,
                revenue_growth_threshold_pct, mcap_reaction_threshold_pct,
                outlier_max_share_pct, shape_mark_ratio, lookback_days, actual_lookback_days,
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
