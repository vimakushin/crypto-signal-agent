"""One-off replay/backtest script (TZ section 6/7; section 8 iteration 3's
"Backtest-module" idea, pulled forward because the user asked for it now).

Runs each of the three MVP signals' REAL evaluation logic -
signals/revenue_price_gap.py's `_evaluate_snapshots`,
signals/oi_divergence.py's `_evaluate_snapshots`,
signals/volume_breakout.py's `_evaluate` - imported and called DIRECTLY
against every historical point already stored in storage/db.sqlite, one
point at a time, pretending each one was "the latest point" in turn. The
formulas that decide whether a signal fires are never re-typed here: this
script only builds the (latest, earlier) row pairs and windows those three
functions expect, then reads the same object main.py would log as "FIRED".

Read-only: only SELECTs from storage/db.sqlite. Never writes, never creates
a table, never talks to any network API. Not part of the daily/6-hourly
main.py cycle - run manually, once (or again later, once more history has
accumulated):

    .venv\\Scripts\\python.exe scripts\\replay_signals.py

Three pitfalls specific to replaying stored history (as opposed to scanning
only the latest point, which is all signals/*.py's scan_watchlist() do
today) are handled here, one per signal - each explained in detail next to
the function that handles it:

  - revenue_price_gap: same-day duplicate defillama_snapshots rows (from
    repeated manual `main.py --cycle defillama` runs) are collapsed to one
    row per calendar day before pairing (see _dedupe_defillama_rows_by_day);
    rows with mcap=NULL are excluded from the "evaluated" denominator
    instead of silently counting as "evaluated, didn't fire" (see
    replay_revenue_price_gap); a pair is discarded if the real gap between
    the two dates is more than 1.5x the configured lookback_days (see
    MAX_GAP_MULTIPLIER); the "earlier" point for each evaluated day is
    looked up with storage.db.get_snapshot_days_before - the SAME production
    function signals/revenue_price_gap.py's own scan_watchlist() calls,
    which compares by full timestamp, not calendar date - so replay's
    pairing can never silently diverge from what the live scan would have
    picked, including on days where the dedupe step above had to choose
    between several same-day rows; consecutive calendar days (no gap) where
    the signal fired are collapsed into "episodes" (see
    _collapse_daily_episodes), the same idea as oi_divergence's
    _collapse_episodes below, because revenue_price_gap's 7-day rolling
    comparison window keeps one real revenue spike visible as "fired" for
    up to ~6-7 consecutive days in a row - left uncollapsed, that one event
    would be counted as 6-7 separate triggers instead of one.

  - oi_divergence: binance_oi_snapshots mixes 4h-spaced backfilled points
    with 1h-spaced live-collector points - evaluating every raw row would
    quadruple-weight the most recent ~36-48h. Replay instead evaluates on a
    fixed 4h UTC grid (see GRID_STEP_HOURS, _build_grid), using the same
    "closest point at or before" lookup the production code already uses
    (storage.db.get_binance_oi_snapshot_hours_before). Consecutive
    grid-points where the signal holds true are collapsed into "episodes"
    (see _collapse_episodes) so one sustained OI move isn't counted as
    several separate triggers.

  - volume_breakout: the first ~18 evaluated days of a protocol's history
    (where the resistance window has less than the full
    resistance_lookback_days but still clears
    volume_breakout.MIN_HISTORY_COVERAGE_FRACTION) have an artificially low
    resistance level - flagged and reported separately from "mature" points
    with a full window (see replay_volume_breakout's is_early tracking).
    Freshest available date per coin is reported, not silently assumed to
    be "today".

No forward-looking leakage: every pair this script builds only ever
compares a point to an EARLIER one already present in storage - the same
guarantee signals/*.py's own scan_watchlist() has (it only ever looks at
"latest" and "earlier", never anything after "latest"), just re-applied at
every historical point in turn instead of only the newest one.
"""
from __future__ import annotations

import json
import logging
import math
import statistics
import sys
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

# Same sys.path fix as scripts/backfill_history.py - this script lives one
# level below the project root, so `from signals import ...` / `from
# storage.db import ...` below would fail with ModuleNotFoundError without
# it (Python only puts the script's OWN directory on sys.path, not the
# project root, when run as `python scripts\replay_signals.py`).
sys.path.insert(0, str(PROJECT_ROOT))

import yaml  # noqa: E402

from signals import oi_divergence, revenue_price_gap, volume_breakout  # noqa: E402
from storage.db import (  # noqa: E402
    get_binance_oi_snapshot_hours_before,
    get_coingecko_price_history_before,
    get_connection,
    get_defillama_daily_revenue_window,
    get_snapshot_days_before,
)
from storage.episodes import Episode, collapse_into_episodes  # noqa: E402

# Grid cadence oi_divergence replay evaluates on - matches
# scripts/backfill_history.py's BINANCE_BACKFILL_PERIOD ("4h"), the coarser
# of the two granularities mixed in storage, so replay never has to
# interpolate or invent data at a finer resolution than backfill actually
# has for most of the stored history.
GRID_STEP_HOURS = 4

# revenue_price_gap: a pair is discarded (not scored either way) if the real
# gap between the two compared dates exceeds this multiple of the configured
# lookback_days - same "actual window can differ from configured" situation
# signals/oi_divergence.py already tracks via its own `actual_lookback_hours`
# field, formalized here as an explicit exclusion rule for replay specifically
# (not applied to the live scan_watchlist() paths, which weren't asked to
# change).
MAX_GAP_MULTIPLIER = 1.5


def _setup_logging() -> None:
    """Console + a rotating file in logs/replay_signals.log, separate from
    main.py's scheduler.log and scripts/backfill_history.py's backfill.log -
    this is a distinct, occasional, read-only analysis run.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        LOG_DIR.mkdir(exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                LOG_DIR / "replay_signals.log",
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
logger = logging.getLogger("replay_signals")


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> dict:
    """Load config.yaml (duplicated from main.py/backfill_history.py's own
    load_config, not imported, so this script doesn't trigger main.py's
    module-level logging setup as an import side effect).
    """
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# --------------------------------------------------------------------------
# Small stats helpers (stdlib only - no new requirements.txt dependency).
# --------------------------------------------------------------------------

def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile of an already-sorted list of floats.

    Args:
        sorted_values: non-empty list, ascending order.
        pct: 0-100.

    Returns:
        The interpolated value at that percentile.
    """
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)


def _describe(values: list[float], label: str) -> str:
    """One-line median/p90/p95/p99 summary of a metric's distribution, for
    judging how far a config.yaml threshold sits from a typical vs. rare
    value of that metric.
    """
    if not values:
        return f"{label}: n=0 (no evaluated points had this metric)"
    sv = sorted(values)
    return (
        f"{label}: n={len(sv)}, min={sv[0]:.2f}, median={_percentile(sv, 50):.2f}, "
        f"p90={_percentile(sv, 90):.2f}, p95={_percentile(sv, 95):.2f}, "
        f"p99={_percentile(sv, 99):.2f}, max={sv[-1]:.2f}"
    )


# --------------------------------------------------------------------------
# revenue_price_gap replay
# --------------------------------------------------------------------------

def _calendar_date(fetched_at_iso: str) -> date_cls:
    return datetime.fromisoformat(fetched_at_iso).date()


def _dedupe_defillama_rows_by_day(rows: list) -> list:
    """Collapse same-day duplicate defillama_snapshots rows down to ONE row
    per calendar date, keeping the row with the latest `fetched_at` that
    day.

    Pitfall this handles: the live collector (`main.py --cycle defillama`)
    wrote several rows for the same calendar date for some protocols during
    manual test runs (different `fetched_at` timestamps, same day) - without
    this, replay would score that one day multiple times, inflating both the
    "evaluated" count and the fired count.

    Args:
        rows: all defillama_snapshots rows for ONE protocol_slug, any order.

    Returns:
        Rows sorted ascending by calendar date, at most one per date.
    """
    best_by_day: dict[date_cls, object] = {}
    for row in rows:
        day = _calendar_date(row["fetched_at"])
        current = best_by_day.get(day)
        if current is None or datetime.fromisoformat(row["fetched_at"]) > datetime.fromisoformat(
            current["fetched_at"]
        ):
            best_by_day[day] = row
    return [best_by_day[d] for d in sorted(best_by_day)]


def _collapse_daily_episodes(entries: list[tuple[date_cls, bool | None]]) -> int:
    """Number of continuous "episodes" in a chronological, per-calendar-day
    fired sequence for ONE protocol - the revenue_price_gap equivalent of
    oi_divergence's _collapse_episodes below, adapted for an irregular
    (non-fixed-step) daily series instead of a fixed grid.

    An episode is a maximal run of CONSECUTIVE CALENDAR DAYS (no gap) where
    the signal held True. Two things break a run, both deliberately: a
    False/None flag (same as _collapse_episodes), AND a calendar-date gap
    between two consecutive entries even if both are True - e.g. day 5 and
    day 8 both firing, with no evaluated row for days 6-7 in between, is two
    episodes, not one, because a real day of data is missing, not just
    "didn't fire that day".

    Thin wrapper around storage/episodes.py's collapse_into_episodes (the
    shared primitive also used by storage/episodes.py's
    get_open_episodes/get_reviewable_episodes for the web labeling screen) -
    this function's own signature/return type (int, not the (start, end)
    pairs collapse_into_episodes returns) is unchanged for existing callers
    in this script.

    Args:
        entries: (calendar date, fired) pairs for one protocol, in ascending
            date order, one entry per evaluated (or skipped) day - fired is
            True (fired), False (evaluated, did not fire), or None (this
            day was skipped - no earlier pair, mcap=NULL, or gap too large;
            see replay_revenue_price_gap).

    Returns:
        Number of episodes.
    """
    episodes = collapse_into_episodes(
        entries, lambda prev_day, day: day == prev_day + timedelta(days=1)
    )
    return len(episodes)


def replay_revenue_price_gap(conn, watchlist: list[str], signal_cfg: dict) -> dict:
    """Replay signals/revenue_price_gap.py's `_evaluate_snapshots` over every
    stored defillama_snapshots day for every watchlist protocol.

    The "earlier" point for each evaluated day is looked up with
    storage.db.get_snapshot_days_before - the SAME function
    signals/revenue_price_gap.py's own scan_watchlist() calls in production,
    which compares by full timestamp (not calendar date) - so this never
    diverges from what the live signal would actually have paired, even on
    a day where _dedupe_defillama_rows_by_day above had to pick among
    several same-day rows for the "latest" side.

    Args:
        conn: open storage/db.py connection.
        watchlist: config.yaml watchlist.defillama_protocols.
        signal_cfg: config.yaml signals.revenue_price_gap.

    Returns:
        Dict with aggregate raw-day-level and episode counts (see
        _collapse_daily_episodes - one real revenue spike can stay visible
        as "fired" for several consecutive days under revenue_price_gap's
        rolling 7-day window, so the raw count alone overstates how many
        distinct events actually happened), a per-protocol breakdown, the
        revenue_growth_pct values seen across FIRED points only (for
        _describe - see the loop below for why this is fired-only, not
        all-evaluated, since this rewrite), and per_protocol_entries (slug
        -> the raw (date, RevenueGapSignal | False | None) list this
        function's own loop already builds to feed _collapse_daily_episodes
        - exposed so build_revenue_price_gap_episodes can turn it into
        dated Episode objects without a second pass over storage).
    """
    lookback_days = signal_cfg["lookback_days"]
    baseline_window_days = signal_cfg["baseline_window_days"]
    # Same check signals/revenue_price_gap.py's scan_watchlist() raises for
    # live use - duplicated here so a misconfigured lookback_days/
    # baseline_window_days doesn't silently make replay compare a "recent"
    # window that isn't actually narrower than the "baseline" it's compared
    # against.
    if lookback_days >= baseline_window_days:
        raise ValueError(
            f"signals.revenue_price_gap.lookback_days ({lookback_days}) must be strictly "
            f"less than baseline_window_days ({baseline_window_days}) in config.yaml - see "
            f"signals/revenue_price_gap.py's scan_watchlist() docstring"
        )
    revenue_threshold = signal_cfg["revenue_growth_threshold_pct"]
    mcap_threshold = signal_cfg["mcap_reaction_threshold_pct"]
    outlier_max_share_pct = signal_cfg.get("outlier_max_share_pct", 100)
    max_gap_days = lookback_days * MAX_GAP_MULTIPLIER
    min_revenue_total_usd = signal_cfg.get("min_revenue_total_usd", 0)
    # math.floor, not math.ceil - mirrors signals/revenue_price_gap.py's own
    # scan_watchlist() exactly (see that function's comment for why: ceil on
    # a small window like lookback_days=7 rounds "~90% coverage" up to
    # "100%, zero gaps tolerated"). Duplicated here rather than imported -
    # keep both in sync.
    min_valid_recent = math.floor(lookback_days * revenue_price_gap.MIN_HISTORY_COVERAGE_FRACTION)
    min_valid_baseline = math.floor(baseline_window_days * revenue_price_gap.MIN_HISTORY_COVERAGE_FRACTION)

    per_protocol: dict[str, dict] = {}
    # Every protocol's (date, RevenueGapSignal | False | None) entries list,
    # keyed by slug - the SAME list this loop already builds below to feed
    # _collapse_daily_episodes for the episode COUNT. Exposed here (an
    # additive return key, see this function's Returns docstring) purely so
    # build_revenue_price_gap_episodes can turn those same entries into
    # dated Episode objects without re-running this loop a second time.
    per_protocol_entries: dict[str, list[tuple[date_cls, "revenue_price_gap.RevenueGapSignal | bool | None"]]] = {}
    evaluated_total = 0
    fired_raw_total = 0
    episodes_total = 0
    no_earlier_pair = 0
    excluded_no_mcap = 0
    excluded_gap_too_large = 0
    excluded_liquidity = 0
    excluded_insufficient_daily_history = 0
    revenue_values: list[float] = []

    for slug in watchlist:
        rows = conn.execute(
            "SELECT * FROM defillama_snapshots WHERE protocol_slug = ? ORDER BY fetched_at ASC",
            (slug,),
        ).fetchall()
        stats = {
            "evaluated": 0, "fired": 0, "episodes": 0, "raw_rows": len(rows), "deduped_days": 0,
        }
        if not rows:
            per_protocol[slug] = stats
            continue

        deduped = _dedupe_defillama_rows_by_day(rows)
        dates = [_calendar_date(r["fetched_at"]) for r in deduped]
        stats["deduped_days"] = len(deduped)

        entries: list[tuple[date_cls, bool | None]] = []

        for i, latest in enumerate(deduped):
            latest_date = dates[i]

            earlier = get_snapshot_days_before(conn, slug, latest["fetched_at"], lookback_days)
            if earlier is None:
                no_earlier_pair += 1
                entries.append((latest_date, None))
                continue

            real_gap_days = (
                datetime.fromisoformat(latest["fetched_at"])
                - datetime.fromisoformat(earlier["fetched_at"])
            ).total_seconds() / 86400
            if real_gap_days > max_gap_days:
                excluded_gap_too_large += 1
                entries.append((latest_date, None))
                continue

            # `<= 0`, not just `is None`: a stored mcap of 0 or negative
            # (seen live for renzo - see signals/revenue_price_gap.py's
            # matching guard) is just as unusable for mcap_growth_pct as a
            # missing one. Without this, ~87 such points fell through this
            # prefilter into evaluated_total and were only actually
            # excluded later, silently, inside _evaluate_snapshots -
            # inflating the denominator of the reported fire rate.
            if (
                latest["mcap"] is None or latest["mcap"] <= 0
                or earlier["mcap"] is None or earlier["mcap"] <= 0
            ):
                excluded_no_mcap += 1
                entries.append((latest_date, None))
                continue

            # Same coverage/liquidity prefilters signals/revenue_price_gap.py's
            # scan_watchlist() applies live, using the same
            # get_defillama_daily_revenue_window() production function - so
            # replay never scores a point production would never even have
            # evaluated, and can never silently diverge from what windowing
            # production would have picked.
            end_date = latest_date.isoformat()
            recent_rows = get_defillama_daily_revenue_window(conn, slug, end_date, lookback_days)
            baseline_rows = get_defillama_daily_revenue_window(
                conn, slug, end_date, baseline_window_days
            )
            valid_recent = [r["revenue_usd"] for r in recent_rows if r["revenue_usd"] is not None]
            valid_baseline = [r["revenue_usd"] for r in baseline_rows if r["revenue_usd"] is not None]

            if len(valid_recent) < min_valid_recent or len(valid_baseline) < min_valid_baseline:
                excluded_insufficient_daily_history += 1
                entries.append((latest_date, None))
                continue

            recent_sum = sum(valid_recent)
            baseline_weekly_equivalent = statistics.median(valid_baseline) * lookback_days
            if recent_sum < min_revenue_total_usd or baseline_weekly_equivalent < min_revenue_total_usd:
                excluded_liquidity += 1
                entries.append((latest_date, None))
                continue

            evaluated_total += 1
            stats["evaluated"] += 1

            try:
                signal = revenue_price_gap._evaluate_snapshots(
                    latest, earlier, recent_rows, baseline_rows,
                    revenue_threshold, mcap_threshold, outlier_max_share_pct, lookback_days,
                )
            except revenue_price_gap._DataQualityRejected:
                # _evaluate_snapshots now raises instead of returning None
                # for its own internal guards (outlier share, base/current
                # median revenue non-positive - the gap and mcap guards are
                # already covered by this function's own prefilters above,
                # so this branch is effectively unreachable for those two).
                # Treat exactly like the old `signal = None` / not-fired
                # case so replay's counts are unaffected.
                signal = None
            fired = signal is not None
            # Stores the fired RevenueGapSignal object itself, not a bare
            # bool - _collapse_daily_episodes/collapse_into_episodes below
            # only ever check `if flag:` (truthiness), so a dataclass
            # instance works exactly like True there, unchanged. This is
            # what lets build_revenue_price_gap_episodes (below) reuse this
            # SAME per-protocol loop, instead of re-running it a second time
            # just to get the signal object back for an episode's
            # first/last_metric_value and details_json - see this
            # function's docstring return value note on per_protocol_entries.
            entries.append((latest_date, signal if fired else False))
            if fired:
                fired_raw_total += 1
                stats["fired"] += 1
                # Only collected when the signal actually fired - unlike the
                # old revenue_change_Xd_pct column (always present on every
                # evaluated row), revenue_growth_pct is only a field on the
                # returned RevenueGapSignal object itself, which
                # _evaluate_snapshots only constructs once it has already
                # decided to fire. This narrows what this distribution shows
                # (fired growth-% values, not all-evaluated growth-%), but
                # is still useful for judging how far above threshold actual
                # fires land.
                revenue_values.append(signal.revenue_growth_pct)

        episodes = _collapse_daily_episodes(entries)
        stats["episodes"] = episodes
        episodes_total += episodes

        per_protocol[slug] = stats
        per_protocol_entries[slug] = entries

    return {
        "lookback_days": lookback_days,
        "baseline_window_days": baseline_window_days,
        "revenue_threshold": revenue_threshold,
        "mcap_threshold": mcap_threshold,
        "outlier_max_share_pct": outlier_max_share_pct,
        "evaluated_total": evaluated_total,
        "fired_raw_total": fired_raw_total,
        "episodes_total": episodes_total,
        "no_earlier_pair": no_earlier_pair,
        "excluded_no_mcap": excluded_no_mcap,
        "excluded_gap_too_large": excluded_gap_too_large,
        "excluded_liquidity": excluded_liquidity,
        "excluded_insufficient_daily_history": excluded_insufficient_daily_history,
        "min_revenue_total_usd": min_revenue_total_usd,
        "per_protocol": per_protocol,
        "revenue_values": revenue_values,
        # See per_protocol_entries's declaration above this loop - not used
        # by _report_revenue_price_gap below, only by
        # build_revenue_price_gap_episodes (backtest date-level prep).
        "per_protocol_entries": per_protocol_entries,
    }


def _report_revenue_price_gap(result: dict) -> None:
    logger.info(
        "revenue_price_gap: lookback_days=%d, baseline_window_days=%d, "
        "revenue_growth_threshold_pct=%s, mcap_reaction_threshold_pct=%s, "
        "outlier_max_share_pct=%s, min_revenue_total_usd=%s (config.yaml)",
        result["lookback_days"], result["baseline_window_days"], result["revenue_threshold"],
        result["mcap_threshold"], result["outlier_max_share_pct"], result["min_revenue_total_usd"],
    )
    logger.info(
        "  Evaluated %d protocol-day point(s) total. Excluded from that count: %d with no "
        "earlier point at all yet (insufficient history), %d where either compared point "
        "had mcap missing or <= 0 (pre-CoinGecko-history era for that protocol, or a known "
        "CoinGecko placeholder value), %d where the real "
        "gap between the two dates exceeded %.1fx the configured %d days (data hole "
        "between backfill and live collection), %d where daily revenue history coverage "
        "(defillama_daily_revenue) was too thin for a trustworthy median on the recent "
        "and/or baseline window, %d where the recent window's revenue sum or the baseline "
        "window's weekly-equivalent median was below the $%s liquidity floor "
        "(min_revenue_total_usd) - same filters signals/revenue_price_gap.py's "
        "scan_watchlist() applies live, so these would never have been evaluated in "
        "production either.",
        result["evaluated_total"], result["no_earlier_pair"], result["excluded_no_mcap"],
        result["excluded_gap_too_large"], MAX_GAP_MULTIPLIER, result["lookback_days"],
        result["excluded_insufficient_daily_history"],
        result["excluded_liquidity"], result["min_revenue_total_usd"],
    )
    rate = (
        100 * result["fired_raw_total"] / result["evaluated_total"] if result["evaluated_total"] else 0.0
    )
    logger.info(
        "  Raw protocol-days where the condition held: %d/%d (%.2f%%). Collapsed into %d "
        "continuous episode(s) - the more meaningful count: revenue_price_gap compares "
        "against a ROLLING 7-day window, so one real revenue spike can stay visible as "
        "'fired' for up to ~6-7 consecutive days in a row and must not be counted as 6-7 "
        "separate triggers.",
        result["fired_raw_total"], result["evaluated_total"], rate, result["episodes_total"],
    )
    logger.info(
        "  Per protocol (evaluated / raw fired days / episodes / raw rows in DB / deduped "
        "calendar days):"
    )
    for slug in sorted(result["per_protocol"]):
        s = result["per_protocol"][slug]
        logger.info(
            "    %-24s evaluated=%-5d raw_fired=%-4d episodes=%-3d raw_rows=%-5d deduped_days=%-5d",
            slug, s["evaluated"], s["fired"], s["episodes"], s["raw_rows"], s["deduped_days"],
        )
    logger.info("  %s", _describe(
        result["revenue_values"], "revenue_growth_pct distribution over FIRED points only"
    ))


def build_revenue_price_gap_episodes(result: dict) -> list[Episode]:
    """Turn replay_revenue_price_gap's per_protocol_entries into dated
    Episode objects (storage/episodes.py's shared shape - the same one the
    manual-labeling web screen already reads from signal_events), for the
    backtest prep this script was extended for (BACKLOG.md's "Бэктест
    сегодня посчитать нельзя..." entry): "how would TODAY's formula have
    scored the whole history", with real episode start/end dates so a
    caller can look up price N days after each one.

    Does not re-run any evaluation - reuses the exact (date, signal) pairs
    replay_revenue_price_gap's own loop already produced, and groups them
    with the SAME storage/episodes.py's collapse_into_episodes primitive
    _collapse_daily_episodes (used for the plain episode COUNT) is already a
    thin wrapper over - not a fourth reimplementation of the grouping logic.

    Args:
        result: replay_revenue_price_gap's return value.

    Returns:
        Episode objects, oldest first within each protocol, protocols in
        the order per_protocol_entries iterates them (insertion order -
        watchlist order). `is_open` is always False - these are backtest
        episodes over closed historical data, not open signal_events rows
        being tracked against a live freshness cursor.
    """
    episodes: list[Episode] = []
    for slug, entries in result["per_protocol_entries"].items():
        day_episodes = collapse_into_episodes(
            entries, lambda prev_day, day: day == prev_day + timedelta(days=1)
        )
        by_day = dict(entries)
        for start_day, end_day in day_episodes:
            first_signal = by_day[start_day]
            last_signal = by_day[end_day]
            episodes.append(
                Episode(
                    signal_name="revenue_price_gap",
                    protocol_slug=slug,
                    episode_start_date=start_day.isoformat(),
                    episode_end_date=end_day.isoformat(),
                    duration_days=(end_day - start_day).days + 1,
                    is_open=False,
                    first_metric_value=first_signal.revenue_growth_pct,
                    first_details_json=_revenue_price_gap_details_json(first_signal, result),
                    last_metric_value=last_signal.revenue_growth_pct,
                    last_details_json=_revenue_price_gap_details_json(last_signal, result),
                    last_triggered_at=f"{end_day.isoformat()}T00:00:00+00:00",
                )
            )
    return episodes


def _revenue_price_gap_details_json(signal, result: dict) -> str:
    """Same field set main.py's run_defillama_cycle puts into
    signal_events.details_json for signal_name="revenue_price_gap" (see
    main.py, next to its save_signal_event(..., signal_name=
    "revenue_price_gap", ...) call) - so notifications/telegram_bot.py's
    _SIGNAL_FORMATTERS can render a backtest episode with the exact same
    formatter a live one uses.
    """
    return json.dumps({
        "revenue_growth_pct": signal.revenue_growth_pct,
        "revenue_growth_threshold_pct": result["revenue_threshold"],
        "mcap_growth_pct": signal.mcap_growth_pct,
        "mcap_reaction_threshold_pct": result["mcap_threshold"],
        "lookback_days": signal.lookback_days,
        "actual_lookback_days": signal.actual_lookback_days,
        "recent_median_daily_revenue": signal.recent_median_daily_revenue,
        "baseline_median_daily_revenue": signal.baseline_median_daily_revenue,
        "recent_week_sum": signal.recent_week_sum,
        "max_day_share_pct": signal.max_day_share_pct,
        "mcap_now": signal.mcap_now,
        "mcap_before": signal.mcap_before,
        "symbol": signal.symbol,
    })


# --------------------------------------------------------------------------
# oi_divergence replay
# --------------------------------------------------------------------------

def _build_grid(start: datetime, end: datetime, step_hours: int) -> list[datetime]:
    """UTC-aligned grid boundaries (00:00, 04:00, 08:00, ... per
    GRID_STEP_HOURS) covering [start, end].

    Args:
        start: earliest stored oi_timestamp for a symbol.
        end: latest stored oi_timestamp for a symbol.
        step_hours: grid spacing (GRID_STEP_HOURS).

    Returns:
        Grid boundaries as tz-aware UTC datetimes, oldest first. The first
        boundary is rounded DOWN to the nearest step from `start` - a
        boundary that lands before any real data simply yields no match at
        lookup time (see replay_oi_divergence), not a crash.
    """
    aligned_start = start.replace(minute=0, second=0, microsecond=0)
    aligned_start = aligned_start.replace(hour=(aligned_start.hour // step_hours) * step_hours)
    step = timedelta(hours=step_hours)
    grid = []
    t = aligned_start
    while t <= end:
        grid.append(t)
        t += step
    return grid


def _collapse_episodes(fired_flags: list[bool | None]) -> int:
    """Number of continuous "episodes" in a chronological sequence of
    per-grid-point fired flags.

    An episode is a maximal run of CONSECUTIVE grid-points where the signal
    held True. A grid-point with insufficient data (`None`, not evaluated)
    breaks the run rather than being skipped over - two fired stretches
    separated by a data gap are treated as two distinct episodes, not
    silently bridged into one, since continuity in the underlying grid was
    actually broken.

    Thin wrapper around storage/episodes.py's collapse_into_episodes - see
    _collapse_daily_episodes above for why this is a shared primitive now.
    Positions here are plain list indices (0, 1, 2, ...), always contiguous
    by construction (the grid itself is already fixed-step - see
    replay_oi_divergence/_build_grid), so the only thing that actually
    breaks a run is a False/None flag, exactly as before.

    Args:
        fired_flags: one entry per grid boundary, in chronological order -
            True (fired), False (evaluated, did not fire), or None (not
            evaluated - insufficient data at that point in time).

    Returns:
        Number of episodes.
    """
    entries = list(enumerate(fired_flags))
    episodes = collapse_into_episodes(entries, lambda prev_i, i: i == prev_i + 1)
    return len(episodes)


def replay_oi_divergence(conn, symbols: list[str], signal_cfg: dict) -> dict:
    """Replay signals/oi_divergence.py's `_evaluate_snapshots` over a fixed
    GRID_STEP_HOURS-spaced grid for every watchlist symbol, instead of over
    every raw stored row.

    Args:
        conn: open storage/db.py connection.
        symbols: config.yaml watchlist.binance_futures_symbols.
        signal_cfg: config.yaml signals.oi_divergence.

    Returns:
        Dict with aggregate raw-point and episode counts, a per-symbol
        breakdown, the raw oi_growth_pct / actual-elapsed-hours values seen
        across all evaluated grid-points, and per_symbol_entries (symbol ->
        the raw (boundary_datetime, OiDivergenceSignal | False | None) list
        this function's own loop already builds to feed _collapse_episodes -
        exposed so build_oi_divergence_episodes can turn it into dated
        Episode objects without a second pass over storage).
    """
    lookback_hours = signal_cfg["lookback_hours"]
    oi_threshold = signal_cfg["oi_growth_threshold_pct"]
    price_threshold = signal_cfg["price_change_threshold_pct"]
    min_oi_value_usdt = signal_cfg.get("min_oi_value_usdt", 0)

    per_symbol_raw: dict[str, dict] = {}
    per_symbol_episodes: dict[str, int] = {}
    # symbol -> (boundary_datetime, OiDivergenceSignal | False | None) list -
    # see this function's Returns docstring; additive, not used by
    # _report_oi_divergence, only by build_oi_divergence_episodes.
    per_symbol_entries: dict[str, list] = {}
    evaluated_total = 0
    insufficient_total = 0
    insufficient_liquidity_total = 0
    fired_raw_total = 0
    episodes_total = 0
    oi_growth_values: list[float] = []
    actual_lookback_values: list[float] = []

    for symbol in symbols:
        ts_rows = conn.execute(
            "SELECT oi_timestamp FROM binance_oi_snapshots WHERE symbol = ? ORDER BY oi_timestamp ASC",
            (symbol,),
        ).fetchall()
        if not ts_rows:
            per_symbol_raw[symbol] = {"evaluated": 0, "fired": 0}
            per_symbol_episodes[symbol] = 0
            per_symbol_entries[symbol] = []
            continue

        first_ts = datetime.fromisoformat(ts_rows[0]["oi_timestamp"])
        last_ts = datetime.fromisoformat(ts_rows[-1]["oi_timestamp"])
        grid = _build_grid(first_ts, last_ts, GRID_STEP_HOURS)

        symbol_evaluated = 0
        symbol_fired = 0
        fired_flags: list[bool | None] = []

        for boundary in grid:
            latest = get_binance_oi_snapshot_hours_before(conn, symbol, boundary.isoformat(), 0)
            if latest is None:
                fired_flags.append(None)
                insufficient_total += 1
                continue

            # Liquidity/noise floor (TZ section 9) - same check
            # signals/oi_divergence.py's scan_watchlist() applies live,
            # BEFORE looking up the earlier point, so replay never scores a
            # grid-point production would never even have evaluated.
            if latest["oi_value_usdt"] is None or latest["oi_value_usdt"] < min_oi_value_usdt:
                fired_flags.append(None)
                insufficient_liquidity_total += 1
                continue

            earlier = get_binance_oi_snapshot_hours_before(
                conn, symbol, latest["oi_timestamp"], lookback_hours
            )
            if earlier is None:
                fired_flags.append(None)
                insufficient_total += 1
                continue

            actual_hours = oi_divergence._hours_between(latest["oi_timestamp"], earlier["oi_timestamp"])
            actual_lookback_values.append(actual_hours)

            evaluated_total += 1
            symbol_evaluated += 1

            # Permissive-threshold call solely to read the raw computed
            # oi_growth_pct that _evaluate_snapshots's own formula produces,
            # regardless of whether the REAL configured thresholds would
            # gate it - this reuses the production formula (via the
            # production function) instead of recomputing the percentage
            # here, so the distribution below can never silently drift from
            # what actually runs live. Thresholds are set far outside any
            # realistic value so the function always proceeds to compute
            # and return them (still returns None if oi/price data itself
            # is missing or zero, same as the real call).
            raw = oi_divergence._evaluate_snapshots(latest, earlier, -1e18, 1e18, actual_hours)
            if raw is not None:
                oi_growth_values.append(raw.oi_growth_pct)

            signal = oi_divergence._evaluate_snapshots(
                latest, earlier, oi_threshold, price_threshold, actual_hours,
            )
            fired = signal is not None
            # Stores the fired OiDivergenceSignal object itself, not a bare
            # bool - same reasoning as replay_revenue_price_gap's entries
            # list above: _collapse_episodes/collapse_into_episodes only
            # check truthiness, so this is unchanged for them, and it's what
            # lets build_oi_divergence_episodes (below) reuse this SAME
            # per-symbol loop instead of a second pass over storage.
            fired_flags.append(signal if fired else False)
            if fired:
                fired_raw_total += 1
                symbol_fired += 1

        per_symbol_raw[symbol] = {"evaluated": symbol_evaluated, "fired": symbol_fired}
        episodes = _collapse_episodes(fired_flags)
        per_symbol_episodes[symbol] = episodes
        episodes_total += episodes
        # grid and fired_flags are the same length and order (one fired_flags
        # entry per grid boundary, appended in the same loop above, even for
        # boundaries with insufficient data - see the `None` appends above).
        # Paired here into (boundary_datetime, signal | False | None) so
        # build_oi_divergence_episodes can group by calendar date without
        # re-running this loop.
        per_symbol_entries[symbol] = list(zip(grid, fired_flags))

    return {
        "lookback_hours": lookback_hours,
        "oi_threshold": oi_threshold,
        "price_threshold": price_threshold,
        "min_oi_value_usdt": min_oi_value_usdt,
        "evaluated_total": evaluated_total,
        "insufficient_total": insufficient_total,
        "insufficient_liquidity_total": insufficient_liquidity_total,
        "fired_raw_total": fired_raw_total,
        "episodes_total": episodes_total,
        "per_symbol_raw": per_symbol_raw,
        "per_symbol_episodes": per_symbol_episodes,
        "oi_growth_values": oi_growth_values,
        "actual_lookback_values": actual_lookback_values,
        "per_symbol_entries": per_symbol_entries,
    }


def _report_oi_divergence(result: dict) -> None:
    logger.info(
        "oi_divergence: lookback_hours=%d, oi_growth_threshold_pct=%s, "
        "price_change_threshold_pct=%s, min_oi_value_usdt=%s (config.yaml); replay grid "
        "step=%dh (resampled from mixed 1h/4h stored granularity - see module docstring)",
        result["lookback_hours"], result["oi_threshold"], result["price_threshold"],
        result["min_oi_value_usdt"], GRID_STEP_HOURS,
    )
    logger.info(
        "  Evaluated %d grid-point(s) across the whole watchlist (%d grid-point(s) skipped "
        "for insufficient history at that point in time - no stored data yet, or not far "
        "enough back to compare; %d grid-point(s) skipped for Open Interest below the $%s "
        "liquidity floor (min_oi_value_usdt) - same filter signals/oi_divergence.py's "
        "scan_watchlist() applies live, so these would never have been evaluated in "
        "production either).",
        result["evaluated_total"], result["insufficient_total"],
        result["insufficient_liquidity_total"], result["min_oi_value_usdt"],
    )
    rate = 100 * result["fired_raw_total"] / result["evaluated_total"] if result["evaluated_total"] else 0.0
    logger.info(
        "  Raw grid-points where the condition held: %d/%d (%.2f%%). Collapsed into %d "
        "continuous episode(s) - the more meaningful count, since one sustained OI move "
        "can hold true across several adjacent %dh grid-points and shouldn't be counted "
        "as several separate triggers.",
        result["fired_raw_total"], result["evaluated_total"], rate,
        result["episodes_total"], GRID_STEP_HOURS,
    )
    logger.info("  Per symbol (evaluated grid-points / raw fired points / episodes):")
    for symbol in sorted(result["per_symbol_raw"]):
        raw = result["per_symbol_raw"][symbol]
        episodes = result["per_symbol_episodes"].get(symbol, 0)
        logger.info(
            "    %-10s evaluated=%-5d raw_fired=%-4d episodes=%-3d",
            symbol, raw["evaluated"], raw["fired"], episodes,
        )
    logger.info("  %s", _describe(
        result["oi_growth_values"], "oi_growth_pct distribution over evaluated grid-points"
    ))
    logger.info("  %s", _describe(
        result["actual_lookback_values"], "actual elapsed hours between compared points"
    ))


def build_oi_divergence_episodes(result: dict) -> list[Episode]:
    """Turn replay_oi_divergence's per_symbol_entries into dated Episode
    objects - the oi_divergence counterpart of
    build_revenue_price_gap_episodes above (see that function's docstring
    for the shared backtest-prep motivation).

    Grouping stays on the same GRID_STEP_HOURS-spaced grid
    replay_oi_divergence itself evaluates on (contiguity = "next boundary is
    exactly GRID_STEP_HOURS later", same idea as _collapse_episodes' plain
    positional-index check, expressed directly over the boundary datetimes
    here instead of list indices since per_symbol_entries already pairs
    each flag with its real boundary) - episode_start_date/episode_end_date
    are still reported as calendar dates ("YYYY-MM-DD", the same shape every
    other Episode in the project uses), taken from the first/last grid
    boundary in the episode.

    Args:
        result: replay_oi_divergence's return value.

    Returns:
        Episode objects, oldest first within each symbol. `is_open` is
        always False - see build_revenue_price_gap_episodes.
    """
    episodes: list[Episode] = []
    step = timedelta(hours=GRID_STEP_HOURS)
    for symbol, entries in result["per_symbol_entries"].items():
        grid_episodes = collapse_into_episodes(
            entries, lambda prev_dt, dt: dt == prev_dt + step
        )
        by_boundary = dict(entries)
        for start_dt, end_dt in grid_episodes:
            first_signal = by_boundary[start_dt]
            last_signal = by_boundary[end_dt]
            start_date = start_dt.date()
            end_date = end_dt.date()
            episodes.append(
                Episode(
                    signal_name="oi_divergence",
                    protocol_slug=symbol,
                    episode_start_date=start_date.isoformat(),
                    episode_end_date=end_date.isoformat(),
                    duration_days=(end_date - start_date).days + 1,
                    is_open=False,
                    first_metric_value=first_signal.oi_growth_pct,
                    first_details_json=_oi_divergence_details_json(first_signal, result),
                    last_metric_value=last_signal.oi_growth_pct,
                    last_details_json=_oi_divergence_details_json(last_signal, result),
                    last_triggered_at=end_dt.isoformat(),
                )
            )
    return episodes


def _oi_divergence_details_json(signal, result: dict) -> str:
    """Same field set main.py's run_binance_futures_cycle puts into
    signal_events.details_json for signal_name="oi_divergence" (see
    main.py, next to its save_signal_event(..., signal_name="oi_divergence",
    ...) call) - see _revenue_price_gap_details_json above for why.
    """
    return json.dumps({
        "lookback_hours": signal.lookback_hours,
        "oi_growth_pct": signal.oi_growth_pct,
        "price_change_pct": signal.price_change_pct,
        "oi_now": signal.oi_now,
        "oi_before": signal.oi_before,
        "price_now": signal.price_now,
        "price_before": signal.price_before,
        "oi_growth_threshold_pct": result["oi_threshold"],
        "price_change_threshold_pct": result["price_threshold"],
    })


# --------------------------------------------------------------------------
# volume_breakout replay
# --------------------------------------------------------------------------

def _resolve_gecko_ids(conn, watchlist: list[str]) -> dict[str, str | None]:
    """slug -> gecko_id, read from whatever's already stored in
    coingecko_price_history (this script never calls collectors/defillama.py
    live, unlike main.py's own resolution via defillama.collect()).
    """
    mapping: dict[str, str | None] = {}
    for slug in watchlist:
        row = conn.execute(
            "SELECT gecko_id FROM coingecko_price_history WHERE protocol_slug = ? LIMIT 1",
            (slug,),
        ).fetchone()
        mapping[slug] = row["gecko_id"] if row else None
    return mapping


def replay_volume_breakout(
    conn, watchlist: list[str], signal_cfg: dict, slug_to_gecko_id: dict[str, str | None]
) -> dict:
    """Replay signals/volume_breakout.py's `_evaluate` over every stored
    coingecko_price_history day for every watchlist protocol.

    Args:
        conn: open storage/db.py connection.
        watchlist: config.yaml watchlist.defillama_protocols.
        signal_cfg: config.yaml signals.volume_breakout.
        slug_to_gecko_id: see _resolve_gecko_ids.

    Returns:
        Dict with aggregate counts (including a separate early-window vs.
        mature-window breakdown - see module docstring), a per-protocol
        breakdown, the raw volume_ratio values seen across all evaluated
        points, and per_protocol_entries (slug -> a (date,
        VolumeBreakoutSignal | False | None) list, one entry per row this
        function's own loop iterates - exposed so
        build_volume_breakout_episodes can turn it into dated Episode
        objects without a second pass over storage; no equivalent list
        existed here before this, unlike the other two replay_* functions).
    """
    resistance_days = signal_cfg["resistance_lookback_days"]
    volume_days = signal_cfg["volume_avg_lookback_days"]
    ratio_threshold = signal_cfg["volume_ratio_threshold"]
    min_resistance_values = math.ceil(resistance_days * volume_breakout.MIN_HISTORY_COVERAGE_FRACTION)
    min_volume_values = math.ceil(volume_days * volume_breakout.MIN_HISTORY_COVERAGE_FRACTION)
    min_volume_avg_usd = signal_cfg.get("min_volume_avg_usd", 0)

    per_protocol: dict[str, dict] = {}
    # slug -> (date, VolumeBreakoutSignal | False | None) list, one entry
    # per row iterated below (not just evaluated ones - see the None
    # appends at each skip point) - NEW tracking, unlike
    # replay_revenue_price_gap/replay_oi_divergence's per_protocol_entries/
    # per_symbol_entries above (which already had an equivalent list to
    # extend): replay_volume_breakout never counted episodes at all before
    # this. Built alongside the existing loop below so
    # build_volume_breakout_episodes can group it with the same
    # collapse_into_episodes primitive the other two signals use, without a
    # second pass over storage.
    per_protocol_entries: dict[str, list] = {}
    latest_dates: dict[str, str] = {}
    evaluated_total = 0
    fired_total = 0
    insufficient_total = 0
    insufficient_liquidity_total = 0
    early_period_total = 0
    mature_period_total = 0
    early_fired = 0
    mature_fired = 0
    volume_ratio_values: list[float] = []

    for slug in watchlist:
        gecko_id = slug_to_gecko_id.get(slug)
        stats = {"evaluated": 0, "fired": 0, "gecko_id": gecko_id}
        if not gecko_id:
            per_protocol[slug] = stats
            per_protocol_entries[slug] = []
            continue

        rows = conn.execute(
            "SELECT * FROM coingecko_price_history WHERE gecko_id = ? ORDER BY date ASC",
            (gecko_id,),
        ).fetchall()
        if rows:
            latest_dates[slug] = rows[-1]["date"]

        entries: list[tuple[date_cls, object]] = []

        for latest in rows:
            day = date_cls.fromisoformat(latest["date"])

            if latest["price"] is None or latest["volume"] is None:
                entries.append((day, None))
                continue

            resistance_rows = get_coingecko_price_history_before(
                conn, gecko_id, latest["date"], resistance_days
            )
            resistance_prices = volume_breakout._valid_prices(resistance_rows)
            if len(resistance_prices) < min_resistance_values:
                insufficient_total += 1
                entries.append((day, None))
                continue

            volume_rows = get_coingecko_price_history_before(conn, gecko_id, latest["date"], volume_days)
            volume_values = volume_breakout._valid_volumes(volume_rows)
            if len(volume_values) < min_volume_values:
                insufficient_total += 1
                entries.append((day, None))
                continue

            # Liquidity/noise floor (TZ section 9) - same check and same
            # `volume_avg` (already coverage-checked volume_values, average
            # over volume_avg_lookback_days) signals/volume_breakout.py's
            # scan_watchlist() applies live, BEFORE calling _evaluate, so
            # replay never scores a point production would never even have
            # evaluated. Computed once here and reused below for
            # volume_ratio_values, not a second divergent computation.
            volume_avg = sum(volume_values) / len(volume_values)
            if volume_avg < min_volume_avg_usd:
                insufficient_liquidity_total += 1
                entries.append((day, None))
                continue

            evaluated_total += 1
            stats["evaluated"] += 1

            # "Early" per the module docstring: resistance window clears the
            # coverage floor (checked above) but is still shorter than the
            # full configured resistance_lookback_days - resistance level is
            # measured over a shorter, structurally lower window there.
            is_early = len(resistance_prices) < resistance_days
            if is_early:
                early_period_total += 1
            else:
                mature_period_total += 1

            # volume_ratio isn't returned by _evaluate() for points where
            # price never broke resistance (that check short-circuits before
            # volume_ratio is computed) - computed directly here instead so
            # the distribution covers ALL evaluated points, not just the
            # rare ones that already cleared the resistance gate. Mirrors
            # _evaluate's own one-line formula (volume_now / average of
            # volume_values) exactly; volume_values itself is already
            # filtered by the real volume_breakout._valid_volumes(), so this
            # is not a second, divergent implementation of any signal logic.
            #
            # `latest["price"]`/`latest["volume"]` truthy (not just non-NULL,
            # already checked above by this loop's own entry filter) matters
            # here specifically: _evaluate()'s FIRST line is
            # `if not price_now or not volume_now: return None`, which stops
            # BEFORE computing volume_ratio for a stored zero (not just a
            # NULL) price/volume - without this same truthy check here,
            # those zero points would still land in volume_ratio_values as
            # "phantom" 0.0 entries the real signal never actually computes,
            # dragging down this distribution's min/median for no reason.
            # (volume_avg itself was already computed above, for the
            # min_volume_avg_usd liquidity check - reused here as-is, not
            # recomputed.)
            if volume_avg and latest["price"] and latest["volume"]:
                volume_ratio_values.append(latest["volume"] / volume_avg)

            signal = volume_breakout._evaluate(
                latest, resistance_prices, volume_values,
                resistance_days, volume_days, ratio_threshold,
            )
            entries.append((day, signal if signal is not None else False))
            if signal is not None:
                fired_total += 1
                stats["fired"] += 1
                if is_early:
                    early_fired += 1
                else:
                    mature_fired += 1

        per_protocol[slug] = stats
        per_protocol_entries[slug] = entries

    return {
        "resistance_days": resistance_days,
        "volume_days": volume_days,
        "ratio_threshold": ratio_threshold,
        "min_volume_avg_usd": min_volume_avg_usd,
        "evaluated_total": evaluated_total,
        "fired_total": fired_total,
        "insufficient_total": insufficient_total,
        "insufficient_liquidity_total": insufficient_liquidity_total,
        "early_period_total": early_period_total,
        "mature_period_total": mature_period_total,
        "early_fired": early_fired,
        "mature_fired": mature_fired,
        "per_protocol": per_protocol,
        "latest_dates": latest_dates,
        "volume_ratio_values": volume_ratio_values,
        # See per_protocol_entries's declaration above this loop - not used
        # by _report_volume_breakout below, only by
        # build_volume_breakout_episodes (backtest date-level prep).
        "per_protocol_entries": per_protocol_entries,
    }


def _report_volume_breakout(result: dict) -> None:
    logger.info(
        "volume_breakout: resistance_lookback_days=%d, volume_avg_lookback_days=%d, "
        "volume_ratio_threshold=%s, min_volume_avg_usd=%s (config.yaml)",
        result["resistance_days"], result["volume_days"], result["ratio_threshold"],
        result["min_volume_avg_usd"],
    )
    logger.info(
        "  Evaluated %d protocol-day point(s) total (%d skipped for insufficient trailing "
        "history - below the %.0f%% coverage floor for the resistance and/or volume "
        "window; see signals/volume_breakout.py's MIN_HISTORY_COVERAGE_FRACTION; %d skipped "
        "for average daily volume below the $%s liquidity floor (min_volume_avg_usd) - same "
        "filter signals/volume_breakout.py's scan_watchlist() applies live, so these would "
        "never have been evaluated in production either).",
        result["evaluated_total"], result["insufficient_total"],
        volume_breakout.MIN_HISTORY_COVERAGE_FRACTION * 100,
        result["insufficient_liquidity_total"], result["min_volume_avg_usd"],
    )
    early_rate = (100 * result["early_fired"] / result["early_period_total"]
                  if result["early_period_total"] else 0.0)
    mature_rate = (100 * result["mature_fired"] / result["mature_period_total"]
                   if result["mature_period_total"] else 0.0)
    logger.info(
        "  Of those, %d were 'early' points (resistance window shorter than the full %d "
        "days, but still above the coverage floor - resistance level is measured over a "
        "shorter, structurally LOWER window there, easier to break) and %d were 'mature' "
        "(full %d-day window). Fired on %d/%d early points (%.2f%%) vs. %d/%d mature "
        "points (%.2f%%) - a notably higher early rate would mean the early period is "
        "inflating the overall fired count with an artifact, not a real breakout.",
        result["early_period_total"], result["resistance_days"], result["mature_period_total"],
        result["resistance_days"], result["early_fired"], result["early_period_total"],
        early_rate, result["mature_fired"], result["mature_period_total"], mature_rate,
    )
    rate = 100 * result["fired_total"] / result["evaluated_total"] if result["evaluated_total"] else 0.0
    logger.info("  Signal would have FIRED on %d/%d evaluated points total (%.2f%%).",
                result["fired_total"], result["evaluated_total"], rate)
    logger.info("  Per protocol (evaluated / fired / gecko_id / most recent stored date):")
    for slug in sorted(result["per_protocol"]):
        s = result["per_protocol"][slug]
        latest_date = result["latest_dates"].get(slug, "n/a")
        logger.info(
            "    %-24s evaluated=%-5d fired=%-4d gecko_id=%-16s latest_date=%s",
            slug, s["evaluated"], s["fired"], s["gecko_id"] or "-", latest_date,
        )
    logger.info("  %s", _describe(
        result["volume_ratio_values"], "volume_ratio distribution over evaluated points"
    ))
    if result["latest_dates"]:
        overall_max = max(result["latest_dates"].values())
        today = datetime.now(timezone.utc).date().isoformat()
        logger.info(
            "  Freshest stored data across the whole watchlist reaches up to %s (today, at "
            "replay run time, is %s) - replay only covers up to that date. If it lags "
            "noticeably behind today, that's a separate CoinGecko-collection question this "
            "script does not try to fix, only reports.",
            overall_max, today,
        )


def build_volume_breakout_episodes(result: dict) -> list[Episode]:
    """Turn replay_volume_breakout's per_protocol_entries into dated Episode
    objects - the volume_breakout counterpart of
    build_revenue_price_gap_episodes/build_oi_divergence_episodes above (see
    build_revenue_price_gap_episodes's docstring for the shared backtest-
    prep motivation).

    Groups on plain consecutive calendar days (coingecko_price_history has
    at most one row per date already), the same contiguity rule
    build_revenue_price_gap_episodes uses - unlike revenue_price_gap this
    signal has no rolling-window "stays visible as fired for several days"
    behavior of its own, but consecutive days CAN still legitimately both
    fire (e.g. a breakout that holds for 2-3 days running), and those should
    still be one episode, not several.

    Args:
        result: replay_volume_breakout's return value.

    Returns:
        Episode objects, oldest first within each protocol. `is_open` is
        always False - see build_revenue_price_gap_episodes.
    """
    episodes: list[Episode] = []
    for slug, entries in result["per_protocol_entries"].items():
        day_episodes = collapse_into_episodes(
            entries, lambda prev_day, day: day == prev_day + timedelta(days=1)
        )
        by_day = dict(entries)
        for start_day, end_day in day_episodes:
            first_signal = by_day[start_day]
            last_signal = by_day[end_day]
            episodes.append(
                Episode(
                    signal_name="volume_breakout",
                    protocol_slug=slug,
                    episode_start_date=start_day.isoformat(),
                    episode_end_date=end_day.isoformat(),
                    duration_days=(end_day - start_day).days + 1,
                    is_open=False,
                    first_metric_value=first_signal.volume_ratio,
                    first_details_json=_volume_breakout_details_json(first_signal, result),
                    last_metric_value=last_signal.volume_ratio,
                    last_details_json=_volume_breakout_details_json(last_signal, result),
                    last_triggered_at=f"{end_day.isoformat()}T00:00:00+00:00",
                )
            )
    return episodes


def _volume_breakout_details_json(signal, result: dict) -> str:
    """Same field set main.py's _run_volume_breakout puts into
    signal_events.details_json for signal_name="volume_breakout" (see
    main.py, next to its save_signal_event(..., signal_name=
    "volume_breakout", ...) call) - see _revenue_price_gap_details_json
    above for why.
    """
    return json.dumps({
        "gecko_id": signal.gecko_id,
        "date": signal.date,
        "price_now": signal.price_now,
        "resistance_level": signal.resistance_level,
        "resistance_lookback_days": signal.resistance_lookback_days,
        "resistance_window_days_available": signal.resistance_window_days_available,
        "volume_now": signal.volume_now,
        "volume_avg": signal.volume_avg,
        "volume_avg_lookback_days": signal.volume_avg_lookback_days,
        "volume_window_days_available": signal.volume_window_days_available,
        "volume_ratio": signal.volume_ratio,
        "volume_ratio_threshold": result["ratio_threshold"],
    })


# --------------------------------------------------------------------------
# Episode-with-dates prep for the backtest (BACKLOG.md's "Бэктест сегодня
# посчитать нельзя..." entry) - see build_revenue_price_gap_episodes/
# build_oi_divergence_episodes/build_volume_breakout_episodes above. This
# script itself doesn't compute any backtest hit-rate - it only logs a
# sample of the dated episodes each builder produces, so a human (or
# signal-validator) can sanity-check dates/values before they're used for
# real by the backtest measurement.
# --------------------------------------------------------------------------

def _report_episode_sample(signal_name: str, episodes: list[Episode], sample_size: int = 3) -> None:
    """Log the total episode count and up to `sample_size` example episodes
    (oldest first) for one signal's build_*_episodes() output.
    """
    logger.info(
        "  %s: %d dated episode(s) built across the whole watchlist.",
        signal_name, len(episodes),
    )
    for ep in episodes[:sample_size]:
        logger.info(
            "    sample: %s %s..%s (%dd) first=%.2f last=%.2f",
            ep.protocol_slug, ep.episode_start_date, ep.episode_end_date,
            ep.duration_days, ep.first_metric_value, ep.last_metric_value,
        )


def main() -> None:
    cfg = load_config()
    db_path = Path(cfg["storage"]["sqlite_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path

    logger.info("=== replay_signals.py: one-off historical replay (read-only) ===")
    logger.info("Database: %s", db_path)

    conn = get_connection(db_path)
    try:
        watchlist_defillama = cfg["watchlist"]["defillama_protocols"]
        watchlist_binance = cfg["watchlist"]["binance_futures_symbols"]

        logger.info("")
        logger.info("--- revenue_price_gap (TZ 4.5) ---")
        rev_result = replay_revenue_price_gap(
            conn, watchlist_defillama, cfg["signals"]["revenue_price_gap"]
        )
        _report_revenue_price_gap(rev_result)
        rev_episodes = build_revenue_price_gap_episodes(rev_result)
        _report_episode_sample("revenue_price_gap", rev_episodes)

        logger.info("")
        logger.info("--- oi_divergence (TZ 4.1) ---")
        oi_result = replay_oi_divergence(
            conn, watchlist_binance, cfg["signals"]["oi_divergence"]
        )
        _report_oi_divergence(oi_result)
        oi_episodes = build_oi_divergence_episodes(oi_result)
        _report_episode_sample("oi_divergence", oi_episodes)

        logger.info("")
        logger.info("--- volume_breakout (TZ 4.7) ---")
        slug_to_gecko_id = _resolve_gecko_ids(conn, watchlist_defillama)
        vb_result = replay_volume_breakout(
            conn, watchlist_defillama, cfg["signals"]["volume_breakout"], slug_to_gecko_id
        )
        _report_volume_breakout(vb_result)
        vb_episodes = build_volume_breakout_episodes(vb_result)
        _report_episode_sample("volume_breakout", vb_episodes)

        logger.info("")
        logger.info("=== replay complete ===")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("replay_signals.py failed")
        sys.exit(1)
