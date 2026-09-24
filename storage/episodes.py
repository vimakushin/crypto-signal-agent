"""Episode grouping: turning raw signal_events rows into "episodes" (one
signal firing on the same protocol for a stretch of consecutive days is one
episode, not N separate observations - see BACKLOG.md's "signal_events
копит почти-дубли" and "Длинные эпизоды revenue_price_gap" entries), plus
the read functions behind the manual-labeling web screen ("ручная разметка
результатов срабатываний", TZ section 6).

`collapse_into_episodes` below is the shared primitive
scripts/replay_signals.py's `_collapse_daily_episodes` (revenue_price_gap,
calendar-day granularity) and `_collapse_episodes` (oi_divergence, fixed
4h-grid-index granularity) are both rewritten on top of - see this module's
own docstring on that function for how one generic implementation covers
both cases without duplicating the run-detection logic twice.

Deliberately placed in storage/, not scoring/: get_open_episodes and
get_reviewable_episodes below need scoring/ranker.py's
default_since_by_signal() (the same "is this signal_events row still fresh"
cursor rank_candidates() uses for the daily ranking - reused as-is here, not
reimplemented) AND storage/db.py's connection/table access. scoring/ranker.py
already imports from storage/db.py, so importing scoring.ranker FROM HERE
(storage/episodes.py -> scoring/ranker.py -> storage/db.py) does not create a
cycle - the reverse (scoring/ranker.py importing storage/episodes.py) is not
done anywhere.
"""
from __future__ import annotations

import sqlite3
import statistics
import sys
from dataclasses import dataclass
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import Callable, TypeVar

# Same sys.path fix as scoring/ranker.py / scripts/replay_signals.py - this
# module lives one level below the project root, so `from scoring.ranker
# import ...` below would fail with ModuleNotFoundError without it when
# this file is run/imported in a context that hasn't already put the
# project root on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scoring.ranker import default_since_by_signal  # noqa: E402
from storage.db import (  # noqa: E402
    get_binance_oi_snapshot_hours_before,
    get_coingecko_price_before,
    get_latest_binance_oi_snapshot,
    get_latest_coingecko_price_history_by_slug,
)

T = TypeVar("T")

# The date signals/revenue_price_gap.py's formula changed from comparing
# WEEKLY SUMS to comparing DAILY MEDIANS (see BACKLOG.md's "revenue_price_gap
# ловит одиночные выбросы - решено 15 сентября 2026"). A revenue_price_gap
# episode that would otherwise span this boundary must be split into two -
# one epsiode computed under each formula - so a human labeling "сработало /
# не сработало" is never judging a single episode that was actually two
# different formulas' worth of triggers glued together. Kept as one named
# constant, not repeated as a literal, since it's referenced both when
# deciding contiguity below and (already) in observations.md/BACKLOG.md.
REVENUE_PRICE_GAP_FORMULA_CHANGE_DATE = date_cls(2026, 9, 15)


def collapse_into_episodes(
    entries: list[tuple[T, bool | None]],
    is_contiguous: Callable[[T, T], bool],
) -> list[tuple[T, T]]:
    """Group a chronologically ordered sequence of (position, fired) entries
    into episodes: maximal runs of POSITIONALLY CONTIGUOUS entries where
    `fired` is True.

    Generalizes scripts/replay_signals.py's two independent episode-counting
    functions into one shared primitive:
      - `_collapse_daily_episodes` (revenue_price_gap): position = calendar
        date, is_contiguous = "next day is exactly one calendar day later".
      - `_collapse_episodes` (oi_divergence): position = grid index,
        is_contiguous = "next index is exactly one more" (always true for a
        plain enumerated list - the ONLY thing that breaks a run there is a
        False/None flag, never a position gap, since the grid itself is
        already fixed-step).

    A run breaks on EITHER a non-contiguous position (per `is_contiguous`)
    OR a False/None `fired` flag - same two break conditions
    `_collapse_daily_episodes` originally implemented directly.

    Args:
        entries: (position, fired) pairs in ascending chronological order.
            `fired` is True (fired), False (evaluated, did not fire), or
            None (not evaluated / insufficient data at that position).
        is_contiguous: given the previous entry's position and the current
            one, returns whether they are adjacent with no gap. Called only
            between consecutive entries in `entries`, never across a larger
            span.

    Returns:
        List of (start_position, end_position) per episode, oldest first.
        A single-entry episode has start_position == end_position.
    """
    episodes: list[list[T]] = []
    in_episode = False
    prev_pos: T | None = None
    for pos, flag in entries:
        contiguous = prev_pos is not None and is_contiguous(prev_pos, pos)
        if not contiguous:
            in_episode = False
        if flag:
            if not in_episode:
                episodes.append([pos, pos])
            else:
                episodes[-1][1] = pos
            in_episode = True
        else:
            in_episode = False
        prev_pos = pos
    return [(start, end) for start, end in episodes]


def _is_contiguous_day(signal_name: str, prev_day: date_cls, day: date_cls) -> bool:
    """Calendar-day contiguity check for get_open_episodes/
    get_reviewable_episodes below: consecutive calendar days, EXCEPT for
    revenue_price_gap, where the boundary at
    REVENUE_PRICE_GAP_FORMULA_CHANGE_DATE always breaks a run even between
    two otherwise-adjacent days (see that constant's docstring).
    """
    if day != prev_day + timedelta(days=1):
        return False
    if signal_name == "revenue_price_gap":
        cutoff = REVENUE_PRICE_GAP_FORMULA_CHANGE_DATE
        if prev_day < cutoff <= day:
            return False
    return True


@dataclass
class Episode:
    signal_name: str
    protocol_slug: str
    episode_start_date: str  # "YYYY-MM-DD"
    episode_end_date: str  # "YYYY-MM-DD"
    duration_days: int
    is_open: bool
    first_metric_value: float | None
    first_details_json: str | None
    last_metric_value: float | None
    last_details_json: str | None
    last_triggered_at: str


def _build_all_episodes(
    conn: sqlite3.Connection, since_by_signal: dict[str, str]
) -> list[Episode]:
    """Every episode found in signal_events, across every signal_name and
    protocol_slug ever recorded - no filtering by age or by whether it has
    already been labeled (get_open_episodes/get_reviewable_episodes below
    apply those on top).

    Args:
        conn: open storage/db.py connection.
        since_by_signal: {signal_name: since_iso}, the freshness cursor from
            scoring/ranker.py's default_since_by_signal() - same source of
            truth rank_candidates() itself uses to decide whether a
            signal_events row is still confirmed.
    """
    rows = conn.execute(
        """
        SELECT signal_name, protocol_slug, triggered_at, metric_value, details_json
        FROM signal_events
        ORDER BY signal_name ASC, protocol_slug ASC, datetime(triggered_at) ASC
        """
    ).fetchall()

    episodes: list[Episode] = []
    if not rows:
        return episodes

    # Group by (signal_name, protocol_slug) - rows are already sorted that
    # way by the query above, so a single linear pass suffices.
    group_key: tuple[str, str] | None = None
    group_rows: list[sqlite3.Row] = []

    def _flush(key: tuple[str, str], group: list[sqlite3.Row]) -> None:
        signal_name, protocol_slug = key
        by_day: dict[date_cls, list[sqlite3.Row]] = {}
        for row in group:
            day = datetime.fromisoformat(row["triggered_at"]).date()
            by_day.setdefault(day, []).append(row)
        days_sorted = sorted(by_day)
        entries = [(day, True) for day in days_sorted]
        day_episodes = collapse_into_episodes(
            entries, partial(_is_contiguous_day, signal_name)
        )
        cursor = since_by_signal.get(signal_name)
        for start_day, end_day in day_episodes:
            first_row = by_day[start_day][0]
            last_row = by_day[end_day][-1]
            last_triggered_at = last_row["triggered_at"]
            is_open = cursor is not None and last_triggered_at >= cursor
            episodes.append(
                Episode(
                    signal_name=signal_name,
                    protocol_slug=protocol_slug,
                    episode_start_date=start_day.isoformat(),
                    episode_end_date=end_day.isoformat(),
                    duration_days=(end_day - start_day).days + 1,
                    is_open=is_open,
                    first_metric_value=first_row["metric_value"],
                    first_details_json=first_row["details_json"],
                    last_metric_value=last_row["metric_value"],
                    last_details_json=last_row["details_json"],
                    last_triggered_at=last_triggered_at,
                )
            )

    for row in rows:
        key = (row["signal_name"], row["protocol_slug"])
        if group_key is not None and key != group_key:
            _flush(group_key, group_rows)
            group_rows = []
        group_key = key
        group_rows.append(row)
    if group_key is not None:
        _flush(group_key, group_rows)

    return episodes


def _labeled_keys(conn: sqlite3.Connection) -> dict[tuple[str, str, str], sqlite3.Row]:
    """{(signal_name, protocol_slug, episode_start_date): episode_outcomes row}
    for every episode already labeled at least once.
    """
    rows = conn.execute("SELECT * FROM episode_outcomes").fetchall()
    return {
        (row["signal_name"], row["protocol_slug"], row["episode_start_date"]): row
        for row in rows
    }


def get_open_episodes(
    conn: sqlite3.Connection, config: dict, min_age_days: int | None = None
) -> list[Episode]:
    """Episodes at least `min_age_days` old that have never been labeled -
    the candidate list for the manual-labeling web screen's main queue.

    Args:
        conn: open storage/db.py connection.
        config: parsed config.yaml - passed through to
            scoring/ranker.py's default_since_by_signal() for the
            freshness cursor used to compute each episode's `is_open`, and
            (when `min_age_days` is not given explicitly) the source of the
            config.yaml's episode_review.min_age_days default.
        min_age_days: only episodes whose episode_start_date is at least
            this many days in the past are returned - a very recent episode
            hasn't had time to show whether it "worked" yet. Overrides
            config.yaml's episode_review.min_age_days when given; falls back
            to that config value (or 7 if config.yaml has no episode_review
            section at all) when None - same pattern as
            get_reviewable_episodes's reopen_after_days below.

    Returns:
        Episodes sorted newest-episode-start-first, excluding any that
        already have a row in episode_outcomes (already labeled - see
        get_reviewable_episodes for those instead).
    """
    if min_age_days is None:
        min_age_days = config.get("episode_review", {}).get("min_age_days", 7)

    since_by_signal = default_since_by_signal(conn, config)
    all_episodes = _build_all_episodes(conn, since_by_signal)
    labeled = _labeled_keys(conn)

    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=min_age_days)

    result = [
        ep
        for ep in all_episodes
        if date_cls.fromisoformat(ep.episode_start_date) <= cutoff
        and (ep.signal_name, ep.protocol_slug, ep.episode_start_date) not in labeled
    ]
    result.sort(key=lambda ep: ep.episode_start_date, reverse=True)
    return result


@dataclass
class MarketComparison:
    coin_change_pct: float  # episode's coin: price at episode start -> latest known price
    market_change_pct: float  # MEDIAN of the same change over the other watchlist coins
    coins_used: int  # how many other coins the median was computed over


def _price_change_pct(start_price: float | None, now_price: float | None) -> float | None:
    """(now - start) / start * 100, or None if either price is missing or
    start is not positive."""
    if start_price is None or now_price is None or start_price <= 0:
        return None
    return (now_price - start_price) / start_price * 100


def get_market_comparison(
    conn: sqlite3.Connection,
    config: dict,
    signal_name: str,
    protocol_slug: str,
    episode_start_date: str,
) -> MarketComparison | None:
    """Compare a coin's price change since episode start with the median
    change of the rest of the watchlist over the same period (so the
    labeler can tell "the signal found the coin" from "the whole market
    rose"). Only reads already-collected prices; computes no signal.

    Args:
        conn: open storage/db.py connection.
        config: parsed config.yaml - source of the watchlist universe
            (defillama_protocols for revenue_price_gap / volume_breakout,
            binance_futures_symbols for oi_divergence).
        signal_name: which signal the episode belongs to.
        protocol_slug: DeFiLlama slug, or Binance ticker for oi_divergence.
        episode_start_date: "YYYY-MM-DD".

    Returns:
        MarketComparison, or None if signal_name is unknown, the coin itself
        has no start/latest price, or no other coin could be computed.
        Other coins lacking a price are skipped silently (see coins_used).
    """
    watchlist = config.get("watchlist", {})
    if signal_name in ("revenue_price_gap", "volume_breakout"):
        universe = watchlist.get("defillama_protocols", [])

        def change(slug: str) -> float | None:
            start = get_coingecko_price_before(conn, slug, episode_start_date)
            now = get_latest_coingecko_price_history_by_slug(conn, slug)
            if start is None or now is None:
                return None
            return _price_change_pct(start["price"], now["price"])

    elif signal_name == "oi_divergence":
        universe = watchlist.get("binance_futures_symbols", [])
        start_iso = f"{episode_start_date}T00:00:00+00:00"

        def change(slug: str) -> float | None:
            start = get_binance_oi_snapshot_hours_before(conn, slug, start_iso, 0)
            now = get_latest_binance_oi_snapshot(conn, slug)
            if start is None or now is None:
                return None
            return _price_change_pct(start["price"], now["price"])

    else:
        return None

    coin_change = change(protocol_slug)
    if coin_change is None:
        return None
    others = [c for c in (change(s) for s in universe if s != protocol_slug) if c is not None]
    if not others:
        return None
    return MarketComparison(coin_change, statistics.median(others), len(others))


@dataclass
class ReviewableEpisode:
    episode: Episode
    prior_outcome: str
    prior_outcome_comment: str | None
    prior_outcome_at: str


def _recheck_due_date(
    episode_start_date: str, outcome_at: datetime, horizons: list[int]
) -> date_cls:
    """Date on which an episode labeled "рано судить" at `outcome_at` should
    come back: episode start + the smallest horizon greater than the
    episode's age (in days, UTC) at labeling time; if the label was made at
    or after the last horizon, `outcome_at` date + the last horizon.
    """
    start = date_cls.fromisoformat(episode_start_date)
    labeled_day = outcome_at.astimezone(timezone.utc).date()
    age = (labeled_day - start).days
    later = [h for h in sorted(horizons) if h > age]
    if later:
        return start + timedelta(days=later[0])
    return labeled_day + timedelta(days=max(horizons))


def get_reviewable_episodes(
    conn: sqlite3.Connection, config: dict, reopen_after_days: int | None = None
) -> list[ReviewableEpisode]:
    """Already-labeled episodes worth a second look. Two kinds; an episode
    appears at most once per call, and every re-label restarts its clock
    (save_episode_outcome overwrites outcome_at), so an episode that is
    still open and keeps being re-labeled comes back every
    `reopen_after_days`:
      1. labeled while still open, still open now, and at least
         `reopen_after_days` have passed since that label (the earlier call
         might no longer hold);
      2. labeled "рано судить" (open OR closed, regardless of
         labeled_when_open) whose recheck date has arrived - see
         _recheck_due_date. The screen can tell them apart by
         `prior_outcome == "рано судить"`.

    Args:
        conn: open storage/db.py connection.
        config: parsed config.yaml - source of the reopen_after_days
            default (config.yaml's episode_review.reopen_after_days) when
            the caller doesn't pass one explicitly, of
            episode_review.recheck_horizon_days, and passed through to
            default_since_by_signal() for the freshness cursor.
        reopen_after_days: overrides config.yaml's
            episode_review.reopen_after_days when given; falls back to that
            config value (or 14 if config.yaml has no episode_review
            section at all) when None.

    Returns:
        ReviewableEpisode entries (the episode itself, plus the previously
        recorded outcome/comment/timestamp), sorted by the earlier
        outcome_at ascending (longest-overdue first).
    """
    if reopen_after_days is None:
        reopen_after_days = config.get("episode_review", {}).get("reopen_after_days", 14)

    since_by_signal = default_since_by_signal(conn, config)
    all_episodes = _build_all_episodes(conn, since_by_signal)
    labeled = _labeled_keys(conn)

    horizons = config.get("episode_review", {}).get("recheck_horizon_days", [7, 14, 30])

    now = datetime.now(timezone.utc)
    reopen_cutoff = now - timedelta(days=reopen_after_days)

    result: list[ReviewableEpisode] = []
    for ep in all_episodes:
        key = (ep.signal_name, ep.protocol_slug, ep.episode_start_date)
        prior = labeled.get(key)
        if prior is None:
            continue
        outcome_at = datetime.fromisoformat(prior["outcome_at"])
        reopen_due = (
            prior["labeled_when_open"]
            and ep.is_open
            and outcome_at <= reopen_cutoff
        )
        recheck_due = (
            prior["outcome"] == "рано судить"
            and now.date() >= _recheck_due_date(ep.episode_start_date, outcome_at, horizons)
        )
        if not (reopen_due or recheck_due):
            continue
        result.append(
            ReviewableEpisode(
                episode=ep,
                prior_outcome=prior["outcome"],
                prior_outcome_comment=prior["outcome_comment"],
                prior_outcome_at=prior["outcome_at"],
            )
        )

    result.sort(key=lambda r: r.prior_outcome_at)
    return result
