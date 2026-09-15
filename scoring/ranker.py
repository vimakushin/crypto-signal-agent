"""Aggregates signal_events (storage/db.py) into one score per candidate
(TZ section 6: scoring/ combines signals; MVP.md checklist item 4).

Deliberately dumb on purpose: this module reads storage/db.py's
signal_events table and config.yaml's signal weights, and does nothing else
- no network calls, no re-deriving thresholds, no new math beyond summing
weights. Every number it uses was already computed and logged by
signals/*.py (via main.py's save_signal_event() calls) at the moment a
signal fired - ranker.py's only job is to combine those already-decided
per-signal results into a single ranked list, so the user can see, per
candidate, WHICH signals fired and how much each contributed (MVP.md's
"по каждому кандидату видно, какие сигналы сработали... и сколько дал
каждый").

This module only ranks/aggregates already-collected data for a human to
review manually - it does not decide or recommend trades (TZ section 1/9).
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Same sys.path fix as scripts/backfill_history.py and
# scripts/replay_signals.py - this module lives one level below the project
# root, so `from storage.db import ...` below would fail with
# ModuleNotFoundError without it when this file is run directly
# (`python scoring/ranker.py`, not imported as a package) - Python only puts
# the script's OWN directory on sys.path in that case, not the project root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storage.db import (  # noqa: E402
    get_latest_binance_oi_fetch_time,
    get_latest_coingecko_fetch_time,
    get_latest_defillama_fetch_time,
    get_signal_events_since,
)

logger = logging.getLogger(__name__)

# Same allowance for normal run-to-run jitter as main.py's
# STALENESS_MULTIPLIER and signals/revenue_price_gap.py's
# MAX_GAP_MULTIPLIER - reused here as-is, not recalibrated: a signal's
# "recent enough to still count" window is 1.5x its expected polling
# interval, not exactly 1x (which would drop a candidate just because
# this run happened to land a little early relative to the last one).
STALENESS_MULTIPLIER = 1.5

# How far back rank_candidates() looks for CANDIDATE signal_events rows,
# before default_since_by_signal()'s per-signal cursor decides whether each
# one is still confirmed fresh (see rank_candidates' docstring). Wider than
# any single signal's STALENESS_MULTIPLIER allowance (the widest today is
# DeFiLlama's ~36h at defillama_poll_hours=24) so that a row this window
# excludes was never going to be considered "still fresh" even under the old
# age-based logic - this is a discovery window for logging what got dropped
# and why, not a second chance for genuinely old history to sneak back in.
DISCOVERY_WINDOW_HOURS = 72


@dataclass
class SignalContribution:
    signal_name: str
    weight: float
    metric_value: float | None
    source: str
    triggered_at: str
    details: dict


@dataclass
class CandidateScore:
    protocol_slug: str
    total_score: float
    contributions: list[SignalContribution] = field(default_factory=list)


def rank_candidates(
    conn: sqlite3.Connection,
    signal_weights: dict[str, float],
    since_by_signal: dict[str, str],
) -> list[CandidateScore]:
    """Combine recent signal_events rows into one ranked score per candidate.

    Args:
        conn: open storage/db.py connection.
        signal_weights: {signal_name: weight}, e.g. config.yaml's
            signals.<name>.weight for every enabled signal. A candidate's
            score for a signal is simply that signal's weight ("this signal
            fired" = its full weight) - no extra formula on top.
        since_by_signal: {signal_name: since_iso} - the per-signal cursor
            produced by default_since_by_signal(), i.e. "the most recent
            actual collector run for this signal's data source, if it's not
            itself stale". A signal_name present in signal_weights but
            missing here means that signal's whole collection pipeline is
            stale or has never run - every row for it is excluded (and
            logged) below.

    Returns:
        One CandidateScore per protocol_slug/ticker that had at least one
        matching signal_events row CONFIRMED by a real collector run at or
        after its signal's cursor, sorted by total_score descending. If the
        SAME signal fired multiple times for the same candidate within the
        window, only its single freshest occurrence counts toward the score
        (a sustained/continuing state, not repeated separate events) - but
        every OTHER distinct signal that also fired for that candidate still
        contributes its own weight on top. A signal_events row whose
        triggered_at predates its signal's cursor - i.e. a later collector
        run happened but did NOT reconfirm this trigger, for example because
        an outlier guard rejected it on the later run - is excluded and
        logged, not silently carried forward with stale numbers.
    """
    # protocol_slug -> {signal_name: SignalContribution}
    by_candidate: dict[str, dict[str, SignalContribution]] = {}

    from datetime import datetime, timedelta, timezone

    discovery_since = (
        datetime.now(timezone.utc) - timedelta(hours=DISCOVERY_WINDOW_HOURS)
    ).isoformat()

    for signal_name, weight in signal_weights.items():
        rows = get_signal_events_since(conn, signal_name, discovery_since)
        for row in rows:
            protocol_slug = row["protocol_slug"]
            # Deliberately NOT by_candidate.setdefault() here - only look up
            # what already exists, so that a candidate whose every row for
            # this signal turns out stale (see cursor check below) never
            # gets an empty-but-present entry in by_candidate, which would
            # otherwise surface as a ghost "score: 0.00" candidate with no
            # contributions in the ranked output.
            if signal_name in by_candidate.get(protocol_slug, {}):
                # Rows are DESC by triggered_at, so the first row seen per
                # (candidate, signal_name) is already the freshest - a
                # repeat trigger of the SAME signal is a continuing state,
                # not a new event, and must not be double-counted.
                continue

            cursor = since_by_signal.get(signal_name)
            if cursor is None or row["triggered_at"] < cursor:
                logger.info(
                    "rank_candidates: excluding stale signal_events row - "
                    "signal=%s protocol=%s last fired at %s, but %s - this "
                    "trigger was not reconfirmed by the most recent actual "
                    "collector run and will not appear in the ranked output",
                    signal_name,
                    protocol_slug,
                    row["triggered_at"],
                    (
                        "this signal's whole collection pipeline is stale "
                        "or has never run (no cursor)"
                        if cursor is None
                        else f"the most recent actual check was at {cursor}"
                    ),
                )
                continue

            candidate_signals = by_candidate.setdefault(protocol_slug, {})
            details: dict = {}
            details_json = row["details_json"]
            if details_json:
                try:
                    details = json.loads(details_json)
                except (TypeError, ValueError):
                    logger.warning(
                        "Could not parse details_json for signal_events row id=%s "
                        "(signal_name=%s, protocol_slug=%s) - keeping the score, "
                        "dropping only the details for this row",
                        row["id"], signal_name, protocol_slug,
                    )

            candidate_signals[signal_name] = SignalContribution(
                signal_name=signal_name,
                weight=weight,
                metric_value=row["metric_value"],
                source=row["source"],
                triggered_at=row["triggered_at"],
                details=details,
            )

    scores = [
        CandidateScore(
            protocol_slug=protocol_slug,
            total_score=sum(c.weight for c in candidate_signals.values()),
            contributions=sorted(
                candidate_signals.values(), key=lambda c: c.triggered_at, reverse=True
            ),
        )
        for protocol_slug, candidate_signals in by_candidate.items()
    ]
    scores.sort(key=lambda cs: cs.total_score, reverse=True)
    return scores


def signal_weights_from_config(config: dict) -> dict[str, float]:
    """Build the {signal_name: weight} map rank_candidates() expects, from
    config.yaml's `signals:` section - every enabled signal contributes its
    own weight (config.yaml signals.<name>.weight, defaulting to 1.0 if
    unset), a disabled signal is skipped entirely.

    Args:
        config: parsed config.yaml (see load_config()).

    Returns:
        {signal_name: weight} for every signal with `enabled: true` (or no
        `enabled` key at all, which defaults to enabled).
    """
    return {
        name: cfg.get("weight", 1.0)
        for name, cfg in config["signals"].items()
        if cfg.get("enabled", True)
    }


def default_since_by_signal(conn: sqlite3.Connection, config: dict) -> dict[str, str]:
    """Build the {signal_name: since_iso} map rank_candidates() expects.

    Unlike the old age-of-the-row approach, this is NOT "now minus some
    multiple of the polling interval" - a signal_events row's own age proved
    unusable as a freshness test (a row that a LATER collector run actually
    rejected, e.g. via an outlier guard, still looks "recent" by that
    measure right up until a full staleness window has passed, so a rejected
    trigger kept showing up in the ranked output with stale numbers). Instead
    each signal gets a cursor from the most recent ACTUAL collector run for
    its data source (storage/db.py's get_latest_*_fetch_time functions, the
    same ones main.py's data-freshness diagnostics already use) -
    rank_candidates() then only counts a signal_events row if it was
    reconfirmed at or after that cursor.

    Each signal is mapped to the data source its numbers actually come from:
    - revenue_price_gap: DeFiLlama (get_latest_defillama_fetch_time).
    - volume_breakout: CoinGecko (get_latest_coingecko_fetch_time) - NOT
      DeFiLlama, even though both run on the same schedule.
      volume_breakout's numbers come from CoinGecko, and if CoinGecko alone
      had an outage on a day DeFiLlama still ran fine, a DeFiLlama-based
      cursor would wrongly "re-confirm" a volume_breakout row that was never
      actually re-checked.
    - oi_divergence: Binance Futures (get_latest_binance_oi_fetch_time).

    STALENESS_MULTIPLIER still applies, but to a different question than
    before: not "how old is this row", but "how old is the latest fetch
    timestamp for this signal's whole collection pipeline" - if even the
    latest fetch is older than STALENESS_MULTIPLIER times the expected
    polling interval, the entire pipeline is considered stale and the signal
    is dropped from the result (same as if it had never run).

    Args:
        conn: open storage/db.py connection.
        config: parsed config.yaml (see load_config()).

    Returns:
        {signal_name: since_iso} for revenue_price_gap, volume_breakout and
        oi_divergence - the only three signal names this function knows the
        data source and expected polling interval for. A signal_name is
        absent from the result if its data source has never been fetched
        (get_latest_*_fetch_time returned None) or its latest fetch is
        itself stale - both cases rank_candidates() already treats as "no
        cursor, exclude every row for this signal".
    """
    from datetime import datetime, timedelta, timezone

    schedule_cfg = config.get("schedule", {})
    defillama_interval = schedule_cfg.get("defillama_poll_hours", 24)
    binance_interval = schedule_cfg.get("binance_futures_poll_hours", 6)

    # signal_name -> (latest fetch time getter, expected polling interval)
    source_by_signal = {
        "revenue_price_gap": (get_latest_defillama_fetch_time, defillama_interval),
        "volume_breakout": (get_latest_coingecko_fetch_time, defillama_interval),
        "oi_divergence": (get_latest_binance_oi_fetch_time, binance_interval),
    }

    now = datetime.now(timezone.utc)
    since_by_signal: dict[str, str] = {}
    for signal_name, (get_latest_fetch_time, interval_hours) in source_by_signal.items():
        latest_fetch = get_latest_fetch_time(conn)
        if latest_fetch is None:
            # This signal's data source has never been fetched at all -
            # nothing to confirm any row against, skip the signal entirely.
            continue

        fetch_age = now - datetime.fromisoformat(latest_fetch)
        if fetch_age > timedelta(hours=interval_hours * STALENESS_MULTIPLIER):
            # The collector itself hasn't run recently enough - the whole
            # pipeline is stale, not just one row, so there's nothing fresh
            # to confirm any signal_events row against.
            continue

        since_by_signal[signal_name] = latest_fetch

    return since_by_signal


if __name__ == "__main__":
    import yaml

    # sys.path already has the project root on it (see the module-level
    # sys.path.insert above), so this is a plain import, not a deferred one.
    from storage.db import get_connection

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

    def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> dict:
        """Load config.yaml (duplicated from main.py/replay_signals.py's own
        load_config, not imported, for the same reason those scripts
        duplicate it: importing main.py would run its module-level logging
        setup as an import side effect).
        """
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    config = load_config()
    db_path = Path(config["storage"]["sqlite_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path

    signal_weights = signal_weights_from_config(config)

    conn = get_connection(db_path)
    try:
        since_by_signal = default_since_by_signal(conn, config)
        # default_since_by_signal() returns an entry for every signal it
        # knows the data source and polling interval for AND whose pipeline
        # isn't stale, regardless of whether that signal is enabled -
        # rank_candidates() only needs the ones actually present in
        # signal_weights, but passing the extra entries through is harmless
        # (rank_candidates() only ever looks up since_by_signal by the names
        # already in signal_weights).
        results = rank_candidates(conn, signal_weights, since_by_signal)
    finally:
        conn.close()

    print("=== scoring/ranker.py: ranked candidates ===")
    if not results:
        print("No candidates found - no signal_events rows within the lookback window for any enabled signal.")
    else:
        for candidate in results:
            print(f"\n{candidate.protocol_slug}  (score: {candidate.total_score:.2f})")
            for contribution in candidate.contributions:
                print(
                    f"  - {contribution.signal_name} (weight {contribution.weight:.2f}, "
                    f"source {contribution.source}): metric_value={contribution.metric_value}, "
                    f"triggered_at={contribution.triggered_at}"
                )
                if contribution.details:
                    print(f"    details: {contribution.details}")
