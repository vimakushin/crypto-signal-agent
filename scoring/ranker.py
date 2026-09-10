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

from storage.db import get_signal_events_since  # noqa: E402

logger = logging.getLogger(__name__)

# Same allowance for normal run-to-run jitter as main.py's
# STALENESS_MULTIPLIER and signals/revenue_price_gap.py's
# MAX_GAP_MULTIPLIER - reused here as-is, not recalibrated: a signal's
# "recent enough to still count" window is 1.5x its expected polling
# interval, not exactly 1x (which would drop a candidate just because
# this run happened to land a little early relative to the last one).
STALENESS_MULTIPLIER = 1.5


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
        since_by_signal: {signal_name: since_iso} - how far back to look for
            each signal. A signal_name present in signal_weights but missing
            here is skipped entirely (no window to look back over).

    Returns:
        One CandidateScore per protocol_slug/ticker that had at least one
        matching signal_events row, sorted by total_score descending. If the
        SAME signal fired multiple times for the same candidate within the
        window, only its single freshest occurrence counts toward the score
        (a sustained/continuing state, not repeated separate events) - but
        every OTHER distinct signal that also fired for that candidate still
        contributes its own weight on top.
    """
    # protocol_slug -> {signal_name: SignalContribution}
    by_candidate: dict[str, dict[str, SignalContribution]] = {}

    for signal_name, weight in signal_weights.items():
        since_iso = since_by_signal.get(signal_name)
        if since_iso is None:
            continue

        rows = get_signal_events_since(conn, signal_name, since_iso)
        for row in rows:
            protocol_slug = row["protocol_slug"]
            candidate_signals = by_candidate.setdefault(protocol_slug, {})
            if signal_name in candidate_signals:
                # Rows are DESC by triggered_at, so the first row seen per
                # (candidate, signal_name) is already the freshest - a
                # repeat trigger of the SAME signal is a continuing state,
                # not a new event, and must not be double-counted.
                continue

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


def default_since_by_signal(config: dict) -> dict[str, str]:
    """Build the {signal_name: since_iso} map rank_candidates() expects, from
    config.yaml's `schedule:` section - "recent enough to still count" for
    each signal is STALENESS_MULTIPLIER times that signal's expected polling
    interval (defillama_poll_hours for the two DeFiLlama-fed signals,
    binance_futures_poll_hours for oi_divergence), not exactly one interval
    (see STALENESS_MULTIPLIER).

    Args:
        config: parsed config.yaml (see load_config()).

    Returns:
        {signal_name: since_iso} for revenue_price_gap, volume_breakout and
        oi_divergence - the only three signal names this function knows the
        expected polling interval for. A signal_name not in this mapping
        (e.g. a future signal type not yet wired up here) is simply absent
        from the result, which rank_candidates() already treats as "no
        window to look back over, skip it".
    """
    from datetime import datetime, timedelta, timezone

    schedule_cfg = config.get("schedule", {})
    defillama_interval = schedule_cfg.get("defillama_poll_hours", 24)
    binance_interval = schedule_cfg.get("binance_futures_poll_hours", 6)
    expected_interval_hours_by_signal = {
        "revenue_price_gap": defillama_interval,
        "volume_breakout": defillama_interval,
        "oi_divergence": binance_interval,
    }

    now = datetime.now(timezone.utc)
    return {
        name: (now - timedelta(hours=hours * STALENESS_MULTIPLIER)).isoformat()
        for name, hours in expected_interval_hours_by_signal.items()
    }


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
    since_by_signal = default_since_by_signal(config)
    # default_since_by_signal() returns an entry for every signal it knows
    # the polling interval for, regardless of whether that signal is
    # enabled - rank_candidates() only needs the ones actually present in
    # signal_weights, but passing the extra entries through is harmless
    # (rank_candidates() only ever looks up since_by_signal by the names
    # already in signal_weights).

    conn = get_connection(db_path)
    try:
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
