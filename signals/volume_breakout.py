"""Signal 4.7: price breaks above a multi-month resistance level on volume.

Condition (TZ section 4.7): price breaks above a multi-month resistance
level - taken here as the highest daily price over the trailing
`resistance_lookback_days` days, EXCLUDING today - while today's volume is
at least `volume_ratio_threshold` times the average daily volume over the
trailing `volume_avg_lookback_days` days (30, per the TZ), also EXCLUDING
today.

"Excluding today" on both windows is deliberate, not an oversight: if
today's own price/volume were folded into the resistance level and the
volume average, an extreme move could partially absorb itself into its own
comparison baseline (a huge volume spike would inflate the very average
it's being measured against; an all-time-high price would define its own
"resistance"). Both baselines have to be built strictly from the days
BEFORE today for the comparison to mean anything.

Uses the daily price/volume history collectors/coingecko.py already fetches
and storage/db.py already stores (coingecko_price_history) - this module
only computes the comparison, it never talks to the network itself.
"""
from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field

from storage.db import get_coingecko_price_history_before, get_latest_coingecko_price_history

# Fraction of the requested lookback window that must actually be present in
# stored history before a resistance level / volume average is trusted as
# meaningful, rather than as an artifact of a short history (e.g. right
# after scripts/backfill_history.py's first run for a protocol, or a
# newly added watchlist protocol that hasn't accumulated enough daily
# collector runs yet). CoinGecko's daily series can also have the odd
# missing day (see collectors/coingecko.py) - this is a coverage floor, not
# a requirement that every single calendar day be present.
MIN_HISTORY_COVERAGE_FRACTION = 0.9


@dataclass
class VolumeBreakoutSignal:
    protocol_slug: str
    gecko_id: str
    date: str
    price_now: float
    resistance_level: float
    resistance_lookback_days: int
    # Number of trailing days of stored history actually used to compute
    # resistance_level - may be less than resistance_lookback_days if
    # storage has occasional gaps, but never less than this module's
    # coverage floor (see MIN_HISTORY_COVERAGE_FRACTION) - reported so the
    # resistance level's reliability is transparent, not just its value.
    resistance_window_days_available: int
    volume_now: float
    volume_avg: float
    volume_avg_lookback_days: int
    volume_window_days_available: int
    volume_ratio: float


@dataclass
class VolumeBreakoutScanResult:
    """Result of scanning the whole watchlist - keeps "not enough data yet"
    separate from "evaluated, but below threshold" so main.py can log an
    accurate reason instead of a single ambiguous "no candidates" line.
    Same shape as signals/revenue_price_gap.py's RevenueGapScanResult and
    signals/oi_divergence.py's OiDivergenceScanResult.
    """
    signals: list[VolumeBreakoutSignal] = field(default_factory=list)
    # Protocol slugs skipped because there's no known gecko_id, no latest
    # price/volume row yet, or not enough trailing history to trust a
    # resistance level / volume average (see MIN_HISTORY_COVERAGE_FRACTION).
    insufficient_history: list[str] = field(default_factory=list)
    # Protocol slugs skipped because the trailing volume_avg is below
    # config.yaml's min_volume_avg_usd - kept separate from
    # insufficient_history: there IS enough history here, it just describes
    # a coin too thin to trust a "breakout on volume" reading from (TZ
    # section 9's low-liquidity noise risk).
    insufficient_liquidity: list[str] = field(default_factory=list)


def _valid_prices(rows: list[sqlite3.Row]) -> list[float]:
    """Non-NULL, positive prices from a set of stored rows - the same
    filter both the coverage check in scan_watchlist and _evaluate below
    must apply, kept in one place so they can never drift apart (see
    scan_watchlist's docstring note on why coverage has to be checked
    AFTER this filter, not on the raw row count).
    """
    return [r["price"] for r in rows if r["price"] is not None and r["price"] > 0]


def _valid_volumes(rows: list[sqlite3.Row]) -> list[float]:
    """Non-NULL, non-negative volumes from a set of stored rows - see
    _valid_prices, same reasoning for volume instead of price. CoinGecko is
    documented (collectors/coingecko.py) to sometimes not report a volume
    at all for a low-liquidity coin on a given day, which is exactly the
    gap this filters out.
    """
    return [r["volume"] for r in rows if r["volume"] is not None and r["volume"] >= 0]


def _evaluate(
    latest: sqlite3.Row,
    resistance_prices: list[float],
    volume_values: list[float],
    resistance_lookback_days: int,
    volume_avg_lookback_days: int,
    volume_ratio_threshold: float,
) -> VolumeBreakoutSignal | None:
    """Compare one coin's latest price/volume point against its own trailing
    resistance level and volume average.

    Args:
        latest: this coin's most recent coingecko_price_history row.
        resistance_prices: already-filtered (see _valid_prices) trailing
            prices - scan_watchlist has already checked there are enough of
            these to trust a resistance level from (see
            MIN_HISTORY_COVERAGE_FRACTION), so this is never empty here.
        volume_values: already-filtered (see _valid_volumes) trailing
            volumes - same coverage guarantee as resistance_prices.
        resistance_lookback_days: config.yaml
            signals.volume_breakout.resistance_lookback_days, carried
            through only for the returned signal's transparency fields.
        volume_avg_lookback_days: same, for volume_avg_lookback_days.
        volume_ratio_threshold: config.yaml
            signals.volume_breakout.volume_ratio_threshold.

    Returns:
        A VolumeBreakoutSignal if both conditions hold, otherwise None -
        including when today's own price/volume is zero/missing, which
        would make the ratio math divide by zero.
    """
    price_now = latest["price"]
    volume_now = latest["volume"]
    if not price_now or not volume_now:
        return None

    resistance_level = max(resistance_prices)
    volume_avg = sum(volume_values) / len(volume_values)
    if not volume_avg:
        return None

    if price_now <= resistance_level:
        return None

    volume_ratio = volume_now / volume_avg
    if volume_ratio < volume_ratio_threshold:
        return None

    return VolumeBreakoutSignal(
        protocol_slug=latest["protocol_slug"],
        gecko_id=latest["gecko_id"],
        date=latest["date"],
        price_now=price_now,
        resistance_level=resistance_level,
        resistance_lookback_days=resistance_lookback_days,
        resistance_window_days_available=len(resistance_prices),
        volume_now=volume_now,
        volume_avg=volume_avg,
        volume_avg_lookback_days=volume_avg_lookback_days,
        volume_window_days_available=len(volume_values),
        volume_ratio=volume_ratio,
    )


def scan_watchlist(
    conn: sqlite3.Connection,
    slug_to_gecko_id: dict[str, str | None],
    resistance_lookback_days: int,
    volume_avg_lookback_days: int,
    volume_ratio_threshold: float,
    min_volume_avg_usd: float = 0,
) -> VolumeBreakoutScanResult:
    """Run the signal for every watchlist protocol using stored CoinGecko
    price/volume history.

    Args:
        conn: open storage/db.py connection.
        slug_to_gecko_id: DeFiLlama protocol slug -> CoinGecko coin id
            (config.yaml watchlist.defillama_protocols, resolved via
            collectors/defillama.py's collect()). A slug mapped to
            None/"" is treated as insufficient history (no coin to look up
            price/volume for).
        resistance_lookback_days: config.yaml
            signals.volume_breakout.resistance_lookback_days.
        volume_avg_lookback_days: config.yaml
            signals.volume_breakout.volume_avg_lookback_days (30 per the TZ).
        volume_ratio_threshold: config.yaml
            signals.volume_breakout.volume_ratio_threshold.
        min_volume_avg_usd: minimum trailing average daily volume in USD
            (config.yaml signals.volume_breakout.min_volume_avg_usd) a
            protocol's volume_avg must clear before it's even evaluated -
            see VolumeBreakoutScanResult.insufficient_liquidity. Defaults to
            0 (no filtering) so existing callers that don't pass it keep
            working unchanged.

    Returns:
        A VolumeBreakoutScanResult: `.insufficient_history` covers missing
        gecko_id/price-volume row, or not enough VALID (non-NULL, see
        _valid_prices/_valid_volumes) trailing history - counted on
        already-filtered values, not raw row count, since CoinGecko can
        omit volume on some days while the row itself still exists.
        `.insufficient_liquidity` covers volume_avg below `min_volume_avg_usd`.
    """
    signals: list[VolumeBreakoutSignal] = []
    insufficient_history: list[str] = []
    insufficient_liquidity: list[str] = []

    min_resistance_values = math.ceil(resistance_lookback_days * MIN_HISTORY_COVERAGE_FRACTION)
    min_volume_values = math.ceil(volume_avg_lookback_days * MIN_HISTORY_COVERAGE_FRACTION)

    for slug, gecko_id in slug_to_gecko_id.items():
        if not gecko_id:
            insufficient_history.append(slug)
            continue

        latest = get_latest_coingecko_price_history(conn, gecko_id)
        if latest is None or latest["price"] is None or latest["volume"] is None:
            insufficient_history.append(slug)
            continue

        resistance_rows = get_coingecko_price_history_before(
            conn, gecko_id, latest["date"], resistance_lookback_days
        )
        resistance_prices = _valid_prices(resistance_rows)
        if len(resistance_prices) < min_resistance_values:
            insufficient_history.append(slug)
            continue

        volume_rows = get_coingecko_price_history_before(
            conn, gecko_id, latest["date"], volume_avg_lookback_days
        )
        volume_values = _valid_volumes(volume_rows)
        if len(volume_values) < min_volume_values:
            insufficient_history.append(slug)
            continue

        # sum()/len() here, not a second implementation of _evaluate's ratio
        # math - just the average needed to gate on min_volume_avg_usd BEFORE
        # calling _evaluate, so a too-thin coin lands in the distinguishable
        # insufficient_liquidity bucket instead of a plain "didn't fire".
        # volume_values is already the same coverage-checked list _evaluate
        # itself would use to compute the identical average.
        volume_avg = sum(volume_values) / len(volume_values)
        if volume_avg < min_volume_avg_usd:
            insufficient_liquidity.append(slug)
            continue

        signal = _evaluate(
            latest, resistance_prices, volume_values,
            resistance_lookback_days, volume_avg_lookback_days, volume_ratio_threshold,
        )
        if signal is not None:
            signals.append(signal)

    return VolumeBreakoutScanResult(
        signals=signals,
        insufficient_history=insufficient_history,
        insufficient_liquidity=insufficient_liquidity,
    )
