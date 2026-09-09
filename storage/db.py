"""SQLite storage: collector snapshots and triggered signal history.

Snapshots accumulate over time so signals that need historical comparison
(e.g. revenue_price_gap, which needs market cap N days ago) can be computed
from our own history, since not every free API exposes historical data.
binance_oi_snapshots follows the same pattern for collectors/binance_futures.py
(TZ 4.1), even though that source already returns 24-48h of history per call -
storing every point still builds a durable local history for backtesting
(TZ section 6/7) instead of only ever seeing whatever window Binance serves.
coingecko_price_history follows the same pattern again for
collectors/coingecko.py (TZ 4.7's volume_breakout signal): CoinGecko's free
`market_chart` endpoint does return up to a year of history per call, but the
signal still needs a stable, growing local copy to compare "today" against a
multi-month trailing window without re-fetching and re-deriving that window
on every single scan.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "db.sqlite"

SCHEMA = """
CREATE TABLE IF NOT EXISTS defillama_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    protocol_slug TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    category TEXT,
    fetched_at TEXT NOT NULL,
    revenue_total_7d REAL,
    revenue_total_30d REAL,
    revenue_change_7d_pct REAL,
    revenue_change_30d_pct REAL,
    mcap REAL
);

CREATE INDEX IF NOT EXISTS idx_defillama_snapshots_slug_time
    ON defillama_snapshots (protocol_slug, fetched_at);

-- Added for scripts/backfill_history.py: that script can write one row per
-- (protocol, historical calendar date) and must be safely re-runnable after
-- a partial failure (e.g. CoinGecko rate-limiting mid-run) without piling
-- up duplicate rows for a date it already wrote. A UNIQUE INDEX (rather
-- than a table-level UNIQUE constraint, which SQLite can't add to an
-- existing table without a full rebuild) is enough for INSERT OR IGNORE in
-- save_defillama_snapshots() below to enforce this on both old and new
-- databases. Does not change the live daily collector's behaviour: each of
-- its runs still writes exactly one row, with a fetched_at that includes
-- the full current time, so a same-second collision with a prior run is
-- not expected in practice.
CREATE UNIQUE INDEX IF NOT EXISTS idx_defillama_snapshots_slug_fetched_unique
    ON defillama_snapshots (protocol_slug, fetched_at);

CREATE TABLE IF NOT EXISTS binance_oi_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    oi_timestamp TEXT NOT NULL,
    oi REAL NOT NULL,
    oi_value_usdt REAL,
    price REAL NOT NULL,
    UNIQUE(symbol, oi_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_binance_oi_snapshots_symbol_time
    ON binance_oi_snapshots (symbol, oi_timestamp);

-- Daily price/volume history from collectors/coingecko.py, for TZ 4.7's
-- volume_breakout signal (and, later, TZ 4.6's sector_rotation - see that
-- module's docstring). Keyed by `gecko_id` (a CoinGecko coin id), NOT by
-- `protocol_slug` (a DeFiLlama protocol id) the way defillama_snapshots is:
-- CoinGecko's data is inherently per-COIN, not per-protocol-version, and a
-- future caller like sector_rotation will want to look up a coin's price
-- history directly by gecko_id without going through a DeFiLlama protocol
-- at all. `protocol_slug` is still stored as a convenience/denormalized
-- column (same idea as defillama_snapshots.symbol/name) so
-- signals/volume_breakout.py can log and report against the watchlist
-- name callers actually recognize, without a second lookup - it is NOT
-- part of the unique key and is allowed to be NULL for a future caller
-- that saves a gecko_id's history independent of any DeFiLlama protocol.
CREATE TABLE IF NOT EXISTS coingecko_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gecko_id TEXT NOT NULL,
    protocol_slug TEXT,
    date TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    price REAL,
    volume REAL,
    UNIQUE(gecko_id, date)
);

CREATE INDEX IF NOT EXISTS idx_coingecko_price_history_gecko_date
    ON coingecko_price_history (gecko_id, date);

CREATE TABLE IF NOT EXISTS signal_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name TEXT NOT NULL,
    protocol_slug TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    source TEXT NOT NULL,
    metric_value REAL,
    details_json TEXT,
    outcome TEXT
);
"""


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Create/upgrade the schema.

    Raises:
        sqlite3.Error: if applying SCHEMA fails - most plausibly the new
            `idx_defillama_snapshots_slug_fetched_unique` unique index (see
            SCHEMA above) failing because a pre-existing database already
            has two rows with the same (protocol_slug, fetched_at), which
            is not expected in practice (see that index's comment) but
            would need a manual dedupe to fix. Left to propagate rather
            than caught and ignored, since silently skipping a schema
            change would leave save_defillama_snapshots()'s INSERT OR
            IGNORE not actually enforcing uniqueness, and nothing in the
            logs would say so.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    except sqlite3.Error:
        logging.getLogger(__name__).exception(
            "Failed to apply storage/db.py SCHEMA to %s - if this is "
            "'UNIQUE constraint failed' on idx_defillama_snapshots_slug_fetched_unique, "
            "the database already has duplicate (protocol_slug, fetched_at) rows that "
            "need a manual dedupe before this index (and therefore idempotent re-runs "
            "of scripts/backfill_history.py) can be enabled.",
            db_path,
        )
        raise
    finally:
        conn.close()


def save_defillama_snapshots(conn: sqlite3.Connection, records: list[dict]) -> None:
    """Persist revenue/mcap snapshots from collectors/defillama.py, or
    historical rows built by scripts/backfill_history.py.

    OR IGNORE (backed by idx_defillama_snapshots_slug_fetched_unique above)
    makes re-running scripts/backfill_history.py after a partial failure
    idempotent - a (protocol_slug, fetched_at) pair already saved is
    skipped instead of duplicated. Does not change the live daily
    collector's behaviour in practice: it always writes a fresh
    `fetched_at` (current time, not a rounded historical date), so its rows
    essentially never collide with each other or with backfilled ones.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO defillama_snapshots (
            protocol_slug, symbol, name, category, fetched_at,
            revenue_total_7d, revenue_total_30d,
            revenue_change_7d_pct, revenue_change_30d_pct, mcap
        ) VALUES (
            :slug, :symbol, :name, :category, :fetched_at,
            :revenue_total_7d, :revenue_total_30d,
            :revenue_change_7d_pct, :revenue_change_30d_pct, :mcap
        )
        """,
        records,
    )
    conn.commit()


def get_latest_snapshot(conn: sqlite3.Connection, protocol_slug: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM defillama_snapshots
        WHERE protocol_slug = ?
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (protocol_slug,),
    ).fetchone()


def get_snapshot_days_before(
    conn: sqlite3.Connection, protocol_slug: str, reference_iso: str, days: int
) -> sqlite3.Row | None:
    """Closest snapshot at or before (reference_iso - days), for growth-% math.

    fetched_at is stored as Python's `datetime.isoformat()`
    ("2026-09-09T07:00:00+00:00"), which is NOT the same string format
    SQLite's own `datetime(...)` function produces ("2026-09-09 07:00:00").
    Comparing the raw column directly against `datetime(?, ?)` compares two
    different formats and is wrong (a space sorts before "T", so ISO
    timestamps can compare as "later" than they really are). Both sides of
    the comparison must go through `datetime(...)` to normalize to the same
    format first.
    """
    return conn.execute(
        """
        SELECT * FROM defillama_snapshots
        WHERE protocol_slug = ?
          AND datetime(fetched_at) <= datetime(?, ?)
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (protocol_slug, reference_iso, f"-{days} days"),
    ).fetchone()


def save_binance_oi_snapshots(conn: sqlite3.Connection, records: list[dict]) -> None:
    """Persist Open-Interest/price points from collectors/binance_futures.py.

    Each collector run re-fetches the whole lookback window, so the same
    (symbol, oi_timestamp) point is seen on multiple runs - INSERT OR IGNORE
    (backed by the UNIQUE constraint on that pair) keeps re-polling
    idempotent instead of piling up duplicate rows.

    Known, accepted limitation: if Binance ever revises a historical OI
    point after the fact (e.g. a late correction), OR IGNORE means our
    already-stored (older) value silently wins and the correction is
    dropped - executemany() also doesn't report how many rows were
    inserted vs ignored, so that would happen without any signal in the
    logs. Not treated as a bug: Binance's OI history endpoint has not been
    observed to revise past points, and the failure mode is "slightly
    stale historical data", not a wrong/missing OI reading right now. If
    this ever needs fixing, switch to INSERT OR REPLACE (or an explicit
    UPSERT) instead of OR IGNORE.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO binance_oi_snapshots (
            symbol, fetched_at, oi_timestamp, oi, oi_value_usdt, price
        ) VALUES (
            :symbol, :fetched_at, :oi_timestamp, :oi, :oi_value_usdt, :price
        )
        """,
        records,
    )
    conn.commit()


def get_latest_binance_oi_snapshot(conn: sqlite3.Connection, symbol: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM binance_oi_snapshots
        WHERE symbol = ?
        ORDER BY oi_timestamp DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()


def get_binance_oi_snapshot_hours_before(
    conn: sqlite3.Connection, symbol: str, reference_iso: str, hours: int
) -> sqlite3.Row | None:
    """Closest stored point at or before (reference_iso - hours), for the
    OI-growth / price-change math in signals/oi_divergence.py (TZ 4.1).

    Both sides of the comparison go through `datetime(...)` - see the
    matching note on get_snapshot_days_before, same bug (raw ISO string
    with "T" and offset vs SQLite's own datetime() output with a space and
    no offset compare incorrectly if only one side is normalized).
    """
    return conn.execute(
        """
        SELECT * FROM binance_oi_snapshots
        WHERE symbol = ?
          AND datetime(oi_timestamp) <= datetime(?, ?)
        ORDER BY oi_timestamp DESC
        LIMIT 1
        """,
        (symbol, reference_iso, f"-{hours} hours"),
    ).fetchone()


def save_coingecko_price_history(conn: sqlite3.Connection, records: list[dict]) -> None:
    """Persist daily price/volume points from
    collectors/coingecko.py's collect_price_volume_history(), or historical
    rows built by scripts/backfill_history.py.

    INSERT OR IGNORE (backed by the UNIQUE(gecko_id, date) constraint on
    coingecko_price_history) makes this idempotent for both callers: the
    live daily cycle re-fetches the whole ~365-day CoinGecko window on every
    run (see collect_price_volume_history's docstring for why re-fetching
    that much is intentional, not a bug), and a re-run of the backfill
    script after a partial failure must not duplicate rows for a date it
    already wrote - both just no-op on a (gecko_id, date) pair already
    saved instead of erroring or duplicating.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO coingecko_price_history (
            gecko_id, protocol_slug, date, fetched_at, price, volume
        ) VALUES (
            :gecko_id, :protocol_slug, :date, :fetched_at, :price, :volume
        )
        """,
        records,
    )
    conn.commit()


def get_latest_coingecko_price_history(conn: sqlite3.Connection, gecko_id: str) -> sqlite3.Row | None:
    """Most recent stored price/volume row for one CoinGecko coin -
    signals/volume_breakout.py treats this as "today" for that coin (the
    latest calendar date actually collected, not necessarily the real
    wall-clock today, in case a run was missed).
    """
    return conn.execute(
        """
        SELECT * FROM coingecko_price_history
        WHERE gecko_id = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (gecko_id,),
    ).fetchone()


def get_coingecko_price_history_before(
    conn: sqlite3.Connection, gecko_id: str, before_date: str, days: int
) -> list[sqlite3.Row]:
    """Stored rows for `gecko_id` strictly BEFORE `before_date`, going back
    `days` calendar days - i.e. dates in [before_date - days, before_date -
    1], oldest-to-newest. Used by signals/volume_breakout.py to build a
    resistance level / average volume from a trailing window that
    deliberately EXCLUDES the day being evaluated (`before_date` itself) -
    see that module's docstring for why today's own extreme volume/price
    must not get folded into its own comparison baseline.

    Unlike get_snapshot_days_before / get_binance_oi_snapshot_hours_before
    above, this does NOT need both sides of the comparison wrapped in
    SQLite's datetime(...) function: `date` here is stored as a plain
    "YYYY-MM-DD" string with no time-of-day or UTC-offset component (unlike
    fetched_at/oi_timestamp elsewhere in this file, which are full
    `datetime.isoformat()` strings), so plain string comparison already
    sorts identically to calendar-date order - there is no "T"/offset
    mismatch to normalize away here. `date(?, ?)` (SQLite's date function,
    not datetime()) is still used for the lower bound, since it correctly
    understands the "-N days" modifier against a plain date string.
    """
    return conn.execute(
        """
        SELECT * FROM coingecko_price_history
        WHERE gecko_id = ?
          AND date < ?
          AND date >= date(?, ?)
        ORDER BY date ASC
        """,
        (gecko_id, before_date, before_date, f"-{days} days"),
    ).fetchall()


def save_signal_event(
    conn: sqlite3.Connection,
    signal_name: str,
    protocol_slug: str,
    triggered_at: str,
    source: str,
    metric_value: float,
    details_json: str,
) -> None:
    conn.execute(
        """
        INSERT INTO signal_events (
            signal_name, protocol_slug, triggered_at, source, metric_value, details_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (signal_name, protocol_slug, triggered_at, source, metric_value, details_json),
    )
    conn.commit()
