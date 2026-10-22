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
from datetime import date, datetime
from pathlib import Path
from typing import NamedTuple

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "db.sqlite"

# How long a process waits for another process's lock on the database before
# failing with "database is locked" (sqlite3's default is 5 s). On 2 Oct 2026
# the PC was switched on late, Task Scheduler (StartWhenAvailable) launched
# the backup, the DeFiLlama collection and the digest at the same moment, and
# the collection died with "database is locked" because the backup was
# copying the file. Spreading the tasks out in time doesn't help: after a
# late start the scheduler runs ALL missed tasks at once, whatever times they
# are set to. So the writer waits instead. Measured in logs/backup.log: a
# backup takes 2-19 s normally and took 95 s that very morning (Google Drive,
# three jobs competing); 300 s is ~3x that worst case, and well under the
# tasks' own 1-hour limit. WAL mode is deliberately NOT used: it adds -wal/-shm
# files next to the database, which backup and the restore instructions
# don't expect.
SQLITE_BUSY_TIMEOUT_SECONDS = 300

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

-- Daily revenue history, one row per (protocol, calendar date), from
-- collectors/defillama.py's fetch_protocol_daily_revenue_history() (relayed
-- by collectors/defillama.py's new collect_daily_revenue() for the live
-- daily cycle, and by scripts/backfill_history.py's
-- backfill_defillama_protocol() for the full historical backfill).
-- Exists because signals/revenue_price_gap.py was found (see BACKLOG.md's
-- "revenue_price_gap ловит одиночные выбросы" entry) to be misled by
-- DeFiLlama's own pre-computed weekly SUM: one unusually large single day
-- can dominate a whole week's total and make a flat trend look like a
-- genuine spike. The fix compares the MEDIAN daily revenue over a recent
-- window against the median over a longer baseline window instead - a
-- statistic that needs the individual daily values, not just a weekly
-- rollup, hence this table. `revenue_usd` can be NULL: DeFiLlama's own
-- totalDataChart occasionally carries a malformed/non-numeric point, which
-- fetch_protocol_daily_revenue_history() already guards against by DROPPING
-- that one date rather than saving a fabricated value for it (see that
-- function's docstring) - so in practice a NULL here would only come from a
-- future caller that stores a placeholder for a known-missing day; callers
-- reading this table must still treat NULL as "no data for this day", the
-- same convention scripts/backfill_history.py's _sum_window already uses
-- for a day absent from revenue_by_date entirely.
CREATE TABLE IF NOT EXISTS defillama_daily_revenue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    protocol_slug TEXT NOT NULL,
    date TEXT NOT NULL,
    revenue_usd REAL,
    fetched_at TEXT NOT NULL,
    UNIQUE(protocol_slug, date)
);

CREATE INDEX IF NOT EXISTS idx_defillama_daily_revenue_slug_date
    ON defillama_daily_revenue (protocol_slug, date);

-- Что DeFiLlama отдаёт СЕЙЧАС по дневной выручке (см. BACKLOG.md, пункт
-- "DeFiLlama пересматривает дневную выручку задним числом"). Таблица выше
-- хранит первое полученное значение навсегда ("что получили тогда", его и
-- читает сигнал); эта - полный набор значений каждого перезабора
-- (scripts/refetch_daily_revenue.py) со своим fetched_at, чтобы перезаборы
-- можно было сравнивать между собой и со старыми значениями. Сигнал эту
-- таблицу НЕ читает.
CREATE TABLE IF NOT EXISTS defillama_daily_revenue_restated (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    protocol_slug TEXT NOT NULL,
    date TEXT NOT NULL,
    revenue_usd REAL,
    fetched_at TEXT NOT NULL,
    UNIQUE(protocol_slug, date, fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_defillama_daily_revenue_restated_slug_date
    ON defillama_daily_revenue_restated (protocol_slug, date);

-- One row per calendar date for which the Telegram digest was actually sent:
-- the digest task is retried every 2 hours (see config.yaml schedule), and
-- this mark is what makes the later runs exit without sending a second one.
-- stale=1 means it went out with the "data are stale" warning; then one more
-- digest (at most two per date) is allowed once the data are fresh, and the
-- row is updated to stale=0.
CREATE TABLE IF NOT EXISTS digest_sends (
    digest_date TEXT PRIMARY KEY,
    sent_at TEXT NOT NULL,
    stale INTEGER NOT NULL DEFAULT 0
);

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

-- Manual "did this hold up" labeling for one EPISODE (a signal firing on the
-- same protocol on consecutive days is one episode, not N separate
-- observations - see storage/episodes.py, built for the new manual-labeling
-- web screen, TZ section 6). One row per (signal_name, protocol_slug,
-- episode_start_date) - re-labeling the same episode overwrites this same
-- row (see save_episode_outcome's INSERT ... ON CONFLICT below), it does not
-- accumulate a second opinion.
CREATE TABLE IF NOT EXISTS episode_outcomes (
    signal_name TEXT NOT NULL,
    protocol_slug TEXT NOT NULL,
    episode_start_date TEXT NOT NULL,
    outcome TEXT NOT NULL,
    outcome_at TEXT NOT NULL,
    outcome_comment TEXT,
    labeled_when_open INTEGER NOT NULL,
    PRIMARY KEY (signal_name, protocol_slug, episode_start_date)
);
"""


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
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


def save_defillama_daily_revenue(conn: sqlite3.Connection, records: list[dict]) -> None:
    """Persist daily revenue points from
    collectors/defillama.py's collect_daily_revenue(), or historical rows
    built by scripts/backfill_history.py.

    INSERT OR IGNORE (backed by the UNIQUE(protocol_slug, date) constraint
    on defillama_daily_revenue) - same idempotency pattern as
    save_coingecko_price_history above: the live daily cycle re-fetches a
    trailing window that overlaps what it already saved on previous runs,
    and a re-run of the backfill script after a partial failure must not
    duplicate rows for a date it already wrote - both just no-op on a
    (protocol_slug, date) pair already saved.

    Known, accepted limitation: same trade-off as
    save_binance_oi_snapshots's own docstring below, applied to DeFiLlama
    instead of Binance. If DeFiLlama later revises the revenue figure for a
    date we already stored (on-chain indexers commonly do this as they
    backfill or reconcile a day after it closes), OR IGNORE means our
    already-stored (older) value silently wins and the revision is dropped
    - executemany() also doesn't report how many rows were inserted vs.
    ignored, so that would happen without any signal in the logs. Not
    treated as a bug: the failure mode is "slightly stale historical
    revenue", not a wrong/missing reading right now. If this ever needs
    fixing, switch to INSERT OR REPLACE (or an explicit UPSERT) instead of
    OR IGNORE - not done here, since REPLACE risks the opposite problem:
    overwriting a more complete backfilled value with a less complete one
    from a live re-fetch.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO defillama_daily_revenue (
            protocol_slug, date, revenue_usd, fetched_at
        ) VALUES (
            :protocol_slug, :date, :revenue_usd, :fetched_at
        )
        """,
        records,
    )
    conn.commit()


def save_defillama_daily_revenue_restated(conn: sqlite3.Connection, records: list[dict]) -> None:
    """Append one re-fetch's daily revenue values to
    defillama_daily_revenue_restated (scripts/refetch_daily_revenue.py).

    Args:
        records: dicts with protocol_slug, date, revenue_usd and fetched_at
            (the same fetched_at for the whole run). INSERT OR IGNORE on
            UNIQUE(protocol_slug, date, fetched_at), so writing the same run
            twice adds nothing; a different fetched_at adds a new full set.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO defillama_daily_revenue_restated (
            protocol_slug, date, revenue_usd, fetched_at
        ) VALUES (
            :protocol_slug, :date, :revenue_usd, :fetched_at
        )
        """,
        records,
    )
    conn.commit()


def get_defillama_daily_revenue_window(
    conn: sqlite3.Connection, protocol_slug: str, end_date: str, days: int
) -> list[sqlite3.Row]:
    """Trailing `days`-day window of stored daily revenue for one protocol,
    ending on `end_date` INCLUSIVE.

    Unlike get_coingecko_price_history_before above (which EXCLUDES its
    `before_date`, so "today" never leaks into its own comparison baseline),
    this deliberately includes `end_date` itself: it is the direct
    replacement for DeFiLlama's own revenue_total_7d - "the last 7 days,
    counting today" - not a lookback baseline that must exclude today. Both
    `end_date` and the stored `date` column are plain "YYYY-MM-DD" strings
    (like coingecko_price_history.date - see
    get_coingecko_price_history_before's docstring for why that means no
    datetime(...) wrap is needed for the equality/inequality side of this
    comparison), so `date(?, ?)` (SQLite's date function) is only needed for
    the lower bound's "-N days" arithmetic.

    With days=7 the (exclusive) lower bound is end_date-7, so the rows
    returned cover end_date-6 .. end_date inclusive - exactly 7 calendar
    dates when there are no gaps in stored history.

    Args:
        protocol_slug: DeFiLlama protocol slug.
        end_date: "YYYY-MM-DD", the most recent date in the window.
        days: window length in calendar days.

    Returns:
        Rows oldest-to-newest, at most `days` of them - fewer if history is
        short or has gaps (callers apply their own coverage floor, e.g.
        signals/revenue_price_gap.py's MIN_HISTORY_COVERAGE_FRACTION).
    """
    return conn.execute(
        """
        SELECT * FROM defillama_daily_revenue
        WHERE protocol_slug = ? AND date <= ? AND date > date(?, ?)
        ORDER BY date ASC
        """,
        (protocol_slug, end_date, end_date, f"-{days} days"),
    ).fetchall()


def get_defillama_daily_revenue_window_as_of(
    conn: sqlite3.Connection,
    protocol_slug: str,
    end_date: str,
    days: int,
    fetched_before: str | None,
) -> list[sqlite3.Row]:
    """Same window as get_defillama_daily_revenue_window, but only rows we had
    ALREADY RECEIVED before `fetched_before` (strictly earlier `fetched_at`).

    Only for scripts/replay_signals.py: a live run sees only values received
    up to that moment, while the table also holds values for the same dates
    received later (stored "first received" via INSERT OR IGNORE, so
    `fetched_at` is the first-receipt time). The live signal does not use this.

    Args:
        protocol_slug: DeFiLlama protocol slug.
        end_date: "YYYY-MM-DD", the most recent date in the window.
        days: window length in calendar days.
        fetched_before: ISO timestamp; None means no cutoff.

    Returns:
        Rows oldest-to-newest, at most `days` of them.
    """
    if fetched_before is None:
        return get_defillama_daily_revenue_window(conn, protocol_slug, end_date, days)
    return conn.execute(
        """
        SELECT * FROM defillama_daily_revenue
        WHERE protocol_slug = ? AND date <= ? AND date > date(?, ?) AND fetched_at < ?
        ORDER BY date ASC
        """,
        (protocol_slug, end_date, end_date, f"-{days} days", fetched_before),
    ).fetchall()


def get_defillama_daily_revenue_window_restated(
    conn: sqlite3.Connection,
    protocol_slug: str,
    end_date: str,
    days: int,
    fetched_at: str,
    visible_before: str | None = None,
) -> list[sqlite3.Row]:
    """Same window as get_defillama_daily_revenue_window (end_date inclusive,
    `days` calendar days back, oldest first), but read from
    defillama_daily_revenue_restated and only the rows of ONE re-fetch.

    Only for scripts/replay_signals.py's `--revenue-source restated`; the live
    signal never reads this table.

    Args:
        protocol_slug: DeFiLlama protocol slug.
        end_date: "YYYY-MM-DD", the most recent date in the window.
        days: window length in calendar days.
        fetched_at: the re-fetch to read (see get_latest_complete_restated_fetch).
        visible_before: ISO timestamp or None. If set, a restated row is
            returned only if defillama_daily_revenue has a row for the same
            (protocol, date) received before it - the values are the restated
            ones, but the set of visible dates is exactly what
            get_defillama_daily_revenue_window_as_of shows.

    Returns:
        Rows oldest-to-newest, at most `days` of them.
    """
    visible_clause = ""
    params: list = [protocol_slug, fetched_at, end_date, end_date, f"-{days} days"]
    if visible_before is not None:
        visible_clause = """
          AND EXISTS (
              SELECT 1 FROM defillama_daily_revenue d
              WHERE d.protocol_slug = r.protocol_slug AND d.date = r.date AND d.fetched_at < ?
          )"""
        params.append(visible_before)
    return conn.execute(
        f"""
        SELECT r.* FROM defillama_daily_revenue_restated r
        WHERE r.protocol_slug = ? AND r.fetched_at = ? AND r.date <= ? AND r.date > date(?, ?){visible_clause}
        ORDER BY r.date ASC
        """,
        params,
    ).fetchall()


def get_latest_complete_restated_fetch(
    conn: sqlite3.Connection, protocol_count: int
) -> tuple[str, int] | None:
    """Newest re-fetch of daily revenue that covers every watchlist protocol
    (BACKLOG.md: complete = COUNT(DISTINCT protocol_slug) equals the number
    of watchlist protocols; an interrupted run leaves fewer).

    Args:
        protocol_count: number of protocols in config.yaml's watchlist.

    Returns:
        (fetched_at, protocol count) of the newest complete re-fetch, or None
        if there is none.
    """
    row = conn.execute(
        """
        SELECT fetched_at, COUNT(DISTINCT protocol_slug) AS n
        FROM defillama_daily_revenue_restated
        GROUP BY fetched_at
        HAVING n = ?
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (protocol_count,),
    ).fetchone()
    return (row["fetched_at"], row["n"]) if row else None


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


class MarketChange(NamedTuple):
    """Result of get_market_change(). median_pct is None when the background
    could not be computed; `reason` then says why, in plain Russian."""
    median_pct: float | None
    n: int
    total: int
    reason: str | None


# Stored points are hourly since 8 Sep 2026, every 4 hours before that. A
# point within an hour of the requested moment is "the price at that moment"
# (at most half an hour off on the hourly grid); a wider tolerance would let
# a data gap quietly turn into a different time window than the coin's own.
MARKET_POINT_TOLERANCE_HOURS = 1


def get_market_change(
    conn: sqlite3.Connection, symbols: list[str], start_iso: str, end_iso: str
) -> MarketChange:
    """Median price change (%) of `symbols` between two moments, from
    binance_oi_snapshots - the "market background" shown next to a coin's own
    move in the digest.

    Median, not mean: one coin that flew (or crashed) by 80% must not shift
    the background. The window must be the coin's own window, otherwise the
    two numbers are not comparable.

    For every symbol the stored point nearest to each moment is taken, if it
    lies within MARKET_POINT_TOLERANCE_HOURS. A symbol missing either point
    is left out. If fewer than half of `symbols` remain, no number is given:
    a median of a handful of coins is not a market background.

    Args:
        conn: open connection.
        symbols: Binance Futures tickers (config.yaml watchlist).
        start_iso: ISO 8601 start of the window.
        end_iso: ISO 8601 end of the window.

    Returns:
        MarketChange; median_pct is None (with `reason`) if not computed.
    """
    total = len(symbols)
    changes: list[float] = []
    for symbol in symbols:
        prices = []
        for moment in (start_iso, end_iso):
            row = conn.execute(
                """
                SELECT price FROM binance_oi_snapshots
                WHERE symbol = ? AND price > 0
                  AND round(abs(julianday(oi_timestamp) - julianday(?)) * 24, 3) <= ?
                ORDER BY abs(julianday(oi_timestamp) - julianday(?))
                LIMIT 1
                """,
                (symbol, moment, MARKET_POINT_TOLERANCE_HOURS, moment),
            ).fetchone()
            prices.append(row["price"] if row else None)
        if prices[0] is not None and prices[1] is not None:
            changes.append((prices[1] - prices[0]) / prices[0] * 100)
    if len(changes) * 2 < total:
        return MarketChange(
            None, len(changes), total,
            f"в базе есть цены Binance на оба момента только по {len(changes)} из {total} монет, "
            "а для медианы нужна хотя бы половина",
        )
    changes.sort()
    mid = len(changes) // 2
    median = changes[mid] if len(changes) % 2 else (changes[mid - 1] + changes[mid]) / 2
    return MarketChange(median, len(changes), total, None)


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


def get_latest_coingecko_price_history_by_slug(
    conn: sqlite3.Connection, protocol_slug: str
) -> sqlite3.Row | None:
    """Most recent stored price/volume row for one DeFiLlama protocol slug -
    same lookup as get_latest_coingecko_price_history above, but keyed by
    `protocol_slug` instead of `gecko_id`. Needed for the episode-labeling
    web screen: signal_events.protocol_slug for revenue_price_gap and
    volume_breakout episodes is the DeFiLlama slug (e.g. "cowswap"), not the
    CoinGecko gecko_id, so the gecko_id-keyed function can't be used
    directly there. Relies on collect_price_volume_history
    (collectors/coingecko.py) having stamped protocol_slug onto every row
    via its DeFiLlama-slug-to-gecko_id mapping.
    """
    return conn.execute(
        """
        SELECT * FROM coingecko_price_history
        WHERE protocol_slug = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (protocol_slug,),
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


def get_coingecko_price_before(conn: sqlite3.Connection, protocol_slug: str, before_date: str) -> sqlite3.Row | None:
    """Closest coingecko_price_history row for `protocol_slug` at or before
    `before_date` (a plain YYYY-MM-DD string, like the `date` column itself -
    see get_coingecko_price_history_before's docstring for why no
    datetime(...) wrap is needed for this column specifically). Used by
    notifications/observations.py to look up a candidate's price on/near a
    given day for the observations journal - never look-ahead (AT OR
    BEFORE, not nearest overall).
    """
    return conn.execute(
        """
        SELECT * FROM coingecko_price_history
        WHERE protocol_slug = ? AND date <= ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (protocol_slug, before_date),
    ).fetchone()


def get_latest_defillama_fetch_time(conn: sqlite3.Connection) -> str | None:
    """Most recent `fetched_at` across ALL of defillama_snapshots, with no
    per-protocol filter - unlike get_latest_snapshot above.

    Used by main.py's data-freshness check, which asks "has this COLLECTOR
    run recently at all", not "what's the latest data for protocol X" - a
    single protocol having a recent row wouldn't tell you the collector as a
    whole is still running on schedule, and vice versa a stale MAX() across
    the whole table is exactly the "the scheduled task stopped starting"
    symptom that check exists to catch. `fetched_at` (when the collector
    call happened) rather than any data-derived timestamp is the right
    column for that question, not just the only one available.

    Returns:
        ISO 8601 string of the latest fetched_at, or None if the table is
        empty (collector has never run yet - not an error).
    """
    return conn.execute("SELECT MAX(fetched_at) FROM defillama_snapshots").fetchone()[0]


def get_latest_binance_oi_fetch_time(conn: sqlite3.Connection) -> str | None:
    """Most recent `fetched_at` across ALL of binance_oi_snapshots.

    Deliberately `fetched_at` (when this collector run wrote the row), NOT
    `oi_timestamp` (when Binance says the OI reading itself occurred) - see
    get_latest_defillama_fetch_time's docstring for why: the freshness
    question is "did the collector process actually run recently", which
    `oi_timestamp` doesn't answer on its own (Binance's OI history endpoint
    could in principle return timestamps for a window that ends before "now"
    even on a perfectly healthy run). `fetched_at` is stamped by our own
    collector at write time and is what actually moves - or stops moving -
    when the scheduled task silently fails to start.

    Returns:
        ISO 8601 string of the latest fetched_at, or None if the table is
        empty (collector has never run yet - not an error).
    """
    return conn.execute("SELECT MAX(fetched_at) FROM binance_oi_snapshots").fetchone()[0]


def get_latest_coingecko_fetch_time(conn: sqlite3.Connection) -> str | None:
    """Most recent `fetched_at` across ALL of coingecko_price_history.

    Deliberately `fetched_at` (when this collector run wrote the row), NOT
    `date` (the calendar date the price/volume point is FOR) - same
    reasoning as get_latest_binance_oi_fetch_time's docstring: `date` is a
    property of the data, not of when the collector last successfully ran,
    and CoinGecko can legitimately return a `date` that lags "today" for
    reasons unrelated to whether the collector itself is still being
    launched on schedule.

    Returns:
        ISO 8601 string of the latest fetched_at, or None if the table is
        empty (collector has never run yet - not an error).
    """
    return conn.execute("SELECT MAX(fetched_at) FROM coingecko_price_history").fetchone()[0]


def sources_without_fetch_today(
    conn: sqlite3.Connection, today: date | None = None
) -> dict[str, str | None]:
    """The project's single rule for "this source is stale": it has no
    successful collection on today's LOCAL calendar date. Used by the
    Telegram digest (warning + waiting) and by `main.py --skip-if-fresh`.

    Args:
        conn: open connection to the live database.
        today: the local date to compare with; defaults to date.today().
            The digest passes its own date (wait_for_fresh_data), so one run
            judges every check against the same day.

    Known limit of the rule: a source counts as fresh if it has AT LEAST ONE
    row from today, so a partial collection counts as a collection.

    Returns:
        {"defillama" | "coingecko" | "binance": last fetched_at ISO string
        (UTC) or None if the table is empty} for every source that has no
        fetch on `today`. Empty dict when all three are fresh.
    """
    today = today or date.today()
    latest = {
        "defillama": get_latest_defillama_fetch_time(conn),
        "coingecko": get_latest_coingecko_fetch_time(conn),
        "binance": get_latest_binance_oi_fetch_time(conn),
    }
    return {
        source: iso
        for source, iso in latest.items()
        if iso is None or datetime.fromisoformat(iso).astimezone().date() != today
    }


# Incident 2026-10-06..22: backup.backup_dir pointed at a missing drive, the
# daily backup failed for two weeks and nobody noticed - the old check in
# main.py logged only INFO for "no folder / no files", and warnings went to
# the log only. One rule for "backup is not OK" now lives here, used by
# main.py's freshness check (log) and the Telegram digest (owner sees it).
#
# scripts/backup_db.py runs once a day (config.yaml schedule.backup_run_time
# sets WHEN, not how often), so 36 h = 24 h * 1.5, the same allowance for
# run-to-run jitter as main.py's STALENESS_MULTIPLIER. Operational tolerance,
# not a signal threshold, hence a constant and not config.yaml.
BACKUP_MAX_AGE_HOURS = 36

# Matches scripts/backup_db.py's BACKUP_FILENAME_GLOB exactly - duplicated
# rather than imported, since importing scripts.backup_db would run that
# module's unconditional _setup_logging() call at import time.
_BACKUP_FILENAME_GLOB = "db_????????_??????.sqlite"


class BackupProblem(NamedTuple):
    """Why the backup is not OK. `reason` is a short plain-Russian phrase;
    `last_backup` is the newest backup's time (local, naive) or None when
    there is no folder / no backup at all."""

    reason: str
    last_backup: datetime | None


def backup_problem(config: dict) -> BackupProblem | None:
    """The project's single rule for "the DB backup is not OK".

    Resolves backup.backup_dir the same way scripts/backup_db.py's main()
    does (config value, else <project>/backups; relative -> from the project
    root). Logs a WARNING with the folder path when there is a problem.

    Args:
        config: parsed config.yaml.

    Returns:
        None if the newest db_YYYYMMDD_HHMMSS.sqlite is at most
        BACKUP_MAX_AGE_HOURS old; otherwise a BackupProblem (folder missing /
        no backup files in it / newest one too old, with its age).
    """
    project_root = Path(__file__).resolve().parent.parent
    backup_dir_str = (config.get("backup") or {}).get("backup_dir")
    backup_dir = Path(backup_dir_str) if backup_dir_str else project_root / "backups"
    if not backup_dir.is_absolute():
        backup_dir = project_root / backup_dir

    problem: BackupProblem | None = None
    if not backup_dir.is_dir():
        problem = BackupProblem("не найдена папка для копий", None)
    else:
        backups = sorted(backup_dir.glob(_BACKUP_FILENAME_GLOB))
        if not backups:
            problem = BackupProblem("в папке для копий нет ни одной копии", None)
        else:
            latest = datetime.fromtimestamp(backups[-1].stat().st_mtime)
            age_hours = (datetime.now() - latest).total_seconds() / 3600
            if age_hours > BACKUP_MAX_AGE_HOURS:
                problem = BackupProblem(
                    f"последней копии {age_hours:.0f} ч ({age_hours / 24:.1f} сут.)", latest
                )
    if problem:
        logging.getLogger(__name__).warning("DB backup is not OK (%s): %s", backup_dir, problem.reason)
    return problem


def get_digest_send_stale(conn: sqlite3.Connection, digest_date: date) -> bool | None:
    """State of the digest_sends mark for `digest_date`.

    Returns:
        None if no digest was sent for that date; True if it was sent on
        stale data (a fresh one may follow); False if it was sent and is final.
    """
    row = conn.execute(
        "SELECT stale FROM digest_sends WHERE digest_date = ?", (digest_date.isoformat(),)
    ).fetchone()
    return None if row is None else bool(row["stale"])


def record_digest_sent(
    conn: sqlite3.Connection, digest_date: date, sent_at: str, stale: bool
) -> None:
    """Remember that the digest for `digest_date` went out; a second call for
    the same date (the fresh digest after a stale one) updates the row.

    Args:
        conn: open connection to the live database.
        digest_date: the local date the digest is for.
        sent_at: ISO 8601 timestamp of the send.
        stale: True if it was built on stale data (a fresh one may follow).
    """
    conn.execute(
        "INSERT INTO digest_sends (digest_date, sent_at, stale) VALUES (?, ?, ?) "
        "ON CONFLICT(digest_date) DO UPDATE SET sent_at = excluded.sent_at, stale = excluded.stale",
        (digest_date.isoformat(), sent_at, int(stale)),
    )
    conn.commit()


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


def get_signal_events_since(
    conn: sqlite3.Connection, signal_name: str, since_iso: str
) -> list[sqlite3.Row]:
    """All signal_events rows for `signal_name` triggered at or after
    `since_iso`, newest first - used by scoring/ranker.py to aggregate
    recent signals into a candidate score.

    Same double-`datetime(...)`-wrap requirement as get_snapshot_days_before
    / get_binance_oi_snapshot_hours_before above: `triggered_at` is stored as
    Python's `datetime.isoformat()` ("2026-09-09T07:00:00+00:00"), not
    SQLite's own `datetime(...)` output format ("2026-09-09 07:00:00").
    Comparing the raw column against a raw `since_iso` string would compare
    two different formats and sort wrong - both sides must go through
    `datetime(...)` first to normalize to the same format. The `ORDER BY`
    wraps `triggered_at` the same way: `isoformat()` drops the microsecond
    part when it's exactly zero, so a handful of raw strings would be
    shorter than the rest and could sort out of chronological order under
    plain lexicographic comparison.
    """
    return conn.execute(
        """
        SELECT * FROM signal_events
        WHERE signal_name = ? AND datetime(triggered_at) >= datetime(?)
        ORDER BY datetime(triggered_at) DESC
        """,
        (signal_name, since_iso),
    ).fetchall()


VALID_EPISODE_OUTCOMES = ("сработало", "не сработало", "рано судить")


def save_episode_outcome(
    conn: sqlite3.Connection,
    signal_name: str,
    protocol_slug: str,
    episode_start_date: str,
    outcome: str,
    outcome_comment: str | None,
    labeled_when_open: bool,
    outcome_at: str,
) -> None:
    """Record (or overwrite) a manual outcome label for one episode - the
    web screen's "разметка результатов срабатываний" (see
    storage/episodes.py's get_open_episodes/get_reviewable_episodes, which
    read this table back).

    Re-labeling the same (signal_name, protocol_slug, episode_start_date)
    triple overwrites the existing row in place (INSERT ... ON CONFLICT ...
    DO UPDATE), rather than accumulating a second opinion - a deliberate
    re-review (e.g. via get_reviewable_episodes, once labeled_when_open=1
    and enough time has passed) replaces the earlier label outright.

    Args:
        conn: open storage/db.py connection.
        signal_name: e.g. "revenue_price_gap".
        protocol_slug: DeFiLlama protocol slug or Binance symbol, matching
            signal_events.protocol_slug for this episode.
        episode_start_date: "YYYY-MM-DD", the episode's first fired date.
        outcome: one of VALID_EPISODE_OUTCOMES - anything else raises.
        outcome_comment: optional free-text note, may be None.
        labeled_when_open: whether the episode was still open (still firing,
            per the same freshness cursor scoring/ranker.py's
            default_since_by_signal() uses) at the moment this label was
            recorded - lets get_reviewable_episodes() later find episodes
            that were labeled early and might be worth a second look.
        outcome_at: ISO 8601 timestamp of when this label was recorded -
            like `triggered_at`/`fetched_at` elsewhere in this file, the
            caller supplies it (typically
            `datetime.now(timezone.utc).isoformat()`) rather than this
            function generating it internally, so this writer stays
            deterministic/testable the same way the rest of this module's
            writers are.

    Raises:
        ValueError: if `outcome` is not one of VALID_EPISODE_OUTCOMES - kept
            as strict as the rest of the project's handling of malformed
            data (see e.g. signals/revenue_price_gap.py's
            _DataQualityRejected guards).
    """
    if outcome not in VALID_EPISODE_OUTCOMES:
        raise ValueError(
            f"save_episode_outcome: outcome={outcome!r} is not one of "
            f"{VALID_EPISODE_OUTCOMES}"
        )

    conn.execute(
        """
        INSERT INTO episode_outcomes (
            signal_name, protocol_slug, episode_start_date, outcome,
            outcome_at, outcome_comment, labeled_when_open
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (signal_name, protocol_slug, episode_start_date) DO UPDATE SET
            outcome = excluded.outcome,
            outcome_at = excluded.outcome_at,
            outcome_comment = excluded.outcome_comment,
            labeled_when_open = excluded.labeled_when_open
        """,
        (
            signal_name,
            protocol_slug,
            episode_start_date,
            outcome,
            outcome_at,
            outcome_comment,
            1 if labeled_when_open else 0,
        ),
    )
    conn.commit()
