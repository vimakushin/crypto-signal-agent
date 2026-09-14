"""Orchestrator entry point (TZ section 5). MVP scope so far: DeFiLlama
collector -> SQLite storage -> revenue_price_gap signal (TZ section 4.5),
DeFiLlama's own gecko_id + CoinGecko collector -> SQLite storage ->
volume_breakout signal (TZ section 4.7, same cycle/schedule as
revenue_price_gap - see _run_volume_breakout), and Binance Futures
collector -> SQLite storage -> oi_divergence signal (TZ section 4.1).

Run this on a schedule (Windows Task Scheduler, or a loop with `schedule`/
APScheduler later) - see TZ section 10 on hosting. TZ section 5 calls OI a
"fast" signal that wants polling every 4-6h, while revenue-gap is "daily" -
the two cycles run on SEPARATE Task Scheduler tasks (see
scripts/register_scheduled_task.ps1, which now registers both), each invoking this script with
`--cycle defillama` or `--cycle binance` respectively, at the schedule
configured in config.yaml (schedule.defillama_run_time /
schedule.binance_futures_poll_hours). `--cycle all` (the default, used for
manual runs) still runs both back to back for convenience - each wrapped in
its own try/except so one source being down (e.g. DeFiLlama unreachable)
never prevents the other from running.

Runs unattended via Task Scheduler have no console to watch, so logging goes
to both stdout (for manual runs) and a rotating file in logs/ (so nothing -
including a failed/unavailable data source - is lost silently, per the
project's logging rule). All paths (config.yaml, logs/, the sqlite file) are
resolved from this script's own location, not the process's current working
directory - Task Scheduler and manual runs from a different folder must not
silently point at the wrong files.

`--cycle defillama` and `--cycle binance` are launched by Task Scheduler as
two SEPARATE OS processes (see scripts/register_scheduled_task.ps1) that can
genuinely run at the same time - confirmed live: running both at once
corrupted logs/scheduler.log with an interleaved, torn line ("
26 symbols..." split off mid-word from a different log line). Python's
logging module only synchronizes writers within a single process;
`RotatingFileHandler` was never designed for two independent processes
writing (let alone rotating) the same file concurrently, so this is not
something to work around by tuning logging further - each single-cycle run
gets its OWN log file (scheduler_defillama.log / scheduler_binance.log)
instead. `--cycle all` (manual runs) still runs both cycles sequentially
inside ONE process, so there's no concurrent-writer risk there - it keeps
using the single scheduler.log as before.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import yaml

from collectors import binance_futures, coingecko, defillama
from signals import oi_divergence, revenue_price_gap, volume_breakout
from storage.db import (
    get_connection,
    get_latest_binance_oi_fetch_time,
    get_latest_coingecko_fetch_time,
    get_latest_defillama_fetch_time,
    init_db,
    save_binance_oi_snapshots,
    save_coingecko_price_history,
    save_defillama_daily_revenue,
    save_defillama_snapshots,
    save_signal_event,
)

PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

# Same allowance for normal run-to-run jitter as
# signals/revenue_price_gap.py's MAX_GAP_MULTIPLIER (that value's own
# comment explains the reasoning: a scheduled run can legitimately land a
# bit early/late without it meaning anything is actually broken) - reused
# here as-is, not recalibrated, for the same reason: a source is considered
# "stale" only once it's overdue by more than 1.5x its expected polling
# interval, not the moment it's a few minutes late.
STALENESS_MULTIPLIER = 1.5

# scripts/backup_db.py runs once a day (config.yaml schedule.backup_run_time
# sets WHEN, not how often) - there's no separate "backup interval" setting
# in config.yaml to read, so this is a plain constant, same category as
# STALENESS_MULTIPLIER above: an operational-diagnostics tolerance, not a
# calibrated signal threshold, so it does not belong in config.yaml either.
BACKUP_EXPECTED_INTERVAL_HOURS = 24

# Matches scripts/backup_db.py's BACKUP_FILENAME_GLOB exactly - duplicated
# rather than imported, since importing scripts.backup_db would run that
# module's unconditional _setup_logging() call at import time (it runs at
# module scope, not inside main()) and stomp on this script's own
# _setup_logging(log_filename) setup for the current --cycle. See
# scripts/backup_db.py's own module docstring for the full backup design.
_BACKUP_FILENAME_GLOB = "db_????????_??????.sqlite"


def _setup_logging(log_filename: str = "scheduler.log") -> None:
    """Configure logging to stdout and a rotating file in logs/.

    Task Scheduler runs have no console to see a crash on, so failures in
    this setup itself (no write permission, disk full, logs/ blocked by a
    same-named file, ...) must not raise and silently kill the run before
    a single line is logged. If the file handler can't be created, this
    prints a warning to stderr and falls back to console-only logging
    instead of failing.

    Args:
        log_filename: name of the file under logs/ to write to. Callers
            running a single cycle as its own OS process (--cycle defillama
            / --cycle binance, launched separately by Task Scheduler) MUST
            each pass a distinct filename - see the module docstring for
            why sharing one file between two concurrently-running processes
            corrupted it in practice. Only --cycle all (one process, both
            cycles run sequentially) is safe to leave on the default.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        LOG_DIR.mkdir(exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                LOG_DIR / log_filename,
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


# NOT called here at import time (unlike before --cycle existed): which log
# file to use depends on args.cycle, which isn't known until argument
# parsing runs under `if __name__ == "__main__"` below. `logger` itself is
# still safe to create now - getLogger() just returns/creates the object;
# it emits nothing until _setup_logging() attaches handlers to the root
# logger later, which happens before run_defillama_cycle/
# run_binance_futures_cycle are ever called.
logger = logging.getLogger("main")


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> dict:
    """Load config.yaml.

    Args:
        path: config file location. Defaults to config.yaml next to this
            script (not the current working directory), so the config is
            found regardless of where the process was launched from.

    Returns:
        Parsed config as a dict (TZ section 6.4: thresholds/weights/watchlist
        /schedule live here, not hardcoded in Python).
    """
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_defillama_cycle(config: dict) -> None:
    watchlist = config["watchlist"]["defillama_protocols"]
    db_path = Path(config["storage"]["sqlite_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    revenue_signal_cfg = config["signals"]["revenue_price_gap"]
    volume_signal_cfg = config["signals"]["volume_breakout"]

    init_db(db_path)

    logger.info("Fetching DeFiLlama data for %d protocols...", len(watchlist))
    records = defillama.collect(watchlist)
    logger.info("Collected %d snapshots (%d watchlist misses)", len(records), len(watchlist) - len(records))

    conn = get_connection(db_path)
    try:
        save_defillama_snapshots(conn, records)
        logger.info("Saved snapshots to %s", db_path)

        logger.info("Fetching DeFiLlama daily revenue history for %d protocols...", len(watchlist))
        daily_revenue_records = defillama.collect_daily_revenue(watchlist)
        save_defillama_daily_revenue(conn, daily_revenue_records)
        logger.info(
            "Collected and saved %d daily revenue point(s) across %d watchlist protocol(s)",
            len(daily_revenue_records), len({r["protocol_slug"] for r in daily_revenue_records}),
        )

        if revenue_signal_cfg.get("enabled", True):
            result = revenue_price_gap.scan_watchlist(
                conn,
                watchlist,
                revenue_growth_threshold_pct=revenue_signal_cfg["revenue_growth_threshold_pct"],
                mcap_reaction_threshold_pct=revenue_signal_cfg["mcap_reaction_threshold_pct"],
                lookback_days=revenue_signal_cfg["lookback_days"],
                baseline_window_days=revenue_signal_cfg["baseline_window_days"],
                min_revenue_total_usd=revenue_signal_cfg.get("min_revenue_total_usd", 0),
                outlier_max_share_pct=revenue_signal_cfg.get("outlier_max_share_pct", 100),
            )

            # Four distinct reasons for "no candidates" - conflating them
            # into one line misleads the user either way: "needs more
            # history" reads as a data problem, "too small to trust" reads
            # as a data problem too but a DIFFERENT one, "bad data on a
            # protocol we DID have enough history for" (gap too large, or a
            # revenue/mcap value that's missing or non-positive - see
            # signals/revenue_price_gap.py's _DataQualityRejected) is yet a
            # THIRD kind of data problem and, left unlogged, could silently
            # zero out every candidate on a day a collector outage widens
            # the snapshot gap - and any of the three obscures "the real
            # market simply didn't cross the threshold".
            evaluated = (
                len(watchlist) - len(result.insufficient_history)
                - len(result.insufficient_liquidity) - len(result.insufficient_data_quality)
            )
            logger.info(
                "revenue_price_gap: %d/%d protocols evaluated (%d still need >= %s days of "
                "collected history to compare market cap, %d skipped for revenue below the "
                "$%s liquidity floor, %d skipped for bad data - snapshot gap too large or "
                "missing/non-positive revenue or market cap on the compared snapshots), "
                "%d candidate(s) found",
                evaluated, len(watchlist), len(result.insufficient_history),
                revenue_signal_cfg["lookback_days"], len(result.insufficient_liquidity),
                revenue_signal_cfg.get("min_revenue_total_usd", 0),
                len(result.insufficient_data_quality), len(result.signals),
            )
            evaluated_at = datetime.now(timezone.utc).isoformat()
            for s in result.signals:
                logger.info(
                    "revenue_price_gap FIRED: %s (%s) median daily revenue +%.1f%% over %dd "
                    "(configured) - recent $%.0f/day vs baseline $%.0f/day, week sum $%.0f "
                    "(largest day %.1f%% of it), actual gap between compared snapshots "
                    "%.1fd, mcap only +%.1f%%",
                    s.protocol_slug, s.symbol, s.revenue_growth_pct, s.lookback_days,
                    s.recent_median_daily_revenue, s.baseline_median_daily_revenue,
                    s.recent_week_sum, s.max_day_share_pct, s.actual_lookback_days,
                    s.mcap_growth_pct,
                )
                save_signal_event(
                    conn,
                    signal_name="revenue_price_gap",
                    protocol_slug=s.protocol_slug,
                    triggered_at=evaluated_at,
                    source="DeFiLlama",
                    metric_value=s.revenue_growth_pct,
                    details_json=json.dumps({
                        "revenue_growth_pct": s.revenue_growth_pct,
                        "revenue_growth_threshold_pct": revenue_signal_cfg["revenue_growth_threshold_pct"],
                        "mcap_growth_pct": s.mcap_growth_pct,
                        "mcap_reaction_threshold_pct": revenue_signal_cfg["mcap_reaction_threshold_pct"],
                        "lookback_days": s.lookback_days,
                        "actual_lookback_days": s.actual_lookback_days,
                        "recent_median_daily_revenue": s.recent_median_daily_revenue,
                        "baseline_median_daily_revenue": s.baseline_median_daily_revenue,
                        "recent_week_sum": s.recent_week_sum,
                        "max_day_share_pct": s.max_day_share_pct,
                        "mcap_now": s.mcap_now,
                        "mcap_before": s.mcap_before,
                        "symbol": s.symbol,
                    }),
                )

        if volume_signal_cfg.get("enabled", True):
            _run_volume_breakout(conn, watchlist, records, volume_signal_cfg)
    finally:
        conn.close()


def _run_volume_breakout(
    conn, watchlist: list[str], defillama_records: list[dict], signal_cfg: dict
) -> None:
    """Top up CoinGecko daily price/volume history for the watchlist and
    scan for the volume_breakout signal (TZ section 4.7).

    Split out of run_defillama_cycle for readability. Deliberately runs
    inside the SAME cycle/schedule as revenue_price_gap (config.yaml
    schedule.defillama_poll_hours / defillama_run_time) rather than getting
    its own --cycle: it shares the same watchlist (config.yaml
    watchlist.defillama_protocols) and, unlike OI (TZ 4.1, a genuinely
    "fast" signal per TZ section 5), CoinGecko's own history here is daily-
    granularity data - polling it more often than once a day would not
    produce any new information, only repeat the same day's point.

    Args:
        conn: open storage/db.py connection (the same one run_defillama_cycle
            is already using for this run, not a second connection).
        watchlist: config.yaml watchlist.defillama_protocols, for logging
            coverage (X/Y protocols evaluated).
        defillama_records: this cycle's defillama.collect() output - reused
            here only for each record's "gecko_id" field (resolved by
            DeFiLlama's own /protocols response, already fetched for mcap -
            no extra network call needed to get it; see
            collectors/defillama.py's collect() docstring).
        signal_cfg: config.yaml signals.volume_breakout.
    """
    # Built over the FULL watchlist, not just the slugs present in
    # defillama_records - a protocol that this cycle's defillama.collect()
    # missed entirely (e.g. no fees/revenue data at DeFiLlama this run, see
    # collect()'s "watchlist miss" logging) must still show up as
    # insufficient_history below, not silently disappear from both the
    # "evaluated" and "insufficient_history" counts.
    gecko_id_by_slug = {r["slug"]: r.get("gecko_id") for r in defillama_records}
    slug_to_gecko_id = {slug: gecko_id_by_slug.get(slug) for slug in watchlist}
    missing_gecko_id = sorted(slug for slug, gid in slug_to_gecko_id.items() if not gid)
    if missing_gecko_id:
        logger.warning(
            "volume_breakout: no gecko_id on file at DeFiLlama for %d/%d watchlist "
            "protocols (%s) - CoinGecko price/volume history can't be collected for them",
            len(missing_gecko_id), len(watchlist), missing_gecko_id,
        )

    logger.info(
        "Fetching CoinGecko price/volume history for %d protocol(s) with a known gecko_id...",
        len(slug_to_gecko_id) - len(missing_gecko_id),
    )
    price_records = coingecko.collect_price_volume_history(slug_to_gecko_id)
    collected_slugs = {r["protocol_slug"] for r in price_records}
    logger.info(
        "Collected %d price/volume point(s) across %d/%d watchlist protocols",
        len(price_records), len(collected_slugs), len(watchlist),
    )

    save_coingecko_price_history(conn, price_records)
    logger.info("Saved CoinGecko price/volume history to storage")

    # Same "don't scan off stale data after a failed collection" guard as
    # run_binance_futures_cycle: if nothing at all came back this run (source
    # down after retries, or no protocol has a known gecko_id), scanning
    # below would only ever see history from a PREVIOUS run - for a "did
    # price break out TODAY" signal, evaluating a stale "today" without
    # saying so would misrepresent how current the result is.
    if not collected_slugs:
        logger.warning(
            "volume_breakout: today's CoinGecko collection returned no data for any "
            "watchlist protocol (source likely unavailable after retries, or no "
            "protocol has a known gecko_id - see warnings above). Skipping this run's "
            "scan so nothing is reported as freshly \"FIRED\" off stale data.",
        )
        return
    elif len(collected_slugs) < len(watchlist) / 2:
        logger.warning(
            "volume_breakout: today's collection only covered %d/%d watchlist protocols - "
            "the scan below still runs, but any signal for a protocol NOT in this run's "
            "collection is based on stale data from a previous run, not today's numbers.",
            len(collected_slugs), len(watchlist),
        )

    result = volume_breakout.scan_watchlist(
        conn,
        slug_to_gecko_id,
        resistance_lookback_days=signal_cfg["resistance_lookback_days"],
        volume_avg_lookback_days=signal_cfg["volume_avg_lookback_days"],
        volume_ratio_threshold=signal_cfg["volume_ratio_threshold"],
        min_volume_avg_usd=signal_cfg.get("min_volume_avg_usd", 0),
    )

    # Same "why zero candidates" distinction as the other two signals: not
    # enough stored history yet vs. too thin to trust vs. evaluated but
    # nobody crossed the threshold.
    evaluated = (
        len(watchlist) - len(result.insufficient_history) - len(result.insufficient_liquidity)
    )
    logger.info(
        "volume_breakout: %d/%d protocols evaluated (%d still need more collected "
        "price/volume history - see signals/volume_breakout.py's coverage rule, %d skipped "
        "for average daily volume below the $%s liquidity floor), %d candidate(s) found",
        evaluated, len(watchlist), len(result.insufficient_history),
        len(result.insufficient_liquidity), signal_cfg.get("min_volume_avg_usd", 0),
        len(result.signals),
    )
    evaluated_at = datetime.now(timezone.utc).isoformat()
    for s in result.signals:
        logger.info(
            "volume_breakout FIRED: %s price %.6g broke above %d-day resistance %.6g, "
            "volume %.0f is %.1fx the %d-day average (%.0f)",
            s.protocol_slug, s.price_now, s.resistance_lookback_days, s.resistance_level,
            s.volume_now, s.volume_ratio, s.volume_avg_lookback_days, s.volume_avg,
        )
        save_signal_event(
            conn,
            signal_name="volume_breakout",
            protocol_slug=s.protocol_slug,
            triggered_at=evaluated_at,
            source="CoinGecko",
            metric_value=s.volume_ratio,
            details_json=json.dumps({
                "gecko_id": s.gecko_id,
                "date": s.date,
                "price_now": s.price_now,
                "resistance_level": s.resistance_level,
                "resistance_lookback_days": s.resistance_lookback_days,
                "resistance_window_days_available": s.resistance_window_days_available,
                "volume_now": s.volume_now,
                "volume_avg": s.volume_avg,
                "volume_avg_lookback_days": s.volume_avg_lookback_days,
                "volume_window_days_available": s.volume_window_days_available,
                "volume_ratio": s.volume_ratio,
                "volume_ratio_threshold": signal_cfg["volume_ratio_threshold"],
            }),
        )


def run_binance_futures_cycle(config: dict) -> None:
    """Collect Binance Futures OI/price history, store it, and scan for the
    oi_divergence signal (TZ section 4.1).

    Args:
        config: parsed config.yaml (see load_config).
    """
    watchlist = config["watchlist"]["binance_futures_symbols"]
    db_path = Path(config["storage"]["sqlite_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    signal_cfg = config["signals"]["oi_divergence"]
    lookback_hours = signal_cfg["lookback_hours"]

    init_db(db_path)

    logger.info("Fetching Binance Futures OI/price history for %d symbols...", len(watchlist))
    records = binance_futures.collect(watchlist, lookback_hours=lookback_hours)
    collected_symbols = {r["symbol"] for r in records}
    logger.info(
        "Collected %d OI/price points across %d/%d watchlist symbols",
        len(records), len(collected_symbols), len(watchlist),
    )

    conn = get_connection(db_path)
    try:
        save_binance_oi_snapshots(conn, records)
        logger.info("Saved OI snapshots to %s", db_path)

        if not signal_cfg.get("enabled", True):
            return

        # binance_futures.collect() never raises for a source outage - it
        # logs per-symbol failures and returns whatever it could get, even
        # an empty list. Scanning below still reads whatever was stored on a
        # previous successful run, so a fully/mostly failed collection here
        # must not lead to a silent "FIRED" based on stale data - the user
        # has to be told this run's numbers may not be current.
        if not collected_symbols:
            logger.warning(
                "oi_divergence: today's Binance Futures collection returned no data for "
                "any of the %d watchlist symbols (source likely unavailable after retries "
                "- see the errors logged above). Skipping this run's scan so nothing is "
                "reported as freshly \"FIRED\" off stale, previously stored data.",
                len(watchlist),
            )
            return
        elif len(collected_symbols) < len(watchlist) / 2:
            logger.warning(
                "oi_divergence: today's collection only covered %d/%d watchlist symbols - "
                "the scan below still runs, but any signal for a symbol NOT in this run's "
                "collection is based on stale data from a previous run, not today's numbers.",
                len(collected_symbols), len(watchlist),
            )

        result = oi_divergence.scan_watchlist(
            conn,
            watchlist,
            oi_growth_threshold_pct=signal_cfg["oi_growth_threshold_pct"],
            price_change_threshold_pct=signal_cfg["price_change_threshold_pct"],
            lookback_hours=lookback_hours,
            min_oi_value_usdt=signal_cfg.get("min_oi_value_usdt", 0),
        )

        # Same "why zero candidates" distinction as revenue_price_gap: not
        # enough stored history yet vs. too thin to trust vs. evaluated but
        # nobody crossed the threshold - conflating these misleads the user
        # about whether there's a data problem or just a quiet market.
        evaluated = (
            len(watchlist) - len(result.insufficient_history) - len(result.insufficient_liquidity)
        )
        logger.info(
            "oi_divergence: %d/%d symbols evaluated (%d still need >= %sh of collected "
            "OI history to compare, %d skipped for Open Interest below the $%s liquidity "
            "floor), %d candidate(s) found",
            evaluated, len(watchlist), len(result.insufficient_history),
            lookback_hours, len(result.insufficient_liquidity),
            signal_cfg.get("min_oi_value_usdt", 0), len(result.signals),
        )
        evaluated_at = datetime.now(timezone.utc).isoformat()
        for s in result.signals:
            logger.info(
                "oi_divergence FIRED: %s OI +%.1f%% over %.1fh (actual), price only %+.1f%%",
                s.symbol, s.oi_growth_pct, s.lookback_hours, s.price_change_pct,
            )
            save_signal_event(
                conn,
                signal_name="oi_divergence",
                # signal_events' protocol_slug column doubles as a generic
                # candidate-identifier column across all signal types - for
                # Binance Futures signals it holds the ticker (e.g.
                # "BTCUSDT"), not a DeFiLlama protocol slug. Deliberate, see
                # this task's instructions - the schema/column isn't renamed.
                protocol_slug=s.symbol,
                triggered_at=evaluated_at,
                source="Binance Futures",
                metric_value=s.oi_growth_pct,
                details_json=json.dumps({
                    "lookback_hours": s.lookback_hours,
                    "oi_growth_pct": s.oi_growth_pct,
                    "price_change_pct": s.price_change_pct,
                    "oi_now": s.oi_now,
                    "oi_before": s.oi_before,
                    "price_now": s.price_now,
                    "price_before": s.price_before,
                    "oi_growth_threshold_pct": signal_cfg["oi_growth_threshold_pct"],
                    "price_change_threshold_pct": signal_cfg["price_change_threshold_pct"],
                }),
            )
    finally:
        conn.close()


def _warn_stale(source: str, elapsed_hours: float, expected_interval_hours: float) -> None:
    """Log a hard-to-miss WARNING that `source` hasn't produced fresh data
    recently, framed by a line of "=" so it stands out from the surrounding
    INFO lines in the log file.

    Args:
        source: human-readable name of the data source/operation.
        elapsed_hours: hours since the last observed activity.
        expected_interval_hours: how often `source` is supposed to run.
    """
    border = "=" * 70
    logger.warning(
        "%s\n"
        "STALE DATA: %s has not produced a new row in %.1f hours (%.1f days) - "
        "expected roughly every %.1f hours. The scheduled task may have "
        "silently failed to start (known low-memory issue on this machine, "
        "see README.md) - check Get-ScheduledTaskInfo and the corresponding "
        "logs\\scheduler_*.log / logs\\backup.log.\n%s",
        border, source, elapsed_hours, elapsed_hours / 24, expected_interval_hours, border,
    )


def check_data_freshness(conn, config: dict) -> None:
    """Warn loudly if any of the three collectors or the DB backup have gone
    quiet for longer than expected (TZ section: transparency about what the
    system is/isn't doing).

    Why this exists at all: on this machine, Windows Task Scheduler has been
    observed to report LastTaskResult=0 (success) for a task whose process
    never actually started (a known low-memory condition here) - so nothing
    in Task Scheduler itself flags the problem, and data collection just
    quietly stops. Since Task Scheduler can't be trusted to notice this,
    main.py checks its OWN evidence instead: how long ago each destination
    table (or the backup folder) last actually received a new row/file. This
    is run on every invocation, regardless of which --cycle was requested,
    so that even a single-cycle run notices if a DIFFERENT source has gone
    stale.

    Each of the four checks is independent and best-effort: a source that
    has genuinely never run yet (fresh install) logs an INFO line, not a
    warning - that's an expected state, not a problem. A source that HAS run
    before but has gone quiet for more than STALENESS_MULTIPLIER times its
    expected interval gets a loud, boxed WARNING via _warn_stale. This
    function never raises - see the try/except around its call site below.

    Args:
        conn: open storage/db.py connection to the live database.
        config: parsed config.yaml (see load_config) - reads
            schedule.defillama_poll_hours, schedule.binance_futures_poll_hours,
            and backup.backup_dir.
    """
    schedule_cfg = config.get("schedule", {})
    defillama_interval = schedule_cfg.get("defillama_poll_hours", 24)
    binance_interval = schedule_cfg.get("binance_futures_poll_hours", 6)

    checks = [
        ("DeFiLlama collector (defillama_snapshots)", get_latest_defillama_fetch_time(conn), defillama_interval),
        ("CoinGecko collector (coingecko_price_history)", get_latest_coingecko_fetch_time(conn), defillama_interval),
        ("Binance Futures collector (binance_oi_snapshots)", get_latest_binance_oi_fetch_time(conn), binance_interval),
    ]

    for source, latest_iso, expected_interval_hours in checks:
        if latest_iso is None:
            logger.info("No data for %s yet, skipping freshness check", source)
            continue
        latest = datetime.fromisoformat(latest_iso)
        elapsed_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
        if elapsed_hours > expected_interval_hours * STALENESS_MULTIPLIER:
            _warn_stale(source, elapsed_hours, expected_interval_hours)

    # Backup check: no storage/db.py table for this - resolve backup_dir the
    # same way scripts/backup_db.py's main() does (explicit config value, or
    # the project-local default), duplicated rather than imported (see
    # _BACKUP_FILENAME_GLOB's comment above for why scripts.backup_db can't
    # be imported here).
    try:
        backup_dir_str = config.get("backup", {}).get("backup_dir")
    except Exception:
        logger.warning(
            "Could not read backup.backup_dir from config.yaml - falling back to default backup dir"
        )
        backup_dir_str = None

    backup_dir = Path(backup_dir_str) if backup_dir_str else PROJECT_ROOT / "backups"
    if not backup_dir.is_absolute():
        backup_dir = PROJECT_ROOT / backup_dir

    if not backup_dir.is_dir():
        logger.info("No backups for %s yet, skipping freshness check", backup_dir)
    else:
        backups = sorted(backup_dir.glob(_BACKUP_FILENAME_GLOB))
        if not backups:
            logger.info("No backups in %s yet, skipping freshness check", backup_dir)
        else:
            latest_backup = backups[-1]
            mtime = latest_backup.stat().st_mtime
            elapsed_hours = (datetime.now() - datetime.fromtimestamp(mtime)).total_seconds() / 3600
            if elapsed_hours > BACKUP_EXPECTED_INTERVAL_HOURS * STALENESS_MULTIPLIER:
                _warn_stale(f"DB backup ({backup_dir})", elapsed_hours, BACKUP_EXPECTED_INTERVAL_HOURS)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI args for this script.

    `--cycle` lets Task Scheduler run the DeFiLlama and Binance Futures
    cycles on separate schedules (see scripts/register_scheduled_task.ps1,
    which registers both) and config.yaml's schedule.defillama_run_time /
    schedule.binance_futures_poll_hours) instead of always running both
    together. Defaults to "all" so a plain `python main.py` (manual runs,
    and any script/documentation written before this flag existed) keeps
    working exactly as before.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cycle",
        choices=["all", "defillama", "binance"],
        default="all",
        help="which cycle(s) to run (default: all - runs both, for manual use)",
    )
    return parser.parse_args(argv)


# Which cycle(s) map to which log filename - see _setup_logging's docstring
# and the module docstring for why --cycle defillama / --cycle binance each
# need their own file instead of sharing scheduler.log.
_CYCLE_LOG_FILENAMES = {
    "all": "scheduler.log",
    "defillama": "scheduler_defillama.log",
    "binance": "scheduler_binance.log",
}


if __name__ == "__main__":
    args = _parse_args()
    _setup_logging(_CYCLE_LOG_FILENAMES[args.cycle])

    try:
        cfg = load_config()
    except Exception:
        # Nothing to run without a config - but still log it (Task Scheduler
        # runs have no console to see a traceback on) instead of crashing
        # silently.
        logger.exception("Failed to load config.yaml")
        sys.exit(1)

    # Each cycle is independent (different data source, different signal) -
    # one failing (e.g. DeFiLlama or Binance temporarily unreachable after
    # all retries) must not stop the other from running when both are
    # requested (--cycle all). Both exceptions are logged with a full stack
    # trace (Task Scheduler runs have no console to see a crash on) and
    # tracked so the process still exits non-zero if either cycle failed.
    failed = False

    if args.cycle in ("all", "defillama"):
        try:
            run_defillama_cycle(cfg)
        except Exception:
            logger.exception("DeFiLlama cycle failed")
            failed = True

    if args.cycle in ("all", "binance"):
        try:
            run_binance_futures_cycle(cfg)
        except Exception:
            logger.exception("Binance Futures cycle failed")
            failed = True

    # Runs regardless of --cycle and regardless of `failed` above - even a
    # single-cycle run should notice if a DIFFERENT source has gone stale
    # (see check_data_freshness's docstring). Must never affect the process's
    # exit code on its own: this is diagnostics about OTHER runs, not a
    # failure of this run.
    try:
        db_path = Path(cfg["storage"]["sqlite_path"])
        if not db_path.is_absolute():
            db_path = PROJECT_ROOT / db_path
        freshness_conn = get_connection(db_path)
        try:
            check_data_freshness(freshness_conn, cfg)
        finally:
            freshness_conn.close()
    except Exception:
        logger.exception("Data freshness check failed")

    if failed:
        sys.exit(1)
