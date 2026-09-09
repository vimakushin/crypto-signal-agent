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
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import yaml

from collectors import binance_futures, coingecko, defillama
from signals import oi_divergence, revenue_price_gap, volume_breakout
from storage.db import (
    get_connection,
    init_db,
    save_binance_oi_snapshots,
    save_coingecko_price_history,
    save_defillama_snapshots,
)

PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"


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

        if revenue_signal_cfg.get("enabled", True):
            result = revenue_price_gap.scan_watchlist(
                conn,
                watchlist,
                revenue_growth_threshold_pct=revenue_signal_cfg["revenue_growth_threshold_pct"],
                mcap_reaction_threshold_pct=revenue_signal_cfg["mcap_reaction_threshold_pct"],
                lookback_days=revenue_signal_cfg["lookback_days"],
            )

            # Two distinct reasons for "no candidates" - conflating them into
            # one line misleads the user either way: "needs more history"
            # reads as a data problem when the real market simply didn't
            # cross the threshold, and vice versa.
            evaluated = len(watchlist) - len(result.insufficient_history)
            logger.info(
                "revenue_price_gap: %d/%d protocols evaluated (%d still need >= %s days of "
                "collected history to compare market cap), %d candidate(s) found",
                evaluated, len(watchlist), len(result.insufficient_history),
                revenue_signal_cfg["lookback_days"], len(result.signals),
            )
            for s in result.signals:
                logger.info(
                    "revenue_price_gap FIRED: %s (%s) revenue +%.1f%% over %dd, "
                    "mcap only +%.1f%%",
                    s.protocol_slug, s.symbol, s.revenue_growth_pct,
                    s.lookback_days, s.mcap_growth_pct,
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
    )

    # Same "why zero candidates" distinction as the other two signals: not
    # enough stored history yet vs. evaluated but nobody crossed the
    # threshold.
    evaluated = len(watchlist) - len(result.insufficient_history)
    logger.info(
        "volume_breakout: %d/%d protocols evaluated (%d still need more collected "
        "price/volume history - see signals/volume_breakout.py's coverage rule), "
        "%d candidate(s) found",
        evaluated, len(watchlist), len(result.insufficient_history), len(result.signals),
    )
    for s in result.signals:
        logger.info(
            "volume_breakout FIRED: %s price %.6g broke above %d-day resistance %.6g, "
            "volume %.0f is %.1fx the %d-day average (%.0f)",
            s.protocol_slug, s.price_now, s.resistance_lookback_days, s.resistance_level,
            s.volume_now, s.volume_ratio, s.volume_avg_lookback_days, s.volume_avg,
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
        )

        # Same "why zero candidates" distinction as revenue_price_gap: not
        # enough stored history yet vs. evaluated but nobody crossed the
        # threshold - conflating the two misleads the user about whether
        # there's a data problem or just a quiet market.
        evaluated = len(watchlist) - len(result.insufficient_history)
        logger.info(
            "oi_divergence: %d/%d symbols evaluated (%d still need >= %sh of collected "
            "OI history to compare), %d candidate(s) found",
            evaluated, len(watchlist), len(result.insufficient_history),
            lookback_hours, len(result.signals),
        )
        for s in result.signals:
            logger.info(
                "oi_divergence FIRED: %s OI +%.1f%% over %.1fh (actual), price only %+.1f%%",
                s.symbol, s.oi_growth_pct, s.lookback_hours, s.price_change_pct,
            )
    finally:
        conn.close()


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

    if failed:
        sys.exit(1)
