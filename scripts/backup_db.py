"""One-off / scheduled backup script for storage/db.sqlite (CLAUDE.md: the
accumulated history - tens of thousands of collector rows going back months
via scripts/backfill_history.py - must not be lost, and the live database is
NOT in git, see .gitignore).

Why not a plain file copy
--------------------------
storage/db.sqlite can be mid-write at any moment: two separate Windows Task
Scheduler tasks (CryptoSignalAgent-DeFiLlama, CryptoSignalAgent-BinanceOI -
see main.py / scripts/register_scheduled_task.ps1) each open this same file
and write to it independently, and this backup script itself is meant to be
run on its own schedule, at an arbitrary time, without coordinating with
either of them first. A plain OS-level file copy (`shutil.copy`, PowerShell
`Copy-Item`) reads the file's bytes with no awareness of SQLite's own
locking - if a writer is mid-transaction at that exact moment, the copy can
capture the file in a torn, inconsistent state (e.g. some pages written,
some not) that SQLite may refuse to open later, exactly when a restore is
needed most.

sqlite3.Connection.backup() (the stdlib binding for SQLite's own Online
Backup API) is used instead: it copies the database through SQLite itself,
which understands its own locking and produces a consistent snapshot even
while another connection is actively reading or writing - this is the
official, documented way to back up a live SQLite database, not a
workaround.

Where backups are stored
-------------------------
Resolved in this priority order (highest first), same pattern as --db-path
below:

  1. --backup-dir, if passed explicitly on the command line - a manual run
     can still point this at any folder, whatever config.yaml says.
  2. backup.backup_dir in config.yaml, if that key is present - this is what
     the scheduled task (CryptoSignalAgent-Backup, run with no arguments at
     all - see scripts/register_scheduled_task.ps1) actually uses day to
     day, so changing where backups go only requires editing config.yaml,
     not the scheduled task itself.
  3. DEFAULT_BACKUP_DIR (<project root>/backups/), if neither of the above
     is set - keeps this script working unchanged on a config.yaml that
     predates the backup: section, or one where it was deliberately omitted.

Two real options for backup.backup_dir / --backup-dir, deliberately left to
the user to choose between (this script doesn't guess):

  1. A local folder inside the project (the default, "backups/"). Simplest
     for an MVP, zero setup - but it lives on the same physical disk as
     storage/db.sqlite itself, so it does NOT protect against that disk
     failing outright, the whole folder being deleted by mistake, or the PC
     being lost/stolen. It is already excluded from git (see .gitignore) -
     backups do not belong in version control (large, binary, and would
     bloat the repository's history forever since every backup is a full
     copy, not a diff).
  2. A folder inside an existing cloud-sync client the user already has
     running (e.g. a OneDrive or Google Drive folder synced on this PC) -
     set it via config.yaml's backup.backup_dir (or pass --backup-dir for a
     one-off run). This survives a local disk failure because a synced copy
     also lives in the cloud (and usually on other synced devices), but only
     works if that sync client is already installed and signed in - this
     script cannot detect or set that up.

Rotation
--------
Only the most recent --keep backups (default: 14, i.e. two weeks of daily
backups) are kept - older ones are deleted automatically so this doesn't
grow forever. Rotation only ever touches files matching this script's own
`db_YYYYMMDD_HHMMSS.sqlite` naming pattern inside --backup-dir, never
anything else that might be stored there (e.g. if --backup-dir points at a
shared cloud folder with other files in it).

Usage
-----
    .venv\\Scripts\\python.exe scripts\\backup_db.py
    .venv\\Scripts\\python.exe scripts\\backup_db.py --backup-dir "C:\\Users\\<you>\\OneDrive\\crypto-signal-agent-backups" --keep 30

Safe to run repeatedly / on a schedule: creates --backup-dir if it doesn't
exist yet, never overwrites a previous backup (each filename carries its own
timestamp down to the second), and any failure (source database missing,
disk full, backup folder unwritable) is logged to logs/backup.log rather
than swallowed - this is a local file operation, not a network call, so no
retry/backoff is attempted (per collectors/*.py's convention for THAT kind
of failure) - it either works now or it doesn't, and re-running it later
(next scheduled run, or by hand) is exactly the retry.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

# Same sys.path fix as scripts/backfill_history.py and scripts/replay_signals.py -
# this script lives one level below the project root.
sys.path.insert(0, str(PROJECT_ROOT))

import yaml  # noqa: E402

# Default location for backups: a folder inside the project itself, NOT
# tracked by git (see .gitignore) - see the module docstring's "Where
# backups are stored" section for why this is the simple default rather than
# the safest possible one, and what the alternative (a cloud-synced folder)
# looks like.
DEFAULT_BACKUP_DIR = PROJECT_ROOT / "backups"

# Two weeks of daily backups. Generous enough to recover from "didn't notice
# a problem for a few days", cheap enough (each backup is a full copy of
# storage/db.sqlite) not to worry about disk space on a personal PC.
DEFAULT_KEEP = 14

BACKUP_FILENAME_FORMAT = "db_%Y%m%d_%H%M%S.sqlite"
# Matches exactly what BACKUP_FILENAME_FORMAT produces - used by
# _rotate_backups to find only this script's own backup files inside
# --backup-dir, never anything else that might be stored there (e.g. a
# shared cloud-sync folder used for other things too).
BACKUP_FILENAME_GLOB = "db_????????_??????.sqlite"


def _setup_logging() -> None:
    """Console + a rotating file in logs/backup.log, same convention as
    main.py / scripts/backfill_history.py / scripts/replay_signals.py - each
    distinct operation gets its own log file rather than sharing one.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        LOG_DIR.mkdir(exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                LOG_DIR / "backup.log",
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
logger = logging.getLogger("backup_db")


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> dict:
    """Load config.yaml (duplicated from main.py/backfill_history.py/
    replay_signals.py's own load_config, not imported, so this script
    doesn't trigger main.py's module-level logging setup as an import side
    effect).
    """
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def backup_database(db_path: Path, backup_dir: Path) -> Path:
    """Take a consistent snapshot of `db_path` into a new timestamped file
    inside `backup_dir`, using SQLite's own Online Backup API (see module
    docstring for why this is used instead of a plain file copy).

    Args:
        db_path: path to the live storage/db.sqlite (or any SQLite database)
            to back up. Must already exist.
        backup_dir: directory to write the backup into. Created if missing.

    Returns:
        Path to the newly created backup file.

    Raises:
        FileNotFoundError: if `db_path` does not exist - deliberately NOT
            caught here, because sqlite3.connect() on a non-existent path
            silently CREATES a new, empty database file instead of raising,
            which would otherwise make this function "succeed" by writing a
            useless, empty backup instead of failing loudly.
        sqlite3.Error: if opening either database or the backup itself
            fails (e.g. `db_path` exists but is not a valid SQLite file).
        OSError: if `backup_dir` can't be created or written to (e.g. no
            free disk space, permissions, or --backup-dir points at an
            unavailable network/cloud-sync path).
    """
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found at {db_path} - nothing to back up yet "
            "(has main.py been run at least once?)"
        )

    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_filename = datetime.now().strftime(BACKUP_FILENAME_FORMAT)
    backup_path = backup_dir / backup_filename

    source_conn = sqlite3.connect(str(db_path))
    try:
        target_conn = sqlite3.connect(str(backup_path))
        try:
            # The Online Backup API: copies page-by-page through SQLite
            # itself, which is aware of its own locking, so this produces a
            # consistent snapshot even if another process (one of the two
            # Task Scheduler tasks) is reading or writing db_path at this
            # exact moment - unlike a raw file copy, see module docstring.
            source_conn.backup(target_conn)
        finally:
            target_conn.close()
    finally:
        source_conn.close()

    return backup_path


def _rotate_backups(backup_dir: Path, keep: int) -> None:
    """Delete all but the `keep` most recent backups in `backup_dir`.

    Only touches files matching BACKUP_FILENAME_GLOB (this script's own
    `db_YYYYMMDD_HHMMSS.sqlite` naming) - anything else in `backup_dir`
    (e.g. other files in a shared cloud-sync folder) is left untouched.
    Sorting is lexicographic on the filename, which is equivalent to
    chronological order here because the timestamp format is fixed-width
    and zero-padded (YYYYMMDD_HHMMSS) - no need to parse each name back into
    a datetime just to sort it.

    Args:
        backup_dir: directory backups were written into.
        keep: how many of the newest backups to retain.
    """
    existing = sorted(backup_dir.glob(BACKUP_FILENAME_GLOB))
    surplus = existing[:-keep] if keep > 0 else existing
    for old_backup in surplus:
        try:
            old_backup.unlink()
            logger.info("Rotation: deleted old backup %s", old_backup.name)
        except OSError as exc:
            # A single backup file that can't be deleted (e.g. locked open
            # by another program, like being previewed in Explorer) must not
            # stop the rest of rotation, and must not be mistaken for the
            # backup itself having failed - it already succeeded above.
            logger.warning(
                "Rotation: could not delete old backup %s: %s", old_backup, exc
            )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help=(
            "Path to the SQLite database to back up. Defaults to "
            "storage.sqlite_path from config.yaml (i.e. the live database)."
        ),
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=None,
        help=(
            "Folder to write timestamped backups into. Defaults to "
            "backup.backup_dir from config.yaml if that key is set, otherwise "
            f"falls back to {DEFAULT_BACKUP_DIR} (a project-local folder excluded "
            "from git) - see the module docstring's 'Where backups are stored' "
            "section for the full priority order."
        ),
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=DEFAULT_KEEP,
        help=f"How many most-recent backups to retain (default: {DEFAULT_KEEP}).",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()

    # Only read config.yaml if at least one of db_path / backup_dir wasn't
    # given explicitly on the command line - an explicit flag never requires
    # config.yaml to even be readable, same as the pre-existing --db-path
    # behaviour this now also applies to --backup-dir.
    cfg: dict | None = None
    if args.db_path is None or args.backup_dir is None:
        try:
            cfg = load_config()
        except Exception:
            logger.exception(
                "Could not read config.yaml - pass --db-path and --backup-dir "
                "explicitly instead"
            )
            sys.exit(1)

    if args.db_path is not None:
        db_path = args.db_path
    else:
        try:
            db_path = Path(cfg["storage"]["sqlite_path"])
        except Exception:
            logger.exception(
                "Could not read storage.sqlite_path from config.yaml - pass "
                "--db-path explicitly instead"
            )
            sys.exit(1)
        if not db_path.is_absolute():
            db_path = PROJECT_ROOT / db_path

    if args.backup_dir is not None:
        backup_dir = args.backup_dir
    else:
        backup_dir_str = None
        try:
            backup_dir_str = cfg.get("backup", {}).get("backup_dir")
        except Exception:
            # A malformed backup: section (e.g. not a mapping) must not stop
            # the backup itself - fall back to the local default below.
            logger.warning(
                "Could not read backup.backup_dir from config.yaml - falling "
                "back to %s",
                DEFAULT_BACKUP_DIR,
            )
        backup_dir = Path(backup_dir_str) if backup_dir_str else DEFAULT_BACKUP_DIR

    if not backup_dir.is_absolute():
        backup_dir = PROJECT_ROOT / backup_dir

    logger.info("Backing up %s -> %s (keeping last %d backup(s))", db_path, backup_dir, args.keep)

    try:
        backup_path = backup_database(db_path, backup_dir)
    except FileNotFoundError as exc:
        logger.error(str(exc))
        sys.exit(1)
    except (sqlite3.Error, OSError):
        logger.exception("Backup failed for %s -> %s", db_path, backup_dir)
        sys.exit(1)

    size_mb = backup_path.stat().st_size / (1024 * 1024)
    logger.info("Backup created: %s (%.2f MB)", backup_path, size_mb)

    try:
        _rotate_backups(backup_dir, args.keep)
    except Exception:
        # Rotation failing must not make a fresh, successful backup look
        # like a failed run - log it and still exit 0, the backup itself is
        # safe on disk.
        logger.exception("Rotation of old backups in %s failed (backup itself still succeeded)", backup_dir)

    remaining = sorted(backup_dir.glob(BACKUP_FILENAME_GLOB))
    logger.info("%d backup(s) now present in %s", len(remaining), backup_dir)


if __name__ == "__main__":
    main()
