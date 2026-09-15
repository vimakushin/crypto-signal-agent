# crypto-signal-agent

*English | [Русский](README.ru.md)*

A service that watches the crypto market for coins in the early stage of a move — before that move shows up on the price chart.

## What problem it solves

By the time a coin's rally is visible on the price chart, the early opportunity is usually already gone. Instead of watching price, this service tracks three independent market signals that tend to lead it: protocol revenue growing while the token's market cap doesn't react, futures positioning building up while price stays flat, and a price breakout on abnormal volume. Every day, a Telegram bot sends a list of candidates — with a breakdown of exactly which signal fired and on what numbers.

This isn't a trading bot. It never says "buy" or "sell," never names a price target, and never places an order — it only collects and ranks candidates for a human to review manually. The call is always the user's.

## How it's built

```
collectors/  →  signals/  →  scoring/  →  notifications/
(data collection) (detectors)  (ranking)   (digest)
```

- **Collectors** (`collectors/`) pull history from three free public APIs — DeFiLlama (protocol revenue/fees), Binance Futures (Open Interest, no API key required) and CoinGecko (price/volume/market cap) — and accumulate it in a local SQLite database (`storage/`), so signals can compare "now" against "before" without re-hitting the source every time.
- **Signal detectors** (`signals/`) — three independent modules:
  - **revenue-gap** — protocol revenue grew X% over 7-30 days while the token's market cap didn't react;
  - **OI divergence** — futures Open Interest grew X% while price barely moved;
  - **volume breakout** — price broke a multi-month resistance level on 2-3× average volume.

  Each detector explicitly distinguishes "the signal didn't fire" from "not enough history yet" or "the data can't be trusted" — silence is never presented as an absence of signal.
- **Scoring** (`scoring/ranker.py`) sums a coin's fired signals into a single score, weighted per `config.yaml`. It never recomputes anything — only aggregates what's already been stored.
- **Notifications** (`notifications/`) send a daily Telegram digest of candidates with the full breakdown, and in parallel maintain `observations.md` — a plain markdown manual-labeling journal: what the system flagged, and what happened to the coin 7/14/30 days later.

All thresholds, signal weights, the watchlist and the schedule live in `config.yaml`, not hardcoded in the code.

## What's working today

MVP readiness is defined in [`MVP.md`](MVP.md) — six concrete items, no "mostly done." As things stand: all three data sources are being collected, all three signals are computed and logged, firings are persisted to the database, the score is computed and explained, and a daily digest goes out on Telegram. A web UI, in-app manual labeling, and a full backtest are deliberately out of scope for MVP — see [`BACKLOG.md`](BACKLOG.md).

## How the project was built

Every line of code here was written and reviewed through Claude Code, with the work split across specialized subagents (`.claude/agents/`) rather than done in one undifferentiated stream:

- **python-dev** writes and fixes the entire Python core: collectors, signal detectors, scoring, storage, notifications.
- **web-dev** owns the future web UI, and only the web UI — it never touches the core.
- **code-reviewer** checks every notable change for correctness — logic bugs, unhandled exceptions, leaked secrets — after it's written and before the next task starts. It only reads and reports; it never edits code itself.
- **signal-validator** checks something different: not whether the code is right, but whether its output means anything — data freshness, false positives, noise on low-liquidity coins, no look-ahead bias when running calculations against historical data.

The default order is: the relevant agent writes the change → `code-reviewer` checks it → if it's a signal, `signal-validator` checks it too → any blocking finding gets fixed before moving on to the next thing.

MVP readiness was defined up front, in writing, in [`MVP.md`](MVP.md) — six items, and anything not on that list is simply out of scope for MVP. Every deviation from the current item gets called out explicitly rather than slipped in quietly. Decisions that were deliberately deferred — with the reasoning for deferring them, not just dropped — are logged in [`BACKLOG.md`](BACKLOG.md) and re-read before each new round of work.

---

## Operations

### Setup

You'll need Python 3.11+ and git.

```powershell
git clone https://github.com/vimakushin/crypto-signal-agent.git
cd crypto-signal-agent
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in a Telegram bot token and your chat id (get a token from [@BotFather](https://t.me/BotFather), then find your chat id by sending the bot any message and checking `https://api.telegram.org/bot<TOKEN>/getUpdates`) — data collection and the signals themselves work fine without this, only the daily Telegram digest won't send.

### Running it

```powershell
.venv\Scripts\python.exe main.py
```

Without `--cycle`, both cycles run together. On the very first run, you probably won't see any candidates yet: revenue-gap needs at least 7 days of accumulated market-cap history, and volume breakout needs nearly the whole `resistance_lookback_days` window (180 days by default) — without `backfill_history.py`, that takes months to build up on its own. OI divergence can fire on the first run, since Binance returns ~36-48h of history right away. To run one cycle at a time:

```powershell
.venv\Scripts\python.exe main.py --cycle defillama
.venv\Scripts\python.exe main.py --cycle binance
```

### Backfilling history

A normal run only accumulates history from the moment it first ran. To give the signals something to work with right away, there's a one-off script that pulls historical revenue/price/market-cap data (DeFiLlama — several years, CoinGecko — up to 365 days, Binance OI — up to ~30 days, which is as far back as Binance itself keeps it):

```powershell
.venv\Scripts\python.exe scripts\backfill_history.py
```

Slow (CoinGecko's rate limits), meant to be run once by hand, not part of the daily cycle. Safe to re-run — days already saved aren't duplicated. Progress and any gaps go to `logs\backfill.log`.

### Scheduled runs

Four separate Windows Task Scheduler tasks:

- **`CryptoSignalAgent-DeFiLlama`** — once a day, `main.py --cycle defillama`.
- **`CryptoSignalAgent-BinanceOI`** — every few hours, `main.py --cycle binance`.
- **`CryptoSignalAgent-Backup`** — once a day, `scripts\backup_db.py`.
- **`CryptoSignalAgent-TelegramDigest`** — once a day, `notifications\telegram_bot.py` (which also updates `observations.md`).

Times and frequency live in `config.yaml` (`schedule.*`). After editing `config.yaml`, re-run this once to apply it:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_scheduled_task.ps1
```

If the computer is off at the scheduled time, that run is simply skipped — it'll run at the next opportunity once the computer is back on.

Check status:

```powershell
Get-ScheduledTask -TaskName "CryptoSignalAgent-DeFiLlama" | Get-ScheduledTaskInfo
Get-ScheduledTask -TaskName "CryptoSignalAgent-BinanceOI" | Get-ScheduledTaskInfo
Get-ScheduledTask -TaskName "CryptoSignalAgent-Backup" | Get-ScheduledTaskInfo
Get-ScheduledTask -TaskName "CryptoSignalAgent-TelegramDigest" | Get-ScheduledTaskInfo
```

Run any of them right now, by hand:

```powershell
Start-ScheduledTask -TaskName "CryptoSignalAgent-DeFiLlama"
Start-ScheduledTask -TaskName "CryptoSignalAgent-BinanceOI"
Start-ScheduledTask -TaskName "CryptoSignalAgent-Backup"
Start-ScheduledTask -TaskName "CryptoSignalAgent-TelegramDigest"
```

Logs:

- `logs\scheduler_defillama.log` — the DeFiLlama task.
- `logs\scheduler_binance.log` — the Binance task.
- `logs\backup.log` — the backup task.
- `logs\telegram.log` — the Telegram digest task.
- `logs\scheduler.log` — manual `main.py` runs without `--cycle` only.
- `logs\backfill.log` — the one-off `backfill_history.py`.

The log always explains exactly why a coin got a signal, and separately distinguishes "not enough history yet" from "there's plenty of data, it just didn't cross the threshold." If a day's data collection failed outright, the log flags it as its own line instead of silently scoring against stale data. Every `main.py` run also checks whether any source's data — or the backup file — has gone stale longer than expected; if so, it prints and logs a hard-to-miss warning instead of failing silently.

If the scheduled tasks ever disappear (a Windows reinstall, moving to a new machine), recreate them with the same `register_scheduled_task.ps1` command above.

### The observations journal (`observations.md`)

An early, cut-down version of manual labeling: a plain markdown table at the project root, committed to git. Every day, when the Telegram digest goes out, one row gets added per new candidate — no more than once every 30 days for the same coin, so a signal that stays fired for weeks doesn't flood the journal with near-duplicates — with the date, the coin, its score, which signals fired, and price/market cap at that moment. Seven days later, the system fills in the actual price/market cap on its own. The "after 14 days," "after 30 days," and "Verdict" columns are filled in by hand; the code never touches them.

### Web UI — episode labeling

The first screen of the internal web UI (TZ section 6): manual "did it hold up" labeling for signal firings. Not a product for end users — that stays Telegram-only — this is a local tool for the owner, since manual "worked / didn't work" labels are the only honest data behind future weight calibration.

```powershell
.venv\Scripts\python.exe -m uvicorn web.app:app --host 127.0.0.1 --port 8000
```

Then open `http://127.0.0.1:8000/episodes` in a browser. Listens on `127.0.0.1` only — never reachable from outside the machine, no login, none planned (see `.claude/agents/web-dev.md`'s boundaries).

The same signal firing a day apart for weeks (one protocol can stay "fired" for weeks under `revenue_price_gap`) is grouped into one episode, not one row per day — `storage/episodes.py` does the grouping, reusing the same freshness logic `scoring/ranker.py` already uses for the daily digest. The screen only lists episodes old enough to judge (7+ days since they started by default, `config.yaml`'s `episode_review.min_age_days`) and not yet labeled; three buttons record "worked / didn't work / too early to tell" straight into a new `episode_outcomes` table (`storage/db.py`) — `observations.md` is untouched by this screen, it stays a separate, read-only journal. An episode labeled while still active gets resurfaced for a second look if it's still going `episode_review.reopen_after_days` (14 by default) later, since the first call may not have held.

`revenue_price_gap`'s formula changed on 2026-09-15 (weekly sums → daily medians, see `BACKLOG.md`) — an episode spanning that date is always split in two, and anything computed under the old formula is marked with a visible badge and can be filtered out, so the two are never averaged together by mistake.

### Database backups

`storage\db.sqlite` isn't tracked in git (see `.gitignore`) — too large and it changes constantly. `scripts\backup_db.py` uses SQLite's own safe way of snapshotting a live database (`sqlite3.Connection.backup`), which doesn't break even if a write is happening at that exact moment.

```powershell
.venv\Scripts\python.exe scripts\backup_db.py
```

Where backups go is set in `config.yaml` (`backup.backup_dir`), not a flag — otherwise the scheduled task, which runs the script with no arguments, would keep writing to the old location forever. If it isn't set, backups land in a local `backups\` folder inside the project (not tracked in git) — the downside being that losing the disk or the machine takes out both the original and the backups at once; pointing it at a cloud-synced folder (OneDrive, Google Drive, etc.) is safer.

To change where backups are stored, edit `backup.backup_dir` in `config.yaml`. The `--backup-dir` flag still works for a manual, one-off run and overrides `config.yaml` just for that run:

```powershell
.venv\Scripts\python.exe scripts\backup_db.py --backup-dir "C:\path\for\a\one-off\test"
```

The 14 most recent backups are kept (`--keep` changes that number); older ones are deleted automatically. Log: `logs\backup.log`.

### Version control (git)

Git is the project's change history — every commit is a snapshot you can come back to. This only covers code (`.py`, `.yaml`, `.md`); the database, logs and backups aren't tracked in git, see the section above for those.

#### Saving changes

```powershell
git add .
git commit -m "short description of what changed"
git push
```

`git add .` marks changes as ready to be saved. `git commit` saves a snapshot locally. `git push` sends it to GitHub, off the machine.

#### Restoring on a new machine

See "Setup" above. Once you've cloned it, the code and `config.yaml` are in place, but:

- **The database history is empty** — restore it from a backup:

```powershell
copy "backups\db_20260909_080000.sqlite" "storage\db.sqlite"
```

  (use the filename of your most recent backup).

- **Secrets** — `.env` isn't tracked in git; recreate it from `.env.example` (see "Setup").
- The scheduled tasks need to be registered again (`scripts\register_scheduled_task.ps1`) — they don't travel with the code.
