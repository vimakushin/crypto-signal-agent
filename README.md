# crypto-signal-agent

Crypto Early Signal Detector — см. `TZ_crypto_early_signal_detector.md`.

## Статус

MVP в процессе (раздел 8 ТЗ). Готово:

- Структура проекта (`collectors/`, `signals/`, `scoring/`, `storage/`, `notifications/`, `web/`)
- Python 3.12 + venv (`.venv`)
- Сборщик DeFiLlama (`collectors/defillama.py`) — revenue и market cap по watchlist
- Сборщик Binance Futures (`collectors/binance_futures.py`) — Open Interest и цены по watchlist фьючерсов (без API-ключа; заменяет платный Coinglass)
- Сборщик CoinGecko (`collectors/coingecko.py`) — дневная история цены/капитализации/объёма
- Хранилище истории в SQLite (`storage/db.py`, `storage/db.sqlite`)
- Сигналы: revenue-gap (`signals/revenue_price_gap.py`, ТЗ 4.5), OI-дивергенция (`signals/oi_divergence.py`, ТЗ 4.1), пробой на объёме (`signals/volume_breakout.py`, ТЗ 4.7)
- Точка входа `main.py` — `--cycle {all,defillama,binance}` (по умолчанию `all`)
- Разовое наполнение истории задним числом — `scripts\backfill_history.py`
- Три задания Планировщика Windows (расписание ниже)
- Бэкап базы данных — `scripts\backup_db.py`

Ещё не сделано: ranker/scoring, Telegram-уведомления, веб-интерфейс.

## Запуск

```powershell
.venv\Scripts\python.exe main.py
```

Без `--cycle` выполняются оба цикла сразу. На первом запуске кандидатов, скорее всего, не будет: revenue-gap нужно минимум 7 дней истории капитализации, пробою на объёме — почти всё окно `resistance_lookback_days` (180 дней по умолчанию) — без `backfill_history.py` это копится месяцами. OI-дивергенция может сработать уже на первом запуске (Binance сразу отдаёт ~36-48ч истории). Запустить по отдельности:

```powershell
.venv\Scripts\python.exe main.py --cycle defillama
.venv\Scripts\python.exe main.py --cycle binance
```

## Наполнение истории задним числом

Обычный запуск копит историю только с момента первого запуска. Чтобы сигналам было на чём проверяться сразу, есть разовый скрипт — грузит историю revenue/цены/капитализации из прошлого (DeFiLlama — несколько лет, CoinGecko — до 365 дней, Binance OI — до ~30 дней, глубже у самого Binance данных нет):

```powershell
.venv\Scripts\python.exe scripts\backfill_history.py
```

Медленно (лимиты CoinGecko), делается один раз вручную, не часть ежедневного цикла. Безопасно перезапускаемый — уже сохранённые дни не дублируются. Прогресс и пропуски — в `logs\backfill.log`.

## Автоматический запуск по расписанию

Три отдельных задания Планировщика Windows:

- **`CryptoSignalAgent-DeFiLlama`** — раз в сутки, `main.py --cycle defillama`.
- **`CryptoSignalAgent-BinanceOI`** — каждые несколько часов, `main.py --cycle binance`.
- **`CryptoSignalAgent-Backup`** — раз в сутки, `scripts\backup_db.py`.

Время и периодичность — в `config.yaml` (`schedule.defillama_run_time`, `schedule.binance_futures_poll_hours`, `schedule.backup_run_time`). После правки `config.yaml` один раз заново выполнить:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_scheduled_task.ps1
```

Если компьютер в назначенное время выключен — пропуск ожидаем, задание выполнится при следующем включении.

Проверить состояние:

```powershell
Get-ScheduledTask -TaskName "CryptoSignalAgent-DeFiLlama" | Get-ScheduledTaskInfo
Get-ScheduledTask -TaskName "CryptoSignalAgent-BinanceOI" | Get-ScheduledTaskInfo
Get-ScheduledTask -TaskName "CryptoSignalAgent-Backup" | Get-ScheduledTaskInfo
```

Запустить прямо сейчас вручную:

```powershell
Start-ScheduledTask -TaskName "CryptoSignalAgent-DeFiLlama"
Start-ScheduledTask -TaskName "CryptoSignalAgent-BinanceOI"
Start-ScheduledTask -TaskName "CryptoSignalAgent-Backup"
```

Логи:

- `logs\scheduler_defillama.log` — задание DeFiLlama.
- `logs\scheduler_binance.log` — задание Binance.
- `logs\backup.log` — задание бэкапа.
- `logs\scheduler.log` — только ручной запуск `main.py` без `--cycle`.
- `logs\backfill.log` — разовый `backfill_history.py`.

Лог всегда объясняет, за что монета получила сигнал, и отдельно различает «истории пока недостаточно» от «данных хватает, но порог не пробит». Если сбор данных за день не удался — лог предупредит отдельной строкой, а не молча оценит по устаревшим данным.

Если задания пропали (переустановка Windows, перенос на другой компьютер) — пересоздать той же командой `register_scheduled_task.ps1` выше.

## Резервные копии базы данных

`storage\db.sqlite` в git не попадает (см. `.gitignore`) — слишком большой и постоянно меняется. `scripts\backup_db.py` использует безопасный способ SQLite снять копию "на лету" (`sqlite3.Connection.backup`), не ломается, даже если в этот момент идёт запись.

```powershell
.venv\Scripts\python.exe scripts\backup_db.py
```

Куда писать копии, задаётся в `config.yaml` (`backup.backup_dir`), а не флагом — иначе задание планировщика (оно запускает скрипт без аргументов) продолжило бы писать в старое место. Сейчас там путь внутри Google Диска — `G:\My Drive\crypto-signal-agent-backups` — копии автоматически попадают в облако, это защищает даже от поломки всего компьютера. Если в `config.yaml` путь не задан, используется локальная `backups\` внутри проекта (не в git) — минус в том, что при утере диска/компьютера пропадут и оригинал, и копии одновременно.

Чтобы поменять место хранения, отредактируйте `backup.backup_dir` в `config.yaml`. Флаг `--backup-dir` при ручном запуске по-прежнему работает и перекрывает config.yaml для одного конкретного запуска:

```powershell
.venv\Scripts\python.exe scripts\backup_db.py --backup-dir "C:\путь\для\разового\теста"
```

Хранятся последние 14 копий (`--keep` меняет это число), старые удаляются автоматически. Лог — `logs\backup.log`.

## Контроль версий (git)

Git — история изменений проекта; каждый коммит — снимок файлов, к которому можно вернуться. Касается только кода (`.py`, `.yaml`, `.md`) — БД, логи и бэкапы в git не попадают, для них раздел выше.

### Сохранить изменения

```powershell
git add .
git commit -m "краткое описание того, что изменилось"
git push
```

`git add .` — отмечает изменения к сохранению. `git commit` — сохраняет снимок локально. `git push` — отправляет на GitHub, отдельно от компьютера.

### Восстановить на новом компьютере

Нужно заранее: Python 3.11+ и git.

```powershell
git clone https://github.com/vimakushin/crypto-signal-agent.git
cd crypto-signal-agent
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

После этого код и `config.yaml` на месте, но:

- **История в БД пустая** — восстановить из бэкапа:

```powershell
copy "backups\db_20260909_080000.sqlite" "storage\db.sqlite"
```

  (подставить имя самой свежей копии).

- Секреты (если появятся — сейчас все источники бесплатны и без ключей) — заново в `.env`, он тоже не в git.
- Задания Планировщика — зарегистрировать заново (`scripts\register_scheduled_task.ps1`), не переносятся вместе с кодом.
