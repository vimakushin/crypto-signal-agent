# Registers TWO Windows Task Scheduler tasks for crypto-signal-agent
# (README.md has the plain-language explanation):
#   - CryptoSignalAgent-DeFiLlama: `main.py --cycle defillama`, once a day,
#     at schedule.defillama_run_time from config.yaml (TZ section 5 calls
#     revenue/fees a "slow" signal - daily polling is enough).
#   - CryptoSignalAgent-BinanceOI: `main.py --cycle binance`, repeating every
#     schedule.binance_futures_poll_hours hours around the clock (TZ section
#     5 calls Open Interest a "fast" signal that needs polling every 4-6h,
#     not once a day).
#
# Run this ONCE, from PowerShell, as the Windows user who will normally be
# logged in on this PC (both tasks only run while that user is logged on -
# neither wakes the PC up or runs while it is off/asleep, see README.md):
#
#   powershell -ExecutionPolicy Bypass -File scripts\register_scheduled_task.ps1
#
# Safe to re-run: it replaces both task definitions (-Force) instead of
# creating duplicates. Also re-run this after changing
# schedule.defillama_run_time or schedule.binance_futures_poll_hours in
# config.yaml, to apply the new schedule.

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe   = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ConfigPath  = Join-Path $ProjectRoot "config.yaml"
$DeFiLlamaTaskName = "CryptoSignalAgent-DeFiLlama"
$BinanceTaskName   = "CryptoSignalAgent-BinanceOI"
# Older, pre-split task name that ran both cycles together once a day -
# removed if present so a machine that registered it before this script was
# updated doesn't end up running the old all-in-one task AND both new ones.
$LegacyTaskName = "CryptoSignalAgent-DailyRun"

if (-not (Test-Path $PythonExe)) {
    throw "venv python not found at $PythonExe - set up the project's .venv first (see README.md)."
}
if (-not (Test-Path $ConfigPath)) {
    throw "config.yaml not found at $ConfigPath"
}

# Read both schedule values from config.yaml (schedule.defillama_run_time,
# schedule.binance_futures_poll_hours) instead of hardcoding them here -
# schedule/thresholds/watchlist live in config.yaml, not in code (CLAUDE.md
# project rule). Uses the project's own venv Python + PyYAML (already a
# dependency, see requirements.txt) rather than adding a separate
# PowerShell YAML module just for this.
#
# The snippet is written to a temp .py file and run as `python file.py`,
# NOT passed inline via `python -c "..."`. PowerShell mangles double quotes
# inside an argument string when it hands it to a native exe (e.g.
# encoding="utf-8" arrives as encoding=utf-8, which Python then parses as
# `utf - 8` and fails with NameError) - a temp file sidesteps that quoting
# entirely, regardless of what the snippet itself contains.
$readConfigScript = @'
import sys
import yaml
with open(sys.argv[1], encoding="utf-8") as f:
    cfg = yaml.safe_load(f)
print(cfg["schedule"]["defillama_run_time"])
print(cfg["schedule"]["binance_futures_poll_hours"])
'@
$tempScriptPath = Join-Path $env:TEMP "crypto-signal-agent_read-schedule.py"
Set-Content -Path $tempScriptPath -Value $readConfigScript -Encoding utf8
try {
    $ConfigOutput = & $PythonExe $tempScriptPath $ConfigPath
    if ($LASTEXITCODE -ne 0 -or $ConfigOutput.Count -lt 2) {
        throw "Could not read schedule.defillama_run_time / schedule.binance_futures_poll_hours from $ConfigPath"
    }
} finally {
    Remove-Item -Path $tempScriptPath -ErrorAction SilentlyContinue
}
$RunTimeRaw = $ConfigOutput[0].Trim()
$PollHoursRaw = $ConfigOutput[1].Trim()

$timeParts = $RunTimeRaw -split ":"
if ($timeParts.Count -ne 2) {
    throw "schedule.defillama_run_time in config.yaml must look like `"HH:MM`" (24h), got '$RunTimeRaw'"
}
$RunHour = [int]$timeParts[0]
$RunMinute = [int]$timeParts[1]
if ($RunHour -lt 0 -or $RunHour -gt 23 -or $RunMinute -lt 0 -or $RunMinute -gt 59) {
    throw "schedule.defillama_run_time in config.yaml is out of range: '$RunTimeRaw'"
}

$PollHours = 0
if (-not [int]::TryParse($PollHoursRaw, [ref]$PollHours) -or $PollHours -le 0 -or $PollHours -gt 24) {
    throw "schedule.binance_futures_poll_hours in config.yaml must be a whole number between 1 and 24, got '$PollHoursRaw'"
}

# Built from integer Hour/Minute, not an "8:00AM"-style string - -At needs an
# actual DateTime, and parsing "HH:MMAM/PM" text depends on the Windows
# locale/culture. That breaks silently on a machine with a different locale,
# which is exactly when this script is most likely to be re-run (fresh
# Windows install, different PC) - see README.md.
$RunTime = Get-Date -Hour $RunHour -Minute $RunMinute -Second 0

# Anchor for the repeating Binance trigger - midnight today. Only its
# time-of-day matters (it fixes which clock hours the repeating trigger
# lands on, e.g. anchoring at 00:00 with a 6h interval fires at
# 00:00/06:00/12:00/18:00); the date itself is irrelevant once the
# repetition pattern is registered, and -StartWhenAvailable below covers a
# PC that's off/asleep at the exact moment a repetition was due.
$BinanceAnchorTime = Get-Date -Hour 0 -Minute 0 -Second 0

# NOTE: Task Scheduler must call python.exe through cmd.exe /c, not directly.
# Calling the venv's python.exe as the task's own -Execute target hangs
# forever (0% CPU, no network) when launched by Task Scheduler outside an
# interactive session - a known Windows issue with venv-python.exe launchers
# inheriting console handles in that mode. The cmd.exe wrapper avoids it.
# Verified working (LastTaskResult=0, clean log, fresh SQLite row) with this
# wrapper; do not "simplify" this back to a direct python.exe call.
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

if (Get-ScheduledTask -TaskName $LegacyTaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $LegacyTaskName -Confirm:$false
    Write-Host "Removed legacy task '$LegacyTaskName' (replaced by '$DeFiLlamaTaskName' + '$BinanceTaskName')."
}

$defillamaAction = New-ScheduledTaskAction -Execute "cmd.exe" `
    -Argument '/c ".venv\Scripts\python.exe" main.py --cycle defillama' `
    -WorkingDirectory $ProjectRoot
$defillamaTrigger = New-ScheduledTaskTrigger -Daily -At $RunTime

Register-ScheduledTask -TaskName $DeFiLlamaTaskName `
    -Action $defillamaAction `
    -Trigger $defillamaTrigger `
    -Settings $settings `
    -Description "crypto-signal-agent: daily DeFiLlama snapshot + revenue_price_gap signal scan (see README.md)" `
    -Force | Out-Null

$binanceAction = New-ScheduledTaskAction -Execute "cmd.exe" `
    -Argument '/c ".venv\Scripts\python.exe" main.py --cycle binance' `
    -WorkingDirectory $ProjectRoot
$binanceTrigger = New-ScheduledTaskTrigger -Once -At $BinanceAnchorTime `
    -RepetitionInterval (New-TimeSpan -Hours $PollHours)
# Deliberately NOT passing -RepetitionDuration ([TimeSpan]::MaxValue) here:
# PowerShell serializes that TimeSpan as the ISO-8601 duration
# "P99999999DT23H59M59S", which Task Scheduler's own XML schema rejects
# outright ("value ... incorrectly formatted or out of range") - confirmed
# live. The documented way to make a repeating trigger run forever is to
# leave Repetition.Duration EMPTY (not omitted, not MaxValue) while
# Repetition.Interval is set - an empty Duration with a non-empty Interval
# means "repeat indefinitely" in Task Scheduler's own schema. That can't be
# expressed through New-ScheduledTaskTrigger's parameters directly, so it's
# set on the trigger object after creation instead.
$binanceTrigger.Repetition.Duration = ""

Register-ScheduledTask -TaskName $BinanceTaskName `
    -Action $binanceAction `
    -Trigger $binanceTrigger `
    -Settings $settings `
    -Description "crypto-signal-agent: Binance Futures OI snapshot + oi_divergence signal scan, every $PollHours hour(s) (see README.md)" `
    -Force | Out-Null

Write-Host "Scheduled task '$DeFiLlamaTaskName' registered - runs daily at $RunTimeRaw while this Windows user is logged in."
Write-Host "Scheduled task '$BinanceTaskName' registered - runs every $PollHours hour(s) while this Windows user is logged in."
Write-Host "Log files: $ProjectRoot\logs\scheduler_defillama.log (DeFiLlama task), $ProjectRoot\logs\scheduler_binance.log (BinanceOI task)"
Write-Host "(separate files on purpose - these two tasks are independent processes that can run at the same time, see main.py's module docstring)"
Write-Host ""
Write-Host "To check either one: Get-ScheduledTask -TaskName '$DeFiLlamaTaskName' | Get-ScheduledTaskInfo"
Write-Host "                     Get-ScheduledTask -TaskName '$BinanceTaskName' | Get-ScheduledTaskInfo"
Write-Host "To run either right now (test): Start-ScheduledTask -TaskName '$DeFiLlamaTaskName'"
Write-Host "                               Start-ScheduledTask -TaskName '$BinanceTaskName'"
