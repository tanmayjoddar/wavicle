# dryrun-10min.ps1 - self-contained 10-minute dry run. Review first, run only on approval.
# Covers: servers via env (NO -p flag exists on the server binary), separate data
# dirs, seed, dual-writer, shadow continuous, soak, watchdog. Default 10 minutes;
# pass -DurationMin for other lengths. Exits non-zero on any setup failure.
param(
  [int]$DurationMin = 10,
  [int]$SeedKeys = 50,
  [int]$SrcPort = 6379,
  [int]$DstPort = 6381
)

$ErrorActionPreference = "Stop"
$RunId  = Get-Date -Format "yyyyMMdd-HHmmss"
$RunDir = "D:\wavicle\docc\shadow-evidence\dryrun-$RunId"
New-Item -ItemType Directory -Path $RunDir -Force | Out-Null
function Log-Event([string]$m) {
  "{0:yyyy-MM-ddTHH:mm:ssZ} | {1}" -f [DateTime]::UtcNow, $m | Tee-Object -FilePath "$RunDir\events.log" -Append | Out-Null
}
Log-Event "RUN START id=$RunId duration_min=$DurationMin"

function Test-Port([int]$Port) {
  try { $t = New-Object Net.Sockets.TcpClient; $t.Connect("127.0.0.1", $Port); $t.Close(); return $true }
  catch { return $false }
}

# 0. Ports must be FREE - never kill anything silently, fail loudly instead.
foreach ($p in @($SrcPort, $DstPort)) {
  if (Test-Port $p) { Log-Event "FATAL port $p busy - clear it yourself, refusing to kill"; exit 1 }
}
Log-Event "ports $SrcPort,$DstPort free"

# 0b. Orphaned writers have NO listening port, so the check above is blind to
# them (root cause of the 3-writer pileup: every relaunch stacked another one).
# This script's own writers are identifiable by command line - stop exactly
# those, loudly, and refuse to proceed if the stop fails.
$orphans = Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -like "*shadow-write.ps1*" }
foreach ($o in $orphans) {
  Stop-Process -Id $o.ProcessId -Force -ErrorAction Stop
  Log-Event "cleared orphaned writer pid=$($o.ProcessId)"
}
if (@($orphans).Count -eq 0) { Log-Event "no orphaned writers found" }

# 1. Servers: env vars ONLY (the binary has no -p flag), separate data dirs.
$srcDir = "$RunDir\src-data"; $dstDir = "$RunDir\dst-data"
New-Item -ItemType Directory -Path $srcDir, $dstDir -Force | Out-Null
$env:WAVICLE_SERVER_LISTEN = "127.0.0.1:$SrcPort"; $env:WAVICLE_STORAGE_DATA_DIR = $srcDir
$srcProc = Start-Process -FilePath "D:\wavicle\wavicle.exe" -WorkingDirectory "D:\wavicle" -PassThru `
  -RedirectStandardOutput "$RunDir\src-stdout.log" -RedirectStandardError "$RunDir\src-stderr.log"
Remove-Item Env:\WAVICLE_SERVER_LISTEN; Remove-Item Env:\WAVICLE_STORAGE_DATA_DIR
$env:WAVICLE_SERVER_LISTEN = "127.0.0.1:$DstPort"; $env:WAVICLE_STORAGE_DATA_DIR = $dstDir
$dstProc = Start-Process -FilePath "D:\wavicle\wavicle.exe" -WorkingDirectory "D:\wavicle" -PassThru `
  -RedirectStandardOutput "$RunDir\dst-stdout.log" -RedirectStandardError "$RunDir\dst-stderr.log"
Remove-Item Env:\WAVICLE_SERVER_LISTEN; Remove-Item Env:\WAVICLE_STORAGE_DATA_DIR
Log-Event "source pid=$($srcProc.Id) target pid=$($dstProc.Id) (env-configured, separate data dirs)"
$deadline = (Get-Date).AddSeconds(60)
while (-not ((Test-Port $SrcPort) -and (Test-Port $DstPort))) {
  if ((Get-Date) -gt $deadline) { Log-Event "FATAL servers never bound ports"; exit 1 }
  Start-Sleep -Seconds 2
}
Log-Event "both ports accepting (PIDs verified via Get-NetTCPConnection by operator)"

# 2. Seed identically on both sides (wavicle-cli -p is the CLIENT port flag - legitimate).
for ($i = 1; $i -le $SeedKeys; $i++) {
  & "D:\wavicle\wavicle-cli.exe" -p $SrcPort SET "dry:$i" "seed-$i" | Out-Null
  & "D:\wavicle\wavicle-cli.exe" -p $DstPort SET "dry:$i" "seed-$i" | Out-Null
}
Log-Event "seeded $SeedKeys keys on both sides"

# 3. Dual-writer (same values both sides, SAME dry:* keyspace the shadow
#    compares, so sweeps exercise live traffic) + shadow comparer, detached.
#    Writer stdout/stderr captured: if it dies again, its last error is on disk,
#    not inferred from absence.
Start-Process -FilePath "powershell.exe" `
  -ArgumentList @("-NoProfile","-ExecutionPolicy","Bypass","-File","D:\wavicle\scripts\shadow-write.ps1","-SrcPort","$SrcPort","-DstPort","$DstPort","-Prefix","dry","-HeartbeatPath","$RunDir\writer-heartbeat.log","-PidFile","$RunDir\writer.pid","-DurationMin","$($DurationMin + 2)") `
  -WorkingDirectory "D:\wavicle" `
  -RedirectStandardOutput "$RunDir\writer-stdout.log" -RedirectStandardError "$RunDir\writer-stderr.log"
Log-Event "dual-writer started (keys dry:*, pidfile $RunDir\writer.pid, self-exits after $($DurationMin + 2)m)"
Start-Process -FilePath "D:\wavicle\wavicle-migrate.exe" -ArgumentList @(
  "-source","localhost:$SrcPort","-target","localhost:$DstPort",
  "-pattern","dry:*","-count","500",
  "-continuous","-interval","30s","-duration","${DurationMin}m",
  "-get-timeout","50ms","-sweep-timeout","90s",
  "-heartbeat","$RunDir\heartbeat.log",
  "-log","$RunDir\mismatch.jsonl","-report","$RunDir\report.json"
  ) -WorkingDirectory "D:\wavicle" -RedirectStandardOutput "$RunDir\shadow-stdout.log"
Log-Event "shadow started (pattern dry:*, interval 30s, sweep-timeout 90s)"

# 4. Soak in-process, explicit duration (never the 15s default, never past go's
#    10m default without -timeout: soak-6h.ps1 passes -timeout 7h always).
$soakMin = $DurationMin
Start-Process -FilePath "powershell.exe" -ArgumentList @(
  "-NoProfile","-ExecutionPolicy","Bypass","-File","D:\wavicle\scripts\soak-6h.ps1",
  "-SoakDuration","${soakMin}m") -WorkingDirectory "D:\wavicle"
Log-Event "soak started (SOAK_DURATION=${soakMin}m, go -timeout 7h)"

# 5. Watchdog: heartbeat, liveness, writer-heartbeat, resources. Failures are
#    LOUD (events.log) and sticky-notified once; nothing restarts anything.
$start = Get-Date; $end = $start.AddMinutes($DurationMin)
$lastBeat = $start; $lastLive = $start; $lastRes = $start; $lastWriter = $start
$writerNotified = $false
"ts,elapsed_s,pid,name,cpu_s,workingset" | Out-File -FilePath "$RunDir\resources.csv" -Encoding ascii
while ((Get-Date) -lt $end) {
  $now = Get-Date; $el = [int](($now - $start).TotalSeconds)
  if (($now - $lastBeat).TotalSeconds -ge 60) {
    "$now | t+${el}s | watchdog alive" | Out-File -FilePath "$RunDir\watchdog-heartbeat.log" -Append
    $lastBeat = $now
  }
  if (($now - $lastLive).TotalSeconds -ge 120) {
    $sAlive = Test-Port $SrcPort; $tAlive = Test-Port $DstPort
    "$now | t+${el}s | source_alive=$sAlive target_alive=$tAlive" | Out-File -FilePath "$RunDir\liveness.log" -Append
    if (-not $sAlive) { Log-Event "SOURCE DOWN at t+${el}s - manual restart required (no silent kills, no silent restarts)" }
    if (-not $tAlive) { Log-Event "TARGET DOWN at t+${el}s - manual restart required (no silent kills, no silent restarts)" }
    $lastLive = $now
  }
  if (($now - $lastWriter).TotalSeconds -ge 60) {
    $whb = Get-Content "$RunDir\writer-heartbeat.log" -ErrorAction SilentlyContinue | Select-Object -Last 1
    "$now | t+${el}s | writer_last=$whb" | Out-File -FilePath "$RunDir\liveness.log" -Append
    # Freshness, not mere existence: a dead writer leaves a stale file behind.
    $wAge = [double]::PositiveInfinity
    if (-not [string]::IsNullOrWhiteSpace($whb)) {
      try { $wAge = ($now - [DateTime]::Parse($whb.Substring(0, 20))).TotalSeconds } catch { $wAge = [double]::PositiveInfinity }
    }
    if ($wAge -gt 120 -and $el -gt 150 -and -not $writerNotified) {
      Log-Event "WRITER HEARTBEAT STALE ($([int]$wAge)s) at t+${el}s - writer dead or stuck; run is comparing static data, treat match rate accordingly"
      $writerNotified = $true
    }
    $lastWriter = $now
  }
  if (($now - $lastRes).TotalSeconds -ge 300) {
    Get-Process -Name "wavicle*" -ErrorAction SilentlyContinue | ForEach-Object {
      "$now,$el,$($_.Id),$($_.Name),$($_.CPU),$($_.WorkingSet64)" | Out-File -FilePath "$RunDir\resources.csv" -Append
    }
    $lastRes = $now
  }
  Start-Sleep -Seconds 10
}
Log-Event "DURATION REACHED - stopping writer via pidfile, servers left running for inspection (kill manually)"
$wPid = Get-Content -LiteralPath "$RunDir\writer.pid" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($wPid -match '^\d+$') {
  Stop-Process -Id ([int]$wPid) -Force -ErrorAction SilentlyContinue
  Log-Event "writer pid=$wPid stopped"
} else {
  Log-Event "writer pidfile missing/unreadable at shutdown (writer self-exited as designed)"
}
Write-Host "=== DRY RUN COMPLETE: $RunDir ==="
Get-ChildItem $RunDir | Format-Table Name, Length, LastWriteTime -AutoSize
