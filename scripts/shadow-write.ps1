# Dual-writer for shadow runs: writes IDENTICAL values to two instances so the
# shadow runner compares converged data (mismatches then mean real drift, a
# kill window, or CDC race - not writer skew).
# Usage: powershell -File scripts/shadow-write.ps1 -SrcPort 6379 -DstPort 6381 -Keys 200
# PID lock (-PidFile): refuses a second start while the recorded PID is alive
# (Get-Process check, no syscalls); stale pidfiles are taken over loudly.
# -DurationMin 0 (default) runs forever; pass N to self-exit after N minutes.
# Heartbeat: appends "alive i=N" every HeartbeatEvery iterations to HeartbeatPath
# (empty = disabled). If the writer dies, the watchdog - not silence - reports it.
param(
  [int]$SrcPort = 6379,
  [int]$DstPort = 6381,
  [int]$Keys = 200,
  [int]$IntervalMs = 200,
  [string]$Prefix = "shadow",
  [string]$Cli = "D:\wavicle\wavicle-cli.exe",
  [string]$HeartbeatPath = "",
  [int]$HeartbeatEvery = 25,
  [string]$PidFile = "",
  [int]$DurationMin = 0
)
if ($PidFile -ne "") {
  $existing = Get-Content -LiteralPath $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($existing -match '^\d+$') {
    $old = Get-Process -Id ([int]$existing) -ErrorAction SilentlyContinue
    if ($null -ne $old) {
      Write-Output "REFUSING TO START: writer pid $($old.Id) ($($old.Name)) still alive per $PidFile - kill it or use a different pidfile"
      exit 1
    }
    Write-Output "TAKEOVER stale writer pidfile $PidFile (pid $existing dead) - previous writer crashed or was killed"
  }
  [string]$PID | Out-File -LiteralPath $PidFile -Encoding ascii -NoNewline
}
$deadline = [DateTime]::MaxValue
if ($DurationMin -gt 0) { $deadline = (Get-Date).AddMinutes($DurationMin) }
$i = 0
while ((Get-Date) -lt $deadline) {
  $i++
  $k = "${Prefix}:" + ((($i * 7919) % $Keys) + 1)
  $v = "live-$i"
  & $Cli -p $SrcPort SET $k $v | Out-Null
  & $Cli -p $DstPort SET $k $v | Out-Null
  if ($HeartbeatPath -ne "" -and ($i % $HeartbeatEvery) -eq 0) {
    "{0:yyyy-MM-ddTHH:mm:ssZ} alive i={1} key={2}" -f [DateTime]::UtcNow, $i, $k |
      Out-File -FilePath $HeartbeatPath -Append -Encoding ascii
  }
  Start-Sleep -Milliseconds $IntervalMs
}
if ($PidFile -ne "" -and (Test-Path -LiteralPath $PidFile)) {
  Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
}
