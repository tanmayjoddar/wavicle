# Dual-writer for shadow runs: writes IDENTICAL values to two instances so the
# shadow runner compares converged data (mismatches then mean real drift, a
# kill window, or CDC race — not writer skew).
# Usage: powershell -File scripts/shadow-write.ps1 -SrcPort 6379 -DstPort 6380 -Keys 200
param(
  [int]$SrcPort = 6379,
  [int]$DstPort = 6380,
  [int]$Keys = 200,
  [int]$IntervalMs = 200,
  [string]$Cli = "D:\wavicle\wavicle-cli.exe"
)
$i = 0
while ($true) {
  $i++
  $k = "shadow:" + ((($i * 7919) % $Keys) + 1)
  $v = "live-$i"
  & $Cli -p $SrcPort SET $k $v | Out-Null
  & $Cli -p $DstPort SET $k $v | Out-Null
  Start-Sleep -Milliseconds $IntervalMs
}
