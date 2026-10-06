# keepawake.ps1 — belt-and-suspenders sleep blocker for long test runs.
# Calls SetThreadExecutionState (ES_CONTINUOUS | ES_SYSTEM_REQUIRED |
# ES_AWAYMODE_REQUIRED) in a loop so idle timers never fire, even if a power
# scheme changes mid-run. No admin needed. Stop it when the run ends:
#   Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" |
#     Where-Object { $_.CommandLine -like "*keepawake*" } |
#     ForEach-Object { taskkill /F /PID $_.ProcessId }
$sig = @"
using System;
using System.Runtime.InteropServices;
public static class StayAwake {
  [DllImport("kernel32.dll")]
  public static extern uint SetThreadExecutionState(uint esFlags);
}
"@
Add-Type -TypeDefinition $sig | Out-Null
$ES_CONTINUOUS = 0x80000000
$ES_SYSTEM_REQUIRED = 0x00000001
$ES_AWAYMODE_REQUIRED = 0x00000040
while ($true) {
  [StayAwake]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED -bor $ES_AWAYMODE_REQUIRED) | Out-Null
  Start-Sleep -Seconds 50
}
