# parsecheck.ps1 - syntax-check a PowerShell file without executing it.
# Usage: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/parsecheck.ps1 -Path <file>
# Exit 0 + PARSE-OK on clean parse; lists every error otherwise.
param([string]$Path = "D:\wavicle\scripts\dryrun-10min.ps1")
$src = [IO.File]::ReadAllText($Path)
$errs = $null
[void][System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$null, [ref]$errs)
if ($errs.Count -eq 0) {
  Write-Output "PARSE-OK: $Path"
} else {
  foreach ($e in $errs) { Write-Output ("PARSE-ERROR line " + $e.Extent.StartLineNumber + ": " + $e.Message) }
  exit 1
}
