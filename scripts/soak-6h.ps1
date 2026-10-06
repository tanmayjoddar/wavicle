# soak-6h.ps1 — detached 6-hour soak wrapper (file-based: inline $env assignments
# get mangled in transit, so env is set in here, not on the command line).
# Ends ~04:25 local. Artifacts: docc/shadow-evidence/soak-6h.csv + soak-6h.log
#
# LESSON 2026-09-29: `go test` kills binaries at a DEFAULT 10-minute timeout.
# A 6-hour run MUST pass -timeout explicitly, or the harness — not the product —
# dies at 10m02s. That is exactly what happened on the first overnight attempt.
# Default 6h; override per-run: -SoakDuration 1h
param([string]$SoakDuration = '6h')
$env:SOAK_DURATION = $SoakDuration
$env:SOAK_SAMPLE_FILE = 'D:\wavicle\docc\shadow-evidence\soak-6h.csv'
Set-Location 'D:\wavicle'
go test -tags=soak -timeout 7h -run 'TestSoak_FrontierCache_2min' ./benchmarks/ -count=1 -v > 'D:\wavicle\docc\shadow-evidence\soak-6h.log' 2>&1
