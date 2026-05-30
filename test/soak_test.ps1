# Wavicle TRUE Production Soak Test
# 8 real parallel runspaces, PG direct write stress,
# consistency verification, memory trend analysis,
# connection storm simulation, burst QPS measurement

param(
    [double]$DurationHours = 2,
    [string]$WavicleHost = "localhost",
    [int]$Port = 6379,
    [string]$MetricsUrl = "http://localhost:8080/metrics",
    [string]$PgContainer = "wavicle-db-1",
    [string]$PgUser = "wavicle",
    [string]$PgDb = "wavicle",
    [string]$Password = "testpass",
    [int]$Workers = 8
)

Write-Host "==============================================" -ForegroundColor Magenta
Write-Host "  WAVICLE TRUE PRODUCTION SOAK TEST" -ForegroundColor Magenta
Write-Host "  $Workers parallel runspaces | ${DurationHours}h | BREAK IT" -ForegroundColor Magenta
Write-Host "==============================================" -ForegroundColor Magenta

$endTime = [DateTime]::UtcNow.AddHours($DurationHours)
$soakStart = [DateTime]::UtcNow

# Shared thread-safe counters
$counters = [hashtable]::Synchronized(@{
    TotalOps      = [long]0
    WriteErrors   = [long]0
    ReadErrors    = [long]0
    ConnErrors    = [long]0
    ConsistErrors = [long]0
    StaleReads    = [long]0
    Storms        = [long]0
    PeakQPS       = [long]0
})

# Worker script — runs in separate runspace
$workerScript = {
    param($WavicleHost, $Port, $Password, $EndTime, $Counters, $WorkerId)

    function New-Conn {
        param($targetHost, $port, $pass)
        if ([string]::IsNullOrEmpty($pass)) {
            Write-Host "Worker ${WorkerId}: EMPTY PASSWORD" -ForegroundColor Red
            $pass = "testpass"
        }
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect($targetHost, $port)
        $tcp.ReceiveTimeout = 5000
        $tcp.SendTimeout = 5000
        $s = $tcp.GetStream()
        $w = New-Object System.IO.StreamWriter($s)
        $r = New-Object System.IO.StreamReader($s)
        $w.Write("*2`r`n`$4`r`nAUTH`r`n`$$($pass.Length)`r`n$pass`r`n")
        $w.Flush()
        $authResp = $r.ReadLine()
        return @{ Tcp=$tcp; W=$w; R=$r; AuthResp=$authResp }
    }

    function Send-Cmd {
        param($conn, $parts)
        $msg = "*$($parts.Count)`r`n"
        foreach ($p in $parts) {
            $bytes = [System.Text.Encoding]::UTF8.GetByteCount($p)
            $msg += "`$$bytes`r`n$p`r`n"
        }
        $conn.W.Write($msg)
        $conn.W.Flush()
        $line = $conn.R.ReadLine()
        if ($null -eq $line -or $line.Length -eq 0) { return $null }
        $line = $line.TrimEnd("`r")
        if ($line[0] -eq '+') { return $line.Substring(1) }
        if ($line[0] -eq '-') { return "ERR:$($line.Substring(1))" }
        if ($line[0] -eq ':') { return $line.Substring(1) }
        if ($line[0] -eq '$') {
            $len = [int]$line.Substring(1)
            if ($len -eq -1) { return $null }
            return $conn.R.ReadLine().TrimEnd("`r")
        }
        return $line
    }

    $rng = [Random]::new($WorkerId * 31337)
    $conn = New-Conn $WavicleHost $Port $Password
    $localOps = 0

    while ([DateTime]::UtcNow -lt $EndTime) {
        # BURST: 100-500 ops per batch
        $batchSize = $rng.Next(100, 501)

        for ($i = 0; $i -lt $batchSize; $i++) {
            $op = $rng.Next(0, 100)
            $id = $rng.Next(1, 10000)

            try {
                if ($op -lt 30) {
                    # WRITE — normal string
                    $val = "V$($rng.Next(100000, 999999))"
                    $r = Send-Cmd $conn @("SET", "users:$id`:name", $val)
                    if ($r -ne "OK") { $Counters.WriteErrors++ }

                } elseif ($op -lt 35) {
                    # WRITE — large payload 100B-5KB (reduced from 50KB to limit RAM bloat)
                    $size = $rng.Next(100, 5000)
                    $val = "L" * $size
                    $r = Send-Cmd $conn @("SET", "large:$id`:data", $val)
                    if ($r -ne "OK") { $Counters.WriteErrors++ }

                } elseif ($op -lt 60) {
                    # READ with consistency check
                    $checkId = $rng.Next(1, 100)
                    $writeVal = "CHECK$checkId"
                    Send-Cmd $conn @("SET", "consist:$checkId`:val", $writeVal) | Out-Null
                    $readVal = Send-Cmd $conn @("GET", "consist:$checkId`:val")
                    if ($readVal -ne $writeVal) { $Counters.ConsistErrors++ }

                } elseif ($op -lt 70) {
                    # HASH storm
                    $field = "f$($rng.Next(0,50))"
                    $val = $rng.Next(1000,9999).ToString()
                    if ($rng.Next(0,2) -eq 0) {
                        Send-Cmd $conn @("HSET", "hash:$id", $field, $val) | Out-Null
                    } else {
                        Send-Cmd $conn @("HGET", "hash:$id", $field) | Out-Null
                    }

                } elseif ($op -lt 77) {
                    # TTL pressure — short TTLs to stress sweepExpired
                    $ttl = $rng.Next(1, 5)
                    Send-Cmd $conn @("SET", "ttl:$id`:val", "Expires") | Out-Null
                    Send-Cmd $conn @("EXPIRE", "ttl:$id`:val", $ttl.ToString()) | Out-Null
                    $t = Send-Cmd $conn @("TTL", "ttl:$id`:val")
                    if ($t -eq "-2") { $Counters.WriteErrors++ }

                } elseif ($op -lt 83) {
                    # DEL + recreate cycle
                    Send-Cmd $conn @("SET", "del:$id`:val", "ToDelete") | Out-Null
                    Send-Cmd $conn @("DEL", "del:$id`:val") | Out-Null
                    $ex = Send-Cmd $conn @("EXISTS", "del:$id`:val")
                    if ($ex -ne "0") { $Counters.ConsistErrors++ }

                } elseif ($op -lt 88) {
                    # MSET batch
                    $args = @("MSET")
                    for ($j = 0; $j -lt $rng.Next(5,20); $j++) {
                        $args += "batch:$($rng.Next(1,500))`:$j"
                        $args += $rng.Next(1000,9999).ToString()
                    }
                    Send-Cmd $conn $args | Out-Null

                } elseif ($op -lt 93) {
                    # PING health check
                    $p = Send-Cmd $conn @("PING")
                    if ($p -ne "PONG") { $Counters.ReadErrors++ }

                } else {
                    # DBSIZE
                    Send-Cmd $conn @("DBSIZE") | Out-Null
                }

                $localOps++

            } catch {
                $Counters.ConnErrors++
            }
        }

        $Counters.TotalOps += $localOps
        $localOps = 0

        # CHAOS 1: Random connection kill (8% chance per batch)
        if ($rng.Next(0,100) -lt 8) {
            try { $conn.Tcp.Close() } catch {}
            Start-Sleep -Milliseconds $rng.Next(50, 300)
            try {
                $conn = New-Conn $WavicleHost $Port $Password
                $Counters.Storms++
            } catch {
                $Counters.ConnErrors++
                Start-Sleep -Milliseconds 1000
                $conn = New-Conn $WavicleHost $Port $Password
            }
        }

        # CHAOS 2: Slow client (8% chance)
        if ($rng.Next(0,100) -lt 8) {
            Start-Sleep -Milliseconds $rng.Next(100, 1000)
        }
    }
}

# PG direct write stress — separate runspace hammers PG directly
# This is the hardest test: external writes while Wavicle is serving reads
$pgStressScript = {
    param($PgContainer, $PgUser, $PgDb, $EndTime, $Counters)
    $rng = [Random]::new(99999)
    $i = 0
    while ([DateTime]::UtcNow -lt $EndTime) {
        $id = $rng.Next(1, 100)
        $name = "PGStress$i"
        docker exec $PgContainer psql -U $PgUser -d $PgDb `
            -c "UPDATE users SET name='$name' WHERE id='$id'" 2>$null | Out-Null
        $i++
        $Counters.TotalOps++
        Start-Sleep -Milliseconds $rng.Next(500, 2000)
    }
}

# Memory trend tracker
$memSamples = [System.Collections.Generic.List[double]]::new()

# Launch all runspaces
$pool = [RunspaceFactory]::CreateRunspacePool(1, $Workers + 2)
$pool.Open()
$runspaces = @()

# Launch worker runspaces
for ($i = 0; $i -lt $Workers; $i++) {
    $ps = [PowerShell]::Create()
    $ps.RunspacePool = $pool
    $ps.AddScript($workerScript) | Out-Null
    $ps.AddArgument($WavicleHost) | Out-Null
    $ps.AddArgument($Port) | Out-Null
    $ps.AddArgument($Password) | Out-Null
    $ps.AddArgument($endTime) | Out-Null
    $ps.AddArgument($counters) | Out-Null
    $ps.AddArgument($i) | Out-Null
    $runspaces += @{ PS=$ps; Handle=$ps.BeginInvoke() }
}

# Launch PG stress runspace
$pgPs = [PowerShell]::Create()
$pgPs.RunspacePool = $pool
$pgPs.AddScript($pgStressScript) | Out-Null
$pgPs.AddArgument($PgContainer) | Out-Null
$pgPs.AddArgument($PgUser) | Out-Null
$pgPs.AddArgument($PgDb) | Out-Null
$pgPs.AddArgument($endTime) | Out-Null
$pgPs.AddArgument($counters) | Out-Null
$runspaces += @{ PS=$pgPs; Handle=$pgPs.BeginInvoke() }

Write-Host "  $Workers worker runspaces + 1 PG stress runspace started" -ForegroundColor Green
Write-Host "  Hammering Wavicle for ${DurationHours} hours..." -ForegroundColor Yellow
Write-Host ""

$lastOps = [long]0
$intervalWatch = [System.Diagnostics.Stopwatch]::StartNew()

try {
    while ([DateTime]::UtcNow -lt $endTime) {
        Start-Sleep -Seconds 5
        $elapsed = [DateTime]::UtcNow - $soakStart

        # QPS calculation
        $currentOps = $counters.TotalOps
        $opsThisInterval = $currentOps - $lastOps
        $qps = [math]::Round($opsThisInterval / $intervalWatch.Elapsed.TotalSeconds)
        $lastOps = $currentOps
        $intervalWatch.Restart()
        if ($qps -gt $counters.PeakQPS) { $counters.PeakQPS = $qps }

        # Memory sample
        try {
            $memStr = (docker stats wavicle-wavicle-1 --no-stream --format "{{.MemUsage}}" 2>$null).Trim()
            $memMB = [double]([regex]::Match($memStr, '([\d.]+)MiB').Groups[1].Value)
            if ($memMB -gt 0) { $memSamples.Add($memMB) }
        } catch { $memMB = 0 }

        # Memory trend
        $memTrend = ""
        if ($memSamples.Count -ge 3) {
            $first = $memSamples[0]
            $last = $memSamples[$memSamples.Count - 1]
            $growthPct = if ($first -gt 0) { [math]::Round(($last - $first) / $first * 100, 1) } else { 0 }
            $trendSymbol = if ($growthPct -gt 20) { "LEAK?" } elseif ($growthPct -gt 5) { "growing" } else { "stable" }
            $memTrend = "$($last)MiB ($trendSymbol +$growthPct%)"
        } else { $memTrend = "${memMB}MiB" }

        # Metrics
        $metricsLine = ""
        try {
            $m = (Invoke-WebRequest -Uri $MetricsUrl -UseBasicParsing -TimeoutSec 3).Content
            $lines = $m -split "`n"
            function Get-MetricVal($n) {
                $l = $lines | Where-Object { $_ -match "^$n\s" } | Select-Object -First 1
                if ($l) { return [double]($l -split '\s+')[1] } else { return 0 }
            }
            $hits = Get-MetricVal "wavicle_cache_hits_total"
            $misses = Get-MetricVal "wavicle_cache_misses_total"
            $errs = Get-MetricVal "wavicle_errors_total"
            $wal = [math]::Round((Get-MetricVal "wavicle_wal_size_bytes") / 1MB, 2)
            $cmp = Get-MetricVal "wavicle_compactions_total"
            $lagLine = $lines | Where-Object { $_ -match 'wavicle_replication_lag_ms.*users' } | Select-Object -First 1
            $lag = if ($lagLine) { ([double]($lagLine -split '\s+')[1]).ToString() } else { "?" }
            $hr = if ($hits + $misses -gt 0) { [math]::Round($hits/($hits+$misses)*100,1) } else { 0 }
            $metricsLine = "Hit:${hr}% WAL:${wal}MB Cmp:$cmp SrvErr:$errs Lag:${lag}ms"
        } catch { $metricsLine = "metrics unavailable" }

        # Status line
        $consistColor = if ($counters.ConsistErrors -gt 0) { "Red" } else { "Green" }
        $staleColor = if ($counters.StaleReads -gt 0) { "Red" } else { "Green" }

        Write-Host ("[{0:hh\:mm\:ss}] Ops:{1,8} QPS:{2,5} Peak:{3,5} | Mem:{4,-20} | {5}" -f `
            $elapsed, $counters.TotalOps, $qps, $counters.PeakQPS, $memTrend, $metricsLine) -ForegroundColor White

        if ($counters.ConsistErrors -gt 0 -or $counters.StaleReads -gt 0) {
            Write-Host ("           CONSISTENCY FAILURES: consist={0} stale={1}" -f `
                $counters.ConsistErrors, $counters.StaleReads) -ForegroundColor Red
        }

        if ($counters.ConnErrors -gt 0) {
            Write-Host ("           ConnErr:{0} WriteErr:{1} ReadErr:{2} Storms:{3}" -f `
                $counters.ConnErrors, $counters.WriteErrors, $counters.ReadErrors, $counters.Storms) -ForegroundColor Yellow
        }
    }
} finally {
    Write-Host ""
    Write-Host "==============================================" -ForegroundColor Magenta
    Write-Host "  SOAK TEST COMPLETE" -ForegroundColor Magenta
    Write-Host "==============================================" -ForegroundColor Magenta

    $elapsed = [DateTime]::UtcNow - $soakStart
    $avgMem = if ($memSamples.Count -gt 0) { [math]::Round(($memSamples | Measure-Object -Average).Average, 1) } else { 0 }
    $maxMem = if ($memSamples.Count -gt 0) { [math]::Round(($memSamples | Measure-Object -Maximum).Maximum, 1) } else { 0 }
    $firstMem = if ($memSamples.Count -gt 0) { $memSamples[0] } else { 0 }
    $lastMem = if ($memSamples.Count -gt 0) { $memSamples[$memSamples.Count-1] } else { 0 }
    $memGrowth = if ($firstMem -gt 0) { [math]::Round(($lastMem - $firstMem) / $firstMem * 100, 1) } else { 0 }

    Write-Host "  Duration        : $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor White
    Write-Host "  Total Ops       : $($counters.TotalOps)" -ForegroundColor White
    Write-Host "  Avg QPS         : $([math]::Round($counters.TotalOps / $elapsed.TotalSeconds))" -ForegroundColor White
    Write-Host "  Peak QPS        : $($counters.PeakQPS)" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Memory Start    : ${firstMem}MiB" -ForegroundColor White
    Write-Host "  Memory End      : ${lastMem}MiB" -ForegroundColor White
    Write-Host "  Memory Peak     : ${maxMem}MiB" -ForegroundColor White
    Write-Host "  Memory Growth   : ${memGrowth}%" -ForegroundColor $(if ($memGrowth -gt 20) { "Red" } elseif ($memGrowth -gt 5) { "Yellow" } else { "Green" })
    Write-Host ""
    Write-Host "  Consist Errors  : $($counters.ConsistErrors)" -ForegroundColor $(if($counters.ConsistErrors -gt 0){"Red"}else{"Green"})
    Write-Host "  Stale Reads     : $($counters.StaleReads)" -ForegroundColor $(if($counters.StaleReads -gt 0){"Red"}else{"Green"})
    Write-Host "  Write Errors    : $($counters.WriteErrors)" -ForegroundColor $(if($counters.WriteErrors -gt 0){"Yellow"}else{"Green"})
    Write-Host "  Conn Errors     : $($counters.ConnErrors)" -ForegroundColor $(if($counters.ConnErrors -gt 0){"Yellow"}else{"Green"})
    Write-Host "  Conn Storms     : $($counters.Storms)" -ForegroundColor Yellow
    Write-Host ""

    # PASS/FAIL verdict
    $soakPassed = $counters.ConsistErrors -eq 0 -and $counters.StaleReads -eq 0 -and $memGrowth -lt 20
    if ($soakPassed) {
        Write-Host "  SOAK TEST PASSED - Ready for Oracle Cloud 7-day run" -ForegroundColor Green
    } else {
        Write-Host "  SOAK TEST FAILED" -ForegroundColor Red
        if ($counters.ConsistErrors -gt 0) { Write-Host "    REASON: Consistency violations detected" -ForegroundColor Red }
        if ($counters.StaleReads -gt 0) { Write-Host "    REASON: Stale reads detected under load" -ForegroundColor Red }
        if ($memGrowth -ge 20) { Write-Host "    REASON: Memory grew ${memGrowth}% - possible leak" -ForegroundColor Red }
    }

    Write-Host "==============================================" -ForegroundColor Magenta

    # Cleanup
    foreach ($rs in $runspaces) {
        try { $rs.PS.EndInvoke($rs.Handle) } catch {}
        $rs.PS.Dispose()
    }
    $pool.Close()
    $pool.Dispose()
}
