# Wavicle TRUE Production Soak Test
# 8 real parallel runspaces, PG direct write stress,
# consistency verification, memory trend analysis,
# connection storm simulation, burst QPS measurement
param(
    [double]$DurationHours = 0.034,
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
Write-Host "  WAVICLE PRODUCTION SOAK TEST" -ForegroundColor Magenta
Write-Host "  $Workers workers | ${DurationHours}h | PG→replication→GET" -ForegroundColor Magenta
Write-Host "==============================================" -ForegroundColor Magenta

$endTime = [DateTime]::UtcNow.AddHours($DurationHours)
$soakStart = [DateTime]::UtcNow

$counters = [hashtable]::Synchronized(@{
    TotalOps      = [long]0
    WriteErrors   = [long]0
    ReadErrors    = [long]0
    ConnErrors    = [long]0
    StaleReads    = [long]0
    Storms        = [long]0
    PeakQPS       = [long]0
    PGWrites      = [long]0
    PGVerified    = [long]0
})

# Worker script — realistic read-heavy production load
$workerScript = {
    param($WavicleHost, $Port, $Password, $EndTime, $Counters, $WorkerId)

    function New-Conn {
        param($targetHost, $port, $pass)
        if ([string]::IsNullOrEmpty($pass)) { $pass = "testpass" }
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
        $batchSize = $rng.Next(100, 501)

        for ($i = 0; $i -lt $batchSize; $i++) {
            $op = $rng.Next(0, 100)
            $id = $rng.Next(1, 9000)

            try {
                if ($op -lt 70) {
                    # READ — production workload is read-heavy
                    $r = Send-Cmd $conn @("GET", "users:$id`:name")
                    if ($null -eq $r) { $Counters.ReadErrors++ }

                } elseif ($op -lt 85) {
                    # WRITE — normal string
                    $val = "V$($rng.Next(100000, 999999))"
                    $r = Send-Cmd $conn @("SET", "users:$id`:name", $val)
                    if ($r -ne "OK") { $Counters.WriteErrors++ }

                } elseif ($op -lt 95) {
                    # HASH operations
                    $field = "f$($rng.Next(0,20))"
                    $val = $rng.Next(1000,9999).ToString()
                    if ($rng.Next(0,2) -eq 0) {
                        Send-Cmd $conn @("HSET", "profile:$id", $field, $val) | Out-Null
                    } else {
                        Send-Cmd $conn @("HGET", "profile:$id", $field) | Out-Null
                    }

                } else {
                    # HEALTH
                    $p = Send-Cmd $conn @("PING")
                    if ($p -ne "PONG") { $Counters.ReadErrors++ }
                }

                $localOps++

            } catch {
                $Counters.ConnErrors++
            }
        }

        $Counters.TotalOps += $localOps
        $localOps = 0

        # CHAOS: Random connection kill (8% chance per batch)
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

        # CHAOS: Slow client (8% chance)
        if ($rng.Next(0,100) -lt 8) {
            Start-Sleep -Milliseconds $rng.Next(100, 1000)
        }
    }
}

# PG stress runspace — continuous external writes on IDs 1-100
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
        $Counters.PGWrites++
        $Counters.TotalOps++
        Start-Sleep -Milliseconds $rng.Next(500, 2000)
    }
}

# Stale read detector — separate runspace, no QPS impact
$staleCheckScript = {
    param($WavicleHost, $Port, $Password, $PgContainer, $PgUser, $PgDb, $EndTime, $Counters)

    function New-Conn {
        param($targetHost, $port, $pass)
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect($targetHost, $port)
        $tcp.ReceiveTimeout = 3000
        $s = $tcp.GetStream()
        $w = New-Object System.IO.StreamWriter($s)
        $r = New-Object System.IO.StreamReader($s)
        $w.Write("*2`r`n`$4`r`nAUTH`r`n`$$($pass.Length)`r`n$pass`r`n")
        $w.Flush()
        $r.ReadLine() | Out-Null
        return @{ Tcp=$tcp; W=$w; R=$r }
    }

    function Send-Cmd {
        param($conn, $parts)
        $msg = "*$($parts.Count)`r`n"
        foreach ($p in $parts) {
            $bytes = [System.Text.Encoding]::UTF8.GetByteCount($p)
            $msg += "`$$bytes`r`n$p`r`n"
        }
        $conn.W.Write($msg); $conn.W.Flush()
        $line = $conn.R.ReadLine()
        if ($null -eq $line -or $line.Length -eq 0) { return $null }
        $line = $line.TrimEnd("`r")
        if ($line[0] -eq '+') { return $line.Substring(1) }
        if ($line[0] -eq '-') { return "ERR" }
        if ($line[0] -eq ':') { return $line.Substring(1) }
        if ($line[0] -eq '$') {
            $len = [int]$line.Substring(1)
            if ($len -eq -1) { return $null }
            return $conn.R.ReadLine().TrimEnd("`r")
        }
        return $line
    }

    $conn = New-Conn $WavicleHost $Port $Password
    $rng = [Random]::new(42)
    $checkId = 0

    while ([DateTime]::UtcNow -lt $EndTime) {
        $checkId++
        # Use unique ID range (9001-9100) — guaranteed collision-free with workers (1-9000) and PG stress (1-100)
        $pgId = $rng.Next(9001, 9100)
        $pgVal = "stale_check_${checkId}_$([DateTime]::UtcNow.Ticks)"

        # Step 1: Write DIRECTLY to PostgreSQL
        docker exec $PgContainer psql -U $PgUser -d $PgDb `
            -c "INSERT INTO users (id, name, email, created_at) VALUES ($pgId, '$pgVal', 'stale@test.com', NOW()) ON CONFLICT (id) DO UPDATE SET name='$pgVal'" 2>$null | Out-Null
        $Counters.PGWrites++

        # Step 2: Wait for replication (500ms = safe margin)
        Start-Sleep -Milliseconds 500

        # Step 3: Read from Wavicle
        $wavicleVal = Send-Cmd $conn @("GET", "users:$pgId`:name")

        # Step 4: Compare
        $Counters.PGVerified++
        if ($wavicleVal -ne $pgVal) {
            $Counters.StaleReads++
            Write-Host "[STALE!] id=$pgId expected=$pgVal got=$wavicleVal" -ForegroundColor Red
        }

        # Check every 10-15 seconds (not every op — doesn't throttle QPS)
        Start-Sleep -Seconds $rng.Next(10, 15)
    }

    try { $conn.Tcp.Close() } catch {}
}

$memSamples = [System.Collections.Generic.List[double]]::new()

$pool = [RunspaceFactory]::CreateRunspacePool(1, $Workers + 3)
$pool.Open()
$runspaces = @()

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

$pgPs = [PowerShell]::Create()
$pgPs.RunspacePool = $pool
$pgPs.AddScript($pgStressScript) | Out-Null
$pgPs.AddArgument($PgContainer) | Out-Null
$pgPs.AddArgument($PgUser) | Out-Null
$pgPs.AddArgument($PgDb) | Out-Null
$pgPs.AddArgument($endTime) | Out-Null
$pgPs.AddArgument($counters) | Out-Null
$runspaces += @{ PS=$pgPs; Handle=$pgPs.BeginInvoke() }

$stalePs = [PowerShell]::Create()
$stalePs.RunspacePool = $pool
$stalePs.AddScript($staleCheckScript) | Out-Null
$stalePs.AddArgument($WavicleHost) | Out-Null
$stalePs.AddArgument($Port) | Out-Null
$stalePs.AddArgument($Password) | Out-Null
$stalePs.AddArgument($PgContainer) | Out-Null
$stalePs.AddArgument($PgUser) | Out-Null
$stalePs.AddArgument($PgDb) | Out-Null
$stalePs.AddArgument($endTime) | Out-Null
$stalePs.AddArgument($counters) | Out-Null
$runspaces += @{ PS=$stalePs; Handle=$stalePs.BeginInvoke() }

Write-Host "  $Workers workers (70% GET / 15% SET / 10% HASH / 5% PING)" -ForegroundColor Green
Write-Host "  PG stress (IDs 1-100) + Stale detector (IDs 9001-9100)" -ForegroundColor Green
Write-Host "  Running for ${DurationHours} hours..." -ForegroundColor Yellow
Write-Host ""

$lastOps = [long]0
$intervalWatch = [System.Diagnostics.Stopwatch]::StartNew()

try {
    while ([DateTime]::UtcNow -lt $endTime) {
        Start-Sleep -Seconds 5
        $elapsed = [DateTime]::UtcNow - $soakStart

        $currentOps = $counters.TotalOps
        $opsThisInterval = $currentOps - $lastOps
        $qps = [math]::Round($opsThisInterval / $intervalWatch.Elapsed.TotalSeconds)
        $lastOps = $currentOps
        $intervalWatch.Restart()
        if ($qps -gt $counters.PeakQPS) { $counters.PeakQPS = $qps }

        try {
            $memStr = (docker stats wavicle-wavicle-1 --no-stream --format "{{.MemUsage}}" 2>$null).Trim()
            $memMB = [double]([regex]::Match($memStr, '([\d.]+)MiB').Groups[1].Value)
            if ($memMB -gt 0) { $memSamples.Add($memMB) }
        } catch { $memMB = 0 }

        $memTrend = ""
        if ($memSamples.Count -ge 3) {
            $first = $memSamples[0]
            $last = $memSamples[$memSamples.Count - 1]
            $growthPct = if ($first -gt 0) { [math]::Round(($last - $first) / $first * 100, 1) } else { 0 }
            $trendSymbol = if ($growthPct -gt 20) { "growing" } else { "stable" }
            $memTrend = "$($last)MiB ($trendSymbol +$growthPct%)"
        } else { $memTrend = "${memMB}MiB" }

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
            $entries = Get-MetricVal "wavicle_atom_count"
            $lagLine = $lines | Where-Object { $_ -match 'wavicle_replication_lag_ms' } | Select-Object -First 1
            $lag = if ($lagLine) { [math]::Round([double](($lagLine -split '\s+')[1])) } else { "?" }
            $hr = if ($hits + $misses -gt 0) { [math]::Round($hits/($hits+$misses)*100,1) } else { 0 }
            $metricsLine = "Hit:${hr}% Atoms:${entries} Lag:${lag}ms"
        } catch { $metricsLine = "metrics unavailable" }

        Write-Host ("[{0:hh\:mm\:ss}] Ops:{1,8} QPS:{2,5} Peak:{3,5} | Mem:{4,-20} | {5}" -f `
            $elapsed, $counters.TotalOps, $qps, $counters.PeakQPS, $memTrend, $metricsLine) -ForegroundColor White

        if ($counters.StaleReads -gt 0) {
            Write-Host ("           STALE READS: {0} / {1} PG writes verified" -f `
                $counters.StaleReads, $counters.PGVerified) -ForegroundColor Red
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

    Write-Host "  Duration      : $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor White
    Write-Host "  Total Ops     : $($counters.TotalOps)" -ForegroundColor White
    Write-Host "  Avg QPS       : $([math]::Round($counters.TotalOps / $elapsed.TotalSeconds))" -ForegroundColor White
    Write-Host "  Peak QPS      : $($counters.PeakQPS)" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Memory Start  : ${firstMem}MiB" -ForegroundColor White
    Write-Host "  Memory End    : ${lastMem}MiB" -ForegroundColor White
    Write-Host "  Memory Peak   : ${maxMem}MiB" -ForegroundColor White
    Write-Host "  Memory Growth : ${memGrowth}%" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Stale Reads   : $($counters.StaleReads) / $($counters.PGVerified) PG writes verified" -ForegroundColor $(if($counters.StaleReads -gt 0){"Red"}else{"Green"})
    Write-Host "  Write Errors  : $($counters.WriteErrors)" -ForegroundColor $(if($counters.WriteErrors -gt 0){"Yellow"}else{"Green"})
    Write-Host "  Conn Errors   : $($counters.ConnErrors)" -ForegroundColor $(if($counters.ConnErrors -gt 0){"Yellow"}else{"Green"})
    Write-Host "  Conn Storms   : $($counters.Storms)" -ForegroundColor Yellow
    Write-Host ""

    $soakPassed = $counters.StaleReads -eq 0
    if ($soakPassed) {
        Write-Host "  SOAK TEST PASSED — Replication loop verified" -ForegroundColor Green
        Write-Host "  Ready for Oracle Cloud 7-day run" -ForegroundColor Green
    } else {
        Write-Host "  SOAK TEST FAILED — Stale reads detected" -ForegroundColor Red
        Write-Host "  PG writes not reflected in Wavicle reads" -ForegroundColor Red
    }

    Write-Host "==============================================" -ForegroundColor Magenta

    foreach ($rs in $runspaces) {
        try { $rs.PS.EndInvoke($rs.Handle) } catch {}
        $rs.PS.Dispose()
    }
    $pool.Close()
    $pool.Dispose()
}
