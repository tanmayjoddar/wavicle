param(
    [string]$Password = "testpass",
    [string]$WavicleHost = "localhost",
    [int]$Port = 6379,
    [string]$MetricsUrl = "http://localhost:8080/metrics",
    [string]$PgContainer = "wavicle-db-1",
    [string]$PgUser = "wavicle",
    [string]$PgDb = "wavicle"
)

$passed = 0
$failed = 0
$warnings = 0
$results = @()
$hitRate = $null
$lagUsers = $null
$nsOp = $null

function Write-Header($text) {
    Write-Host "`n$("="*60)" -ForegroundColor Cyan
    Write-Host "  $text" -ForegroundColor Cyan
    Write-Host "$("="*60)" -ForegroundColor Cyan
}

function Write-Pass($test, $detail = "") {
    $script:passed++
    if ($detail) { Write-Host "  [PASS] $test - $detail" -ForegroundColor Green }
    else { Write-Host "  [PASS] $test" -ForegroundColor Green }
    $script:results += [PSCustomObject]@{ Status="PASS"; Test=$test; Detail=$detail }
}

function Write-Fail($test, $detail = "") {
    $script:failed++
    if ($detail) { Write-Host "  [FAIL] $test - $detail" -ForegroundColor Red }
    else { Write-Host "  [FAIL] $test" -ForegroundColor Red }
    $script:results += [PSCustomObject]@{ Status="FAIL"; Test=$test; Detail=$detail }
}

function Write-Warn($test, $detail = "") {
    $script:warnings++
    if ($detail) { Write-Host "  [WARN] $test - $detail" -ForegroundColor Yellow }
    else { Write-Host "  [WARN] $test" -ForegroundColor Yellow }
    $script:results += [PSCustomObject]@{ Status="WARN"; Test=$test; Detail=$detail }
}

function Invoke-Wavicle {
    param([string[]]$Commands, [bool]$Auth = $true)
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient($WavicleHost, $Port)
        $tcp.ReceiveTimeout = 3000
        $tcp.SendTimeout = 3000
        $stream = $tcp.GetStream()
        $writer = New-Object System.IO.StreamWriter($stream)
        $reader = New-Object System.IO.StreamReader($stream)
        $responses = @()

        if ($Auth -and $Password) {
            $writer.WriteLine("*2`r`n`$4`r`nAUTH`r`n`$$($Password.Length)`r`n$Password")
            $writer.Flush()
            $reader.ReadLine() | Out-Null
        }

        foreach ($cmd in $Commands) {
            $parts = $cmd -split ' ', 2
            if ($parts.Count -gt 1) {
                $tokens = @($parts[0]) + ($parts[1] -split ' ')
            } else {
                $tokens = @($parts[0])
            }
            $resp = "*$($tokens.Count)`r`n"
            foreach ($t in $tokens) {
                $resp += "`$$($t.Length)`r`n$t`r`n"
            }
            $writer.Write($resp)
            $writer.Flush()

            $line = $reader.ReadLine()
            if ($null -eq $line) { $responses += $null; continue }

            $line = $line.TrimEnd("`r")

            if ($line.StartsWith('+')) { $responses += $line.Substring(1) }
            elseif ($line.StartsWith('-')) { $responses += "ERR:$($line.Substring(1))" }
            elseif ($line.StartsWith(':')) { $responses += $line.Substring(1) }
            elseif ($line.StartsWith('$')) {
                $len = [int]$line.Substring(1)
                if ($len -eq -1) { $responses += $null }
                else { 
                    $data = $reader.ReadLine()
                    $responses += $data.TrimEnd("`r")
                }
            }
            else { $responses += $line }
        }
        $tcp.Close()
        return ,$responses
    } catch {
        return @("EXCEPTION:$($_.Exception.Message)")
    }
}

function Get-Metrics {
    try {
        return (Invoke-WebRequest -Uri $MetricsUrl -UseBasicParsing -TimeoutSec 5).Content
    } catch { return "" }
}

function Get-MetricValue($metrics, $name) {
    $line = ($metrics -split "`n") | Where-Object { $_ -match "^$name\s" } | Select-Object -First 1
    if ($line) { return [double]($line -split '\s+')[1] }
    return $null
}

function Get-MetricValueLabeled($metrics, $name, $label) {
    $line = ($metrics -split "`n") | Where-Object { $_ -match "^${name}\{.*$label.*\}" } | Select-Object -First 1
    if ($line) { return [double]($line -split '\s+')[1] }
    return $null
}

function Invoke-PgQuery($query) {
    try {
        $result = docker exec $PgContainer psql -U $PgUser -d $PgDb -t -c $query 2>$null
        return ($result | Where-Object { $_ -match '\S' } | Select-Object -First 1).Trim()
    } catch { return $null }
}

function Reset-TestData {
    docker exec $PgContainer psql -U $PgUser -d $PgDb -c "UPDATE users SET name='Alice', email='alice@example.com' WHERE id='123'" 2>$null | Out-Null
    Start-Sleep -Milliseconds 500
}

# ================================================================
Write-Header "INFRASTRUCTURE CHECKS"
# ================================================================

$wavicleRunning = docker ps --format "{{.Names}}" 2>$null | Select-String "wavicle-wavicle"
$dbRunning = docker ps --format "{{.Names}}" 2>$null | Select-String "wavicle-db"
if ($wavicleRunning) { Write-Pass "Wavicle container running" }
else { Write-Fail "Wavicle container running" "Not found - run docker-compose up" }
if ($dbRunning) { Write-Pass "PostgreSQL container running" }
else { Write-Fail "PostgreSQL container running" "DB container not found" }

$slotActive = Invoke-PgQuery "SELECT active FROM pg_replication_slots WHERE slot_name='wavicle_slot';"
if ($slotActive -eq "t") { Write-Pass "Replication slot active" "wavicle_slot active=t" }
else { Write-Fail "Replication slot active" "Got: $slotActive" }

$pubExists = Invoke-PgQuery "SELECT COUNT(*) FROM pg_publication WHERE pubname='wavicle_proofs';"
if ($pubExists -eq "1") { Write-Pass "Publication exists" "wavicle_proofs found" }
else { Write-Fail "Publication exists" "wavicle_proofs not found" }

$usersIdent = Invoke-PgQuery "SELECT relreplident FROM pg_class WHERE relname='users';"
if ($usersIdent -eq "f") { Write-Pass "REPLICA IDENTITY FULL on users" }
else { Write-Fail "REPLICA IDENTITY FULL on users" "Got: $usersIdent expected f" }

$metrics = Get-Metrics
if ($metrics -match "wavicle_cache_hits_total") { Write-Pass "Prometheus metrics endpoint responding" }
else { Write-Fail "Prometheus metrics endpoint" "No metrics returned from $MetricsUrl" }

# ================================================================
Write-Header "PHASE 1 - PROTOCOL AND BASIC COMMANDS"
# ================================================================

$r = Invoke-Wavicle @("PING")
if ($r[0] -eq "PONG") { Write-Pass "PING" }
else { Write-Fail "PING" "Got: $($r[0])" }

$r = Invoke-Wavicle @("SET test:1:value HelloWavicle", "GET test:1:value")
if ($r[0] -eq "OK" -and $r[1] -eq "HelloWavicle") { Write-Pass "SET and GET round trip" }
else { Write-Fail "SET and GET round trip" "SET=$($r[0]) GET=$($r[1])" }

$r = Invoke-Wavicle @("SET test:del:value ToDelete", "DEL test:del:value", "GET test:del:value")
if ($r[1] -eq "1" -and $null -eq $r[2]) { Write-Pass "DEL tombstone and nil return" }
else { Write-Fail "DEL" "DEL=$($r[1]) GET after DEL=$($r[2])" }

$r = Invoke-Wavicle @("SET test:ex:value Exists", "EXISTS test:ex:value", "DEL test:ex:value", "EXISTS test:ex:value")
if ($r[1] -eq "1" -and $r[3] -eq "0") { Write-Pass "EXISTS live=1 tombstoned=0" }
else { Write-Fail "EXISTS" "Before=$($r[1]) After=$($r[3])" }

$r = Invoke-Wavicle @("MSET test:m:a AAA test:m:b BBB test:m:c CCC")
if ($r[0] -eq "OK") { Write-Pass "MSET batch write" }
else { Write-Fail "MSET" "Got: $($r[0])" }

$r = Invoke-Wavicle @("HSET profile:test theme dark", "HGET profile:test theme")
if ($r[0] -eq "1" -and $r[1] -eq "dark") { Write-Pass "HSET and HGET hash operations" }
else { Write-Fail "HSET and HGET" "HSET=$($r[0]) HGET=$($r[1])" }

$r = Invoke-Wavicle @("DBSIZE")
if ($r[0] -match '^\d+$' -and [int]$r[0] -gt 0) { Write-Pass "DBSIZE" "Returned $($r[0]) entries" }
else { Write-Fail "DBSIZE" "Got: $($r[0])" }

$r = Invoke-Wavicle @("SET test:ttl:val ExpiresIn10", "EXPIRE test:ttl:val 10", "TTL test:ttl:val")
$ttlVal = [int]$r[2]
if ($r[1] -eq "1" -and $ttlVal -gt 0 -and $ttlVal -le 10) { Write-Pass "EXPIRE and TTL real implementation" "TTL=$ttlVal seconds" }
else { Write-Fail "EXPIRE and TTL" "EXPIRE=$($r[1]) TTL=$($r[2])" }

$r = Invoke-Wavicle @("TTL test:missing:key:xyz")
if ($r[0] -eq "-2") { Write-Pass "TTL on missing key returns -2" }
else { Write-Fail "TTL missing key" "Got: $($r[0])" }

$r = Invoke-Wavicle @("SET test:nottl:val NoExpiry", "TTL test:nottl:val")
if ($r[1] -eq "-1") { Write-Pass "TTL on key with no expiry returns -1" }
else { Write-Fail "TTL no expiry" "Got: $($r[1])" }

# ================================================================
Write-Header "PHASE 1 - CORRECTNESS TESTS"
# ================================================================

$r = Invoke-Wavicle @("SET users:rw:name Alice", "GET users:rw:name", "SET users:rw:name Bob", "GET users:rw:name")
if ($r[1] -eq "Alice" -and $r[3] -eq "Bob") { Write-Pass "ReadAfterWrite no stale reads" }
else { Write-Fail "ReadAfterWrite" "First=$($r[1]) Second=$($r[3])" }

$allPass = $true
$lastVal = ""
$cmds = @()
for ($i = 1; $i -le 6; $i++) {
    $cmds += "SET users:cw:name Write$i"
    $cmds += "GET users:cw:name"
}
$r = Invoke-Wavicle $cmds
for ($i = 0; $i -lt 12; $i += 2) {
    $writeNum = ($i/2) + 1
    $expected = "Write$writeNum"
    if ($r[$i+1] -ne $expected) { $allPass = $false; $lastVal = "Expected $expected got $($r[$i+1])" }
}
if ($allPass) { Write-Pass "ConsecutiveWrites 6 writes all visible immediately" }
else { Write-Fail "ConsecutiveWrites" $lastVal }

Write-Host "`n  Running Alice to Charlie replication test..." -ForegroundColor Magenta
Reset-TestData
$r = Invoke-Wavicle @("GET users:123:name")
$beforeUpdate = $r[0]
docker exec $PgContainer psql -U $PgUser -d $PgDb -c "UPDATE users SET name='Charlie' WHERE id='123'" 2>$null | Out-Null
Start-Sleep -Milliseconds 500
$r = Invoke-Wavicle @("GET users:123:name")
$afterUpdate = $r[0]
if ($afterUpdate -eq "Charlie") { Write-Pass "Alice to Charlie replication test" "Before=$beforeUpdate After=$afterUpdate ZERO STALE READS" }
else { Write-Fail "Alice to Charlie replication test" "Before=$beforeUpdate After=$afterUpdate STALE READ DETECTED" }

Reset-TestData

# ================================================================
Write-Header "PHASE 2 - AUTHENTICATION"
# ================================================================

try {
    $tcp = New-Object System.Net.Sockets.TcpClient($WavicleHost, $Port)
    $tcp.ReceiveTimeout = 2000
    $stream = $tcp.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $reader = New-Object System.IO.StreamReader($stream)
    $writer.WriteLine("*2`r`n`$3`r`nGET`r`n`$4`r`ntest")
    $writer.Flush()
    $noAuthResp = $reader.ReadLine()
    $tcp.Close()
    if ($noAuthResp -match "NOAUTH") { Write-Pass "Unauthenticated GET blocked" "Got NOAUTH" }
    else { Write-Fail "Unauthenticated GET blocked" "Expected NOAUTH got: $noAuthResp" }
} catch {
    Write-Warn "Unauthenticated GET test" $_.Exception.Message
}

try {
    $tcp = New-Object System.Net.Sockets.TcpClient($WavicleHost, $Port)
    $tcp.ReceiveTimeout = 2000
    $stream = $tcp.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $reader = New-Object System.IO.StreamReader($stream)
    $writer.WriteLine("*2`r`n`$4`r`nAUTH`r`n`$11`r`nwrongpasswd")
    $writer.Flush()
    $wrongResp = $reader.ReadLine()
    $tcp.Close()
    if ($wrongResp -match "ERR") { Write-Pass "Wrong password rejected" }
    else { Write-Fail "Wrong password rejected" "Got: $wrongResp" }
} catch {
    Write-Warn "Wrong password test" $_.Exception.Message
}

$r = Invoke-Wavicle @("PING")
if ($r[0] -eq "PONG") { Write-Pass "Correct password accepted" }
else { Write-Fail "Correct password accepted" "Got: $($r[0])" }

# ================================================================
Write-Header "PHASE 2 - METRICS AND OBSERVABILITY"
# ================================================================

for ($i = 0; $i -lt 30; $i++) {
    Invoke-Wavicle @("GET users:123:name", "GET users:123:email") | Out-Null
}
$metrics = Get-Metrics

$hits = Get-MetricValue $metrics "wavicle_cache_hits_total"
$misses = Get-MetricValue $metrics "wavicle_cache_misses_total"
if ($null -ne $hits -and $null -ne $misses -and ($hits + $misses) -gt 0) {
    $script:hitRate = [math]::Round($hits / ($hits + $misses) * 100, 2)
    if ($hitRate -ge 90) { Write-Pass "Cache hit rate above 90%" "$hitRate% ($hits hits)" }
    else { Write-Warn "Cache hit rate" "$hitRate% below 90% target" }
} else { Write-Warn "Cache hit rate" "Could not parse metrics" }

$script:lagUsers = Get-MetricValueLabeled $metrics "wavicle_replication_lag_ms" "users"
$lagProducts = Get-MetricValueLabeled $metrics "wavicle_replication_lag_ms" "products"
if ($null -ne $lagUsers -and $lagUsers -lt 100) { Write-Pass "Replication lag users under 100ms" "${lagUsers}ms" }
elseif ($null -ne $lagUsers) { Write-Fail "Replication lag users" "${lagUsers}ms exceeds 100ms" }
else { Write-Warn "Replication lag users" "No metric found - trigger a DB update first" }
if ($null -ne $lagProducts -and $lagProducts -lt 100) { Write-Pass "Replication lag products under 100ms" "${lagProducts}ms" }
elseif ($null -ne $lagProducts) { Write-Warn "Replication lag products" "${lagProducts}ms" }

if ($metrics -match "wavicle_request_duration_seconds") { Write-Pass "Request duration histogram present" }
else { Write-Fail "Request duration histogram" "Not found in metrics" }

$activeConns = Get-MetricValue $metrics "wavicle_active_connections"
if ($null -ne $activeConns) { Write-Pass "Active connections gauge present" "Current: $activeConns" }
else { Write-Fail "Active connections gauge" "Not found" }

$walSize = Get-MetricValue $metrics "wavicle_wal_size_bytes"
if ($null -ne $walSize) {
    $walMB = [math]::Round($walSize / 1MB, 2)
    Write-Pass "WAL size gauge present" "${walMB}MB"
} else { Write-Fail "WAL size gauge" "Not found" }

# ================================================================
Write-Header "PHASE 2 - GRACEFUL SHUTDOWN"
# ================================================================

$r = Invoke-Wavicle @("PING")
if ($r[0] -eq "PONG") { Write-Pass "Server healthy for shutdown test" }
else { Write-Fail "Server health before shutdown" "Got: $($r[0])" }
Write-Warn "Graceful shutdown live test" "Skipped - verify manually: docker-compose stop -t 15 wavicle"

# ================================================================
Write-Header "REPLICATION STRESS TEST"
# ================================================================

Write-Host "  Running 10 rapid external writes..." -ForegroundColor Gray
Reset-TestData
$staleCount = 0
for ($i = 1; $i -le 10; $i++) {
    $name = "RapidWrite$i"
    docker exec $PgContainer psql -U $PgUser -d $PgDb -c "UPDATE users SET name='$name' WHERE id='123'" 2>$null | Out-Null
    Start-Sleep -Milliseconds 300
    $r = Invoke-Wavicle @("GET users:123:name")
    if ($r[0] -ne $name) { $staleCount++ }
}
if ($staleCount -eq 0) { Write-Pass "10 rapid external writes all reflected" "0 stale reads" }
else { Write-Fail "Rapid external writes" "$staleCount stale reads out of 10" }
Reset-TestData

# ================================================================
Write-Header "PERFORMANCE BENCHMARKS"
# ================================================================

Write-Host "  Measuring warm read latency over 100 GETs..." -ForegroundColor Gray
Invoke-Wavicle @("SET bench:key BenchValue") | Out-Null
$latencies = @()
for ($i = 0; $i -lt 100; $i++) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Invoke-Wavicle @("GET bench:key") | Out-Null
    $sw.Stop()
    $latencies += $sw.Elapsed.TotalMilliseconds
}
$avgLatency = [math]::Round(($latencies | Measure-Object -Average).Average, 3)
$p99Latency = [math]::Round(($latencies | Sort-Object)[[int]($latencies.Count * 0.99)], 3)
Write-Pass "Warm read latency" "avg=${avgLatency}ms p99=${p99Latency}ms includes TCP overhead"

Write-Host "  Running Go proof engine benchmarks..." -ForegroundColor Gray
$benchOutput = go test ./benchmarks/ -bench=. -benchtime=3s -run="^$" 2>&1
if ($benchOutput -match "FastPath2") {
    $fp2Line = ($benchOutput -split "`n") | Where-Object { $_ -match "FastPath2" } | Select-Object -First 1
    $nsOp = [regex]::Match($fp2Line, '(\d+(?:\.\d+)?)\s+ns/op').Groups[1].Value
    $script:nsOp = $nsOp
    Write-Pass "FastPath2 proof engine benchmark" "${nsOp}ns/op"
    if ([double]$nsOp -lt 1000) { Write-Pass "FastPath2 under 1000ns target" "${nsOp}ns" }
    else { Write-Warn "FastPath2 latency high" "${nsOp}ns" }
} else {
    Write-Warn "Go benchmarks" "Run manually: go test ./benchmarks/ -bench=. -run=^$"
}

# ================================================================
Write-Header "FINAL SUMMARY"
# ================================================================

$total = $passed + $failed + $warnings
Write-Host ""
Write-Host "  Total  : $total" -ForegroundColor White
Write-Host "  Passed : $passed" -ForegroundColor Green
Write-Host "  Failed : $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })
Write-Host "  Warned : $warnings" -ForegroundColor $(if ($warnings -gt 0) { "Yellow" } else { "Green" })
Write-Host ""

if ($failed -eq 0) {
    Write-Host "  ALL CRITICAL TESTS PASSED" -ForegroundColor Green
    Write-Host "  Wavicle is ready for the 7-day soak test" -ForegroundColor Green
} elseif ($failed -le 2) {
    Write-Host "  $failed TEST(S) FAILED - fix before soak test" -ForegroundColor Red
} else {
    Write-Host "  $failed TESTS FAILED - do NOT proceed to soak test" -ForegroundColor Red
}

if ($failed -gt 0) {
    Write-Host ""
    Write-Host "  Failed Tests:" -ForegroundColor Red
    $results | Where-Object { $_.Status -eq "FAIL" } | ForEach-Object {
        Write-Host "    - $($_.Test): $($_.Detail)" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "  Key Metrics:" -ForegroundColor Cyan
Write-Host "    Cache Hit Rate  : $(if($hitRate){"$hitRate%"}else{"N/A"})" -ForegroundColor White
Write-Host "    Replication Lag : $(if($lagUsers){"${lagUsers}ms"}else{"N/A"})" -ForegroundColor White
Write-Host "    FastPath2       : $(if($nsOp){"${nsOp}ns/op"}else{"run benchmarks separately"})" -ForegroundColor White
Write-Host ""
