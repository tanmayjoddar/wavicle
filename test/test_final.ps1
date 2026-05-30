Write-Host "=== WAVICLE PHASE 2 - COMPLETE PRODUCTION VERIFICATION ===" -ForegroundColor Cyan
$global:pass = 0; $global:fail = 0

function Test-Result($name, $condition, $expected, $actual) {
    if ($condition) {
        Write-Host "[PASS] $name" -ForegroundColor Green
        $global:pass++
    } else {
        Write-Host "[FAIL] $name - Expected: $expected, Got: $actual" -ForegroundColor Red
        $global:fail++
    }
}

function Get-RespValue($stream, $reader) {
    Start-Sleep -Milliseconds 100
    $resp = @()
    while ($stream.DataAvailable) {
        $resp += $reader.ReadLine()
    }
    if ($resp.Count -ge 2 -and $resp[0].StartsWith("$") -and $resp[0] -ne "$-1") {
        return $resp[1]
    }
    if ($resp.Count -eq 1) {
        return $resp[0]
    }
    return $resp
}

# ============================================
# TEST 1: AUTH - Wrong Password Rejection
# ============================================
Write-Host "`n--- TEST 1: Auth Wrong Password ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH wrongpass"); $writer.Flush()
$resp = Get-RespValue $stream $reader
$tcp.Close()
Test-Result "Auth rejects wrong password" ($resp -match "ERR") "ERR" $resp

# ============================================
# TEST 2: AUTH - Unauthenticated Command Rejection
# ============================================
Write-Host "`n--- TEST 2: Unauthenticated Rejection ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("GET users:123:name"); $writer.Flush()
$resp = Get-RespValue $stream $reader
$tcp.Close()
Test-Result "Unauthenticated GET rejected" ($resp -match "NOAUTH") "NOAUTH" $resp

# ============================================
# TEST 3: AUTH - PING Without Auth (Should Work)
# ============================================
Write-Host "`n--- TEST 3: PING Without Auth ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("PING"); $writer.Flush()
$resp = Get-RespValue $stream $reader
$tcp.Close()
Test-Result "PING without auth works" ($resp -eq "+PONG") "+PONG" $resp

# ============================================
# TEST 4: Hash Operations
# ============================================
Write-Host "`n--- TEST 4: Hash Operations ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine("AUTH testpass"); $writer.Flush(); Get-RespValue $stream $reader | Out-Null

$writer.WriteLine("HSET profile:1 theme dark"); $writer.Flush()
$hset1 = Get-RespValue $stream $reader

$writer.WriteLine("HSET profile:1 lang go"); $writer.Flush()
$hset2 = Get-RespValue $stream $reader

$writer.WriteLine("HGET profile:1 theme"); $writer.Flush()
$hget = Get-RespValue $stream $reader

$writer.WriteLine("HGETALL profile:1"); $writer.Flush()
$hgetall = Get-RespValue $stream $reader

$tcp.Close()

Test-Result "HSET theme returns 1" ($hset1 -eq ":1") ":1" $hset1
Test-Result "HSET lang returns 1" ($hset2 -eq ":1") ":1" $hset2
Test-Result "HGET theme returns dark" ($hget -eq "dark") "dark" $hget
Test-Result "HGETALL has elements" ($hgetall -ne $null) "non-null" "count: $($hgetall.Count)"

# ============================================
# TEST 5: TTL / Expiration
# ============================================
Write-Host "`n--- TEST 5: TTL Expiration ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine("AUTH testpass"); $writer.Flush(); Get-RespValue $stream $reader | Out-Null
$writer.WriteLine("SET users:temp:name TTLValue"); $writer.Flush(); Get-RespValue $stream $reader | Out-Null

$writer.WriteLine("EXPIRE users:temp:name 3"); $writer.Flush()
$expire = Get-RespValue $stream $reader

$writer.WriteLine("TTL users:temp:name"); $writer.Flush()
$ttl1 = Get-RespValue $stream $reader
$ttl1_val = $ttl1.Replace(":", "")

Write-Host "Waiting 4 seconds for expiry..."
Start-Sleep -Seconds 4

$writer.WriteLine("GET users:temp:name"); $writer.Flush()
$get_after = Get-RespValue $stream $reader

$writer.WriteLine("TTL users:temp:name"); $writer.Flush()
$ttl2 = Get-RespValue $stream $reader
$ttl2_val = $ttl2.Replace(":", "")

$tcp.Close()

Test-Result "EXPIRE returns 1" ($expire -eq ":1") ":1" $expire
Test-Result "TTL before expiry > 0" ([int]$ttl1_val -gt 0) ">0" $ttl1_val
Test-Result "GET after expiry returns nil" ($get_after -eq "$-1" -or $get_after[0] -eq "$-1") "$-1" "$get_after"
Test-Result "TTL after expiry returns -2" ([int]$ttl2_val -eq -2) "-2" $ttl2_val

# ============================================
# TEST 6: Connection Limit / MaxConns
# ============================================
Write-Host "`n--- TEST 6: Connection Limit ---" -ForegroundColor Yellow
$connections = @()
$maxTest = 25
$rejected = $false

for ($i = 0; $i -lt $maxTest; $i++) {
    try {
        $client = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
        $connections += $client
    } catch {
        $rejected = $true
        break
    }
}

if ($connections.Count -gt 0) {
    $last = $connections[-1]
    $stream = $last.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
    $writer.WriteLine("AUTH testpass"); $writer.Flush()
    $auth_resp = Get-RespValue $stream $reader
}

$connections | ForEach-Object { $_.Close() }
Test-Result "Connection limit enforced" ($rejected -or $auth_resp -match "ERR max number") "rejected or ERR" "accepted $($connections.Count), resp: $auth_resp"

# ============================================
# TEST 7: Prometheus Metrics
# ============================================
Write-Host "`n--- TEST 7: Prometheus Histograms ---" -ForegroundColor Yellow
try {
    $metrics = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content
    $hasHistogram = $metrics -match "wavicle_request_duration_seconds_bucket"
    Test-Result "Has histogram buckets" ($hasHistogram) "buckets present" "present"
    Test-Result "Has active_connections" ($metrics -match "wavicle_active_connections") "present" "present"
    Test-Result "Has wal_compactions" ($metrics -match "wavicle_compactions_total") "present" "present"
} catch {
    Test-Result "Metrics endpoint reachable" $false "200 OK" "connection failed"
}

# ============================================
# TEST 8: Replication Lag + Consistency
# ============================================
Write-Host "`n--- TEST 8: Replication Consistency ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH testpass"); $writer.Flush(); Get-RespValue $stream $reader | Out-Null

docker exec wavicle-db-1 psql -U wavicle -d wavicle -c "UPDATE users SET name='Phase2FinalTest' WHERE id='123'" | Out-Null
Start-Sleep -Milliseconds 500

$writer.WriteLine("GET users:123:name"); $writer.Flush()
$after_val = Get-RespValue $stream $reader
$tcp.Close()

Test-Result "DB update reflected in cache" ($after_val -eq "Phase2FinalTest") "Phase2FinalTest" $after_val

# ============================================
# TEST 9: WAL Compaction Trigger
# ============================================
Write-Host "`n--- TEST 9: WAL Compaction ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH testpass"); $writer.Flush(); Get-RespValue $stream $reader | Out-Null

for ($i = 0; $i -lt 50; $i++) {
    $writer.WriteLine("SET users:bulk_$i:name value$i")
    if ($i % 10 -eq 0) { $writer.Flush(); Start-Sleep -Milliseconds 50 }
}
$writer.Flush(); Start-Sleep -Milliseconds 500
$tcp.Close()

$metrics = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content
$hasCompactionMetric = $metrics -match "wavicle_compactions_total"
Test-Result "WAL compaction metric exists" ($hasCompactionMetric) "present" "present"

# ============================================
# TEST 10: Graceful Shutdown Signal
# ============================================
Write-Host "`n--- TEST 10: Graceful Shutdown ---" -ForegroundColor Yellow
docker-compose stop -t 15 wavicle 2>&1 | Out-Null
Start-Sleep -Seconds 2
$shutdownLogs = docker-compose logs --tail=10 wavicle 2>$null | Select-String -Pattern "shutdown|drain|complete" -CaseSensitive:$false
docker-compose up -d wavicle 2>&1 | Out-Null
Start-Sleep -Seconds 5

Test-Result "Shutdown logs show graceful handling" ($shutdownLogs -ne $null) "shutdown logs" "no logs"

# Verify recovery
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH testpass"); $writer.Flush(); Get-RespValue $stream $reader | Out-Null
$writer.WriteLine("PING"); $writer.Flush()
$ping = Get-RespValue $stream $reader
$tcp.Close()

Test-Result "Server recovers after shutdown" ($ping -eq "+PONG") "+PONG" $ping

# ============================================
# SUMMARY
# ============================================
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "PHASE 2 VERIFICATION COMPLETE" -ForegroundColor Cyan
Write-Host "PASS: $global:pass | FAIL: $global:fail" -ForegroundColor $(if ($global:fail -eq 0) { "Green" } else { "Red" })
Write-Host "========================================" -ForegroundColor Cyan
