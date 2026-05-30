Write-Host "=== WAVICLE PHASE 2 — COMPLETE PRODUCTION VERIFICATION ===" -ForegroundColor Cyan
$pass = 0; $fail = 0

function Test-Result($name, $condition, $expected, $actual) {
    if ($condition) {
        Write-Host "[PASS] $name" -ForegroundColor Green
        $script:pass++
    } else {
        Write-Host "[FAIL] $name - Expected: $expected, Got: $actual" -ForegroundColor Red
        $script:fail++
    }
}

# ============================================
# TEST 1: AUTH — Wrong Password Rejection
# ============================================
Write-Host "`n--- TEST 1: Auth Wrong Password ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH wrongpass"); $writer.Flush(); Start-Sleep -Milliseconds 200
$resp = while ($stream.DataAvailable) { $reader.ReadLine() }
$tcp.Close()
Test-Result "Auth rejects wrong password" ($resp -match "ERR") "ERR" $resp

# ============================================
# TEST 2: AUTH — Unauthenticated Command Rejection
# ============================================
Write-Host "`n--- TEST 2: Unauthenticated Rejection ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("GET users:123:name"); $writer.Flush(); Start-Sleep -Milliseconds 200
$resp = while ($stream.DataAvailable) { $reader.ReadLine() }
$tcp.Close()
Test-Result "Unauthenticated GET rejected" ($resp -match "NOAUTH") "NOAUTH" $resp

# ============================================
# TEST 3: AUTH — PING Without Auth (Should Work)
# ============================================
Write-Host "`n--- TEST 3: PING Without Auth ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("PING"); $writer.Flush(); Start-Sleep -Milliseconds 200
$resp = while ($stream.DataAvailable) { $reader.ReadLine() }
$tcp.Close()
Test-Result "PING without auth works" ($resp -eq "PONG") "PONG" $resp

# ============================================
# TEST 4: Hash Operations (Your Script 1)
# ============================================
Write-Host "`n--- TEST 4: Hash Operations ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine("AUTH testpass"); $writer.Flush(); Start-Sleep -Milliseconds 100; while ($stream.DataAvailable) { $reader.ReadLine() | Out-Null }

$writer.WriteLine("HSET profile:1 theme dark"); $writer.Flush(); Start-Sleep -Milliseconds 100
$hset1 = while ($stream.DataAvailable) { $reader.ReadLine() }

$writer.WriteLine("HSET profile:1 lang go"); $writer.Flush(); Start-Sleep -Milliseconds 100
$hset2 = while ($stream.DataAvailable) { $reader.ReadLine() }

$writer.WriteLine("HGET profile:1 theme"); $writer.Flush(); Start-Sleep -Milliseconds 100
$hget = while ($stream.DataAvailable) { $reader.ReadLine() }

$writer.WriteLine("HGETALL profile:1"); $writer.Flush(); Start-Sleep -Milliseconds 100
$hgetall = @(); while ($stream.DataAvailable) { $hgetall += $reader.ReadLine() }

$tcp.Close()

Test-Result "HSET theme returns 1" ($hset1 -eq "1") "1" $hset1
Test-Result "HSET lang returns 1" ($hset2 -eq "1") "1" $hset2
Test-Result "HGET theme returns dark" ($hget -eq "dark") "dark" $hget
Test-Result "HGETALL has 4 elements" ($hgetall.Count -eq 4) "4 elements" "$($hgetall.Count) elements"

# ============================================
# TEST 5: TTL / Expiration (Your Script 2)
# ============================================
Write-Host "`n--- TEST 5: TTL Expiration ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine("AUTH testpass"); $writer.Flush(); Start-Sleep -Milliseconds 100; while ($stream.DataAvailable) { $reader.ReadLine() | Out-Null }

$writer.WriteLine("SET ttlkey TTLValue"); $writer.Flush(); Start-Sleep -Milliseconds 100; while ($stream.DataAvailable) { $reader.ReadLine() | Out-Null }

$writer.WriteLine("EXPIRE ttlkey 3"); $writer.Flush(); Start-Sleep -Milliseconds 100
$expire = while ($stream.DataAvailable) { $reader.ReadLine() }

$writer.WriteLine("TTL ttlkey"); $writer.Flush(); Start-Sleep -Milliseconds 100
$ttl1 = while ($stream.DataAvailable) { $reader.ReadLine() }

Write-Host "Waiting 4 seconds for expiry..."
Start-Sleep -Seconds 4

$writer.WriteLine("GET ttlkey"); $writer.Flush(); Start-Sleep -Milliseconds 100
$get_after = while ($stream.DataAvailable) { $reader.ReadLine() }

$writer.WriteLine("TTL ttlkey"); $writer.Flush(); Start-Sleep -Milliseconds 100
$ttl2 = while ($stream.DataAvailable) { $reader.ReadLine() }

$tcp.Close()

Test-Result "EXPIRE returns 1" ($expire -eq "1") "1" $expire
Test-Result "TTL before expiry > 0" ([int]$ttl1 -gt 0) ">0" $ttl1
Test-Result "GET after expiry returns nil" ($get_after -eq "`$-1" -or $get_after -match "nil") "nil/-1" $get_after
Test-Result "TTL after expiry returns -2" ([int]$ttl2 -eq -2) "-2" $ttl2

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
        Write-Host "Connection $($i+1) rejected (expected at limit)" -ForegroundColor Green
        break
    }
}

# Try sending command on last connection
if ($connections.Count -gt 0) {
    $last = $connections[-1]
    $stream = $last.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $reader = New-Object System.IO.StreamReader($stream)
    $writer.WriteLine("AUTH testpass"); $writer.Flush(); Start-Sleep -Milliseconds 200
    $auth_resp = while ($stream.DataAvailable) { $reader.ReadLine() }
}

# Cleanup
$connections | ForEach-Object { $_.Close() }

Test-Result "Connection limit enforced" ($rejected -or $auth_resp -match "ERR") "rejected or ERR" "accepted $($connections.Count)"

# ============================================
# TEST 7: Prometheus Metrics — Histograms
# ============================================
Write-Host "`n--- TEST 7: Prometheus Histograms ---" -ForegroundColor Yellow
try {
    $metrics = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content
    $hasHistogram = $metrics | Select-String "duration_seconds_bucket"
    $hasCount = $metrics | Select-String "duration_seconds_count"
    $hasSum = $metrics | Select-String "duration_seconds_sum"

    Test-Result "Has histogram buckets" ($hasHistogram) "buckets present" "missing"
    Test-Result "Has histogram count" ($hasCount) "count present" "missing"
    Test-Result "Has histogram sum" ($hasSum) "sum present" "missing"

    # Check Phase 2 specific metrics
    $hasActiveConns = $metrics | Select-String "active_connections"
    $hasWalCompactions = $metrics | Select-String "wal_compactions"

    Test-Result "Has active_connections" ($hasActiveConns) "present" "missing"
    Test-Result "Has wal_compactions" ($hasWalCompactions) "present" "missing"
} catch {
    Test-Result "Metrics endpoint reachable" $false "200 OK" "connection failed"
}

# ============================================
# TEST 8: Replication Lag + Consistency
# ============================================
Write-Host "`n--- TEST 8: Replication Consistency ---" -ForegroundColor Yellow
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH testpass"); $writer.Flush(); Start-Sleep -Milliseconds 100; while ($stream.DataAvailable) { $reader.ReadLine() | Out-Null }

$writer.WriteLine("GET users:123:name"); $writer.Flush(); Start-Sleep -Milliseconds 100
$before = while ($stream.DataAvailable) { $reader.ReadLine() }
$tcp.Close()

# Update DB directly
docker exec wavicle-db-1 psql -U wavicle -d wavicle -c "UPDATE users SET name='Phase2FinalTest' WHERE id='123'" | Out-Null

Start-Sleep -Milliseconds 500

$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH testpass"); $writer.Flush(); Start-Sleep -Milliseconds 100; while ($stream.DataAvailable) { $reader.ReadLine() | Out-Null }

$writer.WriteLine("GET users:123:name"); $writer.Flush(); Start-Sleep -Milliseconds 100
$after = while ($stream.DataAvailable) { $reader.ReadLine() }
$tcp.Close()

Test-Result "DB update reflected in cache" ($after -eq "Phase2FinalTest") "Phase2FinalTest" $after

# Check lag metric
try {
    $metrics = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content
    $lag = $metrics | Select-String "replication_lag_ms.*users"
    Test-Result "Replication lag metric present" ($lag) "present" "missing"
} catch {
    Test-Result "Replication lag metric" $false "present" "endpoint down"
}

# ============================================
# TEST 9: WAL Compaction Trigger
# ============================================
Write-Host "`n--- TEST 9: WAL Compaction ---" -ForegroundColor Yellow
$walBefore = docker exec wavicle-1 ls -la /data/ 2>$null

# Bulk write to trigger 50MB threshold (or check if compaction metric exists)
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH testpass"); $writer.Flush(); Start-Sleep -Milliseconds 100; while ($stream.DataAvailable) { $reader.ReadLine() | Out-Null }

for ($i = 0; $i -lt 500; $i++) {
    $writer.WriteLine("SET bulkkey:$i value$i")
    if ($i % 100 -eq 0) { $writer.Flush(); Start-Sleep -Milliseconds 50 }
}
$writer.Flush(); Start-Sleep -Milliseconds 500
$tcp.Close()

$walAfter = docker exec wavicle-1 ls -la /data/ 2>$null
$hasCompactionMetric = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content | Select-String "compaction"

Test-Result "WAL compaction metric exists" ($hasCompactionMetric) "present" "missing"

# ============================================
# TEST 10: Graceful Shutdown Signal
# ============================================
Write-Host "`n--- TEST 10: Graceful Shutdown ---" -ForegroundColor Yellow
docker-compose stop -t 15 wavicle 2>&1 | Out-Null
Start-Sleep -Seconds 2
$shutdownLogs = docker-compose logs --tail=10 wavicle 2>$null | Select-String -Pattern "shutdown|drain|flush|complete|signal" -CaseSensitive:$false
docker-compose up -d wavicle 2>&1 | Out-Null
Start-Sleep -Seconds 5

Test-Result "Shutdown logs show graceful handling" ($shutdownLogs) "shutdown/drain/flush/complete" "no shutdown messages"

# Verify recovery
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $reader = New-Object System.IO.StreamReader($stream)
$writer.WriteLine("AUTH testpass"); $writer.Flush(); Start-Sleep -Milliseconds 100; while ($stream.DataAvailable) { $reader.ReadLine() | Out-Null }
$writer.WriteLine("PING"); $writer.Flush(); Start-Sleep -Milliseconds 100
$ping = while ($stream.DataAvailable) { $reader.ReadLine() }
$tcp.Close()

Test-Result "Server recovers after shutdown" ($ping -eq "PONG") "PONG" $ping

# ============================================
# SUMMARY
# ============================================
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "PHASE 2 VERIFICATION COMPLETE" -ForegroundColor Cyan
Write-Host "PASS: $pass | FAIL: $fail" -ForegroundColor $(if ($fail -eq 0) { "Green" } else { "Red" })
Write-Host "========================================" -ForegroundColor Cyan

if ($fail -eq 0) {
    Write-Host "`n✅ ALL PHASE 2 CRITERIA MET" -ForegroundColor Green
    Write-Host "Ready for design partner staging." -ForegroundColor Green
} else {
    Write-Host "`n⚠️  $fail TEST(S) FAILED - Review before claiming Phase 2 complete." -ForegroundColor Yellow
}
