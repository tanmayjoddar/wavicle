# Make a request to initialize the metric vectors
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$writer.WriteLine("PING")
$writer.Flush()
$tcp.Close()
Start-Sleep -Milliseconds 500

$m = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content

$required = @(
    "wavicle_requests_total",
    "wavicle_request_duration_seconds_bucket",
    "wavicle_request_duration_seconds_count",
    "wavicle_request_duration_seconds_sum",
    "wavicle_cache_hits_total",
    "wavicle_cache_misses_total",
    "wavicle_replication_lag_ms",
    "wavicle_active_connections",
    "wavicle_db_reconnects_total"
)

foreach ($metric in $required) {
    $found = $m | Select-String $metric
    Write-Host "$metric : $(if ($found) { 'PASS' } else { 'FAIL' })" -ForegroundColor $(if ($found) { "Green" } else { "Red" })
}

$buckets = $m | Select-String "le=" | Select-Object -First 5
Write-Host "`nSample buckets:" 
$buckets | ForEach-Object { Write-Host "  $_" }
