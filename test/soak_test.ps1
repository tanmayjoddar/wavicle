# Wavicle 7-Day Soak Test Simulator
# This script applies sustained read/write pressure to Wavicle to test for memory leaks,
# connection drops, and WAL compaction stability over long periods.

$authPassword = $env:WAVICLE_AUTH_PASSWORD
if (-not $authPassword) { $authPassword = "testpass" }

Write-Host "Starting Wavicle Soak Test..." -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop." -ForegroundColor Yellow

$writeErrors = 0
$readErrors = 0
$totalOps = 0

$watch = [System.Diagnostics.Stopwatch]::StartNew()

# Robust RESP3 parser to avoid desyncs on errors
function Read-Resp($reader) {
    $line = $reader.ReadLine()
    if ($null -eq $line) { return $null }
    
    $type = $line[0]
    if ($type -eq '+' -or $type -eq '-' -or $type -eq ':') {
        return $line
    } elseif ($type -eq '$') {
        $len = [int]$line.Substring(1)
        if ($len -eq -1) { return "$-1" }
        $data = $reader.ReadLine()
        return $data
    } elseif ($type -eq '*') {
        $count = [int]$line.Substring(1)
        $arr = @()
        for ($i = 0; $i -lt $count; $i++) {
            $arr += Read-Resp $reader
        }
        return $arr
    }
    return $line
}

try {
    # Open persistent connection
    $tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
    $stream = $tcp.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $reader = New-Object System.IO.StreamReader($stream)

    # Authenticate
    $writer.WriteLine("AUTH $authPassword")
    $writer.Flush()
    $authResp = Read-Resp $reader

    if ($authResp -ne "+OK") {
        Write-Host "Authentication failed: $authResp" -ForegroundColor Red
        exit 1
    }

    while ($true) {
        if (-not $tcp.Connected) {
            Write-Host "Connection lost. Reconnecting..." -ForegroundColor Yellow
            $tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
            $stream = $tcp.GetStream()
            $writer = New-Object System.IO.StreamWriter($stream)
            $reader = New-Object System.IO.StreamReader($stream)
            $writer.WriteLine("AUTH $authPassword")
            $writer.Flush()
            Read-Resp $reader | Out-Null
        }

        # Generate random operations
        for ($i = 0; $i -lt 100; $i++) {
            $id = Get-Random -Minimum 1 -Maximum 1000
            $val = Get-Random -Minimum 1000 -Maximum 9999
            
            # Write
            $writer.WriteLine("SET users:$id:name SoakTest$val")
            $writer.Flush()
            $setResp = Read-Resp $reader
            if ($setResp -ne "+OK") { $writeErrors++ }

            # Read
            $writer.WriteLine("GET users:$id:name")
            $writer.Flush()
            $getResp = Read-Resp $reader
            if ($getResp -ne "SoakTest$val") { $readErrors++ }

            $totalOps += 2
        }

        # Print status every ~5 seconds
        if ($watch.ElapsedMilliseconds -gt 5000) {
            $qps = [math]::Round($totalOps / $watch.Elapsed.TotalSeconds)
            
            # Fetch metrics to check memory and WAL size
            try {
                $metrics = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content
                $walSize = [math]::Round([int](($metrics | Select-String "wavicle_wal_size_bytes") -split ' ')[1] / 1MB, 2)
                $compactions = ($metrics | Select-String "wavicle_compactions_total") -split ' ' | Select-Object -Last 1
                
                # Track memory footprint 
                $wavicleMemory = (docker stats wavicle-wavicle-1 --no-stream --format "{{.MemUsage}}").Trim()
                
                Write-Host "[Status] QPS: $qps | Mem: $wavicleMemory | WAL: ${walSize}MB | Comps: $compactions | R_Err: $readErrors | W_Err: $writeErrors"
            } catch {
                Write-Host "[Status] QPS: $qps | Failed to fetch metrics" -ForegroundColor Red
            }

            $watch.Restart()
            $totalOps = 0
        }
        
        # Slight throttle
        Start-Sleep -Milliseconds 10 
    }
} finally {
    if ($tcp -ne $null) { $tcp.Close() }
    Write-Host "`nSoak test terminated." -ForegroundColor Cyan
}
