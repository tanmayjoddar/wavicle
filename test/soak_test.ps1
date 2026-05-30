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

try {
    while ($true) {
        $tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
        $stream = $tcp.GetStream()
        $writer = New-Object System.IO.StreamWriter($stream)
        $reader = New-Object System.IO.StreamReader($stream)

        # Authenticate
        $writer.WriteLine("AUTH $authPassword")
        $writer.Flush()
        $authResp = $reader.ReadLine()

        if ($authResp -eq "+OK") {
            # Generate random operations
            for ($i = 0; $i -lt 100; $i++) {
                $id = Get-Random -Minimum 1 -Maximum 1000
                $val = Get-Random -Minimum 1000 -Maximum 9999
                
                # Write
                $writer.WriteLine("SET users:$id:name SoakTest$val")
                $writer.Flush()
                $setResp = $reader.ReadLine()
                if ($setResp -ne "+OK") { $writeErrors++ }

                # Read
                $writer.WriteLine("GET users:$id:name")
                $writer.Flush()
                $reader.ReadLine() | Out-Null # bulk length
                $getResp = $reader.ReadLine()
                if ($getResp -ne "SoakTest$val") { $readErrors++ }

                $totalOps += 2
            }
        }
        $tcp.Close()

        # Print status every ~5 seconds
        if ($watch.ElapsedMilliseconds -gt 5000) {
            $qps = [math]::Round($totalOps / $watch.Elapsed.TotalSeconds)
            
            # Fetch metrics to check memory and WAL size
            try {
                $metrics = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content
                $walSize = [math]::Round([int](($metrics | Select-String "wavicle_wal_size_bytes") -split ' ')[1] / 1MB, 2)
                $activeConns = ($metrics | Select-String "wavicle_active_connections") -split ' ' | Select-Object -Last 1
                $compactions = ($metrics | Select-String "wavicle_compactions_total") -split ' ' | Select-Object -Last 1
                
                Write-Host "[Status] QPS: $qps | Active Conns: $activeConns | WAL Size: ${walSize}MB | Compactions: $compactions | Read Errs: $readErrors | Write Errs: $writeErrors"
            } catch {
                Write-Host "[Status] QPS: $qps | Failed to fetch metrics" -ForegroundColor Red
            }

            $watch.Restart()
            $totalOps = 0
        }
        
        # Sleep slightly to avoid completely overwhelming the local Docker network 
        # (in production, this would be distributed load)
        Start-Sleep -Milliseconds 10 
    }
} finally {
    Write-Host "`nSoak test terminated." -ForegroundColor Cyan
}
