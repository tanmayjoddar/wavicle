$times = @()
for ($i = 0; $i -lt 20; $i++) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    
    # TCP connect + AUTH + PING + disconnect
    $tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
    $stream = $tcp.GetStream()
    $writer = New-Object System.IO.StreamWriter($stream)
    $reader = New-Object System.IO.StreamReader($stream)
    
    $writer.WriteLine("AUTH testpass")
    $writer.Flush()
    $auth = $reader.ReadLine()
    
    $writer.WriteLine("PING")
    $writer.Flush()
    $ping = $reader.ReadLine()
    
    $tcp.Close()
    
    $sw.Stop()
    $times += $sw.Elapsed.TotalMilliseconds * 1000  # Convert to microseconds
    Start-Sleep -Milliseconds 100
}

$avg = ($times | Measure-Object -Average).Average
$min = ($times | Measure-Object -Minimum).Minimum
$max = ($times | Measure-Object -Maximum).Maximum

Write-Host "Wavicle TCP round-trip:"
Write-Host "  Average: $([math]::Round($avg, 1)) μs ($([math]::Round($avg * 1000, 0)) ns)"
Write-Host "  Min: $([math]::Round($min, 1)) μs"
Write-Host "  Max: $([math]::Round($max, 1)) μs"
Write-Host "  vs Redis: $([math]::Round(340 / ($avg / 1000), 1))x"
