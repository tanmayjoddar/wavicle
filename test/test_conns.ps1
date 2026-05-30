$connections = @()
for ($i = 0; $i -lt 20; $i++) {
    $client = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
    $connections += $client
    Write-Host "Connection $($i+1): $($client.Connected)"
}

$extra = New-Object System.Net.Sockets.TcpClient
try {
    $extra.Connect("localhost", 6379)
    $stream = $extra.GetStream()
    $reader = New-Object System.IO.StreamReader($stream)
    $writer = New-Object System.IO.StreamWriter($stream)
    $writer.WriteLine("PING")
    $writer.Flush()
    $response = $reader.ReadLine()
    Write-Host "Extra connection response: $response" -ForegroundColor Red
} catch {
    Write-Host "Extra connection rejected: $_" -ForegroundColor Green
}

$connections | ForEach-Object { $_.Close() }
$extra.Close()
