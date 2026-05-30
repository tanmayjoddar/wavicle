$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$reader = New-Object System.IO.StreamReader($stream)

$commands = "AUTH testpass", "HSET profile:1 theme dark", "HSET profile:1 lang go", "HGET profile:1 theme", "HGETALL profile:1", "MSET profile:2:theme light profile:2:lang rust", "MGET profile:2:theme profile:2:lang"

foreach ($cmd in $commands) {
    Write-Host "-> $cmd"
    $writer.WriteLine($cmd)
    $writer.Flush()
    Start-Sleep -Milliseconds 100
    while ($stream.DataAvailable) {
        Write-Host "<-" $reader.ReadLine()
    }
}
$tcp.Close()
