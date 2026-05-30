$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$reader = New-Object System.IO.StreamReader($stream)

function Send-Command($cmd) {
    Write-Host "-> $cmd"
    $writer.WriteLine($cmd)
    $writer.Flush()
    Start-Sleep -Milliseconds 200
    while ($stream.DataAvailable) {
        Write-Host "<-" $reader.ReadLine()
    }
}

Send-Command "AUTH testpass"
Send-Command "SET users:123:name ShutdownTest"
$tcp.Close()

Write-Host "Triggering graceful shutdown..."
docker-compose stop -t 30 wavicle
docker-compose logs wavicle | Select-String -Pattern "shutdown|drain|flush|complete" -CaseSensitive:$false

Write-Host "Restarting..."
docker-compose up -d
Start-Sleep -Seconds 3

$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$reader = New-Object System.IO.StreamReader($stream)

Send-Command "AUTH testpass"
Send-Command "GET users:123:name"
$tcp.Close()
