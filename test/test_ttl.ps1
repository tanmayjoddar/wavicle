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
Send-Command "SET users:temp:name TTLTest"
Send-Command "EXPIRE users:temp:name 5"
Send-Command "TTL users:temp:name"
Write-Host "Waiting 2 seconds..."
Start-Sleep -Seconds 2
Send-Command "TTL users:temp:name"
Write-Host "Waiting 4 seconds (for expiry)..."
Start-Sleep -Seconds 4
Send-Command "GET users:temp:name"

$tcp.Close()
