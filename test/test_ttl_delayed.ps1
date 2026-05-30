$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine("AUTH testpass")
$writer.Flush()
Write-Host "AUTH: " $reader.ReadLine()

$writer.WriteLine("SET users:temp2:name TTLValue")
$writer.Flush()
Write-Host "SET: " $reader.ReadLine()
Start-Sleep -Milliseconds 500

$writer.WriteLine("EXPIRE users:temp2:name 3")
$writer.Flush()
Write-Host "EXPIRE: " $reader.ReadLine()
Start-Sleep -Milliseconds 500

$writer.WriteLine("TTL users:temp2:name")
$writer.Flush()
Write-Host "TTL: " $reader.ReadLine()
$tcp.Close()
