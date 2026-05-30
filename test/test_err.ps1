$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine("AUTH testpass")
$writer.Flush()
$reader.ReadLine() | Out-Null

$payload = "A" * 500000 
$writer.WriteLine("SET users:bulk_err:name $payload")
$writer.Flush()
Write-Host "Response:" $reader.ReadLine()
$tcp.Close()
