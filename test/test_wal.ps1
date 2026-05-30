Write-Host "Initial WAL size:"
docker exec wavicle-wavicle-1 ls -la /data/

Write-Host "Generating writes to trigger compaction..."
$tcp = New-Object System.Net.Sockets.TcpClient("localhost", 6379)
$stream = $tcp.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine("AUTH testpass")
$writer.Flush()
$reader.ReadLine() | Out-Null

for ($i = 0; $i -lt 100; $i++) {
    # Generate large payload to quickly hit 50MB
    $payload = "A" * 500000 
    $writer.WriteLine("SET users:bulk_$i:name $payload")
    $writer.Flush()
    $reader.ReadLine() | Out-Null
    if ($i % 10 -eq 0) { Write-Host "Written $i large records" }
}
$tcp.Close()

Write-Host "Waiting for compaction..."
Start-Sleep -Seconds 10

Write-Host "Final WAL size:"
docker exec wavicle-wavicle-1 ls -la /data/
docker exec wavicle-wavicle-1 du -sh /data/crystal.log
