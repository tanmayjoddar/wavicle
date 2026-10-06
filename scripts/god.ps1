<#
 soak-pg.ps1 - Wavicle soak test, real Postgres topology. ONE knob: -Duration.

 Same test for 10m, 2h, 6h, 7d. Drills are scheduled as FRACTIONS of the duration,
 so the test cases never change, only the clock does.

 TOPOLOGY (all four write paths exercised):
   external SQL writer --> Postgres (docker) --WAL/pgoutput--> Wavicle CDC --> frontier
   app writer (RESP SET/MULTI) --> Wavicle --write-through--> Postgres
   readers (RESP GET, ~100/s) + comparer (Wavicle GET vs live PG SELECT)

  USAGE:
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\god.ps1 -Duration 10m
    ... -Duration 2h | 6h | 7d      (total runtime = Duration + ~4 min for final checks)

  NEEDS: Docker daemon running + postgres:16 image (pulled automatically, needs
  one-time internet) + prebuilt wavicle.exe (go build -o wavicle.exe .).
  NO other dependencies: all SQL goes through `docker exec psql`, all RESP over
  raw TCP, no Python, no Go toolchain at runtime.

 ASCII ONLY. No emoji / em-dashes (they broke PowerShell parsing before).
#>
param(
  [string]$Duration      = '10m',
  [string]$WorkDir       = 'D:\wavicle',
  [string]$Exe           = 'D:\wavicle\wavicle.exe',
  [string]$EvidenceRoot  = 'D:\wavicle\docc\shadow-evidence',
  [string]$PgContainer   = 'wavicle-soak-pg',
  [int]$PgPort           = 5433,
  [string]$PgPassword    = 'soak',
  [string]$PgDsn         = '',
  [string]$Publication   = 'wavicle_pub',
  [string]$Slot          = 'wavicle_slot',
  [int]$WavPort          = 6390,
  [int]$MetricsPort      = 8090,
  [int]$KeysPerRange     = 1000,
  [int]$ExtWriteMs       = 200,
  [int]$AppWritesPerSec  = 5,
  [int]$ReadsPerSec      = 100,
  [int]$CdcLagBoundMs    = 1500,
  [int]$ReadP99BoundMs   = 50,
  [int]$RtoSec           = 120,
  [int]$DriftGraceSec    = 20,
  [double]$RssGrowthPct  = 15,
  [int]$MinFreeGB        = 5,
  [switch]$KeepUp
)

$ErrorActionPreference = 'Stop'
$inv = [Globalization.CultureInfo]::InvariantCulture

# ---------------------------------------------------------------- duration knob
if ($Duration -notmatch '^(\d+)([smhd])$') { throw "bad -Duration '$Duration' (use 10m, 2h, 6h, 7d)" }
$mult = @{ s = 1; m = 60; h = 3600; d = 86400 }
$durSec = [int64]$Matches[1] * $mult[$Matches[2]]
if ($durSec -lt 480) { Write-Warning "Duration under 8m: drills will crowd each other. 10m is the minimum meaningful run." }

$N        = $KeysPerRange
$CanaryId = 3 * $N + 1          # ids 1..N external, N+1..2N app, 2N+1..3N contested, 3N+1 canary
$stamp    = Get-Date -Format 'yyyyMMdd-HHmmss'
$RunId    = "$stamp"
$RunDir   = Join-Path $EvidenceRoot "soak-pg-$Duration-$stamp"
$WavData  = Join-Path $RunDir 'wav-data'
New-Item -ItemType Directory -Path $RunDir, $WavData -Force | Out-Null
$MetricsBase = "http://127.0.0.1:$MetricsPort"
if ($PgDsn -eq '') { $PgDsn = "postgres://postgres:$PgPassword@127.0.0.1:$PgPort/soak?sslmode=disable" }
$env:PGPASSWORD = $PgPassword   # every `docker exec psql` below authenticates through this;
                                # without it all SQL fails (container uses scram-sha-256)

function Log-Event([string]$m) {
  $line = "{0:yyyy-MM-ddTHH:mm:ssZ} | {1}" -f [DateTime]::UtcNow, $m
  Add-Content -Path "$RunDir\events.log" -Value $line
  Write-Host $line
}

# ---------------------------------------------------------------- state
$C = @{ sweeps=0; compared=0; race=0; drift=0; refusedOut=0; refusedIn=0; nilOut=0; ioOut=0; rywViol=0
        setErrOut=0; txFail=0; txOk=0; canaryTimeoutOut=0; unexpectedExit=0; censusFail=0; harnessFail=0
        errorsOut=0; quietViol=0; reads=0; appWrites=0; slotBad=0; maxSlotLag=0; minFreeGB=99999; rebooted=0
        churnFail=0 }
$DrillRes   = New-Object Collections.ArrayList
$CanaryMs   = New-Object Collections.Generic.List[double]
$Rss        = New-Object Collections.Generic.List[object]
$Edges      = @(0.1,0.25,0.5,1,2,5,10,25,50,100,250,500,1000,2500,5000,1e9)
function New-Hist { return @{ c = [long[]]::new($Edges.Count); n = 0 } }
function Hist-Add($h, [double]$ms) { for ($i=0; $i -lt $Edges.Count; $i++) { if ($ms -le $Edges[$i]) { $h.c[$i]++; break } }; $h.n++ }
function Hist-Pct($h, [double]$p) {
  if ($h.n -eq 0) { return 0 }
  $t = [math]::Ceiling($h.n * $p); $a = 0
  for ($i=0; $i -lt $Edges.Count; $i++) { $a += $h.c[$i]; if ($a -ge $t) { return $Edges[$i] } }
  return $Edges[-1]
}
$ReadHist = New-Hist
$script:pend = @{}
$script:drill = $null; $script:recovering = $false; $script:quiet = $false
$script:wavUp = $false; $script:pgDown = $false; $script:writerOn = $false
$script:sweepSeq = 0; $script:lastSweepClean = $false
$script:wav = $null; $script:wavStarts = 0; $script:extProc = $null
$script:appCtr = 0; $script:canaryCtr = 0; $script:lastSeq = -1; $script:lastErrTotal = 0.0
$script:fatal = $null; $script:baseConn = $null
$script:bootTime = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime
$script:clock = [Diagnostics.Stopwatch]::StartNew()

# keep Windows awake for the whole run (auto-clears when this process exits)
try {
  Add-Type -Namespace Win -Name Pwr -MemberDefinition '[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint f);'
  [void][Win.Pwr]::SetThreadExecutionState([uint32]2147483649)
} catch { }

# ---------------------------------------------------------------- docker / psql
function Psql([string]$sql) {
  $ErrorActionPreference = 'Continue'
  $out = & docker exec -e PGAPPNAME=soak_ctl $PgContainer psql -U postgres -d soak -At -v ON_ERROR_STOP=1 -c $sql 2>&1
  if ($LASTEXITCODE -ne 0) { throw "psql failed ($LASTEXITCODE): $out | sql=$sql" }
  return @($out | ForEach-Object { "$_" })
}
function Docker-Quiet { $ErrorActionPreference = 'Continue'; & docker @args 2>&1 | Out-Null; return $LASTEXITCODE }

# ---------------------------------------------------------------- RESP client (persistent socket)
$script:tcp = $null; $script:ns = $null
function Resp-Close { try { if ($script:tcp) { $script:tcp.Close() } } catch {}; $script:tcp = $null; $script:ns = $null }
function Resp-Connect {
  Resp-Close
  $c = New-Object Net.Sockets.TcpClient
  $c.NoDelay = $true; $c.ReceiveTimeout = 3000; $c.SendTimeout = 3000
  $c.Connect('127.0.0.1', $WavPort)
  $script:tcp = $c
  $script:ns = New-Object IO.BufferedStream($c.GetStream(), 8192)
}
function Resp-ReadLine {
  $sb = New-Object Text.StringBuilder
  while ($true) {
    $b = $script:ns.ReadByte()
    if ($b -lt 0) { throw 'eof' }
    if ($b -eq 13) { [void]$script:ns.ReadByte(); break }
    [void]$sb.Append([char]$b)
  }
  return $sb.ToString()
}
function Resp-Read {
  $line = Resp-ReadLine
  $t = $line.Substring(0,1); $r = $line.Substring(1)
  switch ($t) {
    '+' { return @{ t='ok'; v=$r } }
    '-' { return @{ t='err'; v=$r } }
    ':' { return @{ t='int'; v=[int64]$r } }
    '$' {
      $n = [int]$r
      if ($n -lt 0) { return @{ t='nil'; v=$null } }
      $buf = New-Object byte[] ($n + 2); $o = 0
      while ($o -lt $n + 2) { $k = $script:ns.Read($buf, $o, $n + 2 - $o); if ($k -le 0) { throw 'eof' }; $o += $k }
      return @{ t='bulk'; v=[Text.Encoding]::UTF8.GetString($buf, 0, $n) }
    }
    '*' {
      $n = [int]$r
      if ($n -lt 0) { return @{ t='nil'; v=$null } }
      $items = @(); for ($i=0; $i -lt $n; $i++) { $items += ,(Resp-Read) }
      return @{ t='arr'; v=$items }
    }
    default { throw "bad resp line: $line" }
  }
}
function Resp-Cmd([string[]]$a) {
  if (-not $script:tcp -or -not $script:tcp.Connected) { Resp-Connect }
  $sb = New-Object Text.StringBuilder
  [void]$sb.Append("*$($a.Count)`r`n")
  foreach ($x in $a) { $n = [Text.Encoding]::UTF8.GetByteCount($x); [void]$sb.Append("`$$n`r`n$x`r`n") }
  $b = [Text.Encoding]::UTF8.GetBytes($sb.ToString())
  $script:ns.Write($b, 0, $b.Length); $script:ns.Flush()
  return (Resp-Read)
}
function Wav-Get([string]$key) {
  $sw = [Diagnostics.Stopwatch]::StartNew()
  try {
    $r = Resp-Cmd @('GET', $key); $ms = $sw.Elapsed.TotalMilliseconds
    if ($r.t -eq 'bulk') { return @{ k='ok'; v=$r.v; ms=$ms } }
    if ($r.t -eq 'nil')  { return @{ k='nil'; v=$null; ms=$ms } }
    return @{ k='err'; v="$($r.v)"; ms=$ms }
  } catch { Resp-Close; return @{ k='io'; v="$_"; ms=$sw.Elapsed.TotalMilliseconds } }
}
function Wav-Set([string]$key, [string]$val) {
  try {
    $r = Resp-Cmd @('SET', $key, $val)
    if ($r.t -eq 'ok') { return @{ k='ok' } }
    return @{ k='err'; v="$($r.v)" }
  } catch { Resp-Close; return @{ k='io'; v="$_" } }
}
function Get-HttpStatus([string]$url) {
  try { $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3; return [int]$r.StatusCode }
  catch { if ($_.Exception.Response) { return [int]$_.Exception.Response.StatusCode }; return 0 }
}
function Get-MetricsText { try { return (Invoke-WebRequest -Uri "$MetricsBase/metrics" -UseBasicParsing -TimeoutSec 3).Content } catch { return $null } }
function Metric-Agg([string]$txt, [string]$name, [string]$mode) {
  if (-not $txt) { return $null }
  $vals = @()
  foreach ($l in ($txt -split "`n")) {
    if ($l.StartsWith('#')) { continue }
    $m = [regex]::Match($l, '^' + [regex]::Escape($name) + '(\{[^}]*\})?\s+(\S+)')
    if ($m.Success) { $d = 0.0; if ([double]::TryParse($m.Groups[2].Value, [Globalization.NumberStyles]::Float, $inv, [ref]$d) -and -not [double]::IsNaN($d)) { $vals += $d } }
  }
  if ($vals.Count -eq 0) { return $null }
  if ($mode -eq 'max') { return ($vals | Measure-Object -Maximum).Maximum }
  return ($vals | Measure-Object -Sum).Sum
}

# ---------------------------------------------------------------- process control
function Start-Wavicle([string]$tag) {
  $script:wavStarts++
  $env:WAVICLE_DB_TYPE = 'postgres'; $env:WAVICLE_DB_DSN = $PgDsn
  $env:WAVICLE_DB_PUBLICATION = $Publication; $env:WAVICLE_DB_REPLICATION_SLOT = $Slot
  $env:WAVICLE_SERVER_LISTEN = "127.0.0.1:$WavPort"
  $env:WAVICLE_STORAGE_DATA_DIR = $WavData; $env:WAVICLE_SNAPSHOT_PATH = "$WavData\frontier.snapshot"
  $env:WAVICLE_METRICS_ENABLED = 'true'; $env:WAVICLE_METRICS_LISTEN = "127.0.0.1:$MetricsPort"
  $env:WAVICLE_MAX_MEMORY = '500MB'
  $p = Start-Process -FilePath $Exe -WorkingDirectory $WorkDir -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput "$RunDir\wavicle-$($script:wavStarts)-$tag.out.log" -RedirectStandardError "$RunDir\wavicle-$($script:wavStarts)-$tag.err.log"
  foreach ($n in 'WAVICLE_DB_TYPE','WAVICLE_DB_DSN','WAVICLE_DB_PUBLICATION','WAVICLE_DB_REPLICATION_SLOT','WAVICLE_SERVER_LISTEN','WAVICLE_STORAGE_DATA_DIR','WAVICLE_SNAPSHOT_PATH','WAVICLE_METRICS_ENABLED','WAVICLE_METRICS_LISTEN','WAVICLE_MAX_MEMORY') { Remove-Item "Env:\$n" -ErrorAction SilentlyContinue }
  # NOTE: PGPASSWORD is deliberately NOT cleaned here - every `docker exec psql`
  # for the rest of the run authenticates through it.
  $script:wav = $p
  Log-Event "wavicle started pid=$($p.Id) tag=$tag (env-configured, data=$WavData)"
}
function Wait-WavReady([int]$maxSec) {
  $sw = [Diagnostics.Stopwatch]::StartNew()
  while ($sw.Elapsed.TotalSeconds -lt $maxSec) {
    if ($script:wav.HasExited) { return -1 }
    $ok = $false
    try { Resp-Close; $r = Resp-Cmd @('PING'); $ok = ($r.t -eq 'ok' -and $r.v -eq 'PONG') } catch { }
    if ($ok -and (Get-HttpStatus "$MetricsBase/readyz") -eq 200) { return $sw.Elapsed.TotalSeconds }
    Start-Sleep -Milliseconds 500
  }
  return -1
}
function Stop-ExtWriter {
  try { if (-not $script:pgDown) { [void](Psql "select pg_terminate_backend(pid) from pg_stat_activity where application_name='soak_ext_writer'") } } catch { }
  try { if ($script:extProc -and -not $script:extProc.HasExited) { Stop-Process -Id $script:extProc.Id -Force -ErrorAction SilentlyContinue } } catch { }
  $script:extProc = $null; $script:writerOn = $false
}
function Start-ExtWriter {
  Stop-ExtWriter
  $every = [string]::Format($inv, '{0:0.###}', $ExtWriteMs / 1000.0)
  $sqlf = "$RunDir\ext-writer.sql"
  $lines = @('\o /dev/null',
    "update soak_kv set val='ext-'||nextval('soak_seq'), updated_at=now() where id = case when random()<0.5 then 1+floor(random()*$N)::int else 2*$N+1+floor(random()*$N)::int end\watch $every")
  Set-Content -Path $sqlf -Value $lines -Encoding ascii
  $before = [int64](Psql 'select last_value from soak_seq')[0]
  $script:extProc = Start-Process -FilePath 'docker' -PassThru -WindowStyle Hidden -RedirectStandardInput $sqlf `
    -RedirectStandardOutput "$RunDir\ext-writer.out.log" -RedirectStandardError "$RunDir\ext-writer.err.log" `
    -ArgumentList @('exec','-i','-e','PGAPPNAME=soak_ext_writer',$PgContainer,'psql','-U','postgres','-d','soak','-q','-f','-')
  Start-Sleep -Seconds 4
  $after = [int64](Psql 'select last_value from soak_seq')[0]
  if ($after -le $before) { throw "external writer is not advancing soak_seq (psql \watch via stdin failed?) - see $RunDir\ext-writer.err.log" }
  $script:writerOn = $true; $script:lastSeq = $after
  Log-Event "external writer started (every ${every}s, ids ext+contested, backend=soak_ext_writer)"
}

# ---------------------------------------------------------------- workload + checks
function Do-Reads {
  for ($i=0; $i -lt $ReadsPerSec; $i++) {
    $id = Get-Random -Minimum 1 -Maximum (3*$N + 1)
    $g = Wav-Get "soak_kv:${id}:val"
    $C.reads++; Hist-Add $ReadHist $g.ms
    if ($g.k -eq 'err') { if ($script:drill) { $C.refusedIn++ } else { $C.refusedOut++ } }
    elseif ($g.k -eq 'io') { if (-not $script:drill) { $C.ioOut++ } }
    elseif ($g.k -eq 'nil') { if (-not $script:drill) { $C.nilOut++ } }
  }
}
function Do-AppWrites {
  for ($i=0; $i -lt $AppWritesPerSec; $i++) {
    $contested = ((Get-Random -Maximum 100) -lt 30)
    if ($contested) { $id = Get-Random -Minimum (2*$N+1) -Maximum (3*$N+1) } else { $id = Get-Random -Minimum ($N+1) -Maximum (2*$N+1) }
    $script:appCtr++; $val = "app-$RunId-$($script:appCtr)"
    $r = Wav-Set "soak_kv:${id}:val" $val; $C.appWrites++
    if ($r.k -ne 'ok') { if (-not $script:drill) { $C.setErrOut++; Log-Event "SET failed outside drill id=$id $($r.v)" }; continue }
    if (-not $contested) {
      $g = Wav-Get "soak_kv:${id}:val"
      if ($g.k -eq 'ok' -and $g.v -ne $val -and -not $script:drill) { $C.rywViol++; Log-Event "READ-YOUR-WRITE VIOLATION id=$id wrote=$val read=$($g.v)" }
      elseif ($g.k -ne 'ok' -and -not $script:drill) { $C.rywViol++; Log-Event "READ-YOUR-WRITE read failed id=$id kind=$($g.k)" }
    }
  }
}
function Do-Tx {
  $a = Get-Random -Minimum ($N+1) -Maximum (2*$N+1); $b = Get-Random -Minimum ($N+1) -Maximum (2*$N+1)
  if ($a -eq $b) { return }
  $script:appCtr++; $va = "tx-$RunId-$($script:appCtr)a"; $vb = "tx-$RunId-$($script:appCtr)b"
  try {
    $r1 = Resp-Cmd @('MULTI'); $r2 = Resp-Cmd @('SET', "soak_kv:${a}:val", $va); $r3 = Resp-Cmd @('SET', "soak_kv:${b}:val", $vb); $r4 = Resp-Cmd @('EXEC')
    if ($r1.t -eq 'ok' -and $r4.t -eq 'arr') {
      $ga = Wav-Get "soak_kv:${a}:val"; $gb = Wav-Get "soak_kv:${b}:val"
      if ($ga.v -eq $va -and $gb.v -eq $vb) { $C.txOk++ } elseif (-not $script:drill) { $C.txFail++; Log-Event "TX RYW mismatch a=$a b=$b" }
    } elseif (-not $script:drill) { $C.txFail++; Log-Event "TX bad replies: $($r1.t) $($r2.t) $($r3.t) $($r4.t)" }
  } catch { Resp-Close; if (-not $script:drill) { $C.txFail++; Log-Event "TX exception $_" } }
}
function Do-Churn {
  for ($i=0; $i -lt 20; $i++) {
    try {
      $c = New-Object Net.Sockets.TcpClient; $c.ReceiveTimeout = 2000; $c.Connect('127.0.0.1', $WavPort)
      $s = $c.GetStream(); $b = [Text.Encoding]::ASCII.GetBytes("*1`r`n`$4`r`nPING`r`n"); $s.Write($b, 0, $b.Length)
      $buf = New-Object byte[] 16; [void]$s.Read($buf, 0, 16); $c.Close()
    } catch { if (-not $script:drill) { $C.churnFail++ } }
  }
}
function Do-Sweep {
  $ids = New-Object Collections.Generic.HashSet[int]
  1..40 | ForEach-Object { [void]$ids.Add((Get-Random -Minimum 1 -Maximum ($N+1))) }
  1..20 | ForEach-Object { [void]$ids.Add((Get-Random -Minimum ($N+1) -Maximum (2*$N+1))) }
  foreach ($k in @($script:pend.Keys)) { [void]$ids.Add([int]$k) }
  $rows = Psql ("select id::text||'|'||val from soak_kv where id in (" + (($ids | ForEach-Object { $_ }) -join ',') + ")")
  $pg = @{}
  foreach ($r in $rows) { if ($r -match '^(\d+)\|(.*)$') { $pg[[int]$Matches[1]] = $Matches[2] } }
  $now = Get-Date; $clean = $true
  foreach ($id in @($ids)) {
    if (-not $pg.ContainsKey($id)) { continue }
    $g = Wav-Get "soak_kv:${id}:val"; $C.compared++
    if ($g.k -eq 'ok' -and $g.v -eq $pg[$id]) {
      if ($script:pend.ContainsKey($id)) { $script:pend.Remove($id); $C.race++ }
      continue
    }
    $clean = $false
    if ($g.k -eq 'err' -or $g.k -eq 'io') { if ($script:drill) { $C.refusedIn++ } else { $C.refusedOut++ }; continue }
    if (-not $script:pend.ContainsKey($id)) { $script:pend[$id] = @{ first = $now; pg = $pg[$id]; wav = $g.v }; continue }
    $age = ($now - $script:pend[$id].first).TotalSeconds
    if ($age -gt $DriftGraceSec -and -not $script:recovering) {
      $C.drift++
      $rec = @{ ts = [DateTime]::UtcNow.ToString('s'); id = $id; pg = $pg[$id]; wav = $g.v; age_s = [int]$age } | ConvertTo-Json -Compress
      Add-Content -Path "$RunDir\drift.jsonl" -Value $rec
      Log-Event "DRIFT id=$id pg=$($pg[$id]) wav=$($g.v) age=$([int]$age)s"
      $script:pend.Remove($id)
    }
  }
  $C.sweeps++; $script:sweepSeq++
  $script:lastSweepClean = ($clean -and $script:pend.Count -eq 0)
}
function Do-Canary([bool]$inDrill) {
  $script:canaryCtr++; $v = "canary-$RunId-$($script:canaryCtr)"
  $sw = [Diagnostics.Stopwatch]::StartNew()
  [void](Psql "update soak_kv set val='$v', updated_at=now() where id=$CanaryId")
  $lag = -1.0
  while ($sw.Elapsed.TotalSeconds -lt 15) {
    $g = Wav-Get "soak_kv:${CanaryId}:val"
    if ($g.k -eq 'ok' -and $g.v -eq $v) { $lag = $sw.Elapsed.TotalMilliseconds; break }
    Start-Sleep -Milliseconds 5
  }
  if (-not $inDrill) {
    if ($lag -ge 0) { $CanaryMs.Add($lag) } else { $C.canaryTimeoutOut++; Log-Event "CANARY TIMEOUT (15s) outside drill - CDC not delivering" }
  }
  return $lag
}
function Do-Sample {
  $el = [int]$script:clock.Elapsed.TotalSeconds
  $rss = 0; $thr = 0; $hnd = 0
  if ($script:wav) { $p = Get-Process -Id $script:wav.Id -ErrorAction SilentlyContinue
    if ($p) { $rss = $p.WorkingSet64; $thr = $p.Threads.Count; $hnd = $p.HandleCount
      if ($script:wavUp) { $Rss.Add([pscustomobject]@{ t = $el; pid = $p.Id; rss = $rss }) } } }
  $slotLag = -1; $slotAct = 'n/a'; $wal = 0; $dbs = 0; $wr = -1; $ws = -1; $seq = -1
  if (-not $script:pgDown) {
  $o = (Psql $SampleSql)[0].Split('|')
  $slotLag = [int64]$o[0]; $slotAct = $o[1]; $wal = [int64]$o[2]; $dbs = [int64]$o[3]; $wr = [int]$o[4]; $ws = [int]$o[5]; $seq = [int64]$o[6]
  if ($slotLag -gt $C.maxSlotLag) { $C.maxSlotLag = $slotLag }
  if (-not $script:drill) {
    if ($slotAct -eq 'none') { $C.slotBad++; Log-Event 'SLOT MISSING outside drill' }
    elseif ($slotAct -ne 't' -and $script:wavUp) { $C.slotBad++; Log-Event "SLOT INACTIVE outside drill (active=$slotAct)" }
    if ($script:writerOn) {
      if ($wr -ne 1) { $C.censusFail++; Log-Event "WRITER CENSUS: $wr backends (want 1) - restarting writer"; Start-ExtWriter }
      elseif ($seq -le $script:lastSeq) { $C.harnessFail++; Log-Event "WRITER NOT ADVANCING seq=$seq last=$($script:lastSeq)" }
      $script:lastSeq = $seq
    }
    if ($script:wavUp -and $ws -ne 1) { $C.censusFail++; Log-Event "WALSENDER CENSUS: $ws (want exactly 1 Wavicle consumer)" }
  }
  } # else: PG truth unreachable (pause/restart window) - metrics/readyz/disk rows below still record; no failure counted
  $txt = Get-MetricsText
  $err = Metric-Agg $txt 'wavicle_errors_total' 'sum'
  $mem = Metric-Agg $txt 'wavicle_memory_bytes' 'sum'
  $lagM = Metric-Agg $txt 'wavicle_replication_lag_ms' 'max'
  $conn = Metric-Agg $txt 'wavicle_active_connections' 'sum'
  if ($null -ne $err) {
    $delta = if ($err -ge $script:lastErrTotal) { $err - $script:lastErrTotal } else { $err }
    if ($delta -gt 0 -and -not $script:drill -and -not $script:recovering) { $C.errorsOut += $delta; Log-Event "wavicle_errors_total +$delta outside drill" }
    $script:lastErrTotal = $err
  }
  $ready = Get-HttpStatus "$MetricsBase/readyz"
  $free = [math]::Round((Get-PSDrive ($RunDir.Substring(0,1))).Free / 1GB, 1)
  if ($free -lt $C.minFreeGB) { $C.minFreeGB = $free }
  if ((Get-CimInstance Win32_OperatingSystem).LastBootUpTime -ne $script:bootTime) { $C.rebooted++; $script:bootTime = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime; Log-Event 'MACHINE REBOOT DETECTED' }
  $row = "{0:yyyy-MM-ddTHH:mm:ssZ},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19}" -f [DateTime]::UtcNow, $el, $(if ($script:wav) { $script:wav.Id } else { 0 }), $rss, $thr, $hnd, $slotLag, $slotAct, $wal, $dbs, $wr, $ws, $err, $mem, $lagM, $conn, $ready, $free, $script:pend.Count, $script:drill
  Add-Content -Path "$RunDir\metrics.csv" -Value $row
}
function Do-Census {
  $procs = @(Get-Process -Name 'wavicle' -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $Exe })
  $owner = $null
  try { $owner = (Get-NetTCPConnection -LocalPort $WavPort -State Listen -ErrorAction Stop | Select-Object -First 1).OwningProcess } catch { }
  $dstat = ''
  try { $ErrorActionPreference = 'Continue'; $dstat = (& docker stats --no-stream --format '{{.MemUsage}}' $PgContainer 2>&1) -join '' } catch { }
  $ok = ($procs.Count -eq 1 -and $owner -eq $script:wav.Id)
  if (-not $ok) { $C.censusFail++ }
  Log-Event "CENSUS wavicle_procs=$($procs.Count) listener_owner=$owner tracked_pid=$($script:wav.Id) pg_mem=$dstat ok=$ok"
}
function Write-Status {
  $el = [int]$script:clock.Elapsed.TotalSeconds
  $st = @{ ts = [DateTime]::UtcNow.ToString('s'); elapsed_s = $el; total_s = $durSec; drill = $script:drill; sweeps = $C.sweeps; race = $C.race; drift = $C.drift
           pending = $script:pend.Count; canary_n = $CanaryMs.Count; reads = $C.reads; app_writes = $C.appWrites; counters = $C }
  Set-Content -Path "$RunDir\status.json" -Value ($st | ConvertTo-Json -Depth 4) -Encoding ascii
  $p99 = if ($CanaryMs.Count -gt 0) { [int](($CanaryMs | Sort-Object)[[math]::Max(0, [int][math]::Ceiling($CanaryMs.Count * 0.99) - 1)]) } else { 0 }
  Write-Host ("[{0}/{1}s] sweeps={2} race={3} DRIFT={4} pend={5} canary_p99={6}ms reads={7} drill={8}" -f $el, $durSec, $C.sweeps, $C.race, $C.drift, $script:pend.Count, $p99, $C.reads, $script:drill)
}
function Pump {
  $ms = $script:clock.ElapsedMilliseconds
  if ($script:wavUp -and -not $script:drill -and $script:wav.HasExited) {
    $C.unexpectedExit++; Log-Event "UNEXPECTED WAVICLE EXIT code=$($script:wav.ExitCode) - restarting (counts as FAIL)"
    Recover-Wavicle 'unexpected-exit'
    return
  }
  try {
    if ($script:wavUp) {
      if ($ms -ge $script:tRead) { Do-Reads; $script:tRead = $ms + 1000 }
      if (-not $script:quiet -and $ms -ge $script:tApp) { Do-AppWrites; $script:tApp = $ms + 1000 }
      if (-not $script:quiet -and $ms -ge $script:tTx -and -not $script:drill) { Do-Tx; $script:tTx = $ms + 10000 }
      if (-not $script:pgDown -and $ms -ge $script:tSweep) { Do-Sweep; $script:tSweep = $ms + $(if ($script:recovering) { 3000 } else { 5000 }) }
      if (-not $script:quiet -and -not $script:drill -and -not $script:pgDown -and $ms -ge $script:tCanary) { Do-Canary $false | Out-Null; $script:tCanary = $ms + 10000 }
      if (-not $script:drill -and $ms -ge $script:tChurn) { Do-Churn; $script:tChurn = $ms + 30000 }
    }
    if ($ms -ge $script:tSample) { Do-Sample; $script:tSample = $ms + 30000 }
    if (-not $script:drill -and $ms -ge $script:tCensus) { Do-Census; $script:tCensus = $ms + 300000 }
    if ($ms -ge $script:tStatus) { Write-Status; $script:tStatus = $ms + 30000 }
  } catch { $C.harnessFail++; Log-Event "HARNESS EXCEPTION: $_" }
}
function Wait-Converged([int]$maxSec) {
  $script:recovering = $true; $sw = [Diagnostics.Stopwatch]::StartNew(); $clean = 0; $seq = $script:sweepSeq
  while ($sw.Elapsed.TotalSeconds -lt $maxSec) {
    Pump; Start-Sleep -Milliseconds 200
    if ($script:sweepSeq -ne $seq) {
      $seq = $script:sweepSeq
      if ($script:lastSweepClean) { $clean++ } else { $clean = 0 }
      if ($clean -ge 3) { $script:recovering = $false; return $sw.Elapsed.TotalSeconds }
    }
  }
  $script:recovering = $false
  return -1
}

# ---------------------------------------------------------------- drills
function Begin-Drill([string]$n) { $script:drill = $n; Log-Event "=== DRILL START: $n at t=$([int]$script:clock.Elapsed.TotalSeconds)s ===" }
function End-Drill([string]$n, [bool]$ok, [double]$rto, [double]$conv, [string]$detail) {
  [void]$DrillRes.Add([pscustomobject]@{ name = $n; ok = $ok; rto_s = [math]::Round($rto,1); converge_s = [math]::Round($conv,1); detail = $detail })
  Log-Event "=== DRILL END: $n ok=$ok rto=${rto}s converge=${conv}s $detail ==="
  $script:drill = $null
}
function Recover-Wavicle([string]$name) {
  Begin-Drill $name; $script:wavUp = $false
  $sw = [Diagnostics.Stopwatch]::StartNew()
  Start-Wavicle $name
  $rto = Wait-WavReady $RtoSec
  if ($rto -lt 0) { End-Drill $name $false -1 -1 'wavicle never became ready'; return }
  $script:wavUp = $true
  $conv = Wait-Converged $RtoSec
  $lag = if ($conv -ge 0) { Do-Canary $true } else { -1 }
  $ok = ($rto -ge 0 -and $rto -le $RtoSec -and $conv -ge 0 -and $lag -ge 0)
  End-Drill $name $ok $rto $conv "post-recovery canary=${lag}ms"
}
function Drill-Kill([string]$name, [int]$downSec) {
  Begin-Drill $name
  $script:wavUp = $false
  Stop-Process -Id $script:wav.Id -Force
  Log-Event "kill -9 wavicle pid=$($script:wav.Id); external writer keeps writing PG for ${downSec}s"
  Start-Sleep -Seconds $downSec
  Start-Wavicle $name
  $rto = Wait-WavReady $RtoSec
  if ($rto -lt 0) { End-Drill $name $false -1 -1 'wavicle never became ready after kill'; return }
  $script:wavUp = $true
  $conv = Wait-Converged $RtoSec
  $lag = if ($conv -ge 0) { Do-Canary $true } else { -1 }
  $ok = ($rto -le $RtoSec -and $conv -ge 0 -and $lag -ge 0)
  End-Drill $name $ok $rto $conv "post-recovery canary=${lag}ms (stale snapshot + CDC checkpoint replay path)"
}
function Wait-PgBack([int]$maxSec) {
  $sw = [Diagnostics.Stopwatch]::StartNew()
  while ($sw.Elapsed.TotalSeconds -lt $maxSec) { try { [void](Psql 'select 1'); return $true } catch { Start-Sleep -Seconds 2 } }
  return $false
}
function Drill-PgPause([int]$pauseSec) {
  $name = 'pg-pause'; Begin-Drill $name
  $script:pgDown = $true   # PG truth unreachable: sweeps and DB sampling stand down (drill's own loop covers behavior)
  [void](Docker-Quiet pause $PgContainer)
  $served = 0; $refused = 0; $io = 0; $notReady = 0
  $sw = [Diagnostics.Stopwatch]::StartNew()
  while ($sw.Elapsed.TotalSeconds -lt $pauseSec) {
    $g = Wav-Get "soak_kv:$(Get-Random -Minimum 1 -Maximum ($N+1)):val"
    if ($g.k -eq 'ok') { $served++ } elseif ($g.k -eq 'err') { $refused++ } else { $io++ }
    if ((Get-HttpStatus "$MetricsBase/readyz") -ne 200) { $notReady++ }
    Start-Sleep -Seconds 2
  }
  [void](Docker-Quiet unpause $PgContainer)
  $back = Wait-PgBack 90
  $script:pgDown = -not $back
  $alive = -not $script:wav.HasExited
  if ($back -and $script:writerOn) { try { $wr = [int](Psql "select count(*) from pg_stat_activity where application_name='soak_ext_writer'")[0]; if ($wr -ne 1) { Start-ExtWriter } } catch { } }
  $conv = if ($back -and $alive) { Wait-Converged $RtoSec } else { -1 }
  $lag = if ($conv -ge 0) { Do-Canary $true } else { -1 }
  $ok = ($back -and $alive -and $conv -ge 0 -and $lag -ge 0)
  End-Drill $name $ok 0 $conv "during pause: served=$served refused=$refused io=$io readyz_not200=$notReady wavicle_alive=$alive canary=${lag}ms"
}
function Drill-PgRestart {
  $name = 'pg-restart'; Begin-Drill $name
  Stop-ExtWriter
  $script:pgDown = $true
  [void](Docker-Quiet restart -t 10 $PgContainer)
  $back = Wait-PgBack 120
  $script:pgDown = $false
  if (-not $back) { End-Drill $name $false -1 -1 'postgres never came back'; return }
  Start-ExtWriter
  $rto = Wait-WavReady $RtoSec
  $alive = -not $script:wav.HasExited
  $conv = if ($rto -ge 0 -and $alive) { Wait-Converged $RtoSec } else { -1 }
  $lag = if ($conv -ge 0) { Do-Canary $true } else { -1 }
  $ok = ($alive -and $rto -ge 0 -and $conv -ge 0 -and $lag -ge 0)
  End-Drill $name $ok $rto $conv "wavicle_alive=$alive (no crash, CDC backoff+reconnect) canary=${lag}ms"
}
function Drill-Quiet([int]$quietSec) {
  $name = 'quiet-idle'; Begin-Drill $name
  Stop-ExtWriter; $script:quiet = $true
  $sw = [Diagnostics.Stopwatch]::StartNew(); $nextProbe = 0; $viol = 0; $probes = 0
  while ($sw.Elapsed.TotalSeconds -lt $quietSec) {
    Pump
    if ($sw.Elapsed.TotalSeconds -ge $nextProbe) {
      $nextProbe += 10; $probes++
      $g = Wav-Get "soak_kv:$(Get-Random -Minimum 1 -Maximum ($N+1)):val"
      $rd = Get-HttpStatus "$MetricsBase/readyz"
      if ($g.k -ne 'ok' -or $rd -ne 200) { $viol++; $C.quietViol++; Log-Event "QUIET VIOLATION: healthy-but-idle refused (get=$($g.k) readyz=$rd)" }
    }
    Start-Sleep -Milliseconds 250
  }
  Start-ExtWriter; $script:quiet = $false
  $lag = Do-Canary $true
  $ok = ($viol -eq 0 -and $lag -ge 0)
  End-Drill $name $ok 0 0 "probes=$probes violations=$viol post-idle canary=${lag}ms (CDC survived idle)"
}

# ---------------------------------------------------------------- reconcile
function Do-Reconcile([string]$label) {
  $rows = Psql "select id::text||'|'||val from soak_kv where id <= $($N*3)"
  $pg = @{}; foreach ($r in $rows) { if ($r -match '^(\d+)\|(.*)$') { $pg[[int]$Matches[1]] = $Matches[2] } }
  $bad = @()
  foreach ($id in $pg.Keys) { $g = Wav-Get "soak_kv:${id}:val"; if ($g.k -ne 'ok' -or $g.v -ne $pg[$id]) { $bad += $id } }
  if ($bad.Count -gt 0) {
    Log-Event "$label : $($bad.Count) differing on first pass, waiting 15s and re-checking"
    Start-Sleep -Seconds 15
    $rows = Psql "select id::text||'|'||val from soak_kv where id <= $($N*3)"
    $pg = @{}; foreach ($r in $rows) { if ($r -match '^(\d+)\|(.*)$') { $pg[[int]$Matches[1]] = $Matches[2] } }
    $bad = @($bad | Where-Object { $g = Wav-Get "soak_kv:${_}:val"; $g.k -ne 'ok' -or $g.v -ne $pg[$_] })
  }
  Log-Event "$label : $($pg.Count) keys compared, $($bad.Count) persistent differences"
  foreach ($id in ($bad | Select-Object -First 20)) { Log-Event "  RECONCILE DIFF id=$id pg=$($pg[$id])" }
  return $bad.Count
}

# ---------------------------------------------------------------- evaluation
function Rss-Growth {
  $worst = $null
  foreach ($g in ($Rss | Group-Object pid)) {
    $pts = @($g.Group); if ($pts.Count -lt 20) { continue }
    if (($pts[-1].t - $pts[0].t) -lt 600) { continue }
    $pts = @($pts[[int]($pts.Count * 0.2)..($pts.Count - 1)])
    $n = $pts.Count; $sx = 0.0; $sy = 0.0; $sxy = 0.0; $sxx = 0.0
    foreach ($p in $pts) { $sx += $p.t; $sy += $p.rss; $sxy += $p.t * $p.rss; $sxx += $p.t * $p.t }
    $den = $n * $sxx - $sx * $sx; if ($den -eq 0) { continue }
    $slope = ($n * $sxy - $sx * $sy) / $den; $mean = $sy / $n
    $growth = $slope * ($pts[-1].t - $pts[0].t) / $mean * 100
    if ($null -eq $worst -or $growth -gt $worst) { $worst = $growth }
  }
  return $worst
}
$crit = New-Object Collections.ArrayList
function Add-Crit([string]$id, [string]$name, [string]$status, [string]$detail) { [void]$crit.Add([pscustomobject]@{ id = $id; name = $name; status = $status; detail = $detail }) }
function PF([bool]$b) { if ($b) { return 'PASS' } else { return 'FAIL' } }

# ================================================================ MAIN
$SampleSql = "select coalesce((select pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint from pg_replication_slots where slot_name='$Slot'),-1)::text||'|'||coalesce((select active::text from pg_replication_slots where slot_name='$Slot'),'none')||'|'||(select coalesce(sum(size),0) from pg_ls_waldir())::text||'|'||pg_database_size('soak')::text||'|'||(select count(*) from pg_stat_activity where application_name='soak_ext_writer')::text||'|'||(select count(*) from pg_stat_replication)::text||'|'||(select last_value from soak_seq)::text"
"ts,elapsed_s,wav_pid,rss,threads,handles,slot_lag_bytes,slot_active,wal_bytes,db_bytes,ext_writer_backends,walsenders,errors_total,wav_mem_bytes,repl_lag_ms,active_conns,readyz,disk_free_gb,pending,drill" | Out-File "$RunDir\metrics.csv" -Encoding ascii

$lockFile = Join-Path $EvidenceRoot 'soak-pg.lock'
try {
  Log-Event "SOAK START id=$RunId duration=$Duration ($durSec s) dir=$RunDir"
  Log-Event "criteria (frozen before launch): drift=0, reconcile diffs=0, cold-restart reconcile=0, RYW viol=0, canary p99<=${CdcLagBoundMs}ms (upper bound incl. docker-exec overhead), slot lag<=100MB, no unexpected exit, all drills recovered<=${RtoSec}s, errors outside drills=0, RSS growth<=${RssGrowthPct}pct, census clean, free disk>=${MinFreeGB}GB"
  @{ duration = $Duration; keys_per_range = $N; ext_write_ms = $ExtWriteMs; app_writes_per_sec = $AppWritesPerSec; reads_per_sec = $ReadsPerSec
     cdc_lag_bound_ms = $CdcLagBoundMs; rto_s = $RtoSec; drift_grace_s = $DriftGraceSec; rss_growth_pct = $RssGrowthPct; min_free_gb = $MinFreeGB } |
    ConvertTo-Json | Set-Content "$RunDir\criteria.json" -Encoding ascii

  # ---- preflight
  if (Test-Path $lockFile) { $old = Get-Content $lockFile | Select-Object -First 1
    if ($old -match '^\d+$' -and (Get-Process -Id ([int]$old) -ErrorAction SilentlyContinue)) { throw "REFUSING TO START: another soak (pid $old) holds $lockFile" } }
  [string]$PID | Out-File $lockFile -Encoding ascii
  if (-not (Test-Path $Exe)) { throw "wavicle.exe not found at $Exe (build it: go build -o wavicle.exe .)" }
  if ((Docker-Quiet version) -ne 0) { throw 'docker is not available - start Docker Desktop' }
  foreach ($port in @($WavPort, $MetricsPort, $PgPort)) {
    $busy = $false; try { $t = New-Object Net.Sockets.TcpClient; $t.Connect('127.0.0.1', $port); $t.Close(); $busy = $true } catch { }
    if ($busy -and -not ($port -eq $PgPort)) { throw "port $port busy - clear it yourself (script never kills by port)" }
  }
  $free0 = [math]::Round((Get-PSDrive ($RunDir.Substring(0,1))).Free / 1GB, 1)
  if ($free0 -lt ($MinFreeGB + 2)) { throw "only ${free0}GB free" }
  Log-Event "preflight ok: free=${free0}GB. REMINDER: AC power, Windows Update paused, active hours set"

  # ---- postgres (fresh container every run = constant starting state)
  [void](Docker-Quiet rm -f $PgContainer)
  $rc = Docker-Quiet run -d --name $PgContainer -e POSTGRES_PASSWORD=$PgPassword -e POSTGRES_DB=soak -p "127.0.0.1:${PgPort}:5432" postgres:16 -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10
  if ($rc -ne 0) { throw 'docker run postgres failed' }
  $sw = [Diagnostics.Stopwatch]::StartNew(); $ready = $false
  while ($sw.Elapsed.TotalSeconds -lt 120) {
    $ErrorActionPreference = 'Continue'; $logs = (& docker logs $PgContainer 2>&1 | Out-String); $ErrorActionPreference = 'Stop'
    if (([regex]::Matches($logs, 'ready to accept connections')).Count -ge 2) { $ready = $true; break }
    Start-Sleep -Seconds 2
  }
  if (-not $ready -or -not (Wait-PgBack 30)) { throw 'postgres did not become ready' }
  [void](Psql "create table soak_kv(id int primary key, val text not null, updated_at timestamptz not null default now()); alter table soak_kv replica identity full; create sequence soak_seq; insert into soak_kv select g,'seed-'||g,now() from generate_series(1,$($N*3+1)) g; create publication $Publication for table soak_kv")
  Log-Event "postgres ready, schema seeded ($($N*3+1) rows), publication=$Publication"

  # ---- wavicle + writers
  Start-Wavicle 'initial'
  $r0 = Wait-WavReady 120
  if ($r0 -lt 0) { throw "wavicle never became ready (see $RunDir\wavicle-1-initial.err.log). Check -PgDsn / replication DSN" }
  $script:wavUp = $true
  $lag0 = Do-Canary $true
  if ($lag0 -lt 0) { throw "FATAL: initial CDC canary never became visible. Check publication/slot names vs deployments/postgres/init.sql, and that Wavicle maps soak_kv:<id>:val" }
  Log-Event "initial CDC canary visible in $([int]$lag0)ms - topology proven"
  Start-ExtWriter
  $m0 = Get-MetricsText; $script:baseConn = Metric-Agg $m0 'wavicle_active_connections' 'sum'
  $script:lastErrTotal = [double](Metric-Agg $m0 'wavicle_errors_total' 'sum')
  $script:clock.Restart()
  $script:tRead = 0; $script:tApp = 0; $script:tTx = 5000; $script:tSweep = 5000; $script:tCanary = 10000
  $script:tChurn = 15000; $script:tSample = 0; $script:tCensus = 60000; $script:tStatus = 0

  $quietSec = [int][math]::Min(1800, [math]::Max(60, $durSec * 0.08))
  $pauseSec = [int][math]::Min(120, [math]::Max(20, $durSec * 0.02))
  $plan = @(
    @{ at = 0.15; run = { Drill-Kill 'kill9-1' 15 } },
    @{ at = 0.30; run = { Drill-PgPause $pauseSec } },
    @{ at = 0.45; run = { Drill-Quiet $quietSec } },
    @{ at = 0.65; run = { Drill-PgRestart } },
    @{ at = 0.80; run = { Drill-Kill 'kill9-2' 15 } }
  )
  $next = 0
  while ($script:clock.Elapsed.TotalSeconds -lt $durSec) {
    Pump
    if ($next -lt $plan.Count -and $script:clock.Elapsed.TotalSeconds -ge ($durSec * $plan[$next].at)) { & $plan[$next].run; $next++ }
    Start-Sleep -Milliseconds 100
  }
  while ($next -lt $plan.Count) { Log-Event "late drill $next (run too short for schedule)"; & $plan[$next].run; $next++ }

  # ---- final: quiesce, reconcile, cold restart, reconcile
  Log-Event '--- FINAL PHASE: stop all writers, quiesce, full reconcile ---'
  $script:quiet = $true; Stop-ExtWriter
  $qs = [Diagnostics.Stopwatch]::StartNew(); while ($qs.Elapsed.TotalSeconds -lt 60) { Pump; Start-Sleep -Milliseconds 250 }
  $script:reconA = Do-Reconcile 'FINAL-RECONCILE'
  Begin-Drill 'final-cold-restart'; $script:wavUp = $false
  Stop-Process -Id $script:wav.Id -Force; Start-Sleep -Seconds 5; Start-Wavicle 'final-cold'
  $rtoF = Wait-WavReady $RtoSec; $script:wavUp = ($rtoF -ge 0)
  $convF = if ($rtoF -ge 0) { Wait-Converged $RtoSec } else { -1 }
  End-Drill 'final-cold-restart' ($rtoF -ge 0 -and $convF -ge 0) $rtoF $convF 'snapshot + CDC checkpoint cold start'
  $script:reconB = if ($rtoF -ge 0) { Do-Reconcile 'COLD-RESTART-RECONCILE' } else { -1 }
  Resp-Close; Start-Sleep -Seconds 3
  $script:finalConn = Metric-Agg (Get-MetricsText) 'wavicle_active_connections' 'sum'
  Do-Sample
}
catch { $script:fatal = "$_"; Log-Event "FATAL: $_" }
finally {
  # ---- verdict
  $p99c = if ($CanaryMs.Count -gt 0) { ($CanaryMs | Sort-Object)[[math]::Max(0, [int][math]::Ceiling($CanaryMs.Count * 0.99) - 1)] } else { -1 }
  $p50c = if ($CanaryMs.Count -gt 0) { ($CanaryMs | Sort-Object)[[int]($CanaryMs.Count * 0.5)] } else { -1 }
  $rp50 = Hist-Pct $ReadHist 0.5; $rp99 = Hist-Pct $ReadHist 0.99
  if ($script:fatal) { Add-Crit 'FATAL' 'run completed without fatal error' 'FAIL' $script:fatal }
  Add-Crit 'C01' 'persistent drift (Wavicle != Postgres past grace)' (PF ($C.drift -eq 0)) "drift=$($C.drift) races_resolved=$($C.race) compared=$($C.compared) sweeps=$($C.sweeps)"
  Add-Crit 'C02' 'final full reconcile (all keys, PG is truth)' (PF ($script:reconA -eq 0)) "differences=$($script:reconA)"
  Add-Crit 'C03' 'cold-restart reconcile (snapshot + CDC checkpoint)' (PF ($script:reconB -eq 0)) "differences=$($script:reconB)"
  Add-Crit 'C04' 'read-your-write on app writes + MULTI/EXEC' (PF ($C.rywViol -eq 0 -and $C.txFail -eq 0)) "ryw_violations=$($C.rywViol) tx_ok=$($C.txOk) tx_fail=$($C.txFail) app_writes=$($C.appWrites)"
  Add-Crit 'C05' "CDC canary visible, p99 <= ${CdcLagBoundMs}ms (loopback, upper bound)" (PF ($CanaryMs.Count -gt 0 -and $C.canaryTimeoutOut -eq 0 -and $p99c -le $CdcLagBoundMs)) "n=$($CanaryMs.Count) p50=$([int]$p50c)ms p99=$([int]$p99c)ms timeouts=$($C.canaryTimeoutOut)"
  Add-Crit 'C06' 'replication slot healthy (lag <= 100MB, never missing/inactive outside drills)' (PF ($C.maxSlotLag -le 104857600 -and $C.slotBad -eq 0)) "max_lag_bytes=$($C.maxSlotLag) slot_bad_samples=$($C.slotBad)"
  Add-Crit 'C07' 'no unexpected Wavicle exit' (PF ($C.unexpectedExit -eq 0)) "unexpected_exits=$($C.unexpectedExit)"
  $names = @($DrillRes | ForEach-Object { $_.name })
  $allRan = ($names -contains 'kill9-1') -and ($names -contains 'pg-pause') -and ($names -contains 'quiet-idle') -and ($names -contains 'pg-restart') -and ($names -contains 'kill9-2')
  $allOk = (@($DrillRes | Where-Object { -not $_.ok }).Count -eq 0)
  Add-Crit 'C08' "all 5 drills ran and recovered within ${RtoSec}s" (PF ($allRan -and $allOk)) (($DrillRes | ForEach-Object { "$($_.name):ok=$($_.ok),rto=$($_.rto_s)s,conv=$($_.converge_s)s" }) -join ' | ')
  Add-Crit 'C09' 'zero errors outside drills (errors_total, refused reads, nil reads, IO, SET fails)' (PF (($C.errorsOut + $C.refusedOut + $C.nilOut + $C.ioOut + $C.setErrOut + $C.churnFail) -eq 0)) "errors_total_delta=$($C.errorsOut) refused=$($C.refusedOut) nil=$($C.nilOut) io=$($C.ioOut) set_err=$($C.setErrOut) churn_fail=$($C.churnFail)"
  Add-Crit 'C10' 'healthy-but-idle stream keeps serving (quiet drill)' (PF ($C.quietViol -eq 0 -and ($names -contains 'quiet-idle'))) "violations=$($C.quietViol)"
  $g = Rss-Growth
  if ($null -eq $g) { Add-Crit 'C11' "RSS growth <= ${RssGrowthPct}pct" 'SKIP' 'no process segment >= 10 min (run longer than ~1h for a meaningful leak signal)' }
  else { Add-Crit 'C11' "RSS growth <= ${RssGrowthPct}pct (worst process lifetime, linear fit)" (PF ($g -le $RssGrowthPct)) "worst_growth=$([math]::Round($g,1))pct" }
  Add-Crit 'C12' 'census clean (1 Wavicle, 1 writer backend, 1 walsender) and harness healthy' (PF ($C.censusFail -eq 0 -and $C.harnessFail -eq 0)) "census_fail=$($C.censusFail) harness_fail=$($C.harnessFail)"
  Add-Crit 'C13' "host healthy (disk >= ${MinFreeGB}GB, no reboot)" (PF ($C.minFreeGB -ge $MinFreeGB -and $C.rebooted -eq 0)) "min_free_gb=$($C.minFreeGB) reboots=$($C.rebooted)"
  if ($null -eq $script:finalConn) { Add-Crit 'C14' 'no connection leak after churn' 'SKIP' 'metric wavicle_active_connections unavailable' }
  else { Add-Crit 'C14' 'no connection leak after churn (final active conns <= 2)' (PF ($script:finalConn -le 2)) "baseline=$($script:baseConn) final=$($script:finalConn)" }
  Add-Crit 'C15' "read latency p99 <= ${ReadP99BoundMs}ms (client-side, loopback)" (PF ($ReadHist.n -gt 0 -and $rp99 -le $ReadP99BoundMs)) "reads=$($C.reads) p50<=${rp50}ms p99<=${rp99}ms"
  $failed = @($crit | Where-Object { $_.status -eq 'FAIL' })
  $verdict = if ($failed.Count -eq 0) { 'PASS' } else { 'FAIL' }
  $crit | ConvertTo-Json -Depth 3 | Set-Content "$RunDir\verdict.json" -Encoding ascii
  $md = @("# Wavicle soak verdict: $verdict", '', "duration=$Duration run=$RunId host=loopback single-node dev laptop", '', '| id | criterion | status | detail |', '|---|---|---|---|')
  foreach ($c in $crit) { $md += "| $($c.id) | $($c.name) | $($c.status) | $($c.detail) |" }
  Set-Content "$RunDir\report.md" -Value $md -Encoding ascii
  Write-Host ''; Write-Host "================ SOAK VERDICT: $verdict ================"
  $crit | Format-Table id, status, name, detail -AutoSize -Wrap | Out-String | Write-Host
  Write-Host "evidence: $RunDir"
  # ---- teardown (script owns what it started)
  try { Stop-ExtWriter } catch { }
  Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue   # don't leave the DB password in the caller's shell
  if (-not $KeepUp) {
    try { if ($script:wav -and -not $script:wav.HasExited) { Stop-Process -Id $script:wav.Id -Force } } catch { }
    [void](Docker-Quiet rm -f $PgContainer)
    Log-Event 'teardown: wavicle stopped, postgres container removed'
  } else { Log-Event 'KeepUp: wavicle and postgres left running for inspection' }
  Remove-Item $lockFile -Force -ErrorAction SilentlyContinue
  if ($verdict -eq 'FAIL') { exit 1 } else { exit 0 }
}
