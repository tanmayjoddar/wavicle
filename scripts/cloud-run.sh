#!/usr/bin/env bash
# cloud-run.sh — the CLOUD GATE in one command (see docc/CLOUD_GATE.md).
# Run on a FRESH Ubuntu 22.04/24.04 VM (2vCPU/4GB+, Oracle free tier or droplet).
# What it does: Go toolchain -> build all tools -> docker compose stack ->
#   publication/identity -> seed Redis + Wavicle identically -> cdcbench(500) ->
#   24h soak + shadow + dual-writer detached under gate-logs/.
# What it does NOT do: kill drills, 7-day extension, result interpretation —
#   those are manual, per the runbook. Every phase logs PASS/FAIL; any failure
#   aborts loudly. Never let a silent partial run pose as a gate pass.
set -euo pipefail

GO_VERSION="1.25.0"
AUTH="testpass"          # must match docker-compose WAVICLE_AUTH_PASSWORD
WORK="$HOME/wavicle-gate"
LOGS="$WORK/gate-logs"
REPO="${REPO:-https://github.com/tanmayjoddar/wavicle.git}"
TAG="${TAG:-v0.1.1-pilot}"

mkdir -p "$LOGS"
exec > >(tee -a "$LOGS/console.log") 2>&1

pass() { echo "GATE-PASS: $1"; }
fail() { echo "GATE-FAIL: $1"; exit 1; }

echo "=== phase 0: prereqs ==="
command -v docker >/dev/null || fail "docker missing (apt install docker.io docker-compose-plugin)"
docker compose version >/dev/null || fail "docker compose plugin missing"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64) GOARCH=amd64 ;;
  aarch64) GOARCH=arm64 ;;
  *) fail "unsupported arch $ARCH" ;;
esac
if ! command -v go >/dev/null || ! go version 2>/dev/null | grep -q "go1\.2[5-9]"; then
  echo "installing Go $GO_VERSION ($GOARCH)..."
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz" -o /tmp/go.tgz \
    || fail "Go download failed"
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
  export PATH="$PATH:/usr/local/go/bin"
fi
go version || fail "go toolchain broken"
pass "prereqs (go $(go version | awk '{print $3}'), $GOARCH, compose OK)"

echo "=== phase 1: repo at pinned tag ==="
if [ ! -d "$WORK/repo" ]; then
  git clone "$REPO" "$WORK/repo" || fail "clone failed"
fi
cd "$WORK/repo"
git fetch --tags origin >/dev/null 2>&1 || true
git checkout "$TAG" || fail "tag $TAG missing"
pass "repo at $TAG ($(git rev-parse --short HEAD))"

echo "=== phase 2: build all tools ==="
go build -o wavicle . || fail "wavicle build"
go build -o wavicle-migrate ./cmd/wavicle-migrate/ || fail "migrate build"
go build -o wavicle-bench ./cmd/wavicle-bench/ || fail "bench build"
go build -o wavicle-cdcbench ./cmd/wavicle-cdcbench/ || fail "cdcbench build"
go build -o wavicle-cli ./cmd/wavicle-cli/ || fail "cli build"
pass "binaries built"

echo "=== phase 3: compose stack ==="
docker compose up -d --build || fail "compose up"
for i in $(seq 1 60); do
  if docker exec "$(docker ps -qf 'name=db')" pg_isready -U wavicle -d wavicle >/dev/null 2>&1; then
    break
  fi
  [ "$i" = 60 ] && fail "db never healthy"
  sleep 2
done
for i in $(seq 1 30); do
  (echo > /dev/tcp/127.0.0.1/6379) >/dev/null 2>&1 && break
  [ "$i" = 30 ] && fail "wavicle never bound :6379"
  sleep 2
done
pass "stack healthy"

echo "=== phase 4: publication + identity + slot ==="
export PGPASSWORD=wavicle
psql -h 127.0.0.1 -U wavicle -d wavicle -v ON_ERROR_STOP=1 <<'SQL' || fail "pg setup"
CREATE PUBLICATION IF NOT EXISTS wavicle_proofs FOR ALL TABLES;
ALTER TABLE IF EXISTS users REPLICA IDENTITY FULL;
SQL
psql -h 127.0.0.1 -U wavicle -d wavicle -tAc \
  "SELECT count(*) FROM pg_replication_slots WHERE slot_name='wavicle_slot' AND active;" \
  | grep -q 1 || fail "slot not active (listener failed?)"
unset PGPASSWORD
pass "publication/identity/slot active"

echo "=== phase 5: seed redis + wavicle identically, start dual-writer ==="
docker run -d --name gate-redis -p 6380:6379 redis:7-alpine || fail "redis start"
sleep 3
for i in $(seq 1 200); do
  docker exec gate-redis redis-cli SET "shadow:$i" "seed-$i" >/dev/null \
    || fail "redis seed $i"
  ./wavicle-cli -p 6379 -auth "$AUTH" SET "shadow:$i" "seed-$i" >/dev/null \
    || fail "wavicle seed $i"
done
setsid nohup bash -c 'i=0; while true; do i=$((i+1)); k="shadow:$(( (i*7919 % 200)+1 ))"; v="live-$i"; docker exec gate-redis redis-cli SET "$k" "$v" >/dev/null 2>&1; ./wavicle-cli -p 6379 -auth "$AUTH" SET "$k" "$v" >/dev/null 2>&1; sleep 0.2; done' \
  >"$LOGS/dual-write.log" 2>&1 < /dev/null &
echo $! > "$LOGS/dual-write.pid"
pass "seeded 200x2, dual-writer pid $(cat "$LOGS/dual-write.pid")"

echo "=== phase 6: cdcbench, 500 samples to CSV ==="
export PGPASSWORD=wavicle
./wavicle-cdcbench \
  -dsn "postgres://wavicle@127.0.0.1:5432/wavicle?sslmode=disable" \
  -target 127.0.0.1:6379 -auth "$AUTH" \
  -table users -id gatecdc1 -column name \
  -samples 500 -csv "$LOGS/cdc-lag.csv" || fail "cdcbench (see output above)"
unset PGPASSWORD
pass "cdc-lag.csv written ($(wc -l < "$LOGS/cdc-lag.csv") lines)"

echo "=== phase 7: launch 24h soak + shadow (detached) ==="
export SOAK_DURATION=24h SOAK_SAMPLE_FILE="$LOGS/soak-24h.csv"
echo "ts,duration_s,sets,gets,hits,misses,errors,keys" > "$LOGS/soak-24h.csv"
setsid nohup go test -tags=soak -run 'TestSoak_FrontierCache_2min' ./benchmarks/ -count=1 -v \
  >"$LOGS/soak-24h.log" 2>&1 < /dev/null &
echo $! > "$LOGS/soak.pid"
setsid nohup ./wavicle-migrate -source localhost:6380 -target localhost:6379 \
  -pattern "shadow:*" -count 500 -continuous -interval 60s \
  -get-timeout 50ms -auth "$AUTH" \
  -log "$LOGS/shadow-mismatch.jsonl" -report "$LOGS/shadow-report.json" \
  >"$LOGS/shadow.log" 2>&1 < /dev/null &
echo $! > "$LOGS/shadow.pid"
echo "soak pid $(cat "$LOGS/soak.pid"), shadow pid $(cat "$LOGS/shadow.pid")"
echo "NEXT: wait 24h, run kill/PG-restart drills per docc/CLOUD_GATE.md §2,"
echo "then ./scripts/cloud-collect.sh"
pass "gate running detached (logs in $LOGS)"
