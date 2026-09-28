#!/usr/bin/env bash
# cloud-collect.sh — gather the gate artifacts into one tarball.
# Run on the VM after the 24h soak + drills (see docc/CLOUD_GATE.md §4).
set -euo pipefail

# Repo root (for `docker compose logs`); artifacts live under $HOME regardless.
cd "$(dirname "$0")/.."
LOGS="$HOME/wavicle-gate/repo/gate-logs"
STAMP="$(date -u +%Y-%m-%d)"
OUT="$HOME/wavicle-gate-artifacts-$STAMP.tar.gz"

[ -d "$LOGS" ] || { echo "no gate-logs dir — did cloud-run.sh execute?"; exit 1; }

echo "--- required artifacts ---"
missing=0
for f in cdc-lag.csv soak-24h.csv shadow-report.json shadow-mismatch.jsonl; do
  if [ -s "$LOGS/$f" ]; then
    echo "OK   $f ($(wc -l < "$LOGS/$f") lines)"
  else
    echo "MISS $f"
    missing=1
  fi
done
[ "$missing" = 0 ] || { echo "collect the missing runs before calling this a gate"; exit 1; }

docker compose logs --no-color > "$LOGS/compose.log" 2>&1 || true
mkdir -p /tmp/gate-art
cp "$LOGS/cdc-lag.csv" "$LOGS/soak-24h.csv" "$LOGS/shadow-report.json" \
   "$LOGS/shadow-mismatch.jsonl" "$LOGS/console.log" "$LOGS/compose.log" /tmp/gate-art/
tar -czf "$OUT" -C /tmp gate-art
echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"
echo "paste the CDC sentence: p50/p99 from cdc-lag.csv, provider, N samples, date"
