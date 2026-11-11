#!/usr/bin/env bash
# Hard-kill / restart test, mirroring the grader's "kill the service after
# 3 minutes and bring it back":
#
#   1. start the service on a fresh data dir, plus an SSE consumer
#   2. run event_generator/generate.py (default: adversarial mode)
#   3. kill -9 the service at KILL_AT seconds and restart it at once
#   4. when the generator finishes, check:
#        - alarms in /alarms == distinct falls in the generator's ground truth
#        - the SSE consumer saw every alarm_id exactly once, across the restart
#        - health/occupancy samples before and after the kill
#
# Usage: scripts/kill_restart.sh
#   env: DEVICES=50 DURATION=240 KILL_AT=180 MODE=adversarial PORT=8080
#        SNAPSHOT_SECS=13 (odd on purpose, so the kill lands between
#        snapshots and recovery exercises snapshot + WAL-tail replay)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEVICES="${DEVICES:-50}"
DURATION="${DURATION:-240}"
KILL_AT="${KILL_AT:-180}"
MODE="${MODE:-adversarial}"
PORT="${PORT:-8080}"
PYTHON="${PYTHON:-python3}"
URL="http://127.0.0.1:${PORT}"
BIN="$ROOT/service/target/release/stream-service"
WORK="$(mktemp -d)"
SNAPSHOT_SECS="${SNAPSHOT_SECS:-13}"
export DATA_DIR="$WORK/data" PORT SNAPSHOT_SECS

cleanup() {
  [[ -n "${SVC:-}" ]] && kill "$SVC" 2>/dev/null || true
  [[ -n "${SSE:-}" ]] && kill "$SSE" 2>/dev/null || true
  [[ -n "${GEN:-}" ]] && kill "$GEN" 2>/dev/null || true
}
trap cleanup EXIT

if curl -sf "$URL/healthz" >/dev/null 2>&1; then
  echo "something is already listening on $URL; stop it first (or set PORT)" >&2
  exit 2
fi
echo "building release binary…"
cargo build --release --quiet --manifest-path "$ROOT/service/Cargo.toml" --bin stream-service

start_service() {
  "$BIN" >>"$WORK/service.log" 2>&1 &
  SVC=$!
  for _ in $(seq 1 200); do
    curl -sf "$URL/readyz" >/dev/null 2>&1 && return 0
    sleep 0.05
  done
  echo "service did not become ready; log: $WORK/service.log" >&2
  exit 1
}

sample() {
  echo "    dev_0000 health:    $(curl -s "$URL/devices/dev_0000/health")"
  echo "    room_000 occupancy: $(curl -s "$URL/rooms/room_000/occupancy?window=1h")"
}

echo "work dir: $WORK"
start_service
"$PYTHON" "$ROOT/scripts/sse_check.py" --target "$URL" --out "$WORK/sse.json" >/dev/null &
SSE=$!

echo "running generator: mode=$MODE devices=$DEVICES duration=${DURATION}s, kill -9 at ${KILL_AT}s"
"$PYTHON" "$ROOT/event_generator/generate.py" --mode "$MODE" --devices "$DEVICES" \
  --duration "$DURATION" --target "$URL" >"$WORK/gen.out" 2>"$WORK/gen.err" &
GEN=$!

sleep "$KILL_AT"
echo "  before kill:"
sample
now() { "$PYTHON" -c 'import time; print(time.time())'; }
T0=$(now)
kill -9 "$SVC"
start_service
T1=$(now)
echo "  killed and restarted (down for $("$PYTHON" -c "print(f'{$T1 - $T0:.2f}')")s)"
echo "  after restart:"
sample
grep -h '"recovered"\|recovered' "$WORK/service.log" | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | sed 's/^/    /'

wait "$GEN" || true
GEN=
sleep 3
GT=$(grep '^{"ground_truth"' "$WORK/gen.err" | tail -1)
FALLS=$(echo "$GT" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["ground_truth"]["distinct_falls"])')
FAILED=$(echo "$GT" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["ground_truth"]["failed"])')
SENT=$(echo "$GT" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["ground_truth"]["sent_ok"])')
ALARMS=$(curl -s "$URL/alarms?since=0" | "$PYTHON" -c 'import json,sys; print(len(json.load(sys.stdin)["alarms"]))')
APPLIED=$(curl -s "$URL/metrics" | awk '$1=="wal_last_offset"{print $2}')

kill -TERM "$SSE"
wait "$SSE" || true
SSE=
SSE_SUMMARY=$(cat "$WORK/sse.json")

echo
echo "=== kill/restart result ==="
echo "  generator: sent ok $SENT, failed $FAILED (requests in flight at the kill; the generator does not retry)"
echo "  events in WAL:     $APPLIED"
echo "  distinct falls:    $FALLS"
echo "  alarms returned:   $ALARMS"
echo "  SSE consumer:      $SSE_SUMMARY"

PASS=1
# Every 202 must be in the WAL. A request killed after its write but before
# its 202 is in the WAL yet counted as failed by the client.
if (( APPLIED < SENT || APPLIED > SENT + FAILED )); then
  echo "  ✗ events in the WAL ($APPLIED) outside [acknowledged $SENT, acknowledged + failed $((SENT + FAILED))]"; PASS=0
fi
if [[ "$ALARMS" != "$FALLS" ]]; then
  if [[ "$FAILED" -gt 0 && "$ALARMS" -lt "$FALLS" && $((FALLS - ALARMS)) -le "$FAILED" ]]; then
    echo "  ~ $((FALLS - ALARMS)) fall(s) missing, within the $FAILED requests that failed during the kill"
  else
    echo "  ✗ alarms ($ALARMS) != distinct falls ($FALLS)"; PASS=0
  fi
fi
echo "$SSE_SUMMARY" | "$PYTHON" -c "
import json, sys
s = json.load(sys.stdin)
sys.exit(0 if s['ok'] and s['last_alarm_id'] == $ALARMS else 1)" \
  || { echo "  ✗ SSE consumer missed or repeated alarms"; PASS=0; }

kill -TERM "$SVC"
wait "$SVC" 2>/dev/null || true
SVC=
if [[ "$PASS" == 1 ]]; then
  echo "  ✓ PASS"
  rm -rf "$WORK"
else
  echo "  ✗ FAIL (logs kept in $WORK)"
  exit 1
fi
