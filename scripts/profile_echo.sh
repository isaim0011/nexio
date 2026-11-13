#!/usr/bin/env bash
# Profile the nexio echo server's user-space cost with perf while the split-process client runs.
# Usage: scripts/profile_echo.sh [backend]   (default nexio_sharded)
set -euo pipefail
source ~/.cargo/env
cd "$(dirname "${BASH_SOURCE[0]}")/.."
BACKEND="${1:-nexio_sharded}"
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release --bench echo >/dev/null 2>&1
BIN="$(ls -t target/release/deps/echo-* | grep -v '\.d$' | head -1)"
PORT=$((20000 + RANDOM % 20000))
LOG=$(mktemp)
taskset -c 0 "$BIN" --server-only "$BACKEND" --port "$PORT" --msg-size 128 --shards 1 >"$LOG" 2>&1 &
for _ in $(seq 1 100); do grep -q READY "$LOG" && break; sleep 0.05; done
PID=$(awk '/READY/{print $2}' "$LOG")
taskset -c 1 "$BIN" --client-only --port "$PORT" --msg-size 128 --connections 100 --messages 50 >/dev/null 2>&1 || true
taskset -c 1 "$BIN" --client-only --port "$PORT" --msg-size 128 --connections 100 --messages 3000 >/dev/null 2>&1 &
sleep 0.5
perf record -F 4999 -g -p "$PID" -o /tmp/perf.data -- sleep 3 >/dev/null 2>&1 || true
wait %2 2>/dev/null || true
kill "$PID" 2>/dev/null || true
perf report -i /tmp/perf.data --no-children --sort sym --stdio 2>/dev/null | grep -v "^#" | grep -v "^$" | grep "%" | head -40
