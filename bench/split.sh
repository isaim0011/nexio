#!/usr/bin/env bash
set -euo pipefail

# Split-process echo benchmark: server and load generator run as separate
# processes pinned to different cores. Reports throughput AND server CPU
# microseconds per message (utime+stime from /proc), which stays meaningful
# even when the client is the bottleneck.
#
# Usage: bench/split.sh [connections] [messages_per_conn] [msg_size] [runs]
#        SERVER_CORE=0 CLIENT_CORE=1 BACKENDS="nexio_sharded compio_sharded" bench/split.sh

CONNS="${1:-100}"
MSGS="${2:-1000}"
SIZE="${3:-128}"
RUNS="${4:-3}"
SERVER_CORE="${SERVER_CORE:-0}"
CLIENT_CORE="${CLIENT_CORE:-1}"
BACKENDS="${BACKENDS:-tokio_unsharded tokio_sharded nexio_sharded compio_sharded monoio_sharded}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
ulimit -n 65535 2>/dev/null || true

cargo build --release --bench echo >/dev/null 2>&1
BIN="$(ls -t target/release/deps/echo-* 2>/dev/null | grep -v '\.d$' | head -1)"
TICKS="$(getconf CLK_TCK)"

echo "conns=$CONNS msgs/conn=$MSGS size=${SIZE}B runs=$RUNS server_core=$SERVER_CORE client_core=$CLIENT_CORE"
printf "%-18s %12s %14s %10s %10s\n" backend "msg/s" "server_us/msg" "p50_us" "p99_us"

for backend in $BACKENDS; do
  for run in $(seq 1 "$RUNS"); do
    PORT=$((20000 + RANDOM % 20000))
    LOG="$(mktemp)"
    taskset -c "$SERVER_CORE" "$BIN" --server-only "$backend" --port "$PORT" --msg-size "$SIZE" --shards 1 >"$LOG" 2>&1 &
    SPID=$!
    for _ in $(seq 1 100); do grep -q READY "$LOG" 2>/dev/null && break; sleep 0.05; done
    if ! grep -q READY "$LOG"; then echo "$backend: server failed"; cat "$LOG"; kill "$SPID" 2>/dev/null || true; continue; fi
    REALPID="$(awk '/READY/{print $2}' "$LOG")"

    # warm-up pass, not measured
    taskset -c "$CLIENT_CORE" "$BIN" --client-only --port "$PORT" --msg-size "$SIZE" --connections "$CONNS" --messages 50 >/dev/null 2>&1 || true

    read -r U0 S0 < <(awk '{print $14, $15}' "/proc/$REALPID/stat")
    OUT="$(taskset -c "$CLIENT_CORE" "$BIN" --client-only --port "$PORT" --msg-size "$SIZE" --connections "$CONNS" --messages "$MSGS" 2>&1 | grep RESULT || true)"
    read -r U1 S1 < <(awk '{print $14, $15}' "/proc/$REALPID/stat")
    kill "$SPID" 2>/dev/null || true; wait "$SPID" 2>/dev/null || true; rm -f "$LOG"

    TOTAL="$(sed -n 's/.*total=\([0-9]*\).*/\1/p' <<<"$OUT")"
    TPUT="$(sed -n 's/.*throughput=\([0-9]*\).*/\1/p' <<<"$OUT")"
    P50="$(sed -n 's/.*p50_us=\([0-9.]*\).*/\1/p' <<<"$OUT")"
    P99="$(sed -n 's/.*p99_us=\([0-9.]*\).*/\1/p' <<<"$OUT")"
    [ -n "$TOTAL" ] || { echo "$backend: client failed"; continue; }
    CPU_US="$(awk -v u0="$U0" -v s0="$S0" -v u1="$U1" -v s1="$S1" -v t="$TICKS" -v n="$TOTAL" 'BEGIN{printf "%.2f", ((u1-u0)+(s1-s0))*1e6/t/n}')"
    printf "%-18s %12s %14s %10s %10s\n" "$backend" "$TPUT" "$CPU_US" "$P50" "$P99"
  done
done
