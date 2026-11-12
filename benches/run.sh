#!/usr/bin/env bash
set -euo pipefail

# benches/run.sh — CI & automated benchmark runner for nexio echo benchmarks
#
# Usage:
#   ./benches/run.sh [OPTIONS]
#
# Examples:
#   ./benches/run.sh --quick
#   ./benches/run.sh --compare-modes --markdown
#   ./benches/run.sh --connections 500 --messages 200 --backends all
#   ./benches/run.sh --backends 3way --connections 100 --messages 500

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "=== nexio Benchmark Runner ==="
echo "Working directory: $(pwd)"
echo "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

# Ensure file descriptor limit is sufficient for high concurrent connections (e.g. 500+ connections)
CURRENT_ULIMIT="$(ulimit -n)"
if [ "${CURRENT_ULIMIT}" -lt 65535 ]; then
    ulimit -n 65535 2>/dev/null || ulimit -n 4096 2>/dev/null || echo "Notice: ulimit -n remained at ${CURRENT_ULIMIT}"
fi
echo "Active file descriptor limit (ulimit -n): $(ulimit -n)"

# Build benchmark binary with release profile
echo "Building benchmark binary (release mode)..."
cargo build --release --bench echo

# Run benchmark with forwarded arguments or default quick run
echo "Running benchmark..."
if [ "$#" -eq 0 ]; then
    cargo bench --bench echo -- --quick --markdown
else
    cargo bench --bench echo -- "$@"
fi

echo "=== Benchmark run complete ==="
