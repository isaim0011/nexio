#!/usr/bin/env bash
set -euo pipefail

# Miri CI Script for nexio pure-Rust verification layer.
# Target: tests/slab_invariants.rs and pure-Rust tests in tests/cancel_on_drop.rs.

echo "=== [1/4] Checking Rust nightly and Miri toolchain ==="
if ! command -v rustup >/dev/null 2>&1; then
    echo "ERROR: rustup is required to run Miri." >&2
    exit 1
fi

if ! rustup toolchain list | grep -q "nightly"; then
    echo "Installing Rust nightly toolchain with Miri and rust-src..."
    rustup toolchain install nightly --component miri --component rust-src
else
    rustup component add miri --toolchain nightly || true
    rustup component add rust-src --toolchain nightly || true
fi

echo "Rust nightly version:"
cargo +nightly --version
cargo +nightly miri --version

echo ""
echo "=== [2/4] Setting Miri configuration flags ==="
# Use a dedicated TMPDIR to avoid workspace conflicts with /tmp
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "${SCRIPT_DIR}/target/tmp"
export TMPDIR="${SCRIPT_DIR}/target/tmp"

# Flags:
# -Zmiri-strict-provenance: catches int-to-pointer casting and pointer provenance violations
# -Zmiri-symbolic-alignment-check: catches unaligned pointer accesses
export MIRIFLAGS="${MIRIFLAGS:- -Zmiri-strict-provenance -Zmiri-symbolic-alignment-check}"
echo "MIRIFLAGS: ${MIRIFLAGS}"
echo "TMPDIR: ${TMPDIR}"

echo ""
echo "=== [3/4] Running Miri on tests/slab_invariants.rs (pure-Rust slab layer) ==="
cargo +nightly miri test --test slab_invariants

echo ""
echo "=== [4/4] Running Miri on pure-Rust tests in tests/cancel_on_drop.rs ==="
cargo +nightly miri test --test cancel_on_drop slab_

echo ""
echo "=== All Miri verification passes succeeded cleanly (0 UB, 0 memory corruption) ==="
