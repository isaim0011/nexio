#!/usr/bin/env bash
# Run nexio's test suite and a short soak test inside real Linux kernels booted
# under QEMU/KVM, to check which kernels nexio actually works on.
#
# Each guest boots with only a tiny init (scripts/kernel_matrix/kinit) that
# mounts /proc, /sys, /dev and /tmp, brings up loopback, runs every statically
# linked (musl) test binary, prints KTEST-* result lines, and powers off.
#
# Requirements: Linux host with /dev/kvm, qemu-kvm (/usr/libexec/qemu-kvm or
# qemu-system-x86_64), cpio, gzip, curl, ar, zstd, xz, and the Rust target
#   rustup target add x86_64-unknown-linux-musl
#
# Usage: scripts/kernel_matrix/run.sh [name=url-base:regex ...]   (defaults below)
#
# Kernels are downloaded from distribution archives as unsigned kernel .debs
# (only the vmlinuz is used). Distributions backport io_uring changes, so a
# pass on "Debian 5.10" means that vendor kernel works, not mainline 5.10.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="${WORK:-$HOME/nexio-kernel-matrix}"
TARGET=x86_64-unknown-linux-musl
QEMU="$(command -v qemu-system-x86_64 || echo /usr/libexec/qemu-kvm)"
mkdir -p "$WORK/kernels" && cd "$ROOT"

echo "== building static test binaries and the soak example"
cargo test --release --target $TARGET --no-run --lib --tests --message-format=json > "$WORK/build.json" 2> "$WORK/build.err" \
  || { cat "$WORK/build.err"; exit 1; }
cargo build --release --target $TARGET --example soak 2> "$WORK/soak_build.err" || { cat "$WORK/soak_build.err"; exit 1; }

echo "== building the guest init"
( cd scripts/kernel_matrix/kinit && cargo build --release --target $TARGET 2> "$WORK/kinit_build.err" ) \
  || { cat "$WORK/kinit_build.err"; exit 1; }

echo "== assembling the initramfs"
rm -rf "$WORK/initfs" && mkdir -p "$WORK/initfs/tests"
cp scripts/kernel_matrix/kinit/target/$TARGET/release/kinit "$WORK/initfs/init"
n=1
for exe in $(grep -o '"executable":"[^"]*"' "$WORK/build.json" | cut -d'"' -f4 | sort -u | grep -v kani_slab); do
  cp "$exe" "$WORK/initfs/tests/$(printf '%02d' $n)_$(basename "$exe" | sed 's/-[0-9a-f]\{16\}$//')"; n=$((n+1))
done
cp target/$TARGET/release/examples/soak "$WORK/initfs/tests/$(printf '%02d' $n)_soak"
( cd "$WORK/initfs" && find . | cpio -o -H newc -R 0:0 2>/dev/null | gzip -9 > "$WORK/initramfs.gz" )

fetch() { # name base-url regex
  local name=$1 base=$2 re=$3 f
  [ -s "$WORK/kernels/vmlinuz-$name" ] && return 0
  f=$(curl -sL "$base" | grep -o "$re" | sort -V | tail -1)
  [ -n "$f" ] || { echo "no kernel package found for $name"; return 1; }
  ( cd "$WORK/kernels" && curl -sL -O "$base$f" && mkdir -p x && cd x && ar x "../$f" &&
    d=$(ls data.tar*) &&
    case $d in *.zst) zstd -dc "$d" | tar -x ./boot;; *.xz) xz -dc "$d" | tar -x ./boot;; *.gz) gzip -dc "$d" | tar -x ./boot;; *) tar -xf "$d" ./boot;; esac &&
    cp boot/vmlinuz-* "../vmlinuz-$name" && cd .. && rm -rf x "$f" )
}
fetch ubuntu-5.15 "http://archive.ubuntu.com/ubuntu/pool/main/l/linux/" 'linux-image-unsigned-5\.15\.0-[0-9]*-generic_[^"]*_amd64\.deb'
fetch debian-6.1  "http://deb.debian.org/debian/pool/main/l/linux/" 'linux-image-6\.1\.0-[0-9]*-amd64-unsigned_[^"<> ]*_amd64\.deb'
fetch debian-5.10 "http://deb.debian.org/debian/pool/main/l/linux/" 'linux-image-5\.10\.0-[0-9]*-amd64-unsigned_[^"<> ]*_amd64\.deb'

fail=0
for k in debian-5.10 ubuntu-5.15 debian-6.1; do
  echo "================ $k"
  out=$(timeout 300 "$QEMU" -M q35 -cpu host -accel kvm -m 1024 -smp 2 -nographic -no-reboot \
        -kernel "$WORK/kernels/vmlinuz-$k" -initrd "$WORK/initramfs.gz" \
        -append "console=ttyS0 panic=-1 rdinit=/init loglevel=3 quiet" 2>&1 | tr -d '\r' || true)
  echo "$out" | grep -E "KTEST-(KERNEL|DONE|FAIL)|^test result|^PASS|^FAIL|baseline|after load|totals|panicked"
  echo "$out" | grep -q "KTEST-DONE failures=0" || fail=1
done
[ $fail -eq 0 ] && echo "ALL KERNELS PASSED" || { echo "KERNEL MATRIX FAILED"; exit 1; }
