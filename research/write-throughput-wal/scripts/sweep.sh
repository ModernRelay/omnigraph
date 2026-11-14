#!/usr/bin/env bash
# Usage: sweep.sh <label> [backends] [modes] [writers]
# Runs the fixed write-throughput instrument over the standard grid and appends
# JSON lines to research/write-throughput-wal/results/<label>.jsonl.
set -euo pipefail
label=${1:?label}
backends=${2:-"file mem"}
modes=${3:-"same,branches"}
writers=${4:-"1,4,16,64"}
root=$(cd "$(dirname "$0")/../../.." && pwd)
bin=$(ls -t "$root"/target/debug/deps/write_throughput-* | grep -v '\.d$' | head -1)
out="$root/research/write-throughput-wal/results/$label.jsonl"
for backend in $backends; do
  lat=0
  [ "$backend" = mem ] && lat=${OG_WT_LATENCY_MS:-20}
  OG_WT_LABEL=$label OG_WT_BACKEND=$backend OG_WT_LATENCY_MS=$lat OG_WT_MODE=$modes \
    OG_WT_WRITERS=$writers OG_WT_DURATION_SECS=${OG_WT_DURATION_SECS:-10} \
    OG_WT_WARMUP_SECS=${OG_WT_WARMUP_SECS:-2} OG_WT_OUT=$out \
    "$bin" --ignored --nocapture 2>&1 | grep -E "^\[|panicked|error|Error" || true
done
