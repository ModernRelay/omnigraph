#!/usr/bin/env bash
# Usage: sweep.sh <binary> <label> <backend:file|s3> [extra env assignments...]
# Runs the fixed sweep (writers 1,4,16,64 x same,separate) and appends JSONL to
# research/write-throughput-structure/results/<label>-<backend>.jsonl
set -euo pipefail
BIN=$1; LABEL=$2; BACKEND=$3; shift 3
HERE=$(cd "$(dirname "$0")/.." && pwd)
OUT="$HERE/results/${LABEL}-${BACKEND}.jsonl"
if [ "$BACKEND" = s3 ]; then source "$HERE/scripts/s3env.sh"; fi
env WT_ROOT=/opt/bench/tmp WT_BACKEND="$BACKEND" WT_LABEL="$LABEL" WT_OUT="$OUT" \
  WT_WRITERS="${WT_WRITERS:-1,4,16,64}" WT_MODE="${WT_MODE:-same,separate}" \
  WT_SECS="${WT_SECS:-15}" WT_WARMUP_SECS="${WT_WARMUP_SECS:-3}" WT_HISTORY="${WT_HISTORY:-32}" \
  WT_REPS="${WT_REPS:-1}" WT_TYPES="${WT_TYPES:-1}" "$@" \
  "$BIN" --ignored --exact write_throughput_sweep --nocapture 2>&1 | grep -E "^WT|panicked|VERIFY|error:" || true
