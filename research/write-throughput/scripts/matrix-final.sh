#!/usr/bin/env bash
# Final run on group12-opt (best configuration) beside the baseline,
# interleaved per repetition.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
SWEEP=$ROOT/research/write-throughput/scripts/sweep.sh
run() { local label=$1 bin=$2 target=$3 writers=$4 mode=$5; shift 5
  echo "$bin" > /tmp/cw_bin; env "$@" "$SWEEP" "$label" "$target" "$writers" "$mode" 1 20; }
B=/srv/bins/baseline-opt; G=/srv/bins/group12-opt
for rep in 1 2 3; do
  for target in s3 local; do
    run fin-baseline $B $target 16,64 same
    run fin-best $G $target 16,64 same OMNIGRAPH_GROUP_COMMIT=1
    run fin-best-claim $G $target 16,64 same OMNIGRAPH_GROUP_COMMIT=1 OG_CW_CLAIM=1
    run fin-ordinary $G $target 16,64 same
    if [ "$rep" -le 2 ]; then
      run fin-baseline $B $target 16,64 separate
      run fin-best $G $target 16,64 separate OMNIGRAPH_GROUP_COMMIT=1
      run fin-ordinary $G $target 16,64 separate
    fi
  done
done
for target in s3 local; do
  run fin-best $G $target 1,4 same OMNIGRAPH_GROUP_COMMIT=1
  run fin-best-claim $G $target 1,4 same OMNIGRAPH_GROUP_COMMIT=1 OG_CW_CLAIM=1
  run fin-ordinary $G $target 1,4 same
  run fin-best $G $target 1,4 separate OMNIGRAPH_GROUP_COMMIT=1
  run fin-best $G $target 128,256 same OMNIGRAPH_GROUP_COMMIT=1
  run fin-best-claim $G $target 128,256 same OMNIGRAPH_GROUP_COMMIT=1 OG_CW_CLAIM=1
done
echo FINAL-DONE
