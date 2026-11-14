#!/usr/bin/env bash
# Run the fixed instrument for one variant.
#   sweep.sh <label> <local|s3> [writers] [modes] [runs] [duration]
# Appends JSON lines to results/<label>-<target>.jsonl and the human lines to
# results/<label>-<target>.log. The test binary is the engine integration
# binary built by `cargo dev-build-tests` (path cached in /tmp/cw_bin).
set -euo pipefail
LABEL=$1; TARGET=$2
WRITERS=${3:-1,4,16,64}; MODES=${4:-same,separate}; RUNS=${5:-1}; DURATION=${6:-20}
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
RES=$ROOT/research/write-throughput/results
mkdir -p "$RES"
BIN=$(cat /tmp/cw_bin)
cd "$ROOT/crates/omnigraph"
if [ "$TARGET" = s3 ]; then
  source "$ROOT/research/write-throughput/scripts/s3-env.sh"
else
  unset OG_CW_TARGET
fi
export LANCE_MEM_POOL_SIZE=1073741824
OG_CW_LABEL=$LABEL OG_CW_WRITERS=$WRITERS OG_CW_MODES=$MODES OG_CW_RUNS=$RUNS \
OG_CW_DURATION_SECS=$DURATION OG_CW_WARMUP_SECS=${WARMUP:-3} \
OG_CW_OUT=$RES/$LABEL-$TARGET.jsonl \
  "$BIN" concurrent_write_throughput --ignored --nocapture 2>&1 \
  | grep --line-buffered -E "^\[cw\]|panicked|FAILED|error" | tee -a "$RES/$LABEL-$TARGET.log"
