#!/usr/bin/env bash
# Known-keys A/B on group11-opt, interleaved per repetition.
set -euo pipefail
REPS=${1:-2}
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
SWEEP=$ROOT/research/write-throughput/scripts/sweep.sh
BIN=/srv/bins/group11-opt
run() { local label=$1 target=$2 writers=$3 mode=$4; shift 4
  echo "$BIN" > /tmp/cw_bin; env "$@" "$SWEEP" "$label" "$target" "$writers" "$mode" 1 20; }
for rep in $(seq 1 "$REPS"); do
  for cell in "local 16,64 same" "s3 16,64 same" "s3 1,16 separate" "local 64 separate"; do
    set -- $cell
    run kk-off "$1" "$2" "$3" OMNIGRAPH_GROUP_COMMIT=1 OMNIGRAPH_KNOWN_KEYS=0
    run kk-on "$1" "$2" "$3" OMNIGRAPH_GROUP_COMMIT=1
    run kk-ordinary "$1" "$2" "$3" OMNIGRAPH_GROUP_COMMIT=0
  done
done
echo KK-DONE
