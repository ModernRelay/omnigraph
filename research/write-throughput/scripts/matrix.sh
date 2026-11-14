#!/usr/bin/env bash
# Interleaved A/B matrix on the optimized binaries: for each repetition, run
# every (variant, target, mode) cell back to back so session drift hits all
# variants alike. Usage: matrix.sh <reps> <writers> <modes> <targets>
#   e.g. matrix.sh 3 1,4,16,64 same,separate s3,local
# Variants are hard-wired below: label -> binary and env.
set -euo pipefail
REPS=$1; WRITERS=$2; MODES=$3; TARGETS=$4
DUR=${DUR:-20}
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
SWEEP=$ROOT/research/write-throughput/scripts/sweep.sh
run() { # label bin target mode env...
  local label=$1 bin=$2 target=$3 mode=$4; shift 4
  echo "$bin" > /tmp/cw_bin
  env "$@" "$SWEEP" "$label" "$target" "$WRITERS" "$mode" 1 "$DUR"
}
for rep in $(seq 1 "$REPS"); do
  for target in ${TARGETS//,/ }; do
    for mode in ${MODES//,/ }; do
      run opt-baseline /srv/bins/baseline-opt "$target" "$mode" OG_CW_REP="$rep"
      run opt-group /srv/bins/group10-opt "$target" "$mode" OG_CW_REP="$rep" OMNIGRAPH_GROUP_COMMIT=1
      if [ "$mode" = same ]; then
        run opt-baseline-claim /srv/bins/baseline-opt "$target" "$mode" OG_CW_REP="$rep" OG_CW_CLAIM=1
        run opt-group-claim /srv/bins/group10-opt "$target" "$mode" OG_CW_REP="$rep" OMNIGRAPH_GROUP_COMMIT=1 OG_CW_CLAIM=1
        run opt-group-pipe /srv/bins/group10-opt "$target" "$mode" OG_CW_REP="$rep" OMNIGRAPH_GROUP_COMMIT=1 OMNIGRAPH_GROUP_COMMIT_PIPELINE=1
      fi
    done
  done
done
