#!/bin/sh
# usage: run.sh <out-dir> <case>...  (cases from gen_cases.py in $CASES; binary $GQT_BIN)
# Runs up to $JOBS cases at once.
set -u
out=$1; shift
cases=${CASES:-/src/ndwork/cases}
bin=${GQT_BIN:-target/debug/omnigraph-gqt}
jobs=${JOBS:-4}
mkdir -p "$out"
printf '%s\n' "$@" | xargs -P "$jobs" -I{} sh -c \
  '"$0" "$1/{}.gqt" --measure --artifacts "$2/{}" > "$2/{}.log" 2>&1; echo "{} exit=$? $(date +%T)" >> "$2/done.txt"' \
  "$bin" "$cases" "$out"
