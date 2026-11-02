#!/bin/sh
# usage: run.sh <out-dir> <case>...   (cases from gen_cases.py in $CASES, default /tmp/ar/cases)
set -u
out=$1; shift
cases=${CASES:-/tmp/ar/cases}
bin=${GQT_BIN:-target/debug/omnigraph-gqt}
mkdir -p "$out"
for c in "$@"; do
  ( "$bin" "$cases/$c.gqt" --measure --artifacts "$out/$c" > "$out/$c.log" 2>&1
    echo "$c exit=$? $(date +%T)" >> "$out/done.txt" ) &
done
wait
