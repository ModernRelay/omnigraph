#!/usr/bin/env bash
# Build the engine integration test binary with every crate at opt-level 2
# (the dev profile already builds dependencies at 2; this raises the
# workspace crates too) into a separate target dir, and copy it to
# /srv/bins/<name>. Usage: build-opt.sh <source-dir> <name>
set -euo pipefail
SRC=$1; NAME=$2
cd "$SRC"
export CARGO_TARGET_DIR=/srv/target-opt CARGO_PROFILE_DEV_OPT_LEVEL=2
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst \
  --locked --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints \
  -p omnigraph-engine --test integration --no-run 2>&1 | tail -3
BIN=""
for b in $(ls -t /srv/target-opt/debug/deps/integration-* | grep -v '\.d$'); do
  [ -x "$b" ] || continue
  if [ "$("$b" --list 2>/dev/null | grep -c concurrent_write_throughput)" -gt 0 ]; then BIN=$b; break; fi
done
cp "$BIN" "/srv/bins/$NAME"
echo "built /srv/bins/$NAME from $BIN"
