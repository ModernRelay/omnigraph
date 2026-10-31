#!/bin/sh
# Seeds a fresh worktree's `target/` as an APFS copy-on-write clone of a warm
# checkout's `target/`, so the first build recompiles only the
# workspace crates instead of all ~770 dependencies. The clone shares disk
# blocks with its source until either side rewrites a file.
#
# Run it from the new worktree. Source: $OMNIGRAPH_WARM_TARGET, else the
# `target/` of the repository's main worktree (the first `git worktree list`
# entry). Keep that source warm with `cargo dev-build-tests` on `main`.
#
# Every failure is silent and leaves no partial `target/`: an existing
# `target/`, a missing source, a non-macOS host, or a source on another or a
# non-APFS volume all fall back to an ordinary cold build.
set -u

dest="$(git rev-parse --show-toplevel 2>/dev/null || pwd)/target"
main_worktree=$(git worktree list --porcelain 2>/dev/null | awk 'NR == 1 { sub(/^worktree /, ""); print }')
src="${OMNIGRAPH_WARM_TARGET:-${main_worktree:+$main_worktree/target}}"

[ "$(uname -s)" = Darwin ] || exit 0
[ -n "$src" ] && [ -d "$src/debug" ] || exit 0
[ -e "$dest" ] && exit 0
src_real=$(cd "$src" && pwd -P) || exit 0
[ "$src_real" = "$(cd "$(dirname "$dest")" && pwd -P)/target" ] && exit 0

# `cp -c` clones each file with clonefile(2) but falls back to a full byte
# copy across volumes, so clone only within one APFS volume.
[ "$(stat -f %d "$src_real")" = "$(stat -f %d "$(dirname "$dest")")" ] || exit 0
dev=$(df "$src_real" | awk 'NR == 2 { print $1 }')
/sbin/mount | grep -q "^$dev on .*(apfs," || exit 0
# `-p` keeps the source's mtimes, which Cargo's freshness check depends on:
# an artifact stamped with the clone time would look newer than this
# worktree's checked-out sources, and Cargo would skip rebuilding a
# workspace crate whose source differs from the warm checkout's.
tmp="$dest.cloning.$$"
if cp -c -R -p "$src_real" "$tmp" 2>/dev/null && mv "$tmp" "$dest" 2>/dev/null; then
  echo "seed-target: cloned target/ from $src_real"
else
  rm -rf "$tmp"
fi
exit 0
