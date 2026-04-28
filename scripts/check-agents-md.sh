#!/usr/bin/env bash
# Verify that AGENTS.md and docs/ stay in sync.
#
# Two checks:
#   1. Every docs/*.md path linked from AGENTS.md exists on disk.
#   2. Every doc in the canonical set is linked from AGENTS.md.
#
# Exit non-zero on any drift.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

agents_file="AGENTS.md"
if [[ ! -f "$agents_file" ]]; then
  echo "error: $agents_file not found" >&2
  exit 1
fi

# Canonical set: every docs/*.md (top-level), plus the releases/ index dir if present.
canonical=()
while IFS= read -r line; do
  canonical+=("$line")
done < <(find docs -mindepth 1 -maxdepth 1 -type f -name '*.md' | sort)
if [[ -d docs/releases ]]; then
  canonical+=("docs/releases/")
fi

# Extract docs/ links from AGENTS.md (markdown link form: (docs/...))
linked=()
while IFS= read -r line; do
  linked+=("$line")
done < <(grep -oE '\(docs/[^)]+\)' "$agents_file" | sed -E 's/^\(|\)$//g' | sort -u)

fail=0

# Check 1: every linked path exists.
for link in "${linked[@]}"; do
  # Strip in-page anchors like #foo
  path="${link%%#*}"
  if [[ "$path" == */ ]]; then
    if [[ ! -d "$path" ]]; then
      echo "error: AGENTS.md links to missing directory: $path" >&2
      fail=1
    fi
  else
    if [[ ! -f "$path" ]]; then
      echo "error: AGENTS.md links to missing file: $path" >&2
      fail=1
    fi
  fi
done

# Check 2: every canonical doc is linked at least once.
for doc in "${canonical[@]}"; do
  found=0
  for link in "${linked[@]}"; do
    path="${link%%#*}"
    if [[ "$path" == "$doc" ]]; then
      found=1
      break
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    echo "error: doc not linked from AGENTS.md: $doc" >&2
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  echo >&2
  echo "AGENTS.md / docs/ are out of sync. Either update AGENTS.md links or rename/remove the doc." >&2
  exit 1
fi

echo "AGENTS.md ↔ docs/ links OK (${#linked[@]} links, ${#canonical[@]} docs)."
