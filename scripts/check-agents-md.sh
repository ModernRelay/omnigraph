#!/usr/bin/env bash
# Verify that AGENTS.md and the docs audience indexes stay in sync.
#
# Checks:
#   1. Every docs/ link from AGENTS.md, docs/user/index.md, and
#      docs/dev/index.md exists.
#   2. Every canonical docs file is discoverable from those indexes.
#
# Release notes are represented by the docs/releases/ directory entry instead
# of requiring every per-version release note to be linked individually.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

index_files=(AGENTS.md docs/user/index.md docs/dev/index.md)
for index_file in "${index_files[@]}"; do
  if [[ ! -f "$index_file" ]]; then
    echo "error: $index_file not found" >&2
    exit 1
  fi
done

normalize_path() {
  python3 - "$1" <<'PY'
import os
import sys

print(os.path.normpath(sys.argv[1]).replace(os.sep, "/"))
PY
}

canonical=()
while IFS= read -r line; do
  canonical+=("$line")
done < <(find docs -type f -name '*.md' ! -path 'docs/releases/*' ! -path 'docs/internal/*' | sort)
if [[ -d docs/releases ]]; then
  canonical+=("docs/releases/")
fi

linked=()
for index_file in "${index_files[@]}"; do
  base_dir="$(dirname "$index_file")"

  # Markdown links.
  while IFS= read -r raw_link; do
    link="${raw_link%%#*}"
    [[ -z "$link" ]] && continue
    [[ "$link" =~ ^[a-zA-Z][a-zA-Z0-9+.-]*: ]] && continue
    [[ "$link" == /* ]] && continue

    if [[ "$link" == docs/* ]]; then
      normalized="$(normalize_path "$link")"
    else
      normalized="$(normalize_path "$base_dir/$link")"
    fi
    if [[ "$link" == */ ]]; then
      normalized="${normalized%/}/"
    fi
    linked+=("$normalized")
  done < <(
    grep -oE '\[[^]]+\]\([^)]+\)' "$index_file" \
      | sed -E 's/.*\(([^)]+)\).*/\1/' || true
  )

  # Agent import directives in AGENTS.md.
  while IFS= read -r raw_link; do
    link="${raw_link#@}"
    linked+=("$(normalize_path "$link")")
  done < <(grep -oE '^@docs/[^[:space:]]+' "$index_file" || true)
done

deduped=()
while IFS= read -r line; do
  deduped+=("$line")
done < <(printf '%s\n' "${linked[@]}" | sort -u)
linked=("${deduped[@]}")

fail=0

for link in "${linked[@]}"; do
  if [[ "$link" == */ ]]; then
    if [[ ! -d "$link" ]]; then
      echo "error: docs index links to missing directory: $link" >&2
      fail=1
    fi
  else
    if [[ ! -f "$link" ]]; then
      echo "error: docs index links to missing file: $link" >&2
      fail=1
    fi
  fi
done

for doc in "${canonical[@]}"; do
  found=0
  for link in "${linked[@]}"; do
    if [[ "$link" == "$doc" ]]; then
      found=1
      break
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    echo "error: doc not linked from AGENTS.md or audience indexes: $doc" >&2
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  echo >&2
  echo "AGENTS.md / docs indexes are out of sync. Update AGENTS.md, docs/user/index.md, or docs/dev/index.md." >&2
  exit 1
fi

echo "AGENTS.md ↔ docs indexes OK (${#linked[@]} links, ${#canonical[@]} docs)."
