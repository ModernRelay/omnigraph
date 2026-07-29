#!/usr/bin/env bash
# Emit GitHub-output-compatible classification for the two always-reporting
# firehose checks.

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <base-sha> <head-sha>" >&2
    exit 2
fi

base="$1"
head="$2"
repo_root="${FIREHOSE_REPO_ROOT:-$(git rev-parse --show-toplevel)}"
key_script="${FIREHOSE_KEY_SCRIPT:-$repo_root/scripts/ci/firehose-key.sh}"

cd "$repo_root"

changed_count=0
source_change=false
while IFS= read -r -d '' path; do
    changed_count=$((changed_count + 1))
    printf 'firehose classifier: %s\n' "$path" >&2
    case "$path" in
        docs/*.md|docs/*.mdx|docs/*.rst|docs/*.adoc) ;;
        README.md|AGENTS.md|CLAUDE.md|CHANGELOG.md|CONTRIBUTING.md|CODE_OF_CONDUCT.md) ;;
        LICENSE|LICENSE.md) ;;
        *) source_change=true ;;
    esac
done < <(git diff --name-only -z "$base" "$head")

# Empty/indeterminate diffs fail closed into the bounded source tier.
if [ "$changed_count" -eq 0 ]; then
    source_change=true
fi

base_key="$(FIREHOSE_SOURCE_ONLY=1 "$key_script" "$base")"
head_key="$(FIREHOSE_SOURCE_ONLY=1 "$key_script" "$head")"

if [ "$source_change" = true ]; then
    printf 'firehose_scope=source\n'
else
    printf 'firehose_scope=docs-only\n'
fi
printf 'firehose_base_source_key=%s\n' "$base_key"
printf 'firehose_head_source_key=%s\n' "$head_key"
if [ "$base_key" = "$head_key" ]; then
    printf 'firehose_dependency_inputs_changed=false\n'
else
    printf 'firehose_dependency_inputs_changed=true\n'
fi
