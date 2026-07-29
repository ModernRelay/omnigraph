#!/usr/bin/env bash
# Compute the immutable dependency-environment key used by the firehose PR
# smoke tier. The key intentionally excludes ordinary Rust source: the
# published artifact contains dependencies/build inputs, never a compiled
# workspace result.

set -euo pipefail

repo_root="${FIREHOSE_REPO_ROOT:-$(git rev-parse --show-toplevel)}"
treeish="${1:---worktree}"
source_only="${FIREHOSE_SOURCE_ONLY:-0}"

cd "$repo_root"

is_key_input() {
    case "$1" in
        Cargo.toml|*/Cargo.toml|Cargo.lock|*/Cargo.lock) return 0 ;;
        rust-toolchain|rust-toolchain.toml) return 0 ;;
        .cargo/*|*/.cargo/*) return 0 ;;
        */build.rs|*.proto) return 0 ;;
        .github/workflows/ci.yml|.github/branch-protection.json) return 0 ;;
        scripts/ci/*) return 0 ;;
        *) return 1 ;;
    esac
}

emit_paths() {
    if [ "$treeish" = "--worktree" ]; then
        git ls-files -z
    else
        git ls-tree -r -z --name-only "$treeish"
    fi
}

blob_id() {
    local path="$1"
    if [ "$treeish" = "--worktree" ]; then
        git hash-object --no-filters -- "$path"
    else
        git rev-parse "${treeish}:${path}"
    fi
}

runtime_value() {
    local explicit="$1"
    shift
    if [ -n "$explicit" ]; then
        printf '%s' "$explicit"
    else
        "$@"
    fi
}

{
    printf 'omnigraph-firehose-dependency-key-v1\0'
    while IFS= read -r -d '' path; do
        if is_key_input "$path"; then
            printf 'file\0%s\0%s\0' "$path" "$(blob_id "$path")"
        fi
    done < <(emit_paths)

    if [ "$source_only" != "1" ]; then
        rustc_identity="$(runtime_value "${FIREHOSE_RUSTC_IDENTITY:-}" rustc -Vv)"
        protoc_identity="$(runtime_value "${FIREHOSE_PROTOC_IDENTITY:-}" protoc --version)"
        system_packages="${FIREHOSE_SYSTEM_PACKAGES:-unknown}"
        runner_image="${FIREHOSE_RUNNER_IMAGE:-unknown}"
        target="${FIREHOSE_TARGET:-x86_64-unknown-linux-gnu}"
        profile="${FIREHOSE_PROFILE:-test}"
        features="${FIREHOSE_FEATURES:-omnigraph-engine/failpoints,omnigraph-cluster/failpoints}"
        flags="${FIREHOSE_FLAGS:---workspace --locked --no-run}"

        printf 'rustc\0%s\0' "$rustc_identity"
        printf 'protoc\0%s\0' "$protoc_identity"
        printf 'system-packages\0%s\0' "$system_packages"
        printf 'runner-image\0%s\0' "$runner_image"
        printf 'target\0%s\0' "$target"
        printf 'profile\0%s\0' "$profile"
        printf 'features\0%s\0' "$features"
        printf 'flags\0%s\0' "$flags"
    fi
} | shasum -a 256 | awk '{print $1}'
