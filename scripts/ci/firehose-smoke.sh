#!/usr/bin/env bash
# The single bounded source-smoke owner. The dependency-rebuild context invokes
# this on a changed/missing trusted key; otherwise Firehose PR smoke invokes it
# after restoring the protected-main artifact.

set -euo pipefail

workspace_compile=(
    cargo test --workspace --locked
    --features "omnigraph-engine/failpoints,omnigraph-cluster/failpoints"
    --no-run
)
focused_engine_tests=(
    cargo test --locked -p omnigraph-engine --features failpoints
    --test lifecycle
    --test recovery
    --test memwal_stream
    --test failpoints
    --test forbidden_apis
    --
    --nocapture
)
focused_authority_tests=(
    cargo test --locked
    -p omnigraph-control-authority
    -p omnigraph-storage
    --
    --nocapture
)
focused_cluster_tests=(
    cargo test --locked -p omnigraph-cluster streaming_
    --
    --nocapture
)

if [ "${FIREHOSE_SMOKE_DRY_RUN:-0}" = "1" ]; then
    printf '%q ' "${workspace_compile[@]}"
    printf '\n'
    printf '%q ' "${focused_engine_tests[@]}"
    printf '\n'
    printf '%q ' "${focused_authority_tests[@]}"
    printf '\n'
    printf '%q ' "${focused_cluster_tests[@]}"
    printf '\n'
    exit 0
fi

timeout_seconds="${FIREHOSE_SMOKE_TIMEOUT_SECONDS:-780}"
start_seconds="$SECONDS"

run_bounded() {
    local remaining
    remaining=$((timeout_seconds - (SECONDS - start_seconds)))
    if [ "$remaining" -le 0 ]; then
        echo "error: firehose smoke exhausted its aggregate timeout" >&2
        exit 124
    fi
    timeout "$remaining" "$@"
}

run_bounded "${workspace_compile[@]}"
run_bounded "${focused_engine_tests[@]}"
run_bounded "${focused_authority_tests[@]}"
run_bounded "${focused_cluster_tests[@]}"

printf 'firehose smoke completed in %ss\n' "$((SECONDS - start_seconds))"
