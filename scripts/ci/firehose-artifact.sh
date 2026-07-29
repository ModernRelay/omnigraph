#!/usr/bin/env bash
# Pack, locate, verify, and restore the protected-main dependency artifact.
# Pull-request callers are read-only. Publication is performed by a separate
# main-only workflow job after the exact archive has been produced by the
# workspace job.

set -euo pipefail

command_name="${1:-}"
if [ -z "$command_name" ]; then
    echo "usage: $0 <pack|lookup|verify|restore> ..." >&2
    exit 2
fi
shift

repo_root="${FIREHOSE_REPO_ROOT:-$(git rev-parse --show-toplevel)}"
cargo_home="${FIREHOSE_CARGO_HOME:-${CARGO_HOME:-$HOME/.cargo}}"

file_digest() {
    shasum -a 256 "$1" | awk '{print $1}'
}

metadata_json() {
    local key="$1"
    jq -n \
        --arg format "omnigraph-firehose-dependencies-v1" \
        --arg key "$key" \
        --arg rustc "${FIREHOSE_RUSTC_IDENTITY:-$(rustc -Vv)}" \
        --arg protoc "${FIREHOSE_PROTOC_IDENTITY:-$(protoc --version)}" \
        --arg packages "${FIREHOSE_SYSTEM_PACKAGES:-unknown}" \
        --arg runner "${FIREHOSE_RUNNER_IMAGE:-unknown}" \
        --arg target "${FIREHOSE_TARGET:-x86_64-unknown-linux-gnu}" \
        --arg profile "${FIREHOSE_PROFILE:-test}" \
        --arg features "${FIREHOSE_FEATURES:-omnigraph-engine/failpoints,omnigraph-cluster/failpoints}" \
        --arg flags "${FIREHOSE_FLAGS:---workspace --locked --no-run}" \
        '{
          format: $format,
          key: $key,
          rustc: $rustc,
          protoc: $protoc,
          system_packages: $packages,
          runner_image: $runner,
          target: $target,
          profile: $profile,
          features: $features,
          flags: $flags
        }'
}

case "$command_name" in
    pack)
        if [ "$#" -ne 2 ]; then
            echo "usage: $0 pack <archive.tar.zst> <key>" >&2
            exit 2
        fi
        archive="$1"
        key="$2"
        archive="$(cd "$(dirname "$archive")" && pwd)/$(basename "$archive")"
        mkdir -p "$(dirname "$archive")"

        if [ "${FIREHOSE_SKIP_CARGO_CLEAN:-0}" != "1" ]; then
            clean_args=()
            while IFS= read -r package; do
                clean_args+=(--package "$package")
            done < <(
                cargo metadata --locked --no-deps --format-version 1 |
                    jq -r \
                        '.workspace_members[] as $member
                         | .packages[]
                         | select(.id == $member)
                         | .name'
            )
            if [ "${#clean_args[@]}" -eq 0 ]; then
                echo "error: Cargo metadata returned no workspace packages" >&2
                exit 1
            fi
            cargo clean --target-dir "$repo_root/target" "${clean_args[@]}"
        fi

        test -d "$repo_root/target"
        test -d "$cargo_home/registry"

        metadata_dir="$(mktemp -d)"
        trap 'rm -rf "$metadata_dir"' EXIT
        metadata_json "$key" > "$metadata_dir/firehose-metadata.json"

        tar_args=(
            --zstd
            -cf "$archive"
            -C "$repo_root" target
            -C "$cargo_home" registry
        )
        if [ -d "$cargo_home/git" ]; then
            tar_args+=(-C "$cargo_home" git)
        fi
        tar_args+=(-C "$metadata_dir" firehose-metadata.json)
        tar "${tar_args[@]}"
        printf '%s  %s\n' "$(file_digest "$archive")" "$(basename "$archive")" \
            > "${archive}.sha256"
        ;;

    lookup)
        if [ "$#" -ne 1 ]; then
            echo "usage: $0 lookup <key>" >&2
            exit 2
        fi
        key="$1"
        repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
        artifact_name="firehose-deps-${key}"
        response="$(gh api \
            "repos/${repo}/actions/artifacts?name=${artifact_name}&per_page=100")"
        trusted_matches='[]'
        while IFS=$'\t' read -r artifact_id run_id head_sha; do
            [ -n "$artifact_id" ] || continue
            run="$(gh api "repos/${repo}/actions/runs/${run_id}")"
            if jq -e \
                --arg repo "$repo" \
                --arg sha "$head_sha" \
                '.event == "push"
                 and .path == ".github/workflows/ci.yml"
                 and .head_branch == "main"
                 and .head_sha == $sha
                 and .repository.full_name == $repo
                 and .head_repository.full_name == $repo' \
                <<<"$run" >/dev/null
            then
                jobs="$(
                    gh api \
                        "repos/${repo}/actions/runs/${run_id}/jobs?filter=all&per_page=100"
                )"
                if ! jq -e \
                    '[.jobs[]
                      | select(
                          .name == "Publish protected-main firehose dependencies"
                          and .status == "completed"
                          and .conclusion == "success"
                        )]
                     | length >= 1' \
                    <<<"$jobs" >/dev/null
                then
                    continue
                fi
                trusted_matches="$(
                    jq \
                        --arg artifact_id "$artifact_id" \
                        --arg run_id "$run_id" \
                        --arg head_sha "$head_sha" \
                        '. + [{
                          artifact_id: $artifact_id,
                          run_id: $run_id,
                          head_sha: $head_sha
                        }]' \
                        <<<"$trusted_matches"
                )"
            fi
        done < <(
            jq -r \
                --arg name "$artifact_name" \
                '.artifacts[]
                 | select(.name == $name and (.expired | not))
                 | [.id, .workflow_run.id, .workflow_run.head_sha]
                 | @tsv' \
                <<<"$response"
        )

        count="$(jq 'length' <<<"$trusted_matches")"
        if [ "$count" -gt 1 ]; then
            echo "error: write-once firehose key has $count trusted live artifacts: $artifact_name" >&2
            exit 1
        fi
        if [ "$count" -eq 0 ]; then
            printf 'found=false\n'
            printf 'artifact_name=%s\n' "$artifact_name"
            exit 0
        fi

        artifact_id="$(jq -r '.[0].artifact_id' <<<"$trusted_matches")"
        run_id="$(jq -r '.[0].run_id' <<<"$trusted_matches")"
        head_sha="$(jq -r '.[0].head_sha' <<<"$trusted_matches")"

        printf 'found=true\n'
        printf 'artifact_name=%s\n' "$artifact_name"
        printf 'artifact_id=%s\n' "$artifact_id"
        printf 'run_id=%s\n' "$run_id"
        printf 'head_sha=%s\n' "$head_sha"
        ;;

    verify)
        if [ "$#" -ne 3 ]; then
            echo "usage: $0 verify <archive.tar.zst> <key> <publisher-sha>" >&2
            exit 2
        fi
        archive="$1"
        key="$2"
        publisher_sha="$3"
        repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

        gh attestation verify "$archive" \
            --repo "$repo" \
            --signer-workflow "${repo}/.github/workflows/ci.yml" \
            --signer-digest "$publisher_sha" \
            --source-ref refs/heads/main \
            --source-digest "$publisher_sha" \
            --deny-self-hosted-runners >/dev/null

        checksum="${archive}.sha256"
        test -f "$checksum"
        expected_digest="$(awk 'NR == 1 { print $1 }' "$checksum")"
        test "${#expected_digest}" -eq 64
        test "$(file_digest "$archive")" = "$expected_digest"

        metadata="$(tar --zstd -xOf "$archive" firehose-metadata.json)"
        jq -e \
            --arg key "$key" \
            '.format == "omnigraph-firehose-dependencies-v1" and .key == $key' \
            <<<"$metadata" >/dev/null
        ;;

    restore)
        if [ "$#" -ne 1 ]; then
            echo "usage: $0 restore <archive.tar.zst>" >&2
            exit 2
        fi
        archive="$1"
        unpack_dir="$(mktemp -d)"
        trap 'rm -rf "$unpack_dir"' EXIT

        if tar --zstd -tf "$archive" | grep -Eq '(^/|(^|/)\.\.(/|$))'; then
            echo "error: unsafe path in firehose dependency artifact" >&2
            exit 1
        fi
        tar --zstd -xf "$archive" -C "$unpack_dir"
        test -d "$unpack_dir/target"
        test -d "$unpack_dir/registry"
        test -f "$unpack_dir/firehose-metadata.json"
        if [ -e "$repo_root/target" ] || [ -e "$cargo_home/registry" ]; then
            echo "error: firehose dependency restore requires an empty target/registry" >&2
            exit 1
        fi
        mkdir -p "$cargo_home"
        mv "$unpack_dir/target" "$repo_root/target"
        mv "$unpack_dir/registry" "$cargo_home/registry"
        if [ -d "$unpack_dir/git" ]; then
            test ! -e "$cargo_home/git"
            mv "$unpack_dir/git" "$cargo_home/git"
        fi
        ;;

    *)
        echo "error: unknown command: $command_name" >&2
        exit 2
        ;;
esac
