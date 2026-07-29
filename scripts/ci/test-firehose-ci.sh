#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fixture="$tmp/repo"
mkdir -p "$fixture/scripts/ci" "$fixture/docs" "$fixture/crates/demo"
cp "$script_dir/firehose-key.sh" "$fixture/scripts/ci/firehose-key.sh"
cp "$script_dir/firehose-classify.sh" "$fixture/scripts/ci/firehose-classify.sh"
printf '[workspace]\nmembers=[]\n' > "$fixture/Cargo.toml"
printf '# lock\n' > "$fixture/Cargo.lock"
printf '[toolchain]\nchannel=\"stable\"\n' > "$fixture/rust-toolchain.toml"
printf '[package]\nname=\"demo\"\nversion=\"0.0.0\"\n' > "$fixture/crates/demo/Cargo.toml"
printf 'fn main() {}\n' > "$fixture/crates/demo/lib.rs"
printf '# docs\n' > "$fixture/docs/readme.md"
printf 'fixture\n' > "$fixture/crates/demo/input.txt"

git -C "$fixture" init -q
git -C "$fixture" config user.email ci@example.invalid
git -C "$fixture" config user.name "CI test"
git -C "$fixture" add .
git -C "$fixture" commit -qm base
base="$(git -C "$fixture" rev-parse HEAD)"

printf 'more docs\n' >> "$fixture/docs/readme.md"
git -C "$fixture" add docs/readme.md
git -C "$fixture" commit -qm docs
docs_head="$(git -C "$fixture" rev-parse HEAD)"
docs_result="$(
    FIREHOSE_REPO_ROOT="$fixture" \
    FIREHOSE_KEY_SCRIPT="$fixture/scripts/ci/firehose-key.sh" \
    "$fixture/scripts/ci/firehose-classify.sh" "$base" "$docs_head"
)"
grep -Fxq 'firehose_scope=docs-only' <<<"$docs_result"
grep -Fxq 'firehose_dependency_inputs_changed=false' <<<"$docs_result"

printf '// source\n' >> "$fixture/crates/demo/lib.rs"
git -C "$fixture" add crates/demo/lib.rs
git -C "$fixture" commit -qm source
source_head="$(git -C "$fixture" rev-parse HEAD)"
source_result="$(
    FIREHOSE_REPO_ROOT="$fixture" \
    FIREHOSE_KEY_SCRIPT="$fixture/scripts/ci/firehose-key.sh" \
    "$fixture/scripts/ci/firehose-classify.sh" "$docs_head" "$source_head"
)"
grep -Fxq 'firehose_scope=source' <<<"$source_result"
grep -Fxq 'firehose_dependency_inputs_changed=false' <<<"$source_result"

printf 'changed fixture\n' >> "$fixture/crates/demo/input.txt"
git -C "$fixture" add crates/demo/input.txt
git -C "$fixture" commit -qm text-fixture
fixture_head="$(git -C "$fixture" rev-parse HEAD)"
fixture_result="$(
    FIREHOSE_REPO_ROOT="$fixture" \
    FIREHOSE_KEY_SCRIPT="$fixture/scripts/ci/firehose-key.sh" \
    "$fixture/scripts/ci/firehose-classify.sh" "$source_head" "$fixture_head"
)"
grep -Fxq 'firehose_scope=source' <<<"$fixture_result"
grep -Fxq 'firehose_dependency_inputs_changed=false' <<<"$fixture_result"

printf '# dependency movement\n' >> "$fixture/Cargo.lock"
git -C "$fixture" add Cargo.lock
git -C "$fixture" commit -qm dependency
dependency_head="$(git -C "$fixture" rev-parse HEAD)"
dependency_result="$(
    FIREHOSE_REPO_ROOT="$fixture" \
    FIREHOSE_KEY_SCRIPT="$fixture/scripts/ci/firehose-key.sh" \
    "$fixture/scripts/ci/firehose-classify.sh" "$fixture_head" "$dependency_head"
)"
grep -Fxq 'firehose_scope=source' <<<"$dependency_result"
grep -Fxq 'firehose_dependency_inputs_changed=true' <<<"$dependency_result"

dry_run="$(FIREHOSE_SMOKE_DRY_RUN=1 "$script_dir/firehose-smoke.sh")"
grep -Fq -- '--workspace' <<<"$dry_run"
grep -Fq -- '--test memwal_stream' <<<"$dry_run"
grep -Fq -- '--test forbidden_apis' <<<"$dry_run"
grep -Fq -- '-p omnigraph-control-authority' <<<"$dry_run"
grep -Fq -- '-p omnigraph-storage' <<<"$dry_run"
grep -Fq -- '-p omnigraph-cluster streaming_' <<<"$dry_run"

fake_workspace="$tmp/workspace"
fake_cargo="$tmp/cargo"
fake_restore="$tmp/restore"
fake_restore_cargo="$tmp/restore-cargo"
mkdir -p "$fake_workspace/target/debug" "$fake_cargo/registry" "$fake_restore"
printf 'dependency\n' > "$fake_workspace/target/debug/dependency.rlib"
printf 'registry\n' > "$fake_cargo/registry/index"
archive="$tmp/firehose-deps.tar.zst"
FIREHOSE_REPO_ROOT="$fake_workspace" \
FIREHOSE_CARGO_HOME="$fake_cargo" \
FIREHOSE_SKIP_CARGO_CLEAN=1 \
FIREHOSE_RUSTC_IDENTITY=test-rustc \
FIREHOSE_PROTOC_IDENTITY=test-protoc \
    "$script_dir/firehose-artifact.sh" pack "$archive" test-key
test -s "$archive"
test -s "${archive}.sha256"
tar --zstd -xOf "$archive" firehose-metadata.json \
    | jq -e '.key == "test-key"' >/dev/null

FIREHOSE_REPO_ROOT="$fake_restore" \
FIREHOSE_CARGO_HOME="$fake_restore_cargo" \
    "$script_dir/firehose-artifact.sh" restore "$archive"
cmp "$fake_workspace/target/debug/dependency.rlib" \
    "$fake_restore/target/debug/dependency.rlib"
cmp "$fake_cargo/registry/index" "$fake_restore_cargo/registry/index"

fake_bin="$tmp/bin"
fake_runs="$tmp/runs"
mkdir -p "$fake_bin" "$fake_runs"
# These single-quoted lines deliberately become the fake executable's source.
# shellcheck disable=SC2016
printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'test "$1" = "api"' \
    'case "$2" in' \
    '  *"/actions/artifacts?"*) cat "$FIREHOSE_FAKE_ARTIFACTS" ;;' \
    '  *"/actions/runs/"*"/jobs?"*)' \
    '    run_path="${2%%/jobs?*}"' \
    '    cat "$FIREHOSE_FAKE_RUNS/${run_path##*/}-jobs.json"' \
    '    ;;' \
    '  *"/actions/runs/"*) cat "$FIREHOSE_FAKE_RUNS/${2##*/}.json" ;;' \
    '  *) exit 2 ;;' \
    'esac' > "$fake_bin/gh"
chmod +x "$fake_bin/gh"

artifact_name="firehose-deps-test-key"
artifacts_json="$tmp/artifacts.json"
printf '%s\n' \
    "{\"artifacts\":[" \
    "{\"id\":1,\"name\":\"$artifact_name\",\"expired\":false,\"workflow_run\":{\"id\":101,\"head_sha\":\"failed-sha\"}}," \
    "{\"id\":2,\"name\":\"$artifact_name\",\"expired\":false,\"workflow_run\":{\"id\":102,\"head_sha\":\"running-sha\"}}," \
    "{\"id\":3,\"name\":\"$artifact_name\",\"expired\":false,\"workflow_run\":{\"id\":103,\"head_sha\":\"trusted-sha\"}}" \
    "]}" > "$artifacts_json"
printf '%s\n' \
    '{"event":"push","path":".github/workflows/ci.yml","head_branch":"main","head_sha":"failed-sha","status":"completed","conclusion":"failure","repository":{"full_name":"test/repo"},"head_repository":{"full_name":"test/repo"}}' \
    > "$fake_runs/101.json"
printf '%s\n' \
    '{"event":"push","path":".github/workflows/ci.yml","head_branch":"main","head_sha":"running-sha","status":"in_progress","conclusion":null,"repository":{"full_name":"test/repo"},"head_repository":{"full_name":"test/repo"}}' \
    > "$fake_runs/102.json"
printf '%s\n' \
    '{"event":"push","path":".github/workflows/ci.yml","head_branch":"main","head_sha":"trusted-sha","status":"in_progress","conclusion":null,"repository":{"full_name":"test/repo"},"head_repository":{"full_name":"test/repo"}}' \
    > "$fake_runs/103.json"
printf '%s\n' \
    '{"jobs":[{"name":"Publish protected-main firehose dependencies","status":"completed","conclusion":"failure"}]}' \
    > "$fake_runs/101-jobs.json"
printf '%s\n' \
    '{"jobs":[{"name":"Publish protected-main firehose dependencies","status":"in_progress","conclusion":null}]}' \
    > "$fake_runs/102-jobs.json"
printf '%s\n' \
    '{"jobs":[{"name":"Publish protected-main firehose dependencies","status":"completed","conclusion":"success"}]}' \
    > "$fake_runs/103-jobs.json"
lookup_result="$(
    PATH="$fake_bin:$PATH" \
    GITHUB_REPOSITORY=test/repo \
    FIREHOSE_FAKE_ARTIFACTS="$artifacts_json" \
    FIREHOSE_FAKE_RUNS="$fake_runs" \
        "$script_dir/firehose-artifact.sh" lookup test-key
)"
grep -Fxq 'found=true' <<<"$lookup_result"
grep -Fxq 'artifact_id=3' <<<"$lookup_result"
grep -Fxq 'run_id=103' <<<"$lookup_result"

printf '%s\n' \
    '{"event":"push","path":".github/workflows/ci.yml","head_branch":"main","head_sha":"second-sha","status":"completed","conclusion":"success","repository":{"full_name":"test/repo"},"head_repository":{"full_name":"test/repo"}}' \
    > "$fake_runs/104.json"
printf '%s\n' \
    '{"jobs":[{"name":"Publish protected-main firehose dependencies","status":"completed","conclusion":"success"}]}' \
    > "$fake_runs/104-jobs.json"
jq \
    --arg name "$artifact_name" \
    '.artifacts += [{
      id: 4,
      name: $name,
      expired: false,
      workflow_run: {id: 104, head_sha: "second-sha"}
    }]' \
    "$artifacts_json" > "$tmp/duplicate-artifacts.json"
if PATH="$fake_bin:$PATH" \
    GITHUB_REPOSITORY=test/repo \
    FIREHOSE_FAKE_ARTIFACTS="$tmp/duplicate-artifacts.json" \
    FIREHOSE_FAKE_RUNS="$fake_runs" \
        "$script_dir/firehose-artifact.sh" lookup test-key >/dev/null 2>&1
then
    echo "error: duplicate trusted write-once bindings were accepted" >&2
    exit 1
fi

if grep -R --line-number -E 'pull_request_target|packages: write' \
    "$repo_root/.github/workflows/ci.yml"; then
    echo "error: PR workflow contains a privileged trigger or package writer" >&2
    exit 1
fi

while IFS= read -r action_ref; do
    if [[ ! "$action_ref" =~ ^[0-9a-f]{40}$ ]]; then
        echo "error: mutable action ref in CI workflow: $action_ref" >&2
        exit 1
    fi
done < <(
    sed -nE 's/^[[:space:]]*uses:[[:space:]]+[^@[:space:]]+@([^[:space:]]+).*/\1/p' \
        "$repo_root/.github/workflows/ci.yml"
)

grep -Fq 'cargo metadata --locked --no-deps' "$script_dir/firehose-artifact.sh"
grep -Fq 'crates/omnigraph-storage/*' "$repo_root/.github/workflows/ci.yml"
grep -Fq 'crates/omnigraph-control-authority/*' "$repo_root/.github/workflows/ci.yml"
grep -Fq "cancel-in-progress: \${{ github.event_name == 'pull_request' }}" \
    "$repo_root/.github/workflows/ci.yml"

# `download-artifact` treats `artifact-ids` as a multi-artifact selection even
# when one ID is supplied; without merge-multiple it inserts an artifact-name
# directory and every exact verification path below it is wrong.
artifact_id_downloads="$(
    grep -c '^[[:space:]]*artifact-ids:' "$repo_root/.github/workflows/ci.yml"
)"
merged_downloads="$(
    grep -c '^[[:space:]]*merge-multiple: true' "$repo_root/.github/workflows/ci.yml"
)"
if [ "$artifact_id_downloads" -ne "$merged_downloads" ]; then
    echo "error: every artifact-id download must merge into its exact path" >&2
    exit 1
fi

publisher_block="$(
    sed -n \
        '/^  firehose_publish_dependencies:/,/^  v9_v10_format_fence:/p' \
        "$repo_root/.github/workflows/ci.yml"
)"
while IFS= read -r job_id; do
    if [ "$job_id" = "firehose_publish_dependencies" ]; then
        continue
    fi
    if ! grep -Fq "      - $job_id" <<<"$publisher_block"; then
        echo "error: publisher is not terminal on CI job: $job_id" >&2
        exit 1
    fi
done < <(
    sed -n '/^jobs:/,$p' "$repo_root/.github/workflows/ci.yml" |
        sed -nE 's/^  ([a-z][a-z0-9_]*):$/\1/p'
)

echo "firehose CI script tests passed"
