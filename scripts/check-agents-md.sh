#!/usr/bin/env bash
# Verify that AGENTS.md and the docs audience indexes stay in sync.
#
# Checks:
#   1. Every docs/ link from AGENTS.md, docs/user/index.md, and
#      docs/dev/index.md exists.
#   2. Every canonical docs file is discoverable from those indexes.
#   3. The engine crate reads no session-setting environment variable and does
#      not reach the environment through the settings table: the session doors
#      (server startup, CLI) read them once and the engine takes every setting
#      from its session, so no test or example sets one either and
#      instrumentation.rs carries no per-setting override static. The variable
#      list is derived from the compiler's DEFINITIONS table, not copied.
#
# Release notes are represented by the docs/releases/ directory entry instead
# of requiring every per-version release note to be linked individually.
#
# `--self-test` runs the same checks and then feeds check 3 a fixture tree
# holding every forbidden spelling, so a pattern that stops matching is caught
# instead of leaving the guard silently blind.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

self_test=0
case "${1:-}" in
  "") ;;
  --self-test) self_test=1 ;;
  *)
    echo "usage: $(basename "$0") [--self-test]" >&2
    exit 2
    ;;
esac

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
done < <(find docs -type f -name '*.md' ! -path 'docs/releases/*' ! -path 'docs/internal/*' ! -path 'docs/rfcs/*' | sort)
if [[ -d docs/releases ]]; then
  canonical+=("docs/releases/")
fi
# RFCs are a growing collection (like releases): represent the directory, not
# every per-RFC file. The dir must be linked from an audience index.
if [[ -d docs/rfcs ]]; then
  canonical+=("docs/rfcs/")
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

# Session settings: the variables are the `env` column of the compiler's
# DEFINITIONS table, read from the source instead of copied here, plus the
# retired traversal variable that no setting owns any more.
settings_table="crates/omnigraph-compiler/src/settings.rs"
if [[ ! -f "$settings_table" ]]; then
  echo "error: $settings_table not found; the setting variables cannot be derived" >&2
  exit 1
fi
setting_variables=()
while IFS= read -r variable; do
  [[ -z "$variable" ]] && continue
  setting_variables+=("$variable")
done < <(
  grep -oE 'env: "OMNIGRAPH_[A-Z0-9_]+"' "$settings_table" \
    | sed -E 's/.*"(.+)"/\1/' \
    | sort -u
)
if [[ "${#setting_variables[@]}" -eq 0 ]]; then
  echo "error: no \`env: \"OMNIGRAPH_…\"\` row found in $settings_table; the guard would scan nothing" >&2
  exit 1
fi
setting_variables+=("OMNIGRAPH_TRAVERSAL_MODE")

# Scan roots. `--self-test` repoints them at a fixture tree.
engine_src="crates/omnigraph/src"
engine_test_dirs=(crates/omnigraph/tests crates/omnigraph/examples)
engine_instrumentation="crates/omnigraph/src/instrumentation.rs"

# Sets `engine_fail` and reports every violation under the current roots.
scan_engine_settings() {
  engine_fail=0
  for variable in "${setting_variables[@]}"; do
    while IFS= read -r hit; do
      [[ -z "$hit" ]] && continue
      echo "error: $engine_src reads $variable; it is a session setting (or the retired traversal variable); take it from the session: $hit" >&2
      engine_fail=1
    done < <(grep -rnE "(std::env::var|env::var|var_os)\(\s*\"$variable\"" "$engine_src" || true)
    # `EnvGuard::set(&[` lists its pairs on the following lines, hence the
    # trailing context window.
    while IFS= read -r hit; do
      [[ -z "$hit" ]] && continue
      echo "error: a test or example sets $variable; it is a session setting (or the retired traversal variable); pass it through db.session(..): $hit" >&2
      engine_fail=1
    done < <(grep -rnE -A 8 "(EnvGuard::set|set_var|remove_var)\b" "${engine_test_dirs[@]}" 2>/dev/null | grep -F "\"$variable\"" || true)
  done
  # The table is not a second door: no `from_env`, no `DEFINITIONS[..].env`.
  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    echo "error: $engine_src reaches the environment through the settings table; take every setting from the session: $hit" >&2
    engine_fail=1
  done < <(grep -rnE "settings::from_env(_with)?[[:space:]]*\(|use[^;]*settings::[^;]*from_env|(DEFINITIONS\[[^]]*\]|spec\(\)|definition\(\))[[:space:]]*\.[[:space:]]*env\b" "$engine_src" || true)
  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    echo "error: $engine_instrumentation carries a per-setting override static; the session owns rrf_plan and stage_write_concurrency: $hit" >&2
    engine_fail=1
  done < <(grep -nE "\b(TRAVERSAL_MODE_OVERRIDE|RRF_PLAN_OVERRIDE|STAGE_WRITE_CONCURRENCY_OVERRIDE)\b" "$engine_instrumentation" || true)
}

scan_engine_settings
if [[ "$engine_fail" -ne 0 ]]; then
  echo >&2
  echo "The engine crate reads a session setting outside its session. Read it from the Session (docs/user/queries/index.md, Session settings)." >&2
  exit 1
fi

echo "AGENTS.md ↔ docs indexes OK (${#linked[@]} links, ${#canonical[@]} docs); engine reads no setting variable."

if [[ "$self_test" -eq 0 ]]; then
  exit 0
fi

# Negative self-test: each spelling below must be refused under a fixture
# tree, and a fixture tree without them must pass.
fixture_root="$(mktemp -d "${TMPDIR:-/tmp}/check-agents-md.XXXXXX")"
trap 'rm -rf "$fixture_root"' EXIT
probe_variable="${setting_variables[0]}"

reset_fixture() {
  rm -rf "$fixture_root/probe"
  mkdir -p "$fixture_root/probe/src" "$fixture_root/probe/tests" "$fixture_root/probe/examples"
  : > "$fixture_root/probe/src/instrumentation.rs"
  : > "$fixture_root/probe/src/lib.rs"
  : > "$fixture_root/probe/tests/settings.rs"
  engine_src="$fixture_root/probe/src"
  engine_test_dirs=("$fixture_root/probe/tests" "$fixture_root/probe/examples")
  engine_instrumentation="$fixture_root/probe/src/instrumentation.rs"
}

self_test_fail=0
probes_run=0
probe_refuses() {
  local label="$1"
  local relative="$2"
  local line="$3"
  reset_fixture
  printf '%s\n' "$line" >> "$fixture_root/probe/$relative"
  scan_engine_settings 2>/dev/null
  probes_run=$((probes_run + 1))
  if [[ "$engine_fail" -eq 0 ]]; then
    echo "error: self-test: the guard accepted a forbidden spelling ($label): $line" >&2
    self_test_fail=1
  fi
}

probe_refuses "std::env::var" src/lib.rs "    let mode = std::env::var(\"$probe_variable\").ok();"
probe_refuses "env::var" src/lib.rs "    let mode = env::var(\"$probe_variable\").ok();"
probe_refuses "var_os" src/lib.rs "    let mode = std::env::var_os(\"$probe_variable\");"
probe_refuses "test sets the variable" tests/settings.rs "    let _guard = EnvGuard::set(&[(\"$probe_variable\", \"off\")]);"
probe_refuses "settings::from_env" src/lib.rs "    let (settings, sources) = settings::from_env()?;"
probe_refuses "settings::from_env_with" src/lib.rs "    let (settings, _) = omnigraph_compiler::settings::from_env_with(lookup)?;"
probe_refuses "from_env import" src/lib.rs "use omnigraph_compiler::settings::from_env;"
probe_refuses "DEFINITIONS index" src/lib.rs "    let variable = DEFINITIONS[0].env;"
probe_refuses "SettingSpec env read" src/lib.rs "    let variable = SettingId::RrfPlan.spec().env;"
probe_refuses "override static" src/instrumentation.rs "static RRF_PLAN_OVERRIDE: AtomicUsize = AtomicUsize::new(0);"

reset_fixture
printf '%s\n' "    let settings = self.session_settings();" >> "$fixture_root/probe/src/lib.rs"
printf '%s\n' "    let doc = DEFINITIONS[id as usize].doc;" >> "$fixture_root/probe/src/lib.rs"
scan_engine_settings
if [[ "$engine_fail" -ne 0 ]]; then
  echo "error: self-test: the guard refused a clean fixture tree" >&2
  self_test_fail=1
fi

if [[ "$self_test_fail" -ne 0 ]]; then
  echo >&2
  echo "The engine setting guard is blind to a spelling it must refuse. Fix the patterns in $(basename "$0")." >&2
  exit 1
fi

echo "self-test OK ($probes_run forbidden spellings refused over ${#setting_variables[@]} variables, a clean tree accepted)."
