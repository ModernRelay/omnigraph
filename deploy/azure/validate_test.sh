#!/bin/sh
set -eu

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/bin" "$work/bundle"

cat >"$work/bin/az" <<'EOF'
#!/bin/sh
set -eu
if [ "${OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN+x}" = x ]; then
  echo 'server bearer token leaked to az' >&2
  exit 96
fi
printf '%s\n' "$*" >>"$AZURE_VALIDATE_TEST_AZ_CALLS"
if [ "$#" -ge 2 ] && [ "$1" = bicep ] && [ "$2" = build ]; then
  if [ ! -e "$AZURE_VALIDATE_TEST_MUTATED" ]; then
    printf '%s\n' 'mutated after snapshot' >"$AZURE_VALIDATE_TEST_SOURCE_BUNDLE/cluster.yaml"
    : >"$AZURE_VALIDATE_TEST_MUTATED"
  fi
  exit 0
fi
echo "validation attempted a mutating Azure command: $*" >&2
exit 97
EOF

cat >"$work/bin/omnigraph" <<'EOF'
#!/bin/sh
set -eu
if [ "${OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN+x}" = x ]; then
  echo 'server bearer token leaked to omnigraph' >&2
  exit 96
fi
printf '%s\n' "$*" >>"$AZURE_VALIDATE_TEST_OMNIGRAPH_CALLS"
test "$1" = cluster
test "$2" = validate
test "$3" = --config
cp "$4/cluster.yaml" "$AZURE_VALIDATE_TEST_VALIDATED_CONFIG"
EOF

chmod 0755 "$work/bin/az" "$work/bin/omnigraph"
cat >"$work/bundle/cluster.yaml" <<'EOF'
version: 1
storage: az://omnigraph/clusters/company-brain
graphs: {}
EOF

az_calls="$work/az-calls"
omnigraph_calls="$work/omnigraph-calls"
validated_config="$work/validated-cluster.yaml"
mutated="$work/source-mutated"
: >"$az_calls"
: >"$omnigraph_calls"

PATH="$work/bin:$PATH" \
AZURE_VALIDATE_TEST_AZ_CALLS="$az_calls" \
AZURE_VALIDATE_TEST_OMNIGRAPH_CALLS="$omnigraph_calls" \
AZURE_VALIDATE_TEST_SOURCE_BUNDLE="$work/bundle" \
AZURE_VALIDATE_TEST_VALIDATED_CONFIG="$validated_config" \
AZURE_VALIDATE_TEST_MUTATED="$mutated" \
OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN='must-not-reach-a-child' \
  "$here/deploy.sh" validate \
    --bundle "$work/bundle" \
    --server-image example.invalid/omnigraph-server@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    >/dev/null

test "$(wc -l <"$az_calls" | tr -d ' ')" = 2
grep -Eq '^bicep build --file .*/foundation\.bicep --stdout$' "$az_calls"
grep -Eq '^bicep build --file .*/runtime\.bicep --stdout$' "$az_calls"
validation_call=$(cat "$omnigraph_calls")
case "$validation_call" in
  "cluster validate --config $work/bundle"*)
    echo 'semantic validation consumed the mutable source bundle' >&2
    exit 1
    ;;
  "cluster validate --config "*/bundle) ;;
  *)
    echo "unexpected semantic-validation call: $validation_call" >&2
    exit 1
    ;;
esac
grep -Fq 'storage: az://omnigraph/clusters/company-brain' "$validated_config"
test "$(cat "$work/bundle/cluster.yaml")" = 'mutated after snapshot'
grep -Fq 'require omnigraph' "$here/deploy.sh"
if grep -Fq 'omnigraph is not on PATH' "$here/deploy.sh"; then
  echo 'deploy validation must not skip semantic bundle validation' >&2
  exit 1
fi

# Deployment mode must carry the same immutable snapshot through semantic
# validation, evidence hashing, and the ACR build context even when the source
# bundle changes immediately after the copy. Every fake child also proves the
# exported bearer-token name was removed before process launch.
mkdir -p "$work/deploy-bundle" "$work/deploy-original"
cat >"$work/deploy-bundle/cluster.yaml" <<'EOF'
version: 1
storage: az://omnigraph/clusters/company-brain
graphs: {}
EOF
cp "$work/deploy-bundle/cluster.yaml" "$work/deploy-original/cluster.yaml"

cat >"$work/bin/az" <<'EOF'
#!/bin/sh
set -eu
if [ "${OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN+x}" = x ]; then
  echo 'server bearer token leaked to az' >&2
  exit 96
fi
printf '%s\n' "$*" >>"$AZURE_DEPLOY_TEST_AZ_CALLS"

if [ "$#" -ge 2 ] && [ "$1" = bicep ] && [ "$2" = build ]; then
  if [ ! -e "$AZURE_DEPLOY_TEST_MUTATED" ]; then
    printf '%s\n' 'mutated after snapshot' >"$AZURE_DEPLOY_TEST_SOURCE_BUNDLE/cluster.yaml"
    : >"$AZURE_DEPLOY_TEST_MUTATED"
  fi
  exit 0
fi

case "$1 $2 ${3:-} ${4:-}" in
  'group create --name '* ) exit 0 ;;
  'deployment group create '* )
    case "$*" in
      *'-foundation'*)
        cat <<'JSON'
{"storageAccountName":{"value":"storage"},"registryName":{"value":"registry"},"registryLoginServer":{"value":"registry.invalid"},"identityName":{"value":"identity"},"environmentName":{"value":"environment"}}
JSON
        ;;
      *'activateServer=true'*)
        printf '%s\n' '{"serverFqdn":{"value":"server.invalid"}}'
        ;;
      *)
        printf '%s\n' '{"bootstrapJobName":{"value":"bootstrap"}}'
        ;;
    esac
    exit 0
    ;;
  'acr import --name '* ) exit 0 ;;
  'acr repository show '* )
    case "$*" in
      *'omnigraph-server:'*)
        printf 'sha256:%064d\n' 0 | tr '0' b
        ;;
      *)
        printf 'sha256:%064d\n' 0 | tr '0' c
        ;;
    esac
    exit 0
    ;;
  'acr build --registry '* )
    context=
    previous=
    for argument do
      if [ "$argument" = --output ]; then
        context=$previous
        break
      fi
      previous=$argument
    done
    test -n "$context"
    printf '%s\n' "$context" >"$AZURE_DEPLOY_TEST_BUILD_CONTEXT"
    cp "$context/bundle/cluster.yaml" "$AZURE_DEPLOY_TEST_BUILT_CONFIG"
    exit 0
    ;;
  'containerapp show --resource-group '* ) exit 1 ;;
  'containerapp job start '* )
    printf '%s\n' execution-1
    exit 0
    ;;
  'containerapp job execution show' )
    printf '%s\n' Succeeded
    exit 0
    ;;
esac

echo "unexpected deploy-mode Azure command: $*" >&2
exit 97
EOF

cat >"$work/bin/omnigraph" <<'EOF'
#!/bin/sh
set -eu
if [ "${OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN+x}" = x ]; then
  echo 'server bearer token leaked to omnigraph' >&2
  exit 96
fi
test "$1" = cluster
test "$2" = validate
test "$3" = --config
cp "$4/cluster.yaml" "$AZURE_DEPLOY_TEST_VALIDATED_CONFIG"
EOF

cat >"$work/bin/curl" <<'EOF'
#!/bin/sh
set -eu
if [ "${OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN+x}" = x ]; then
  echo 'server bearer token leaked to curl' >&2
  exit 96
fi
exit 0
EOF
chmod 0755 "$work/bin/az" "$work/bin/omnigraph" "$work/bin/curl"

deploy_az_calls="$work/deploy-az-calls"
deploy_validated="$work/deploy-validated-cluster.yaml"
deploy_built="$work/deploy-built-cluster.yaml"
deploy_context="$work/deploy-build-context"
deploy_mutated="$work/deploy-source-mutated"
deploy_evidence="$work/deploy-evidence.json"
: >"$deploy_az_calls"

PATH="$work/bin:$PATH" \
AZURE_DEPLOY_TEST_AZ_CALLS="$deploy_az_calls" \
AZURE_DEPLOY_TEST_SOURCE_BUNDLE="$work/deploy-bundle" \
AZURE_DEPLOY_TEST_VALIDATED_CONFIG="$deploy_validated" \
AZURE_DEPLOY_TEST_BUILT_CONFIG="$deploy_built" \
AZURE_DEPLOY_TEST_BUILD_CONTEXT="$deploy_context" \
AZURE_DEPLOY_TEST_MUTATED="$deploy_mutated" \
OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN='must-not-reach-a-child' \
  "$here/deploy.sh" deploy \
    --bundle "$work/deploy-bundle" \
    --server-image example.invalid/omnigraph-server@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    --resource-group test-rg \
    --location test-region \
    --evidence-out "$deploy_evidence" \
    >/dev/null

cmp "$work/deploy-original/cluster.yaml" "$deploy_validated"
cmp "$work/deploy-original/cluster.yaml" "$deploy_built"
test "$(cat "$work/deploy-bundle/cluster.yaml")" = 'mutated after snapshot'
case "$(cat "$deploy_context")" in
  "$work/deploy-bundle"*)
    echo 'ACR build consumed the mutable source bundle' >&2
    exit 1
    ;;
esac

python3 - "$work/deploy-original" "$deploy_evidence" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1]).resolve()
digest = hashlib.sha256()
for path in sorted(p for p in root.rglob('*') if p.is_file()):
    relative = path.relative_to(root).as_posix().encode()
    body = path.read_bytes()
    digest.update(len(relative).to_bytes(8, 'big'))
    digest.update(relative)
    digest.update(len(body).to_bytes(8, 'big'))
    digest.update(body)
evidence = json.loads(pathlib.Path(sys.argv[2]).read_text())
if evidence['bundle_sha256'] != digest.hexdigest():
    raise SystemExit('evidence digest did not bind the immutable bundle snapshot')
PY

# Storage account names always retain the complete stable suffix even when the
# human deployment prefix is long.
grep -Fq "var storageName = 'st\${take(compactPrefix, 10)}\${stableSuffix}'" \
  "$here/foundation.bicep"

# The platform deadline is derived, not independently configurable: direct
# Bicep callers cannot collapse the lease-release headroom accidentally.
grep -Fq 'var terminationGraceSeconds = admissionDrainSeconds + 60' \
  "$here/runtime.bicep"
grep -Fq 'terminationGracePeriodSeconds: terminationGraceSeconds' \
  "$here/runtime.bicep"

# The server owns the Blob lease before it binds /healthz. Pin a Startup probe
# whose last allowed failure comes after deploy.sh's own health deadline, so a
# slow-but-valid open cannot be killed by liveness while the deployer still
# considers it in progress.
python3 - "$here/runtime.bicep" "$here/deploy.sh" <<'PY'
import re
import sys
from pathlib import Path

runtime = Path(sys.argv[1]).read_text()
deployer = Path(sys.argv[2]).read_text()

def bicep_int(name):
    match = re.search(rf'^var {re.escape(name)} = (\d+)$', runtime, re.MULTILINE)
    if match is None:
        raise SystemExit(f'missing integer Bicep variable: {name}')
    return int(match.group(1))

if runtime.count("type: 'Startup'") != 1:
    raise SystemExit('runtime must define exactly one Startup probe')
for fragment in (
    'initialDelaySeconds: serverStartupInitialDelaySeconds',
    'periodSeconds: serverStartupPeriodSeconds',
    'failureThreshold: serverStartupFailureThreshold',
    "type: 'Liveness'",
):
    if fragment not in runtime:
        raise SystemExit(f'missing probe contract: {fragment}')

health_loop = re.search(
    r'for _ in \$\(seq 1 (\d+)\); do\n'
    r'.*?sleep (\d+)\n'
    r'done\n'
    r'curl .*?server did not become healthy',
    deployer,
    re.DOTALL,
)
if health_loop is None:
    raise SystemExit('could not derive deployer health deadline')
health_deadline = int(health_loop.group(1)) * int(health_loop.group(2))

initial_delay = bicep_int('serverStartupInitialDelaySeconds')
period = bicep_int('serverStartupPeriodSeconds')
threshold = bicep_int('serverStartupFailureThreshold')
last_failure = initial_delay + period * (threshold - 1)
if last_failure <= health_deadline:
    raise SystemExit(
        f'Startup probe may restart at {last_failure}s before the '
        f'{health_deadline}s deployer health deadline'
    )
PY

echo "azure validation-mode test: passed"
