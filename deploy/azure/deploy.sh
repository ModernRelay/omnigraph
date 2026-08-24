#!/usr/bin/env bash
set -euo pipefail

here=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

usage() {
  cat <<'EOF'
Usage:
  deploy/azure/deploy.sh validate --bundle DIR --server-image IMAGE@sha256:... [options]
  deploy/azure/deploy.sh deploy   --bundle DIR --server-image IMAGE@sha256:... [options]

Required for deploy:
  --resource-group NAME  --location REGION
  OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN must be set in the environment.

Options:
  --name PREFIX             Resource-name prefix (default: omnigraph)
  --container NAME          Private Blob container (default: omnigraph)
  --cluster-prefix PREFIX   Dedicated cluster prefix (default: clusters/company-brain)
  --actor ACTOR             Bootstrap/apply actor (default: azure-bootstrap)
  --evidence-out FILE       Write non-secret image/deployment evidence as JSON
EOF
}

mode=${1:-}
case "$mode" in
  validate|deploy) shift ;;
  -h|--help|'') usage; exit 0 ;;
  *) usage >&2; exit 64 ;;
esac

bundle=
server_image=
resource_group=
location=
name_prefix=omnigraph
container_name=omnigraph
cluster_prefix=clusters/company-brain
actor=azure-bootstrap
evidence_out=
# Keep the platform's outer kill budget strictly larger than the wrapper's
# child-drain budget so a successful drain still has time to release its lease.
admission_drain_seconds=90

while (($#)); do
  case "$1" in
    --bundle) bundle=${2:?}; shift 2 ;;
    --server-image) server_image=${2:?}; shift 2 ;;
    --resource-group) resource_group=${2:?}; shift 2 ;;
    --location) location=${2:?}; shift 2 ;;
    --name) name_prefix=${2:?}; shift 2 ;;
    --container) container_name=${2:?}; shift 2 ;;
    --cluster-prefix) cluster_prefix=${2:?}; shift 2 ;;
    --actor) actor=${2:?}; shift 2 ;;
    --evidence-out) evidence_out=${2:?}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 64 ;;
  esac
done

# Capture the deployment secret into a non-exported shell variable, then
# remove the inherited name before any child process starts. Only the one
# parameter-file writer receives it explicitly below.
token=${OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN:-}
unset OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN

die() { echo "azure deploy: $*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || die "required command not found: $1"; }

require az
require omnigraph
require python3
[[ -d "$bundle" && -f "$bundle/cluster.yaml" ]] || die "--bundle must contain cluster.yaml"
[[ "$server_image" =~ @sha256:[0-9a-f]{64}$ ]] \
  || die "--server-image must be pinned by sha256 manifest digest"
(( ${#name_prefix} >= 3 && ${#name_prefix} <= 20 )) \
  || die "--name must be 3-20 characters"
[[ "$name_prefix" =~ ^[a-z][a-z0-9-]*[a-z0-9]$ ]] \
  || die "--name must start with a letter, end alphanumeric, and use lowercase letters, digits, or hyphens"
[[ "$name_prefix" != *--* ]] || die "--name may not contain consecutive hyphens"
(( ${#container_name} >= 3 && ${#container_name} <= 63 )) \
  || die "--container must be 3-63 characters"
[[ "$container_name" =~ ^[a-z0-9][a-z0-9-]*[a-z0-9]$ ]] \
  || die "--container is not a canonical Azure Blob container name"
[[ "$container_name" != *--* ]] || die "--container may not contain consecutive hyphens"
[[ "$cluster_prefix" =~ ^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ ]] \
  || die "--cluster-prefix must contain canonical, non-empty path segments"
[[ "$cluster_prefix" != __omnigraph_azure_admission* ]] \
  || die "--cluster-prefix uses the reserved admission namespace"
IFS=/ read -r -a prefix_segments <<<"$cluster_prefix"
for segment in "${prefix_segments[@]}"; do
  [[ "$segment" != . && "$segment" != .. ]] \
    || die "--cluster-prefix may not contain dot segments"
done
if find "$bundle" ! -type f ! -type d -print -quit | grep -q .; then
  die "bundle may contain only regular files and directories"
fi

# Freeze the operator input exactly once. Validation, the evidence digest, and
# the bootstrap image all consume this private snapshot, so a concurrent edit
# cannot make the deployed bundle differ from the one that was validated.
umask 077
tmp=$(mktemp -d)
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
chmod 0700 "$tmp"
bundle_snapshot="$tmp/bundle"
mkdir "$bundle_snapshot"
cp -R "$bundle"/. "$bundle_snapshot"/
if find "$bundle_snapshot" ! -type f ! -type d -print -quit | grep -q .; then
  die "bundle snapshot may contain only regular files and directories"
fi

cluster_root="az://${container_name}/${cluster_prefix}"
CLUSTER_CONFIG="$bundle_snapshot/cluster.yaml" CLUSTER_ROOT="$cluster_root" python3 - <<'PY' \
  || die "cluster.yaml storage must be exactly ${cluster_root}"
import os
import re
from pathlib import Path

matches = []
for line in Path(os.environ['CLUSTER_CONFIG']).read_text().splitlines():
    match = re.match(r'^\s*storage:\s*(.*?)\s*(?:#.*)?$', line)
    if match:
        matches.append(match.group(1).strip().strip('"\''))
if matches != [os.environ['CLUSTER_ROOT']]:
    raise SystemExit(1)
PY

az bicep build --file "$here/foundation.bicep" --stdout >/dev/null
az bicep build --file "$here/runtime.bicep" --stdout >/dev/null

omnigraph cluster validate --config "$bundle_snapshot"

if [[ "$mode" == validate ]]; then
  echo "Azure reference deployment validation passed (no resources changed)."
  exit 0
fi

[[ -n "$resource_group" && -n "$location" ]] \
  || die "deploy requires --resource-group and --location"
[[ -n "$token" ]] || die "OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN is required"

az group create --name "$resource_group" --location "$location" --output none
foundation_outputs=$(az deployment group create \
  --resource-group "$resource_group" \
  --name "${name_prefix}-foundation" \
  --template-file "$here/foundation.bicep" \
  --parameters namePrefix="$name_prefix" location="$location" containerName="$container_name" \
  --query properties.outputs --output json)

read_output() {
  python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]]["value"])' "$1" \
    <<<"$foundation_outputs"
}

storage_account=$(read_output storageAccountName)
registry_name=$(read_output registryName)
registry_login=$(read_output registryLoginServer)
identity_name=$(read_output identityName)
environment_name=$(read_output environmentName)

source_digest=${server_image##*@sha256:}
source_tag=${source_digest:0:20}
az acr import \
  --name "$registry_name" \
  --source "$server_image" \
  --image "omnigraph-server:${source_tag}" \
  --force --output none
server_digest=$(az acr repository show \
  --name "$registry_name" \
  --image "omnigraph-server:${source_tag}" \
  --query digest --output tsv)
server_ref="${registry_login}/omnigraph-server@${server_digest}"

bundle_digest=$(python3 - "$bundle_snapshot" <<'PY'
import hashlib
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
print(digest.hexdigest())
PY
)

cp "$here/bootstrap.Dockerfile" "$tmp/bootstrap.Dockerfile"
cp "$here/bootstrap-entrypoint.sh" "$tmp/bootstrap-entrypoint.sh"
az acr build \
  --registry "$registry_name" \
  --image "omnigraph-bootstrap:${bundle_digest}" \
  --build-arg "BASE_IMAGE=${server_ref}" \
  --file "$tmp/bootstrap.Dockerfile" \
  "$tmp" --output none
bootstrap_digest=$(az acr repository show \
  --name "$registry_name" \
  --image "omnigraph-bootstrap:${bundle_digest}" \
  --query digest --output tsv)
bootstrap_ref="${registry_login}/omnigraph-bootstrap@${bootstrap_digest}"

app_name="${name_prefix}-server"
if az containerapp show --resource-group "$resource_group" --name "$app_name" >/dev/null 2>&1; then
  az containerapp ingress disable --resource-group "$resource_group" --name "$app_name" --output none || true
  while IFS= read -r revision; do
    [[ -n "$revision" ]] || continue
    az containerapp revision deactivate \
      --resource-group "$resource_group" --name "$app_name" \
      --revision "$revision" --output none
  done < <(az containerapp revision list \
    --resource-group "$resource_group" --name "$app_name" \
    --query '[?properties.active].name' --output tsv)
fi

parameters_file="$tmp/runtime.parameters.json"
: >"$parameters_file"
chmod 0600 "$parameters_file"
RUNTIME_PARAMETERS_FILE="$parameters_file" \
SERVER_BEARER_TOKEN="$token" \
python3 - <<'PY'
import json
import os
from pathlib import Path

Path(os.environ['RUNTIME_PARAMETERS_FILE']).write_text(json.dumps({
    '$schema': 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#',
    'contentVersion': '1.0.0.0',
    'parameters': {
        'serverBearerToken': {'value': os.environ['SERVER_BEARER_TOKEN']},
    },
}))
PY
unset token

deploy_runtime() {
  local activate=$1
  local bootstrap_mode=$2
  local bootstrap_timeout=$3
  az deployment group create \
    --resource-group "$resource_group" \
    --name "${name_prefix}-runtime" \
    --template-file "$here/runtime.bicep" \
    --parameters @"$parameters_file" \
      namePrefix="$name_prefix" \
      location="$location" \
      storageAccountName="$storage_account" \
      containerName="$container_name" \
      clusterPrefix="$cluster_prefix" \
      registryName="$registry_name" \
      identityName="$identity_name" \
      environmentName="$environment_name" \
      serverImage="$server_ref" \
      bootstrapImage="$bootstrap_ref" \
      bootstrapActor="$actor" \
      bootstrapMode="$bootstrap_mode" \
      bootstrapReplicaTimeoutSeconds="$bootstrap_timeout" \
      admissionDrainSeconds="$admission_drain_seconds" \
      activateServer="$activate" \
    --query properties.outputs --output json
}

start_job() {
  az containerapp job start \
    --resource-group "$resource_group" --name "$1" \
    --query name --output tsv
}

wait_for_job() {
  local job=$1
  local execution=$2
  local max_wait=$3
  local deadline=$((SECONDS + max_wait))
  local status=
  while ((SECONDS < deadline)); do
    status=$(az containerapp job execution show \
      --resource-group "$resource_group" \
      --name "$job" \
      --job-execution-name "$execution" \
      --query properties.status --output tsv 2>/dev/null || true)
    case "$status" in
      Succeeded) return 0 ;;
      Failed) return 1 ;;
    esac
    sleep 10
  done
  return 2
}

# First prove that the UAMI can pull the immutable image and read the private
# Blob container. This mode never acquires a lease or starts OmniGraph, so it
# is the only phase the deployer may retry while new RBAC grants propagate.
runtime_outputs=$(deploy_runtime false readiness 600)
job_name=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["bootstrapJobName"]["value"])' \
  <<<"$runtime_outputs")
readiness_ok=false
for readiness_attempt in 1 2 3; do
  execution_name=$(start_job "$job_name")
  if wait_for_job "$job_name" "$execution_name" 720; then
    readiness_ok=true
    break
  fi
  echo "azure deploy: identity readiness attempt ${readiness_attempt}/3 did not converge" >&2
  if [[ "$readiness_attempt" -lt 3 ]]; then
    sleep 20
  fi
done
[[ "$readiness_ok" == true ]] \
  || die "managed-identity image-pull/Blob readiness failed before lease acquisition"

# Switch the same Job resource to its lease-capable bootstrap. This phase runs
# exactly once: any non-success may have acquired the infinite lease and must
# go through inspection/recovery, never an automatic deploy retry.
runtime_outputs=$(deploy_runtime false apply 1800)
job_name=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["bootstrapJobName"]["value"])' \
  <<<"$runtime_outputs")
execution_name=$(start_job "$job_name")
if ! wait_for_job "$job_name" "$execution_name" 2100; then
  die "bootstrap execution ${execution_name} failed or timed out; do not retry automatically; inspect the admission lease and follow the recovery runbook"
fi

runtime_outputs=$(deploy_runtime true apply 1800)
server_fqdn=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["serverFqdn"]["value"])' \
  <<<"$runtime_outputs")
[[ -n "$server_fqdn" ]] || die "runtime deployment returned no server FQDN"

for _ in $(seq 1 60); do
  if curl --fail --silent --show-error "https://${server_fqdn}/healthz" >/dev/null; then
    break
  fi
  sleep 5
done
curl --fail --silent --show-error "https://${server_fqdn}/healthz" >/dev/null \
  || die "server did not become healthy"

echo "cluster_root=${cluster_root}"
echo "server_url=https://${server_fqdn}"
echo "server_image=${server_ref}"
echo "bootstrap_image=${bootstrap_ref}"

if [[ -n "$evidence_out" ]]; then
  EVIDENCE_OUT="$evidence_out" \
  CLUSTER_ROOT="$cluster_root" \
  SERVER_URL="https://${server_fqdn}" \
  SERVER_IMAGE="$server_ref" \
  BOOTSTRAP_IMAGE="$bootstrap_ref" \
  RESOURCE_GROUP="$resource_group" \
  LOCATION="$location" \
  NAME_PREFIX="$name_prefix" \
  CONTAINER_NAME="$container_name" \
  CLUSTER_PREFIX="$cluster_prefix" \
  BUNDLE_SHA256="$bundle_digest" \
  BOOTSTRAP_EXECUTION="$execution_name" \
  READINESS_ATTEMPTS="$readiness_attempt" \
  python3 - <<'PY'
import datetime
import json
import os
from pathlib import Path

result = {
    'utc_time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'result': 'server_healthy_after_bootstrap_release',
    'resource_group': os.environ['RESOURCE_GROUP'],
    'location': os.environ['LOCATION'],
    'name_prefix': os.environ['NAME_PREFIX'],
    'container': os.environ['CONTAINER_NAME'],
    'cluster_prefix': os.environ['CLUSTER_PREFIX'],
    'cluster_root': os.environ['CLUSTER_ROOT'],
    'server_url': os.environ['SERVER_URL'],
    'server_image': os.environ['SERVER_IMAGE'],
    'bootstrap_image': os.environ['BOOTSTRAP_IMAGE'],
    'bundle_sha256': os.environ['BUNDLE_SHA256'],
    'bootstrap_execution': os.environ['BOOTSTRAP_EXECUTION'],
    'readiness_attempts': int(os.environ['READINESS_ATTEMPTS']),
}
Path(os.environ['EVIDENCE_OUT']).write_text(json.dumps(result, indent=2) + '\n')
PY
fi
