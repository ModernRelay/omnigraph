#!/bin/sh
set -eu

root=${OMNIGRAPH_CLUSTER:?OMNIGRAPH_CLUSTER is required}
actor=${OMNIGRAPH_BOOTSTRAP_ACTOR:?OMNIGRAPH_BOOTSTRAP_ACTOR is required}
mode=${OMNIGRAPH_BOOTSTRAP_MODE:-apply}
grace=${OMNIGRAPH_AZURE_ADMISSION_GRACE_SECONDS:-90}
bundle=/opt/omnigraph/bundle

case "$mode" in
  readiness)
    # This phase is intentionally read-only and runs before any lease acquire,
    # so a first deployment may safely retry while UAMI Blob/AcrPull grants
    # propagate. Once the apply phase starts, there is no automatic retry.
    attempt=1
    while [ "$attempt" -le 60 ]; do
      if /usr/local/bin/omnigraph-azure-admission inspect --root "$root"; then
        exit 0
      fi
      if [ "$attempt" -eq 60 ]; then
        echo "managed-identity Blob readiness did not converge within 10 minutes" >&2
        exit 1
      fi
      echo "waiting for managed-identity Blob readiness ($attempt/60)" >&2
      attempt=$((attempt + 1))
      sleep 10
    done
    ;;
  apply) ;;
  *) echo "unsupported OMNIGRAPH_BOOTSTRAP_MODE: $mode" >&2; exit 64 ;;
esac

# The positional parameters in the single-quoted body belong to the supervised
# inner shell, not this entrypoint.
wrapper_pid=
forward_termination() {
  if [ -n "$wrapper_pid" ]; then
    kill -TERM "$wrapper_pid" 2>/dev/null || true
  fi
}
trap forward_termination TERM INT

set +e
# shellcheck disable=SC2016
/usr/local/bin/omnigraph-azure-admission run \
  --mode job \
  --root "$root" \
  --grace-seconds "$grace" \
  -- \
  /bin/sh -eu -c '
    omnigraph cluster validate --config "$1"
    omnigraph cluster import --config "$1"
    omnigraph cluster apply --config "$1" --as "$2"
  ' omnigraph-bootstrap "$bundle" "$actor" &
wrapper_pid=$!
while :; do
  wait "$wrapper_pid"
  wrapper_status=$?
  if ! kill -0 "$wrapper_pid" 2>/dev/null; then
    break
  fi
done
set -e
trap - TERM INT
wrapper_pid=
[ "$wrapper_status" -eq 0 ] || exit "$wrapper_status"

# A successful Job is not enough: confirm that its exact-ID release is visible
# before the deployment script is allowed to activate the serving revision.
inspection=$(/usr/local/bin/omnigraph-azure-admission inspect --root "$root")
printf '%s\n' "$inspection"
printf '%s\n' "$inspection" | grep -Eq '^lease_status=unlocked$'
