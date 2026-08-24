#!/bin/sh
set -eu

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
work=$(mktemp -d)
entrypoint_pid=
cleanup() {
  if [ -n "$entrypoint_pid" ]; then
    kill -TERM "$entrypoint_pid" 2>/dev/null || true
    wait "$entrypoint_pid" 2>/dev/null || true
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT

cat >"$work/admission" <<'EOF'
#!/bin/sh
set -eu
if [ "${1:-}" = inspect ]; then
  if [ "${BOOTSTRAP_TEST_APPLY:-}" = 1 ]; then
    echo "lease_status=unlocked"
    exit 0
  fi
  attempts_file=${BOOTSTRAP_TEST_ATTEMPTS:?}
  attempts=$(cat "$attempts_file")
  attempts=$((attempts + 1))
  printf '%s\n' "$attempts" >"$attempts_file"
  if [ "$attempts" -lt 3 ]; then
    echo "not ready" >&2
    exit 1
  fi
  echo "lease_state=missing"
  exit 0
fi
if [ "${1:-}" = run ] && [ "${BOOTSTRAP_TEST_SIGNAL:-}" = 1 ]; then
  : >"${BOOTSTRAP_TEST_SIGNAL_READY:?}"
  trap 'printf forwarded >"$BOOTSTRAP_TEST_SIGNAL_FORWARDED"; exit 1' TERM
  while :; do sleep 1; done
fi
echo "ADMISSION: $*"
EOF
chmod 0755 "$work/admission"

# Replace only the binary path and wait duration; production behavior remains
# otherwise identical while the retry test completes immediately.
sed \
  -e "s#/usr/local/bin/omnigraph-azure-admission#$work/admission#g" \
  -e 's/sleep 10/sleep 0/' \
  "$here/bootstrap-entrypoint.sh" >"$work/entrypoint"
chmod 0755 "$work/entrypoint"

attempts_file="$work/attempts"
printf '0\n' >"$attempts_file"
readiness=$(BOOTSTRAP_TEST_ATTEMPTS="$attempts_file" \
  OMNIGRAPH_CLUSTER=az://omnigraph/clusters/company-brain \
  OMNIGRAPH_BOOTSTRAP_ACTOR=act-bootstrap \
  OMNIGRAPH_BOOTSTRAP_MODE=readiness \
  "$work/entrypoint" 2>/dev/null)
test "$readiness" = "lease_state=missing"
test "$(cat "$attempts_file")" = 3

apply=$(BOOTSTRAP_TEST_ATTEMPTS="$attempts_file" \
  BOOTSTRAP_TEST_APPLY=1 \
  OMNIGRAPH_CLUSTER=az://omnigraph/clusters/company-brain \
  OMNIGRAPH_BOOTSTRAP_ACTOR=act-bootstrap \
  OMNIGRAPH_BOOTSTRAP_MODE=apply \
  "$work/entrypoint")
case "$apply" in
  "ADMISSION: run --mode job --root az://omnigraph/clusters/company-brain --grace-seconds 90 -- /bin/sh -eu -c "*) ;;
  *) echo "unexpected apply admission command: $apply" >&2; exit 1 ;;
esac

signal_ready="$work/signal-ready"
signal_forwarded="$work/signal-forwarded"
BOOTSTRAP_TEST_ATTEMPTS="$attempts_file" \
BOOTSTRAP_TEST_SIGNAL=1 \
BOOTSTRAP_TEST_SIGNAL_READY="$signal_ready" \
BOOTSTRAP_TEST_SIGNAL_FORWARDED="$signal_forwarded" \
OMNIGRAPH_CLUSTER=az://omnigraph/clusters/company-brain \
OMNIGRAPH_BOOTSTRAP_ACTOR=act-bootstrap \
OMNIGRAPH_BOOTSTRAP_MODE=apply \
  "$work/entrypoint" >/dev/null 2>&1 &
entrypoint_pid=$!
for _ in 1 2 3 4 5; do
  [ -f "$signal_ready" ] && break
  sleep 1
done
test -f "$signal_ready"
kill -TERM "$entrypoint_pid"
if wait "$entrypoint_pid"; then
  entrypoint_pid=
  echo "signalled bootstrap entrypoint unexpectedly succeeded" >&2
  exit 1
fi
entrypoint_pid=
test "$(cat "$signal_forwarded")" = forwarded

if BOOTSTRAP_TEST_ATTEMPTS="$attempts_file" \
  OMNIGRAPH_CLUSTER=az://omnigraph/clusters/company-brain \
  OMNIGRAPH_BOOTSTRAP_ACTOR=act-bootstrap \
  OMNIGRAPH_BOOTSTRAP_MODE=invalid \
  "$work/entrypoint" >/dev/null 2>&1
then
  echo "invalid bootstrap mode unexpectedly succeeded" >&2
  exit 1
else
  test "$?" = 64
fi

echo "azure bootstrap-entrypoint test: passed"
