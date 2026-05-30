#!/bin/sh
# Self-contained test for docker/entrypoint.sh argument composition.
# Runs the entrypoint against a stub server that echoes its args, and
# asserts the forwarded argv for each startup mode. No Docker required.
#
#   sh docker/entrypoint_test.sh
#
# Exits 0 on success, 1 on the first mismatch.
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
entrypoint="$here/entrypoint.sh"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin"
cat > "$work/bin/omnigraph-server" <<'EOF'
#!/bin/sh
echo "ARGS: $*"
EOF
chmod +x "$work/bin/omnigraph-server"

# Run the real entrypoint with SERVER_BIN pointed at the stub.
ep="$work/entrypoint.sh"
sed "s#SERVER_BIN=\"/usr/local/bin/omnigraph-server\"#SERVER_BIN=\"$work/bin/omnigraph-server\"#" \
  "$entrypoint" > "$ep"

fail=0
check() {
  desc=$1; want=$2; got=$3
  if [ "$got" != "$want" ]; then
    echo "FAIL: $desc"
    echo "  want: $want"
    echo "  got:  $got"
    fail=1
  else
    echo "ok: $desc"
  fi
}

got=$(OMNIGRAPH_TARGET_URI="s3://b/g" OMNIGRAPH_BIND="0.0.0.0:8080" sh "$ep")
check "TARGET_URI only (legacy)" \
  "ARGS: s3://b/g --bind 0.0.0.0:8080" "$got"

got=$(OMNIGRAPH_TARGET_URI="s3://b/g" OMNIGRAPH_CONFIG="/etc/omnigraph/omnigraph.yaml" OMNIGRAPH_BIND="0.0.0.0:8080" sh "$ep")
check "TARGET_URI + CONFIG composes (policy)" \
  "ARGS: s3://b/g --config /etc/omnigraph/omnigraph.yaml --bind 0.0.0.0:8080" "$got"

got=$(OMNIGRAPH_CONFIG="/etc/omnigraph/omnigraph.yaml" OMNIGRAPH_BIND="0.0.0.0:8080" sh "$ep")
check "CONFIG only" \
  "ARGS: --config /etc/omnigraph/omnigraph.yaml --bind 0.0.0.0:8080" "$got"

got=$(OMNIGRAPH_CONFIG="/etc/omnigraph/omnigraph.yaml" OMNIGRAPH_TARGET="active" OMNIGRAPH_BIND="0.0.0.0:8080" sh "$ep")
check "CONFIG + TARGET" \
  "ARGS: --config /etc/omnigraph/omnigraph.yaml --target active --bind 0.0.0.0:8080" "$got"

got=$(sh "$ep" some-uri --bind 1.2.3.4:9 --extra)
check "explicit args passthrough" \
  "ARGS: some-uri --bind 1.2.3.4:9 --extra" "$got"

if [ "$fail" -ne 0 ]; then
  echo "entrypoint_test: FAILED"
  exit 1
fi
echo "entrypoint_test: all cases passed"
