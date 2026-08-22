#!/bin/sh
set -eu

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
work=$(mktemp -d)
suffix=$$
base_image="omnigraph-bootstrap-test-base:$suffix"
derived_image="omnigraph-bootstrap-test-derived:$suffix"
cleanup() {
  docker image rm -f "$derived_image" "$base_image" >/dev/null 2>&1 || true
  rm -rf -- "$work"
}
trap cleanup EXIT

cat >"$work/base.Dockerfile" <<'EOF'
FROM public.ecr.aws/debian/debian:bookworm-slim@sha256:1f6767130e3479e42348856acee11bbe78d26cc558b4bf52ac5106f3fcf594ff
RUN groupadd --system omnigraph \
    && useradd --system --gid omnigraph --create-home --home-dir /var/lib/omnigraph omnigraph
USER omnigraph:omnigraph
EOF
docker build --quiet --file "$work/base.Dockerfile" --tag "$base_image" "$work" >/dev/null

cp "$here/bootstrap.Dockerfile" "$work/bootstrap.Dockerfile"
cp "$here/bootstrap-entrypoint.sh" "$work/bootstrap-entrypoint.sh"
mkdir "$work/bundle"
cat >"$work/bundle/cluster.yaml" <<'EOF'
version: 1
storage: az://omnigraph/clusters/bootstrap-image-test
graphs: {}
EOF

# This is a real derived build from a base whose final USER is non-root. It
# catches a root-owned COPY followed by an impossible unprivileged chmod/chown.
docker build --quiet \
  --build-arg "BASE_IMAGE=$base_image" \
  --file "$work/bootstrap.Dockerfile" \
  --tag "$derived_image" \
  "$work" >/dev/null

test "$(docker image inspect --format '{{.Config.User}}' "$derived_image")" = \
  "omnigraph:omnigraph"
docker run --rm --entrypoint /bin/sh "$derived_image" -eu -c '
  test -x /opt/omnigraph/bootstrap-entrypoint.sh
  test "$(stat -c %U:%G /opt/omnigraph/bootstrap-entrypoint.sh)" = omnigraph:omnigraph
  test "$(stat -c %U:%G /opt/omnigraph/bundle/cluster.yaml)" = omnigraph:omnigraph
  test -r /opt/omnigraph/bundle/cluster.yaml
'

echo "azure bootstrap-image test: passed"
