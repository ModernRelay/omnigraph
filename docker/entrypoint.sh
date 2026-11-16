#!/bin/sh
set -eu

SERVER_BIN="/usr/local/bin/omnigraph-server"
ADMISSION_BIN="/usr/local/bin/omnigraph-azure-admission"
TINI_BIN="/usr/bin/tini"
admission_grace="${OMNIGRAPH_AZURE_ADMISSION_GRACE_SECONDS:-90}"

exec_server() {
  cluster_root=$1
  shift
  case "$cluster_root" in
    az://*)
      exec "$TINI_BIN" -- "$ADMISSION_BIN" run \
        --mode server \
        --root "$cluster_root" \
        --grace-seconds "$admission_grace" \
        -- "$SERVER_BIN" "$@"
      ;;
    *) exec "$SERVER_BIN" "$@" ;;
  esac
}

if [ "$#" -gt 0 ]; then
  explicit_cluster=""
  want_cluster=0
  for arg in "$@"; do
    if [ "$want_cluster" -eq 1 ]; then
      explicit_cluster=$arg
      break
    fi
    if [ "$arg" = "--cluster" ]; then
      want_cluster=1
      continue
    fi
    case "$arg" in
      --cluster=*)
        explicit_cluster=${arg#--cluster=}
        break
        ;;
    esac
  done
  exec_server "$explicit_cluster" "$@"
fi

bind="${OMNIGRAPH_BIND:-0.0.0.0:8080}"

# The server boots only from a cluster. The single-graph variables of earlier
# releases name arguments the server no longer accepts (a positional URI,
# `--config`, `--target`), so they are refused by name instead of failing
# later in argument parsing.
for removed in OMNIGRAPH_TARGET_URI OMNIGRAPH_CONFIG OMNIGRAPH_TARGET; do
  case "$removed" in
    OMNIGRAPH_TARGET_URI) value="${OMNIGRAPH_TARGET_URI:-}" ;;
    OMNIGRAPH_CONFIG) value="${OMNIGRAPH_CONFIG:-}" ;;
    OMNIGRAPH_TARGET) value="${OMNIGRAPH_TARGET:-}" ;;
  esac
  if [ -n "$value" ]; then
    echo "$removed is no longer supported: the server boots only from a cluster; unset it and set OMNIGRAPH_CLUSTER to the cluster directory or storage root" >&2
    exit 64
  fi
done

if [ -n "${OMNIGRAPH_CLUSTER:-}" ]; then
  set -- --cluster "${OMNIGRAPH_CLUSTER}" --bind "${bind}"
  case "${OMNIGRAPH_REQUIRE_ALL_GRAPHS:-}" in
    ""|0|false|FALSE) ;;
    *) set -- "$@" --require-all-graphs ;;
  esac
  exec_server "${OMNIGRAPH_CLUSTER}" "$@"
fi

cat >&2 <<'EOF'
omnigraph-server container startup requires:
  - OMNIGRAPH_CLUSTER     (a cluster directory or storage root, such as
                           s3://bucket/prefix or az://container/prefix)

Optional:
  - OMNIGRAPH_BIND (default: 0.0.0.0:8080)
  - OMNIGRAPH_REQUIRE_ALL_GRAPHS (fail startup unless every applied graph is
    healthy)
EOF
exit 64
