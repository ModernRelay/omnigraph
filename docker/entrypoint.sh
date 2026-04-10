#!/bin/sh
set -eu

SERVER_BIN="/usr/local/bin/omnigraph-server"

if [ "$#" -gt 0 ]; then
  exec "$SERVER_BIN" "$@"
fi

bind="${OMNIGRAPH_BIND:-0.0.0.0:8080}"

if [ -n "${OMNIGRAPH_TARGET_URI:-}" ]; then
  exec "$SERVER_BIN" "${OMNIGRAPH_TARGET_URI}" --bind "${bind}"
fi

if [ -n "${OMNIGRAPH_CONFIG:-}" ]; then
  if [ -n "${OMNIGRAPH_TARGET:-}" ]; then
    exec "$SERVER_BIN" --config "${OMNIGRAPH_CONFIG}" --target "${OMNIGRAPH_TARGET}" --bind "${bind}"
  fi
  exec "$SERVER_BIN" --config "${OMNIGRAPH_CONFIG}" --bind "${bind}"
fi

cat >&2 <<'EOF'
omnigraph-server container startup requires one of:
  - OMNIGRAPH_TARGET_URI
  - OMNIGRAPH_CONFIG

Optional:
  - OMNIGRAPH_BIND (default: 0.0.0.0:8080)
  - OMNIGRAPH_TARGET (used with OMNIGRAPH_CONFIG)
EOF
exit 64
