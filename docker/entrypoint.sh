#!/bin/sh
set -eu

SERVER_BIN="/usr/local/bin/omnigraph-server"

if [ "$#" -gt 0 ]; then
  exec "$SERVER_BIN" "$@"
fi

bind="${OMNIGRAPH_BIND:-0.0.0.0:8080}"

# Cluster mode first, and exclusive (the server's mode-inference rule 0):
# a deployment serves from cluster state XOR omnigraph.yaml, never a merge.
# Fail fast here with the same contract the server enforces.
if [ -n "${OMNIGRAPH_CLUSTER:-}" ]; then
  if [ -n "${OMNIGRAPH_TARGET_URI:-}" ] || [ -n "${OMNIGRAPH_CONFIG:-}" ] || [ -n "${OMNIGRAPH_TARGET:-}" ]; then
    echo "OMNIGRAPH_CLUSTER is an exclusive boot source; unset OMNIGRAPH_TARGET_URI/OMNIGRAPH_CONFIG/OMNIGRAPH_TARGET" >&2
    exit 64
  fi
  exec "$SERVER_BIN" --cluster "${OMNIGRAPH_CLUSTER}" --bind "${bind}"
fi

# URI comes from the env var (the positional arg wins over any config
# `graphs` block in resolve_target_uri). OMNIGRAPH_CONFIG, when also set,
# is forwarded as --config purely to supply a policy file — the two
# compose. Without OMNIGRAPH_CONFIG the behavior is unchanged.
if [ -n "${OMNIGRAPH_TARGET_URI:-}" ]; then
  exec "$SERVER_BIN" "${OMNIGRAPH_TARGET_URI}" \
    ${OMNIGRAPH_CONFIG:+--config "$OMNIGRAPH_CONFIG"} \
    --bind "${bind}"
fi

if [ -n "${OMNIGRAPH_CONFIG:-}" ]; then
  if [ -n "${OMNIGRAPH_TARGET:-}" ]; then
    exec "$SERVER_BIN" --config "${OMNIGRAPH_CONFIG}" --target "${OMNIGRAPH_TARGET}" --bind "${bind}"
  fi
  exec "$SERVER_BIN" --config "${OMNIGRAPH_CONFIG}" --bind "${bind}"
fi

cat >&2 <<'EOF'
omnigraph-server container startup requires one of:
  - OMNIGRAPH_CLUSTER     (serve a cluster directory's applied revision;
                           exclusive — cannot combine with the others)
  - OMNIGRAPH_TARGET_URI
  - OMNIGRAPH_CONFIG

Optional:
  - OMNIGRAPH_BIND (default: 0.0.0.0:8080)
  - OMNIGRAPH_TARGET (used with OMNIGRAPH_CONFIG)
  - OMNIGRAPH_CONFIG (may also accompany OMNIGRAPH_TARGET_URI to add a
    policy file; the URI still comes from OMNIGRAPH_TARGET_URI)
EOF
exit 64
