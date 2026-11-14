#!/usr/bin/env bash
# Start RustFS (the CI-pinned 1.0.0-beta.12 binary) and a toxiproxy in front
# of it that adds LATENCY_MS each way (default 15 ms, so about 30 ms per round
# trip, the setup PR #783/#785/#784 measured with). Idempotent.
#
#   RustFS        : 127.0.0.1:9000 (direct, no added latency)
#   toxiproxy API : 127.0.0.1:8474
#   proxied S3    : 127.0.0.1:19000 (+LATENCY_MS each way)
#
# Binaries are downloaded from the GitHub releases into /opt/s3dl (see
# research/write-throughput/LOG.md for the checksums verified).
set -euo pipefail
LATENCY_MS="${LATENCY_MS:-15}"
RUSTFS=/opt/s3dl/rustfs/bin/rustfs
TOXI=/opt/s3dl/toxiproxy/toxiproxy-server-linux-amd64
DATA=/srv/rustfs-data
LOGS=/srv/s3logs
mkdir -p "$DATA" "$LOGS"

export RUSTFS_ACCESS_KEY=omnigraphci RUSTFS_SECRET_KEY=omnigraphci-secret
if ! curl -s -o /dev/null http://127.0.0.1:9000/ 2>/dev/null; then
  nohup "$RUSTFS" server --address 127.0.0.1:9000 "$DATA" >"$LOGS/rustfs.log" 2>&1 &
  for _ in $(seq 1 60); do
    curl -s -o /dev/null http://127.0.0.1:9000/ && break
    sleep 0.5
  done
fi

if ! curl -s http://127.0.0.1:8474/version >/dev/null 2>&1; then
  nohup "$TOXI" -host 127.0.0.1 -port 8474 >"$LOGS/toxiproxy.log" 2>&1 &
  for _ in $(seq 1 40); do
    curl -s http://127.0.0.1:8474/version >/dev/null 2>&1 && break
    sleep 0.25
  done
fi

# (Re)create the proxy and its two latency toxics.
curl -s -X DELETE http://127.0.0.1:8474/proxies/rustfs >/dev/null 2>&1 || true
curl -s -X POST http://127.0.0.1:8474/proxies \
  -d '{"name":"rustfs","listen":"127.0.0.1:19000","upstream":"127.0.0.1:9000"}' >/dev/null
for s in upstream downstream; do
  curl -s -X POST http://127.0.0.1:8474/proxies/rustfs/toxics \
    -d "{\"name\":\"lat_$s\",\"type\":\"latency\",\"stream\":\"$s\",\"attributes\":{\"latency\":$LATENCY_MS,\"jitter\":0}}" >/dev/null
done
curl -s http://127.0.0.1:8474/proxies/rustfs | head -c 600; echo
