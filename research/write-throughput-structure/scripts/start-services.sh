#!/usr/bin/env bash
# Start RustFS (127.0.0.1:9000) and toxiproxy (127.0.0.1:19000 -> 9000, +LAT ms each way).
# Binaries: RustFS 1.0.0-beta.12 musl (the CI image's version) and toxiproxy 2.13.0, downloaded
# from their GitHub releases with checksums verified, installed under /opt/bench/bin.
set -euo pipefail
LAT=${LAT:-15}
mkdir -p /opt/bench/rustfs-data /opt/bench/logs
if ! curl -s -o /dev/null http://127.0.0.1:9000/; then
  RUSTFS_ACCESS_KEY=omnigraphci RUSTFS_SECRET_KEY=omnigraphci-secret \
    nohup /opt/bench/bin/rustfs server --address 127.0.0.1:9000 /opt/bench/rustfs-data \
    > /opt/bench/logs/rustfs.log 2>&1 &
  sleep 5
fi
curl -s -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" --user "omnigraphci:omnigraphci-secret" \
  http://127.0.0.1:9000/omnigraph-bench >/dev/null || true
if ! curl -s -o /dev/null http://127.0.0.1:8474/version; then
  nohup /opt/bench/bin/toxiproxy-server -host 127.0.0.1 -port 8474 > /opt/bench/logs/toxiproxy.log 2>&1 &
  sleep 2
fi
curl -s -X DELETE localhost:8474/proxies/rustfs >/dev/null || true
curl -s -X POST localhost:8474/proxies \
  -d '{"name":"rustfs","listen":"127.0.0.1:19000","upstream":"127.0.0.1:9000"}' >/dev/null
for s in upstream downstream; do
  curl -s -X POST localhost:8474/proxies/rustfs/toxics \
    -d "{\"name\":\"lat_$s\",\"type\":\"latency\",\"stream\":\"$s\",\"attributes\":{\"latency\":$LAT}}" >/dev/null
done
echo "rustfs on :9000, toxiproxy on :19000 (+${LAT} ms each way)"
