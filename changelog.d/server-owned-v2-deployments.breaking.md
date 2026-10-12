- Cluster execution now uses ledger v2 only. Explicit `cluster upgrade-ledger --writers-stopped` preserves existing data; legacy `import`, `refresh`, `approve` and apply fallback are removed. Fresh apply creates graphs directly, and `cluster apply --server` changes schemas and stored queries or adds graphs without restarting the server. See [cluster operations][server-owned-v2-clusters].

[server-owned-v2-clusters]: ../docs/user/clusters/index.md
