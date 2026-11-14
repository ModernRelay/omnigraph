# WAL research log: many concurrent writes, far more commits per second

Branch: `research/write-throughput-wal` (fork `ragnorc/omnigraph`).
Started 2026-11-14 on a Modal dev box (2 vCPU reported by nproc, 64 GiB RAM, gVisor kernel).

Angle: a write-ahead log in front of `__manifest` publication.

## Timeline

- 2026-11-14: branch created from origin/main 2c0941e0.
