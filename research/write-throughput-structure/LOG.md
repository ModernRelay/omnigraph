# Research log: catalog structure and publication for concurrent writes

Branch `research/write-throughput-structure` (fork `ragnorc/omnigraph`). Base: `origin/main` at
`2c0941e0` (2026-11-14). Box: 2 vCPU (nproc), 64 GiB RAM, gVisor Linux.

Angle: data structures, protocols and Lance primitives (two other agents cover group commit /
broad, and a WAL).

## 2026-11-14 — prior work read

Prior measurements (not mine; from docs/PRs, older bases):

- Shared-schema-gate RFC decision log (2026-09-28): one writer, one branch holds the gates
  7.3 ms locally (revalidation 0.6, detached commit 1.1, publication 5.6) and 670 ms at
  +30 ms/RTT (298, 85, 284 ms). 8 writers/1 branch cannot beat 1 writer: 31.7 commits/s
  local, 0.87 at +30 ms. Stale-attempt early-fail prototype: 0.40 -> 0.53 at +30 ms.
  8 branches: 154 commits/s local, 2.13 at +30 ms.
- PR #785 group-commit spike (base 97f293e8, RustFS + toxiproxy 15 ms each way):
  1 writer 1.83/s; 8 writers 0.87 -> 9.23; 16 writers 0.50 -> 13.63. Local: 1 writer 31.8,
  8 writers 21.5 -> 65.0. Batch of one = today's cost (3.05 `__manifest` writes/commit).
- PR #781 COW catalog publication: 1w/1b 21.8 -> 60.9 commits/s local; 4w/4b 22.2 -> 113.4.
- Issue #837 trace (warm single-row insert, DST unit model, main 0a62bb7b): 14 requests,
  makespan 12; requests 7-14 gated: HEAD v / HEAD v+1 (revalidation), PUT txn + PUT detached
  table manifest, LIST `__manifest/_versions` (publisher freshness), PUT `__manifest` data,
  PUT txn, PUT `_versions/{v+1}.manifest` (the CAS). Ungated: HEAD v/v+1 capture, HEAD+GET
  table manifest, GET key check, PUT table data file.
- Writer epoch (#1350): a holder skips the capture/revalidation probes on its bound branch.
- `concurrent-writes` instrument is deferred (benchmarks/deferred, does not build) — #895.

Initial hypotheses (to test):

- H1 the per-branch ceiling is the critical-section length (~8 serial object-store requests),
  not the single CAS; the physical floor is 1 CAS round trip per publication (per batch).
- H2 every publication invalidates every other prepared write on the branch (revalidation
  fails on any head move); precise footprint conflict detection + re-anchoring/rebase would
  let commuting writes pass without re-preparing.
- H3 pipelining: the next publication's payload (catalog data file, txn) and table detached
  commits can be prepared while the previous CAS is in flight; only the CAS needs the gate.
