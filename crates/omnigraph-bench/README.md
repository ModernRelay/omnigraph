# OmniGraph benchmark harness

`omnigraph-bench` owns benchmark definitions, planning, and the first narrow
execution path. It parses strict, versioned YAML case and suite documents,
validates their semantics, computes stable experiment identities, emits plans,
and can execute supported local `branch-merge-v1` points. Run wall-clock
measurements from a release build only:

```bash
cargo run --release --locked -p omnigraph-bench -- \
  suite run benchmarks/suites/local-smoke.suite-v1.yaml
```

The checked-in catalog and command examples are in
[`../../benchmarks/README.md`](../../benchmarks/README.md). RFC 0039 owns the
measurement protocol and identity vocabulary.

## Boundaries

- A case describes exactly one benchmark point.
- A suite references cases and sets repetition counts. Repetitions acquire more
  evidence for a point; they do not change its identity.
- `point_id` is the full SHA-256 of the canonical typed experiment identity.
  The readable `point_name` is display text, never a key.
- Process lifecycle, engine preparation, OS page-cache treatment, and the
  named warm-up program are separate cache-condition fields in point identity.
  Process-cold explicitly leaves the OS page cache uncontrolled. Neither
  page-cache-cold nor storage-cold is representable without stronger controls.
- The engine does not depend on this crate. The runner depends on the public
  engine surface and consumes the plans produced here.
- Runner-v1 builds one already-diverged fixture at a stable `active` path,
  verifies it, closes it, and freezes its complete physical tree by SHA-256.
  Public execution performs construction in a dedicated process group under a
  bounded watchdog. That child also byte-digests the completed tree, makes the
  never-opened APFS clonefile template, and removes `active` before returning
  an identity-checked handoff. The parent accepts it only after the direct child
  has been reaped and the process group is gone. Any failed, partial, or
  panicked fixture build quarantines its disposable workspace instead of
  deleting possibly active state. Every repetition clone-restores the template
  to that same `active` path, so Lance shallow
  branches retain valid absolute base paths and samples do not accumulate
  branch, manifest, or deletion history. Reset and pre-timer proof traverse
  metadata but never read file contents or fall back to a byte copy.
- Every repetition runs in a fresh worker process whose executable SHA-256,
  release-profile facts, source commit/dirty evidence, and effective
  `LANCE_MEM_POOL_SIZE` must match the parent attestation. Its complete cache
  condition is part of point identity. When declared, its read-only warm-up
  program runs before measured counters and the monotonic merge timer begin. A
  storage firewall permits only the engine's one balanced,
  empty create-if-absent capability probe during each read-write open; every
  other pre-measurement write is rejected. The worker also proves the restored
  tree's complete metadata shape before it declares itself ready. Each timed
  repetition performs exactly one branch merge. `reopened-after-program`
  additionally drops and reopens the engine handle after the warm-up while the
  firewall remains closed; it makes no cache-invalidation claim.
  `preparation-only` is executable as process-cold: it has no declared warm-up,
  while ordinary engine open and protected-head capture still occur and the OS
  page cache remains explicitly uncontrolled.
- After the measured window closes, the worker reads and verifies the exact
  expected rows and values across every table on target, source, and main,
  including untouched tables, and proves that source and main branch heads did
  not move. The parent does not independently read those content bytes. It
  independently derives and validates the point/case identities, merge route,
  and declared table/total-row count attestations returned by the worker. The
  run also requires a real three-way merge and one `TableWalk` interval per
  diverged edge table, so a fast-forward or otherwise vacuous sample fails
  loudly. This runner-v1 verification is deliberately O(store), outside the
  measured window. Receipt-based O(delta) certification begins with the future
  versioned-S3 reset slice. A future DST oracle may add an independent check,
  but never replaces the per-repetition content probes.
- The supervisor starts the declared hard deadline immediately before sending
  `Begin`. If the worker has not sent `Settled` by that deadline, the supervisor
  sends `SIGKILL` to its process group, waits for and reaps the child, and proves
  the group is gone. Every repetition failure, including a trailing protocol
  frame after `Complete`, rejects the sample. Cleanup is permitted only when
  the direct child was reaped, its process group is gone, and bounded
  stdout/stderr capture reached clean EOF; otherwise the disposable workspace
  is quarantined. Thus a contained failure may be cleaned up, while an unproved
  containment state is preserved for inspection. A killed or partial mutation
  never becomes a sample.
  Preparation and exact verification use separate bounded watchdogs, each with
  a 300-second minimum allowance.
- Store counts are **logical store calls** made by the engine. They do not
  observe retries, pagination, multipart fan-out, or other physical attempts,
  and therefore are not network-request or cloud-cost measurements.

## Runner-v1 support envelope

Execution currently supports only synthetic builder v2 with seed `0`. Its even
total table count is split equally between immutable node endpoint tables and
uniform-ring edge tables; declared divergence applies to edge tables. It uses
scalar uniform bulk-loaded data, no indexes or pre-existing deletion history,
local filesystem storage, same-host embedded execution, one client, distinct-key
write-heavy divergence, manual scheduling, local-clonefile reset, per-phase
attribution, and a monotonic timer. Process-cold, warmed-by-program, and
reopened-after-program engine preparation are supported with their exact
cross-validated cache declarations.

The host probe currently proves APFS on an internal macOS NVMe or SATA SSD. A
debug build, S3 or Azure backend, server execution, plain-copy reset, unproved
host declaration, unsupported scenario axis, deadline breach, reset witness
mismatch, non-general merge route, or content mismatch is refused instead of
being approximated. A true OS-page-cache-cold claim is not representable yet;
it requires a named platform/backend control and a post-control eviction
witness. Storage-cold is likewise unsupported and unrepresentable.

Before initialization, runner-v1 derives the exact builder publication recipe
and refuses an emergent history depth. Its local construction envelope is at
most 256 tables, 10 million base rows, 4 GiB of conservatively estimated
generated bytes, 100,000 commits per branch, and 750,000 estimated fixture
entries. It also requires free scratch capacity for a 16x byte-amplification
allowance plus 1 GiB before writing the fixture. These are runner safety limits,
not case-schema or engine limits.

`suite run --json` emits a versioned diagnostic execution projection with
`durable_record: false`. It is useful for inspecting samples, phase timings,
per-repetition worker peak RSS, logical calls, the exact worker executable
SHA-256 plus release profile/optimization/debug-assertion attestation, source
commit/dirty evidence, effective `LANCE_MEM_POOL_SIZE`, fixture identity, and
verification, but it is not a benchmark record and must not be
archived as one. Immutable JSON records, their
rebuildable database projection, fixture caching, and AWS orchestration belong
to later, separately reviewed slices.

Machine-readable failures keep the suite/case/point identity and all completed
runs or repetitions. A worker killed at its hard deadline contributes
structured process-containment evidence, but never a partial sample.
