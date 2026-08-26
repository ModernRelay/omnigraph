# OmniGraph benchmark harness

`omnigraph-bench` owns benchmark definitions, planning, execution, durable run
records, and the rebuildable result projection. It parses strict, versioned
YAML case and suite documents, validates their semantics, computes stable
experiment identities, and can execute supported local `branch-merge-v1`
points. Run wall-clock measurements from a clean release build only:

```bash
cargo run --release --locked -p omnigraph-bench -- \
  suite run benchmarks/suites/local-smoke.suite-v1.yaml \
  --archive .bench/archive
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
  source commit/dirty state, Cargo's
  release/opt-level observations, compiler-effective debug assertions, the
  checked-in release-profile declaration, build-script-visible flag/override
  state, Cargo features reported by `omnigraph-engine` itself, and a versioned
  allowlist of effective engine environment. Unknown `LANCE_*` or
  engine-facing `OMNIGRAPH_*` overrides are refused without serializing their
  values. Cargo does not expose the final target rustc invocation to
  `build.rs`, so effective LTO/codegen-unit/strip settings remain explicitly
  unproved until a later controlled digest-bound build receipt supplies them.
  Its complete cache condition is part of point identity. When declared, its
  read-only warm-up program runs before measured counters and the monotonic
  merge timer begin. A storage firewall permits only the engine's one balanced,
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
- Fixture validation covers the exact schema, empty index inventory, and every
  row on `main`, `bench-source`, and `bench-target`. Its logical-content digest
  is stable across rebuilt Lance ids, timestamps, and encodings; a separate
  physical tree digest pins the exact bytes restored for every repetition.

## Durable records and archive

Passing `--archive <DIR>` changes successful `suite run` finalization from a
diagnostic-only run into durable telemetry publication. The harness mints one
session ULID for the command and one invocation ULID per suite entry. Each
record contains the complete typed run spec, exact point identity, clean source
commit and declared release-build evidence, executable digest, process-effective machine and
backend evidence,
stamped fixture manifest, raw repetition rows, dispersion, logical calls, and
explicit presence or absence statements for physical attempts, request timing,
calibration, and concurrency witnesses.

A dirty or unproved source tree cannot publish a record because the source
commit would not honestly describe its provenance. The exact executable is
identified by its digest and normalized compiler/build/engine facts. Build
after committing the intended source, then verify the resulting archive:

```bash
target/release/omnigraph-bench suite run \
  benchmarks/suites/local-smoke.suite-v1.yaml \
  --archive .bench/archive

target/release/omnigraph-bench archive verify .bench/archive
```

If publication reports `possibly_published`, reconcile that exact candidate
before minting a replacement invocation:

```bash
target/release/omnigraph-bench archive reconcile .bench/archive \
  --invocation-id <INVOCATION_ULID> \
  --record-sha256 <RECORD_SHA256> --json
```

Reconciliation holds the publication lock, validates the exact immutable
pointer and canonical record, and retries the required file/directory syncs.
It returns `durable`, `absent`, or `conflict`; only `durable` exits successfully.

Canonical compact JSON objects live below `objects/sha256/`. Publication makes
the object durable first, then atomically installs an immutable invocation
pointer below `invocations/`. Only reachable, fully validated pointers are
records; a crash can leave an unreferenced content object but cannot expose a
partial record. Reusing an invocation for unequal bytes fails closed, and
publishing identical bytes is idempotent. The JSON archive is the only result
authority. `archive verify` streams a fixed invocation inventory and returns a
compact count and inventory digest; it does not retain or print the complete
record set.

Machine evidence records OS/kernel/CPU/memory facts, the worker-inherited nice
level and scheduler policy/priority, and a versioned digest of a fixed common
set of soft/hard process resource limits. The hostname-derived label omits the
raw hostname but is only a non-secret, non-stable correlation hint: it is not
anonymization, a privacy boundary, or proof of machine identity. On Linux the
record also includes process CPU affinity and the effective cgroup-v2
CPU/memory limits plus a bounded fingerprint of every stable controller
setting across the inherited hierarchy; cgroup-v1, hybrid control, and
scheduler policies whose canonical parameters are not fully represented are
refused rather than published with incomplete identity.
Every repetition worker captures this identity immediately before `Ready`;
the parent refuses a run if any repetition differs, and record finalization
uses that worker-attested identity rather than a long-lived CLI snapshot.

## Rebuildable query projection

The team-facing query database is an OmniGraph read model generated from the
complete archive. It is never written in a measured window and has no
incremental mutation API:

```bash
target/release/omnigraph-bench projection rebuild \
  --archive .bench/archive \
  --root .bench/projection

target/release/omnigraph-bench projection list-points \
  --root .bench/projection --limit 100

target/release/omnigraph-bench projection list-runs \
  --root .bench/projection \
  --point-id <FULL_SHA256_POINT_ID> --limit 100
```

Rebuild validates every archive record, collision-checks point and invocation
identity, loads a fresh bounded generation through the public engine surface,
verifies its complete inventory and a canonical digest over every projected
point and run field. Only then does it atomically replace `CURRENT`. Public and
internal queries are bounded and use exclusive cursors pinned to one immutable
generation. Pass the JSON `next_cursor` from one page back through `--cursor`
to continue. A generation id is derived from the projection schema,
source-to-row transform contract, sorted archive inventory, and complete
projected-row digest, so neither stale formulas nor bad field mappings can be
reused silently. Rebuilds serialize across processes with a bounded lock wait,
clean abandoned staging directories, and retain at most eight published
generations; reaching that ceiling requires deleting the disposable projection
root and rebuilding it.
Query callers choose fixed, parameterized names; arbitrary GQ text is not
accepted. The projection may be deleted at any time and rebuilt without losing
evidence.

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

Without `--archive`, `suite run --json` still emits a versioned diagnostic
execution projection whose runs have `durable_record: false`; it must not be
copied into the archive. With `--archive`, each complete run is canonically
encoded, published, and then dropped from CLI memory. The success output omits
the raw `runs` array and carries only `completed_run_count` plus authoritative
content addresses and invocation pointers. Archive-mode failures likewise
report the completed count and known receipts without duplicating previously
published raw samples. A completed run whose record could not be published is
retained once as state-neutral `completed_run` recovery evidence, because an
indeterminate pointer sync may already be authoritative. That sync also carries
`possibly_published` for candidate-specific reconciliation. Human-mode failures
print the same complete recovery JSON envelope, not only a timing summary.
Fixture caching and AWS orchestration belong to later, separately reviewed
slices.

Machine-readable diagnostic-mode failures keep the suite/case/point identity
and all completed runs or repetitions. A worker killed at its hard deadline
contributes structured process-containment evidence, but never a partial
sample.
