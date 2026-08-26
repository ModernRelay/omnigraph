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
  It then makes a never-opened APFS clonefile template. Every repetition
  clone-restores the template to that same `active` path, so Lance shallow
  branches retain valid absolute base paths and samples do not accumulate
  branch, manifest, or deletion history. Reset and pre-timer proof traverse
  metadata but never read file contents or fall back to a byte copy.
- Every repetition runs in a fresh worker process whose executable SHA-256 and
  release-profile facts must match the parent attestation. Its declared
  read-only warmth program runs before measured counters and the monotonic
  merge timer begin. A storage firewall permits only the engine's one balanced,
  empty create-if-absent capability probe during each read-write open; every
  other pre-measurement write is rejected. The worker also proves the restored
  tree's complete metadata shape before it declares itself ready. Each timed
  repetition performs exactly one branch merge. Post-invalidation additionally
  drops and reopens the engine handle after the warmth program while the
  firewall remains closed. Cold execution is still refused: a fresh process
  does not prove that operating-system page caches are cold.
- After the measured window closes, verification checks the exact expected
  rows and values across every target table, including untouched tables. It
  separately proves that source and main still have their exact frozen content
  and that their branch heads did not move. The run also requires a real
  three-way merge and one `TableWalk` interval per diverged table, so a
  fast-forward or otherwise vacuous sample fails loudly.
- The supervisor starts the declared hard deadline immediately before sending
  `Begin`. If the worker has not sent `Settled` by that deadline, the supervisor
  sends `SIGKILL` to its process group, waits for and reaps the child, and proves
  the group is gone before the active tree can be removed. Finalization also
  requires bounded, clean EOF on stdout and stderr with no trailing protocol
  frame; otherwise the disposable workspace is quarantined. A killed or
  partial mutation never becomes a sample. Preparation and exact verification
  use separate bounded watchdogs, each with a 300-second minimum allowance.
- Store counts are **logical store calls** made by the engine. They do not
  observe retries, pagination, multipart fan-out, or other physical attempts,
  and therefore are not network-request or cloud-cost measurements.

## Runner-v1 support envelope

Execution currently supports only synthetic builder v1 with seed `0`, scalar
uniform bulk-loaded data, no indexes or pre-existing deletion history, local
filesystem storage, same-host embedded execution, one client, distinct-key
write-heavy divergence, manual scheduling, local-clonefile reset, per-phase
attribution, and a monotonic timer. Warm and post-invalidation regimes are
supported.

The host probe currently proves APFS on an internal macOS NVMe or SATA SSD. A
debug build, cold regime, S3 or Azure backend, server execution, plain-copy
reset, unproved host declaration, unsupported scenario axis, deadline breach,
reset witness mismatch, non-general merge route, or content mismatch is
refused instead of being approximated.

Before initialization, runner-v1 derives the exact builder publication recipe
and refuses an emergent history depth. Its local construction envelope is at
most 256 tables, 10 million base rows, 4 GiB of conservatively estimated
generated bytes, 100,000 commits per branch, and 750,000 estimated fixture
entries. It also requires free scratch capacity for a 16x byte-amplification
allowance plus 1 GiB before writing the fixture. These are runner safety limits,
not case-schema or engine limits.

`suite run --json` emits a versioned diagnostic execution projection with
`durable_record: false`. It is useful for inspecting samples, phase timings,
logical calls, parent/worker build attestation, fixture identity, and
verification, but it is not a benchmark record and must not be archived as
one. Immutable JSON records, their
rebuildable database projection, fixture caching, and AWS orchestration belong
to later, separately reviewed slices.

Machine-readable failures keep the suite/case/point identity and all completed
runs or repetitions. A worker killed at its hard deadline contributes
structured process-containment evidence, but never a partial sample.
