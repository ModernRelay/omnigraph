# OmniGraph benchmark harness

`omnigraph-bench` owns benchmark definitions and planning. In this first slice
it parses strict, versioned YAML case and suite documents, validates their
semantics, computes stable experiment identities, and emits execution plans.
It deliberately does not execute workloads or persist measurements yet.

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
- The engine does not depend on this crate. A later runner slice will depend on
  the engine and consume the plans produced here.
- Raw immutable JSON run records will be the telemetry source of truth. A
  database projection and AWS orchestration belong to later, separately
  reviewed slices.
