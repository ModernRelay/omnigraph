# One seam type for test-time substitution

**Decides:** every place a test substitutes engine behavior (decision sites, clock, id source, write gate, storage and object-store decoration) is a static of one type, `Seam<B, S>`, with an empty slot in production.
**Contract:** none; serves invariant 13
**Code:** `crates/omnigraph-seams`, `crates/omnigraph-core/src/seams.rs`, `crates/omnigraph/src/seams/catalog.rs`, `crates/omnigraph-dst/src/store_places.rs`, `crates/omnigraph-gqt/src/dst_runner/seams.rs`, `scripts/seam_corpus.py`
**Cited as:** RFC 0066

## Context

Substitution had six families and four install styles: failpoints that
injected errors, failpoints that flipped a code path, callback rendezvous,
three thread-local install pairs, and two ways to wrap storage. No list held
them all. The logic-test runner could prove a fault fired only by finding its
error text, so a site that returns success when it fires could not be
tested from a case. The failpoint library's callback could not serve as
proof, because installing one stopped the point from firing.

## Decision

- A seam is a named static with a slot. Production leaves the slot empty,
  and every read compiles out without the `failpoints` or `dst` feature.
  `omnigraph-seams` has no dependencies; its `install` and `decide` features
  follow the reader, so a build cannot arm a seam it never consults.
- The slot scope follows where the seam is crossed. Decision seams,
  `STORAGE` and `OBJECT_STORE` are global, since worker and Lance pool threads
  cross them. `CLOCK`, `IDS` and `GATE` are thread-local, one per actor
  thread.
- Installing returns a guard that empties the slot on drop. Installing over a
  filled slot panics, so two guards never hold one seam.
- A decision seam declares the operation that crosses it and the set of
  effects its site honors (`Fail`, `Skip`, `Contention`), and may declare
  store effects (`Misdirect`). A decider that fires outside the set panics at
  the crossing.
- A seam is declared with `decide_seam!` beside the site it guards, so the
  compiler records its location. The engine's `catalog!` list indexes every
  one. A seam name is a place, never an outcome.
- Site helpers `fail`, `skip` and `contention` each take only a seam declaring
  exactly that effect. The injected text `injected failpoint triggered:
  <name>` is kept.
- `failpoint_names_guard.rs` walks the sources: every helper argument is a
  catalog static, every static is listed and referenced, and no hand-written
  `Seam::decide` remains.
- `OBJECT_STORE` is read on every call, not applied when a store is built,
  because Lance's registry caches stores beyond any guard's life.
- A `--- seam` block is admitted by exact name from the engine catalog or
  `STORE_PLACES`, its action must be in the declared set, and its step must
  be of the seam's operation. Delivery is proved by the installed decider's
  own count, reported as `seam_delivered`. `--- fault` is refused.
- Store places live in `STORE_PLACES` beside the storage decoration. A cell is
  admitted only when the decoration implements it, and a step admits at most
  one store action.

## Rejected

- **A fire listener inside the old failpoint calls:** it proves delivery, but
  no site declares its effect or operation, so the action word and step kind
  stay unchecked.
- **Inject every seam through constructors:** sites in free functions and
  Lance's process-wide registry have no instance to hold the value.
- **A typed catalog over `fail-parallel`:** it keeps four install paths and
  the dependency.
- **Patch `fail-parallel` so a callback can fire:** a forked dependency for
  one feature, with the clock, ids and gate still outside it.
- **Generate the catalog or use `inventory`:** a second source of truth.
