# Typed storage failures

**Decides:** a storage failure is `OmniError::Storage(StorageFailure)` with a closed `StorageFailureKind` that records only what typed evidence proves, and the kind never decides whether to retry.
**Contract:** keeps E4
**Code:** `crates/omnigraph-storage/src/lib.rs`, `crates/omnigraph-core/src/error.rs`, `crates/omnigraph-server/src/lib.rs`
**Cited as:** RFC 0038

## Context

A storage error flattened to a string cannot tell a timeout from a missing
dataset, bad credentials, a lost compare-and-swap or corruption without
parsing text, and every consumer would rediscover its own partial taxonomy.
A broad "retryable" label is worse: it lets a caller replay an operation that
never proved the replay safe. Only the operation that knows whether its
effect happened may decide to retry.

## Decision

- `StorageFailureKind` is `Transient`, `Configuration`, `NotFound`,
  `Precondition`, `Permanent` or `Unknown`. `StorageFailure` holds the kind
  and the complete operator-facing message, which displays unchanged.
- `Unknown` means the evidence is insufficient. It is not a weak
  `Transient` and implies neither retry nor permanent failure.
- `is_transient()` is exactly `kind == Transient`. There is no
  `should_retry`, replay or idempotency API.
- `omnigraph-storage` classifies `object_store` and storage-owned
  `std::io` errors. The engine core classifies Lance and Lance Namespace
  errors, every variant mapped explicitly; a new upstream variant is
  reviewed at the dependency bump.
- Classification follows typed sources only, at most eight links, and never
  reads display text. An explicit outer condition wins over its source.
  Reaching the bound, a cycle or an opaque source gives `Unknown`.
- There is no blanket `From` from `lance::Error`, `ArrowError` or
  `DataFusionError` into `OmniError`. Each call site chooses a storage
  failure, a domain error or an internal or integrity error. Manifest Arrow
  failures are manifest-internal, Blob descriptor contradictions are
  `BlobIntegrity`, and user query failures stay `DataFusion`.
- Every Lance conflict family maps to `Precondition`: state must be
  re-evaluated. Only the exact table-commit adapter, after its own
  effect-free proof, reports `RetryableCommitConflict`.
- Failures of a user-supplied external Blob are not storage failures of the
  graph.
- The server answers `OmniError::Storage` with its generic 500 and the
  message. The kind is not on the wire or in OpenAPI.

## Rejected

- **Keep strings:** keeps the ambiguity and makes every consumer parse
  unstable text.
- **Expose `should_retry`:** a failure class cannot prove effect certainty,
  idempotency or safe replay.
- **Default unknown sources to transient:** turns missing evidence into a
  retry promise, with retry storms and unsafe replays.
- **Default unknown sources to permanent:** strands recoverable failures and
  also claims evidence the substrate did not give.
- **Expose the kind over HTTP:** no client needs it, and it would make the
  taxonomy a wire-compatibility surface too early.
