# RFC 0038: Typed storage failures

| | |
|---|---|
| **Status** | Proposed |
| **Author track** | Public contribution |
| **Author(s)** | Ragnor Comerford ([`ragnorc`](https://github.com/ragnorc)) |
| **Discussion** | [PR #491](https://github.com/ModernRelay/omnigraph/pull/491) and its [maintainer review](https://github.com/ModernRelay/omnigraph/pull/491#issuecomment-5281302575) |
| **Implementation** | [PR #491](https://github.com/ModernRelay/omnigraph/pull/491) is the obsolete prototype; a replacement PR will implement this RFC from current `main`. |

> Status is maintained by maintainers: `Proposed` while the PR is open,
> `Accepted` on merge, `Declined` on close, and `Superseded by NNNN` later.

## Summary

OmniGraph will expose storage failures as a closed, typed Rust API without
turning that classification into a generic retry decision. `OmniError::Lance`
becomes `OmniError::Storage(StorageFailure)`, and `StorageFailureKind`
distinguishes positive evidence of transient, configuration, absence,
precondition, and permanent conditions from `Unknown`. Storage diagnostics keep
their historical operator-facing text. HTTP, OpenAPI, and persisted graph data
do not change.

## Motivation

Today `OmniError::Lance(String)` erases the structured condition reported by
Lance, `object_store`, `std::io`, and Lance Namespace. A caller can display the
message but cannot distinguish a timeout from a missing dataset, bad
credentials, a stale compare-and-swap, or corruption without parsing text.
That invites two long-lived liabilities:

1. Every consumer has to rediscover an incomplete, string-based taxonomy.
2. A broad label such as "retryable" can accidentally authorize replay where
   the operation has not proved that replay is safe.

The prototype in PR #491 moved in the right direction but made three unsafe
collapses. Opaque `object_store::Error::Generic` sources defaulted to transient,
`NotModified` and already-exists conditions lost their precondition meaning,
and every Lance retryable commit conflict became an engine replay signal.
It also inherited stale Arrow, DataFusion, Blob, and conflict conversions from
an older `main`, including doubled `storage:` display prefixes.

The durable contract needs to represent only what the typed cause proves. The
operation that knows whether an effect occurred remains the only layer allowed
to decide whether and how to retry.

## Guide-level explanation

The Rust API becomes:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFailureKind {
    Transient,
    Configuration,
    NotFound,
    Precondition,
    Permanent,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageFailure {
    pub kind: StorageFailureKind,
    pub message: String,
}

impl StorageFailure {
    pub fn is_transient(&self) -> bool {
        self.kind == StorageFailureKind::Transient
    }
}

pub enum OmniError {
    // Existing exhaustive variants remain.
    Storage(StorageFailure),
}
```

`StorageFailureKind` means:

| Kind | Evidence carried |
|---|---|
| `Transient` | A timeout, throttling response, cancellation, or recognized transport interruption occurred. |
| `Configuration` | Authentication, permission, unsupported operation, malformed input/location, or an exhausted configured disk cap prevents the request as configured. |
| `NotFound` | The requested object, dataset, ref, version, index, or namespace entity is absent. |
| `Precondition` | Already-exists, not-modified, CAS/concurrency conflict, stale transaction/ref, or fenced authority requires state to be re-evaluated. |
| `Permanent` | Typed evidence reports corruption, a schema/storage invariant failure, panic, or substrate-internal failure. |
| `Unknown` | The typed evidence is insufficient. Neither retry nor permanent escalation is implied. |

The enum classifies the observed failure, not the safety of repeating an
operation. `is_transient()` is deliberately equivalent to
`kind == StorageFailureKind::Transient`. There is no `should_retry`, replay, or
idempotency API. A caller may use the classification as one input to an
operation-local policy, but it must separately prove the operation's effect and
replay boundary.

`StorageFailure.message` is the complete operator-facing diagnostic, and
`OmniError::Storage` displays it unchanged. Existing genuine storage messages
therefore remain exact:

```text
storage read failed for 's3://bucket/key': <object-store error>
storage: <Lance error>
storage: nearest: <Lance error>
```

The server maps `OmniError::Storage` explicitly to its existing generic HTTP
500 response and uses this complete display string. It does not add another
prefix or expose `StorageFailureKind` in a response body or OpenAPI schema.
Existing domain errors retain their operation-specific status codes.

## Reference-level design

### Ownership and dependency boundaries

`omnigraph-storage` owns `StorageFailure`, `StorageFailureKind`, and the bounded
classification of `object_store::Error` and storage-owned `std::io::Error`.
The engine re-exports the public types and owns Lance and Lance Namespace
classification because the storage adapter deliberately does not depend on
Lance.

There is no blanket `From<lance::Error> for OmniError`. Named engine helpers
construct a direct storage diagnostic or a contextual storage diagnostic. The
absence of a blanket conversion forces each Lance call site to choose one of
three meanings:

- a substrate storage failure;
- an existing operation-local domain error; or
- an OmniGraph internal/integrity failure.

There are likewise no blanket `From<ArrowError>` or
`From<DataFusionError>` implementations. Arrow batch, shape, and computation
failures in manifest machinery are manifest-internal errors. Persisted Blob
descriptor contradictions remain `BlobIntegrity`. User query planning, schema,
and execution failures remain `DataFusion`.

`ExternalBlobPolicy` and `ExternalBlobSource` continue to describe user-supplied
external Blob references. Failure to admit or fetch such a reference is not a
failure of the graph's own storage substrate and is not folded into `Storage`.

### Bounded typed-source traversal

Classification follows typed sources only. It never searches upstream display
text for status codes, phrases, provider names, or retry hints. At most eight
source links are inspected. Reaching the bound without typed evidence returns
`Unknown`. Cyclic source chains also terminate at the bound and return
`Unknown`.

For wrappers with both an outer typed condition and a source, the explicit
outer condition wins. Only opaque wrappers such as object-store `Generic` and
Lance `IO`, `Wrapped`, and `External` recurse into the source.

### `object_store` and storage-owned I/O

The adapter maps `object_store` 0.13.2 as follows:

| Upstream condition | `StorageFailureKind` |
|---|---|
| `NotFound` | `NotFound` |
| `NotModified`, `Precondition`, `AlreadyExists` | `Precondition` |
| `InvalidPath`, `NotSupported`, `NotImplemented`, `PermissionDenied`, `Unauthenticated`, `UnknownConfigurationKey` | `Configuration` |
| `JoinError` cancelled | `Transient` |
| `JoinError` panic | `Permanent` |
| `Generic` | recursively classify its typed source; otherwise `Unknown` |
| future non-exhaustive variant | `Unknown` |

Storage-owned `std::io::ErrorKind` values map as follows:

| I/O condition | `StorageFailureKind` |
|---|---|
| `NotFound` | `NotFound` |
| `AlreadyExists` | `Precondition` |
| `PermissionDenied`, `InvalidInput`, `Unsupported` | `Configuration` |
| `TimedOut`, `Interrupted`, `ConnectionAborted`, `ConnectionRefused`, `ConnectionReset`, `BrokenPipe`, `NotConnected`, `HostUnreachable`, `NetworkUnreachable`, `WouldBlock` | `Transient` |
| `InvalidData` | `Permanent` |
| every other kind without stronger typed evidence | `Unknown` |

Local filesystem backend failures use this same path. `OmniError::Io` remains
for non-storage I/O only. The adapter's
`CreateIfAbsentUnsupported` condition becomes `Configuration`. These changes
carry the existing complete display text, including `io: ...` and
`storage <operation> failed for ...` where those are the historical messages.

### Lance 10 mapping

Every current `lance::Error` variant is mapped explicitly:

| `StorageFailureKind` | Lance variants |
|---|---|
| `Transient` | `Timeout` |
| `Configuration` | `DiskCapExceeded`, `InvalidInput`, `InvalidTableLocation`, `InvalidRef`, `NotSupported`, `FieldNotFound`, `Unprocessable` |
| `NotFound` | `DatasetNotFound`, `NotFound`, `RefNotFound`, `VersionNotFound`, `IndexNotFound` |
| `Precondition` | `DatasetAlreadyExists`, `CommitConflict`, `IncompatibleTransaction`, `RetryableCommitConflict`, `TooMuchWriteContention`, `RefConflict`, `VersionConflict`, `Fenced` |
| `Permanent` | `CorruptFile`, `SchemaMismatch`, `Internal`, `Arrow`, `Schema` |
| `Unknown` | source-free `Execution`, `Index`, `Cleanup`, `Cloned`, `PrerequisiteFailed`, and `Stop` |

`IO`, `Wrapped`, and `External` recursively classify their typed source and
otherwise become `Unknown`. `Namespace` downcasts its source to
`lance_namespace::NamespaceError`; a failed downcast uses the same bounded
typed-source rule and otherwise becomes `Unknown`.

The apparently counterintuitive corrections are deliberate. A Lance
`Execution` string is not automatically a DataFusion user error, and a
source-free execution/index/cleanup failure contains no typed proof of either a
transient transport condition or a permanent substrate invariant failure.
Conversely Lance's own `Arrow`, `Schema`, and `Internal` variants are positive
evidence of a substrate-internal or persisted-schema failure and are
`Permanent`, not generic query errors.

### Lance Namespace 10 mapping

All 24 current `lance_namespace::ErrorCode` values are exhaustive inputs:

| `StorageFailureKind` | Namespace codes |
|---|---|
| `Transient` | `ServiceUnavailable`, `Throttling` |
| `NotFound` | `NamespaceNotFound`, `TableNotFound`, `TableIndexNotFound`, `TableTagNotFound`, `TransactionNotFound`, `TableVersionNotFound`, `TableColumnNotFound`, `TableBranchNotFound` |
| `Configuration` | `Unsupported`, `InvalidInput`, `PermissionDenied`, `Unauthenticated`, `TableSchemaValidationError` |
| `Precondition` | `NamespaceAlreadyExists`, `TableAlreadyExists`, `TableIndexAlreadyExists`, `TableTagAlreadyExists`, `TableBranchAlreadyExists`, `ConcurrentModification`, `NamespaceNotEmpty`, `InvalidTableState` |
| `Permanent` | `Internal` |

If Lance Namespace later adds a code, the exhaustive mapping must be reviewed
when the dependency is upgraded. No namespace code currently maps to
`Unknown`.

### Conflict and retry boundaries

The generic Lance classifier maps every Lance conflict family, including
`RetryableCommitConflict`, to `Storage(Precondition)`. That result means only
that current state must be re-evaluated.

Existing operation-local boundaries retain their narrower meanings:

- the exact table commit adapter may translate an effect-free Lance contention
  result to `RetryableCommitConflict` after its existing proof;
- manifest row CAS contention remains
  `ManifestConflictDetails::RowLevelCasContention`;
- optimize keeps its local raw-Lance retry classifier; and
- no other Lance conflict becomes a replay signal.

RFC-034 recovery disposition and RFC-036 supervision remain separate
contracts. A future supervisor may consider `Transient` after its recovery and
effect-state rules authorize another attempt, but this RFC supplies neither
that authorization nor scheduling policy.

### Compatibility

This is an intentional pre-1.0 Rust API break:

- `OmniError::Lance(String)` is removed;
- exhaustive consumers must handle `OmniError::Storage(StorageFailure)`; and
- callers may inspect `StorageFailureKind` instead of parsing text.

Exact operator text is preserved for genuine adapter and Lance storage
failures. Some errors intentionally change category: manifest-owned Arrow
failures become manifest-internal, persisted Blob contradictions become
`BlobIntegrity`, and user DataFusion failures remain `DataFusion`. Those
corrections may change Rust matching and existing operation-specific HTTP
status behavior, but they restore the domain boundary instead of preserving a
misclassification.

There is no graph-format, manifest-schema, Lance-format, recovery-sidecar, wire,
HTTP-schema, or OpenAPI migration. No persisted data changes.

### Acceptance evidence

Tests extend the existing in-source owners and staged table-store suite:

- every object-store variant, recognized and opaque `Generic` sources, nested
  depth seven/eight, a cyclic source, and future/depth fallback;
- local I/O cases for all six categories plus cancelled/panicked join tasks;
- all 24 Lance Namespace codes and every Lance family, including recursive
  `IO`, `Wrapped`, and `External` sources;
- exact direct-adapter, direct-Lance, and contextual-Lance display strings;
- DataFusion user errors, nested typed substrate errors, Arrow/internal
  failures, and Blob-integrity contradictions at their owning boundaries;
- the table-store proof that only the effect-free adapter emits
  `RetryableCommitConflict`, while the generic classifier reports the same
  Lance variants as `Precondition`;
- unchanged publisher and optimize retry vocabularies;
- exact server status/message mapping and an unchanged generated OpenAPI spec;
  and
- a source guard forbidding global `From<lance::Error>`, `From<ArrowError>`,
  or `From<DataFusionError>` implementations.

The implementation must pass the affected baseline before editing, focused
storage/engine/server tests, the canonical failpoint-superset workspace test,
both all-target Clippy graphs with warnings denied, formatting, OpenAPI drift,
and the server AWS-feature suite.

## Invariants & deny-list check

This RFC strengthens Hard Invariant 13: failures become typed and bounded, and
uncertain evidence stays explicit. Its tests follow Invariant 14 by asserting
the adapter, substrate, conflict, and transport boundaries separately. It does
not change graph publication, recovery, schema identity, or source-of-truth
rules.

It also closes two deny-list risks. No caller needs to parse string-flattened
errors for semantics, and exact observable error text is treated as a contract.
The change does not add a transaction manager, retry queue, alternate write
path, or side channel for replay semantics.

## Drawbacks & alternatives

The exhaustive mapping adds maintenance whenever Lance adds an error variant or
Namespace code. That is intentional review pressure at the substrate boundary.
The closed public `StorageFailureKind` is also a pre-1.0 compatibility surface;
adding a kind will break exhaustive downstream matches.

Alternatives rejected:

- **Keep strings.** This preserves ambiguity and forces every consumer to parse
  unstable text.
- **Expose `should_retry`.** Failure class alone cannot prove effect certainty,
  idempotency, or safe replay.
- **Default unknown sources to transient.** That turns missing evidence into an
  operational promise and can create retry storms or replay unsafe work.
- **Default unknown sources to permanent.** That can strand recoverable
  failures and also claims evidence the substrate did not provide.
- **Expose classification over HTTP.** No current client contract needs it, and
  it would prematurely make the taxonomy a wire-compatibility surface.
- **Reuse PR #491's branch.** It predates current Lance 10 Blob, recovery,
  precondition, and manifest code. Resolving its textual conflicts would risk
  resurrecting deleted helpers and obsolete control flow.

## Reversibility

The source and Rust API changes are reversible before 1.0, though reverting
would again erase useful typed evidence. Persisted state and public wire
formats are untouched, so no data migration or rollback procedure is needed.
The implementation will be a fresh patch over current `main`; PR #491 remains
available as historical context but is not merged or rebased.

## Unresolved questions

None. New typed substrate evidence may justify finer classification in a later
RFC, but provider message parsing is not an acceptable substitute.
