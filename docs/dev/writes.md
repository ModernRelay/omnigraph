# Graph write protocol

**Audience:** engine and storage contributors
**Authority:** current graph-visible write path; recovery classification is in
[recovery.md](recovery.md)

Every successful graph-content write has one visibility point: a conditional
`__manifest` publication. Lance table effects may happen earlier, but only
after a durable recovery sidecar owns their exact intended outcome.

## The protocol

```text
capture accepted authority
        ↓
prepare logical change and validate it
        ↓
stage exact Lance transactions (no HEAD movement)
        ↓
acquire schema → branch → sorted-table gates
        ↓
recheck recovery barrier and complete authority
        ↓
persist identity-bearing recovery sidecar
        ↓
commit participant table effects
        ↓
confirm achieved effects
        ↓
publish every table pointer + graph lineage in one manifest CAS
        ↓
audit and remove recovery sidecar
```

An error before the sidecar/effects leaves graph storage unchanged. Once any
participant effect is possible, an error that cannot prove a complete terminal
outcome returns `RecoveryRequired`; it never replans around the partial state.

## Captured authority

A write attempt captures one immutable `WriteTxn` containing the accepted
schema/catalog, target graph branch, optional graph head, native branch
identity, table-incarnation identities, and expected table versions. Every
planning and validation step uses that view.

Finalization acquires the root-shared gate order:

1. schema;
2. target branch;
3. touched `(table identity, physical branch)` entries in deterministic order;
4. coordinator publication.

These gates order work inside one process. Correctness still depends on the
persisted manifest precondition, exact Lance transaction identity, and recovery
record. A retryable pre-effect attempt discards all staged work, captures a new
`WriteTxn`, and repeats boundedly; it never reuses batches against a new base.

## Writer adapters

All current graph-visible writers share the publication door but have different
physical-effect proofs:

| Writer | Physical adapter | Publication |
|---|---|---|
| Mutation / Load | One exact staged keyed, overwrite, or delete transaction per touched table | One graph commit |
| SchemaApply | Exact existing-table rewrites plus owned first-touch table creation and the complete schema/manifest delta | One main-branch graph commit |
| BranchMerge | Pointer adoption, a proven insertion chain, or a bounded ordered-diff transaction chain | One target-branch graph commit |
| EnsureIndices | One exact mixed `CreateIndex` transaction per productive table; untrainable vector work remains pending | One pointer publication when work lands |
| Optimize | Bounded compaction and index-fold maintenance over the complete planned table set | At most one monotonic main publication |

Native graph-branch create/delete is a control exception. `BranchContents` is
the logical authority; clone/delete residue is derived physical state and is
reclaimed only when its target is provable from that authority. It does not
invent an alternate graph-content publisher. Each branch life owns a native ref
named `{logical}.{ULID}` (see [RFC 0042](../rfcs/0042-incarnation-suffixed-branch-refs.md));
a recreated branch therefore never shares a path with its dead predecessor, and
the predecessor's forks are reclaimed by `cleanup` rather than healed in place.

## Mutation and Load

`MutationStaging` accumulates read-your-writes batches and delete predicates
in memory. It performs all type, value, uniqueness, endpoint, cardinality, and
resource validation before staging. `stage_all` produces one exact transaction
per touched table without moving HEAD; `commit_all` enters the gate and
recovery sequence above.

Existing-table constructive transactions stage independently with bounded
concurrency. `OMNIGRAPH_LOAD_CONCURRENCY` selects that width for both Load and
ordinary insert/update mutations (default 8). Deferred first-touch branch
effects and delete transactions remain serial. The setting changes only
fragment preparation: every participant still crosses the same recovery
boundary and one graph-manifest publication.

The D2 rule keeps one mutation query constructive (insert/update) or
destructive (delete), never both. Compose mixed work through separate
mutations, or through a branch when a later merge must expose one combined
result.

## Keyed writes

Every v6 graph table has exactly the non-null physical `id` field as Lance's
unenforced primary key. Production strict insert and upsert route through the
sealed, exact-`id`, filter-bearing MergeInsert adapter:

- strict insert probes the pinned parent and returns `KeyConflict` for an
  existing ID;
- upsert updates or inserts without changing modes on retry;
- a bare Lance Append is not a production graph-table write;
- one table's keyed input is bounded to 8,192 rows and 32 MiB before recovery
  arm.

An insertion-only transaction may carry the internal
`omnigraph.insert_absence = "v1"` certificate after its absence and physical
shape are proven. Branch merge accepts the shortcut only across a complete,
contiguous, structurally valid history. The certificate is an optimization
capability, not an authenticity mechanism; unfamiliar or cleaned history falls
back to the general merge.

## First-touch tables and lazy branches

A named graph branch may inherit a main-table version without owning a native
table ref. The first write stages against the inherited snapshot, records the
intended ref/table ownership in recovery, and creates the physical branch only
inside the protected effect window. Recovery may delete only a ref or dataset
whose exact creation it owns. Table forks are named by the branch's native ref;
sidecar table pins carry that native name while the sidecar's `branch` stays
logical.

Stable table/incarnation identity, not `table_key`, determines whether a
registration, rename, tombstone, pointer, or recovery effect belongs to the
same lifetime.

## External Blob inputs

Blob URI admission is part of preparation. The graph's
`ExternalBlobPolicy` defaults to deny; served graphs retain only server-safe
bases. The adapter normalizes and coalesces authorized sources, bounds selected
reference count and URI metadata, probes each source once, and charges selected
payload ranges before reading bytes or arming recovery.

Overwrite can preserve an allowed external descriptor through Lance
`WriteParams`. Keyed writes and row-writing merge paths materialize selected
external bytes under the operation's 32 MiB budget because Lance's MergeInsert
surface has no equivalent reference-preservation hook. A pointer-only branch
adoption does no source I/O. See [blob.md](blob.md).

## Failure outcomes

| Observation | Outcome |
|---|---|
| Parse, validation, policy, limit, or authority failure before effects | Typed error; no graph movement |
| Retryable authority movement before effects on a replay-safe adapter | Discard the complete attempt and reprepare boundedly |
| Strict read-set movement | `ReadSetChanged` |
| Exact duplicate on strict insert | `KeyConflict` |
| Every owned table effect achieved, manifest not yet published | Recovery rolls the fixed outcome forward |
| A proven subset achieved | Full recovery compensates or completes according to the writer's fixed plan |
| Foreign or ambiguous effect | Fail closed; never claim or publish it |

An acknowledgement is returned only after the manifest commit is durable and
visible.

## Policy and attribution

Every public mutating `_as` entry point enforces its Cedar action/scope/actor
gate in the engine. The trusted server resolves the actor; direct embedded
callers must pass one when a policy checker is installed. Actor attribution
travels with the pre-minted graph lineage and is published with the same
manifest CAS.

## Maintenance of this protocol

A new writer must:

- declare its complete authority token and effect set;
- use the shared gate order and publication primitive;
- define exact recovery classification and compensation;
- add its sidecar kind/shape and recovery tests;
- join the durable-call/source guards in `forbidden_apis.rs`;
- prove bounds and crash windows at the owning layer.

Design rationale and rejected alternatives live in
[RFC 0022](../rfcs/0022-unified-write-path.md),
[RFC 0023](../rfcs/0023-key-conflict-fencing.md), and
[RFC 0028](../rfcs/0028-stable-schema-identity.md).
