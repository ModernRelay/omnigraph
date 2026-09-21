# Graph write protocol

**Audience:** engine and storage contributors
**Authority:** current graph-visible write path; what a crash leaves and who
finishes it is in [recovery.md](recovery.md)

Every successful graph-content write has one visibility point: a conditional
`__manifest` publication. Table effects happen earlier, but as detached Lance
commits of the pinned base that nothing references until that publication
([RFC 0067](../rfcs/0067-detached-table-commits.md)).

## The protocol

```text
capture accepted authority
        ↓
prepare logical change and validate it
        ↓
stage exact Lance transactions (no HEAD movement)
        ↓
acquire schema → branch → sorted-table gates, recheck the complete authority
        ↓
commit each participant as a detached version of its pin
        ↓
publish every pin (target, staged version, transaction uuid) + lineage in one manifest CAS
        ↓
promote each pin from the held handles (best effort; a pending pin is promoted later)
```

An error before the manifest CAS leaves the graph unchanged: the detached
versions are unreferenced staging, retained until reclamation can be proved.
The caller retries from a fresh snapshot. A successful publication is
acknowledged whether or not promotion ran. A lost acknowledgement is resolved
against the exact attempted manifest; unavailable readback stays indeterminate.
No writer arms a recovery record, and no write replans around a partial state, because
no partial state is ever visible. Schema apply and the system-column upgrade
additionally stage and install the schema contract around their CAS; only
they can report `RecoveryRequired`, naming a manifest commit that landed
while the contract installation did not.

## Captured authority

A write attempt captures one immutable `WriteTxn` containing the accepted
schema/catalog, target graph branch, optional graph head, native branch
identity, table-incarnation identities, and expected table versions. Every
planning and validation step uses that view.

Branch merge also uses the captured target for physical table opens and
publication. It never changes the `Omnigraph` handle's active branch while the
merge runs. Publication reuses the active coordinator, or takes the cached
non-active target coordinator, only when its branch identity, graph head, and
manifest version match the captured transaction; otherwise it opens the target
coordinator from durable state. The publisher independently reads fresh authority
and enforces the exact graph-head precondition on every attempt. Successful
publication returns a taken coordinator to the one-entry merge cache; failure
drops it. Commit IDs and timestamps are minted for the captured branch without
reloading manifest history. The existing schema and branch gates still serialize
conflicting control operations.

Native branch creation uses an operation-local capture of the bound coordinator
or that same one-entry cache after the control gates. Reuse
requires a fresh match of the complete manifest incarnation, including the
native branch lifetime; a stale or missing view takes the existing refresh/open
path. Captures share immutable lineage and the Lance session, copy current
table state, and leave the handle's active branch unchanged.

After a content publication, the publisher returns the projection (the
in-memory `__manifest` state folded from the journal) it already built from the
successful attempt's freshly read base. The coordinator retains
it only when that exact base matches its previously coherent view and its graph
cache has adopted the published lineage. A foreign advance, unsupported base,
or failure before lineage adoption leaves the full-refresh fallback armed.
Registration replacement, rename, tombstone, and same-version physical-owner
handoff use the existing complete fold. This is disposable process memory;
`__manifest` remains the only durable graph authority. Publication still scans
history for collision, expected-version, and lineage validation.

Finalization acquires the root-shared gate order:

1. schema;
2. target branch;
3. touched `(table identity, physical branch)` entries in deterministic order;
4. coordinator publication.

These gates order work inside one process. Correctness still depends on the
persisted manifest precondition and the exact Lance transaction identity each
pin records. A retryable pre-effect attempt discards all staged work, captures a new
`WriteTxn`, and repeats boundedly; it never reuses batches against a new base.

## Writer adapters

All current graph-visible writers share the publication door but have different
physical-effect proofs:

| Writer | Physical adapter | Publication |
|---|---|---|
| Mutation / Load | One exact staged keyed, overwrite, or delete transaction per touched table | One graph commit |
| SchemaApply | Exact existing-table rewrites plus owned first-touch table creation and the complete schema/manifest delta | One main-branch graph commit |
| BranchMerge | Pointer adoption, or a chain of detached chunk commits (proven insertion chain or bounded ordered diff) published as one pin per table (RFC 0067) | One target-branch graph commit |
| EnsureIndices / full-text rebuild | One detached `CreateIndex` batch per productive table, published as a pin like a mutation's effect (RFC 0067); ordinary ensure leaves untrainable vector work pending, explicit FTS rebuild replaces postings from rows | One graph publication when work lands |
| Optimize | One detached compaction `Rewrite` per productive table, chained with a detached whole rebuild of each index whose coverage lags and a detached build of each declared-but-unbuilt index, published as pins (RFC 0067) | One main-branch graph commit with an exact CAS on the pins the batch was planned from |

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
resource validation before staging. `stage_all` opens each touched table at
its pin through the read-handle cache, promoting a pending predecessor pin
first, and produces one exact transaction per table without moving HEAD.
`commit_all` enters the gates, revalidates the complete authority, and commits
every participant as a detached version of its pinned base: no recovery
sidecar is armed, a table's linear HEAD never moves, and nothing can rebase.
The manifest then publishes every pin as `(base + 1, staged version,
transaction uuid)` in one CAS; a publish that loses the CAS returns the plain
`ReadSetChanged` and unproven detached staging is retained. After
publication the writer promotes each pin from the handles it already holds,
replaying the recorded transaction at `base` so the linear history gains an
identical twin. A promotion that fails or is blocked never fails the write:
the pin stays pending, readable through its staged version, and the next
writer of that table or `cleanup` promotes it. A writer whose captured
snapshot predates a promotion sees the linear HEAD one past its published
version; the current manifest explains that HEAD, so the writer reprepares
(`ReadSetChanged`) rather than reporting drift. Cleanup skips version GC on a
table whose pin is blocked. Detached manifests are reclaimed only when a
published pin and matching transaction UUID prove their linear twins. Age
filters proven surplus; it does not prove that an unpublished writer stopped.
Cleanup retains uncertain staging and defers version/file GC for its table,
while continuing on unaffected tables. Historical pins and chain links use the
same proof so successful writes do not leave redundant detached copies.

A pin whose target version a
foreign linear commit occupies is blocked: a later mutation stages from the
detached version and its own promotion waits behind the block, while the
graph-global writers (schema apply, Optimize) promote every pending pin
before they plan and refuse a blocked one. Optimize plans each table's
compaction from its pin and stages the rewrite detached with fragment ids
above the base's high-water mark, so it needs no `ReserveFragments`; a
lagging scalar or vector index is rebuilt whole as a detached commit under
its name (Lance 11 folds only through a linear commit), keeping a vector
index's partition count; the batch publishes once with an exact CAS on every
planned pin, and a pin a concurrent writer moved fails the run with a
read-set conflict so the next run re-plans. A failure before publication
leaves unproven detached versions that cleanup retains; one after it leaves
pending pins the next writer or cleanup promotes. A strict mutation prepared
before Optimize's publication reports the same read-set conflict as it would
after any other writer. `omnigraph repair` reports blocked pins as
`blocked_promotion` and never adopts the foreign commit. First-touch branch
forks are created without an intent record; an unreferenced fork is garbage
that cleanup classifies.

The index writer (`ensure_indices` and the explicit full-text rebuild)
follows the same protocol: it opens each productive table at its pin, stages
the complete BTREE/FTS/vector batch before the gates (a first-touch fork on a
branch is created under the gates, with no intent record), commits every
batch as a detached version of the pin, publishes the pins once and promotes
them. A failure before publication leaves no residue; one after publication
leaves a pending pin that reads, including full-text search through the
batch's certificate, serve from the staged version.

Schema apply stages each existing-table rewrite as a detached Overwrite of
the promoted HEAD, publishes it as a pin one past the published version and
promotes it after the manifest commit. An added type is a linear
version-one create at its identity path; that path is a deterministic
function of the accepted identity allocator, so an attempt that died after
creating the dataset left it exactly where the retry creates it, and the
retry reclaims the unregistered leftover under the schema sentinel before
creating. The schema contract is staged before the manifest commit with the
graph commit it publishes recorded in `__schema_state.json.staging`, and the
writer installs the live contract from memory after the commit. No sidecar is
armed: a failure before the commit leaves detached versions, a created
dataset and a staged contract that the next read-write open discards; a
failure after it leaves a published manifest whose contract installation the
same handle's next write, or the next read-write open, completes because the
recorded commit is in main's lineage. A read-only open refuses that state and serves
an unpublished staging as if it were absent. The open also reclaims a
sentinel left by a crashed apply, under the same one-mutation-process
boundary as every other open-time recovery decision.

Branch merge follows it too. Each target table opens at its pin; every chunk
of the proven insertion chain or the bounded ordered diff commits as a
detached version of the previous chunk (one link per chunk, within the
merge's transaction ceiling), a pointer adoption copies the source's entry
including a pending pin, and a first-touch fork on a named target is created
under the gates with no intent record. The target publishes once, with each
chained table's pin naming its linear base plus the chain length and the
tip as the staged version; the writer then promotes every link in order. A
failure anywhere before publication leaves the target untouched and the
chain as reclaimable garbage; a target that advanced meanwhile makes the
merge lose its manifest CAS and return the ordinary conflict.

Existing-table constructive transactions stage independently with bounded
concurrency. The `stage_write_concurrency` session setting (`process` scope,
range `1..=64`; process default `OMNIGRAPH_LOAD_CONCURRENCY`, an invalid or
`0` value refusing startup instead of running the default) selects that
width for both Load and
ordinary insert/update mutations (default 8). Deferred first-touch branch
effects and delete transactions remain serial. The setting changes only
fragment preparation: every participant still commits detached and crosses
one graph-manifest publication.

The D2 rule keeps one mutation query constructive (insert/update) or
destructive (delete), never both. Compose mixed work through separate
mutations, or through a branch when a later merge must expose one combined
result.

## Keyed writes

Every v6-or-later graph table has exactly the non-null physical `id` field as Lance's
unenforced primary key. Production strict insert and upsert route through the
sealed, exact-`id`, filter-bearing MergeInsert adapter:

- strict insert probes the pinned parent and returns `KeyConflict` for an
  existing ID;
- upsert updates or inserts without changing modes on retry;
- a bare Lance Append is not a production graph-table write;
- one table's keyed input is bounded to 8,192 rows and 32 MiB before any
  effect.

An insertion-only transaction may carry the internal
`omnigraph.insert_absence = "v1"` certificate after its absence and physical
shape are proven. Branch merge accepts the shortcut only across a complete,
contiguous, structurally valid history. The certificate is an optimization
capability, not an authenticity mechanism; unfamiliar or cleaned history falls
back to the general merge.

Full-text builds stage an artifact-scoped analyzer certificate before their
CreateIndex metadata is published. The explicit rebuild uses this same writer,
including first-touch branch ownership; it does not rewrite retained snapshots
or add a separate migration publisher. See
[full-text compatibility](lance.md#full-text-compatibility).

## First-touch tables and lazy branches

A named graph branch can read an exact table version owned by another branch.
Its first write stages against that inherited snapshot and prepares a fresh
native name, `fork.{owner incarnation ULID}.m{base manifest version}.{graph commit ULID}`.
A legacy owner without an incarnation uses `legacy` in that position.
The name has at most 80 ASCII bytes, independent of the logical branch name;
the existing unique commit ID separates attempts across legacy owners.
The base manifest version and graph commit ID already belong to the captured
attempt. Name construction adds no storage request, version reservation, or
rename. The fork is created from the captured source ref and version without
any intent record; a fork no manifest entry references is garbage that
cleanup classifies.

`TableVersionMetadata.table_fork_owner` records ownership in the existing
manifest metadata. Existing owned writes retain the actual physical ref;
pointer adoption preserves the source owner. Missing owner metadata uses only
exact native-ref equality for legacy ownership. Parsing a name cannot establish
ownership, because older legal names can resemble the new spelling.

First-touch writes and merges leave old forks alone. A new attempt gets a new
commit ID and therefore a new fork name. Correctness gates, exact effect
identity, and baseline checks remain in place. The name's
base version is preparation context; the successful manifest publication orders
the new registration within that graph branch.

Explicit `cleanup` protects every live table endpoint, pending pin chains,
tags, and native ancestry before reclaiming unused forks. Branch deletion starts no
background table reclamation. Branch deletion records retirement metadata on
its exact native `__manifest` ref, so descendants keep their physical parent
history while the logical name becomes unavailable. Cleanup reclaims unused
retired leaves after proving their dependencies. Cached write admission checks
the ref's identifier and retirement metadata through the existing lookup, with
no additional storage request. Cold branch enumeration includes retained refs
and filters retirement metadata; explicit cleanup reclaims unneeded refs.

Stable table/incarnation identity, not `table_key`, determines whether a
registration, rename, tombstone, or pointer belongs to the same lifetime.

## External Blob inputs

Blob URI admission is part of preparation. The graph's
`ExternalBlobPolicy` defaults to deny; served graphs retain only server-safe
bases. The adapter normalizes and coalesces authorized sources, bounds selected
reference count and URI metadata, probes each source once, and charges selected
payload ranges before reading bytes or staging any effect.

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
| Any writer fails before publication, after any detached effect | Typed error; no graph movement; unproven detached staging is retained |
| Any writer fails after publication, before promotion | Acknowledged; the pin stays pending, readable through its staged version, and the next writer of the table or cleanup promotes it |
| Schema apply or the system-column upgrade fails after publication, before its contract is installed | `RecoveryRequired` naming the published commit; the next read-write open, `refresh`, or that handle's next write installs the staged contract |
| A foreign linear commit occupies a pin's target version | The pin is blocked: content writers keep writing behind it, graph-global writers refuse the table, `repair` reports `blocked_promotion` |
| A sidecar from a build that predates detached commits is present | A read-write open and the storage upgrade refuse until that build has resolved it |

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
- stage every table effect as a detached commit whose replay conflicts with
  its own twin (see the surface guards), publish pins, and promote held
  handles after the CAS;
- add its windows to `detached_commit_matrix.rs` and its crash cells to
  `failpoints.rs`;
- join the durable-call/source guards in `forbidden_apis.rs`;
- prove bounds and crash windows at the owning layer.

Design rationale and rejected alternatives live in
[RFC 0067](../rfcs/0067-detached-table-commits.md),
[RFC 0022](../rfcs/0022-unified-write-path.md),
[RFC 0023](../rfcs/0023-key-conflict-fencing.md), and
[RFC 0028](../rfcs/0028-stable-schema-identity.md).
