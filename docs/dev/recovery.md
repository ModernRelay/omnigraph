# Graph recovery

**Audience:** engine and storage contributors
**Authority:** current recovery model; serialized structs and classifiers in
`crates/omnigraph/src/db/manifest/recovery.rs` are the exact wire authority

Recovery closes the interval between a durable Lance effect and the manifest
publication that makes it graph-visible. It is part of every graph writer's
commit protocol, not an offline repair convenience.

## Persisted authority

Active writers emit identity-aware recovery sidecar schema **v9**. Manifest
schema and recovery schema are independent version spaces; the current
manifest is v8.

Every owned table slot carries:

- non-zero stable table and incarnation identity;
- diagnostic alias and physical dataset URI;
- graph and physical branch identity, including the intended table-fork owner;
- expected manifest-visible Lance version;
- the planned transaction or bounded maintenance outcome;
- fixed manifest delta and graph lineage where applicable.

The retained JSON field names `protocol_v3`, `protocol_v4`, `protocol_v7`,
and `protocol_v8` identify established writer payload shapes. They do **not**
mean an active sidecar uses an old outer schema.

| Sidecar kind | Current v9 payload |
|---|---|
| Mutation / Load | Exact one-transaction effect identity and confirmation |
| BranchMerge | Fixed bounded transaction chain, source/target authority, and lineage |
| SchemaApply | Exact existing/first-touch effects, durable schema staging, and complete catalog delta |
| EnsureIndices / full-text rebuild | Exact CreateIndex effects and complete pointer delta; rebuild retains the same fixed actor and lineage |
| Optimize | Bounded maintenance plan and complete graph-wide pointer outcome |

Pre-v9 identity-less artifacts are never upgraded by guessing from aliases.
Unsupported future schemas are refused before their payload is interpreted.

## Sidecar lifecycle

1. The writer completes pre-effect validation and stages every participant.
2. Under schema → branch → sorted-table gates, it revalidates the complete
   authority and persists the sidecar.
3. It commits participant effects and durably confirms what was achieved.
4. It publishes the fixed manifest outcome.
5. It appends the recovery audit and removes the sidecar.

A crash may interrupt any step after 2. Re-running classification must be
idempotent: an already-published outcome is success, an owned unpublished
outcome converges once, and cleanup can be retried.

## Classification

Recovery compares each sidecar slot with both manifest authority and the actual
Lance transaction/version history. The useful states are:

- **No effect:** the participant remains at its expected baseline.
- **Exact owned effect:** the observed transaction identity and achieved
  version match the sidecar.
- **Confirmed owned chain:** the complete bounded merge chain reached its fixed
  confirmed version.
- **Owned partial effect:** only a prefix/subset of the fixed plan landed.
- **Already published:** manifest state contains the fixed outcome.
- **Foreign or ambiguous movement:** the sidecar cannot prove ownership.

Only the first five may be finalized. Foreign, missing, malformed, or
history-buried evidence fails closed; recovery never adopts a plausible version
because the alias and number happen to match.

## Two recovery modes

### Full

A read-write open runs the full sweep before returning the graph handle. With
the graph quiescent, Full recovery may:

- roll a complete owned effect set forward;
- restore/compensate an owned partial set to the pinned graph state;
- retire recovery ownership of a proven unpublished private first-touch fork,
  leaving its storage for explicit cleanup;
- promote or discard owned schema staging;
- refuse an invariant violation or ambiguous effect.

Lance Restore can defeat a concurrent writer, so ordinary in-process healing
must not run a destructive Full sweep.

A merge that returns an error before durable effect confirmation resolves only
its own BranchMerge sidecar while retaining its schema, branch and table gates.
It re-reads the durable record and reuses the exact Full classifier to retire
an effect-free attempt or compensate owned unconfirmed effects. The original
merge error is returned after cleanup; failed or deferred cleanup retains
`RecoveryRequired`. Confirmed effects remain on the ordinary roll-forward path.
This scoped error cleanup does not handle a cancelled future and does not add
cross-process fencing or prove the completion of an already-transmitted remote
write after an ambiguous I/O failure; the existing recovery support boundary
still applies.

### RollForwardOnly

Long-lived handles and write-entry barriers use the concurrency-safe
roll-forward-only sweep. It takes the same ordered gates, re-reads the artifact
under those gates, and may publish a complete confirmed outcome with the
manifest CAS. It may also retire a provably effect-free Armed mutation/load
intent whose exact transaction-identity classification proves no owned effect,
under the same one-mutation-process boundary destructive full-recovery
decisions assume (see invariants.md, current support boundaries). Anything
requiring Restore, destructive compensation, or an unproven decision remains on
disk for the next Full open and blocks only the authority it affects.

This split lets the common “all table commits landed; final manifest publish
failed” case heal without a restart while preserving concurrent writers.

## Ordering and visibility

Recovery uses the same gate order as writers. It never treats a warm
coordinator or cache as current authority. Successful recovery invalidates
derived handles before later operations continue.

Roll-forward publishes the sidecar's pre-minted lineage and complete manifest
delta; it does not create a new semantic commit. Compensation restores the
previous accepted graph view and never acknowledges the failed operation.

## Initialization ownership

Fresh-graph initialization uses a separate root-scoped
`__init_claim.json`; it is not a recovery-v9 sidecar. Strict and `force` init
both acquire it with create-if-absent and repeat target preflight while holding
the claim. `force` may replace orphan schema artifacts only when no graph
manifest exists; it never rebinds an existing graph or purges data datasets.

A failure proven to precede physical initialization may clean up schema files
owned by that claim. Once a Lance dataset Create may have started, the result is
acknowledgement-unknown and OmniGraph probes the exact attempt-local genesis:

- an exact committed genesis resumes final validation;
- a later validation failure returns `InitializationCommitted` and preserves
  the committed graph;
- an unavailable or mismatched proof returns `InitializationIndeterminate` and
  preserves the schema artifacts and claim.

Do not retry initialization or remove an indeterminate claim until every
initializer for that root is quiescent and the root has been inspected. The
claim prevents a concurrent force attempt from overwriting another attempt's
schema contract or racing delayed cleanup.

## Graph branch controls

Native branch create/delete residue is different from a data-table effect.
An unreferenced clone-only tree is reclaimable only when no physical
`BranchContents` exists, including a logically retired native ref. A sidecar owning a real graph-table effect may
not be discarded merely because its target branch was deleted; the complete
effect/compensation proof still applies. Each first-touch table effect names
its prepared unique native ref. Recovery never substitutes a newly generated
name or a recreated logical branch. Physical pin owners and confirmed table
metadata preserve the captured owner through roll-forward and rollback.

Private unreachable forks may remain after recovery retires their sidecars.
Explicit cleanup still proves that live table pins, recovery, tags, and Lance
ancestry no longer require them before deletion. An old owner's absence from
the logical branch list alone is not proof.

Graph deletion writes the reserved `omnigraph.retired_manifest_branch`
metadata value through Lance's public `Branches::replace_metadata`. That
single native-ref update removes logical authority while preserving the exact
physical ref and unrelated metadata. The versioned marker binds the native
name and identifier; unknown fields, versions, or mismatched identity fail
closed. A lost acknowledgement is classified by reading the same ref and
validating its retirement marker. Absence is not proof of completed retirement.
Native history remains readable for descendants, while branch-name reads and
writes require an unretired ref. The existing process-local branch controls
serialize this read/replace protocol; they are not distributed fencing.
Explicit cleanup alone reclaims retired lifetimes after proving their native
descendants, paths, tags and current table references no longer need them.
The v8 storage fence keeps older binaries from exposing retired branches.

## Maintenance boundary

Mutation, Load, SchemaApply, BranchMerge, and EnsureIndices carry exact
transaction identities. Optimize uses Lance maintenance operations that do not
yet expose the same caller-owned transaction proof, so its classifier is
bounded but looser and retains the documented one-mutation-process boundary for
destructive recovery. Do not widen that claim to distributed takeover without
a new proof and compatibility tests.

## Test ownership

- `crates/omnigraph/tests/recovery.rs` owns serialized grammar and core
  classification.
- `crates/omnigraph/tests/failpoints.rs` owns writer crash windows.
- The initialization cells in `failpoints.rs` own exact-genesis recovery,
  committed-versus-indeterminate outcomes, and claim retention.
- `crates/omnigraph/src/db/manifest/recovery.rs` owns classifier truth tables.
- `crates/omnigraph/tests/forbidden_apis.rs` guards durable-call and writer
  registration.
- Cluster recovery has a separate control-plane protocol described in
  [control-plane.md](control-plane.md); it never substitutes for graph recovery.

The design rationale is [RFC 0022](../rfcs/0022-unified-write-path.md).
