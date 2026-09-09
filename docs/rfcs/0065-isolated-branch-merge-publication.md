---
rfc: "0065"
title: "Isolated branch merge publication"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-09-09
updated: 2026-09-10
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Storage: qualify bounded owner ancestry and metadata, shared files, indexes and Blob retention on pinned Lance 11."
  - "Engine: prove adopted-owner writes, maintenance and change-feed lifetime witnesses on main and named branches."
  - "Recovery: prove publication-versus-abandonment ordering and crash-safe admission independent of terminal GC backlog."
  - "Compatibility: specify the format fence and lossless supported upgrade path before enabling publication."
---

# RFC 0065: Isolated branch merge publication

> Number provisional: recheck the registry and open reservations before publication.

## Summary

Prepare merge results on ***private Lance branches***, internal table branches
that no accepted graph snapshot selects before the merge finishes. Keep the
target's existing table versions unchanged during preparation. Make the entire
result visible through one ***publication***, an atomic conditional update of
the target branch's `__manifest` containing all table references and merge lineage.
An attempt proven unable to publish becomes work for
***garbage collection (GC)***, later reclamation of unreferenced storage. Returning
that failure does not wait for physical deletion.

This changes merge storage ownership and the behavior of subsequent writers.
It does not remove recovery for uncertain publication, promise cheaper successful
merges, or introduce distributed writer fencing. Native private branches are
the starting proposal, conditional on bounded ancestry and owner metadata under
repeated writes and retention. Detached commits are a separate alternative.
Acceptance depends on the storage, writer, recovery and compatibility gates below.

## Motivation

This proposal could address [issue #694: failed merges blocking later writes](https://github.com/ModernRelay/omnigraph/issues/694)
by keeping failed preparation off accepted target tables.
The current merge commits its physical table effects before the final graph
publication. Recovery must distinguish no effect, owned partial effects,
confirmed effects and foreign movement before allowing more writes.

Returned-error cleanup can resolve some failures using the existing protocol.
This proposal addresses a different boundary: failed preparation should leave
only private output, without requiring Restore of a table still used by the target.
That requires more than a change to the merge error handler. Writers, readers,
retention and recovery must agree on the physical owners selected by the graph.

## User and operational behavior

The existing branch-merge API and row-conflict rules remain. Internal Lance
branches do not appear in the public graph-branch list and are not authorization
boundaries. Policy applies to the source and target graph branches as before.

| Outcome | Observable result |
|---|---|
| Preparation or validation fails, with proof the attempt cannot publish | Original error; target graph head and target table heads unchanged; no restart required solely to reclaim the private output. |
| Target changed before publication | Conflict or an existing bounded fresh-attempt retry; no overwrite of the winning graph state. |
| Publication succeeds | One complete merge result; existing snapshot readers retain their previous versions. |
| Publication response is uncertain | Resolve the fixed operation identity; preserve `RecoveryRequired` for the affected authority if the result cannot be established. |
| GC is delayed or fails | Unused storage remains; an already-abandoned private attempt does not block graph writes. |

A proven abandoned attempt is nonblocking even if reclamation fails. If its
durable disposition cannot be established, preserve ownership and return the
existing recovery/refusal outcome.

Cancellation has the preparation-failure outcome only while there is proof that
no publication was submitted and the attempt cannot resume. If that proof is lost,
including after a pending-marker write followed by a crash before submission,
follow the uncertain-publication path. Cancellation is not permission to delete
the output.

Active work may still hold the existing write gates. This RFC removes the need
to compensate live target tables after private preparation fails; it does not
promise lock-free preparation or independent-process mutation concurrency.
Existing explicit cleanup invokes GC; no new background service is required.

## Design

> A term set in bold italics is being defined at that exact spot.

### Definitions

- **Private Lance branch:** an internal table branch dedicated to one attempt,
  initially absent from every accepted graph snapshot.
- **Publication:** the target branch's atomic `__manifest` update selecting the
  complete result and recording its graph commit.
- **GC:** reclamation of storage no accepted or retained state needs.
- **Physical owner:** the dataset path and native Lance branch containing a
  table version. A logical graph branch may select several physical owners.
- **Attempt identity:** a unique operation identifier fixed before durable work;
  neither its storage names nor its intended graph-commit identifier are reused.

### Authority and representation

The target branch's `__manifest` remains the sole authority for visible graph
content and lineage. `DatasetEntry` selects each accepted table's stable identity,
dataset path, native branch and exact version. A ***physical owner*** is that
path and native branch; its numeric versions are comparable only within that
owner's lineage. Publication is not a per-table Restore onto canonical main.

Give each merge an ***attempt identity***, a unique operation identifier fixed
before durable work. Each changed table receives a fresh native branch derived
from this identity and the stable table identity. Record the actual native branch
identifier returned by Lance. Unchanged tables keep their exact accepted entries.
New logical types and mixed-schema merges remain subject to the existing schema
contract; this RFC does not make them implicitly legal.

The ***RecoverySidecar***, the existing durable record of unfinished engine work,
gets an explicitly versioned payload for isolated merges. It contains the attempt
and intended graph-commit identities, source/target authority, source dependencies,
private owners, validated final entries and publication disposition. It describes
ownership; it is not a second graph head, custom WAL or separately maintained queue.
Legacy BranchMerge payloads retain their existing recovery semantics.

### Prepare and publish

1. Capture immutable source and target snapshots, table/schema identities and
   target authority. Protect their storage dependencies under the existing
   schema, branch and table gate ordering.
2. Persist the isolated-attempt ownership record before creating private refs.
   Build every changed table result on its private owner. Reads may use captured
   source data; writes must not advance any previously accepted owner's HEAD.
3. Validate the complete graph result, including keys, edges, required fields,
   cardinalities and schema identity. Finish all planned table effects and record
   their exact versions, native identities and retained dependencies.
4. Revalidate the target authority. Persist the publication-pending disposition
   before submitting the one target publication. Use the existing
   ***compare-and-swap (CAS)***, a conditional update that succeeds only against
   the expected accepted state. A conflict cannot transparently publish a stale
   merge result. Preserve the captured source parent and intended graph-commit id.
5. Confirm that exact publication, refresh derived state and return the normal
   merge receipt. Its data is now owned through accepted graph references.
   Retiring an attempt record or temporary label must not delete published data.

No second invocation of ordinary `branch_merge` promotes the result. The final
operation selects the already validated table versions together.

### Publication, abandonment and cancellation

The payload distinguishes `Preparing`, `PublicationPending` and `Abandoned`.
A committed graph result, identified by the intended graph-commit id and complete
entries, is authoritative evidence of publication regardless of payload phase.
A payload saying "published" alone can never establish success.

A preparing attempt can become abandoned only after its execution has ended and
there is proof that no publication was submitted. Classification happens under
the same ordered gates as publication and GC. Phase changes must be monotonic and reject stale writes;
use the existing storage conditional-update contract. Publication may be submitted
only after `PublicationPending` is durably acknowledged. Abandonment conditionally
replaces the observed `Preparing` revision. Losing or ambiguous phase transitions
require a reread and never authorize deletion. Do not infer abandonment from an
expired timer, absent client or an error string.

A publication-pending attempt cannot be abandoned because a read did not find
its result. The request may still complete. Keep its output and identity until
recovery proves the exact publication or proves it cannot land. Cancellation or
shutdown during this window retains the existing supported recovery/refusal
boundary. A conclusively rejected publication CAS can become `Abandoned` only
after all submissions and retries for that attempt have ended. No new takeover
lease or metadata transaction manager is introduced.

A pending-marker write whose acknowledgement is lost, and an acknowledged marker
followed by a crash before CAS submission, can both leave `PublicationPending`
without durable evidence that no publication was submitted. Reopen must treat
these as uncertain even when no graph result is visible. A live caller's proof
of non-submission must not be assumed to survive the crash.

Before submission, a dropped writer cannot submit graph publication later.
Already-transmitted writes to private storage may complete later; they must
remain confined to never-reused private paths. This protects graph content,
but does not prove all private garbage has stopped arriving.

### Subsequent writes and history

Every writer opening an adopted table must resolve the physical owner from its
accepted `DatasetEntry`, including writes to main. Never choose a native target
solely from the logical branch name or substitute a newer physical HEAD.

Continuing writes must preserve the accepted version and every retained
dependency. Their physical representation must bound ancestry depth and per-write
owner metadata independently of the total number of completed writes, for a
fixed-size table and fixed retention policy. Recursively creating a native
successor on every write is not an acceptable implementation: its ancestry can
keep growing after graph history expires. Before acceptance, qualify a supported
strategy that meets this bound or reject the representation. This requirement
applies to mutation, load, schema rewrite, index work and optimize, not only to
merges. Their durable effects remain covered by their writer protocols; any
in-place continuation requires an explicit ownership proof.

Graph commits retain exact physical locators. Change feeds, merge-base discovery
and snapshot reads must interpret version numbers in their physical lineage.
When a physical transaction interval is unavailable, use an existing exact
fallback or refuse the unsupported operation; never infer an empty delta. This
allowance cannot refuse ordinary retained history created by the enabled writer.

Change feeds must prove the lifetime of each opened physical owner against its
accepted graph publication. A private native ref does not name a public graph
branch. Replace the logical-branch inference with a native-owner lifetime witness
that binds the exact owner identity and version to that publication. The witness
must detect replacement during table opens, including on stores without e_tags.
After the final witness, enumeration must use captured handles and evidence,
without rereading replaceable numeric-path history. Qualify both the pre-table-open
and post-head-witness failure windows through repeated owner changes and source
graph-branch deletion. The witness encoding is an acceptance decision below.
A shallow fork's inherited data, deletion files, indexes and Blob payloads must
remain readable through source deletion, later writes and cleanup.

### GC and nonblocking abandoned work

Extend existing recovery discovery and explicit cleanup with these dispositions:

| Disposition | Required treatment |
|---|---|
| Active preparation | Protect private outputs and all captured input dependencies. |
| Publication pending | Protect outputs and publication evidence until the outcome is resolved. |
| Published output | Protect through current and retained historical graph references and their transitive storage dependencies. |
| Abandoned output | Reclaim only after excluding publication and all accepted/historical references; failures are retryable GC work. |

The write-entry barrier must recognize an isolated `Abandoned` record as
nonblocking. Cleanup must be able to process it. The current rule that refuses
cleanup whenever any recovery sidecar exists cannot remain unchanged for these
records. Finding active or pending recovery on write entry must not enumerate
the terminal GC backlog. Define crash-safe discovery that preserves one durable
ownership authority while leaving terminal ownership discoverable to GC. An
in-memory filter after listing every record does not meet this requirement.
Unknown payloads and legacy unresolved sidecars must still cause the existing
write refusal; a bounded scan cannot silently omit an unclassified record.
The exact discovery encoding and its crash transitions must be settled before
acceptance, without introducing a separate queue or graph authority.

Re-read ownership under the GC gates immediately before destructive actions.
Use native Lance ref and cleanup APIs; do not recursively delete a table path
because a logical branch disappeared. Remove a native ref only after proving
that current graph branches, retained snapshots, descendants and tags do not
need it. A retained graph `DatasetEntry` is not itself a native Lance GC root.
Protect every retained exact owner/version through native references or a
qualified cleanup exclusion before collection. Native refusal preserves the
work for a later cleanup.

GC does not run synchronously on the merge error path. An abandoned record can
be removed only after private I/O is quiescent and reclamation is complete.
If completion cannot be proved, retain discoverable ownership and rescan on later
cleanup; late private writes may recreate garbage. Age alone is not proof.
Records and bytes can accumulate during prolonged failure. Each cleanup invocation
must bound discovery and deletion work without dropping ownership evidence.
Repeated invocations must reach later eligible records even when earlier records
remain uncollectable; an unchanged blocked prefix cannot starve the backlog.

## Invariants

This proposal preserves architectural invariants 2 and 4 by publishing all graph
content and lineage once. Invariants 3 and 6 require captured authority and stable
table identities across physical-owner changes. Invariant 5 still requires durable
ownership and explicit ambiguous outcomes. Invariants 1, 7 and 12 require native
storage lifecycle, derived indexes and one graph-content authority.

Invariant 11 requires ordinary write admission and owner metadata work to scale
with current work and supported retention, not accumulated writes or terminal GC
records. The boundedness qualifications above are acceptance conditions.

The compatibility and failure evidence required by invariant 13 is an acceptance
gate. The deny-list's custom storage protocol and distributed-fencing boundaries
are unchanged: extending engine ownership does not replace Lance's commits or
make process-local gates fence a second mutation process.

## Compatibility and reversibility

Selecting arbitrary private owners changes assumptions even if the existing
`DatasetEntry` fields can encode them. Older writers and GC must refuse such a
graph before performing any mutation or reclamation. A runtime flag is not a
persistent compatibility fence.

Before enabling the first isolated publication, define and test a new internal
format/capability boundary with reader, writer, cleanup and downgrade refusal.
Use [RFC 0064](0064-explicit-storage-upgrades.md)'s explicit upgrade framework.
The supported conversion must preserve current rows, branch topology, retained
snapshots and graph lineage, or explicitly refuse the graph. No silent flattening.
Exact encoding and version allocation are settled with compatibility evidence.

After isolated owners become authoritative, disabling this writer does not make
old binaries safe. Reversion requires an explicit supported conversion; otherwise
refuse. Legacy recovery records must be resolved or retained with their old
meaning before conversion. No mixed-format writer fleet is supported initially.

## Alternatives

| Alternative | Assessment |
|---|---|
| Keep the current protocol with returned-error cleanup | Retains compensation and does not isolate cancelled preparation. |
| Stage uncommitted files without private owners | Preferable where one atomic native effect suffices; insufficient for a merge's durable multi-commit table chain without touching the target. |
| Private branches without an ownership record | A crash after fork creation leaves GC unable to distinguish still-needed source dependencies from abandoned work. Native ref identity alone does not establish publication intent. |
| Remove ownership immediately on failure | A late private write can recreate storage after collection. Keep ownership until quiescence, or preserve a discoverable rescan path. |
| Native branches, as proposed | Reuses native version/ref traversal; requires a supported strategy with bounded ancestry and per-write metadata, adopted-owner routing and transitive retention. Reject a recursive successor chain if no such strategy qualifies. |
| Fresh per-attempt dataset paths | May preserve more main-write routing by publishing a new ordinary dataset root. Must meet the same ancestry, metadata and retention bounds; fresh names alone do not prove shared-file independence. Qualify path/base references, stable table identity, history, GC and subsequent writes alongside native branches. |
| Detached commits | Avoid native branch refs, but pinned Lance 11 needs additional continuation and retention integration. Not selected as a shortcut. |
| Restore each private result onto live target tables | Reintroduces the prepublication target changes and compensation this proposal seeks to remove. |
| Scan all sidecars and ignore terminal dispositions | Retains one ownership record but charges every write for accumulated garbage. Active discovery must avoid that enumeration while preserving unknown-record refusal and crash-safe ownership. |
| New transaction log or background queue | No separate authority is needed: extend existing recovery ownership and derive reclaimable work. |

The nearest engine precedents are exact version-pinned `DatasetEntry` reads,
incarnation-specific native refs, recovery sidecars and one graph publication.
[Ref incarnations](0042-incarnation-suffixed-branch-refs.md) protect against name
reuse but do not by themselves establish arbitrary physical-owner lifetime.
[Unified writes](0022-unified-write-path.md) remain the publication framework;
[durable recovery authority](0034-durable-recovery-authority.md#103-detached-commits)
explicitly defers detached publication.

External precedents establish the pattern, not Lance compatibility:
[lakeFS transactions](https://pydocs-lakefs.lakefs.io/lakefs.branch.html#lakefs.branch.Branch.transact)
use an ephemeral branch and final merge;
[Nessie cross-table transactions](https://projectnessie.org/guides/transactions/#cross-table-transactions)
expose several table commits together;
[Iceberg audit branches](https://iceberg.apache.org/docs/latest/branching/#audit-branch)
validate and advance one table's main reference.

## Evidence and tests

Source survey: OmniGraph `fe8ae062`, pinned Lance `11.0.0`. This RFC contains a
proposal and source inspection, not an implementation or performance result.

- `exec/merge.rs` applies table effects before `commit_updates_on_branch_with_expected`.
- `db/omnigraph/table_ops.rs::open_for_mutation_on_branch` routes main writes to
  native main and named writes toward the logical branch's canonical native ref.
- `db/omnigraph/optimize.rs::cleanup_all_datasets` refuses pending sidecars before GC.
- `db/manifest/recovery.rs::list_sidecars` lists, sorts and parses all sidecar records;
  retaining terminal records there would leave write-entry discovery dependent on
  the garbage backlog.
- `changes/enumerate.rs::reprove_named_branch_heads` derives a logical branch from
  each native ref and reproves its graph head after table opens. Private owners
  require a replacement lifetime witness.
- Lance `dataset/refs.rs::BranchIdentifier::new` copies the parent version mapping
  and appends an entry. Native branch deletion refuses ancestors needed by
  descendants, including with force; recursive successors do not bound ancestry.
- Lance `dataset/write/commit.rs::with_detached` supports detached commits only for
  existing datasets with V2 manifest naming. This is separate from data-file encoding.
- Lance `io/commit.rs::commit_detached_transaction` assigns random high-bit versions;
  ordinary commit sequencing does not continue them as normal monotonic history.
- Lance cleanup scans ordinary manifest locations; detached filenames are excluded
  by version parsing. A graph reference to a detached version is not sufficient
  native GC retention evidence. This requires a runtime probe before any alternative
  relies on it.

Implementation must qualify the public native APIs with tests, including
inherited index and Blob references.

| Gate and existing owner | Required evidence before activation |
|---|---|
| `lance_surface_guards.rs` | Private fork, cold reopen, shared indexes/Blob reads and GC preserve exact accepted rows and versions. Run many updates to a fixed-size table under fixed retention and cleanup; bound ancestry depth, retained owner metadata and metadata work per write independently of total write count. Compare the fresh-dataset alternative under the same workload. |
| GQT corpus and ordinary merge owners | Main/named targets, fast-forward, divergent merges, conflicts, inserts/updates/deletes, edge-only and net-zero results; same-instance next write succeeds after prepublication failure. |
| `failpoints.rs`, `recovery.rs` | Fail/cancel/drop/crash before and after ownership, each private effect, pending marker, CAS, response and ownership retirement. Cover a lost marker acknowledgement and acknowledged marker followed by crash before CAS; neither absence nor cancellation proves abandonment. No target table head moves during failed preparation. |
| Recovery and concurrency owners | Publish/abort and publish/GC races have one classified outcome; lost acknowledgement never becomes deletion; late private writes never alter accepted state. With increasing terminal backlog and fixed mutation work, write admission must avoid terminal enumeration. Inject discovery-transition crashes, unknown and legacy records, and uncollectable prefixes; preserve ownership/refusal and reach later eligible GC work. |
| `writes.rs`, `schema_apply.rs`, `maintenance.rs` | Mutation, load, rewrite, index creation, optimize and another merge all work after adoption on main and named branches. |
| `point_in_time.rs`, `branching.rs`, `changes.rs`, lineage owners | Retained snapshots, descendants, source deletion, ordinary change feeds and merge bases remain correct through repeated publication and GC. Test owner replacement at pre-table-open and post-head-witness windows with e_tag-less storage; prove the accepted owner lifetime without a fabricated public branch or skipped witness. |
| CLI cross-version owners | Old binaries refuse new state; explicit upgrade preserves every supported retained object; unsupported conversions refuse before changing authority. |
| `merge_cost.rs`, `branch_control_cost.rs`, maintenance cost owners | Record baseline/private requests, bytes, allocations, gate time and abandoned-storage growth for small success, large success, conflict and failure. No performance improvement is claimed without measurements. |

Logical rows/errors belong in GQT; Rust mechanism tests inspect ownership, native
heads, I/O and concurrency that the format cannot express. Required release gates must
exercise configured file/S3/Azure backends rather than count skipped tests as proof.

## Rollout

1. Qualify native branches versus fresh dataset paths with public Lance APIs,
   following the same repeated fixed-size writes, retention and GC workload.
   Reject unbounded ancestry or per-write metadata. Record the representation
   decision and its actual costs before acceptance.
2. Settle the deciding roles' required evidence in Unresolved questions before
   acceptance: compatibility fencing, ownership encoding, lifetime witnesses,
   publication ambiguity and discovery independent of terminal backlog. Enable no
   private graph publication yet.
3. Implement all adopted-owner read/write/maintenance paths and the gated merge
   protocol. Activation waits for every correctness and compatibility row above;
   do not ship a merge-only path whose next mutation or GC cannot consume it.
4. Enable the supported format through explicit upgrade and monitor private owner
   counts, bytes awaiting GC, ambiguous outcomes and merge latency. Other writers
   retain their own failure protocols unless a separate decision changes them.

## Unresolved questions

The following are proposed deciding roles, not recorded approvals. Each decision
must close with the specified evidence and a corresponding Design update before
acceptance.

| Decision | Deciding role | Closure evidence |
|---|---|---|
| Native branches or fresh per-attempt dataset paths | Storage maintainer | Compare repeated fixed-size writes under fixed retention and cleanup, including indexes/Blob retention. Demonstrate bounded ancestry and owner metadata and record lifetime costs; reject a representation lacking a supported bounded strategy. |
| Isolated-sidecar encoding, physical-owner lifetime witness and format/capability fence | Engine and storage maintainers | Specify exact durable identities and crash transitions. Prove change-feed replacement detection on e_tag-less storage and old-binary refusal before effects. |
| Supported retained-history conversion | Storage maintainer | Cross-version evidence preserving each supported current and retained object without silent rebuilding or flattening; document explicit refusal for unsupported conversion. |
| Abandonment-record removal after cancellation | Recovery maintainer | Demonstrate task/I/O quiescence and completed reclamation, or specify retained ownership, rescan behavior and operational limits with late-write tests. Distinguish provable non-submission from lost evidence. |
| Active recovery discovery independent of terminal garbage | Recovery and storage maintainers | Specify one durable ownership authority and crash-safe discovery transitions. Increasing-backlog tests must bound write admission, preserve unknown/legacy refusal, and demonstrate GC progress past uncollectable records. |

## Decision log

No acceptance decision has been recorded.
