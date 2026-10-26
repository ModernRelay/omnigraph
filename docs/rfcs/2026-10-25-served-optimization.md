---
rfc: "2026-10-25-served-optimization"
title: "Served optimization"
track: maintainer
status: draft
implementation: in-progress
authors:
  - aaltshuler
created: 2026-10-25
updated: 2026-10-26
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Native qualification of resource bounds, retained data and shutdown settlement before deployment."
---

# RFC: Served optimization

## Problem and scope

`optimize` currently opens graph storage directly. A running cluster already
owns that storage, so an independent process cannot safely run this command
beside it. Expose the existing optimization through that owning server, retain
an exact operation identity, and let a disconnected client inspect its result.

The first implementation operates on one graph's `main`. It can block writes
while holding existing native gates. Reads keep their admitted snapshots.
It does not introduce automatic scheduling, independent task workers, cleanup,
repair, history deletion or a second graph publication path.

## Authority and execution

Add the explicit graph policy action `optimize`, scoped to target branch
`main`. `admin` stays reserved; configuration management and graph membership
do not imply optimization permission. HTTP authenticates the actor. Native
cluster acceptance checks current applied policy; the engine entry point
checks the same action before work. No bearer bytes enter durable records.

The server uses its existing cluster admission and owned-operation runtime.
One process gate serializes maintenance with configuration deployment. Native
acceptance checks the exact cluster owner and refuses outstanding deployment
work. Every accepted optimization remains owned after its HTTP waiter exits.
Shutdown uses the existing owned-operation and storage-settlement rules.

The engine retains the existing schema, branch and sorted-table gates, exact
accepted snapshot, detached staging and single graph-manifest publication.
Its report captures the before/after graph commit while those gates remain
held, plus exact table statistics, skips and deferred indexes. A report must
not substitute a later head read for this operation's publication.

## Durable invocation and lookup

Store a separate bounded native invocation journal at
`__cluster/maintenance.json`. This is an operation record, not graph authority
or another ownership lock. Storage uses the admitted root's existing adapter,
conditional writes and I/O scope. It never modifies deployment outcomes.

An operation ID is `<cluster-ledger-id>:<sequence>:<nonce>`; the ledger ID and
nonce are canonical ULIDs and the sequence is a canonical positive integer.
Maintenance has its own increasing sequence. Read-only status exposes the
next sequence but reserves nothing. Acceptance consumes one sequence and
persists the exact graph, kind, options, budget and authenticated actor before
any effect. It returns a single-use in-process execution token. No caller can
construct an accepted token from the serialized journal.

After the engine completes source preflight under its graph gates, it
conditionally binds the exact input commit and source inventory to that
invocation before any physical rewrite. Success must match this binding.
A failed binding write retains uncertainty and never permits execution.
The invocation also records its applied configuration revision and schema
contract; these identify admission, while the immutable input commit binds
the actual graph schema and table pins used by optimization.

Only one outstanding invocation is allowed per cluster. A duplicate identity
and matching input/actor returns the recorded status without execution.
Different input or actor refuses. A different nonce at a retained sequence
refuses. A consumed sequence whose result was evicted reports expired,
unknown acceptance/outcome; it never becomes executable again. Future
sequences and another ledger are refused.

Completion conditionally records the exact native report or a proven
pre-effect refusal. A refusal consumes its ID and releases the slot; an
uncertain failure keeps it outstanding. Keep at most 32
terminal reports, at most 1 MiB each and 4 MiB combined. The complete journal
is limited to 8 MiB. Eviction only removes terminal result payloads; the
monotonic sequence retains the no-replay boundary. Outstanding work cannot
be evicted. A result larger than its retention bound leaves uncertainty rather
than publishing a truncated success.

Before physical effects, the engine derives a conservative report bound from
the exact accepted table and index strings, all supported pending-index
outcomes and maximal numeric widths. Cluster admission includes the complete
invocation/result wrapper, prepared witness and interrupted-result shapes in
the reservation. An envelope that cannot fit refuses before physical work.
The final exact-size check remains a defensive guard. No report is truncated,
and no input/output size ratio substitutes for this proof.

A crash or storage error between invocation acceptance, graph publication and
result recording leaves an outstanding operation. Lookup labels it unresolved
when no local owner exists. Neither a new process nor another request replays
it or infers success from equal data or a changed head. It preserves its evidence
and refuses new maintenance, deployment and writable process admission. Even
after explicit predecessor exclusion, a successor cannot start ordinary
serving or direct graph operations while the journal is unresolved. The
original process can expose its exact receipt while its admission remains
available. Explicit recovery takes a restricted admission for the original
operation after the administrator establishes predecessor and accepted-I/O
exclusion and handles the exact old lock. It cannot serve requests, deploy
configuration or rerun optimization.

Before publication, retain the exact native publication witness and prepared
report. Recovery compares that witness against native graph authority. No
prepared witness means graph publication was never attempted. A prepared
candidate's absence alone does not exclude a delayed storage write: use the
existing native no-effect settlement publication to resolve that race, then
validate the exact winner. Missing, conflicting or unclassifiable evidence
leaves the original invocation unresolved. A recovered receipt explicitly
reports interrupted unpublished or interrupted published work; publication
does not claim subsequent derived-artifact work completed. Detached output
remains subject to ordinary native cleanup safety. Recovery never deletes it.

Current `optimize` permission authorizes status for the requested graph. The
exact authenticated initiator may retrieve its retained individual receipt
after permission revocation, without gaining graph data or other operations.
Missing or expired ownership evidence cannot grant this exception.

## Bounds and enablement

Served optimization is explicitly enabled at boot; it is off by default.
The optional `OMNIGRAPH_MAINTENANCE_CONFIG` names a local YAML file (at most
4 KiB) containing positive `max_tables`, `max_fragments`, `max_rows`,
`max_source_bytes` and `max_files`. Embedders may provide the same limits
through the server builder. There are no implicit defaults that enable work.
The server chooses finite ceilings for source tables, fragments, rows, bytes,
file references and dataset concurrency. Requests may lower those ceilings, never raise them.
The first implementation uses one dataset at a time. Limits are validated
before durable acceptance and the complete accepted work is checked before
the first physical effect, including dropped-column rewrite tasks.

Source accounting includes `__manifest` and all pinned table manifests, data and deletion
files, index-file inventories and live managed Blob lengths. It conservatively
counts whole accepted tables, including dropped-column rewrites; Blob lengths
may count bytes already inside a data file, and each nonempty managed Blob
counts as a file reference even when values share a packed file. External Blob
payloads are not read by compaction. Missing file sizes use exact object HEADs;
unknown index inventories or physical formats, inherited storage bases, system
indexes, overlays and unsupported Blob layouts refuse before effects. Index
accounting inspects native metadata before reader compatibility filtering;
an unsupported index cannot disappear from the accepted inventory.
No historical prefix is listed.
Unknown sizes or unsupported accounting refuse; a per-dataset concurrency
setting alone is not a whole-operation limit. A bounded input is not a claim
of a hard process memory, CPU or storage-rate cap. Live enablement additionally
requires process/platform resource isolation and measured limits. An elapsed
HTTP deadline never interrupts an uncertain storage write.

This accounting was checked against the pinned Lance 13.0.0 source:
[`CompactionOptions`, planning and task execution](https://docs.rs/crate/lance/13.0.0/source/src/dataset/optimize.rs),
[`Fragment` and `DataFile` metadata](https://docs.rs/crate/lance-table/13.0.0/source/src/format/fragment.rs),
[`IndexMetadata.files`](https://docs.rs/crate/lance-table/13.0.0/source/src/format/index.rs),
the native [`deletion_file_path`](https://docs.rs/crate/lance-table/13.0.0/source/src/io/deletion.rs),
[`Dataset::object_store` and `branch_location`](https://docs.rs/crate/lance/13.0.0/source/src/dataset.rs),
and [`ObjectStore::size`](https://docs.rs/crate/lance-io/13.0.0/source/src/object_store.rs).
Lance's own compaction byte option omits Blob-v2 payloads and can select only
part of the work. It therefore does not substitute for the complete accepted
inventory checked here. These source checks establish the accounting contract,
not a measured process-memory or throughput limit.

Index reconciliation defers an existing vector index only when it has no
non-null vectors. With positive input, native Lance chooses its supported
training layout. Statistics, storage and index builder errors propagate; they
cannot masquerade as insufficient training data.
New declared vector indexes with no non-null rows retain their existing pending
status. Full-text reconciliation keeps its explicit rebuild boundary.

No externally scheduled capacity allocation is defined here. A deployment
requiring a fleet-wide budget must keep this capability disabled until native
admission validates that allocation on every entry point. A proxy-only check
is insufficient. Root-bound signed-token and OIDC identity profiles refuse
new optimization at boot and native request admission while this allocation
contract is absent. Disabling execution retains original-ID lookup.
Independent workers additionally require retained immutable
inputs, serialized bounded plans, output-only credentials and native result
validation/publication; none is implied by this endpoint.

## HTTP and CLI

Use the shared operation registry and existing HTTP version:

- `GET /cluster/maintenance?graph=<id>`: authorized read-only status and next ID
  components for one graph.
- `POST /cluster/maintenance`: exact operation ID, graph, `optimize` kind and
  bounded options; return after durable acceptance or a recorded refusal.
- `GET /cluster/maintenance/{id}?graph=<id>`: exact original result/status
  lookup with the original graph, without scanning other graph policies.

Malformed input, denied policy, disabled capability and exhausted admission
refuse before physical effects. An ambiguous POST response is not a refusal.
Clients retain the original ID and use GET; no automatic POST replay.

These cluster maintenance routes have no MCP binding in this slice. Their
durable invocation and recovery contract is separate from the non-streaming
graph operations exposed as MCP tools. The shared registry records this
explicit transport boundary.

Extend the existing `optimize` command and target resolution. Remote execution
waits by default; `--no-wait` returns the known operation ID.
`--operation-id <id>` observes that original operation without submitting it.
`--timeout` bounds observation only. Direct storage retains its existing
command and output; remote refusal never falls back to direct storage.
For administrator recovery, `optimize --operation-id <id> --writers-stopped`
requires an explicit direct-root target and graph. The attestation means the
prior process and accepted graph/control I/O are excluded, as for deployment
recovery; it never overrides a held lock. This operation reconciles the
original receipt and never executes a new optimization.

## Evidence required

Extend existing maintenance, policy, cluster, server and CLI test owners.
Cover denied and allowed actors, complete bounds before effects, concurrent
reads and write contention, identical/mismatched duplicates, response loss,
result eviction, accepted-operation interruption, shutdown and restart lookup.
Compare logical rows, schema, Blob values, index behavior, retained history and
live branches before and after. Test failures before and after publication
without treating a lost response as permission to execute again.

Local tests establish the candidate's contract. They do not qualify cloud
storage settlement, fleet admission or production resource ceilings.
