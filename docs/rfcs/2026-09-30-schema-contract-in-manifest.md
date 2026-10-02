---
rfc: "2026-09-30-schema-contract-in-manifest"
title: "Schema contract in the manifest"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-30
updated: 2026-09-30
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Obtain the designated human verification of the measured DST cost golden; no such approval is recorded."
  - "Obtain the designated human review of the required durable-call registry classifications; no such approval is recorded."
---

# RFC: Schema contract in the manifest

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

Store the ***schema contract***, the accepted schema source, compiled schema
and their identity, in one live row of `__manifest`, the Lance dataset that
publishes a graph's table references and commit history. Schema apply replaces
that row in the same commit as its table references and graph head. A reader
therefore accepts a complete published contract together with the graph state
that uses it.

This replaces the root schema files and the schema-apply sentinel with the
existing manifest publication boundary. The process-local schema gate stays.
The format change requires an explicit offline conversion; normal open does
not migrate a graph. Historical queries keep their current-contract behavior,
described under User and operational behavior.

## Motivation

The previous protocol publishes table changes first and installs three schema
files afterward. A crash between those steps leaves a published graph whose
contract still needs installation. Open, refresh and write entry must decide
whether staged files belong to a published apply or an unfinished one. A
sentinel branch announces that an apply is in progress, but the process-local
gate cannot fence another process's decisions about those files.

The graph already has a ***compare-and-swap (CAS)***, a publication that succeeds
only against the captured manifest version. Bringing the contract into that
publication removes the separate installation decision. It also removes the
root-file reads and sentinel checks used to coordinate it. This has a byte
cost: the current manifest publisher rewrites its live rows, so the contract
text is copied on ordinary publications too. Design states that tradeoff.

## User and operational behavior

The schema commands and schema language keep their existing interfaces.
Schema apply retains its main-only, single-live-branch restriction and existing
policy checks. A successful apply makes its schema and table changes visible
together. A failure before publication leaves the previous graph and contract
visible. A lost acknowledgement still requires the existing publication-outcome
check; it is not proof that the apply lost.

A live read uses the contract of its resolved manifest version. A historical
query uses the accepted live contract, with table aliases rebound by stable
identity to the selected historical image. For example, after renaming a type,
the current name still addresses its retained data. Retaining an older
`schema_contract` row does not introduce historical schema-language semantics
or a schema-history API. An unrefreshed handle must observe a completed apply
when it next captures the live contract for such a query; that regression is
an explicit evidence gate below.

`omnigraph schema upgrade-system-columns` uses the same atomic contract
publication as schema apply. The schema IR, the compiled representation of the
schema, still determines system-column spelling. Storage conversion does not
rename user data or change its system-column vintage.

Operators use `omnigraph upgrade <root> --check --json`, then execute the same
command without `--check` while all readers, writers and maintenance processes
are stopped and a restorable whole-root backup is retained. The new target and
route are specified under Compatibility and reversibility. Interrupted
conversion is resumed with the upgrade-capable executable; removing a pending
marker by hand is not a recovery procedure. Cluster-managed conversion remains
outside the supported boundary of the existing offline protocol.

## Design

### Definitions and existing authorities

The schema contract consists of its exact source text, serialized SchemaIR and
small identity fields. The graph head identifies the accepted graph commit.
A table registration names the table identity and exact physical version used
by that graph state. A ***detached commit*** is a Lance table version that can
be opened by its identifier without advancing that table's linear history.
These table effects become graph-visible only when a manifest registration
names them.

The design follows the replace-in-place `graph_head` row and existing manifest
publisher. It keeps the ordering of the
[shared schema gate](2026-09-18-shared-schema-gate.md) and the table-version
rules of [Detached-only tables](2026-09-21-detached-only-tables.md). It changes
the contract-installation part of those protocols. It introduces neither a
second catalog dataset nor a new lock or identity namespace.

### Row and validation

The canonical row has `object_id = "schema_contract"` and
`object_type = "schema_contract"`. Its `metadata` contains `schema_ir_hash`,
`schema_identity_version` and `schema_identity_domain`. The exact source and IR
texts occupy nullable `LargeUtf8` columns, `schema_source` and `schema_ir`,
beside the packed `record` column. Both texts are required on this row and null
on every other row. The row has no table identity or table-version fields.
There is exactly one live contract row per converted manifest branch.

Serving captures read contract text with the state in one manifest scan.
Historical and migration readers may project only the small identity.
Content loading pins both native branch and manifest version; equal version
numbers on different branches are not interchangeable. The engine checks the
identity envelope, parses and validates the IR, recomputes its compiler-defined
hash and proves that the source compiles to the IR's semantic shape. The IR
hash is not a hash of the raw JSON formatting. It then validates table
identities and paths against the captured catalog.

The accepted-catalog cache retains the complete validated row and its exact
captured manifest image. A new image must supply its row before a catalog with
the same semantic identity can be reused. Unchanged images reuse validation;
an acknowledged publication can supply its exact retained rows without I/O.
A cache hit cannot supply a contract from another identity domain, or accept
changed source or IR merely because its head is unchanged.
A missing, duplicate, malformed or mismatching row fails admission;
normal serving has no root-file fallback.

Fresh initialization includes the row in its genesis manifest Create. It
retains the existing `__init_claim.json` ownership and exact-genesis outcome
checks. The claim remains necessary to coordinate competing initializers;
removing schema files does not remove that ownership problem.

### Apply and table creation

Schema apply stages existing-table rewrites as detached commits. Its one main
publication contains the new registrations and tombstones, the graph-lineage
update and replacement contract row. It checks the captured graph head, branch
identity and expected table versions. The system-column upgrade submits its
renamed table references and replacement contract through the same publisher.
Installing a new in-memory view afterward is cache maintenance, not another
durable contract installation. A published schema needs no file promotion on
open or refresh.

Added-type creation needs special care because retries derive the same path
from the accepted identity allocator. The rule is to preserve that
path and every existing version. An original empty, stable-row-ID version-one
dataset with the desired physical schema can be reused. If its schema differs
because an earlier uncommitted apply requested another shape, stage an empty
detached Overwrite based on that original version. Publish that result at
logical table version 2, with its detached version, transaction UUID and
`last_linear_version = 1`, against expected absent table version 0. A fresh or
matching original Create remains logical version 1. The detached replacement
must be above the last linear version so readers open it rather than the old
linear image. Nonempty or otherwise unqualified leftovers refuse.

The final manifest CAS decides which competing apply may publish. A stale
attempt must not delete or overwrite a path another attempt may already have
registered. The schema-apply owners cover matching reuse, changed desired
schema and a stale competing apply that loses without deleting the path.

### Projection and write cost

The content columns remain outside the packed record so ordinary catalog folds
can omit their bytes. A cache miss reads the contract row once for the accepted
identity and validates it; later captures can reuse the accepted catalog.
This removes contract-file requests, not all manifest I/O.

Cold open captures state, lineage and contract content in one scan of a pinned
image. It preserves the later freshness refresh: a changed manifest incarnation
discards the captured content and reads the refreshed image. Content errors are
held until that choice, so a concurrent repair is judged at the same boundary.
Routine state scans continue to omit the content columns.

Ordinary open retains the metadata image used for format admission before the
process-local schema queue. It consumes that image for the combined scan, then
checks the serving format of the image held after the freshness refresh. If a
captured scan fails, one fresh metadata open may replace it only when the
complete image changed; an unchanged failed image remains an error.

The combined scan can use a temporary reader for direct data files whose known
encoded sizes total at most 64 KiB, when the existing read block is smaller.
Each eligible file is fetched through a bounded request and its response size
is validated. Body failures retain Lance's normal download-retry behavior.
The coordinator keeps its original reader. Large files, indirect layouts,
pending upgrades and raw in-memory bindings retain their existing path. This
bounds the encoded files eligible for this policy, not total decoded memory.

Named-branch queries may borrow an existing matching write coordinator after a
fresh incarnation probe; queries do not insert or refresh that cache. Deletion
takes a matching cached coordinator after its branch gate and validates it after
the table gates. A stale or absent capture uses the ordinary coherent opener.
Native-ref retirement and catalog validation remain required.

The publisher may retain the exact stored rows of its last acknowledged
publication. A fresh open must match the complete immutable Lance manifest,
its native location and schema metadata before reuse. Its one-entry cache has
an 8 MiB accounted Arrow-buffer/key budget; misses use the same full scan.
The same row decoder, logical preconditions and version CAS still run. Exact
source and IR bytes are never reconstructed from the accepted-catalog cache.

The existing copy-on-write publisher rewrites the live row set. Keeping the
contract inline therefore carries its source and IR payload on each publication
and in retained manifest versions. Physical bytes and request counts depend on
Lance encoding, compression and range-read block sizes. Evaluation includes
request counts and bytes across schema sizes and warm and cold operations.
Retained manifest versions may retain older contract text under ordinary
retention; this proposal adds no separate audit-retention guarantee.

A synthetic measurement against base `7f1233bb` uses 140 node types, four
properties per type and one populated row. Its source is 14,839 bytes and its
pretty SchemaIR is 311,064 bytes. Fresh debug executables run the same seeded
case on the in-memory object store with a 4 KiB read block size, the unit cost
model and two-process replay:

| Executing operation | Requests, base → candidate | Bytes read, base → candidate | Bytes written, base → candidate |
|---|---:|---:|---:|
| Warm query before or after the write | 1 → 1 | 0 → 0 | 0 → 0 |
| Update one populated row on main | 25 → 21 | 79,988 → 407,859 | 143,808 → 472,046 |

This fixture measures the first write after setup, so its publisher has no
retained prior publication. It does not measure a second warm write.

These measurements cover Lance object-store traffic, excluding setup and the
runner's checks. Root schema-file requests are measured separately by the
engine cost owners. The write adds two manifest-data requests and removes six
sentinel-ref requests; carrying the contract costs about 328 KB more read and
written per publication in this fixture. Request savings do not imply byte
savings. Small files may cross a range-read threshold and require more GETs.
The experiment measures neither real cloud latency nor CPU parsing time.

The workspace pins Lance 11.0.0 and writes stable file version V2_2. For a
filtered scan of the whole packed record, Lance's default materialization
heuristic can split fixed-width and variable-width children between early and
late reads. The contract-row scan uses `MaterializationStyle::AllEarly`, which
keeps the projected record together. The exact filtered-shape guard also
passes with `AllLate`; neither explicit style splits the record as the default
heuristic does in this fixture. This does not make isolated packed-child projection supported,
and it makes no claim that every row-address read is broken. The storage
surfaces are described in [the Lance contract](../dev/lance.md); the relevant
locked implementation is `lance-11.0.0`'s scanner materialization and strict
overwrite paths.

### Explicit conversion and cleanup

The migration extends the intent, per-branch receipt and final activation
protocol of [RFC 0064](0064-explicit-storage-upgrades.md).

1. Inspect each live native branch at its captured version. Accept actual flat
   v11 and packed v12 layouts, including either mixture across main and named
   branches. Preserve exact branch identities, table registrations, commit IDs,
   ancestry, unrelated metadata, retirement records and retained versions.
   Refuse incompatible physical schemas, legacy contract staging, a residual
   schema-apply sentinel and unresolved recovery sidecars before effects.
2. Read and validate `_schema.pg`, `_schema.ir.json` and `__schema_state.json`
   through the upgrade-only legacy reader. Preserve the chosen source and IR
   bytes exactly. Pin their raw-text SHA-256 digests and the semantic contract
   identity in protocol 5's existing durable intent. Comment or JSON-format
   changes must be detected even when the semantic identity stays equal.
3. Fence main using the existing pending-upgrade metadata. Convert each branch
   from its own captured version. One strict overwrite writes its preserved
   logical rows, the contract row, target stamp and completion receipt. A
   schema-format conversion creates no artificial graph commit. Strict
   overwrite uses zero Lance commit retries; foreign movement is not rebased
   into an accepted conversion.
4. Validate every receipt and converted row against the pinned contract,
   including the exact text digests, and revalidate the complete branch
   inventory. Convert main after named branches and keep its pending marker.
   Before completion, retry still needs the intact legacy contract. Once all
   branches are converted, main's validated row supplies the canonical bytes.
5. Delete the three legacy contract files only after that validation. Missing
   files are tolerated at this stage, so retry can finish after any subset of
   deletes. Revalidate ownership and activate main last by clearing the pending
   marker. A cleanup failure leaves normal open fenced until retry finishes.

The earlier v10-to-v11 step needs an engine handle before the source has a
contract row. A narrow upgrade-only constructor takes a validated contract,
requires the existing v10 conversion admission and supplies it only when that
handle's source snapshot lacks a row. It cannot hide a malformed present row
or relax normal open. The override is immutable per-handle state, not a new
global contract hook. An ordinary open must remain refused while such a
conversion handle exists.

## Invariants

- A published schema change has one manifest version containing its contract,
  table references and graph-lineage change; there is no published contract
  installation left for another process to finish.
- The table references and accepted catalog used by an operation come from one
  captured authority. A retry discards the previous attempt's captured state.
- Ordinary serving requires the row and validates its full identity. Root
  schema files are inputs only to explicit conversion.
- An added-type attempt preserves an existing path and its versions. Only the
  manifest publication grants its candidate graph authority.
- Conversion preserves logical graph history and data identities. Before
  activation, main remains fenced; after activation, every live branch has the
  same validated contract bytes and no legacy contract file is required.
- A local schema permit provides process ordering. The manifest publication
  remains the cross-process authority, and offline upgrade still requires
  operator-enforced quiescence.

## Compatibility and reversibility

Set both the current internal manifest stamp and serving floor to **13**.
The physical decoder retains the distinction between flat v11 and packed v12
for conversion and retained history. Ordinary open refuses both source stamps
and every future stamp before serving.

Register `schema-contract-v11-v12-to-v13` and make 13 the default upgrade
target. Qualified older inputs compose through the existing steps:
`6 → 7 → 8 → 10 → 11 → 13`; v9 starts with the existing step to v10. Preserve
explicit intermediate targets 7, 8, 10 and 11 for compatible executables.
Format 12 remains an unsupported explicit target. A valid v13 graph reports
`already_current` without requiring deleted legacy files. Check mode has no
effects and reports later checks that depend on intermediate output as deferred.

Source formats without an admitted route still require export with a compatible
binary and rebuild. Conversion does not grant serving admission to an
intermediate format. Rollback restores the complete pre-upgrade backup with
the compatible executable; retained old manifest versions are not a downgrade
mechanism. Writes after conversion are absent from that backup.

## Alternatives

**Keep the root files and remove only the sentinel.** This removes an advisory
mechanism but leaves the manifest commit and file replacements separate. A
crash after publication still needs a durable way to identify and finish the
contract installation. It does not achieve the stated publication invariant.

**Put only identity in the manifest and store immutable text by hash.** This
keeps the manifest small, but creates contract objects that must exist before
publication and remain available for every referencing version. It adds their
lookup and reclamation rules. Inline content uses the existing manifest
lifetime and accepts the byte tradeoff, subject to the measurements above.

**Store the contract in a separate dataset.** A second dataset does not share
the manifest CAS. A pointer to an immutable version could bridge the two, at
the cost of another dataset, version reference and retention dependency.
The one-row design avoids that additional lifetime.

**Keep the new row but omit exact-text ownership from upgrade intent.** The
semantic IR identity permits source-comment and JSON-format changes. If those
files change between branch conversions, branches could receive different
bytes while every semantic check passes. Raw-text digests in the existing
intent reject that concrete partial-conversion case. They are conversion
evidence, not another serving authority.

**Delete an unregistered added-type path on retry.** Another apply can register
that deterministic path after the retry's inspection. Deletion then destroys a
published table before the retry loses its final CAS. Reusing the original
empty version or staging a detached replacement avoids that destructive step.

## Evidence and tests

The following owners define the required proof. Local database unit tests
cover the converter's layouts, exact contract ownership and interrupted cleanup.
Both released-predecessor journeys and the system-column and Blob composition
owners pass locally. The packed guard confirms default failure and identical
AllEarly/AllLate results. Final workspace correctness and cost gates, and
designated human review, remain prerequisites for complete implementation.

| Boundary | Existing owner to extend | Required proof |
|---|---|---|
| Row format and publication | `omnigraph-catalog` tests | Exact text round trip; one row through ordinary publishes; duplicate, missing, null and identity mismatch refusals; branch-and-version pinning; atomic replacement with graph changes. |
| Schema lifecycle | `tests/schema_apply.rs`, `tests/system_column_upgrade.rs`, `tests/failpoints.rs`, `tests/detached_commit_matrix.rs` | Before/after publication windows; fresh read-only and read-write open; same-handle progress after faults; no contract staging or sentinel; existing system-column behavior. |
| Added-type retry | `tests/schema_apply.rs` and failure-window owners | Matching reuse; changed desired schema; stale competing loser; original nonempty/foreign-state refusals; reads, cleanup, repair and change discovery of logical version 2 with last linear version 1. |
| Read binding | `tests/point_in_time.rs` and schema owners | Unrefreshed handle after another handle's rename; current aliases on retained images; full-identity cache key; live freshness and retained old storage layouts. |
| Packed scan | `tests/lance_surface_guards.rs` | Default heuristic, AllLate and AllEarly against the same filtered whole-record request; exact AllEarly output with fixed-width and variable-width children. |
| Conversion | `db/upgrade/tests.rs` | Real flat and packed source layouts; both mixed directions; byte/identity/history preservation; source-text drift with equal semantic identity; strict branch movement; every fence, conversion, delete and activation interruption; no-op retry and check mode; ordinary-open isolation from the upgrade constructor. |
| Released predecessors | CLI `tests/crossversion_upgrade.rs` | `genuine_v09_explicit_storage_upgrade_preserves_history` and `genuine_v010_explicit_storage_upgrade_preserves_history` create sources with the released executables and complete the new route with branches and retained history. Missing executables and skipped cases do not count. |
| Cost and structural gates | `write_cost.rs`, `warm_read_cost.rs`, `branch_control_cost.rs`, `forbidden_apis.rs`, `check-storage-upgrade-ci.py` | Contract request removal, manifest-byte cost across schema sizes, reviewed durable-call ownership and required test execution. Backend qualification remains specific to each tested environment. |

## Rollout

Complete the catalog and engine changes, then qualify the explicit converter
and its earlier-route dependency. Extend existing test owners rather than
adding a second upgrade harness. Run the required predecessor and correctness
gates, then record the public cost evidence and remaining backend limits.

Update the current versioning, upgrade, writes, recovery, invariants and test
ownership documents together with the release note. The current docs must
describe implemented behavior; they must not gain authority from this draft.
Link the changed contract-installation and added-type cases from the earlier
RFCs without marking their entire protocols superseded. A maintainer decision
on this RFC remains separate from implementation progress.

## Unresolved questions

There are no unresolved design choices within the selected scope. The evidence
and designated human-review gates are listed in the frontmatter and Evidence
and tests.

## Decision log

No external review outcome or maintainer acceptance is recorded.
