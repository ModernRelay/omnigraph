---
rfc: "0064"
title: "Explicit storage upgrades"
track: maintainer
status: draft
implementation: in-progress
authors:
  - Azim Afroozeh
created: 2026-09-09
updated: 2026-09-10
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Qualify object-store backends and deferred orphan reclamation before expanding local standalone support"
  - "Storage-maintainer review of the implemented protocol and genuine v0.9/v0.10 compatibility evidence before acceptance"
---

# RFC 0064: Explicit storage upgrades

> Number provisional: recheck the registry and open reservations before publication.
> A term in ***bold italics*** is defined at that spot.

## Summary

Extend `omnigraph upgrade` to select developer-written
***migration handlers***, code for named conversions with declared inputs,
outputs, prerequisites, checks, effects, validation, and recovery.
Compose internal v6 to v7 registration metadata ([RFC 0062](0062-manifest-version-clock.md))
with metadata-only v7 to v8 retirement admission ([RFC 0042](0042-incarnation-suffixed-branch-refs.md));
reuse table data only where its meaning and references remain valid.
Later handlers can cover [RFC 0040](0040-system-column-namespace.md)'s system
columns and separately qualified historical fork-ownership conversion. Arbitrary
conversions are unsupported; the default is fast, in-place migration that appends metadata and reuses table
data. Publication and recovery still require proof; reclamation runs later.

## Motivation

Normal open under the [versioning policy](../dev/versioning.md) refuses
incompatible graphs. Export/import preserves exported entities but restarts
commit history, snapshots, and shared branch ancestry. RFC 0062 identifies a metadata
converter using `_row_last_updated_at_version`; RFC 0040 specifies a recoverable
explicit upgrade. Extend that entry point to cover both without conflating their protocols.

Full backward compatibility by v1.0 is a product goal, not a release-number
guarantee. For storage, it means newer binaries directly read and write
supported older graphs. This RFC establishes no wire, query-language, or SDK
compatibility; explicit upgrade remains useful for future required conversions.

## User and operational behavior

Proposed migration behavior and options for `omnigraph upgrade`:

```text
omnigraph upgrade <graph>
omnigraph upgrade <graph> --check --to-format 8 --json
omnigraph upgrade <graph> --to-format 8 --json
```

The binary selects registered handlers from the stored format/capabilities to
one declared default target, independent of its serving range and release
number. The current default is v8. `--to-format` overrides the target, never
names a handler: explicit v7 stops after registration conversion for a
v7-compatible executable, while normal open in the current binary still requires
v8. The default does not adapt to the graph.
Missing steps, cycles, ambiguous routes, and unimplemented handlers block
execution before effects. Numeric adjacency proves no route: graph format,
schema IR, and recovery format are separate compatibility axes.

| Stage | Contract |
|---|---|
| `--check` | Select the route and run read-only handler preflight: formats, branches, retained history, references, dependencies, unsupported state, and pending recovery. No conversion, recovery writes, indexes, or implicit initialization. Report work categories and copying/rewriting estimates; distinguish known, unknown, and deferred checks. Unknown work is never zero. |
| Execution | Stop servers, embedded writers, maintenance, and cluster applies; prevent restart. Local locks or unchanged heads do not prove exclusivity. Repeat preflight under exclusive operator control. Validate checks requiring intermediate output before the affected handler's effects. |
| Preparation | Preflight identifies the source-compatible executable and command for pre-existing recovery; resolve it before starting a new handler. Unknown recovery formats refuse. Retain the old executable and verified backup covering the root and required dependencies. |
| Retry | After interruption or lost output, rerun the same command and target under exclusive control. Inspect authoritative state before effects; resume owned recovery or refuse with the required executable and exact action. Unknown/ambiguous ownership refuses. Handler-owned recovery may follow an early format fence: the source executable is not automatically safe. |

An online check is advisory: it proves neither converted-output correctness
nor future availability and never authorizes serving with the target binary.
`--check` may report recovery actions but cannot perform them. A v6 → v8 check
cannot inspect the future converted v7 output: `work.deferred_checks` names those
output-dependent checks, which execution must run before the v7 → v8 handler's
effects. A passing check does not imply those deferred checks have passed.

| Outcome | Meaning | Exit |
|---|---|---|
| `check_passed` | Read-only checks pass; conversion remains required | 0 |
| `already_current` | No handler needed; no owned upgrade recovery remains | 0 |
| `completed` | Execution validated every requested handler and the resulting graph | 0 |
| `check_failed` | Blocking check or execution-preflight finding | Nonzero |
| `interrupted` | Execution stopped before route completion | Nonzero |
| `recovery_required` | Durable state requires an identified recovery action | Nonzero |

`--check` never returns `completed`; partial chains never succeed. JSON and
human results carry mode (`check`/`execute`), outcome, graph identity/location,
observed format, target and whether defaulted, ordered route, completed
handlers, blocking findings with stable codes, and last durable completed
boundary. When recovery is needed, include failed handler, executable
compatibility, and exact operator action. Unestablished values are unknown.

Cluster-managed graphs use the same engine handlers through a qualified
cluster operation; direct execution cannot bypass applied configuration,
policy, or runtime ownership. Refuse that route until implemented and tested.

## Design

### Terms and shared machinery

- ***Storage format***: persisted structure and meaning identified by the
  graph's internal-schema stamp, independent of binary release.
- ***Retained snapshot***: historical state readable under source retention.
- ***Native ref***: a Lance branch reference for a graph or table; logical
  names do not prove native lifetime identity.

Use compiled concrete handler functions, sharing admission, planning, reporting,
and results. Developers supply rules and tests; the binary selects rather than
invents conversions. A v6 graph targeting v8 selects protocol 1 (v6 to v7),
then protocol 2 (v7 to v8). A v7 graph selects only protocol 2. Explicit target
v7 selects only the first handler when required; longer routes require every
declared prerequisite. Compatible releases need no new handler. No plugins, third-party scripts, schema-diff language, or
generic migration ledger/scheduler. Share recovery only when effect identities
and publication rules match. Durable graph state and owned recovery establish
completion; progress reports and early-fence target stamps cannot establish it alone.

Each handler defaults to in-place, append-only metadata work. Give it a restricted
storage interface for source reads and owned metadata staging, without methods
to overwrite/delete existing objects or copy/rewrite table payloads. Handlers
return a conversion plan; the shared runner validates it and publishes through
the existing graph-content publication path. Raw storage handles must not bypass
these restrictions. Protocol-owned publication and fencing effects use the
sealed publication/recovery APIs, not unrestricted handler writes.

This is a default, not a claim that every format change fits it. Encoding,
physical-type, or encryption changes may require data movement. Such a handler
must declare an explicit exception, its effects and scope, storage-maintainer
approval, separate qualification tests, and work budgets. `--check` reports the
exception and its expected cost; there is no silent fallback to full rewriting.
Measure metadata scans and validation reads separately from payload copying or
rewriting. The v6 to v7 prototype must prove the default contract.

### v6 to v7

RFC 0062 changes `table_version`/`table_tombstone` key suffixes from table data
versions to the owning branch's `__manifest` publication versions, preserving
data-pointer meaning. The candidate conversion is:

1. Admit genuine released v6 with valid schema, recovery, identity, and version
   evidence. Stamp-only admission cannot distinguish reused experimental formats.
2. Inventory all live branches, native identities, retained snapshots, and table
   references needed for reads, merge ancestry, and cleanup. Never drop non-main
   branches to satisfy another handler.
3. Decode selected snapshots explicitly as v6. Capture each registration and
   tombstone's original `_row_last_updated_at_version` before rewriting; retries
   must reuse this source evidence, not the rewritten row's update version.
4. Preserve source clocks only with proved continuity; otherwise qualify an
   order-preserving translation into each destination branch's `__manifest`
   publication clock. All retained snapshots must accept translated registrations
   and tombstones, including inherited precedence; later ordinary writes must
   order after converted state. Locator translation alone is insufficient.
5. Preserve stable table/incarnation identities and `(table_version, table_branch)`
   pointer meaning. Validate keys, ordering, tombstone precedence, lifetimes,
   and exact data availability; duplicate/ambiguous identities refuse before
   publication. Publish through the selected whole-graph protocol and validate
   retained reads, reopen, and subsequent writes before success.

This is not proof of historical conversion: overwritten registrations may exist
only in older retained snapshots; current rows cannot reconstruct missing history.

Preserve node/edge IDs, properties, schema identity, vectors, stored Blob values
and external descriptors, logical branch names/ancestry, retained commit IDs,
historical logical rows, and logical change-feed contents. Compare canonical
snapshot data or explicitly equivalent query semantics, not query bugs or wire
representations. Release-level query changes need separate expectations and
cannot hide conversion damage. Lost history is not recreated; unavailable
full-text history retains existing index-compatibility restrictions without an
inline rebuild. The same rule applies to pre-0.10 Blob property-lifetime
restrictions: retained bytes and descriptors are preserved, but missing identity
evidence is not fabricated. Preflight reports affected fields and tests read
retained bytes independently of the delivery guard. Physical version changes require validated resolution of every
exposed locator and dependent reference; no implicit renumbering.

For registration-order damage, distinguish preservation, explicit repair, and
refusal. This handler refuses diagnostically when reordering would change
logical rows; automatic repair is excluded. Test this independently of query
representation changes.

### Physical protocol and dependencies

The v6-to-v7 implementation uses the existing graph location and a sealed
manifest publication gateway. It requires exclusive operator control: stop all
servers, embedded writers, cluster reconciliation and maintenance. Process-local
root exclusion does not fence other processes or already-open old handles.

1. Preflight reads exact source versions, branch lifetime identities and the
   schema identity. It refuses ambiguous logical branch names before fencing:
   a suffixed native ref needs its own post-fork logical-head witness, not an
   inherited head. It counts version references before loading history. It compares the legacy data-version fold with the proposed
   registration-clock fold and refuses changes in logical state.
2. A main-manifest UpdateConfig commit atomically sets format 7 and
   `omnigraph:storage_upgrade_pending`. Its versioned intent contains an attempt
   ULID, source/target formats, schema identity and every native branch's source
   version and lifetime identity. Old executables reject the new stamp; new
   normal opens reject the pending intent before recovery or decoding.
3. With main still fenced, each native branch and then main appends converted
   manifest fragments from its pinned v6 source. Only registration/tombstone key
   suffixes change. Publication includes `omnigraph:storage_upgrade_receipt`,
   binding that branch to the attempt and source. Main keeps the pending intent.
   Publication has no automatic retry or cleanup; a changed head refuses.
4. Validate all converted branches against their source state. Only then append
   a main UpdateConfig commit removing the pending intent. This is activation;
   success requires it. No new serving pointer, locator map or generic ledger
   is introduced. Durable authority remains the main manifest.

Released v0.9 creates two unstamped bootstrap snapshots before stamping v6.
Preflight accepts those only at versions 1 and 2 with the exact empty-table,
version-one-pointer, single-parentless-genesis contract. Their registration keys
are already clock 1; normal root admission still rejects unstamped graphs.

Retained source snapshots are deliberately not rewritten. After normal root
admission, an explicit v6 decoder preserves their original data-version ordering;
v7 snapshots use registration clocks. Numeric locators, commit IDs and table
pointers remain unchanged. Current registration clocks come from the pinned
source row provenance and precede subsequent publication versions.

Retry validates the exact branch inventory, lifetime identities, source versions
and receipts. Completed branch publications are reused; unpublished staging is
recreated from immutable source evidence. Unknown ownership or foreign movement
refuses. A failure after activation is an already-current no-op on retry. The
pending main intent owns manifest staging for the attempt; cleanup is excluded
throughout the operation and remains a later retention-controlled activity.

This is an explicit amendment to the head-only proposal: legacy historical
decoding and main-owned intent/receipts are necessary protocol state. The
alternative of rewriting historical snapshots would require a locator map and
would break the existing numeric version contract without further machinery.

Every staged artifact needs attributable ownership before effects. Cleanup must
distinguish abandoned/obsolete artifacts from active graph data, retained history,
shared dependencies, and recovery-owned state. Reclaim only after existing
retention and recovery rules allow it; names alone do not establish abandonment.
Reuse or extend the existing cleanup owner rather than add a migration scheduler.
Cleanup reclaims space; it must not finish logical conversion or make an already
successful migration correct. Its compatibility must be qualified before shipping.

Inventory the ***dependency closure***, all objects required by retained states,
including shared files outside the root. Required bytes need enforced retention
and immutability or verified backup/restore. Stop or exclude every writer and
cleanup process that could invalidate them, including other graphs; graph-local
exclusion and root-only backup are insufficient. Unknown dependencies or
unprotected required bytes refuse. Test cleanup, source retirement, and rollback.

Preserve external Blob descriptors exactly. Externally managed URI bytes are
excluded from preservation/rollback unless immutability, retention, or verified
backup is established; preflight and results enumerate exclusions. Identical
descriptors do not prove byte preservation. Required shared Lance files cannot
use this exclusion.

### v7 to v8 and route composition

The v7-to-v8 handler preserves registration rows, table pointers, commit IDs and
historical locators. It changes only manifest configuration through the sealed
publication gateway; no table data or registration keys are rewritten. Retained
v7 snapshots keep their original interpretation. This handler enables the v8
retirement contract without retiring a branch or inferring legacy fork ownership.

Protocol 2 retains all-branch admission, an owned pending intent on main,
per-branch publication receipts and main-last activation. Every branch must
validate before main clears the pending intent. The current binary refuses v7
before conversion and any pending state during conversion. Current v8 no-op
admission validates retirement markers and excludes valid retired refs from
logical branch enumeration while preserving their physical ancestry. Source
v6/v7 admission refuses any reserved retirement metadata, including a marker
that would be valid under v8; conversion cannot legitimize ambiguous source state.

Protocol 1 remains exactly the v6-to-v7 protocol above. Existing pending attempts
retain their source, target, attempt identity and receipts. A request for v8
finishes that owned v7 target before starting protocol 2; it never reinterprets
the earlier pending intent as a v8 attempt. A later-step failure does not undo
v7 activation. Explicit target v7 stops there, and cannot downgrade a pending
v7-to-v8 attempt. Normal serving still requires v8 after the complete route.

### Later handlers

RFC 0040 owns main-only admission, preflight, `SchemaApply` recovery, and ordered
stamp/schema effects. Its CLI can delegate here without changing its cluster
declaration or branch restriction; this RFC does not activate it. Its early
stamp fence precludes a universal stamp-last rule.

Fork conversion requires an accepted ownership representation and durable proof
of existing ownership. Never infer it from plausible names, recreate missing
incarnations, or assign current ownership to historical borrowers. Ambiguity
refuses. Rewriting historical fork ownership remains a separate conversion;
the registered v7-to-v8 handler introduces retirement interpretation only.

## Invariants

The [architectural invariants](../dev/invariants.md) apply: one graph-content
publication door, coherent snapshots without mixed formats, stable identities,
and loud refusal of unsupported state or missing required history. Incompatible
effects require owned recovery or an isolated disposable destination, never CLI
progress authority. Use Lance APIs inside the sealed boundary: no custom
transaction manager, public writable dataset handle, or raw metadata rewriting.
Bound scan batches and retries; report progress. Upgrade may scale with retained
history, ordinary requests must not. Engine action/scope admission covers direct
and cluster callers; actor attribution grants no permission by itself.

## Compatibility and reversibility

Converter input support does not widen serving support. Historical legacy
decoding requires explicit acceptance, not silently lowering
`MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION`. Earlier, future, and experimental
formats without handlers retain refusal and rebuild guidance.

No reverse handler: restore the complete backup and old deployment, including
required dependencies and declared external-byte exclusions. Retained old objects
in the same root do not by themselves provide rollback after activation or fencing. Plan explicitly for post-cutover writes absent from that backup.
Later-handler failure never automatically undoes completed handlers.

Qualified registered routes amend rebuild-only policy while retaining the
fallback. User guidance and the support matrix name only implemented, qualified
routes; unqualified backends, cluster entry points and later handlers remain
excluded while this RFC's acceptance gates are open.

## Alternatives

| Approach | Tradeoff / disposition |
|---|---|
| Separate destination | Isolates source authority but adds relocation, storage and cutover cost. Retain as an explicitly qualified exception if in-place conversion cannot satisfy the contract; no assumed root-copy/rename support. Shared shallow-clone dependencies must survive cleanup and source retirement; disposable restart requires source immutability and output ownership. |
| Export/import only | Existing, no converter state; loses ancestry/history. Retain fallback. |
| Separate commands | Locally small; duplicate admission/reporting and obscure prerequisite order. Share entry point. |
| Automatic open conversion | Less interaction; expensive effects/recovery become startup behavior. Reject initially. |
| Generic ledger/scheduler | Arbitrary resume chains add authority; handler recovery suffices for current scope. Omit. |
| Current heads only | Small rewrite cannot preserve history without legacy decoding. |
| Direct compatibility now | Avoids conversion but commits the engine to older read/write semantics. Evaluate toward v1.0. |

RFCs 0040 and 0062 are the nearest precedents; all-branch historical conversion
has a different visibility boundary from main-only schema apply, so recovery
formats need not match. PostgreSQL's [pg_upgrade](https://www.postgresql.org/docs/current/pgupgrade.html)
and Neo4j's [database migrate](https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/migrate-database/)
establish an operator pattern, not Lance safety.

## Evidence and tests

Existing evidence is limited: `db/manifest/migrations.rs` separates refusal from
the converter seam; `db/manifest/graph.rs` enables stable row IDs;
`tests/lance_version_columns.rs` exercises physical version columns. None proves
conversion. Read complete relevant [Lance pages](../dev/lance.md), then qualify
pinned Lance 11.0.0; current website prose alone is insufficient.

Every handler needs rule tests, supported/refused inputs, genuine predecessor
fixtures, interruption/retry, and work assertions. Extend these owners, not a
parallel harness:

| Owner | Required coverage |
|---|---|
| `crates/omnigraph-cli/tests/crossversion_upgrade.rs` | Genuine predecessor creates nodes, edges, properties, branches, updates, deletions, and retained snapshots. Capture expected state; check, upgrade, compare the full preservation contract above, then reopen/write/merge/cleanup and recheck history. Old/new refusal, rerun, rollback. Counts alone are insufficient. |
| `crates/omnigraph/tests/lance_version_columns.rs`, `lance_surface_guards.rs` | Update, inheritance, compaction, retained provenance, clock translation, and selected physical protocol. |
| GQT | Equal/lower table-version adoption, key replacement, tombstones, borrowed forks, delete/recreate, and equivalent-semantics historical rows; damaged-input refusal separate from query changes. |
| `tests/failpoints.rs`, `tests/recovery.rs` | Before/after every durable effect/completion boundary, lost acknowledgements, same-command retry, source versus handler recovery, old-binary refusal, no readable mixed state. Every interruption leaves usable source or defined recovery to complete target. |
| Branch/maintenance/change-feed owners | Every retained branch/commit addressable; identity and results survive later operations. External deletion/mutation either refuses or satisfies retention/restore. |
| Existing local/object-store journeys | Same contract on every advertised backend; no atomic-root-rename or process-local distributed-fencing assumption. |
| Shared runner tests | Selection/order, missing/cyclic/ambiguous routes, later-step failure, already-current state, check success/failure, unknown/unavailable input, JSON/exit reporting, zero writes from `--check`. |

Independently instrument storage operations, not handler counters. Separate
metadata work from payload bytes read/copied/rewritten/reused. Metadata-only CI
asserts zero payload copying/rewriting, including storage-side copies, and tests
reused-file readability across branches/history. Data-moving handlers need
fixture-specific budgets and declared-scope coverage. For the default handler,
also fail forbidden existing-object overwrites/deletes at the restricted interface
and storage-operation boundary, allowing only separately specified runner-owned
publication/fencing effects. Set metadata-operation and metadata-byte budgets on
representative fixtures: append-only is not a performance bound. Measure downtime
separately; no universal latency or size promise.

Crash tests must exercise staging, validation, activation, and later cleanup.
Verify old-state visibility or explicit recovery refusal before activation,
complete new state afterward, safe retries, retained history, and eventual
reclamation of eligible leftovers without deleting live/shared/recovery data.
Successful migration must not depend on having run cleanup.

### Required compatibility CI

Once this migration support ships, run a required job on every change, including those
without format bumps. A version-controlled support matrix specifies predecessor
binaries, fixture coverage, and expected routes; it is test configuration, not
graph state. Test the declared route, never whichever happens to pass:

| Route | Required result |
|---|---|
| Direct compatibility | Candidate reads/writes/reopens predecessor graphs without migration; preservation checks pass. |
| Explicit upgrade | Incompatible serving refuses before conversion; complete registered route and all handler/post-upgrade tests pass. |

Cover every persisted feature available in each predecessor; extend fixtures
for new persisted state. Format/target changes require complete routes from
every supported conversion input and tests in the same change. Keep refusal
tests for unsupported formats; no support deletion merely to pass CI.

Storage-maintainer review covers persisted formats, handler contracts, fixtures,
expectations, support scope, and gate configuration. Required binaries, executed
case inventories, and non-skipped cases must all be present. Fixture coverage
limits detection of unexercised breaks; this RFC configures neither CI nor review
enforcement itself.

| Evasion | Defense |
|---|---|
| Omit format bump | Real predecessor journeys on every change. |
| Empty handler | Exact state, later writes, recovery, and work assertions. |
| Missing binary, skipped/filtered cases | Fail missing executables, skipped cases, or zero matches against required inventory. |
| Weaken expectations/delete fixtures | Storage-maintainer review of coverage and expectation changes. |
| Bypass handler restrictions or call full copying metadata-only | Restricted capabilities plus independent operation accounting; reject undeclared payload work or existing-object mutations. |
| Hide unbounded metadata work or require cleanup to finish conversion | Fixture metadata budgets; crash and preservation checks before cleanup, followed by safe reclamation tests. |

| Author route | Acceptance |
|---|---|
| Preserve compatibility | Direct journeys pass; no new handler. |
| Convert supported input | Format change, handler, and tests together; upgrade journeys pass. |
| Intentional query representation change | Reviewed equivalent-semantics expectations; retain storage assertions. |
| Explicit support-policy change | Maintainer-approved policy/matrix amendment plus refusal coverage; no automatic missing-handler exception. |

## Rollout

1. Resolve decisions below and record the protocol. Before design acceptance,
   obtain prototype evidence for in-place staging/activation, history, clocks,
   recovery, safe deferred cleanup, and genuine v6-binary compatibility.
2. Qualify the production handler against the full matrix on every advertised
   backend; prototype acceptance does not replace shipping evidence.
3. Ship check and qualified v6 → v7 → v8 and v7 → v8 execution with
   upgrade/versioning guidance, required compatibility CI, and genuine-binary evidence. Check may ship earlier
   only if execution unavailability is explicit.
4. Integrate RFC 0040 and settled fork handlers under their own gates; refuse unsupported
   chains before effects.

Target v0.11 only if gates pass. The release maintainer chooses delay for this
work or the documented rebuild path; v1.0 goals waive no gates or imply a date.
This draft changes no runtime behavior.

## Unresolved questions

Proposed responsibilities, not named approvals. Close all before acceptance;
record actual decider and evidence in the decision log.

| Decision | Responsible role | Closure evidence |
|---|---|---|
| How does in-place append-only activation use the existing publication path? | Storage maintainer | Prove restricted handler effects, all-branch activation, history/clocks, writer fencing, recovery, rollback and deferred cleanup on pinned Lance. Any exception needs explicit effects, cost and qualification. |
| Cluster entry point? | Cluster maintainer with policy maintainer concurrence | Invocation, applied configuration, cutover, runtime exclusion, action/scope admission and old-format loading; preserve RFC 0040 declaration; qualify direct/cluster refusals. |
| Preserve or translate historical locators? | Storage maintainer with change-feed maintainer concurrence | Enumerate snapshot selectors and cursor/token bindings; prove state resolution and clock ordering. Preserve continuations or specify explicit refusal/restart; no implicit renumbering. |

Release maintainer verifies production qualification before shipping.

## Decision log

No maintainer decision recorded yet.
