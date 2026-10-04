# Versioning and compatibility

**Audience:** storage, API, and release maintainers
**Authority:** current compatibility policy

OmniGraph has independent release, wire, graph-storage, recovery, and Lance
version axes. Never derive one axis from another.

| Axis | Policy | Guard |
|---|---|---|
| Release | Published workspace artifacts move in lockstep. | Workspace manifests, lockfile, generated metadata, release automation. |
| CLI ↔ server wire | One v0.12 contract; coordinated client/server upgrades. Exact request admission and CLI discovery/response validation. | Shared contract header, DTOs, HTTP refusal tests and OpenAPI drift tests. |
| Graph storage | Closed stamp range `[MIN_SUPPORTED, CURRENT]`, independent of system column vintage; a standalone graph at stamp 8, 9 or 13 is converted by the offline `omnigraph upgrade`, a graph at any other stamp is rebuilt by export and load; no open-time migration. | Main-manifest stamp guard on both bounds. |
| Lance dependency and file format | One deliberately pinned Lance family and explicit stable file version. | Lockfile, write parameters, and Lance surface guards. |

## Current storage contract

The current binary serves **internal manifest schema v14**. Both
`MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION` and
`INTERNAL_MANIFEST_SCHEMA_VERSION` are 14. Each live branch's `__manifest`
carries one `schema_contract` row containing the accepted schema source,
compiled IR and identity. Fresh initialization writes it in the genesis
Create; schema apply and the system-column upgrade replace it in the same
publication as their table references. Normal serving has no root-file
fallback. See [Schema contract in the manifest](../rfcs/2026-09-30-schema-contract-in-manifest.md).

Both system-column vintages of [RFC 0040](../rfcs/0040-system-column-namespace.md),
`id`/`src`/`dst` and `__id`/`__src`/`__dst`, remain supported. The schema IR's
feature set determines the vintage, never the storage stamp.
`omnigraph schema upgrade-system-columns` converts the spellings on a
supported standalone graph without changing its storage stamp. Normal open
refuses a graph at any other stamp. The one in-place storage conversion is
the offline `omnigraph upgrade`, from stamps 8 and 9 (release 0.11.x) and
from stamp 13.

- v4 was the last released pre-identity format, used by OmniGraph 0.8.x.
- v5 was an unreleased development format that introduced SchemaIR v2,
  stable table/incarnation identity, identity-keyed manifest rows, and
  identity-derived paths.
- v6 preserves v5 and adds exact non-null physical `id` fencing through
  Lance's unenforced primary-key metadata. It is the 0.9.x/0.10.x format.
- v7 preserves v6's columns and re-keys `__manifest` registration and
  tombstone rows ([RFC 0062](../rfcs/0062-manifest-version-clock.md)): the
  trailing segment of a `table_version:` / `table_tombstone:` `object_id` is
  the `__manifest` version that wrote the row, and a table's current
  registration is the one with the greatest manifest version, not the greatest
  per-native-ref Lance version.
- v8 preserves v7's registration clock and adds logical retirement metadata
  on native graph refs ([RFC 0042](../rfcs/0042-incarnation-suffixed-branch-refs.md)).
  Branch deletion retains exact native parent history while removing logical
  branch authority. Older binaries do not interpret this metadata and could
  expose retired branches; they must refuse v8 graphs before reading or
  reclaiming their storage.
- v9 preserves v8's `__manifest` layout and retirement metadata and marks the
  RFC 0040 system column spellings `__id`/`__src`/`__dst` in every node and
  edge table. Until v10 a v8 graph retained its stamp until
  `omnigraph schema upgrade-system-columns` converted it in place (RFC 0040
  Rollout step 3: stamp advance first, one rename-only commit per table,
  schema promotion last). Since v10 the vintage has no separate stamp and the
  upgrade stages detached renames like schema apply (RFC 0067). Since v13 it
  publishes the renamed table references and replacement contract row together.
- v10 preserves v9's layout and lets a table registration carry
  `omnigraph.staged_version` and `omnigraph.transaction_uuid` (RFC 0067): the
  detached Lance version a pin was staged as and the transaction promotion
  replays at its linear target. Existing registrations without the keys are
  linear pins. Both system column vintages are stamped v10; the
  system-column upgrade no longer advances the stamp.
- v11 preserves v10's layout and stops promoting pins (RFC "Detached-only
  tables"): `published_dataset_version` names no Lance version, a table's
  history is its chain of detached commits, and a registration carries
  `omnigraph.last_linear_version`, the highest linear version a v10 pin
  reached; rows at or below it resolve through their linear twin, rows above
  it open their detached version directly.
- v12 kept the ten logical fields present at v11 (the unused `base_objects`
  list was removed) and stored them as three columns: `object_id`,
  `object_type` and `record`, a Lance packed struct (`lance-encoding:packed`)
  holding the eight remaining fields row-major plus a `present` bitmask, one
  bit per field that is null in the logical row (Lance 11 refuses a null
  value inside a packed struct child, so every child is declared non-null and
  a null is stored as `""` or `0` with its bit set). A scan reads one column's
  pages for the record instead of one per field, and always projects `record`
  whole. Lance 11's default filtered-scan materialization can split packed
  children between early and late reads; the contract-row scan uses
  `MaterializationStyle::AllEarly` to keep the projected record together.
  Isolated packed-child projection remains unsupported. The original v12
  transition converted a v11 branch on its next publish, so predecessor roots
  can mix v11 and v12. Neither is a served format. A Lance
  directory-namespace client, which reads `__manifest` by its own catalog
  column names, fails at `location` on a stamp-12 manifest; it found no
  OmniGraph table at stamp 11 either.
- v13 preserves the packed record and adds nullable `LargeUtf8` columns
  `schema_source` and `schema_ir`. Only the `schema_contract` row carries
  these texts; its `metadata` records `schema_ir_hash`,
  `schema_identity_version` and `schema_identity_domain`. Serving scans capture
  the texts, identity and table state together from the exact branch and
  version. Contract acceptance validates the IR, source shape and captured
  table identities; catalog reuse requires the same validated image or row
  content. Historical and migration folds may read the identity alone.
  Historical queries keep using the accepted
  live contract, with aliases rebound by stable identity to the retained
  table image; an older row does not add historical schema-language semantics.
- v14 keeps the current state, the head commit and a byte-bounded buffer of
  its latest ancestors in a branch's `__manifest`. Older commits are immutable
  Lance files under `__history`, addressed by the commit id, and each commit
  names its archived schema content under `__history/schemas/`. The buffer is
  one `settled_commit` row per buffered commit and one `replaced_table` row
  per `table` row a buffered publish changed, both stored as what differs. A
  `settled_commit` row stores `parent_commit_id` null when the parent is the
  previous buffered commit, and `schema_ir_hash`, `schema_identity_domain` and
  `schema_content_hash` null with `schema_identity_version` =
  `INHERITED_SCHEMA_IDENTITY_VERSION` (0; a contract's own version is 1 or
  more, and a commit without a contract stores all four null) when its
  contract and content hash equal the next newer commit's. The head's
  `graph_commit` row stays whole, and a `__history` record refuses the marker.
  A `replaced_table` row stores no `location`, `table_key` only across a
  rename, and a pin's metadata as a JSON delta against the current pin
  (`manifest_file` in place of `manifest_path` when only the file name
  differs; the whole metadata when the current row holds no pin). The commit
  whose publish makes `CommitBuffer::buffered_bytes`, the rows as stored plus
  the head whole, reach `HISTORY_RELEASE_BYTES` (or the lower
  `history_release_bytes` the session set) takes slot 0 and closes the block;
  the publish after it writes the block under `__history` and empties the
  buffer. `TAIL_MAX_COMMITS` only bounds a buffer of tiny commits. A v13 binary
  would read the buffer as the whole history, so the stamp refuses it before
  any open. Two unreleased prototype layouts were stamped 14 and 15 in
  development trees only; neither shipped and neither has a conversion route.
  A root converted from stamp 8, 9 or 13 also holds `__history/legacy/`, written once
  by the upgrade and never again: the pre-upgrade commits as Lance data files
  under `legacy/data/` and the flat locator objects under `legacy/locator/`
  that find a commit by id and a writer by native name. A root born at 14 has
  no `legacy/`. The layout and its bounds are in
  [RFC 0068](../rfcs/0068-graph-commit-record.md#amendment-legacy-commits-of-the-stamp-13-upgrade).
  A stamp-14 binary built before the upgrade landed reads no `legacy/`: on a
  converted root it reports pre-upgrade commit ids as not found and refuses
  full-history reads, with no stamp to fence it.
- the unreleased v7–v19 stamps of the rejected MemWAL experiment never shipped
  and are not supported migration inputs. Reuse of a numeric stamp by another
  design (RFC 0062, RFC 0042 or RFC 0040) does not make an experimental graph
  compatible. Such graphs require export with the build that wrote them and
  rebuild at a fresh root.

Normal open refuses lower and higher stamps before recovery or table decoding;
it never migrates a graph. `omnigraph upgrade` is the one conversion, on a
standalone root, under one protocol (`UPGRADE_PROTOCOL` 6) from the stamps in
`UPGRADE_SOURCE_FORMATS` (8, 9 and 13). `UpgradeIntent.source_format` chooses
the route, and the report names it per source:
`history-lance-files-v8-to-v14`, `history-lance-files-v9-to-v14` and
`history-lance-files-v13-to-v14`. A stop before any route is chosen (an
unreadable intent, or recovery sidecars on a stamp no route converts) names
`history-lance-files-to-v14` as its failed handler. A graph at stamp 14
reports `already_current`. A stamp above 14 is the finding
`newer_than_binary`, any other stamp outside `UPGRADE_SOURCE_FORMATS` is
`unsupported_source` with the open guard's refusal text, and a `--to-format`
other than 14 is `unsupported_target`, checked before the stamp. A graph
carrying `UPGRADE_PENDING_KEY` (`omnigraph:storage_upgrade_pending`) stays
refused by normal open; `--check` reports it `recovery_required` and a run
without `--check` resumes the attempt its intent names.

The two kinds of source differ only in where the schema contract comes from
and in the shape of the `__manifest` rows read (`legacy::LegacyManifestSource`):

- Stamp 13 (`Stamp13Source`): the packed shape, with the contract in the
  `schema_contract` row of each version.
- Stamps 8 and 9 (`RootContractSource`): the flat shape of release 0.11.x,
  which holds no contract row. The contract is read from `_schema.pg`,
  `_schema.ir.json` and `__schema_state.json` at the graph root
  (`db/upgrade/root_schema.rs`), each object in one read bounded by
  `MAX_METADATA_BYTES` (64 MiB, the head bound), and is the contract of
  every version. A `.staging` copy of one of the three objects, or
  a recovery sidecar, is `source_recovery_required`, with or without a live
  `__schema_apply_lock__` ref beside it; a live `__schema_apply_lock__` ref on
  an otherwise clean root (no `.staging` copy, no sidecar) is reported as
  `schema_apply_lock_retired` and handled once main is
  fenced, so it is not a live ref of the converted graph, as the 0.11.x apply
  that created it would have left it; a missing, inconsistent or over-bound
  object, a stamp 8 whose IR declares `__id`/`__src`/`__dst`, a live ref whose
  tables are not exactly those of the contract, or a registered table whose
  Lance columns are not the ones the contract declares (one table open per
  table, counted as `table_opens`; the check runs in `--check` and before any
  write, so root objects restored from a backup older than a property-only
  apply are refused), is `unsupported_source`. An absent object, or one whose
  bytes cannot be read, is `unsupported_source`; every other storage failure
  reading the three objects is `preflight_failed`: rerun the check. Once main
  is fenced the contract is read from the archive under `__history/schemas/`
  the intent binds, never from the root objects again, so a root object
  changed or removed after the fence does not stop the rerun. Every decoded
  pin gets
  `last_linear_version = Some(table_version)`. The three root objects are
  left in place: no stamp-14 reader opens them, and they go stale at the next
  schema apply.

The run, in order (`crates/omnigraph/src/db/upgrade.rs`):

1. Preflight, writing nothing, after the actor is authorized (a denied actor
   gets the policy denial, not a source finding): every live ref carries the
   source stamp in the shape of its source and the contract is read (from the
   contract row at 13, from the root objects at 8 and 9), `__history/` holds nothing but
   schema archives, and
   the census (`omnigraph-catalog/src/legacy/census.rs`) reads every own
   version of every live and retired ref at its pinned head and derives the
   `HistoryRecord` of every commit. `--check` stops here with `check_passed`
   and the `UpgradeWork` counts.
2. The fence: one commit on main that stamps it 14 and sets the pending key
   to the `UpgradeIntent` (the live refs at their source versions, the schema
   contract, and the `LegacyPlan` with the SHA-256 of the legacy directory).
   Retired refs are not in the intent; the writer shards bind them.
3. The legacy objects, created with `put_if_absent`: data files, id shards,
   writer shards, then the directory.
4. One conversion commit per live ref, main last, leaving the head alone in
   the stamp-14 layout with an `omnigraph:storage_upgrade_receipt`.
5. Validation: every legacy object is read again against the directory and
   the intent, and every converted ref is compared with its source version.
6. Activation: main drops the pending key.

A stopped run is resumed from the intent. Before the directory exists the
census is rerun at the pinned versions under the intent's `LegacyLayout` and
must plan the same directory bytes (`legacy_plan_changed` otherwise); after
it exists the directory is the plan and no census runs. The bounds are
`MAX_BRANCHES` (1024 live refs, and as many retired refs), 1,000,000 rows or
64 MiB per live or retired head, checked before every census,
`HISTORY_RELEASE_BYTES` of commit fields and `MAX_RECORD_BYTES` per record,
`TAIL_BYTES` for the directory, and `MAX_CENSUS_CELLS` (2^33): a stamp-13
publish rewrites every row, so the census reads about 1.5 N² cells for N
commits on one lineage and refuses above about 75,000 commits. A stamp-8 or
stamp-9 `__manifest` never loses a row or a version: its head holds one
registration row per table and publish, the census reads every version since
`init`, and the commit count at which it refuses has not been derived.
`MAX_CENSUS_SNAPSHOT_BYTES` (1 GiB) bounds the `TableRow` values the census
retains, one per table for every commit record and head, and the
`TableRegistration` values it keeps for the first version of every ref and
for every version below a ref's first own commit. It refuses from the head
scans alone, counting each ref's own commits plus one head times the tables
of its head, so a graph whose tables were added late is over-counted; and
again while reading, as each value is retained. The bound counts inline
bytes only (`size_of` of each value): the strings they own are not counted
and the plan holds a second copy of the records, so the process needs
several times the bound. A resume after the directory exists
verifies one data file at a time and keeps only the commit ids.

After the upgrade a commit id from before it resolves through the locator, a
full lineage read lists the legacy data files beside the native blocks, and
a numeric snapshot at a version below the upgrade serves the legacy record of
the commit at that version, or of the nearest commit below it, relabelled
with the requested version; a version of main below its genesis commit is
`manifest_not_found`. Retired refs are never converted: their heads resolve
through the writer shards.

What a converted 0.11.x root (stamp 8 or 9) does not carry over:

- One schema contract for every pre-upgrade commit. 0.11.x overwrote the root
  objects at each schema apply, so every legacy `HistoryRecord` names the
  contract live at the upgrade (`UpgradeWork.schema_contents` is 1). The
  table set and pins of an old commit are exact. A read at an old commit id
  is planned against the live contract, as 0.11.x did.
- Lance table versions above a pin, left by an interrupted 0.11.x write, are
  not detected: the upgrade opens each table of main's head at its pinned
  version only, for the column comparison. `omnigraph repair` reports them
  as `foreign_drift` and `cleanup` lists them as foreign versions.
- 0.11.x `cleanup`, and 0.11.x `schema apply --allow-data-loss` (which ran
  the same version removal on the tables it changed), removed table versions
  and no `__manifest` version, so a retained pre-upgrade `__manifest` version
  can pin a table version that is gone. The collector then reports `is absent
  from the listing` and sweeps nothing. `cleanup --keep 1` alone, as the first
  cleanup after the upgrade, drops those `__manifest` versions; the upgrade
  guide gives the order.
- `_graph_commit_recoveries.lance/` stays as an unreferenced dataset.
- The route writes no `last_linear_version` fill commit: `commit list` after
  the upgrade equals `commit list` before it.

Stamps 10 to 13 were written by main development builds only, 13 from
2026-10-01 to 2026-10-04. Stamps 10 to 12 and every stamp below 8 stay
rebuild-only. Stamps that exist only between releases get no
conversion unless a route is written for them, as for 13; each layout change
still takes a new stamp, so a graph written in between is refused and never
misread. [RFC 0064](../rfcs/0064-explicit-storage-upgrades.md) holds the
offline protocol.

## Recovery sidecars

There is no recovery sidecar protocol to version. Builds up to the 0.11 line
wrote recovery sidecar schema v9 under `__recovery/`; since RFC 0067 no writer
arms one and no build interprets one. A read-write open refuses a graph that
still carries a sidecar until the build that wrote it has resolved it. See
[recovery.md](recovery.md).

## Lance contract

The workspace resolves the unmodified crates.io Lance package family to
**11.0.0** and explicitly writes stable data storage version **V2_2**.
Engine adapters and compatibility boundaries are documented in
[lance.md](lance.md). A dependency bump alone
does not change the OmniGraph manifest format. Adopting a new Lance file format
or a behavior that changes persisted graph meaning does.

Current compatibility fences and the required upstream reading set are in
[lance.md](lance.md).

## Rebuild

A graph at stamps 10 to 12 or below 8, and a cluster-managed graph at 8, 9 or
13, is rebuilt:
export with the binary that wrote it, relocate `data.id` into top-level `id`
in each record that has no top-level `id` (an existing one is preserved), then
`init` and `load --mode overwrite` with this binary.
Data, vectors and blobs are preserved; commit history and branches are not. See
[the upgrade guide](../user/operations/upgrade.md).

## Storage upgrade support matrix

The `storage_upgrade_compatibility` CI job (Storage Upgrade Compatibility)
builds the genuine stamp-13 CLI from main commit `c0a4519f`, installs the
released 0.10.0 and 0.11.0 CLIs, and requires the
`storage_upgrade` cases of `crossversion_upgrade.rs`, the engine
`db::upgrade::tests`, Lance version qualification and protocol guards on
every change. Missing test cases, empty runs, a missing predecessor binary
and skipped required cases fail the job.

| Source format | Normal open | `omnigraph upgrade` | Required coverage owner |
|---|---|---|---|
| Current / v14 | Accepted | `already_current` for the default or target 14; any other target is `unsupported_target` | `upgrade/tests.rs` and CLI `crossversion_upgrade.rs::storage_upgrade_current_binary_reports_already_current_on_a_fresh_graph`; stamp tests in `migrations.rs` |
| v13 (main builds 2026-10-01 to 2026-10-04), standalone | Refused, naming `omnigraph upgrade` | Converted in place: `check_passed`, then `completed`; branches and history kept | CLI `crossversion_upgrade.rs::genuine_v13_storage_upgrade_preserves_history` against the genuine `c0a4519f` binary; `upgrade/tests.rs` over `legacy::write::LegacyHistory` fixtures; `omnigraph-catalog` `legacy_` tests |
| v9 (release 0.11.x), standalone | Refused, naming `omnigraph upgrade` | Converted in place; branches and history kept, one schema contract for every pre-upgrade commit, root schema objects left in place | CLI `crossversion_upgrade.rs::genuine_v0_11_0_storage_upgrade_preserves_history` and `genuine_v0_11_0_storage_upgrade_after_predecessor_cleanup` against the released 0.11.0 binary, `genuine_v0_10_0_to_stamp_9_by_default_storage_upgrade_preserves_history` against the released 0.10.0 and 0.11.0 binaries; `upgrade/tests.rs` over `LegacyHistory::create_stamped` fixtures (`a_stamp_9_root_reached_from_6_by_the_released_upgrade_converts_its_retained_versions` for the retained stamp-6/7/8 versions); `omnigraph-catalog` `RootContractSource` tests |
| v8 (release 0.11.x, legacy system column spellings), standalone | Refused, naming `omnigraph upgrade` | Converted in place, as v9 | CLI `crossversion_upgrade.rs::genuine_v0_10_0_to_stamp_8_storage_upgrade_preserves_history` against the released 0.10.0 and 0.11.0 binaries; the stamp-8 cases of `upgrade/tests.rs` |
| v8, v9 or v13, cluster-managed | Refused | Refused before any read; export and rebuild | `crossversion_upgrade.rs::storage_upgrade_refuses_cluster_path_aliases` |
| Any other stamp | Refused | `unsupported_source` (`newer_than_binary` above 14); export and rebuild | `upgrade/tests.rs`, the format fences and the refuse-and-rebuild tests in `crossversion_upgrade.rs` |
| Pending conversion marker | Refused | `--check`: `recovery_required`; execute: resumes its own intent, refuses another's | `upgrade/tests.rs` |

These tests cover local standalone roots. Object-store backend qualification
and deployment branch-protection configuration require their own environment
evidence; a local pass is not evidence for those gates. Cluster-managed entry
points are refused
(`crossversion_upgrade.rs::storage_upgrade_refuses_cluster_path_aliases`).

## Wire compatibility

The v0.12 HTTP boundary requires `Omnigraph-Http-Api: 0.12` exactly once on
protected graph and registry requests. Authentication precedes contract admission;
missing, duplicate or unsupported values return typed 400 `api_contract_mismatch`
before graph resolution or body execution. Ordinary responses carry the same
header. Public health, readiness and OpenAPI routes remain accessible without it.
MCP and OAuth metadata retain their separate standard protocols.

The CLI checks the configured server's `HEAD /healthz` before each data request,
without credentials and with a five-second discovery bound, then validates the
actual response header before consuming its body. Discovery failure means that
request was not sent; a response mismatch after dispatch leaves effects unknown.
Graph HTTP requests follow no redirects and retry nothing automatically. See the
[HTTP admission decision](../rfcs/2026-09-30-v012-http-admission.md) and
[operator guidance](../user/operations/server.md).

Upgrade CLI, server and integrations together. The shared contract identifier is
independent of package and graph-storage versions; it establishes neither support
for another server increment nor permission to replay a write. No old-client
fallback or negotiation framework is supported. New wire changes update shared
DTOs, the contract decision, OpenAPI and transport qualification together.

Server URLs identify the service root, preserving any proxy prefix. Migrate
graph-qualified URLs to that root plus `--graph`, `default_graph` or alias `graph`;
the CLI does not infer a root by stripping path segments.

Storage upgrades and the Lance analyzer boundary remain separately owned. The
Lance 9/10 to 11 analyzer transition requires a quiesced fleet and explicit
full-text rebuilds; see the
[upgrade procedure](../user/operations/upgrade.md#full-text-index-upgrade).

## Registry publication status

crates.io publication is paused as of 2026-08 because access to the account
owning the historical `omnigraph-*` names is unavailable. Those registry
packages remain frozen at 0.8.0; `omnigraph-db` is reserved at 0.0.1 as a
fallback. Current binaries ship through the installer, Homebrew, Docker, and
GitHub Releases, and the TypeScript SDK ships through npm. Do not document
`cargo install` until this status changes.

## Changing an axis

### Graph storage

1. Write an RFC for the irreversible format decision.
2. Bump the manifest stamp. Raise `MIN_SUPPORTED` with it unless every lower
   served stamp keeps a meaning the binary reads (as v8 and v9 did for the
   system column vintages until v10 folded both under one stamp); never
   lower the floor without a real converter.
   Register explicit conversion separately from serving admission.
3. Refuse old/future formats before decoding.
4. Add genuine predecessor evidence for every declared direct or migration route,
   plus refusal and rebuild fallback evidence.
5. Update the upgrade guide and release notes.

A stamp that changes only the stored shape of `__manifest` rows, keeps the
previous stamp's logical rows and is read beside it (`MIN_SUPPORTED` unchanged)
takes a shorter path, as v12 did: no RFC, since no format decision is
irreversible while both shapes are read; conversion is each branch's next
publish, so a graph holds both stamps across its branches until every branch
has published, and every reader accepts that state; for a stamp no released binary
wrote, the synthetic conversion test is the predecessor evidence and the matrix
row says so. Steps 3 and 5 apply unchanged.

### Pins and schema contracts

1. A change to what a pin or a schema-contract row records is a manifest
   format change: it takes a stamp or contract version and the
   graph-storage checklist above.
2. A new detached transaction kind needs its twin-replay surface guard first.
3. Never infer missing lifetime identity from aliases.

### Lance

1. Read every full page in the relevant domains from [lance.md](lance.md).
2. Review the complete upstream release/source delta.
3. Run `lance_surface_guards.rs` first, then the focused engine suites and
   canonical workspace test.
4. Update only the current compatibility-fence table.
5. Put bump evidence and historical findings in the release note or RFC that
   consumed them, not an accumulating live-doc audit ledger.

### Wire and release

Regenerate OpenAPI for wire changes. Release changes update every shipped
artifact, installer/package metadata, and the surveyed version in
[AGENTS.md](../../AGENTS.md) in the same change.
