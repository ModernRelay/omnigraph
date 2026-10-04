# Versioning and compatibility

**Audience:** storage, API, and release maintainers
**Authority:** current compatibility policy

OmniGraph has independent release, wire, graph-storage, recovery, and Lance
version axes. Never derive one axis from another.

| Axis | Policy | Guard |
|---|---|---|
| Release | Published workspace artifacts move in lockstep. | Workspace manifests, lockfile, generated metadata, release automation. |
| CLI ↔ server wire | One v0.12 contract; coordinated client/server upgrades. Exact request admission and CLI discovery/response validation. | Shared contract header, DTOs, HTTP refusal tests and OpenAPI drift tests. |
| Graph storage | Closed stamp range `[MIN_SUPPORTED, CURRENT]`, independent of system column vintage; a graph at any other stamp is rebuilt by export and load; no open-time migration. | Main-manifest stamp guard on both bounds. |
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
refuses a graph at any other stamp, and this build has no in-place storage
conversion.

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
- the unreleased v7–v19 stamps of the rejected MemWAL experiment never shipped
  and are not supported migration inputs. Reuse of a numeric stamp by another
  design (RFC 0062, RFC 0042 or RFC 0040) does not make an experimental graph
  compatible. Such graphs require export with the build that wrote them and
  rebuild at a fresh root.

Normal open refuses lower and higher stamps before recovery or table decoding;
it never migrates a graph. This build has no in-place storage conversion:
`omnigraph upgrade` reads main's `__manifest` stamp, reports the storage
format state and writes nothing. A graph at stamp 14 reports
`already_current`. Any other stamp is the finding `unsupported_source` with
the open guard's refusal text, and a `--to-format` other than 14 is the
finding `unsupported_target`. A graph carrying `UPGRADE_PENDING_KEY`
(`omnigraph:storage_upgrade_pending`) reports `recovery_required` and stays
refused by normal open; the conversion is finished with the executable that
started it.

The in-place conversion is written once per release, by the release that ships
a new format, and it converts from the last released stamp only. Stamps that
exist only between releases get no conversion; each layout change still takes
a new stamp, so a graph written in between is refused and never misread.
[RFC 0064](../rfcs/0064-explicit-storage-upgrades.md) holds the offline
protocol.

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

A graph at any stamp other than 14 is rebuilt: export with the binary of the
release that wrote it, relocate each record's `data.id` into top-level `id`,
then `init` and `load --mode overwrite` with this binary. Data, vectors and
blobs are preserved; commit history and branches are not. See
[the upgrade guide](../user/operations/upgrade.md).

## Storage upgrade support matrix

The `storage_upgrade_compatibility` CI job (Storage Upgrade Compatibility)
requires the `omnigraph upgrade` report and cluster-refusal tests of
`crossversion_upgrade.rs`, Lance version qualification and protocol guards on
every change. Missing test cases, empty runs and skipped required cases fail
the job.

| Source format | Normal open | `omnigraph upgrade` | Required coverage owner |
|---|---|---|---|
| Current / v14 | Accepted | `already_current` for the default or target 14; any other target is `unsupported_target` | `upgrade/tests.rs` and CLI `crossversion_upgrade.rs::storage_upgrade_current_binary_reports_already_current_on_a_fresh_graph`; stamp tests in `migrations.rs` |
| Any other stamp | Refused | `unsupported_source`; export and rebuild | `upgrade/tests.rs`, the format fences and the refuse-and-rebuild tests in `crossversion_upgrade.rs` |
| Pending conversion marker | Refused | `recovery_required` | `upgrade/tests.rs` |

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
