# Versioning and compatibility

**Audience:** storage, API, and release maintainers
**Authority:** current compatibility policy

Omnigraph has independent release, wire, graph-storage, recovery, and Lance
version axes. Never derive one axis from another.

| Axis | Policy | Guard |
|---|---|---|
| Release | Published workspace artifacts move in lockstep. | Workspace manifests, lockfile, generated metadata, release automation. |
| CLI ↔ server wire | One v0.14 contract; coordinated client/server upgrades. Exact request admission and CLI discovery/response validation. | Shared contract header, DTOs, HTTP refusal tests and OpenAPI drift tests. |
| Graph storage | Closed stamp range `[MIN_SUPPORTED, CURRENT]`, independent of system column vintage; explicit offline conversion, stamp-only upgrade and rebuild routes follow the [support matrix](#storage-upgrade-support-matrix), never open-time migration. | Main-manifest stamp guard on both bounds. |
| Lance dependency and file format | One deliberately pinned Lance family and explicit stable file version. | Lockfile, write parameters, and Lance surface guards. |

## Current storage contract

The current binary serves **internal manifest schema v14 to v18**, with v18
current. `MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION` is 14 and
`INTERNAL_MANIFEST_SCHEMA_VERSION` is 18. A branch below 18 is served as it
is: the first publish of this binary stamps a v14 or v15 branch 16, a publish
on any other branch writes back the stamp it loaded, and no publish stamps a
branch 17 or 18. A graph this binary creates is born at 18; an existing v14 to v17 graph
reaches 18 only by the stamp-only route of `omnigraph upgrade`. Each live branch's `__manifest`
carries one `schema_contract` row containing the accepted schema source,
compiled IR and identity. Fresh initialization writes it in the genesis
Create; schema apply and the system-column upgrade replace it in the same
publication as their table references. Normal serving has no root-file
fallback. See [Schema contract in the manifest](../decisions/2026-09-30-schema-contract-in-manifest.md).

Serving scans capture the contract texts, identity and table state together
from the exact branch and version. Contract acceptance validates the IR,
source shape and captured table identities; catalog reuse requires the same
validated image or row content. Historical queries use the accepted live
contract, with aliases rebound by stable identity to the retained table image.

What each served stamp (14 to 18) and each converted stamp (8, 9, 13) records,
and why an older binary refuses it, is the `INTERNAL_MANIFEST_SCHEMA_VERSION`
doc comment in `crates/omnigraph-catalog/src/migrations.rs`; the stored row
shape is `crates/omnigraph-catalog/src/record.rs`, the commit buffer
`CommitBuffer` in `state.rs`, and `__history` with its `legacy/` objects
`history.rs`.

Normal open refuses lower and higher stamps before recovery or table decoding;
it never migrates a graph. `omnigraph upgrade` is the one conversion, on a
standalone root, from the stamps in `UPGRADE_SOURCE_FORMATS` (8, 9 and 13).
Its route selection and findings are the module doc of
`crates/omnigraph/src/db/upgrade.rs`, the stamp-only route is
`stamp_live_refs` there, and the two kinds of source are
`LegacyManifestSource` in `crates/omnigraph-catalog/src/legacy.rs` and the
root schema objects of a stamp-8 or stamp-9 root,
`crates/omnigraph/src/db/upgrade/root_schema.rs`.

The run, in order (`crates/omnigraph/src/db/upgrade.rs`):

1. Preflight, writing nothing. With a policy installed, `SchemaApply` is
   checked once a route is chosen, and a denial is the finding
   `preflight_failed`, reported before any contract or census finding: every live ref carries the
   source stamp in the shape of its source and the contract is read (from the
   contract row at 13, from the root objects at 8 and 9), `__history/` holds nothing but
   schema archives, and
   the census (`omnigraph-catalog/src/legacy/census.rs`) reads every own
   version of every live and retired ref at its pinned head and derives the
   `HistoryRecord` of every commit. `--check` stops here with `check_passed`
   and the `UpgradeWork` counts.
2. The fence: after main's schema archive is written under
   `__history/schemas/`, one commit on main that stamps it 18, the current
   stamp, and sets the pending key
   to the `UpgradeIntent` (the live refs at their source versions, the schema
   contract, and the `LegacyPlan` with the SHA-256 of the legacy directory).
   Retired refs are not in the intent; the writer shards bind them. On a
   stamp-8 or stamp-9 source the run then retires any leftover
   `__schema_apply_lock__` ref.
3. The legacy objects, created with `put_if_absent`: data files, id shards,
   writer shards, then the directory.
4. One conversion commit per live ref, main last, leaving the head alone in
   the current (stamp-18) layout with an `omnigraph:storage_upgrade_receipt`.
5. Validation: every legacy object is read again against the directory and
   the intent, and every converted ref is compared with its source version.
6. Activation: main drops the pending key, in the commit that carries a
   `branch` row per live ref and a retired row per retired ref
   (`publish_activation`); the route then unlinks every ref file
   (`unlink_refs`), its last durable step.

A stopped run is resumed from the intent. Before the directory exists the
census is rerun at the pinned versions under the intent's `LegacyLayout` and
must plan the same directory bytes (`legacy_plan_changed` otherwise); after
it exists the directory is the plan and no census runs. Each bound is
documented on its constant: `MAX_BRANCHES` (`migrations.rs`), the head bounds
in `db/upgrade.rs`, `HISTORY_RELEASE_BYTES`, `MAX_RECORD_BYTES` and
`TAIL_BYTES` (`omnigraph-catalog`), and `MAX_CENSUS_CELLS` and
`MAX_CENSUS_SNAPSHOT_BYTES` (`legacy/census.rs`). A resume after the directory exists
verifies one data file at a time and keeps only the commit ids.

After the upgrade a commit id from before it resolves through the locator, a
full lineage read lists the legacy data files beside the native blocks, and
a numeric snapshot at a version below the upgrade serves the legacy record of
the commit at that version, or of the nearest commit below it, relabelled
with the requested version; a version of main below its genesis commit is
`manifest_not_found`. Retired refs are never converted: their heads resolve
through the writer shards.

What a converted 0.11.x root (stamp 8 or 9) does not carry over (the old
commits' schemas, table versions above a pin, table versions a 0.11.x
`cleanup` removed, `_graph_commit_recoveries.lance/`) is in
[the upgrade guide](../user/operations/upgrade.md#storage-upgrade). Every
legacy `HistoryRecord` names the contract live at the upgrade
(`UpgradeWork.schema_contents` is 1). `cleanup` deletes no version of main's
or a live branch's `__manifest` (`would_prune` is only reported; retired trees
are reclaimed separately), so a cleanup policy that retains a pre-upgrade
`__manifest` version pinning a removed table version reports that table `is
absent from the listing` and sweeps nothing for it. The route writes no `last_linear_version` fill commit: `commit list` after the
upgrade equals `commit list` before it.

Stamps 10 to 13 were written by main development builds only. Stamps 10 to 12 and every stamp below 8 stay
rebuild-only. Stamps that exist only between releases get no
conversion unless a route is written for them, as for 13; each layout change
still takes a new stamp, so a graph written in between is refused and never
misread. [RFC 0064](../decisions/0064-explicit-storage-upgrades.md) holds the
offline protocol.

## Recovery sidecars

There is no recovery sidecar protocol to version. See
[recovery.md](recovery.md#sidecars-from-older-builds).

## Lance contract

The workspace resolves the unmodified crates.io Lance package family to
**13.0.0** and explicitly writes stable data storage version **V2_2**.
Engine adapters and compatibility boundaries are documented in
[Lance compatibility fences](lance-fences.md). A dependency bump alone
does not change the Omnigraph manifest format. Adopting a new Lance file format
or a behavior that changes persisted graph meaning does.
Experimental V2_3, spilled row lineage and stable-row-ID fragment-reuse indexes
remain outside the graph contract. Retaining V2_2 alone is not a general rollback
guarantee: qualify old readers against actual new writes using the predecessor
journey in [test ownership](test-ownership.md#lance-compatibility).

Current compatibility fences are in
[Lance compatibility fences](lance-fences.md); the required upstream reading
set is in [lance.md](lance.md).

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
provisions the genuine predecessors `stamp-13`, `lance-11`, `release-0.10.0`
and `release-0.11.0` of the [predecessor registry](test-ownership.md#predecessor-binaries), and requires the
`storage_upgrade` cases of `crossversion_upgrade.rs`, the engine
`db::upgrade::tests`, Lance version qualification and protocol guards on
every change. Missing test cases, empty runs, a missing predecessor binary
and skipped required cases fail the job.

| Source format | Normal open | `omnigraph upgrade` | Required coverage owner |
|---|---|---|---|
| Current / v18 | Accepted | `already_current` for the default or target 18; any other target is `unsupported_target` | `upgrade/tests.rs` and CLI `crossversion_upgrade.rs::storage_upgrade_current_binary_reports_already_current_on_a_fresh_graph`; stamp tests in `migrations.rs` |
| v17 (release 0.16.0) | Accepted; a publish keeps it at 17, branches as ref files | `stamp-v14-to-v18`: one metadata commit per live ref, main first with the branch rows, then every ref file unlinked; `check_passed` then `completed` | `upgrade/tests.rs` (the stamp route's row and unlink tests); stamp tests in `migrations.rs` |
| v16 (releases 0.14.x and 0.15.0) | Accepted; a publish keeps it at 16 | `stamp-v14-to-v18`: one metadata commit per live ref, main first with the branch rows, then every ref file unlinked; `check_passed` then `completed` | `upgrade/tests.rs::a_v16_graph_is_stamped_current_per_live_ref` and `a_v16_branch_holding_cold_registry_fragments_is_stamped_and_keeps_them`; `omnigraph-catalog` `tests.rs::v17_a_publish_writes_back_the_stamp_it_loaded`; CLI `crossversion_upgrade.rs::genuine_lance11_storage_upgrade_round_trip`, whose predecessor reads what the candidate published |
| v15 (main builds 2026-10-10 to 2026-10-11) | Accepted; stamped 16 by its first publish | `stamp-v14-to-v18`: as v16; `check_passed` then `completed` | `upgrade/tests.rs::a_v15_graph_is_stamped_current_per_live_ref` and `a_v15_graph_is_served_and_stamped_by_its_first_publish`; stamp tests in `migrations.rs` |
| v14 (releases 0.12.x and 0.13.x; main builds 2026-10-04 to 2026-10-10) | Accepted; stamped 16 by its first publish | `stamp-v14-to-v18`: as v16; `check_passed` then `completed` | `upgrade/tests.rs::a_v14_graph_is_stamped_current_per_live_ref` and `a_live_ref_left_at_v14_is_stamped_alone`; stamp tests in `migrations.rs` |
| v13 (main builds 2026-10-01 to 2026-10-04), standalone | Refused, naming `omnigraph upgrade` | Converted in place: `check_passed`, then `completed`; branches and history kept | CLI `crossversion_upgrade.rs::genuine_v13_storage_upgrade_preserves_history` against the genuine `stamp-13` predecessor; `upgrade/tests.rs` over `legacy::write::LegacyHistory` fixtures; `omnigraph-catalog` `legacy_` tests |
| v9 (release 0.11.x), standalone | Refused, naming `omnigraph upgrade` | Converted in place; branches and history kept, one schema contract for every pre-upgrade commit, root schema objects left in place | CLI `crossversion_upgrade.rs::genuine_v0_11_0_storage_upgrade_preserves_history` and `genuine_v0_11_0_storage_upgrade_after_predecessor_cleanup` against the released 0.11.0 binary, `genuine_v0_10_0_to_stamp_9_by_default_storage_upgrade_preserves_history` against the released 0.10.0 and 0.11.0 binaries; `upgrade/tests.rs` over `LegacyHistory::create_stamped` fixtures (`a_stamp_9_root_reached_from_6_by_the_released_upgrade_converts_its_retained_versions` for the retained stamp-6/7/8 versions); `omnigraph-catalog` `RootContractSource` tests |
| v8 (release 0.11.x, legacy system column spellings), standalone | Refused, naming `omnigraph upgrade` | Converted in place, as v9 | CLI `crossversion_upgrade.rs::genuine_v0_10_0_to_stamp_8_storage_upgrade_preserves_history` against the released 0.10.0 and 0.11.0 binaries; the stamp-8 cases of `upgrade/tests.rs` |
| v8, v9 or v13, cluster-managed | Refused | Refused before any read; export and rebuild | `crossversion_upgrade.rs::storage_upgrade_refuses_cluster_path_aliases` |
| Any other stamp | Refused | `unsupported_source` (`newer_than_binary` above 18); export and rebuild | `upgrade/tests.rs`, the format fences and the refuse-and-rebuild tests in `crossversion_upgrade.rs` |
| Pending conversion marker | Refused | `--check`: `recovery_required`; execute: resumes its own intent, refuses another's | `upgrade/tests.rs` |

These tests cover local standalone roots. Object-store backend qualification
and deployment branch-protection configuration require their own environment
evidence; a local pass is not evidence for those gates. Cluster-managed entry
points are refused
(`crossversion_upgrade.rs::storage_upgrade_refuses_cluster_path_aliases`).

## Wire compatibility

The HTTP boundary requires `Omnigraph-Http-Api: 0.14` exactly once on
protected graph and registry requests. Authentication precedes contract admission;
missing, duplicate or unsupported values return typed 400 `api_contract_mismatch`
before graph resolution or body execution. Ordinary responses carry the same
header. Public health, readiness and OpenAPI routes remain accessible without it.
MCP and OAuth metadata retain their separate standard protocols.

The CLI checks the configured server's `HEAD /healthz` before each data request,
without credentials and with a five-second discovery bound, then validates the
actual response header before consuming its body. Discovery failure means that
request was not sent; a response mismatch after dispatch leaves effects unknown
([`E4`](contract.md#rules-for-every-operation)).
Graph HTTP requests follow no redirects and retry nothing automatically. See the
[HTTP admission decision](../decisions/2026-09-30-v012-http-admission.md) and
[operator guidance](../user/operations/server.md).

Upgrade CLI, server and integrations together. The shared contract identifier is
independent of package and graph-storage versions; it establishes neither support
for another server increment nor permission to replay a write. No old-client
fallback or negotiation framework is supported. New wire changes update shared
DTOs, the contract decision, OpenAPI and transport qualification together.

The [HTTP API guide](http-api.md#compatibility-and-coverage) owns the field and
SDK-name changes. The CLI access-report protocol is 2. MCP keeps its own
protocol negotiation and tool schemas; its endpoint does not use the HTTP header.
The [migration note](../../changelog.d/http-contract-014.breaking.md) owns the steps.

Server URLs identify the service root, preserving any proxy prefix. Migrate
graph-qualified URLs to that root plus `--graph`, `default_graph` or alias `graph`;
the CLI does not infer a root by stripping path segments.

Storage upgrades and the Lance analyzer boundary remain separately owned. A full-text index without a compatibility certificate requires an explicit
rebuild before search; see the
[upgrade procedure](../user/operations/upgrade.md#full-text-index-upgrade).

## Registry publication status

crates.io publication is paused because access to the account
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

### Pins and schema contracts

1. A change to what a pin or a schema-contract row records is a manifest
   format change: it takes a stamp or contract version and the
   graph-storage checklist above.
2. A new detached transaction kind needs its twin-replay surface guard first.
3. Never infer missing lifetime identity from aliases.

### Lance

Follow the [dependency bump checklist](lance.md#dependency-bump-checklist).

### Wire and release

Regenerate OpenAPI for wire changes. Release changes update every shipped
artifact and installer/package metadata in the same change.
