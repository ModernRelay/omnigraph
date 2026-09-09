# Versioning and compatibility

**Audience:** storage, API, and release maintainers
**Authority:** current compatibility policy

OmniGraph has independent release, wire, graph-storage, recovery, and Lance
version axes. Never derive one axis from another.

| Axis | Policy | Guard |
|---|---|---|
| Release | Published workspace artifacts move in lockstep. | Workspace manifests, lockfile, generated metadata, release automation. |
| CLI ↔ server wire | Prefer additive changes; documented breaking release boundaries require coordinated upgrades. No global version handshake. | Shared DTOs, OpenAPI drift tests, and release-specific migration guidance. |
| Graph storage | Closed stamp range `[MIN_SUPPORTED, CURRENT]`, one value per system column vintage; explicit registered upgrades into the floor, otherwise rebuild; no open-time migration. | Main-manifest stamp guard on both bounds. |
| Recovery sidecar | Independently versioned persisted protocol. | Sidecar grammar/version refusal before classification. |
| Lance dependency and file format | One deliberately pinned Lance family and explicit stable file version. | Lockfile, write parameters, and Lance surface guards. |

## Current storage contract

The current binary serves **internal manifest schema v8 and v9**:
`MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION` is 8 and
`INTERNAL_MANIFEST_SCHEMA_VERSION` is 9. The two values are the two system
column vintages of [RFC 0040](../rfcs/0040-system-column-namespace.md): a
supported existing graph with legacy spellings `id`/`src`/`dst` retains stamp 8,
and every registered upgrade route ends there. New graphs use
`__id`/`__src`/`__dst` and stamp 9. The stamp is a storage-format fence for
older binaries; the vintage itself is read from the schema IR's feature set.

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
  reclaiming their storage. Qualified v7 graphs have an explicit metadata-only
  upgrade to v8.
- v9 preserves v8's `__manifest` layout and retirement metadata and marks the
  RFC 0040 system column spellings `__id`/`__src`/`__dst` in every node and
  edge table. A v8 graph retains its stamp. RFC 0040 defines an in-place
  upgrade in Rollout step 3, which is not available in this build.
- the unreleased v7–v19 stamps of the rejected MemWAL experiment never shipped
  and are not supported migration inputs. Reuse of a numeric stamp by another
  design (RFC 0062, RFC 0042 or RFC 0040) does not make an experimental graph
  compatible. Such graphs require export with the build that wrote them and
  rebuild at a fresh root.

Normal open refuses lower and higher stamps before recovery or table decoding;
neither served stamp is rewritten on open.
`omnigraph upgrade` defaults to v8: qualified standalone v6 graphs run the
registered v6 → v7 → v8 route, and qualified v7 graphs run v7 → v8. No route
targets v9. Original retained snapshots remain unchanged; historical v6
registrations use an explicit legacy decoder after main-root admission. A
pending upgrade marker refuses normal opens until every branch validates and
main activation completes. Explicit `--to-format 7` retains the intermediate
target for a v7-compatible executable; the current binary still refuses normal
open of that result.
See [RFC 0064](../rfcs/0064-explicit-storage-upgrades.md) for the offline protocol.

## Recovery version

Active graph writers emit **recovery sidecar schema v9**. The retained
writer-payload field names refer to earlier payload designs but the outer
artifact is v9 and every table slot carries stable lifetime identity.

Never change the recovery ceiling merely because the manifest schema changes,
or lower it to match v6. See [recovery.md](recovery.md).

## Lance contract

The workspace resolves the unmodified crates.io Lance package family to
**11.0.0** and explicitly writes stable data storage version **V2_2**.
Engine adapters and compatibility boundaries are documented in
[lance.md](lance.md). A dependency bump alone
does not change the OmniGraph manifest format. Adopting a new Lance file format
or a behavior that changes persisted graph meaning does.

Current compatibility fences and the required upstream reading set are in
[lance.md](lance.md).

## Registered conversion and rebuild fallback

The registered v6-to-v7 handler converts registration metadata; v7-to-v8 only
changes manifest configuration metadata. Both retain table files, branch
ancestry, commit IDs and historical locators. A pending v6-to-v7 attempt keeps
its original protocol, target and ownership until it completes; a request for
v8 then runs the next handler. Read-only checks of v6-to-v8 report output-dependent
checks for the second handler as deferred, and execution runs them before its
effects. Operators must stop all writers and maintenance and preserve a
restorable backup before execution.
Cluster-managed conversion is refused until its admission protocol is qualified.

Other source formats still require export with the source executable,
relocation of each record's `data.id` into top-level `id`, fresh
initialization and load. Rebuild preserves logical values but intentionally
restarts physical history and identities. See
[the upgrade guide](../user/operations/upgrade.md).

## Storage upgrade support matrix

The `storage_upgrade_compatibility` CI job (Storage Upgrade Compatibility)
requires genuine predecessor migration and admission regressions, engine
conversion/recovery tests, Lance version qualification and protocol guards on
every change. Missing binaries, missing test cases, empty runs and skipped
required cases fail the job. The binaries are version-checked before fixture
creation. Branch naming without a post-fork logical-name witness is refused;
see the [admission limits](../user/operations/upgrade.md).

| Source executable / format | Normal open | Default explicit route | Required coverage owner |
|---|---|---|---|
| 0.9.0 / v6 | Refused | v6 → v7 → v8 | `crossversion_upgrade.rs::genuine_v09_explicit_storage_upgrade_preserves_history` |
| 0.10.0 / v6 | Refused | v6 → v7 → v8 | `crossversion_upgrade.rs::genuine_v010_explicit_storage_upgrade_preserves_history` |
| Qualified development / v7 | Refused | v7 → v8 | Engine storage-upgrade tests: metadata-only conversion, history and retry |
| Legacy vintage / v8 | Accepted | Already-current no-op | Both predecessor journeys after conversion; engine retired-ref admission tests |
| Current / v9 | Accepted | No route; already the newest vintage | Engine legacy-column and stamp tests (`legacy_columns.rs`, `migrations.rs`) |
| Older, future or unqualified experimental format | Refused | No route; source-compatible export/rebuild | Existing format fences and engine refusal tests |

Source v6/v7 admission rejects any reserved native-ref retirement metadata.
v8 no-op admission validates retirement markers and excludes valid
retired refs from the logical branch census while preserving physical ancestry.
An active v7-to-v8 attempt cannot be resumed as target v7.

These journeys cover local standalone roots. Object-store backend qualification
and deployment branch-protection configuration require their own environment
evidence; a local pass is not evidence for those gates. Cluster-managed entry
points remain refused. Changing a declared route requires updating its fixture,
refusal expectations and this matrix together, with storage-maintainer review.

## Wire compatibility

Prefer additive wire changes so compatible CLI and server releases can roll
independently:

- new request fields are optional or have a server-side default;
- new response fields do not change existing field meaning;
- enum growth must be represented in a rolling-safe shape when old clients use
  closed switches;
- intentional API changes regenerate and commit `openapi.json`.

Do not infer wire compatibility from a shared graph-storage version. The
v0.9/v0.10 boundary deliberately removes legacy graph-facing field names and
public aliases; it is **not rolling-safe**. Upgrade CLI, server, and client
integrations together according to the [v0.10 release notes](../releases/v0.10.0.md).
The Lance 9/10 to 11 analyzer transition separately requires a quiesced fleet
and explicit full-text rebuilds; see [the upgrade procedure](../user/operations/upgrade.md#full-text-index-upgrade).

Future incompatible wire changes must identify the affected release boundary,
document the consumer migration, and test fail-closed behavior where an older
server could otherwise ignore a new write precondition. There is no global
wire-version handshake; storage strictness is not a reason to add one.

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
   served stamp keeps a meaning the binary reads (as v8 and v9 do for the
   system column vintages); never lower the floor without a real converter.
   Register explicit conversion separately from serving admission.
3. Refuse old/future formats before decoding.
4. Add genuine predecessor evidence for every declared direct or migration route,
   plus refusal and rebuild fallback evidence.
5. Update the upgrade guide and release notes.

### Recovery

1. Bump only when persisted ownership or classification meaning changes.
2. Keep writer-kind validation exhaustive.
3. Add malformed, old, future, crash, roll-forward, and compensation tests.
4. Never infer missing lifetime identity from aliases.

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
