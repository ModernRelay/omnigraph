# Versioning and compatibility

**Audience:** storage, API, and release maintainers
**Authority:** current compatibility policy

OmniGraph has independent release, wire, graph-storage, recovery, and Lance
version axes. Never derive one axis from another.

| Axis | Policy | Guard |
|---|---|---|
| Release | Published workspace artifacts move in lockstep. | Workspace manifests, lockfile, generated metadata, release automation. |
| CLI ↔ server wire | Prefer additive changes; documented breaking release boundaries require coordinated upgrades. No global version handshake. | Shared DTOs, OpenAPI drift tests, and release-specific migration guidance. |
| Graph storage | Current-format serving; explicit registered upgrades, otherwise rebuild. | Main-manifest stamp with `MIN_SUPPORTED == CURRENT`. |
| Recovery sidecar | Independently versioned persisted protocol. | Sidecar grammar/version refusal before classification. |
| Lance dependency and file format | One deliberately pinned Lance family and explicit stable file version. | Lockfile, write parameters, and Lance surface guards. |

## Current storage contract

Normal graph open and new writes require **internal manifest schema v7**.
`INTERNAL_MANIFEST_SCHEMA_VERSION` and `MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION`
are both 7.

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
- the unreleased v7–v19 stamps of the rejected MemWAL experiment never shipped
  and were never migration inputs; v7 is reused by RFC 0062. A graph stamped
  v8–v19 by that experiment is refused as a future stamp; the refusal's
  "upgrade omnigraph" advice cannot be satisfied for it, and such a graph is
  rebuilt from an export taken with the build that wrote it.

Normal open refuses lower and higher stamps before recovery or table decoding.
`omnigraph upgrade` explicitly converts supported standalone v6 graphs to v7.
It preserves original retained snapshots and decodes their v6 registrations
explicitly after main-root admission. A pending upgrade marker refuses normal
opens until every branch validates and main activation completes.
See [RFC 0064](../rfcs/0064-explicit-storage-upgrades.md) for the offline protocol.

## Recovery version

Active graph writers emit **recovery sidecar schema v9**. The retained
writer-payload field names refer to earlier payload designs but the outer
artifact is v9 and every table slot carries stable lifetime identity.

Never change the recovery ceiling merely because the manifest schema changes,
or lower it to match v6. See [recovery.md](recovery.md).

## Lance contract

The workspace resolves the complete Lance package family to **11.0.0** and
explicitly writes stable data storage version **V2_2**. A dependency bump alone
does not change the OmniGraph manifest format. Adopting a new Lance file format
or a behavior that changes persisted graph meaning does.

Current compatibility fences and the required upstream reading set are in
[lance.md](lance.md).

## Registered conversion and rebuild fallback

The registered v6-to-v7 handler appends manifest metadata and retains table
files, branch ancestry, commit IDs and historical locators. Operators must stop
all writers and maintenance and preserve a restorable backup before execution.
Cluster-managed conversion is refused until its admission protocol is qualified.

Other source formats still require export with the source executable, fresh
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

| Source executable / format | Normal open | Explicit route | Required case in `crossversion_upgrade.rs` |
|---|---|---|---|
| 0.9.0 / v6 | Refused | v6 → v7 | `genuine_v09_explicit_storage_upgrade_preserves_history` |
| 0.10.0 / v6 | Refused | v6 → v7 | `genuine_v010_explicit_storage_upgrade_preserves_history` |
| Current / v7 | Accepted | Already-current no-op | Both migration journeys, after conversion |
| Older or unknown / not v6 or v7 | Refused | No route; source-compatible export/rebuild | Existing format fences and engine refusal tests |

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
2. Bump the manifest stamp and keep normal-open `MIN_SUPPORTED == CURRENT`.
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
