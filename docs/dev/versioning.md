# Versioning and compatibility

**Audience:** storage, API, and release maintainers
**Authority:** current compatibility policy

OmniGraph has independent release, wire, graph-storage, recovery, and Lance
version axes. Never derive one axis from another.

| Axis | Policy | Guard |
|---|---|---|
| Release | Published workspace artifacts move in lockstep. | Workspace manifests, lockfile, generated metadata, release automation. |
| CLI ↔ server wire | Prefer additive changes; documented breaking release boundaries require coordinated upgrades. No global version handshake. | Shared DTOs, OpenAPI drift tests, and release-specific migration guidance. |
| Graph storage | Strict single version; rebuild across an incompatible change. | Main-manifest stamp with `MIN_SUPPORTED == CURRENT`. |
| Recovery sidecar | Independently versioned persisted protocol. | Sidecar grammar/version refusal before classification. |
| Lance dependency and file format | One deliberately pinned Lance family and explicit stable file version. | Lockfile, write parameters, and Lance surface guards. |

## Current storage contract

The current binary reads and writes exactly **internal manifest schema v6**.
`INTERNAL_MANIFEST_SCHEMA_VERSION` and `MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION`
are both 6.

- v4 was the last released pre-identity format, used by OmniGraph 0.8.x.
- v5 was an unreleased development format that introduced SchemaIR v2,
  stable table/incarnation identity, identity-keyed manifest rows, and
  identity-derived paths.
- v6 preserves v5 and adds exact non-null physical `id` fencing through
  Lance's unenforced primary-key metadata. It is the 0.9.x/0.10.x format.
- unreleased v7–v19 belonged to the rejected MemWAL experiment. They are
  abandoned future stamps, not migration inputs for a v6 binary.

A lower stamp is refused with export/rebuild guidance. A higher stamp is
refused before recovery or table decoding. There is no in-place migration
dispatcher.

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

## Why rebuild instead of migrate

An in-place migration permanently adds legacy readers, crash windows, and
version-pair tests. The present strand model keeps one readable physical shape:
export the old logical graph with the old binary, initialize a fresh current
graph, and load the export. Rows, vectors, Blob values, and schema meaning are
preserved; physical histories and stable identities intentionally restart.

The operator procedure is documented in
[the upgrade guide](../user/operations/upgrade.md).

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
2. Bump the manifest stamp and keep `MIN_SUPPORTED == CURRENT` unless a real
   converter is implemented.
3. Refuse old/future formats before decoding.
4. Add genuine old-binary/new-binary refusal and rebuild evidence.
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
