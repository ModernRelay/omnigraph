# Versioning & compatibility policy

**Audience:** engine / storage / release maintainers
**Status:** living document

Omnigraph has four independent version axes. They have different compatibility
contracts because they fail in different ways and at different costs. Conflating
them (for example, treating a storage-format change like a wire change) is how you
either ship an unsafe silent-misread or carry migration code you do not need.

| Axis | Policy | Mechanism |
|---|---|---|
| **Release (semver)** | All published crates move in lockstep. | Maintenance-contract rule 4 in [AGENTS.md](../../AGENTS.md): a release bump updates every crate manifest, `Cargo.lock`, `openapi.json`, and the surveyed version line together. |
| **CLI ↔ server wire** | Additive and rolling-safe; **no version gate**. New fields are optional; old clients ignore unknown fields and omit new ones. | Additive JSON DTOs in `omnigraph-api-types`; the OpenAPI-drift test (`crates/omnigraph-server/tests/openapi.rs`) catches an unintended wire change. |
| **Storage (internal manifest schema)** | **Strict single version**; upgrade is a cutover via export/import, never an in-place migration. | A stamp (`omnigraph:internal_schema_version`) in `__manifest`'s schema metadata + `refuse_if_stamp_unsupported`, with `MIN_SUPPORTED == CURRENT`. |
| **Lance on-disk format** | Pinned to one Lance version; bumped deliberately with the engine. | `data_storage_version: V2_2` at every write site + the surface guards in [lance.md](lance.md), re-run on every Lance bump. |

## Why storage is strict-single-version (the strand model)

The internal-schema stamp gates the on-disk shape of `__manifest`. The contract is:
**this binary reads exactly one internal-schema version.** `Omnigraph::open` (both
read-write and read-only) reads main's stamp before any data and refuses anything
it cannot serve:

- a stamp **below** CURRENT → refused with a rebuild-via-export/import message (see
  [the upgrade guide](../user/operations/upgrade.md));
- a stamp **above** CURRENT → refused with "upgrade omnigraph", so an old binary
  cannot silently misread a newer format.

There is no in-place migration dispatcher. The single source file
`db/manifest/migrations.rs` holds only the version constant, the stamp read/write,
and `refuse_if_stamp_unsupported`.

This is a liability decision, not a limitation we have not gotten around to. In-place
migration code is permanent surface: every future format change has to write,
test, and keep working a `vN → vN+1` step, plus the legacy readers and crash-recovery
paths each step needs, for a storage format that is still pre-release and changing.
The strand model trades that ongoing cost for a one-time operator action (export +
import) when a format changes. Per "engineering is programming integrated over time"
(see [AGENTS.md](../../AGENTS.md)), the lower-liability option is to **not** carry
the machinery until a concrete graph demands it.

The stamp + `refuse_if_stamp_unsupported` floor is exactly the seam a future in-place
migration would re-introduce: re-add a dispatcher and lower `MIN_SUPPORTED` below
CURRENT for the versions it can actually walk forward. Until then that machinery is
deliberately absent.

### Gating altitude

The stamp is validated at the **graph (main) level**: `Omnigraph::open` checks main
once, and branch reads trust it. The stamp is a graph-wide storage-format property
(the upgrade path is a whole-graph export/import), so with one binary version every
branch is always CURRENT — init stamps main, `create_branch` forks the stamp, and the
publisher writes rows without re-stamping. A branch stamped out of range while main
stays in range is only reachable with concurrent multi-version writers, an
unsupported topology; the residual is recorded as a known gap in
[invariants.md](invariants.md).

## Why the wire is additive-rolling-safe instead

The CLI↔server boundary is the opposite case: clients and servers are deployed
independently and a hard gate there would force lockstep redeploys for every field
addition. So that axis is additive — old and new coexist — and the OpenAPI-drift test
is the guard that a change stayed additive rather than breaking the shape.

## When you change each axis

- **Storage format**: bump `INTERNAL_MANIFEST_SCHEMA_VERSION`, keep
  `MIN_SUPPORTED == CURRENT` (unless you are re-introducing migration), update the
  stamp history on the constant's doc-comment, and add a release note pointing at
  the upgrade guide. The change is breaking by construction — pre-bump graphs are
  refused.
- **Wire**: keep it additive; regenerate `openapi.json`
  (`OMNIGRAPH_UPDATE_OPENAPI=1`); do not add a version gate.
- **Lance**: follow the Lance-bump checklist in [lance.md](lance.md) — re-run the
  surface guards first, then `cargo test --workspace` (a clean build is not a clean
  alignment).
- **Release**: lockstep per the maintenance contract.
