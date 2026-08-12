# Versioning & compatibility policy

**Audience:** engine / storage / release maintainers
**Status:** living document

OmniGraph has four independent version axes. They have different compatibility
contracts and must not be conflated.

| Axis | Policy | Mechanism |
|---|---|---|
| **Release (semver)** | All published crates move in lockstep. | A release bump updates every crate manifest, `Cargo.lock`, generated API metadata, and the surveyed version in [AGENTS.md](../../AGENTS.md). |
| **CLI ↔ server wire** | Additive and rolling-safe; no version gate. | Optional JSON DTO fields plus the OpenAPI drift test. |
| **Storage (internal manifest schema)** | Strict single version; upgrade by export/init/load, never in-place migration. | `omnigraph:internal_schema_version` metadata plus `refuse_if_stamp_unsupported`, with `MIN_SUPPORTED == CURRENT`. |
| **Lance on-disk format** | Pinned to one Lance version and bumped deliberately. | `data_storage_version: V2_2` at write sites plus the checks in [lance.md](lance.md). |

## Current storage contract

The current binary reads and writes exactly **internal manifest schema v6**.

- **v4** is the last released format, shipped by OmniGraph v0.8.x.
- **v5** is an unreleased development format that introduced SchemaIR v2,
  immutable stable-table/incarnation identity, identity-keyed manifest rows,
  and identity-derived table paths.
- **v6** is the current format. It preserves v5 and makes every graph table's
  exact non-null physical `id` field Lance's unenforced primary key; supported
  strict insert/upsert writers use the exact-`id`, filter-bearing adapter.
- **v7-v19** were unreleased development formats belonging to the rejected
  RFC-026 MemWAL experiment. They are abandoned and are not compatibility
  obligations. The v6 binary refuses them as future formats before recovery or
  table decoding.

The exact v6 meaning is the one established immediately before RFC-026. A fresh
v6 root has no `_stream_tokens.lance`, `_mem_wal`, stream manifest rows,
stream profile, hidden stream metadata column, fold attribution, or stream
recovery protocol.

Recovery sidecars use a separate version space. The ordinary graph writers emit
**recovery sidecar schema v9** for identity-aware Mutation/Load, BranchMerge,
SchemaApply, EnsureIndices, and Optimize recovery. Do not lower that number to
6 merely because the manifest schema is v6.

The rationale and historical links are in
[Streaming ingestion after RFC-026](wal-removal.md).

## Why storage is strict-single-version

`Omnigraph::open` reads main's manifest stamp before decoding graph or recovery
state:

- a stamp below v6 is refused with rebuild guidance;
- a stamp above v6 is refused with an upgrade-binary message;
- an absent stamp on a manifest with the modern (v5+) column layout is refused
  as either an interrupted older-binary init (those binaries stamped in a
  separate commit after creating `__manifest`) or damaged/externally modified
  metadata. The remaining metadata cannot distinguish those cases, so the
  guard fails closed. It advises deletion and re-init only when the operator
  independently knows initialization never completed; otherwise it says to
  preserve the root for investigation or recovery. An absent stamp on a
  pre-modern layout is the genuine pre-stamp world, treated as v1; a stamp
  that is present but not a version number is refused naming the raw value.
  Current binaries cannot produce the interrupted-init state: the `__manifest`
  Create commit is the manifest's entire birth — entries, genesis lineage, and
  the stamp ride that single commit, so the stamp is atomic with manifest
  birth.

There is no in-place migration dispatcher. A released v4 graph is exported with
its v0.8.x binary, initialized as a fresh v6 graph, and loaded through the
current writer. That rebuild preserves logical rows, vectors, blobs, and schema
shape while intentionally starting fresh physical histories and identities.

This is a liability decision. A migration framework permanently multiplies
legacy readers, crash paths, and version-pair tests. The stamp guard is the seam
for a future converter if a concrete deployment justifies that cost.

## Gating altitude

The stamp is a graph-wide property and is checked on main. Branches inherit the
format when created. A branch with a different stamp is reachable only through
unsupported concurrent multi-version writers.

Format refusal must happen before recovery-sidecar or table decoding. In
particular, the current binary never tries to interpret abandoned v7-v19 state
as v6 and never cleans it opportunistically.

## Why the wire is additive

CLI and server versions roll independently, so their JSON boundary remains
additive. New fields are optional, old clients ignore unknown fields, and the
OpenAPI drift test guards unintended breaking changes. Storage strictness does
not justify a wire-version gate.

## When changing an axis

- **Storage format:** bump the manifest version, keep
  `MIN_SUPPORTED == CURRENT` unless a real migration is introduced, update the
  format history and release notes, and add genuine-binary refusal/rebuild
  evidence for a released boundary.
- **Recovery grammar:** bump the recovery-sidecar ceiling only when persisted
  recovery meaning changes. Never derive it from the manifest version.
- **Wire:** keep changes additive and regenerate `openapi.json`.
- **Lance:** on a bump, run `lance_surface_guards.rs` first (see
  [testing.md](testing.md)), review every intervening upstream commit, then
  refresh [lance.md](lance.md)'s index and add a dated audit stanza in the
  same change.
- **Release:** update all published crates and generated metadata in lockstep.

## Registry publication status

crates.io publication is **paused** as of 2026-08. Access to the account
owning the historical `omnigraph-*` crate names was lost, so those names are
frozen at their 0.8.0 versions and no current release publishes to the
registry; recovery of the account is being pursued. The name `omnigraph-db`
is reserved (0.0.1) as a fallback. Binaries ship via the installer, Homebrew,
Docker, and GitHub Releases; the TypeScript SDK ships via npm. Docs must not
instruct users to `cargo install` until this paragraph is updated.
