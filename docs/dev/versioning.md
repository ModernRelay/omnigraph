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

The below-CURRENT refusal names the release line that wrote the stamp
(`release_for_internal_schema_version` in `db/manifest/migrations.rs`) and prints
the exact `export` / `init` / `load` commands, so the upgrade is fail-closed **and**
self-service — the operator can fetch the right old binary without guessing.

Internal schema v5 was the RFC-028 identity boundary: SchemaIR v2, its graph
identity domain and allocator, and the identity-keyed manifest journal activate
together. A v4 graph cannot be backfilled safely because its logical IDs,
registration keys, paths, versions, and tombstones are all name-derived; the
normal strand rebuild mints a fresh domain and table incarnations instead.

Internal schema v6 was a 0.9.0-dev format: like v5, v7, and v8 it was written
only by source builds off `main` during 0.9.0 development, and no published
release serves it. It preserved the v5 identity
contract and activated RFC-023 key fencing: every graph node/edge dataset
declares exactly non-null physical `id` as Lance's unenforced primary key from
creation, and production strict insert/upsert routes use the exact-`id`
filter-bearing adapter.

Internal schema v9 maps to OmniGraph **0.9.x** — the first published release
line to serve any of these formats.
It preserves v8's private data-bearing MemWAL core, then activates
RFC-026's common B2 storage/recovery contract: stream-config v3, lifecycle
state v2, the grammar-impossible trusted base-row field
`__omnigraph_stream_v1$`, one manifest-selected `_stream_tokens.lance`
authority, compare-and-chain token attribution, and recovery-v12's exact base
plus token participants. One manifest CAS still owns graph visibility. The
selected B2a profile retains every canonical MemWAL object indefinitely; it
does not claim a retained-storage bound or online GC.

This remains a private storage/recovery capability, not a public streaming
feature. V9 has no `@stream`, production SDK/HTTP/CLI surface, persistent
quiesce/resume controls, correction lane, or fresh-read surface. Its test seam
is feature-gated and doc-hidden. Row count, logical dense-slice bytes,
canonical payloads, token projections, recovery JSON, and exact-authority
lookup retention are bounded; the measured near-cap fold RSS remains evidence,
not a runtime allocator promise.

A v8 (0.9.0-dev) graph is not reinterpreted or migrated in place: config-v2 and
recovery-v11 never become config-v3/state-v2/recovery-v12 authority. Export it
with the v8 binary, initialize a different v9 root, and load through the v9
writer. The physical field's trailing `$` is outside the `.pg` identifier
grammar, so a genuine v8 user property named `__omnigraph_stream_v1` remains
ordinary user data and round-trips unchanged.

Internal schema v10 was the first 0.10.0-dev streaming-profile format. It
preserved the complete v9 contract and added RFC-026 §4.7 P1's enablement
authority: one required graph-global `stream_profile` singleton row, present
from genesis (disabled, revision 1), flipped through the shared publisher's
exact-entry CAS with a strict revision advance. The same bump added an
explicit-null fold-attribution dead-letter compatibility placeholder
(`dead_letter_object`). That incomplete v10 shape is now frozen null; the
finalized protocol uses a new versioned attribution shape rather than
activating it in place. The placeholder exists because the summary is
`deny_unknown_fields` and structurally equality-compared between the recovery
sidecar and the lineage row. The bump was forced by decode semantics, not row
volume: v9 decoders silently skip unknown row kinds, so only the stamp can make
a v9 binary refuse a streaming-capable graph instead of writing blind to the
freeze. A v9 graph crosses by export/init/load rebuild. The genuine v9↔v10
fence remains historical upgrade evidence.

Internal schema v11 replaced the v10
boolean payload with stream-profile protocol v2: a bounded receipt-chain
reference plus strict `DISABLED`, delegated `ENABLED`, planned `DISABLING`, and
receipt/cut-bound `RETIRED` states. `DISABLING` carries a drain-only fold
continuation; `RETIRED` has no outgoing transition and decodes fail-closed.
Only `cluster apply --confirm-stream-offline`, under the persisted cluster
state lock and bound to the exact graph, declaration, profile revision,
operation, and authenticated actor, may change the profile. When enabled,
ordinary Mutation/Load/delete requires the exact non-forgeable runtime
authority minted for the cluster-booted server; embedded SDK and direct
`--store` writers are refused before input reads or durable effects.
BranchMerge is refused while the profile is `ENABLED` or `DISABLING` even
through that runtime because no token-aware merge transition exists.

V11 raised the recovery-sidecar ceiling from v12 to v13 for that bounded
profile-authority tranche. `StreamProfileChange` is the only emitted and
accepted v13 discriminator. It owns the exact token-ledger
`ProfileManagementReceipt` transaction; only the terminal manifest CAS that
selects its achieved token witness and the next profile together makes either
authoritative.

Internal schema v12 was an unreleased 0.10.0-dev format. It replaces lifecycle
state-v2's inline receipt histories with lifecycle-v3 fixed-size
ledger-chain/current pointers and an authenticated WAL-tail commitment. The
recovery-sidecar ceiling is v14. Its active hidden discriminators are
`StreamEnrollmentV2`, `StreamClaim`, `StreamFoldV2`, `StreamDrainFold`, and
`StreamLifecycleReceipt`; resume/correction/retirement/ledger-maintenance/
sealed-maintenance/rebind vocabulary decodes fail-closed. Cold writer claims
are recovery-covered before Lance is invoked. Ordinary and drain folds bind
the selected current claim and exact authenticated full-generation projection.
The restartable private quiesce path handles never-written and non-empty lanes
through `OPEN → DRAINING → SEALED`.

The v14 `StreamResume` discriminator's three-field scaffold is now permanently
historical. It did not encode the complete prior lifecycle/profile/topology,
the physical claim attempt, or the two terminal receipt families required to
recover resume safely, so v13 does not reinterpret it.

Internal schema v13 was an unreleased 0.10.0-dev format. It preserves lifecycle-v3
and raises the recovery-sidecar ceiling to v15. Recovery-v15 has one active
hidden discriminator, `StreamResume`, which owns the complete revision-fenced
`SEALED → OPEN` resume or guarded `DRAINING → OPEN` abort: exact request and
actor, prior authority, a restartable physical higher-epoch claim, terminal
`ClaimReceipt` plus `ManagementReceipt`, and the sole final `OPEN` publication.
Receipt lookup precedes revision refusal for idempotent retry. The v14
resume/correction/retirement/ledger-maintenance/sealed-maintenance/rebind
scaffolds retain their old bytes and continue to fail closed.

Internal schema v14 was an unreleased 0.10.0-dev format. It raised the sidecar
ceiling to recovery-v16 for one active hidden discriminator,
`StreamSealedEnsureIndices`. V16 reuses the frozen
recovery-v8 exact CreateIndex plan and layers the enabled profile, selected
token-authority witness, and complete sorted prior/next `SEALED` lifecycle rows
around it. The table pointer, both HEAD witnesses, recomputed empty proof, and
lifecycle revision publish atomically. This capability-only operation writes
no token row, advances no receipt chain, and accepts no caller operation ID;
recovery settlement followed by convergent EnsureIndices replanning supplies
retry idempotency. Ambient EnsureIndices remains refused for enrolled tables.
Recovery-v14's sealed-maintenance
scaffold keeps its original bytes and is not reinterpreted.

Internal schema v15 was an unreleased 0.10.0-dev format. It raised the sidecar
ceiling to recovery-v17 for the distinct hidden `StreamSealedOptimize`
discriminator. V17 owns Optimize's bounded,
internally committing maintenance plan, exact confirmed outputs, and complete
sorted prior/next `SEALED` lifecycle rows. Productive table pointers and proof
refreshes publish atomically; a true no-work invocation is effect-free. It
writes no token row, advances no receipt chain, and accepts no caller operation
ID. Ambient Optimize remains refused for enrolled tables.

Internal schema v16 was an unreleased 0.10.0-dev format. It raised the sidecar
ceiling to recovery-v18 for the distinct hidden `StreamRebind` discriminator.
V18 binds the complete prior `SEALED`
authority, exact fresh physical enrollment and empty shard, immutable binding
and fence-only claim receipts, and the exact next `SEALED` row/proof. Only that
complete outcome may publish. Rebind never opens admission; a separate
recovery-v15 resume must claim a higher epoch within the fresh binding scope.
The v14 rebind scaffold and recovery-v17 Optimize envelope retain their exact
historical meanings and are never reinterpreted.

Internal schema **v17 is the currently served format** (unreleased, current
0.10.0-dev source builds). It raises the sidecar ceiling to recovery-v19 for
one active `StreamAuthorityRetirement` discriminator. Under checked
stopped/offline cluster authority, a read-only plan proves an exact `DISABLED`
profile, settled recovery, every enrolled lane `SEALED`, base/token parity, and
at least one current `WITHDRAWN` token. Confirmation appends one immutable,
actor- and plan-bound retirement receipt and then performs the sole
lineage-neutral manifest CAS selecting its token pointer and
`DISABLED → RETIRED`. It moves no graph or branch head, emits no `GraphCommit`
or `RecoveryAudit`, and has no outgoing transition. A retired source is
query/status/export-only; export re-proves the frozen logical cut and emits the
root receipt plus a closed witness binding the canonical selected branch,
exact Lance branch identifier, graph head, manifest version, and branch-member
digest (`branch_member_digest`). Authority correction remains
inactive, and this slice adds no production path that creates `WITHDRAWN`.

Recovery-v13 remains exactly the v11 profile-change protocol. Historical
recovery-v10 enrollment and recovery-v12 lifecycle-v2 folds retain their old
wire meanings and are refused under lifecycle-v3 rather than synthesized.
There is still no public firehose ingress, public production enrollment,
quiesce, resume/abort, correction, physical rebind, or streaming or maintenance
transport surface. Retirement is exposed only by the narrow offline
`cluster stream retire-for-rebuild plan|confirm` control-plane handshake. The
v15 through v18 recovery paths remain crate-private and feature-gated.

A v16 graph crosses by export/init/load rebuild into a different root. Because
`MIN_SUPPORTED == CURRENT == 17`, v17 refuses v16 and a v16 binary refuses
v17. The genuine v15↔v16 fence remains historical evidence.

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
RFC-023 follows that rule: `ErrorOutput.key_conflict` is optional, and its
`key` member remains optional on the wire for additive compatibility. The v9
engine returns `KeyConflict` only after a fresh exact-ID probe identifies an
attempted key; Lance's broader retryable conflict class is not serialized as a
key conflict without that evidence. `ErrorOutput.resource_limit` is likewise
optional and additive; v9 servers use it with HTTP 413 for pre-arm keyed-write
ceilings.

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
