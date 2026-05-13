# Schema-lint chassis v1 — implementation plan

Work-in-progress checklist for the next slice of the chassis. v0 (the code-tagged diagnostics layer, MR-694 first PR #87) shipped on `main`. v1 brings the chassis to **enforced behavior**: `--allow-data-loss` flag, the `Soft | Hard` mode dimension on drops, and the first real "destructive but supported" migration step.

This document tracks scope so the PR can land in incremental commits without losing the thread. Delete after the work is merged.

## Done in this branch so far

- [x] `SchemaMigrationStep::diagnostic()` helper — returns the full `DiagnosticCode` (family + tier + severity) for `UnsupportedChange` steps with codes.
- [x] CLI `omnigraph schema plan` output displays tier alongside the code: `unsupported change on node:Person.age [OG-DS-104, destructive]: ...`.
- [x] No behavior change. All 11 existing `schema_apply` tests still pass.

## Next commits (in order)

### Commit 2 — `Soft` / `Hard` mode enum on drop steps

- [ ] Add `DropMode { Soft, Hard }` enum in `schema_plan.rs`. Tier follows the mode: `Soft → safe` (catalog tombstone, no data touched), `Hard → destructive` (Lance-level removal).
- [ ] Add new `SchemaMigrationStep` variants:
  - `DropProperty { type_kind, type_name, property_name, mode }`
  - `DropType { type_kind, name, mode }`
- [ ] Existing `UnsupportedChange` emission paths for property/type removal keep emitting `UnsupportedChange` until the planner is wired in commit 3. New variants are dormant.
- [ ] Unit test: the variants serialize/deserialize cleanly (SchemaIR round-trip).

### Commit 3 — Tombstone fields on catalog IR

- [ ] Add `tombstoned: bool` (default `false`, `#[serde(default)]` for backward compat) to `NodeIR`, `EdgeIR`, `PropertyIR`. Same on the `Catalog` types if needed.
- [ ] Readers (`build_catalog`, `build_catalog_from_ir`) propagate the flag.
- [ ] Manifest read path filters tombstoned items from the query-visible surface — they exist on disk, not in `Catalog`'s public iteration.
- [ ] Unit tests: a tombstoned property is invisible to `node_type.properties.get(...)`; the underlying Arrow column survives the rewrite.

### Commit 4 — Planner emits `DropProperty { Soft }` by default

- [ ] In `plan_properties`'s leftover-property branch: emit `DropProperty { Soft }` instead of `UnsupportedChange` for OG-DS-104.
- [ ] Same shape for node-type removal (`plan_nodes` leftover branch → `DropType { Soft }`, OG-DS-102) and edge-type removal (`plan_edges` leftover → `DropType { Soft }`, OG-DS-103).
- [ ] CLI plan output: render the new variants with mode visible.

### Commit 5 — Apply path implements `Soft` mode

- [ ] `apply_schema_with_lock` handles `DropProperty { Soft }`: writes `tombstoned: true` into the catalog/IR, no data manipulation.
- [ ] Same for `DropType { Soft }`.
- [ ] Recovery sidecar discipline: register a `RecoveryKind::CatalogOnly` sidecar entry; soft drops are roll-forward-safe.
- [ ] Integration test (extends `tests/schema_apply.rs`): remove a property, assert apply succeeds, row count preserved, schema query no longer surfaces the property.

### Commit 6 — Convert PR #62 destructive-rejection tests

- [ ] `tests/schema_apply.rs`'s 6 PR #62 tests currently assert "removing X fails with `OG-DS-XXX`". Convert each to:
  - **Without `--allow-data-loss`**: soft drop succeeds (catalog tombstone, rows preserved).
  - **With `--allow-data-loss`**: hard drop succeeds (rows deleted) — covered by commit 7.
- [ ] Add new tests for the supported soft path (verify tombstone state).

### Commit 7 — `--allow-data-loss` CLI flag + `Hard` mode

- [ ] Add `--allow-data-loss` boolean flag to `omnigraph schema apply` and `omnigraph schema plan` (plan shows what would happen if applied with the flag).
- [ ] Thread through to `apply_schema_with_lock(.., allow_data_loss: bool)`.
- [ ] Planner: when `--allow-data-loss` is set, emit `Hard` mode instead of `Soft` for drop paths.
- [ ] Apply path for `Hard` mode: `stage_overwrite` removing the column (DropProperty) or `Dataset::restore`-style table drop (DropType). Full recovery-sidecar discipline (`RecoveryKind::FullRewrite`).
- [ ] Integration tests: hard drop deletes data; without flag, hard drop is impossible (planner only emits soft).

### Commit 8 — Tombstone unhide / restore command (optional)

- [ ] `omnigraph schema unhide <type-or-property>` reverses a soft drop within a grace period.
- [ ] Tests: tombstone → unhide → property is queryable again with rows intact.
- [ ] Defer if scope creeps; can ship in a follow-up PR.

## Open questions

- **Tombstone visibility in `omnigraph schema show`**: should the CLI list tombstoned items by default (with a flag indicator), or hide them by default? Recommend: hide by default; `--include-tombstoned` reveals.
- **Query-level enforcement on tombstoned types**: should a `match { $p: TombstonedType }` query fail at parse time, lint time, or runtime? Recommend: lint warning at parse time (new code in the QL family); runtime falls back to empty result.
- **Hard-drop semantics for `DropType`**: drop the Lance dataset entirely vs. retain on disk for forensics. Recommend: dataset deletion via Lance API (the `--allow-data-loss` flag justifies it; operators can take a snapshot first via `branch_create` if they want forensics).

## Not in v1 scope (deferred)

- Severity config in `omnigraph.yaml` (per-rule `error` / `warn` / `force`).
- `@allow(OG-XXX-NNN, "rationale")` suppression directives.
- Pre-migration checks (MR-941).
- CD / VE / LK / NM family rules (MR-942..MR-945).
- CI integration (MR-946).
- The remaining 12 of 17 `UnsupportedChange` paths still untagged with codes (interface removal, edge endpoint change, edge cardinality change, etc.). Each goes through its own MR-XXX issue.
