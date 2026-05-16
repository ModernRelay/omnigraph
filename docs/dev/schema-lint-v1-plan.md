# Schema-lint chassis v1 — implementation plan

Work-in-progress checklist for the next slice of the chassis. v0 (the code-tagged diagnostics layer, MR-694 first PR #87) shipped on `main`. v1 brings the chassis to **enforced behavior**: `--allow-data-loss` flag, the `Soft | Hard` mode dimension on drops, and the first real "destructive but supported" migration step.

This document tracks scope so the PR can land in incremental commits without losing the thread. Delete after the work is merged.

## Lance substrate alignment (revised 2026-05-13)

After a substrate audit against the [Lance data-evolution guide](https://lance.org/guide/data_evolution/), the v1 plan was simplified. Lance's `drop_columns()` is **already metadata-only and reversible via time travel until cleanup**:

> `drop_columns` is metadata-only and remains reversible as long as old versions are retained. After `compact_files()` rewrites data files and `cleanup_old_versions()` removes old manifests/files, removed data may become permanently unrecoverable.

This means:
- **Soft mode = `Dataset::drop_columns([name])`**. No separate `tombstoned: bool` catalog field. Lance's version graph IS the tombstone.
- **Hard mode = `drop_columns()` + `compact_files()` + `cleanup_old_versions()`** (the existing `omnigraph cleanup` pipeline).
- **No `omnigraph schema unhide` command needed**. Undo is `omnigraph snapshot --at <commit>` or `omnigraph branch create --from <commit>` — the existing time-travel surface.

Two commits dropped from the v1 plan as a result: the old commit 3 (tombstone fields on catalog IR) and commit 8 (unhide command). Net: ~250 LoC less surface, more substrate-aligned, fewer new concepts.

The broader substrate migration (using Lance native APIs across all migration steps, not just drop) is tracked in **MR-948** — out of scope for this branch but linked from each commit below.

## Done in this branch so far

- [x] **Commit 1** — `SchemaMigrationStep::diagnostic()` helper + CLI plan output displays tier alongside the code: `unsupported change on node:Person.age [OG-DS-104, destructive]: ...`. No behavior change. All 11 existing `schema_apply` tests still pass.
- [x] **Commit 2** — `DropMode { Soft, Hard }` enum + dormant `DropType` and `DropProperty` variants on `SchemaMigrationStep`. Apply path has an exhaustive-match arm returning `manifest_internal` if either variant arrives via deserialization. Serde round-trip pinned for stable wire shape.

## Next commits (in order)

### Commit 3 — Planner emits `DropProperty { Soft }` + apply calls `Dataset::drop_columns`

Replaces the earlier "tombstone fields on catalog IR" commit. No catalog IR changes needed — Lance handles the tombstone via its version graph.

- [ ] In `plan_properties`'s leftover-property branch: emit `DropProperty { Soft }` instead of `UnsupportedChange` for OG-DS-104.
- [ ] Same for node-type removal (`plan_nodes` leftover → `DropType { Soft }`, OG-DS-102) and edge-type removal (`plan_edges` leftover → `DropType { Soft }`, OG-DS-103).
- [ ] `apply_schema_with_lock` handles `DropProperty { Soft }`: calls `Dataset::drop_columns(&[property_name])` and commits via the staged-write path. **Substrate primitive: Lance metadata-only commit.**
- [ ] `apply_schema_with_lock` handles `DropType { Soft }`: marks the table tombstoned in `__manifest` (data files retained). Reversible via branch / snapshot restore.
- [ ] Recovery sidecar: standard `catalog_only` discipline — the Lance commit IS the recoverable unit.
- [ ] CLI plan output renders the new variants with mode visible.
- [ ] Integration test (extends `tests/schema_apply.rs`): remove a property, assert apply succeeds, row count preserved, current-version schema query no longer surfaces the property, prior-version time-travel query still sees it.

### Commit 4 — Convert PR #62 destructive-rejection tests

- [ ] `tests/schema_apply.rs`'s 6 PR #62 tests currently assert "removing X fails with `OG-DS-XXX`". Convert each to:
  - **Without `--allow-data-loss`**: soft drop succeeds (Lance metadata-only, rows preserved in current version, recoverable via time travel).
  - **With `--allow-data-loss`**: hard drop succeeds (column data deleted after compact + cleanup) — covered by commit 5.
- [ ] Add a new test that asserts **time-travel reversibility**: drop a column, query at prior version, verify the column is still present in that snapshot.

### Commit 5 — `--allow-data-loss` CLI flag + `Hard` mode

- [ ] Add `--allow-data-loss` boolean flag to `omnigraph schema apply` and `omnigraph schema plan` (plan shows what would happen if applied with the flag).
- [ ] Thread through to `apply_schema_with_lock(.., allow_data_loss: bool)`.
- [ ] Planner: when `--allow-data-loss` is set, emit `Hard` mode instead of `Soft` for drop paths.
- [ ] Apply path for `Hard` mode `DropProperty`: `drop_columns()` + `compact_files()` + `cleanup_old_versions()`. **Substrate primitives:** Lance's existing cleanup pipeline; same APIs `omnigraph cleanup` already uses.
- [ ] Apply path for `Hard` mode `DropType`: remove the manifest entry + drop the Lance dataset.
- [ ] Recovery sidecar discipline: `full_rewrite` (the cleanup phase is the rewrite).
- [ ] Integration tests: hard drop deletes data; without flag, hard drop is impossible (planner only emits soft).

## Open questions

- **`Hard` mode cleanup: inline vs. deferred.** Should `--allow-data-loss` on apply run `compact_files` + `cleanup_old_versions` **inline** (operator gets data deletion immediately) or **defer** to the next `omnigraph cleanup` run (operator's existing pipeline)? Recommend **inline** for ergonomic guarantee — the flag is explicit consent and operators who said "yes data loss" expect data to actually be gone after the apply returns.
- **Query-level enforcement on dropped types**: should a `match { $p: DroppedType }` query fail at parse time, lint time, or runtime? Recommend: lint warning at parse time (new code in the QL family); runtime returns empty result (Lance no longer has the column). Different from before: no "tombstoned" state to surface — the type is genuinely gone in the current version's catalog.
- **`Hard` mode for `DropType` data forensics**: dataset deletion via Lance is irreversible after `cleanup_old_versions`. Operators who want forensics should take a snapshot or tag first. Document this in `docs/schema-language.md` once this lands. No special escape hatch in the apply path.

## Not in v1 scope (deferred)

- Severity config in `omnigraph.yaml` (per-rule `error` / `warn` / `force`).
- `@allow(OG-XXX-NNN, "rationale")` suppression directives.
- Pre-migration checks (MR-941).
- CD / VE / LK / NM family rules (MR-942..MR-945).
- CI integration (MR-946).
- The remaining 12 of 17 `UnsupportedChange` paths still untagged with codes (interface removal, edge endpoint change, edge cardinality change, etc.). Each goes through its own MR-XXX issue.
- **Substrate alignment of non-drop migration steps** (Lance native `add_columns`, `alter_columns` for renames, type casts, defaults). Tracked in **MR-948**. Today's `AddProperty` and `RenameProperty` still go through `stage_overwrite`; v1 doesn't change that.

## What changed from the original plan

For posterity / reviewers comparing against the initial plan committed in commit 1:

| Was | Now | Why |
|---|---|---|
| Commit 3: `tombstoned: bool` on `NodeIR`/`EdgeIR`/`PropertyIR` | **Removed.** No catalog IR change needed. | Lance's `drop_columns()` is metadata-only; Lance's version graph is the tombstone. |
| Commit 5: apply path writes `tombstoned: true` into catalog | Apply path calls `Dataset::drop_columns([name])` | Substrate-aligned. Lance handles the metadata commit. |
| Commit 7 Hard mode: `stage_overwrite` removing the column | `drop_columns` + `compact_files` + `cleanup_old_versions` | Substrate-aligned. Reuses `omnigraph cleanup` pipeline. |
| Commit 8: `omnigraph schema unhide <name>` | **Removed.** | Time travel is the undo. `omnigraph snapshot --at <commit>` reaches the pre-drop version. |
| 8 commits total | 5 commits total | Smaller surface, fewer new concepts. |

The chassis types (`DropMode` enum, `DropType` / `DropProperty` variants — commit 2) are kept exactly as designed; only the implementation strategy changed.
