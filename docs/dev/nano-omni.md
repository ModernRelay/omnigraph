# Nano -> Omni

Roadmap and product structure for splitting Nanograph and Omnigraph into two products with two separate repositories.

---

## Goal

Create:

- **Nanograph**: the embedded local-first graph database
- **Omnigraph**: the repo-native graph database with branch/merge/clone/push/pull

The split should preserve one shared frontend/compiler layer while allowing the two engines to diverge.

---

## Current Nanograph Baseline

The Omnigraph roadmap must be written against the current Nanograph implementation, not the older archived design.

Current baseline:

- durable history is a single `_wal.jsonl`
- tx and CDC views are projected from WAL
- many CLI flows already have metadata-first / Lance-first execution paths
- graph-aware queries still retain too much row data in memory
- some mutation paths are already delta-first, but the write path is still mixed

This matters because Omnigraph is not replacing a dual-log architecture anymore. It is replacing:

- a single-db embedded API
- a single-product storage/runtime model
- Nanograph-specific identity and mutation assumptions

---

## Product Definitions

### Nanograph

Nanograph remains:

- embedded
- single-db oriented
- local-first
- optimized for direct `Database` use from CLI and SDKs
- free to keep its current identity/runtime model while it evolves

### Omnigraph

Omnigraph adds:

- repo-native API
- branch/ref/tag model
- repo-level storage coordination
- remote sync semantics
- different persisted identity model
- engine decisions optimized for repo workflows rather than embedded-db compatibility

---

## What We Should Share

Share only the compiler/frontend layer.

Recommended shared crates:

- `graph-schema`: schema AST + parser
- `graph-query`: query AST + parser
- `graph-catalog`: catalog + schema IR
- `graph-typecheck`: typed validation over the catalog
- `graph-ir`: lowered query/mutation IR
- `graph-result`: shared result/diagnostic transport helpers

These crates should be free of:

- storage layout assumptions
- repo semantics
- SDK/runtime object models
- Nanograph-specific mutation/WAL logic

---

## What We Should Not Share

Do not try to make the current backend a shared engine.

Keep product-specific:

- `store/*`
- `plan/*`
- `loader/*`
- `database::*`
- current WAL/manifest coordination
- product CLIs
- product SDK shells

The current Nanograph runtime is tightly coupled to its identity model, mutation path, and `Database` abstraction. Sharing it would slow both products.

---

## Recommended Structure

### Nanograph Repository

```text
nanograph/
  crates/
    graph-schema
    graph-query
    graph-catalog
    graph-typecheck
    graph-ir
    graph-result
    nanograph-engine
    nanograph-cli
    nanograph-ts
    nanograph-ffi
```

### Omnigraph Repository

```text
omnigraph/
  crates/
    omnigraph-repo
    omnigraph-engine
    omnigraph-cli
    omnigraph-ts
    omnigraph-ffi
```

Omnigraph depends on the shared frontend/compiler crates, but owns the repo/runtime/storage layers outright.

---

## Migration Strategy

### Phase 0: Clarify The Boundary

Deliverables:

- updated Omnigraph spec that reflects the current Nanograph baseline
- explicit statement of shared vs non-shared layers
- agreement that Omnigraph is a new product, not a rename

Exit criteria:

- no ambiguity about whether `Database` is the Omnigraph API
- no ambiguity about whether current `u64` ids are being preserved

### Phase 1: Build The Branching Prototype First

Deliverables:

- a vertical slice for `Repo -> Branch -> Snapshot`
- branch-scoped schema and catalog loading
- pinned-snapshot read execution
- branch-local writes
- basic branch diff and merge behavior

Why this phase comes first:

- branching is the highest-leverage new product boundary
- it will reveal what must change in schema loading, catalog/schema IR, runtime caches, and engine APIs
- it is likely to influence crate boundaries more than parser syntax or query semantics

Rules:

- do not rewrite the parser/typechecker up front
- do not rewrite DataFusion planning up front
- use the prototype to discover which abstractions actually need to move
- treat compiler and engine changes as evidence-driven follow-up, not assumptions

Exit criteria:

- Omnigraph has a credible branch-aware repo/snapshot prototype
- we know which schema/catalog/runtime seams are real
- we have concrete evidence for what must change in shared crates and what can remain product-specific

### Phase 2: Extract Shared Frontend Crates

Deliverables:

- move parser/typechecker/IR/catalog code into shared crates
- keep behavior unchanged
- keep Nanograph tests passing through the new crate boundaries

Rules:

- do not move runtime/storage code into shared crates
- do not change product behavior in this phase

Exit criteria:

- Nanograph builds against extracted frontend crates
- Omnigraph can depend on those crates without depending on Nanograph runtime code

### Phase 3: Finish Nanograph's On-Demand Direction

Deliverables:

- complete the current move away from full-row residency
- keep topology in memory, hydrate properties on demand
- unify remaining write paths around direct Lance mutation + delta-first internals

Why this phase matters:

- it removes the biggest old assumptions from the parts most likely to be shared by accident
- it gives Omnigraph a cleaner conceptual starting point

Exit criteria:

- Nanograph is no longer architecturally defined by full graph residency at open
- remaining backend code is clearly Nanograph-owned

### Phase 4: Introduce The Omnigraph Repo Layer

Deliverables:

- `Repo`
- `Branch`
- `Snapshot`
- refs/tags/remotes metadata model
- repo-aware CLI surface

Rules:

- do not bolt this onto Nanograph's `Database` API
- the repo layer must own branch/merge/clone/push/pull semantics

Exit criteria:

- Omnigraph has a top-level repo abstraction that does not depend on Nanograph's public API

### Phase 5: Build The Omnigraph Engine

Deliverables:

- per-type Lance tables plus manifest table
- string persisted IDs plus transient dense traversal IDs
- lazy graph index cache
- tiered hydration
- indexed search backend

Exit criteria:

- Omnigraph runs shared `.pg` / `.gq` frontend assets on its own runtime
- Omnigraph no longer relies on Nanograph backend crates

### Phase 6: SDK And CLI Productization

Deliverables:

- Omnigraph CLI
- Omnigraph TS/FFI shells as thin wrappers
- product docs that clearly separate Nanograph and Omnigraph use cases

Exit criteria:

- both products have their own top-level user story
- both products can version independently

### Phase 7: Split Repositories

Deliverables:

- Nanograph repo contains shared crates plus Nanograph product
- Omnigraph repo contains Omnigraph product
- CI and release workflows operate independently

Rules:

- split only after shared crate APIs stabilize
- avoid cross-repo circular development

Exit criteria:

- two clean repositories
- shared frontend crates remain the only intentional coupling

---

## Decision Rules

- Extract by architecture, not by file count.
- Prototype branching before finalizing shared crate boundaries.
- Share the frontend/compiler layer first.
- Keep runtime/storage product-specific unless a seam proves stable in production.
- Prefer finishing the current Nanograph runtime cleanup before forcing reuse.
- Do not let Omnigraph requirements distort Nanograph's embedded product API.

---

## Risks

### Risk: Sharing Too Much

If we share the current backend, Nanograph's old constraints become Omnigraph's constraints.

Mitigation:

- limit sharing to frontend/compiler crates
- keep repo/runtime/storage ownership separate

### Risk: Splitting Repos Too Early

If we split before the shared crate API settles, both repos will churn constantly.

Mitigation:

- do the logical split first
- do the physical repo split later

### Risk: Omnigraph Becomes "Nanograph Plus Features"

That would keep the wrong public API and identity model.

Mitigation:

- require a first-class `Repo` boundary
- require Omnigraph-owned runtime/storage crates

---

## Short Version

The path is:

1. prototype the branching model
2. share the compiler frontend based on what the prototype proves
3. keep Nanograph's engine Nanograph-specific
4. build Omnigraph as a new repo-native engine
5. split repositories only after the boundary is proven

That gives two products in two repos without forcing a fake shared backend.
