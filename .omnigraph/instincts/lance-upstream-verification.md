---
id: lance-upstream-verification
trigger: "when modifying storage code, dataset access, Lance schema conversion, or index operations"
confidence: 0.95
domain: storage
source: architectural-invariant
action: "Inspect upstream Lance implementations and docs/dev/lance.md before modifying storage or dataset code"
---

# Upstream Lance Source Inspection Discipline

## Action
Before implementing or modifying any code that interfaces with Lance datasets, readers, writers, fragment operations, or scalar/vector indexes, always read `docs/dev/lance.md` and fetch/inspect the actual upstream Lance implementation source. Never assume internal behaviors.

## Explanation & Invariant
Omnigraph coordinates versioned Lance datasets with specific guarantees (detached-only commits, single manifest publication door, strict schema identity). Lance APIs and internal assumptions (such as deletion vector application, batch schema alignment, and object store write semantics) evolve across versions.

Guessing Lance API contracts or internal behavior without reading upstream implementation frequently introduces silent data corruption risks, broken deletion vector handling, or violated invariant bounds.

## Rules of Engagement
1. Read the matching domain page listed in `docs/dev/lance.md`.
2. Inspect the exact Lance crate version pinned in the workspace `Cargo.toml`.
3. Check upstream implementation files (via `curl` or docs) for the exact method contracts, concurrency rules, and error return types.
4. Verify all detached commit and fragment assumptions against existing tests in `crates/omnigraph/tests/lance_surface_guards.rs`.
