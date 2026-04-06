# Omnigraph On Lance 4

This document tracks the state of the Lance 4 migration and the namespace refactor after the cutover to `__manifest`.

## What Actually Shipped

The active runtime is no longer the older `_manifest.lance` design.

Current state:

- workspace uses Lance `4.0.0`
- repo format uses namespace-style `__manifest`
- `table_version_management=true` is enabled on `__manifest`
- reads open tables through namespace adapters
- table-local writes use a staged namespace adapter
- graph-visible publish still goes through one Omnigraph-owned batch publisher

In other words, the refactor is real, but it is not a pure handoff to the upstream Rust namespace APIs yet.

## The Current Split

### Lance owns

- native table storage
- native table version history
- branch/version checkout on individual datasets
- namespace-based table lookup for read paths
- table-local version publication for staged writes

### Omnigraph owns

- the graph publish boundary on `__manifest`
- Omnigraph-specific manifest columns like `table_key`, `row_count`, and `table_branch`
- graph commit ancestry in `_graph_commits.lance`
- run lifecycle in `_graph_runs.lance`
- merge policy and graph-level validation

That is the intended boundary for now.

For a shorter separation-of-concerns note with concrete examples, see
`docs/dev/soc-lance-manifest.md`.

## Runtime Modules

The runtime split is:

- `crates/omnigraph/src/db/manifest.rs`
  coordinator facade and snapshot shaping
- `crates/omnigraph/src/db/manifest/repo.rs`
  repo/bootstrap and historical manifest loading
- `crates/omnigraph/src/db/manifest/state.rs`
  `__manifest` row model and state reconstruction
- `crates/omnigraph/src/db/manifest/metadata.rs`
  version metadata and namespace request translation
- `crates/omnigraph/src/db/manifest/layout.rs`
  URI/layout helpers
- `crates/omnigraph/src/db/manifest/namespace.rs`
  read/write namespace adapters
- `crates/omnigraph/src/db/manifest/publisher.rs`
  atomic graph publish into `__manifest`

## What `__manifest` Means Now

`__manifest` is the graph-visible coordination table.

It stores:

- one stable `table` row per logical graph table
- one append-only `table_version` row per published version

The visible graph snapshot is reconstructed by selecting the latest visible `table_version` row per `table_key`.

This is close to the Lance Directory Namespace V2 model, but extended with Omnigraph-specific columns:

- `table_key`
- `table_branch`
- `row_count`

That keeps the existing Omnigraph snapshot contract intact while using the namespace-shaped manifest layout.

## Why `publisher.rs` Still Exists

This is the last meaningful custom storage/control-plane layer.

It exists because Omnigraph still needs all three of these properties at once:

1. atomic publication of multiple table versions
2. branch-aware publication against the current graph branch
3. a graph-visible publish point that updates only after all table-local work succeeds

The Lance 4 Rust namespace surface gives us:

- per-table `create_table_version`
- `describe_table_version`
- `list_table_versions`
- namespace-based table discovery

What it does not give us, in the public Rust surface today, is the exact batch operation Omnigraph needs:

- branch-aware `BatchCreateTableVersions` for `DirectoryNamespace`

So `GraphNamespacePublisher` currently owns only:

- validating batch publish invariants against current `__manifest` state
- building immutable `table_version` rows
- one atomic merge-insert into `__manifest`
- returning the refreshed manifest dataset

If Lance Rust exposes branch-aware batch version publication directly, this module should shrink away.

The concrete spike result and exact API mismatch are documented in
`docs/dev/namespace-publisher-gap.md`.

## What Counts As “Max Lance”

“Max Lance” does not mean zero Omnigraph logic.

It means:

- Lance owns table storage and table-local versioning
- Lance owns table lookup and native version history
- Omnigraph owns only graph semantics that are above a single Lance table

Today that remaining graph semantic is the atomic multi-table publish boundary.

That is a reasonable stopping point until the Rust namespace surface grows.

## What Did Not Change

The refactor did not replace higher-level Omnigraph concepts:

- `CommitGraph`
- `RunRegistry`
- branch merge semantics
- lazy table branching
- change detection
- point-in-time query support

Those still sit above the namespace layer and are still Omnigraph-specific.

## MemWAL

MemWAL is still out of scope for this refactor.

The current split is intentionally compatible with a later MemWAL project:

- staged table-local writes already have their own namespace boundary
- `__manifest` remains the graph publish boundary

That is the right shape for future write-path work, but this refactor does not implement MemWAL.

## Lance 5

Lance 5 makes namespaces more important strategically, but it does not change the immediate architecture:

- the biggest remaining gap is still graph-wide batch publish
- the current Rust namespace surface is still the limiting factor
- moving from Lance 4 to Lance 5 should be treated as a later SDK upgrade, not as a reason to redo this refactor

## Verification Baseline

The post-refactor acceptance bar is:

- `cargo test --workspace --locked`
- manifest state regressions around historical reads, refresh, and publish conflicts
- branch, run, CLI, server, search, and S3 tests still green

That is the baseline before any further push toward upstream namespace APIs.
