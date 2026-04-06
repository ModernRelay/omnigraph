# Namespace Publisher Gap

This note captures the current spike result for replacing Omnigraph's custom
graph publisher with Lance's native namespace APIs.

## Goal

Determine whether `GraphNamespacePublisher` in
`crates/omnigraph/src/db/manifest/publisher.rs` can be replaced directly with
`DirectoryNamespace` from `lance-namespace-impls`.

## What Works Today

The Lance 4 Rust namespace surface is usable for direct per-table version
operations:

- `create_table_version`
- `list_table_versions`
- `describe_table_version`
- namespace-based table lookup

We now have an Omnigraph smoke test for that direct path in:

- `crates/omnigraph/src/db/manifest/tests.rs`
  `test_directory_namespace_direct_publish_cannot_replace_native_omnigraph_write_path`

That test shows two things:

- `DirectoryNamespace` can see only manifest-visible history through
  `list_table_versions`
- native Lance table history can advance underneath that manifest-visible view
- `DirectoryNamespace::create_table_version` cannot be dropped into the current
  Omnigraph write path after a native Lance append, because the final version
  file already exists by then

## Exact Gaps

### 1. No batch table-version API on the public Rust trait

The public `LanceNamespace` trait exposes per-table version methods, but not a
batch version publication method.

That means there is no trait-level replacement for Omnigraph's atomic
cross-table graph publish.

### 2. No branch-aware namespace handle

`DirectoryNamespace` operations are rooted at one directory namespace, but the
trait surface has no branch parameter and no branch-scoped namespace handle.

Omnigraph needs graph publication against the active graph branch because the
graph head is the active branch head of `__manifest`.

Without a branch-aware namespace API, a direct replacement would lose the
current branch semantics.

### 3. `DirectoryNamespace::create_table_version` is per-table only

The `DirectoryNamespace` implementation copies one staging manifest to a final
version location and optionally records that table version in `__manifest`.

That is useful for table-local version publication, but it is not enough for:

- atomic publication of multiple updated graph tables
- one visible graph commit point after all table work succeeds

There is also a concrete integration mismatch with Omnigraph's current write
flow: after Omnigraph performs a native table write, the target `_versions`
entry already exists. Calling `DirectoryNamespace::create_table_version` at that
point fails with an "already exists" error instead of giving Omnigraph a graph
publish hook.

## Why `GraphNamespacePublisher` Still Exists

Omnigraph still needs one custom layer because it must provide all of these at
once:

1. atomic cross-table publish
2. branch-aware publish
3. graph-visible publish only after all table-local work succeeds

That remaining layer is currently:

- `crates/omnigraph/src/db/manifest/publisher.rs`

## What Would Need To Change Upstream

The custom publisher can disappear once Lance Rust exposes a namespace API that
can express:

- batch table-version publication at the trait level
- branch-aware publication against the active `__manifest` branch
- results rich enough for Omnigraph to refresh graph-visible state without
  re-deriving semantics outside the namespace API

The generated namespace reqwest client already contains models and HTTP methods
for batch version operations. The missing piece is the public Rust trait and a
branch-aware `DirectoryNamespace` path that Omnigraph can call directly.

## Current Decision

Keep the current architecture:

- Lance owns table storage, namespace lookup, and table-local versioning
- Omnigraph owns graph-wide batch publication through `GraphNamespacePublisher`

That is the narrowest custom boundary we can justify with the Lance 4 Rust API
surface that exists today.
