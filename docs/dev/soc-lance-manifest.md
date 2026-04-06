# Separation Of Concerns: Lance vs Omnigraph Manifest

This note explains the current storage/control-plane split.

Short version:

- Lance owns table-local storage and table-local versioning.
- Omnigraph owns graph-visible publish semantics.
- `__manifest` is the boundary between those two layers.

## Core Idea

Think of Lance as owning the history of each individual table, while Omnigraph
owns the answer to one higher-level question:

> Which table versions make up the visible graph right now?

That answer lives in `__manifest`.

## Who Owns What

| Concern | Lance native API owns | Omnigraph custom manifest owns | Could move to Lance later? |
| --- | --- | --- | --- |
| Physical table storage | Node/edge datasets, fragments, manifests, `_versions`, indices | No | No |
| Table-local version history | Native version IDs and version files for one table | No | Already native |
| Table lookup for reads | Namespace-based lookup and per-table open | No | Already native |
| Table-local staged writes | Creating a new native version for one table | No | Already native |
| Table-local version metadata | Manifest path, etag, size, version metadata for one table | No | Already native-ish |
| Graph-visible table selection | No | Yes, via `__manifest` rows | Possibly, with richer namespace APIs |
| Atomic multi-table publish | No public Rust API for this | Yes | Yes, if Rust gets batch table-version publish |
| Branch-aware graph publish | No branch-aware namespace handle today | Yes | Yes, if namespaces become branch-aware |
| Omnigraph manifest fields | No | `table_key`, `table_branch`, `row_count` | Partially, but these remain graph-specific |
| Graph commit ancestry | No | `_graph_commits.lance` | Unlikely |
| Run lifecycle | No | `_graph_runs.lance` | Unlikely |

## Example 1: A Table Write

Suppose a mutation updates `node:Person`.

Lance-native part:

- write the updated rows into the `Person` table
- create a new native Lance table version, say `v2`
- record native version metadata for that one table

Omnigraph part:

- decide whether `Person@v2` is visible to the graph yet
- if not published yet, readers still see the older graph-visible version

So Lance answers:

> What happened to the `Person` table?

Omnigraph answers:

> Is that new table version part of the current graph yet?

## Example 2: Atomic Graph Publish

Suppose one change updates both:

- `node:Person` from `v1` to `v2`
- `edge:Knows` from `v3` to `v4`

Omnigraph needs one graph-visible commit point:

- either readers see `Person@v2` and `Knows@v4` together
- or readers keep seeing `Person@v1` and `Knows@v3`

That is not the same as two independent table writes.

This is why `crates/omnigraph/src/db/manifest/publisher.rs` still exists: it
owns the graph-wide publish step into `__manifest`.

## Example 3: Branches

Suppose we publish on branch `feature-x`.

Omnigraph needs:

- publish the new graph state into the `feature-x` graph head
- keep `main` unchanged

Today the public Rust namespace surface does not give us a branch-aware
namespace handle for `DirectoryNamespace`, so Omnigraph still owns that
branch-aware graph publish behavior.

## What We Already Replaced With Namespace APIs

Compared with the older `_manifest.lance` design, a lot has already moved onto
Lance-native tooling:

- read-side table resolution
- namespace-based table lookup
- native table-local version history
- staged write-side table versioning
- table-local version metadata flow

The custom layer is much smaller than before.

## What Is Still Custom

The intentionally custom layer is now narrow:

- validate graph publish invariants against current `__manifest` state
- publish multiple updated table versions as one graph-visible batch
- do that against the active graph branch
- refresh graph-visible state after the publish succeeds

That logic lives in:

- `crates/omnigraph/src/db/manifest/publisher.rs`

## Why The Last Custom Layer Still Exists

The spike in `docs/dev/namespace-publisher-gap.md` showed three concrete gaps
in the Lance 4 Rust namespace surface:

1. No public batch table-version publish API on the Rust trait.
2. No branch-aware namespace handle for graph publication.
3. `DirectoryNamespace::create_table_version` does not fit Omnigraph's current
   flow after a native table write has already finalized the target version.

That means Omnigraph can use namespace APIs for table-local behavior, but it
still needs one custom graph publish layer on top.

## What “Max Lance” Means From Here

“Max Lance” does not mean removing all Omnigraph logic.

It means keeping the split as thin as possible:

- Lance owns table storage, table-local versions, and namespace lookup.
- Omnigraph owns only graph semantics above a single table.

Today the only meaningful storage/control-plane semantic left in that category
is graph-wide atomic publish into `__manifest`.
