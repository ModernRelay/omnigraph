# Lance Namespace

Reference notes on the Lance Namespace specification and how Omnigraph uses it.

For the gap analysis between the current Lance Namespace Rust surface and what
Omnigraph needs, see `docs/dev/namespace-publisher-gap.md`.

---

## What Lance Namespace Is

Lance Namespace is an open specification for managing collections of Lance
tables across different catalog systems. It provides a single `LanceNamespace`
trait that adapts to any catalog backend — local directory, REST catalog, Unity
Catalog, Hive Metastore, Iceberg REST Catalog, etc.

Repository: https://github.com/lance-format/lance-namespace

### Architecture (three layers)

1. **Client spec** — the `LanceNamespace` trait (async, `Send + Sync + Debug`).
   All catalog backends implement this trait.
2. **Native catalog specs** — two built-in backends:
   - `DirectoryNamespace` (storage-only, no external metadata service; tables
     live directly on filesystem / S3 / GCS)
   - `REST Namespace` (for custom enterprise catalog implementations)
3. **Implementation specs** — mappings for external catalogs (Apache Polaris,
   Unity Catalog, Hive Metastore, Iceberg REST Catalog). Third parties can add
   more without upstream approval.

### Rust crates

| Crate | Purpose |
|---|---|
| `lance-namespace` | Core `LanceNamespace` trait and model types |
| `lance-namespace-impls` | `DirectoryNamespace` and other built-in backends |
| `lance-namespace-reqwest-client` | REST client for the REST Namespace spec |

All three are transitive dependencies of `lance` (v4.x). Omnigraph depends on
`lance-namespace` and `lance-namespace-impls` through `lance`.

---

## The `LanceNamespace` Trait

One required method:

```rust
fn namespace_id(&self) -> String
// e.g. "rest(endpoint=https://api.example.com)"
// e.g. "dir(root=/path/to/data)"
```

All other methods have default implementations that return `Unsupported`. A
backend overrides only the operations it supports.

### Method surface (46 methods)

| Category | Methods |
|---|---|
| Namespace ops | `create_namespace`, `list_namespaces`, `describe_namespace`, `drop_namespace`, `namespace_exists` |
| Table management | `create_table`, `declare_table`, `register_table`, `list_tables`, `list_all_tables`, `describe_table`, `table_exists`, `drop_table`, `deregister_table`, `rename_table`, `count_table_rows` |
| Data ops | `insert_into_table`, `merge_insert_into_table`, `update_table`, `delete_from_table`, `query_table` |
| Version management | `list_table_versions`, `create_table_version`, `describe_table_version`, `batch_delete_table_versions`, `restore_table` |
| Index ops | `create_table_index`, `create_table_scalar_index`, `drop_table_index`, `list_table_indices`, `describe_table_index_stats` |
| Schema / metadata | `alter_table_add_columns`, `alter_table_alter_columns`, `alter_table_drop_columns`, `update_table_schema_metadata`, `get_table_stats` |
| Tags | `list_table_tags`, `get_table_tag_version`, `create_table_tag`, `delete_table_tag`, `update_table_tag` |
| Transactions | `describe_transaction`, `alter_transaction` |
| Query planning | `explain_table_query_plan`, `analyze_table_query_plan` |

### Key model types (`lance_namespace::models`)

| Type | Purpose |
|---|---|
| `CreateTableVersionRequest` | Create a new table version (`put_if_not_exists` semantics) |
| `BatchCommitTablesRequest` | Atomic batch of `DeclareTable` / `CreateTableVersion` / `DeleteTableVersions` / `DeregisterTable` — all-or-nothing |
| `CommitTableOperation` | Single operation within a batch commit |
| `TableVersion` | Describes one version: version number, manifest path, size, e-tag, timestamp, metadata map |
| `DescribeTableResponse` | Table info: location, version, URI, schema, managed-versioning flag |
| `ListTableVersionsRequest` | Pagination, ordering (ascending/descending), limit |

### Opening a table through a namespace

```rust
let dataset = DatasetBuilder::from_namespace(namespace, vec![table_key])
    .await?
    .with_branch(branch, Some(version))
    .load()
    .await?;
```

---

## How Omnigraph Uses Lance Namespace

Omnigraph implements two custom `LanceNamespace` backends
(`crates/omnigraph/src/db/manifest/namespace.rs`):

### `BranchManifestNamespace`

- Presents the `__manifest` table as a namespace catalog
- Reads are satisfied by scanning `__manifest` for matching `table_key` /
  `table_version` rows
- `create_table_version` delegates to `GraphNamespacePublisher` for atomic
  graph-level publish
- Used for read-path table resolution: `open_table_at_version_from_manifest`
  calls `DatasetBuilder::from_namespace(branch_manifest_namespace(…), …)`

### `StagedTableNamespace`

- Presents one Lance table as a single-table namespace
- `create_table_version` copies a staging manifest to the final version path
  with `PutMode::Create` (put-if-not-exists)
- Used for write-path table access: `open_table_head_for_write` calls
  `DatasetBuilder::from_namespace(staged_table_namespace(…), …)`

### `GraphNamespacePublisher`

(`crates/omnigraph/src/db/manifest/publisher.rs`)

This module fills the gap that Lance Namespace does not yet cover:

- **Atomic cross-table publish**: validates batch invariants, then inserts
  immutable `table_version` rows into `__manifest` via `merge_insert` with
  `WhenMatched::Fail` / `WhenNotMatched::InsertAll`
- **Branch-aware publish**: always operates against the active `__manifest`
  branch
- **Graph-visible commit point**: the `__manifest` version only advances once
  all table work succeeds

This module should disappear once Lance Rust exposes a branch-aware
`BatchCreateTableVersions` path. See `docs/dev/namespace-publisher-gap.md` for
the exact upstream requirements.

---

## Upstream Gaps (Summary)

These are documented in detail in `namespace-publisher-gap.md`:

1. **No batch table-version API on the public Rust trait** — the trait exposes
   per-table version methods only; `BatchCommitTablesRequest` exists in the
   models but is not surfaced on the trait
2. **No branch-aware namespace handle** — `DirectoryNamespace` operations are
   rooted at one directory; there is no branch parameter
3. **`create_table_version` is per-table only** — useful for table-local
   version publication but not for atomic multi-table graph commits
