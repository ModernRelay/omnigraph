//! Forbidden-API and graph-write protocol guard tests.
//!
//! Engine code (`exec/`, `db/omnigraph/`, `loader/`, `changes/`) MUST NOT
//! call Lance's inline-commit data-write APIs directly. The
//! `Storage` trait (`crate::storage_layer::TableStorage`) is the canonical
//! surface; staged primitives (`stage_append`, `stage_merge_insert`,
//! `stage_overwrite`, `stage_create_indices`) plus `commit_staged` are the only
//! way to advance Lance HEAD.
//!
//! The raw storage modules are crate-private and the trait is sealed (only
//! `TableStore` implements it), so Rust visibility is the primary boundary.
//! This test is **defense in depth**: it catches known cases where engine code
//! reaches around that boundary by importing `lance::dataset::*` types directly.
//!
//! ## How it works
//!
//! Walks `crates/omnigraph/src/**/*.rs`. The legacy forbidden-Lance check keeps
//! a lexical deny-list for type and builder construction, while the graph-write
//! guard parses Rust with `syn` so split calls, method syntax, and UFCS are
//! counted structurally. Lines whose preceding line contains the sentinel
//! comment `// forbidden-api-allow: <reason>` are exempt from the lexical
//! deny-list — reviewers see the sentinel in diff and can ask whether the
//! exemption is justified.
//!
//! The graph-write protocol guard is structural rather than grep-based. It
//! parses Rust with `syn`, classifies all public async inherent `Omnigraph`
//! methods plus loader conveniences, and counts registered durable-call shapes
//! by file. The pinned method and UFCS forms, and production items after a test
//! module, therefore cannot evade it. Self-tests also pin the selected
//! rename/macro-token shapes that are rejected. This source scanner is not a
//! Rust macro expander or general function-pointer alias analysis; visibility
//! remains the structural closure.
//!
//! ## What's deliberately out of scope (allow-listed by exact file)
//!
//! - `crates/omnigraph/src/table_store.rs` — the crate-private storage layer.
//!   The forbidden Lance APIs live here legitimately.
//! - The exact manifest gateway implementations listed below — bootstrap,
//!   namespace plumbing, row-level publishing, and recovery — plus their
//!   out-of-line test module. Other files under `db/manifest/` are scanned.
//! - `crates/omnigraph/src/storage_layer.rs` — IS the trait module.
//!
//! ## Allow-list shape
//!
//! After the exact EnsureIndices adapter, `db.storage()` (`&dyn TableStorage`)
//! exposes only staged primitives + reads and there is no separate inline
//! residual surface. Vector index creation uses beta.21's full-table
//! `execute_uncommitted` path inside `stage_create_indices`; `delete` likewise
//! migrated to `stage_delete` in MR-A (Lance 7.0 #6658).
//! The dead legacy methods
//! (trait `append_batch` / `merge_insert_batches`, inherent
//! `merge_insert_batch{,es}`, `create_{btree,inverted}_index`) were
//! removed entirely. This guard's scope is unchanged: it catches direct
//! `lance::*` inline-commit misuse outside the storage layer. The
//! file-level allow-list below matches that boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, Item, Meta, Token, Type, Visibility};

const FORBIDDEN_PATTERNS: &[&str] = &[
    // Builder types — direct construction is the side door around the
    // staged-write surface.
    "MergeInsertBuilder",
    "InsertBuilder::",
    "DeleteBuilder",
    "CommitBuilder::new",
    ".create_index_builder(",
    ".create_index_segment_builder(",
    // Associated-function forms of inline-commit Lance APIs. These would
    // only appear in source if the file imports `lance::Dataset` and
    // calls the static fn — exactly the misuse we want to catch. These
    // patterns deliberately exclude `.append(` / `.delete(` / `.write(`
    // because those would over-match (`.delete_branch(`, `Vec::append`,
    // arrow-array `.append(`, etc.).
    "Dataset::write",
    "Dataset::append",
    "Dataset::delete",
    "Dataset::merge_insert",
    "Dataset::add_columns",
    "Dataset::update_columns",
    "Dataset::drop_columns",
    "Dataset::truncate_table",
    "Dataset::restore",
    // Raw dataset OPENS — all reads must route through `Snapshot::open` (the
    // held-handle cache + shared Session, Fix 3). Only the instrumented opener
    // (`instrumentation.rs`) and the storage/manifest layers (allow-listed below)
    // open datasets directly; forbidding these in the read/exec layer keeps a
    // future read from silently bypassing the cache.
    "Dataset::open",
    "DatasetBuilder::from_uri",
    "DatasetBuilder::from_namespace",
    // Lance-specific method names that don't clash with our `TableStore`
    // wrappers (we use `merge_insert_batch{,es}`, `add_columns_to_*`,
    // etc. — never the bare Lance names). Engine code that writes
    // `ds.merge_insert(...)` against a `Dataset` value is reaching
    // around the trait surface.
    ".merge_insert(",
    ".add_columns(",
    ".update_columns(",
    ".drop_columns(",
    ".truncate_table(",
    // `.restore(` is Lance-specific (no other library in this workspace
    // exposes a `.restore(` method); safe to ban without false-positive
    // risk. Used to revert a Lance dataset to a prior version — never
    // an operation engine code should perform directly.
    ".restore(",
    // NOT included: `.append(`, `.delete(`, `.write(`. Each over-matches
    // legitimate non-Lance uses (`Vec::append`, `String::append`, arrow
    // array `BuilderArray::append`, `ObjectStore::delete`, etc.).
    // Engine code calling `ds.append(reader, params)` is handled by the
    // structural inventory for its supported call shape. The lexical pass is
    // intentionally only defense in depth; crate visibility is what prevents
    // downstream SDK callers from obtaining the raw storage surface.
];

/// Exact source-relative files exempt from the lexical Lance guard. These are
/// the legitimate storage-layer
/// or manifest-layer implementations that USE the forbidden APIs to
/// provide the staged primitives or to maintain the system tables
/// (manifest, recovery audit).
const ALLOW_LIST_FILES: &[&str] = &[
    "table_store.rs",              // The storage layer itself.
    "table_store/staged_tests.rs", // Unit tests for private staged primitives.
    "storage_layer.rs",            // The trait module.
    "db/graph_coordinator.rs",     // Drives the manifest publisher / branch coordinator.
    "db/recovery_audit.rs",        // Maintains `_graph_commit_recoveries.lance`.
    "db/manifest/graph.rs",        // Bootstraps the manifest and commit datasets.
    "db/manifest/namespace.rs",    // Opens manifest datasets through the shared namespace.
    "db/manifest/publisher.rs",    // Lowest row-level manifest publish gateway.
    "db/manifest/recovery.rs",     // Recovery executor; exactly inventoried below.
    "db/manifest/tests.rs",        // Out-of-line tests for the trusted gateways.
    "instrumentation.rs",          // The instrumented dataset opener.
];

/// Lowest-level implementations which define the primitives inventoried at
/// their callers, plus the out-of-line manifest test module. This list is
/// intentionally file-exact: a new sibling under `db/manifest/` is scanned.
const PROTOCOL_SCAN_EXCLUDED_FILES: &[&str] = &[
    "table_store.rs",
    "table_store/staged_tests.rs",
    "storage_layer.rs",
    "storage.rs",
    "instrumentation.rs",
    "db/manifest/graph.rs",
    "db/manifest/namespace.rs",
    "db/manifest/publisher.rs",
    "db/manifest/tests.rs",
];

const SENTINEL: &str = "// forbidden-api-allow:";

/// Closed classification of the supported public async inherent `Omnigraph`
/// and loader graph-write surfaces. This lives in the guard, not as a runtime
/// label a new caller could simply lie about. The callsite registry below
/// observes the registered durable-call shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteProtocol {
    Exact(&'static str),
    Bounded(&'static str),
    Composed(&'static str),
    ManifestAdoption,
    NativeRefControl,
    PhysicalOnly,
    EphemeralScratch,
    TestOnly,
    Bootstrap,
    RecoveryExecutor,
    LegacySchemaBootstrap,
    ReadOnlyAccess,
}

impl WriteProtocol {
    fn label(self) -> String {
        match self {
            Self::Exact(name) => format!("exact adapter ({name})"),
            Self::Bounded(name) => format!("bounded adapter ({name})"),
            Self::Composed(name) => format!("composed protocol ({name})"),
            Self::ManifestAdoption => "manifest adoption".into(),
            Self::NativeRefControl => "native ref control".into(),
            Self::PhysicalOnly => "physical-only maintenance".into(),
            Self::EphemeralScratch => "ephemeral scratch".into(),
            Self::TestOnly => "test/failpoint-only".into(),
            Self::Bootstrap => "bootstrap".into(),
            Self::RecoveryExecutor => "recovery executor".into(),
            Self::LegacySchemaBootstrap => "legacy schema bootstrap".into(),
            Self::ReadOnlyAccess => "read-only raw snapshot access".into(),
        }
    }
}

const MUTATION_V3: WriteProtocol = WriteProtocol::Exact("Mutation v3");
const LOAD_V3: WriteProtocol = WriteProtocol::Exact("Load v3");
const SCHEMA_V7: WriteProtocol = WriteProtocol::Exact("SchemaApply v7");
const MERGE_V4: WriteProtocol = WriteProtocol::Exact("BranchMerge v4");
const INDICES_V8: WriteProtocol = WriteProtocol::Exact("EnsureIndices v8");
const OPTIMIZE_V2: WriteProtocol = WriteProtocol::Bounded("Optimize v2");

#[derive(Debug, Clone, Copy)]
struct WriteSurface {
    file: &'static str,
    function: &'static str,
    protocol: WriteProtocol,
}

macro_rules! write_surfaces {
    ($($file:literal => $protocol:expr => [$($function:literal),+ $(,)?]),+ $(,)?) => {
        const WRITE_SURFACES: &[WriteSurface] = &[
            $($(WriteSurface { file: $file, function: $function, protocol: $protocol },)+)+
        ];
    };
}

write_surfaces! {
    "db/omnigraph.rs" => WriteProtocol::Bootstrap => ["init", "init_with_options"],
    "db/omnigraph.rs" => WriteProtocol::RecoveryExecutor => ["open", "open_with_storage", "refresh"],
    "db/omnigraph.rs" => WriteProtocol::LegacySchemaBootstrap => ["open_read_only"],
    "exec/mutation.rs" => MUTATION_V3 => ["mutate", "mutate_as"],
    "loader/mod.rs" => LOAD_V3 => ["load_jsonl", "load_jsonl_file", "load", "load_file"],
    "loader/mod.rs" => WriteProtocol::Composed("optional branch create, then Load v3") => ["load_as", "load_file_as"],
    "loader/mod.rs" => WriteProtocol::Composed("branch create when absent, then Load v3 alias") => ["ingest", "ingest_as", "ingest_file", "ingest_file_as"],
    "db/omnigraph.rs" => WriteProtocol::Composed("SchemaApply v7 + sentinel ref + optional hard-drop GC") => ["apply_schema", "apply_schema_with_options", "apply_schema_as", "apply_schema_as_with_catalog_check"],
    "exec/merge.rs" => MERGE_V4 => ["branch_merge", "branch_merge_as"],
    "db/omnigraph.rs" => INDICES_V8 => ["ensure_indices", "ensure_indices_on"],
    "db/omnigraph.rs" => WriteProtocol::TestOnly => ["failpoint_publish_table_head_without_index_rebuild_for_test"],
    "db/omnigraph.rs" => OPTIMIZE_V2 => ["optimize"],
    "db/omnigraph.rs" => WriteProtocol::ManifestAdoption => ["repair"],
    "db/omnigraph.rs" => WriteProtocol::PhysicalOnly => ["cleanup"],
    "db/omnigraph.rs" => WriteProtocol::NativeRefControl => ["branch_create", "branch_create_as", "branch_create_from", "branch_create_from_as", "branch_delete", "branch_delete_as"],
}

// Every public async inherent Omnigraph method (wherever its impl lives), plus
// top-level loader convenience functions, must appear in either this read-only
// set or WRITE_SURFACES. Within that supported API shape this is
// name-independent: a newly named `transact`, `publish`, or `vacuum` method
// cannot evade discovery.
const READ_ONLY_SURFACES: &[(&str, &str)] = &[
    ("db/omnigraph.rs", "plan_schema"),
    ("db/omnigraph.rs", "plan_schema_with_options"),
    ("db/omnigraph.rs", "preview_schema_apply_with_options"),
    ("db/omnigraph.rs", "snapshot_of"),
    ("db/omnigraph.rs", "version_of"),
    ("db/omnigraph.rs", "internal_schema_version_of"),
    ("db/omnigraph.rs", "resolved_branch_of"),
    ("db/omnigraph.rs", "sync_branch"),
    ("db/omnigraph.rs", "resolve_snapshot"),
    ("db/omnigraph.rs", "diff_between"),
    ("db/omnigraph.rs", "diff_commits"),
    ("db/omnigraph.rs", "entity_at_target"),
    ("db/omnigraph.rs", "entity_at"),
    ("db/omnigraph.rs", "snapshot_at_version"),
    ("db/omnigraph.rs", "export_jsonl"),
    ("db/omnigraph.rs", "export_jsonl_to_writer"),
    ("db/omnigraph.rs", "graph_index"),
    ("db/omnigraph.rs", "read_blob"),
    ("db/omnigraph.rs", "branch_list"),
    ("db/omnigraph.rs", "get_commit"),
    ("db/omnigraph.rs", "list_commits"),
    ("exec/query.rs", "query"),
    ("exec/query.rs", "run_query_at"),
];

// Every crate-visible async method on the two low-level coordinators is also
// classified. The types themselves are crate-private, but an unregistered
// crate-visible wrapper would otherwise create a new internal route to the
// manifest publisher without changing the durable gateway count.
const LOW_LEVEL_READ_ONLY_SURFACES: &[(&str, &str, &str)] = &[
    ("db/graph_coordinator.rs", "GraphCoordinator", "open"),
    ("db/graph_coordinator.rs", "GraphCoordinator", "open_branch"),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "branch_identifier",
    ),
    ("db/graph_coordinator.rs", "GraphCoordinator", "refresh"),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "probe_latest_incarnation",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "refresh_manifest_only",
    ),
    ("db/graph_coordinator.rs", "GraphCoordinator", "branch_list"),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "all_branches",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "branch_descendants",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "snapshot_at_version",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "resolve_snapshot_id",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "resolve_target",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "resolve_commit",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "head_commit_id",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "list_commits",
    ),
    ("db/manifest.rs", "ManifestCoordinator", "open"),
    ("db/manifest.rs", "ManifestCoordinator", "open_at_branch"),
    ("db/manifest.rs", "ManifestCoordinator", "snapshot_at"),
    ("db/manifest.rs", "ManifestCoordinator", "refresh"),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "read_graph_lineage_at",
    ),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "probe_latest_version",
    ),
    ("db/manifest.rs", "ManifestCoordinator", "branch_identifier"),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "probe_latest_incarnation",
    ),
    ("db/manifest.rs", "ManifestCoordinator", "list_branches"),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "descendant_branches",
    ),
];

const LOW_LEVEL_WRITE_SURFACES: &[(&str, &str, &str, WriteProtocol)] = &[
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "init",
        WriteProtocol::Bootstrap,
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "branch_create",
        WriteProtocol::NativeRefControl,
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "branch_delete",
        WriteProtocol::NativeRefControl,
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "commit_updates_with_actor_with_expected",
        WriteProtocol::Exact("shared publisher gateway"),
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "commit_changes_with_intent_and_expected",
        WriteProtocol::Exact("shared publisher gateway"),
    ),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "init",
        WriteProtocol::Bootstrap,
    ),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "commit_changes_with_lineage_and_precondition",
        WriteProtocol::Exact("lowest manifest publisher gateway"),
    ),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "create_branch",
        WriteProtocol::NativeRefControl,
    ),
    (
        "db/manifest.rs",
        "ManifestCoordinator",
        "delete_branch",
        WriteProtocol::NativeRefControl,
    ),
];

#[derive(Debug, Clone, Copy)]
struct DurableCallsite {
    file: &'static str,
    primitive: &'static str,
    count: usize,
    protocol: WriteProtocol,
}

macro_rules! durable_calls {
    ($(($file:literal, $primitive:literal, $count:literal, $protocol:expr)),+ $(,)?) => {
        const DURABLE_CALLS: &[DurableCallsite] = &[
            $(DurableCallsite { file: $file, primitive: $primitive, count: $count, protocol: $protocol },)+
        ];
    };
}

// Exact per-file counts, not file allow-lists: a second caller in an approved
// adapter is still a new writer and fails this guard. The storage and manifest
// publisher implementations provide the lowest gateways and are excluded;
// recovery execution is deliberately included and inventoried below.
durable_calls! {
    ("exec/staging.rs", "write_sidecar(", 1, WriteProtocol::Exact("Mutation/Load v3")),
    ("exec/merge.rs", "write_sidecar(", 1, MERGE_V4),
    ("db/omnigraph/schema_apply.rs", "write_sidecar(", 1, SCHEMA_V7),
    ("db/omnigraph/table_ops.rs", "write_sidecar(", 1, INDICES_V8),
    ("db/omnigraph/optimize.rs", "write_sidecar(", 1, OPTIMIZE_V2),
    ("exec/staging.rs", ".commit_staged_exact(", 1, WriteProtocol::Exact("Mutation/Load v3")),
    ("exec/merge.rs", ".commit_staged_exact(", 1, MERGE_V4),
    ("db/omnigraph/schema_apply.rs", ".commit_staged_create_exact(", 2, SCHEMA_V7),
    ("db/omnigraph/schema_apply.rs", ".commit_staged_exact(", 1, SCHEMA_V7),
    ("db/omnigraph/table_ops.rs", ".commit_staged_exact(", 1, INDICES_V8),
    ("db/omnigraph/table_ops.rs", ".commit_staged(", 1, WriteProtocol::Composed("shared merge/Optimize index tail")),
    ("db/omnigraph/table_ops.rs", ".fork_branch_from_state(", 1, WriteProtocol::Composed("adapter-owned first-touch data ref")),
    ("exec/staging.rs", "confirm_occ_sidecar_phase_b(", 1, WriteProtocol::Exact("Mutation/Load v3")),
    ("exec/merge.rs", "confirm_branch_merge_sidecar_phase_b(", 1, MERGE_V4),
    ("db/omnigraph/schema_apply.rs", "confirm_schema_apply_sidecar_v7(", 1, SCHEMA_V7),
    ("db/omnigraph/table_ops.rs", "confirm_ensure_indices_sidecar_v8(", 1, INDICES_V8),
    ("exec/mutation.rs", "delete_sidecar(", 1, MUTATION_V3),
    ("loader/mod.rs", "delete_sidecar(", 1, LOAD_V3),
    ("exec/merge.rs", "delete_sidecar(", 1, MERGE_V4),
    ("db/omnigraph/schema_apply.rs", "delete_sidecar(", 1, SCHEMA_V7),
    ("db/omnigraph/table_ops.rs", "delete_sidecar(", 1, INDICES_V8),
    ("db/omnigraph/optimize.rs", "delete_sidecar(", 1, OPTIMIZE_V2),
    ("exec/mutation.rs", "commit_updates_on_branch_with_expected(", 1, MUTATION_V3),
    ("loader/mod.rs", "commit_updates_on_branch_with_expected(", 1, LOAD_V3),
    ("exec/merge.rs", "commit_updates_on_branch_with_expected(", 1, MERGE_V4),
    ("db/omnigraph.rs", "commit_updates_on_branch_with_expected(", 1, WriteProtocol::Exact("shared publisher wrapper")),
    ("db/omnigraph/table_ops.rs", "commit_updates_on_branch_with_expected(", 1, WriteProtocol::Exact("shared publisher")),
    ("db/omnigraph/table_ops.rs", ".commit_changes_with_intent_and_expected(", 2, WriteProtocol::Exact("shared publisher")),
    ("db/omnigraph/schema_apply.rs", ".commit_changes_with_intent_and_expected(", 1, SCHEMA_V7),
    ("db/omnigraph/repair.rs", ".commit_updates_with_actor_with_expected(", 1, WriteProtocol::ManifestAdoption),
    ("db/omnigraph/optimize.rs", ".commit_updates_with_actor_with_expected(", 1, OPTIMIZE_V2),
    ("db/graph_coordinator.rs", ".commit_changes_with_intent_and_expected(", 1, WriteProtocol::Exact("publisher gateway")),
    ("db/graph_coordinator.rs", ".commit_changes_with_lineage_and_precondition(", 1, WriteProtocol::Exact("lowest manifest publisher gateway")),
    ("db/manifest.rs", ".publish_with_precondition(", 1, WriteProtocol::Exact("lowest manifest publisher gateway")),
    ("db/omnigraph/table_ops.rs", ".commit_updates_with_actor_with_expected(", 2, WriteProtocol::TestOnly),
    ("db/omnigraph.rs", ".write_text_if_absent(", 1, WriteProtocol::Bootstrap),
    ("db/omnigraph.rs", ".write_text(", 1, WriteProtocol::Bootstrap),
    ("db/schema_state.rs", ".write_text(", 2, WriteProtocol::Composed("schema state publication")),
    ("db/manifest/recovery.rs", ".write_text(", 6, WriteProtocol::RecoveryExecutor),
    ("db/omnigraph/schema_apply.rs", ".write_text(", 1, SCHEMA_V7),
    ("db/omnigraph.rs", ".delete(", 1, WriteProtocol::Bootstrap),
    ("db/schema_state.rs", ".delete(", 3, WriteProtocol::Composed("schema staging cleanup")),
    ("db/schema_state.rs", ".rename_text(", 1, WriteProtocol::Composed("schema staging promotion")),
    ("db/manifest/recovery.rs", ".delete(", 2, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", ".delete_prefix(", 2, WriteProtocol::RecoveryExecutor),
    ("db/omnigraph.rs", "GraphCoordinator::init(", 1, WriteProtocol::Bootstrap),
    ("db/omnigraph.rs", "recover_manifest_drift(", 1, WriteProtocol::RecoveryExecutor),
    ("db/omnigraph.rs", "heal_pending_sidecars_roll_forward(", 2, WriteProtocol::RecoveryExecutor),
    ("db/omnigraph.rs", "recover_schema_state_files(", 2, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "recover_schema_state_files(", 1, WriteProtocol::RecoveryExecutor),
    ("db/omnigraph.rs", "load_or_bootstrap_schema_contract(", 2, WriteProtocol::LegacySchemaBootstrap),
    ("db/omnigraph/optimize.rs", "compact_files(", 2, WriteProtocol::Composed("Optimize v2 data + physical manifest compaction")),
    ("db/omnigraph/optimize.rs", ".optimize_indices(", 1, OPTIMIZE_V2),
    ("db/omnigraph/optimize.rs", "cleanup_old_versions(", 1, WriteProtocol::PhysicalOnly),
    ("db/omnigraph/schema_apply.rs", "cleanup_old_versions(", 1, WriteProtocol::Composed("SchemaApply hard-drop GC")),
    ("db/omnigraph/schema_apply.rs", "write_schema_contract_staging(", 1, SCHEMA_V7),
    ("db/omnigraph/schema_apply.rs", "promote_exact_schema_staging(", 1, SCHEMA_V7),
    ("db/omnigraph.rs", ".branch_create(", 2, WriteProtocol::NativeRefControl),
    ("db/omnigraph.rs", ".branch_delete(", 1, WriteProtocol::NativeRefControl),
    ("db/omnigraph/schema_apply.rs", ".branch_create(", 1, SCHEMA_V7),
    ("db/omnigraph/schema_apply.rs", ".branch_delete(", 1, SCHEMA_V7),
    ("db/graph_coordinator.rs", ".create_branch(", 1, WriteProtocol::NativeRefControl),
    ("db/graph_coordinator.rs", ".delete_branch(", 1, WriteProtocol::NativeRefControl),
    ("branch_control.rs", ".create_branch(", 1, WriteProtocol::Composed("graph/data native refs")),
    ("branch_control.rs", ".delete_branch(", 1, WriteProtocol::Composed("graph/data native refs")),
    ("branch_control.rs", ".force_delete_branch(", 1, WriteProtocol::Composed("graph/data native refs")),
    ("db/omnigraph.rs", ".force_delete_branch(", 1, WriteProtocol::NativeRefControl),
    ("db/omnigraph/table_ops.rs", ".force_delete_branch(", 1, WriteProtocol::Composed("first-touch reclaim")),
    ("db/omnigraph/optimize.rs", ".force_delete_branch(", 1, WriteProtocol::PhysicalOnly),
    ("db/manifest/recovery.rs", ".force_delete_branch(", 2, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", ".publish_with_precondition(", 1, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", ".publish(", 1, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", ".restore(", 1, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", ".append(RecoveryAuditRecord", 7, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "publish_recovery_commit(", 8, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "restore_table_to_version(", 3, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "record_audit(", 9, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "delete_sidecar_by_operation_id(", 18, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "delete_sidecar(", 1, WriteProtocol::RecoveryExecutor),
    ("db/recovery_audit.rs", ".raw_dataset_append(", 1, WriteProtocol::RecoveryExecutor),
    ("db/recovery_audit.rs", "Dataset::write(", 1, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "promote_exact_schema_staging(", 2, WriteProtocol::RecoveryExecutor),
    ("db/manifest/recovery.rs", "discard_exact_schema_staging(", 2, WriteProtocol::RecoveryExecutor),
    ("exec/merge.rs", "TableStore::create_empty_dataset(", 1, WriteProtocol::EphemeralScratch),
    ("exec/merge.rs", "TableStore::append_or_create_batch(", 1, WriteProtocol::EphemeralScratch),
    ("db/omnigraph.rs", ".dataset()", 1, WriteProtocol::ReadOnlyAccess),
    ("db/omnigraph.rs", ".into_arc()", 1, WriteProtocol::ReadOnlyAccess),
    ("db/omnigraph/table_ops.rs", ".dataset()", 1, WriteProtocol::ReadOnlyAccess),
    ("db/omnigraph/export.rs", ".dataset()", 1, WriteProtocol::ReadOnlyAccess),
    ("db/omnigraph/schema_apply.rs", ".dataset()", 1, SCHEMA_V7),
    ("db/omnigraph/repair.rs", ".dataset()", 1, WriteProtocol::ManifestAdoption),
    ("db/omnigraph/optimize.rs", ".dataset()", 5, WriteProtocol::Composed("Optimize v2 planning + physical cleanup")),
    ("db/omnigraph/optimize.rs", ".into_dataset()", 2, OPTIMIZE_V2),
    ("db/omnigraph/optimize.rs", "SnapshotHandle::new(", 1, OPTIMIZE_V2),
    ("exec/merge.rs", "SnapshotHandle::new(", 4, MERGE_V4),
}

const DURABLE_PRIMITIVES: &[&str] = &[
    "write_sidecar(",
    "confirm_occ_sidecar_phase_b(",
    "confirm_branch_merge_sidecar_phase_b(",
    "confirm_schema_apply_sidecar_v7(",
    "confirm_ensure_indices_sidecar_v8(",
    "delete_sidecar(",
    ".commit_staged_create_exact(",
    ".commit_staged_exact(",
    ".commit_staged(",
    ".fork_branch_from_state(",
    "commit_updates_on_branch_with_expected(",
    ".commit_changes_with_intent_and_expected(",
    ".commit_changes_with_lineage_and_precondition(",
    ".commit_updates_with_actor_with_expected(",
    ".commit_updates_with_actor(",
    ".publish_with_precondition(",
    ".publish(",
    ".write_text_if_absent(",
    ".write_text(",
    ".delete(",
    ".delete_prefix(",
    ".rename_text(",
    "GraphCoordinator::init(",
    "recover_manifest_drift(",
    "heal_pending_sidecars_roll_forward(",
    "recover_schema_state_files(",
    "load_or_bootstrap_schema_contract(",
    "compact_files(",
    ".optimize_indices(",
    "cleanup_old_versions(",
    "write_schema_contract_staging(",
    "promote_exact_schema_staging(",
    "discard_exact_schema_staging(",
    ".branch_create(",
    ".branch_delete(",
    ".create_branch(",
    ".delete_branch(",
    ".force_delete_branch(",
    "TableStore::create_empty_dataset(",
    "TableStore::append_or_create_batch(",
    "TableStore::write_dataset(",
    "Dataset::write(",
    ".raw_dataset_append(",
    ".merge_insert(",
    ".add_columns(",
    ".update_columns(",
    ".drop_columns(",
    ".truncate_table(",
    ".append(RecoveryAuditRecord",
    ".restore(",
    "publish_recovery_commit(",
    "restore_table_to_version(",
    "record_audit(",
    "delete_sidecar_by_operation_id(",
    ".dataset()",
    ".into_arc()",
    ".into_dataset()",
    "SnapshotHandle::new(",
];

fn engine_src_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir).join("src")
}

fn is_allow_listed(src: &Path, path: &Path) -> bool {
    let relative = relative_to_src(src, path);
    ALLOW_LIST_FILES.contains(&relative.as_str())
}

fn is_protocol_scan_excluded(src: &Path, path: &Path) -> bool {
    let relative = relative_to_src(src, path);
    PROTOCOL_SCAN_EXCLUDED_FILES.contains(&relative.as_str())
}

fn walk_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_into(root, &mut out);
    out
}

fn walk_into(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_into(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn relative_to_src<'a>(src: &Path, file: &'a Path) -> String {
    file.strip_prefix(src)
        .unwrap_or(file)
        .to_string_lossy()
        .replace('\\', "/")
}

fn parse_rust_source(contents: &str, context: &str) -> syn::File {
    syn::parse_file(contents)
        .unwrap_or_else(|error| panic!("failed to parse Rust source {context}: {error}"))
}

fn nested_meta(list: &syn::MetaList) -> Vec<Meta> {
    Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())
        .map(Punctuated::into_iter)
        .map(Iterator::collect)
        .unwrap_or_default()
}

/// True only when the cfg predicate itself proves the item is test-only.
/// Unknown and `not(...)` predicates remain in the scan (fail closed).
fn meta_requires_test(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) if list.path.is_ident("all") => {
            nested_meta(list).iter().any(meta_requires_test)
        }
        Meta::List(list) if list.path.is_ident("any") => {
            let alternatives = nested_meta(list);
            !alternatives.is_empty() && alternatives.iter().all(meta_requires_test)
        }
        _ => false,
    }
}

fn cfg_requires_test(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        let Meta::List(cfg) = &attribute.meta else {
            return false;
        };
        cfg.path.is_ident("cfg") && nested_meta(cfg).iter().any(meta_requires_test)
    })
}

fn final_path_ident(expr: &Expr) -> Option<String> {
    let Expr::Path(path) = expr else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn path_ends_with(expr: &Expr, suffix: &[&str]) -> bool {
    let Expr::Path(path) = expr else {
        return false;
    };
    let segments = path
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    segments.len() >= suffix.len()
        && segments[segments.len() - suffix.len()..]
            .iter()
            .map(String::as_str)
            .eq(suffix.iter().copied())
}

fn expr_is_struct_named(expr: &Expr, expected: &str) -> bool {
    match expr {
        Expr::Struct(value) => value
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == expected),
        Expr::Reference(reference) => expr_is_struct_named(&reference.expr, expected),
        Expr::Group(group) => expr_is_struct_named(&group.expr, expected),
        Expr::Paren(paren) => expr_is_struct_named(&paren.expr, expected),
        _ => false,
    }
}

fn type_final_ident(ty: &Type) -> Option<&syn::Ident> {
    let Type::Path(path) = ty else {
        return None;
    };
    path.path.segments.last().map(|segment| &segment.ident)
}

fn type_contains_identifier(ty: &Type, expected: &str) -> bool {
    match ty {
        Type::Path(path) => {
            path.path.segments.iter().any(|segment| {
                segment.ident == expected
                    || match &segment.arguments {
                        syn::PathArguments::AngleBracketed(arguments) => {
                            arguments.args.iter().any(|argument| {
                                matches!(argument, syn::GenericArgument::Type(inner) if type_contains_identifier(inner, expected))
                            })
                        }
                        syn::PathArguments::Parenthesized(arguments) => {
                            arguments
                                .inputs
                                .iter()
                                .any(|inner| type_contains_identifier(inner, expected))
                                || matches!(
                                    &arguments.output,
                                    syn::ReturnType::Type(_, inner)
                                        if type_contains_identifier(inner, expected)
                                )
                        }
                        syn::PathArguments::None => false,
                    }
            })
        }
        Type::Reference(reference) => type_contains_identifier(&reference.elem, expected),
        Type::Group(group) => type_contains_identifier(&group.elem, expected),
        Type::Paren(paren) => type_contains_identifier(&paren.elem, expected),
        Type::Ptr(pointer) => type_contains_identifier(&pointer.elem, expected),
        Type::Slice(slice) => type_contains_identifier(&slice.elem, expected),
        Type::Array(array) => type_contains_identifier(&array.elem, expected),
        Type::Tuple(tuple) => tuple
            .elems
            .iter()
            .any(|inner| type_contains_identifier(inner, expected)),
        _ => false,
    }
}

fn return_type_contains_identifier(output: &syn::ReturnType, expected: &str) -> bool {
    matches!(output, syn::ReturnType::Type(_, ty) if type_contains_identifier(ty, expected))
}

fn use_tree_contains_identifier(tree: &syn::UseTree, expected: &str) -> bool {
    match tree {
        syn::UseTree::Path(path) => {
            path.ident == expected || use_tree_contains_identifier(&path.tree, expected)
        }
        syn::UseTree::Name(name) => name.ident == expected,
        syn::UseTree::Rename(rename) => rename.ident == expected,
        syn::UseTree::Group(group) => group
            .items
            .iter()
            .any(|item| use_tree_contains_identifier(item, expected)),
        syn::UseTree::Glob(_) => false,
    }
}

fn use_tree_contains_glob(tree: &syn::UseTree) -> bool {
    match tree {
        syn::UseTree::Path(path) => use_tree_contains_glob(&path.tree),
        syn::UseTree::Group(group) => group.items.iter().any(use_tree_contains_glob),
        syn::UseTree::Glob(_) => true,
        syn::UseTree::Name(_) | syn::UseTree::Rename(_) => false,
    }
}

fn primitive_terminal_identifier(primitive: &str) -> &str {
    primitive
        .trim_start_matches('.')
        .split('(')
        .next()
        .expect("durable primitive identifier")
        .rsplit("::")
        .next()
        .expect("durable primitive terminal identifier")
}

fn is_durable_identifier(identifier: &str) -> bool {
    [
        "Dataset",
        "GraphCoordinator",
        "ManifestBatchPublisher",
        "ManifestCoordinator",
        "GraphNamespacePublisher",
        "RecoveryAudit",
        "SnapshotHandle",
        "StorageAdapter",
        "TableStorage",
        "TableStore",
    ]
    .contains(&identifier)
        || DURABLE_PRIMITIVES
            .iter()
            .any(|primitive| primitive_terminal_identifier(primitive) == identifier)
}

fn collect_durable_use_renames(tree: &syn::UseTree, hits: &mut Vec<String>) {
    match tree {
        syn::UseTree::Rename(rename) if is_durable_identifier(&rename.ident.to_string()) => {
            hits.push(format!(
                "durable identifier `{}` renamed to `{}`",
                rename.ident, rename.rename
            ));
        }
        syn::UseTree::Path(path) => collect_durable_use_renames(&path.tree, hits),
        syn::UseTree::Group(group) => {
            for item in &group.items {
                collect_durable_use_renames(item, hits);
            }
        }
        _ => {}
    }
}

#[derive(Default)]
struct CallInventory {
    counts: BTreeMap<String, usize>,
    macro_hits: Vec<String>,
}

impl CallInventory {
    fn record(&mut self, key: impl Into<String>) {
        *self.counts.entry(key.into()).or_default() += 1;
    }
}

impl<'ast> Visit<'ast> for CallInventory {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if cfg_requires_test(&node.attrs) {
            return;
        }
        visit::visit_item_mod(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if cfg_requires_test(&node.attrs) {
            return;
        }
        visit::visit_item_fn(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if cfg_requires_test(&node.attrs) {
            return;
        }
        visit::visit_item_impl(self, node);
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if cfg_requires_test(&node.attrs) {
            return;
        }
        visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_macro(&mut self, node: &'ast syn::ItemMacro) {
        if cfg_requires_test(&node.attrs) {
            return;
        }
        visit::visit_item_macro(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        collect_durable_use_renames(&node.tree, &mut self.macro_hits);
        visit::visit_item_use(self, node);
    }

    fn visit_item_type(&mut self, node: &'ast syn::ItemType) {
        if type_final_ident(&node.ty)
            .is_some_and(|identifier| is_durable_identifier(&identifier.to_string()))
        {
            self.macro_hits.push(format!(
                "durable owner `{}` hidden behind type alias `{}`",
                type_final_ident(&node.ty).expect("checked above"),
                node.ident
            ));
        }
        visit::visit_item_type(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method = node.method.to_string();
        self.record(method.clone());
        if method == "append" && node.args.len() == 2 {
            self.record(".raw_dataset_append(");
        }
        if method == "append"
            && node
                .args
                .first()
                .is_some_and(|argument| expr_is_struct_named(argument, "RecoveryAuditRecord"))
        {
            self.record(".append(RecoveryAuditRecord");
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let Some(identifier) = final_path_ident(&node.func) {
            self.record(identifier.clone());
            if identifier == "append" && node.args.len() == 3 {
                self.record(".raw_dataset_append(");
            }
            if identifier == "append"
                && node
                    .args
                    .iter()
                    .any(|argument| expr_is_struct_named(argument, "RecoveryAuditRecord"))
            {
                self.record(".append(RecoveryAuditRecord");
            }
        }
        for (suffix, key) in [
            (&["GraphCoordinator", "init"][..], "GraphCoordinator::init("),
            (
                &["TableStore", "create_empty_dataset"][..],
                "TableStore::create_empty_dataset(",
            ),
            (
                &["TableStore", "append_or_create_batch"][..],
                "TableStore::append_or_create_batch(",
            ),
            (
                &["TableStore", "write_dataset"][..],
                "TableStore::write_dataset(",
            ),
            (&["Dataset", "write"][..], "Dataset::write("),
            (&["SnapshotHandle", "new"][..], "SnapshotHandle::new("),
        ] {
            if path_ends_with(&node.func, suffix) {
                self.record(key);
            }
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let tokens = node.tokens.to_string();
        let macro_name = node
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_else(|| "macro".into());
        if macro_name == "include" {
            self.macro_hits
                .push("include! can hide unparsed durable calls".into());
        }
        if ["Omnigraph", "GraphCoordinator", "ManifestCoordinator"]
            .iter()
            .any(|owner| tokens.contains(owner))
            && (tokens.contains("pub async fn") || tokens.contains("impl "))
        {
            self.macro_hits.push(format!(
                "{macro_name}! may generate a public graph/coordinator API"
            ));
        }
        for primitive in DURABLE_PRIMITIVES {
            let key = primitive_inventory_key(primitive);
            if !key.contains("::") && !key.contains('(') && tokens.contains(&format!("{key} (")) {
                self.macro_hits
                    .push(format!("{macro_name}! contains `{key}`"));
            }
        }
        for (marker, key) in [
            ("GraphCoordinator :: init (", "GraphCoordinator::init("),
            (
                "TableStore :: create_empty_dataset (",
                "TableStore::create_empty_dataset(",
            ),
            (
                "TableStore :: append_or_create_batch (",
                "TableStore::append_or_create_batch(",
            ),
            (
                "TableStore :: write_dataset (",
                "TableStore::write_dataset(",
            ),
            ("Dataset :: write (", "Dataset::write("),
            ("SnapshotHandle :: new (", "SnapshotHandle::new("),
        ] {
            if tokens.contains(marker) {
                self.macro_hits
                    .push(format!("{macro_name}! contains `{key}`"));
            }
        }
        if tokens.contains("append (") && tokens.contains("RecoveryAuditRecord") {
            self.macro_hits
                .push(format!("{macro_name}! contains recovery-audit append"));
        }
        visit::visit_macro(self, node);
    }
}

fn primitive_inventory_key(primitive: &str) -> String {
    match primitive {
        "GraphCoordinator::init("
        | "TableStore::create_empty_dataset("
        | "TableStore::append_or_create_batch("
        | "TableStore::write_dataset("
        | "Dataset::write("
        | ".raw_dataset_append("
        | ".append(RecoveryAuditRecord"
        | "SnapshotHandle::new(" => primitive.to_string(),
        _ => primitive
            .trim_start_matches('.')
            .split('(')
            .next()
            .expect("durable primitive identifier")
            .to_string(),
    }
}

fn call_inventory(ast: &syn::File) -> CallInventory {
    let mut inventory = CallInventory::default();
    inventory.visit_file(ast);
    inventory
}

fn protocol_scan_files(src: &Path) -> Vec<PathBuf> {
    walk_rust_files(src)
        .into_iter()
        .filter(|file| !is_protocol_scan_excluded(src, file))
        .collect()
}

fn is_omnigraph_type(ty: &Type) -> bool {
    is_named_type(ty, "Omnigraph")
}

fn is_named_type(ty: &Type, expected: &str) -> bool {
    matches!(
        ty,
        Type::Path(path)
            if path.path.segments.last().is_some_and(|segment| segment.ident == expected)
    )
}

struct PublicSurfaceCollector<'a> {
    relative: &'a str,
    surfaces: BTreeSet<(String, String)>,
}

impl<'ast> Visit<'ast> for PublicSurfaceCollector<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if cfg_requires_test(&node.attrs) {
            return;
        }
        visit::visit_item_mod(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if cfg_requires_test(&node.attrs) {
            return;
        }
        if node.trait_.is_none() && is_omnigraph_type(&node.self_ty) {
            for item in &node.items {
                let syn::ImplItem::Fn(function) = item else {
                    continue;
                };
                if cfg_requires_test(&function.attrs) {
                    continue;
                }
                if matches!(function.vis, Visibility::Public(_)) && function.sig.asyncness.is_some()
                {
                    self.surfaces
                        .insert((self.relative.to_string(), function.sig.ident.to_string()));
                }
            }
        }
        visit::visit_item_impl(self, node);
    }
}

fn public_graph_surfaces(src: &Path) -> BTreeSet<(String, String)> {
    let mut surfaces = BTreeSet::new();
    for file in walk_rust_files(src) {
        let relative = relative_to_src(src, &file);
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        let mut collector = PublicSurfaceCollector {
            relative: &relative,
            surfaces: BTreeSet::new(),
        };
        collector.visit_file(&ast);
        surfaces.extend(collector.surfaces);

        if relative == "loader/mod.rs" {
            for item in &ast.items {
                let Item::Fn(function) = item else {
                    continue;
                };
                if matches!(function.vis, Visibility::Public(_)) && function.sig.asyncness.is_some()
                {
                    surfaces.insert((relative.clone(), function.sig.ident.to_string()));
                }
            }
        }
    }
    surfaces
}

fn low_level_async_surfaces(src: &Path, owner: &str) -> BTreeSet<(String, String, String)> {
    let mut surfaces = BTreeSet::new();
    for file in walk_rust_files(src) {
        let relative = relative_to_src(src, &file);
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        for item in &ast.items {
            let Item::Impl(implementation) = item else {
                continue;
            };
            if cfg_requires_test(&implementation.attrs)
                || implementation.trait_.is_some()
                || !is_named_type(&implementation.self_ty, owner)
            {
                continue;
            }
            for item in &implementation.items {
                let syn::ImplItem::Fn(function) = item else {
                    continue;
                };
                if cfg_requires_test(&function.attrs)
                    || function.sig.asyncness.is_none()
                    || matches!(function.vis, Visibility::Inherited)
                {
                    continue;
                }
                surfaces.insert((
                    relative.clone(),
                    owner.to_string(),
                    function.sig.ident.to_string(),
                ));
            }
        }
    }
    surfaces
}

#[test]
fn graph_write_surfaces_are_registered() {
    let src = engine_src_root();
    let mut registered = BTreeMap::new();
    for surface in WRITE_SURFACES {
        let key = (surface.file.to_string(), surface.function.to_string());
        assert!(
            registered.insert(key.clone(), surface.protocol).is_none(),
            "duplicate graph-write surface registration: {}::{}",
            key.0,
            key.1
        );
    }

    let discovered = public_graph_surfaces(&src);

    let registered_keys = registered.keys().cloned().collect::<BTreeSet<_>>();
    let read_only = READ_ONLY_SURFACES
        .iter()
        .map(|(file, function)| (file.to_string(), function.to_string()))
        .collect::<BTreeSet<_>>();
    assert!(
        registered_keys.is_disjoint(&read_only),
        "a public surface cannot be both a graph writer and read-only"
    );
    let classified = registered_keys
        .union(&read_only)
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing = classified
        .difference(&discovered)
        .cloned()
        .collect::<Vec<_>>();
    let unregistered = discovered
        .difference(&classified)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty() && unregistered.is_empty(),
        "public graph API registry drifted. Missing definitions: {missing:?}. \
         Unclassified public async functions: {unregistered:?}"
    );
}

#[test]
fn low_level_coordinator_surfaces_are_registered() {
    let src = engine_src_root();
    let mut registered = BTreeMap::new();
    for (file, owner, function, protocol) in LOW_LEVEL_WRITE_SURFACES {
        let key = (
            (*file).to_string(),
            (*owner).to_string(),
            (*function).to_string(),
        );
        assert!(
            registered.insert(key.clone(), *protocol).is_none(),
            "duplicate low-level writer registration: {}::{}",
            key.1,
            key.2
        );
    }
    let read_only = LOW_LEVEL_READ_ONLY_SURFACES
        .iter()
        .map(|(file, owner, function)| {
            (
                (*file).to_string(),
                (*owner).to_string(),
                (*function).to_string(),
            )
        })
        .collect::<BTreeSet<_>>();
    let registered_keys = registered.keys().cloned().collect::<BTreeSet<_>>();
    assert!(
        registered_keys.is_disjoint(&read_only),
        "a low-level coordinator surface cannot be both a writer and read-only"
    );

    let graph_surfaces = low_level_async_surfaces(&src, "GraphCoordinator");
    let manifest_surfaces = low_level_async_surfaces(&src, "ManifestCoordinator");
    let discovered = graph_surfaces
        .union(&manifest_surfaces)
        .cloned()
        .collect::<BTreeSet<_>>();
    let classified = registered_keys
        .union(&read_only)
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing = classified
        .difference(&discovered)
        .cloned()
        .collect::<Vec<_>>();
    let unregistered = discovered
        .difference(&classified)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty() && unregistered.is_empty(),
        "low-level coordinator API registry drifted. Missing definitions: {missing:?}. \
         Unclassified crate-visible async functions: {unregistered:?}"
    );
}

#[test]
fn graph_visible_write_chokepoints_are_registered() {
    let src = engine_src_root();
    let mut expected = BTreeMap::new();
    let mut labels = BTreeMap::new();
    for callsite in DURABLE_CALLS {
        let key = (callsite.file.to_string(), callsite.primitive.to_string());
        assert!(
            expected.insert(key.clone(), callsite.count).is_none(),
            "duplicate durable-call registration for {} `{}`",
            key.0,
            key.1
        );
        labels.insert(key, callsite.protocol.label());
    }

    let mut observed = BTreeMap::new();
    for file in protocol_scan_files(&src) {
        let relative = relative_to_src(&src, &file);
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        let inventory = call_inventory(&ast);
        assert!(
            inventory.macro_hits.is_empty(),
            "{} hides durable primitive identifiers inside macro token streams, \
             which the structural call scanner cannot classify: {:?}",
            relative,
            inventory.macro_hits
        );
        for primitive in DURABLE_PRIMITIVES {
            let inventory_key = primitive_inventory_key(primitive);
            let count = inventory.counts.get(&inventory_key).copied().unwrap_or(0);
            if count > 0 {
                observed.insert((relative.clone(), primitive.to_string()), count);
            }
        }
    }

    let mut violations = Vec::new();
    for (key, count) in &observed {
        match expected.get(key) {
            Some(expected_count) if expected_count == count => {}
            Some(expected_count) => violations.push(format!(
                "{} `{}`: observed {count}, registered {expected_count} ({})",
                key.0,
                key.1,
                labels.get(key).expect("label for registered callsite")
            )),
            None => violations.push(format!(
                "{} `{}`: observed {count}, no registered protocol",
                key.0, key.1
            )),
        }
    }
    for (key, count) in &expected {
        if !observed.contains_key(key) {
            violations.push(format!(
                "{} `{}`: registered {count} ({}) but observed 0",
                key.0,
                key.1,
                labels.get(key).expect("label for registered callsite")
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "graph-write chokepoint registry drifted. Every new durable effect, \
         visibility publish, recovery-authority transition, native ref mutation, \
         or explicit physical/scratch exception must be dispositioned here; a file \
         allow-list is insufficient because a second caller in an approved file is \
         still a new writer:\n  {}",
        violations.join("\n  ")
    );
}

#[test]
fn structural_call_scanner_counts_method_and_ufcs_not_text() {
    let ast = parse_rust_source(
        r#"
        fn commit_staged_exact() {}
        fn exercise(storage: &Storage) {
            storage.commit_staged_exact();
            TableStorage::commit_staged_exact(storage);
            let _ = "commit_staged_exact()";
            // storage.commit_staged_exact();
        }
        "#,
        "scanner method/UFCS self-test",
    );
    let inventory = call_inventory(&ast);
    assert_eq!(inventory.counts.get("commit_staged_exact"), Some(&2));
}

#[test]
fn structural_call_scanner_skips_test_module_but_keeps_later_production() {
    let ast = parse_rust_source(
        r#"
        #[cfg(test)]
        mod tests {
            fn helper(storage: &Storage) {
                storage.commit_staged_exact();
            }
        }

        fn production_after_tests(storage: &Storage) {
            storage.commit_staged_exact();
        }
        "#,
        "scanner cfg(test) self-test",
    );
    let inventory = call_inventory(&ast);
    assert_eq!(inventory.counts.get("commit_staged_exact"), Some(&1));
}

#[test]
fn structural_call_scanner_closes_raw_dataset_and_ufcs_routes() {
    let ast = parse_rust_source(
        r#"
        fn exercise(ds: &Dataset, storage: &Storage, audit: &mut RecoveryAudit) {
            ds.append(reader, params);
            Dataset::append(ds, reader, params);
            <Dataset>::merge_insert(ds, params);
            ds.add_columns(transforms, None);
            Dataset::write(reader, uri, params);
            TableStore::write_dataset(uri, batch);
            storage.delete(uri);
            StorageAdapter::write_text(storage, uri, contents);
            audit.append(RecoveryAuditRecord {});
        }
        "#,
        "scanner raw Dataset/UFCS self-test",
    );
    let inventory = call_inventory(&ast);
    assert_eq!(inventory.counts.get(".raw_dataset_append("), Some(&2));
    assert_eq!(inventory.counts.get("merge_insert"), Some(&1));
    assert_eq!(inventory.counts.get("add_columns"), Some(&1));
    assert_eq!(inventory.counts.get("Dataset::write("), Some(&1));
    assert_eq!(inventory.counts.get("TableStore::write_dataset("), Some(&1));
    assert_eq!(inventory.counts.get("delete"), Some(&1));
    assert_eq!(inventory.counts.get("write_text"), Some(&1));
    assert_eq!(
        inventory.counts.get(".append(RecoveryAuditRecord"),
        Some(&1)
    );
}

#[test]
fn structural_call_scanner_skips_test_functions_and_rejects_hidden_shapes() {
    let ast = parse_rust_source(
        r#"
        use crate::table_store::TableStore as Store;

        #[cfg(test)]
        fn fixture(ds: &Dataset) {
            ds.append(reader, params);
        }

        fn production(ds: &Dataset) {
            ds.append(reader, params);
        }

        macro_rules! expose {
            () => {
                impl Omnigraph {
                    pub async fn transact(&self) {}
                }
            }
        }
        include!("generated.rs");
        "#,
        "scanner hidden-shape self-test",
    );
    let inventory = call_inventory(&ast);
    assert_eq!(inventory.counts.get(".raw_dataset_append("), Some(&1));
    assert!(
        inventory
            .macro_hits
            .iter()
            .any(|hit| hit.contains("TableStore") && hit.contains("Store"))
    );
    assert!(
        inventory
            .macro_hits
            .iter()
            .any(|hit| hit.contains("public graph/coordinator API"))
    );
    assert!(
        inventory
            .macro_hits
            .iter()
            .any(|hit| hit.contains("include!"))
    );
}

#[test]
fn lexical_allow_list_matches_only_exact_source_paths() {
    let src = Path::new("/engine/src");
    assert!(is_allow_listed(
        src,
        Path::new("/engine/src/table_store.rs")
    ));
    assert!(is_allow_listed(
        src,
        Path::new("/engine/src/db/manifest/publisher.rs")
    ));
    assert!(!is_allow_listed(
        src,
        Path::new("/engine/src/nested/table_store.rs")
    ));
    assert!(!is_allow_listed(
        src,
        Path::new("/engine/src/db/manifest/backdoor.rs")
    ));
}

#[test]
fn protocol_scan_exclusions_match_only_exact_gateway_files() {
    let src = Path::new("/engine/src");
    assert!(is_protocol_scan_excluded(
        src,
        Path::new("/engine/src/db/manifest/publisher.rs")
    ));
    assert!(!is_protocol_scan_excluded(
        src,
        Path::new("/engine/src/db/manifest/recovery.rs")
    ));
    assert!(!is_protocol_scan_excluded(
        src,
        Path::new("/engine/src/db/manifest/backdoor.rs")
    ));
}

#[test]
fn public_snapshot_and_storage_boundaries_do_not_leak_writable_datasets() {
    let src = engine_src_root();
    let lib_contents = std::fs::read_to_string(src.join("lib.rs")).unwrap();
    let lib = parse_rust_source(&lib_contents, "lib.rs");

    for module_name in ["runtime_cache", "storage_layer", "table_store"] {
        let visibility = lib.items.iter().find_map(|item| match item {
            Item::Mod(module) if module.ident == module_name => Some(&module.vis),
            _ => None,
        });
        assert!(
            matches!(
                visibility,
                Some(Visibility::Restricted(restricted)) if restricted.path.is_ident("crate")
            ),
            "{module_name} exposes raw Dataset/storage capabilities and must remain crate-private"
        );
    }

    let dangerous_storage_types = [
        "TableStore",
        "TableStorage",
        "SnapshotHandle",
        "StagedHandle",
        "ExactCommitOutcome",
        "IndexBuildSpec",
        "StagedTransactionIdentity",
        "StagedWrite",
    ];
    for item in &lib.items {
        match item {
            Item::Use(export) if matches!(export.vis, Visibility::Public(_)) => {
                for dangerous in dangerous_storage_types {
                    assert!(
                        !use_tree_contains_identifier(&export.tree, dangerous),
                        "lib.rs must not publicly re-export crate-private storage type `{dangerous}`"
                    );
                }
                assert!(
                    !(use_tree_contains_glob(&export.tree)
                        && (use_tree_contains_identifier(&export.tree, "table_store")
                            || use_tree_contains_identifier(&export.tree, "storage_layer")
                            || use_tree_contains_identifier(&export.tree, "runtime_cache"))),
                    "lib.rs must not glob-re-export a crate-private raw storage module"
                );
            }
            Item::Type(alias) if matches!(alias.vis, Visibility::Public(_)) => {
                for dangerous in dangerous_storage_types {
                    assert!(
                        !type_contains_identifier(&alias.ty, dangerous),
                        "lib.rs must not expose crate-private storage type `{dangerous}` through public alias `{}`",
                        alias.ident
                    );
                }
            }
            _ => {}
        }
    }

    let manifest_contents = std::fs::read_to_string(src.join("db/manifest.rs")).unwrap();
    let manifest = parse_rust_source(&manifest_contents, "db/manifest.rs");
    for owner in ["SnapshotTable", "SnapshotScanner"] {
        let structure = manifest.items.iter().find_map(|item| match item {
            Item::Struct(structure) if structure.ident == owner => Some(structure),
            _ => None,
        });
        let structure = structure.unwrap_or_else(|| panic!("missing public {owner}"));
        assert!(matches!(structure.vis, Visibility::Public(_)));
        assert!(
            structure
                .fields
                .iter()
                .all(|field| matches!(field.vis, Visibility::Inherited)),
            "{owner} fields must remain private"
        );
    }

    for item in &manifest.items {
        let Item::Impl(implementation) = item else {
            continue;
        };
        if implementation.trait_.is_some()
            || !(is_named_type(&implementation.self_ty, "Snapshot")
                || is_named_type(&implementation.self_ty, "SnapshotTable")
                || is_named_type(&implementation.self_ty, "SnapshotScanner"))
        {
            continue;
        }
        for item in &implementation.items {
            let syn::ImplItem::Fn(function) = item else {
                continue;
            };
            if !matches!(function.vis, Visibility::Public(_)) {
                continue;
            }
            for forbidden in ["Dataset", "Scanner", "ExecutionPlan"] {
                assert!(
                    !return_type_contains_identifier(&function.sig.output, forbidden),
                    "{owner}::{} must not return raw `{forbidden}`",
                    function.sig.ident,
                    owner =
                        type_final_ident(&implementation.self_ty).expect("checked Snapshot owner")
                );
            }
        }
    }
}

#[test]
fn graph_manifest_writer_methods_are_not_public_escape_hatches() {
    let src = engine_src_root();
    for (relative, owner) in [
        ("db/graph_coordinator.rs", "GraphCoordinator"),
        ("db/manifest.rs", "ManifestCoordinator"),
    ] {
        let file = src.join(relative);
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, relative);
        let visibility = ast.items.iter().find_map(|item| match item {
            Item::Struct(item) if item.ident == owner => Some(&item.vis),
            _ => None,
        });
        assert!(
            matches!(
                visibility,
                Some(Visibility::Restricted(restricted)) if restricted.path.is_ident("crate")
            ),
            "{relative}::{owner} must remain crate-private"
        );
    }

    let methods = [
        ("db/manifest.rs", "init"),
        ("db/manifest.rs", "commit"),
        ("db/manifest.rs", "commit_with_expected"),
        ("db/manifest.rs", "commit_changes"),
        ("db/manifest.rs", "commit_changes_with_expected"),
        ("db/manifest.rs", "commit_changes_with_lineage"),
        (
            "db/manifest.rs",
            "commit_changes_with_lineage_and_precondition",
        ),
        ("db/manifest.rs", "create_branch"),
        ("db/manifest.rs", "delete_branch"),
        ("db/graph_coordinator.rs", "init"),
        ("db/graph_coordinator.rs", "branch_create"),
        ("db/graph_coordinator.rs", "branch_delete"),
        ("db/graph_coordinator.rs", "commit_updates_with_actor"),
        (
            "db/graph_coordinator.rs",
            "commit_updates_with_actor_with_expected",
        ),
        (
            "db/graph_coordinator.rs",
            "commit_changes_with_intent_and_expected",
        ),
    ];
    for (relative, method) in methods {
        let file = src.join(relative);
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, relative);
        let visibilities = ast
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Impl(item) => Some(item),
                _ => None,
            })
            .flat_map(|implementation| implementation.items.iter())
            .filter_map(|item| match item {
                syn::ImplItem::Fn(function) if function.sig.ident == method => Some(&function.vis),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            visibilities.len() == 1,
            "{relative}::{method} must resolve to exactly one inherent method, found {}",
            visibilities.len()
        );
        assert!(
            matches!(
                visibilities[0],
                Visibility::Restricted(restricted) if restricted.path.is_ident("crate")
            ),
            "{relative}::{method} is a graph-writer escape hatch; it must remain crate-private"
        );
    }
}

#[test]
fn engine_code_does_not_call_forbidden_lance_apis() {
    let src = engine_src_root();
    let mut violations = Vec::new();

    // The final inline-commit storage escape hatch was retired with staged
    // full-table vector indexing. Pin its absence across the storage trait and
    // Omnigraph accessor so a future change cannot silently reopen it.
    for relative in ["storage_layer.rs", "db/omnigraph.rs"] {
        let file = src.join(relative);
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        for forbidden in ["InlineCommitResidual", "storage_inline_residual"] {
            assert!(
                !contents.contains(forbidden),
                "{} must not reintroduce retired inline storage symbol `{forbidden}`",
                file.display()
            );
        }
    }

    for file in walk_rust_files(&src) {
        if is_allow_listed(&src, &file) {
            continue;
        }
        let contents = match std::fs::read_to_string(&file) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let lines: Vec<&str> = contents.lines().collect();
        for (idx, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            // Skip comment-only lines — references to forbidden API
            // names in doc-comments, design notes, or residual-marker
            // comments are documentation, not code use. The trait
            // surface (sealed + trait-only) is the actual enforcement;
            // this test only catches code use.
            if trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with("*") {
                continue;
            }
            // Allow lines marked with the sentinel on the SAME line or
            // the immediately preceding line.
            if line.contains(SENTINEL) {
                continue;
            }
            if idx > 0 && lines[idx - 1].contains(SENTINEL) {
                continue;
            }
            for pattern in FORBIDDEN_PATTERNS {
                if line.contains(pattern) {
                    let rel = file
                        .strip_prefix(&src)
                        .unwrap_or(&file)
                        .display()
                        .to_string();
                    violations.push(format!(
                        "{}:{}: forbidden pattern `{}` — {}",
                        rel,
                        idx + 1,
                        pattern,
                        line.trim()
                    ));
                }
            }
        }
    }

    if !violations.is_empty() {
        panic!(
            "Forbidden-API guard found {} violation(s) in engine code. \
             Engine code MUST route through the `TableStorage` trait (or its \
             inherent counterparts on `TableStore`) instead of calling Lance's \
             inline-commit APIs directly. If a use is genuinely justified, add \
             the comment `// forbidden-api-allow: <reason>` on the same line or \
             the line above.\n\nViolations:\n  {}",
            violations.len(),
            violations.join("\n  ")
        );
    }
}
