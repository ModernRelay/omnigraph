//! Forbidden-API and graph-write protocol guard tests.
//!
//! Engine code (`exec/`, `db/omnigraph/`, `loader/`, `changes/`) MUST NOT
//! call Lance's inline-commit data-write APIs directly. The
//! `Storage` trait (`crate::storage_layer::TableStorage`) is the canonical
//! surface; graph rows route through the exact-id `stage_keyed_write` adapter,
//! while the remaining staged primitives plus `commit_staged` are the only way
//! to advance Lance HEAD. Bare `stage_append` is compiled only for private
//! primitive tests, never as a graph-visible keyed route.
//!
//! The raw storage modules are crate-private and the trait is sealed (only
//! `TableStore` implements it), so Rust visibility is the primary boundary.
//! This test is **defense in depth**: it catches known cases where engine code
//! reaches around that boundary by importing `lance::dataset::*` types directly.
//!
//! ## How it works
//!
//! The walks cover `crates/omnigraph/src/**/*.rs` plus the `GUARDED_CRATES`
//! sources (`crates/omnigraph-core/src`, `crates/omnigraph-catalog/src`), labeled
//! by crate (`omnigraph-core/<relative>`, `omnigraph-catalog/<relative>`); the
//! durable-call inventory and the callable-gateway registry also walk
//! `crates/omnigraph-storage/src` (`omnigraph-storage/<relative>`), and the
//! public re-export walk reads the engine alone. No walk reads a `tests/`
//! directory. The
//! legacy forbidden-Lance check keeps a lexical deny-list for type and builder
//! construction, while the graph-write guard parses Rust with `syn` so split
//! calls, method syntax, and UFCS are counted structurally. Lines whose
//! preceding line contains the sentinel comment
//! `// forbidden-api-allow: <reason>` are exempt from the lexical deny-list —
//! reviewers see the sentinel in diff and can ask whether the exemption is
//! justified.
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
//! ## What's deliberately outside the lexical deny-list
//!
//! - `crates/omnigraph/src/table_store.rs` — the crate-private storage layer.
//!   The forbidden Lance APIs live here legitimately.
//! - The exact manifest gateway implementations listed below — bootstrap,
//!   namespace plumbing, row-level publishing, and recovery — plus their
//!   out-of-line test module.
//! - `crates/omnigraph/src/storage_layer.rs` — IS the trait module.
//!
//! Every production file in that lexical allow-list is still parsed by the
//! structural registry. Only standalone files compiled exclusively through a
//! parent `#[cfg(test)]` module are omitted from that pass. The callable
//! surfaces of the storage and manifest gateways are also pinned exactly, so a
//! new wrapper cannot hide behind a new primitive name.
//!
//! ## Allow-list shape
//!
//! After the exact EnsureIndices adapter, `db.storage()` (`&dyn TableStorage`)
//! exposes only staged primitives + reads and there is no separate inline
//! residual surface. Vector index creation uses pinned Lance's full-table
//! `execute_uncommitted` path inside `stage_create_indices`; `delete` likewise
//! migrated to `stage_delete` in MR-A (Lance 7.0 #6658).
//! The dead legacy methods
//! (trait `append_batch` / `merge_insert_batches`, inherent
//! `merge_insert_batch{,es}`, `create_{btree,inverted}_index`) were
//! removed entirely. The lexical pass catches direct `lance::*` misuse outside
//! the implementation boundary; the structural pass exact-counts the raw
//! builder/commit shapes inside that boundary as well.

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
    // Raw dataset OPENS — all reads must route through
    // `Snapshot::open_lance_dataset` (the held-handle cache + shared Session,
    // Fix 3). Only the instrumented opener
    // (`omnigraph-core/instrumentation.rs`) and the storage/manifest layers (allow-listed below)
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
    "table_store.rs",                    // The storage layer itself.
    "table_store/staged_tests.rs",       // Unit tests for private staged primitives.
    "storage_layer.rs",                  // The trait module.
    "db/graph_coordinator.rs",           // Drives the manifest publisher / branch coordinator.
    "omnigraph-catalog/graph.rs",        // Bootstraps the manifest and commit datasets.
    "omnigraph-catalog/namespace.rs",    // Opens manifest datasets through the shared namespace.
    "omnigraph-catalog/publisher.rs",    // Lowest row-level manifest publish gateway.
    "omnigraph-catalog/tests.rs",        // Out-of-line tests for the trusted gateways.
    "db/catalog_tests.rs", // Catalog tests that construct `Omnigraph`, so engine-side.
    "core_tests.rs", // Core tests that need `TableStore` or `seams::FailScenario`, so engine-side.
    "omnigraph-core/instrumentation.rs", // The instrumented dataset opener.
    "db/upgrade.rs",
    "db/upgrade/tests.rs",
    "omnigraph-catalog/migrations.rs",
    "omnigraph-catalog/history.rs",
    "omnigraph-catalog/commit.rs",
    "omnigraph-core/lance_clone.rs",
];

/// Out-of-line modules are parsed as standalone files, so the walker cannot see
/// their parent's `mod` gate: `#[cfg(test)]` for each entry but
/// `omnigraph-catalog/namespace.rs`, gated `#[cfg(any(test, feature = "test-util"))]`.
/// Production gateway and primitive-definition files are deliberately *not*
/// excluded: their current durable calls are exact-registered below, so adding
/// a new call inside a trusted implementation still requires an explicit
/// protocol disposition.
const PROTOCOL_SCAN_EXCLUDED_FILES: &[&str] = &[
    "table_store/staged_tests.rs",
    "omnigraph-catalog/namespace.rs",
    "omnigraph-catalog/tests.rs",
    "db/upgrade/tests.rs",
    "db/catalog_tests.rs",
    "core_tests.rs",
    "db/system_roles_tests.rs", // Test-only raw Lance rename fixture.
];

const SENTINEL: &str = "// forbidden-api-allow:";

/// Closed classification of the supported public async inherent `Omnigraph`
/// and loader graph-write surfaces. This lives in the guard, not as a runtime
/// label a new caller could simply lie about. The callsite registry below
/// observes the registered durable-call shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteProtocol {
    Exact(&'static str),
    Composed(&'static str),
    ManifestAdoption,
    NativeRefControl,
    PhysicalOnly,
    EphemeralScratch,
    TestOnly,
    Bootstrap,
    ReadOnlyAccess,
    ReadTaskCancellation,
}

impl WriteProtocol {
    fn label(self) -> String {
        match self {
            Self::Exact(name) => format!("exact adapter ({name})"),
            Self::Composed(name) => format!("composed protocol ({name})"),
            Self::ManifestAdoption => "manifest adoption".into(),
            Self::NativeRefControl => "native ref control".into(),
            Self::PhysicalOnly => "physical-only maintenance".into(),
            Self::EphemeralScratch => "ephemeral scratch".into(),
            Self::TestOnly => "test/failpoint-only".into(),
            Self::Bootstrap => "bootstrap".into(),
            Self::ReadOnlyAccess => "read-only raw snapshot access".into(),
            Self::ReadTaskCancellation => {
                "cancel a queued Tokio read worker; no storage effect".into()
            }
        }
    }
}

const MUTATION_V9: WriteProtocol = WriteProtocol::Exact("Mutation v9");
const LOAD_V9: WriteProtocol = WriteProtocol::Exact("Load v9");
const SCHEMA_V9: WriteProtocol =
    WriteProtocol::Exact("schema apply: table pins and schema contract in one manifest CAS");
const SYSTEM_COLUMNS_V9: WriteProtocol =
    WriteProtocol::Exact("system-column upgrade: detached renames and schema contract in one CAS");
const MERGE_V9: WriteProtocol = WriteProtocol::Exact("BranchMerge v9");
const INDICES_V9: WriteProtocol = WriteProtocol::Exact("EnsureIndices v9");
const OPTIMIZE_V9: WriteProtocol =
    WriteProtocol::Exact("Optimize (RFC 0067 detached rewrite and exact pin CAS)");

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
    "db/omnigraph.rs" => WriteProtocol::Bootstrap => ["init", "init_with_options", "init_with_storage"],
    "db/omnigraph/prepared_create.rs" => WriteProtocol::Bootstrap => ["apply_prepared_graph_create"],
    "db/omnigraph/prepared_create.rs" => WriteProtocol::Exact("quiesced birth settlement: exact init claim, bounded original empty tables, no published manifest or refs") => ["settle_prepared_graph_create_after_quiescence"],
    "db/omnigraph.rs" => WriteProtocol::Composed("read-write admission + owned local-root create-if-absent capability probe") => ["open", "open_with_storage"],
    "exec/mutation.rs" => MUTATION_V9 => ["mutate", "mutate_with_receipt", "mutate_as", "mutate_as_with_receipt", "mutate_as_with_expected_head", "mutate_as_with_expected_head_receipt"],
    "loader/mod.rs" => LOAD_V9 => ["load_jsonl", "load_jsonl_file", "load", "load_with_receipt", "load_file", "load_graph_batch"],
    "loader/mod.rs" => WriteProtocol::Composed("optional branch create, then Load v9") => ["load_as", "load_as_with_receipt", "load_file_as", "load_file_as_with_receipt", "load_graph_batch_as", "load_graph_batch_as_with_receipt"],
    "loader/mod.rs" => WriteProtocol::Composed("branch create when absent, then Load v9 alias") => ["ingest", "ingest_as", "ingest_file", "ingest_file_as"],
    "db/omnigraph.rs" => SCHEMA_V9 => ["apply_schema", "apply_schema_as", "apply_schema_as_with_catalog_check", "apply_prepared_schema_as"],
    "db/omnigraph.rs" => WriteProtocol::Exact("schema settlement: strict numeric neutral lineage publication") => ["settle_prepared_schema_as"],
    "db/omnigraph.rs" => SYSTEM_COLUMNS_V9 => ["upgrade_system_columns", "upgrade_system_columns_as"],
    "exec/merge.rs" => MERGE_V9 => ["branch_merge", "branch_merge_as"],
    "db/omnigraph.rs" => INDICES_V9 => [
        "ensure_indices", "ensure_indices_on",
        "rebuild_full_text_indices_on", "rebuild_full_text_indices_on_as",
    ],
    "db/omnigraph.rs" => WriteProtocol::TestOnly => ["failpoint_publish_table_head_without_index_rebuild_for_test", "init_with_legacy_system_columns_for_tests"],
    "db/omnigraph.rs" => OPTIMIZE_V9 => ["optimize"],
    "db/omnigraph.rs" => WriteProtocol::ManifestAdoption => ["repair"],
    "db/omnigraph.rs" => WriteProtocol::PhysicalOnly => ["cleanup"],
    "db/omnigraph.rs" => WriteProtocol::NativeRefControl => ["branch_create", "branch_create_as", "branch_create_from", "branch_create_from_as", "branch_delete", "branch_delete_as"],
}

// Every public async inherent Omnigraph or Session method (wherever its impl
// lives) must appear in either this read-only set or WRITE_SURFACES. Within
// that supported API shape this is
// name-independent: a newly named `transact`, `publish`, or `vacuum` method
// cannot evade discovery.
const READ_ONLY_SURFACES: &[(&str, &str)] = &[
    ("db/omnigraph/prepared_create.rs", "prepare_graph_create"),
    (
        "db/omnigraph/prepared_create.rs",
        "reconcile_prepared_graph_create",
    ),
    ("db/omnigraph.rs", "open_read_only"),
    ("db/omnigraph.rs", "refresh"),
    ("db/omnigraph.rs", "cleanup_plan"),
    ("db/omnigraph.rs", "cleanup_plan_missing_paths"),
    ("db/omnigraph.rs", "cleanup_plan_path_snapshot"),
    ("db/omnigraph.rs", "open_read_only_with_storage"),
    ("db/omnigraph.rs", "ensure_no_pending_recovery"),
    ("db/omnigraph.rs", "manifest_has_external_base_paths"),
    ("db/omnigraph/export.rs", "capture_served_export_cut"),
    (
        "db/omnigraph/export.rs",
        "capture_served_change_baseline_cut",
    ),
    ("db/omnigraph.rs", "plan_schema"),
    ("db/omnigraph.rs", "preview_schema_apply"),
    ("db/omnigraph.rs", "prepare_schema_apply_as"),
    ("db/omnigraph.rs", "prepare_schema_settlement_as"),
    ("db/omnigraph.rs", "reconcile_schema_apply_as"),
    ("db/omnigraph.rs", "snapshot_of"),
    ("db/omnigraph.rs", "graph_manifest_version_of"),
    ("db/omnigraph.rs", "internal_schema_version_of"),
    ("db/omnigraph.rs", "internal_schema_version_at"),
    ("db/omnigraph.rs", "resolved_branch_of"),
    ("db/omnigraph.rs", "sync_branch"),
    ("db/omnigraph.rs", "resolve_snapshot"),
    ("db/omnigraph.rs", "diff_between"),
    ("db/omnigraph.rs", "diff_commits"),
    ("db/omnigraph.rs", "commit_changes_page"),
    ("db/omnigraph.rs", "poll_change_feed"),
    ("db/omnigraph.rs", "capture_change_baseline"),
    ("db/omnigraph.rs", "entity_at_target"),
    ("db/omnigraph.rs", "entity_at"),
    ("db/omnigraph.rs", "snapshot_at_graph_manifest_version"),
    ("db/omnigraph.rs", "export_jsonl"),
    ("db/omnigraph.rs", "export_jsonl_to_writer"),
    ("db/omnigraph.rs", "export_jsonl_unordered_to_writer"),
    ("db/omnigraph.rs", "graph_index"),
    ("blob.rs", "read_blob_at"),
    ("db/omnigraph.rs", "branch_list"),
    ("db/omnigraph.rs", "get_commit"),
    ("db/omnigraph.rs", "list_commits"),
    ("exec/query_doors.rs", "query"),
    ("exec/query_doors.rs", "query_with_head"),
    ("exec/query_doors.rs", "run_query_at"),
    ("exec/query_doors.rs", "explain_query"),
    ("exec/query_doors.rs", "query_inspected"),
    ("exec/query_doors.rs", "replay_bound_plan"),
];

// Every crate-visible async method on the two low-level coordinators is also
// classified. The types themselves are crate-private, but an unregistered
// crate-visible wrapper would otherwise create a new internal route to the
// manifest publisher without changing the durable gateway count.
const LOW_LEVEL_READ_ONLY_SURFACES: &[(&str, &str, &str)] = &[
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "prepare_open_with_contract",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "open_prepared_with_lineage_and_contract",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "open_with_contract",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "open_with_lineage_and_contract",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "read_schema_contract",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "read_schema_contract",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "read_schema_contract_at",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "read_schema_contract_for_snapshot",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "internal_schema_stamp_for_snapshot",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "finish_init_with_storage",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "open_exact_genesis_with_storage",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "open_with_session",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "open_branch_with_session",
    ),
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
        "load_commits",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "effective_graph_head",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "captured_lineage",
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
        "snapshot_at_graph_manifest_version",
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
        "resolve_commit_range",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "capture_change_cut",
    ),
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "build_change_feed_cut",
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
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "finish_init",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "open_exact_genesis",
    ),
    ("omnigraph-catalog/lib.rs", "ManifestCoordinator", "open"),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "open_with_session",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "open_at_branch",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "open_at_branch_with_session",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "snapshot_at",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "snapshot_at_in",
    ),
    (
        "omnigraph-catalog/retention.rs",
        "ManifestCoordinator",
        "pinned_graph_commit",
    ),
    (
        "omnigraph-catalog/retention.rs",
        "ManifestCoordinator",
        "retired_commit_graphs",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "collector_branch_under_control_gates",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "table_registrations_under_control_gates",
    ),
    ("omnigraph-catalog/lib.rs", "ManifestCoordinator", "refresh"),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "read_head_at",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "read_held_at",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "record_in_live_branches",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "ensure_incarnation_live",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "branch_identifier",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "probe_latest_incarnation",
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "list_graph_branches",
    ),
];

const LOW_LEVEL_WRITE_SURFACES: &[(&str, &str, &str, WriteProtocol)] = &[
    (
        "db/graph_coordinator.rs",
        "GraphCoordinator",
        "init_commit_with_session",
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
        "branch_delete_captured",
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
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "init_commit",
        WriteProtocol::Bootstrap,
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "commit_changes_with_lineage_and_precondition",
        WriteProtocol::Exact("lowest manifest publisher gateway"),
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "create_branch",
        WriteProtocol::NativeRefControl,
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "delete_branch",
        WriteProtocol::NativeRefControl,
    ),
    (
        "omnigraph-catalog/lib.rs",
        "ManifestCoordinator",
        "delete_branch_with_expected",
        WriteProtocol::NativeRefControl,
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayDisposition {
    ReadOrPure,
    StageOnly,
    Durable(WriteProtocol),
}

impl GatewayDisposition {
    fn label(self) -> String {
        match self {
            Self::ReadOrPure => "read/pure".into(),
            Self::StageOnly => "stage-only physical effect".into(),
            Self::Durable(protocol) => protocol.label(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct GatewaySurface {
    file: &'static str,
    owner: &'static str,
    function: &'static str,
    disposition: GatewayDisposition,
}

macro_rules! gateway_surfaces {
    ($($file:literal => $owner:literal => $disposition:expr => [$($function:literal),+ $(,)?]),+ $(,)?) => {
        const GATEWAY_SURFACES: &[GatewaySurface] = &[
            $($(GatewaySurface {
                file: $file,
                owner: $owner,
                function: $function,
                disposition: $disposition,
            },)+)+
        ];
    };
}

/// The owner a registry row names for a crate-visible free `fn`.
const FREE_FUNCTION_OWNER: &str = "(free fn)";

/// Files whose crate-visible free `fn`s the registry pins: the index and staging
/// helpers that were `TableStore` methods before the crate split, plus the
/// exact module containing the bounded manifest scan reader.
const FREE_FUNCTION_GATEWAY_FILES: &[&str] = &[
    "omnigraph-core/dataset_index.rs",
    "omnigraph-core/staging.rs",
    "omnigraph-core/instrumentation/small_manifest_reads.rs",
];

// Closed callable surface for the primitive/gateway types themselves. The raw
// call inventory below catches body growth; this registry catches a new wrapper
// or an entirely new primitive name before a crate-internal caller can use it.
gateway_surfaces! {
    "omnigraph-core/instrumentation/small_manifest_reads.rs" => "(free fn)" => GatewayDisposition::ReadOrPure => [
        "manifest_scan_dataset",
    ],
    "omnigraph-core/storage.rs" => "StorageAdapter" => GatewayDisposition::ReadOrPure => [
        "read_text", "read_text_if_exists", "read_text_if_exists_bounded",
        "read_bytes_if_exists_bounded", "exists",
        "list_dir", "list_dir_bounded", "read_text_versioned",
    ],
    "omnigraph-core/storage.rs" => "StorageAdapter" => GatewayDisposition::Durable(WriteProtocol::Composed("object storage primitive")) => [
        "write_text", "write_bytes", "write_text_if_absent", "rename_text", "delete",
        "write_text_if_match", "delete_prefix",
    ],
    "omnigraph-storage/lib.rs" => "StorageAdapter" => GatewayDisposition::ReadOrPure => [
        "read_text", "read_text_if_exists", "read_text_if_exists_bounded",
        "read_bytes_if_exists_bounded", "read_text_versioned_if_exists_bounded", "exists",
        "list_dir", "list_dir_bounded", "read_text_versioned",
    ],
    "omnigraph-storage/lib.rs" => "StorageAdapter" => GatewayDisposition::Durable(WriteProtocol::Composed("shared object storage primitive")) => [
        "write_text", "write_bytes", "write_text_if_absent", "rename_text", "delete",
        "write_text_if_match", "delete_prefix",
    ],
    "storage_layer.rs" => "TableStorage" => GatewayDisposition::ReadOrPure => [
        "validate_initial_empty_table",
        "transaction_identity",
        "open_snapshot_at_entry", "open_snapshot_at_table", "open_dataset_head",
        "branch_identifier", "list_native_branches",
        "ensure_expected_version", "scan", "scan_with_row_id", "scan_filtered", "scan_batches",
        "count_rows", "count_rows_with_staged",
        "scan_with_staged", "scan_with_pending", "scan_with_pending_materialized_blobs",
        "first_row_id_for_filter", "table_state", "has_btree_index",
        "has_fts_index", "has_vector_index", "root_uri", "dataset_uri", "scan_stream",
        "scan_stream_bounded", "scan_stream_for_rewrite_bounded",
        "scan_proven_insert_delta_bounded",
        "preflight_external_blob_uris", "prepare_keyed_write_batch_with_preflight",
        "prepare_overwrite_blob_references_with_preflight",
        "prepare_keyed_write_batch", "validate_keyed_write_batch", "first_existing_id",
    ],
    "storage_layer.rs" => "TableStorage" => GatewayDisposition::StageOnly => [
        "stage_create", "stage_keyed_write", "stage_proven_strict_insert", "stage_overwrite",
        "stage_rename_columns", "stage_schema_evolution", "stage_delete", "stage_create_indices",
        "stage_compaction", "stage_index_fold",
    ],
    "storage_layer.rs" => "TableStorage" => GatewayDisposition::Durable(WriteProtocol::NativeRefControl) => [
        "force_delete_branch",
    ],
    "storage_layer.rs" => "TableStorage" => GatewayDisposition::Durable(WriteProtocol::Composed("shared staged commit gateway")) => [
        "commit_staged",
    ],
    "storage_layer.rs" => "TableStorage" => GatewayDisposition::Durable(WriteProtocol::Exact("staged create gateway")) => [
        "commit_staged_create_exact",
    ],
    "storage_layer.rs" => "TableStorage" => GatewayDisposition::Durable(WriteProtocol::Exact("staged exact commit gateway")) => [
        "commit_staged_exact",
    ],
    "storage_layer.rs" => "TableStorage" => GatewayDisposition::Durable(WriteProtocol::Exact("RFC 0067 detached staged commit gateway")) => [
        "commit_staged_detached",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::ReadOrPure => [
        "validate_initial_empty_table",
        "transaction_identity",
        "new", "root_uri", "dataset_uri", "open_snapshot_table", "open_at_entry",
        "open_at_entry_verified", "open_dataset_head", "list_native_branches",
        "named_fork_is_absent", "ensure_expected_version",
        "scan_batches", "scan_stream_for_rewrite_bounded",
        "scan_proven_insert_delta_bounded", "include_proven_insert_blob_selection",
        "scan_stream", "scan_stream_bounded",
        "scan_stream_with", "scan_plan_with", "ordered_scan_error", "scan", "scan_with",
        "fts_covers_all_fragments",
        "count_rows",
        "dataset_version", "table_state", "scan_with_staged", "scan_with_pending",
        "scan_with_pending_materialized_blobs", "count_rows_with_staged",
        "has_btree_index", "has_fts_index",
        "has_vector_index", "first_row_id_for_filter",
        "with_external_blob_policy", "preflight_external_blob_uris",
        "preflight_persisted_blob_selection", "prepare_keyed_write_batch_with_preflight",
        "prepare_overwrite_blob_references_with_preflight",
        "prepare_keyed_write_batch", "validate_keyed_write_batch", "first_existing_id",
        "predicted_materialized_blob_batch_bytes",
        "materialize_blob_batch_bounded_with_preflight_cache", "managed_blob_payloads",
        "can_fold_index", "has_foldable_unindexed_fragments", "index_is_vector",
        "plan_table_compaction",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::StageOnly => [
        "stage_create", "stage_keyed_write", "stage_proven_strict_insert", "stage_overwrite",
        "stage_rename_columns", "renamed_schema", "stage_schema_evolution",
        "plan_schema_evolution", "stage_delete", "stage_create_indices", "stage_compaction",
        "stage_index_fold",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::Durable(WriteProtocol::NativeRefControl) => [
        "force_delete_branch",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::Durable(WriteProtocol::Composed("shared staged commit gateway")) => [
        "commit_staged",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::Durable(WriteProtocol::Exact("staged create gateway")) => [
        "commit_staged_create_exact",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::Durable(WriteProtocol::Exact("staged exact commit gateway")) => [
        "commit_staged_exact",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::Durable(WriteProtocol::Exact("RFC 0067 detached staged commit gateway")) => [
        "commit_staged_detached",
    ],
    "table_store.rs" => "TableStore" => GatewayDisposition::Durable(WriteProtocol::EphemeralScratch) => [
        "append_or_create_batch", "create_empty_dataset", "write_dataset",
    ],
    "omnigraph-core/dataset_index.rs" => "(free fn)" => GatewayDisposition::ReadOrPure => [
        "validate_full_text_scan", "validate_full_text_demand", "is_full_text_index",
        "key_column_index_coverage", "has_unindexed_fragments", "user_indices_for_column",
        "has_btree_index_on", "has_fts_index_on", "has_vector_index_on",
    ],
    "omnigraph-core/staging.rs" => "(free fn)" => GatewayDisposition::ReadOrPure => [
        "is_detached_version",
    ],
    "omnigraph-catalog/publisher.rs" => "ManifestBatchPublisher" => GatewayDisposition::ReadOrPure => [
        "cached_rows",
    ],
    "omnigraph-catalog/publisher.rs" => "ManifestBatchPublisher" => GatewayDisposition::Durable(WriteProtocol::Exact("manifest publisher gateway")) => [
        "publish_with_precondition",
    ],
    "omnigraph-catalog/publisher.rs" => "GraphNamespacePublisher" => GatewayDisposition::ReadOrPure => [
        "new_with_session",
    ],
}

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
// adapter is still a new writer and fails this guard. Production storage and
// manifest implementations are included; only standalone test-only sources
// whose parent cfg is invisible to this file walker are excluded.
durable_calls! {
    // Exact token-owned unpublished birth only, after external native/control
    // quiescence. The full bounded deletion set is authenticated first; claim
    // deletion is last, so interrupted settlement can safely finish later.
    ("db/omnigraph/prepared_create.rs", ".delete_prefix(", 2, WriteProtocol::Exact("quiesced token-owned empty birth tables and unpublished manifest files; never graph root or foreign refs")),
    ("db/omnigraph/prepared_create.rs", ".delete(", 1, WriteProtocol::Exact("exact prepared init claim, removed only after all verified birth artifacts")),
    ("db/upgrade.rs", "CommitBuilder::new(", 3, WriteProtocol::Exact("offline storage upgrade: fence, conversion and activation commits with zero retries under main-owned intent")),
    ("db/upgrade.rs", "InsertBuilder::new(", 1, WriteProtocol::Exact("manifest-only conversion rows staged under durable upgrade ownership")),
    ("db/upgrade.rs", ".execute_uncommitted_stream(", 1, WriteProtocol::Exact("manifest-only conversion rows staged under durable upgrade ownership")),
    ("omnigraph-core/fts_compat.rs", ".put(", 1, WriteProtocol::Composed("staged index artifact")),
    ("table_store.rs", ".put(", 1, WriteProtocol::Composed("deleted-ids record spilled to `_omnigraph/deleted_ids/<uuid>.json` before the detached delete commit that names it in its transaction properties; marked by the collector as one of the root's files")),
    // The `__manifest` Create write is the manifest's entire birth: entries,
    // genesis lineage, and the internal-schema stamp all ride the one commit,
    // so the stamp is atomic with birth and no bootstrap write follows it.
    // (A `table_version_management` config key is deliberately not written:
    // neither the pinned Lance substrate nor this crate reads it.)
    ("omnigraph-catalog/graph.rs", "Dataset::write(", 2, WriteProtocol::Bootstrap),
    ("omnigraph-catalog/history.rs", ".commit(", 4, WriteProtocol::ReadOnlyAccess),
    ("omnigraph-catalog/history.rs", ".put_if_absent(", 2, WriteProtocol::Composed("create-only puts of immutable `__history` objects: an extent of settled commit records (`write_extent`), issued by the publisher before its `__manifest` CAS and by branch deletion before the ref is retired, and a schema content object named by its hash")),
    ("omnigraph-catalog/publisher.rs", ".dataset()", 1, WriteProtocol::ReadOnlyAccess),
    ("omnigraph-catalog/publisher.rs", ".publish_with_precondition(", 1, WriteProtocol::Exact("manifest publisher trait forwarding")),
    ("omnigraph-catalog/commit.rs", "InsertBuilder::new(", 1, WriteProtocol::Exact("lowest manifest publisher gateway")),
    ("omnigraph-catalog/commit.rs", ".execute_uncommitted(", 1, WriteProtocol::Exact("lowest manifest publisher gateway")),
    ("omnigraph-catalog/commit.rs", "CommitBuilder::new(", 1, WriteProtocol::Exact("lowest manifest publisher gateway")),
    // The persisted CSR/CSC adjacency artifact (`__graph_index/csr-current.bin`):
    // derived, regenerable topology written ONLY from `optimize`'s tail (never
    // the query path, which only loads), outside graph visibility — a stale or
    // partial object is rejected by its identity stamps + payload digest and
    // rebuilt in memory, so this write can never change a query's result.
    ("graph_index/persist.rs", ".write_bytes(", 1, WriteProtocol::PhysicalOnly),
    ("omnigraph-core/instrumentation.rs", ".write_bytes(", 1, WriteProtocol::Composed("instrumented storage forwarding")),
    ("omnigraph-core/instrumentation.rs", ".write_text(", 1, WriteProtocol::Composed("instrumented storage forwarding")),
    ("omnigraph-core/instrumentation.rs", ".write_text_if_absent(", 1, WriteProtocol::Composed("instrumented storage forwarding")),
    ("omnigraph-core/instrumentation.rs", ".write_text_if_match(", 1, WriteProtocol::Composed("instrumented storage forwarding")),
    ("omnigraph-core/instrumentation.rs", ".rename_text(", 1, WriteProtocol::Composed("instrumented storage forwarding")),
    ("omnigraph-core/instrumentation.rs", ".delete(", 1, WriteProtocol::Composed("instrumented storage forwarding")),
    ("omnigraph-core/instrumentation.rs", ".delete_prefix(", 1, WriteProtocol::Composed("instrumented storage forwarding")),
    ("omnigraph-core/lance_access.rs", ".put_opts(", 1, WriteProtocol::Composed("Lance-realm object-store seam forwarding (dst only)")),
    ("omnigraph-core/storage.rs", ".write_bytes(", 1, WriteProtocol::Composed("engine storage compatibility forwarding")),
    ("omnigraph-core/storage.rs", ".write_text(", 1, WriteProtocol::Composed("engine storage compatibility forwarding")),
    ("omnigraph-core/storage.rs", ".write_text_if_absent(", 1, WriteProtocol::Composed("engine storage compatibility forwarding")),
    ("omnigraph-core/storage.rs", ".write_text_if_match(", 1, WriteProtocol::Composed("engine storage compatibility forwarding")),
    ("omnigraph-core/storage.rs", ".rename_text(", 1, WriteProtocol::Composed("engine storage compatibility forwarding")),
    ("omnigraph-core/storage.rs", ".delete(", 1, WriteProtocol::Composed("engine storage compatibility forwarding")),
    ("omnigraph-core/storage.rs", ".delete_prefix(", 1, WriteProtocol::Composed("engine storage compatibility forwarding")),
    ("omnigraph-storage/lib.rs", ".delete(", 3, WriteProtocol::Composed("storage adapter primitive, including Azure rename source retirement")),
    // One `.put(` beyond upstream's Azure set: the binary `write_bytes`
    // primitive (graph-index artifact), same atomic-visibility PUT contract
    // as `write_text`.
    ("omnigraph-storage/lib.rs", ".put(", 5, WriteProtocol::Composed("object storage put primitive, including Azure rename destination publication and the binary write_bytes primitive")),
    ("omnigraph-storage/lib.rs", ".put_opts(", 2, WriteProtocol::Composed("object storage conditional put primitive")),
    ("omnigraph-storage/lib.rs", ".put_multipart(", 1, WriteProtocol::Composed("Azure multipart rename staging")),
    ("omnigraph-storage/lib.rs", ".put_part(", 1, WriteProtocol::Composed("Azure multipart rename staging")),
    ("omnigraph-storage/lib.rs", ".complete(", 1, WriteProtocol::Composed("Azure multipart rename destination publication")),
    ("omnigraph-storage/lib.rs", ".abort(", 1, WriteProtocol::Composed("Azure multipart rename failure cleanup")),
    ("engine/operators/memory.rs", ".abort(", 1, WriteProtocol::ReadTaskCancellation),
    ("omnigraph-storage/lib.rs", ".rename(", 1, WriteProtocol::Composed("object storage rename primitive")),
    ("storage_layer.rs", ".force_delete_branch(", 1, WriteProtocol::NativeRefControl),
    ("storage_layer.rs", ".commit_staged_create_exact(", 1, WriteProtocol::Exact("sealed TableStorage create forwarding")),
    ("storage_layer.rs", ".commit_staged(", 1, WriteProtocol::Composed("sealed TableStorage forwarding")),
    ("storage_layer.rs", ".commit_staged_exact(", 1, WriteProtocol::Exact("sealed TableStorage forwarding")),
    ("storage_layer.rs", ".commit_staged_detached(", 1, WriteProtocol::Exact("sealed TableStorage forwarding")),
    ("db/omnigraph/promotion.rs", "SnapshotHandle::new(", 1, WriteProtocol::ReadOnlyAccess),
    ("storage_layer.rs", ".dataset()", 31, WriteProtocol::Composed("sealed TableStorage forwarding")),
    ("storage_layer.rs", ".into_arc()", 5, WriteProtocol::Composed("sealed TableStorage forwarding")),
    ("storage_layer.rs", "SnapshotHandle::new(", 3, WriteProtocol::Composed("sealed TableStorage forwarding")),
    ("table_store.rs", ".raw_dataset_append(", 1, WriteProtocol::EphemeralScratch),
    ("table_store.rs", "Dataset::write(", 2, WriteProtocol::EphemeralScratch),
    ("table_store.rs", "DeleteBuilder::from_expr(", 1, WriteProtocol::Composed("staged delete primitive")),
    ("table_store.rs", "InsertBuilder::new(", 3, WriteProtocol::Composed("staged insert primitive")),
    ("table_store.rs", "MergeInsertBuilder::try_new(", 1, WriteProtocol::Composed("staged merge primitive")),
    ("table_store.rs", "CommitBuilder::new(", 3, WriteProtocol::Composed("staged commit primitive")),
    ("table_store.rs", ".create_index_builder(", 5, WriteProtocol::Composed("staged index primitive and RFC 0067 whole-rebuild fold")),
    ("table_store.rs", ".execute_uncommitted(", 10, WriteProtocol::Composed("staged physical primitive")),
    ("db/omnigraph/system_column_upgrade.rs", ".commit_staged_detached(", 1, WriteProtocol::Exact("RFC 0067 detached rename-only system-column effect")),
    ("db/omnigraph/system_column_upgrade.rs", ".commit_changes_with_intent_and_expected(", 1, SYSTEM_COLUMNS_V9),
    ("db/omnigraph/system_column_upgrade.rs", ".dataset()", 1, SYSTEM_COLUMNS_V9),
    ("exec/merge.rs", ".commit_staged_detached(", 1, WriteProtocol::Exact("RFC 0067 detached merge chain")),
    ("exec/merge.rs", ".dataset()", 4, WriteProtocol::Exact("RFC 0067 detached merge chain")),
    ("db/omnigraph/schema_apply.rs", ".commit_staged_create_exact(", 1, SCHEMA_V9),
    ("db/omnigraph/schema_apply.rs", ".commit_staged_detached(", 2, WriteProtocol::Exact("detached metadata-only schema evolution + incompatible original-empty-table retry")),
    ("db/omnigraph/table_ops.rs", ".commit_staged(", 1, WriteProtocol::Composed("shared merge/Optimize index tail")),
    ("db/omnigraph/table_ops.rs", ".commit_staged_detached(", 1, WriteProtocol::Exact("RFC 0067 detached index batch")),
    ("exec/staging.rs", ".commit_staged_detached(", 1, WriteProtocol::Exact("Mutation/Load detached staging (RFC 0067)")),
    ("exec/staging.rs", ".dataset()", 1, WriteProtocol::Exact("Mutation/Load detached staging (RFC 0067)")),
    ("exec/mutation.rs", "commit_updates_on_branch_with_expected(", 1, MUTATION_V9),
    ("loader/mod.rs", "commit_updates_on_branch_with_expected(", 1, LOAD_V9),
    ("exec/merge.rs", "commit_updates_on_branch_with_expected(", 1, MERGE_V9),
    ("db/omnigraph.rs", "commit_updates_on_branch_with_expected(", 1, WriteProtocol::Exact("shared publisher wrapper")),
    ("db/omnigraph/table_ops.rs", "commit_updates_on_branch_with_expected(", 1, WriteProtocol::Exact("shared publisher")),
    ("db/omnigraph/table_ops.rs", ".commit_changes_with_intent_and_expected(", 2, WriteProtocol::Exact("shared publisher")),
    ("db/omnigraph/schema_apply.rs", ".commit_changes_with_intent_and_expected(", 1, SCHEMA_V9),
    ("db/omnigraph/schema_apply/settlement.rs", ".commit_changes_with_intent_and_expected(", 1, WriteProtocol::Exact("schema settlement: strict numeric neutral lineage publication")),
    ("db/omnigraph/repair.rs", ".commit_updates_with_actor_with_expected(", 1, WriteProtocol::ManifestAdoption),
    ("db/graph_coordinator.rs", ".commit_changes_with_intent_and_expected(", 1, WriteProtocol::Exact("publisher gateway")),
    ("db/graph_coordinator.rs", ".commit_changes_with_lineage_and_precondition(", 1, WriteProtocol::Exact("lowest manifest publisher gateway")),
    ("omnigraph-catalog/lib.rs", ".publish_with_precondition(", 1, WriteProtocol::Exact("lowest manifest publisher gateway")),
    ("db/omnigraph/table_ops.rs", ".commit_updates_with_actor_with_expected(", 2, WriteProtocol::TestOnly),
    ("db/omnigraph.rs", ".write_text_if_absent(", 2, WriteProtocol::Composed("bootstrap init claim + owned local-root create-if-absent probe")),
    ("db/omnigraph.rs", ".delete(", 2, WriteProtocol::Composed("bootstrap init claim release + owned local-root probe removal")),
    ("db/omnigraph.rs", "GraphCoordinator::init_commit_with_session(", 1, WriteProtocol::Bootstrap),
    ("db/omnigraph/optimize.rs", "compact_files(", 1, WriteProtocol::PhysicalOnly),
    ("db/omnigraph/optimize.rs", ".commit_staged_detached(", 3, WriteProtocol::Exact("RFC 0067 detached compaction rewrite, index fold and deferred index build")),
    ("db/omnigraph/optimize.rs", "commit_updates_on_branch_with_expected(", 1, OPTIMIZE_V9),
    ("db/omnigraph/optimize.rs", ".update_config(", 1, WriteProtocol::PhysicalOnly),
    ("db/omnigraph.rs", ".branch_create(", 2, WriteProtocol::NativeRefControl),
    ("db/omnigraph.rs", ".branch_delete_captured(", 1, WriteProtocol::NativeRefControl),
    ("db/graph_coordinator.rs", ".create_branch(", 1, WriteProtocol::NativeRefControl),
    ("db/graph_coordinator.rs", ".delete_branch_with_expected(", 1, WriteProtocol::NativeRefControl),
    ("omnigraph-core/branch_control.rs", ".create_branch(", 1, WriteProtocol::Composed("graph/data native refs")),
    ("omnigraph-core/lance_clone.rs", ".create_branch(", 1, WriteProtocol::Composed("scoped native clone index-origin forwarding")),
    ("omnigraph-core/lance_clone.rs", ".commit(", 1, WriteProtocol::Composed("Lance commit-handler publication forwarding")),
    ("omnigraph-core/lance_clone.rs", ".delete(", 1, WriteProtocol::Composed("Lance commit-handler deletion forwarding")),
    ("omnigraph-core/branch_control.rs", ".replace_metadata(", 1, WriteProtocol::NativeRefControl),
    ("omnigraph-core/branch_control.rs", ".put_opts(", 1, WriteProtocol::Composed("create-only exact retirement archive before native ref unlink")),
    ("omnigraph-core/branch_control.rs", ".delete(", 1, WriteProtocol::Composed("unlink only the freshly validated retired ref after its exact archive is durable")),
    ("omnigraph-core/branch_control.rs", ".tags(", 1, WriteProtocol::ReadOnlyAccess),
    ("omnigraph-catalog/retention.rs", ".tags(", 3, WriteProtocol::Composed("nonce-owned merge input tag creation and immutable collector tag inventories")),
    ("omnigraph-catalog/retention.rs", ".delete(", 1, WriteProtocol::Composed("release acknowledged nonce-owned merge tags or tags with proven dead target authority")),
    ("db/omnigraph/optimize.rs", ".tags(", 1, WriteProtocol::ReadOnlyAccess),
    ("db/omnigraph/collector.rs", ".tags(", 2, WriteProtocol::ReadOnlyAccess),
    ("omnigraph-core/branch_control.rs", ".force_delete_branch(", 1, WriteProtocol::Composed("graph/data native refs")),
    ("db/omnigraph/optimize.rs", ".force_delete_branch(", 1, WriteProtocol::PhysicalOnly),
    ("exec/merge.rs", "TableStore::create_empty_dataset(", 1, WriteProtocol::EphemeralScratch),
    ("exec/merge.rs", "TableStore::append_or_create_batch(", 1, WriteProtocol::EphemeralScratch),
    ("db/omnigraph/table_ops.rs", ".dataset()", 1, WriteProtocol::ReadOnlyAccess),
    // Two pinned entity row scans and one schema read for system-role resolution.
    ("db/omnigraph/export.rs", ".dataset()", 3, WriteProtocol::ReadOnlyAccess),
    // Blob live-branch recheck: lists the table's refs to prove a vanished
    // fork before the incarnation refusal; read-only access to the handle.
    ("blob.rs", ".dataset()", 1, WriteProtocol::ReadOnlyAccess),
    // Commit-change enumeration: pinned parent/child handles for the ordered
    // merge's typed row comparison. Read-only by construction — the enumerator
    // stages no transaction and publishes nothing.
    ("changes/enumerate.rs", ".dataset()", 2, WriteProtocol::ReadOnlyAccess),
    // Candidate-pruning emitter: pinned parent/child handles for the O(delta)
    // candidate scan + parent before-image probe and the full-merge fallback.
    // Read-only — it stages and publishes nothing.
    ("changes/candidate_scan.rs", ".dataset()", 5, WriteProtocol::ReadOnlyAccess),
    // Net-diff cross-branch path: the same typed row comparison over two
    // pinned snapshot handles, plus the same-lineage path reading each
    // side's schema to resolve its system column spellings (RFC 0040
    // Historical reads). Read-only — the diff stages and publishes nothing.
    ("changes/mod.rs", ".dataset()", 11, WriteProtocol::ReadOnlyAccess),
    ("db/omnigraph/collector.rs", ".dataset()", 17, WriteProtocol::ReadOnlyAccess),
    ("db/omnigraph/collector.rs", ".delete(", 1, WriteProtocol::Composed("detached-only sweep through the table's own Lance store: freed files, then the published manifests no retained `__manifest` version pins (links before tips) and the dead stagings; an interrupted pass is re-swept by the next")),
    ("db/omnigraph/schema_apply.rs", ".dataset()", 1, SCHEMA_V9),
    ("db/omnigraph/repair.rs", ".dataset()", 1, WriteProtocol::ManifestAdoption),
    // The sixth accessor reports deferred FTS coverage from an immutable
    // snapshot; it only reads index metadata and never stages or publishes.
    ("db/omnigraph/optimize.rs", ".dataset()", 8, WriteProtocol::Composed("Optimize planning + read-only coverage and native-fork inventory + physical cleanup")),
    ("db/omnigraph/optimize.rs", ".into_dataset()", 1, WriteProtocol::PhysicalOnly),
    ("exec/merge.rs", "SnapshotHandle::new(", 4, MERGE_V9),
}

const DURABLE_PRIMITIVES: &[&str] = &[
    ".tags(",
    "write_sidecar(",
    "confirm_occ_sidecar_v9(",
    "confirm_branch_merge_sidecar_v9(",
    "confirm_schema_apply_sidecar_v9(",
    "confirm_ensure_indices_sidecar_v9(",
    "delete_sidecar(",
    "delete_sidecar_after_publish(",
    ".commit(",
    ".commit_staged_create_exact(",
    ".commit_staged_exact(",
    ".commit_staged_detached(",
    ".promote_detached(",
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
    ".write_text_if_match(",
    ".write_text(",
    ".write_bytes(",
    ".delete(",
    ".delete_prefix(",
    ".put(",
    ".put_opts(",
    ".put_if_absent(",
    ".put_multipart(",
    ".put_part(",
    ".complete(",
    ".abort(",
    ".rename(",
    ".rename_text(",
    "GraphCoordinator::init_commit_with_session(",
    "recover_manifest_drift(",
    "heal_pending_sidecars_roll_forward(",
    "recover_schema_state_files(",
    "compact_files(",
    ".optimize_indices(",
    "cleanup_old_versions(",
    ".update_config(",
    ".update_schema_metadata(",
    "write_schema_contract_staging(",
    "promote_exact_schema_staging(",
    "discard_exact_schema_staging(",
    ".branch_create(",
    ".branch_delete(",
    ".branch_delete_captured(",
    ".create_branch(",
    ".delete_branch(",
    ".delete_branch_with_expected(",
    ".replace_metadata(",
    ".force_delete_branch(",
    "TableStore::create_empty_dataset(",
    "TableStore::append_or_create_batch(",
    "TableStore::write_dataset(",
    "Dataset::write(",
    "DeleteBuilder::new(",
    "DeleteBuilder::from_expr(",
    "InsertBuilder::new(",
    "MergeInsertBuilder::try_new(",
    "CommitBuilder::new(",
    ".create_index_builder(",
    ".execute_uncommitted(",
    ".execute_uncommitted_stream(",
    ".execute_reader(",
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
    "delete_healed_sidecar(",
    ".dataset()",
    ".into_arc()",
    ".into_dataset()",
    "SnapshotHandle::new(",
];

fn engine_src_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir).join("src")
}

fn sibling_crate_src(engine_src: &Path, crate_name: &str) -> PathBuf {
    engine_src
        .parent()
        .and_then(Path::parent)
        .expect("engine crate lives below the workspace crates directory")
        .join(crate_name)
        .join("src")
}

/// The crates split out of the engine, walked by every source walk beside it.
const GUARDED_CRATES: &[&str] = &["omnigraph-core", "omnigraph-catalog"];

/// The shared storage crate: walked only by the durable-call inventory and the
/// callable-gateway registry, so its labels resolve but no other walk reads it.
const STORAGE_CRATE: &str = "omnigraph-storage";

/// The file a guard label names: a sibling-crate label resolves into that crate's `src`.
fn guarded_path(engine_src: &Path, label: &str) -> PathBuf {
    for crate_name in GUARDED_CRATES.iter().chain([&STORAGE_CRATE]) {
        if let Some(rest) = label
            .strip_prefix(crate_name)
            .and_then(|rest| rest.strip_prefix('/'))
        {
            return sibling_crate_src(engine_src, crate_name).join(rest);
        }
    }
    engine_src.join(label)
}

/// Every `.rs` file of `crate_name`'s `src`, labeled `<crate_name>/<relative>`.
fn sibling_crate_files(engine_src: &Path, crate_name: &str) -> Vec<(String, PathBuf)> {
    let crate_src = sibling_crate_src(engine_src, crate_name);
    walk_rust_files(&crate_src)
        .into_iter()
        .map(|path| {
            (
                format!("{crate_name}/{}", relative_to_src(&crate_src, &path)),
                path,
            )
        })
        .collect()
}

/// Every `.rs` file of the `GUARDED_CRATES`, labeled by crate, sorted by label.
fn guarded_sibling_files(engine_src: &Path) -> Vec<(String, PathBuf)> {
    let mut files = GUARDED_CRATES
        .iter()
        .flat_map(|crate_name| sibling_crate_files(engine_src, crate_name))
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

/// The engine's files labeled relative to `src`, then `guarded_sibling_files`;
/// `protocol_scan` drops the `PROTOCOL_SCAN_EXCLUDED_FILES`.
fn labeled_scan_files(src: &Path, protocol_scan: bool) -> Vec<(String, PathBuf)> {
    walk_rust_files(src)
        .into_iter()
        .map(|path| (relative_to_src(src, &path), path))
        .chain(guarded_sibling_files(src))
        .filter(|(relative, _)| {
            !(protocol_scan && PROTOCOL_SCAN_EXCLUDED_FILES.contains(&relative.as_str()))
        })
        .collect()
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

fn relative_to_src(src: &Path, file: &Path) -> String {
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
/// `any(test, feature = "test-util")` is the crate-boundary form of `cfg(test)`.
fn meta_requires_test(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) if list.path.is_ident("all") => {
            nested_meta(list).iter().any(meta_requires_test)
        }
        Meta::List(list) if list.path.is_ident("any") => {
            let alternatives = nested_meta(list);
            let is_test = |meta: &Meta| matches!(meta, Meta::Path(path) if path.is_ident("test"));
            (!alternatives.is_empty() && alternatives.iter().all(meta_requires_test))
                || (alternatives.iter().any(is_test)
                    && alternatives
                        .iter()
                        .all(|meta| is_test(meta) || is_test_util_feature(meta)))
        }
        _ => false,
    }
}

fn is_test_util_feature(meta: &Meta) -> bool {
    matches!(
        meta,
        Meta::NameValue(name_value)
            if name_value.path.is_ident("feature")
                && matches!(
                    &name_value.value,
                    Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(value), .. })
                        if value.value() == "test-util"
                )
    )
}

fn cfg_requires_test(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        let Meta::List(cfg) = &attribute.meta else {
            return false;
        };
        cfg.path.is_ident("cfg") && nested_meta(cfg).iter().any(meta_requires_test)
    })
}

fn has_doc_hidden(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        if !attribute.path().is_ident("doc") || !matches!(&attribute.meta, Meta::List(_)) {
            return false;
        }
        let mut hidden = false;
        attribute
            .parse_nested_meta(|meta| {
                hidden |= meta.path.is_ident("hidden");
                Ok(())
            })
            .unwrap_or_else(|error| panic!("failed to parse doc attribute: {error}"));
        hidden
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
        "CommitBuilder",
        "DeleteBuilder",
        "GraphCoordinator",
        "GraphNamespacePublisher",
        "InsertBuilder",
        "ManifestBatchPublisher",
        "ManifestCoordinator",
        "MergeInsertBuilder",
        "RecoveryAudit",
        "SnapshotHandle",
        "StorageAdapter",
        "TableStorage",
        "TableStore",
    ]
    .contains(&identifier)
        || DURABLE_PRIMITIVES.iter().any(|primitive| {
            !primitive.trim_start_matches('.').contains("::")
                && primitive_terminal_identifier(primitive) == identifier
        })
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
        // The durable primitive is the zero-argument raw-handle accessor
        // `.dataset()`. `Snapshot::dataset(type_key)` is a logical metadata
        // lookup introduced by the graph-vocabulary API and cannot expose a
        // Lance handle. Keep the write guard exact by distinguishing arity.
        if method != "dataset" || node.args.is_empty() {
            self.record(method.clone());
        }
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
            // UFCS adds the receiver as the first argument, so
            // `Owner::dataset(owner)` is the zero-argument method form while
            // `Snapshot::dataset(snapshot, type_key)` is the logical lookup.
            if identifier != "dataset" || node.args.len() <= 1 {
                self.record(identifier.clone());
            }
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
        for primitive in DURABLE_PRIMITIVES {
            let qualified = primitive.trim_start_matches('.').trim_end_matches('(');
            if !qualified.contains("::") {
                continue;
            }
            let suffix = qualified.split("::").collect::<Vec<_>>();
            if path_ends_with(&node.func, &suffix) {
                self.record(*primitive);
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
        if [
            "Omnigraph",
            "Session",
            "GraphCoordinator",
            "ManifestCoordinator",
        ]
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
            // Common object-store verbs occur in logging strings inside macro
            // tokens. Their real method calls are exact-counted by the AST
            // visitor, but treating arbitrary literal text as code would make
            // ordinary diagnostics fail the guard.
            if ["put", "put_opts", "rename"].contains(&key.as_str()) {
                continue;
            }
            if !key.contains("::") && !key.contains('(') && tokens.contains(&format!("{key} (")) {
                self.macro_hits
                    .push(format!("{macro_name}! contains `{key}`"));
            }
        }
        for primitive in DURABLE_PRIMITIVES {
            let qualified = primitive.trim_start_matches('.').trim_end_matches('(');
            if !qualified.contains("::") {
                continue;
            }
            let marker = format!("{} (", qualified.replace("::", " :: "));
            if tokens.contains(&marker) {
                self.macro_hits
                    .push(format!("{macro_name}! contains `{primitive}`"));
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
    let identifier = primitive
        .trim_start_matches('.')
        .split('(')
        .next()
        .expect("durable primitive identifier");
    if identifier.contains("::")
        || matches!(
            primitive,
            ".raw_dataset_append(" | ".append(RecoveryAuditRecord"
        )
    {
        primitive.to_string()
    } else {
        identifier.to_string()
    }
}

fn call_inventory(ast: &syn::File) -> CallInventory {
    let mut inventory = CallInventory::default();
    inventory.visit_file(ast);
    inventory
}

fn durable_protocol_scan_files(engine_src: &Path) -> Vec<(String, PathBuf)> {
    let mut files = labeled_scan_files(engine_src, true);
    files.extend(sibling_crate_files(engine_src, STORAGE_CRATE));
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

/// The two owners of the public graph API: the handle, and the session that
/// carries the operations consulting a setting (the Session settings RFC).
fn is_omnigraph_type(ty: &Type) -> bool {
    is_named_type(ty, "Omnigraph") || is_named_type(ty, "Session")
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
    for (relative, file) in labeled_scan_files(src, false) {
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

#[test]
fn export_cut_is_hidden_move_only_and_non_forgeable() {
    let path = engine_src_root().join("db/omnigraph/export.rs");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let ast = parse_rust_source(&contents, "db/omnigraph/export.rs");
    let mut cut_structs = 0;
    let mut capture_methods = 0;
    let mut cut_methods = BTreeSet::new();

    for item in &ast.items {
        match item {
            Item::Struct(item) if item.ident == "ExportCut" => {
                cut_structs += 1;
                assert!(matches!(item.vis, Visibility::Public(_)));
                assert!(has_doc_hidden(&item.attrs));
                assert!(
                    item.fields
                        .iter()
                        .all(|field| matches!(field.vis, Visibility::Inherited)),
                    "every ExportCut field must remain private"
                );
                let mut derives_clone = false;
                for attribute in &item.attrs {
                    if attribute.path().is_ident("derive") {
                        attribute
                            .parse_nested_meta(|meta| {
                                derives_clone |= meta.path.is_ident("Clone");
                                Ok(())
                            })
                            .unwrap_or_else(|error| panic!("failed to parse cut derive: {error}"));
                    }
                }
                assert!(!derives_clone, "ExportCut must remain move-only");
            }
            Item::Impl(item)
                if item.trait_.is_none()
                    && type_final_ident(&item.self_ty)
                        .is_some_and(|ident| ident == "ExportCut") =>
            {
                for member in &item.items {
                    let syn::ImplItem::Fn(function) = member else {
                        continue;
                    };
                    if !matches!(function.vis, Visibility::Public(_)) {
                        continue;
                    }
                    assert!(function.sig.asyncness.is_some());
                    assert!(matches!(
                        function.sig.inputs.first(),
                        Some(syn::FnArg::Receiver(receiver)) if receiver.reference.is_none()
                    ));
                    cut_methods.insert(function.sig.ident.to_string());
                }
            }
            Item::Impl(item) if item.trait_.is_none() && is_omnigraph_type(&item.self_ty) => {
                for member in &item.items {
                    let syn::ImplItem::Fn(function) = member else {
                        continue;
                    };
                    // The two registered cut-capture surfaces: served export
                    // and the served baseline handshake. Both return the one
                    // move-only ExportCut type.
                    if function.sig.ident != "capture_served_export_cut"
                        && function.sig.ident != "capture_served_change_baseline_cut"
                    {
                        continue;
                    }
                    capture_methods += 1;
                    assert!(matches!(function.vis, Visibility::Public(_)));
                    assert!(function.sig.asyncness.is_some());
                    assert!(has_doc_hidden(&function.attrs));
                    assert!(return_type_contains_identifier(
                        &function.sig.output,
                        "ExportCut"
                    ));
                }
            }
            _ => {}
        }
    }

    assert_eq!(cut_structs, 1, "exactly one export-cut type is allowed");
    assert_eq!(
        capture_methods, 2,
        "exactly the two registered export-cut captures are allowed"
    );
    assert_eq!(
        cut_methods,
        BTreeSet::from([
            "into_jsonl".to_string(),
            "write_chunks".to_string(),
            "write_to".to_string(),
        ])
    );
    assert!(!contents.contains("impl Clone for ExportCut"));
}

fn low_level_async_surfaces(src: &Path, owner: &str) -> BTreeSet<(String, String, String)> {
    let mut surfaces = BTreeSet::new();
    for (relative, file) in labeled_scan_files(src, false) {
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

fn is_gateway_owner(relative: &str, owner: &str) -> bool {
    GATEWAY_SURFACES
        .iter()
        .any(|surface| surface.file == relative && surface.owner == owner)
}

fn callable_gateway_surfaces(src: &Path) -> BTreeSet<(String, String, String)> {
    let mut surfaces = BTreeSet::new();
    let mut files = labeled_scan_files(src, false);
    files.extend(sibling_crate_files(src, STORAGE_CRATE));
    for (relative, file) in files {
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        for item in &ast.items {
            match item {
                Item::Fn(function)
                    if FREE_FUNCTION_GATEWAY_FILES.contains(&relative.as_str())
                        && !cfg_requires_test(&function.attrs)
                        && !matches!(function.vis, Visibility::Inherited) =>
                {
                    surfaces.insert((
                        relative.clone(),
                        FREE_FUNCTION_OWNER.to_string(),
                        function.sig.ident.to_string(),
                    ));
                }
                Item::Trait(definition)
                    if !cfg_requires_test(&definition.attrs)
                        && is_gateway_owner(&relative, &definition.ident.to_string()) =>
                {
                    for item in &definition.items {
                        let syn::TraitItem::Fn(function) = item else {
                            continue;
                        };
                        if cfg_requires_test(&function.attrs) {
                            continue;
                        }
                        surfaces.insert((
                            relative.clone(),
                            definition.ident.to_string(),
                            function.sig.ident.to_string(),
                        ));
                    }
                }
                Item::Impl(implementation)
                    if !cfg_requires_test(&implementation.attrs)
                        && implementation.trait_.is_none() =>
                {
                    let Some(owner) = type_final_ident(&implementation.self_ty) else {
                        continue;
                    };
                    if !is_gateway_owner(&relative, &owner.to_string()) {
                        continue;
                    }
                    for item in &implementation.items {
                        let syn::ImplItem::Fn(function) = item else {
                            continue;
                        };
                        if cfg_requires_test(&function.attrs)
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
                _ => {}
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
fn callable_storage_and_manifest_gateway_surfaces_are_registered() {
    let src = engine_src_root();
    let mut registered = BTreeMap::new();
    for surface in GATEWAY_SURFACES {
        let key = (
            surface.file.to_string(),
            surface.owner.to_string(),
            surface.function.to_string(),
        );
        assert!(
            registered
                .insert(key.clone(), surface.disposition)
                .is_none(),
            "duplicate callable gateway registration: {}::{} ({})",
            key.1,
            key.2,
            surface.disposition.label()
        );
    }

    let discovered = callable_gateway_surfaces(&src);
    let classified = registered.keys().cloned().collect::<BTreeSet<_>>();
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
        "callable storage/manifest gateway registry drifted. Missing definitions: {missing:?}. \
         Unclassified callable methods and free functions: {unregistered:?}"
    );
}

/// RFC-023: the raw `stage_append` is test-only behind the sealed storage
/// adapter, and every production graph writer selects the exact-id fenced
/// adapter. The walk is syntactic, so comments and test fixtures cannot weaken it.
#[test]
fn graph_visible_keyed_writes_cannot_reach_unfenced_append() {
    let src = engine_src_root();
    let mut violations = Vec::new();
    for (relative, file) in durable_protocol_scan_files(&src) {
        // This is the one sealed forwarding boundary. TableStore's inherent
        // implementation and its cfg(test) primitive coverage contain no
        // graph-facing call site.
        if relative == "storage_layer.rs" {
            continue;
        }
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        let inventory = call_inventory(&ast);
        let count = inventory.counts.get("stage_append").copied().unwrap_or(0);
        if count > 0 {
            violations.push(format!("{relative}: stage_append called {count} time(s)"));
        }
    }

    assert!(
        violations.is_empty(),
        "graph-visible writes must use the exact-id fenced adapter; bare append call sites found:\n  {}",
        violations.join("\n  ")
    );
}

/// Removing the proven adapter's own target lookup makes its opaque chunk a
/// correctness capability. Pin the sole production mint to the branch-merge
/// history classifier/physical replay module; primitive tests may construct
/// chunks directly, but no other production caller may admit one.
#[test]
fn proven_insert_capability_has_one_production_mint_site() {
    let src = engine_src_root();
    let mut sites = Vec::new();
    for (relative, file) in labeled_scan_files(&src, true) {
        if relative.contains("/staged_tests.rs") || relative.ends_with("staged_tests.rs") {
            continue;
        }
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        let count = call_inventory(&ast)
            .counts
            .get("from_verified_history")
            .copied()
            .unwrap_or(0);
        if count > 0 {
            sites.push((relative, count));
        }
    }
    assert_eq!(
        sites,
        vec![("exec/merge.rs".to_string(), 1)],
        "ProvenInsertChunk admission must remain exclusive to verified branch-merge history"
    );

    let merge = std::fs::read_to_string(src.join("exec/merge.rs"))
        .expect("read branch-merge implementation for capability-route guard");
    assert_eq!(
        merge.matches("KeyedChunkStage::ProvenStrictInsert").count(),
        2,
        "the proven staging mode must appear only in its shared-loop match arm and the verified pure-insert publisher; another admission caller would bypass the constructor-site count"
    );
}

/// The CDC candidate-pruning classifier (`changes::candidate_scan`) treats every
/// persisted `Operation::Update` as row-set-preserving, deriving the change feed
/// from a candidate scan without a delete pass. That is sound only because
/// OmniGraph's `merge_insert` never deletes an unmatched-by-source row — Lance
/// defaults the by-source arm to Keep and no engine code sets it otherwise. If a
/// delete-capable by-source merge arm were ever introduced, a persisted
/// `Operation::Update` could remove rows and the feed would silently drop that
/// delete. Lock the floor: the by-source merge arm must be absent from engine
/// source (production or test), so introducing one forces this classifier to be
/// re-gated first.
#[test]
fn no_delete_capable_merge_arm_in_engine_source() {
    let src = engine_src_root();
    let mut offenders: Vec<String> = Vec::new();
    for (relative, file) in labeled_scan_files(&src, false) {
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if contents.contains("WhenNotMatchedBySource")
            || contents.contains("when_not_matched_by_source")
        {
            offenders.push(relative);
        }
    }
    assert!(
        offenders.is_empty(),
        "a by-source merge arm appeared in engine source, which can make an \
         Operation::Update delete rows. The CDC candidate-pruning classifier \
         (changes::candidate_scan) assumes every Update is non-deleting; re-gate \
         it before introducing this. Found in: {offenders:?}"
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
    for (relative, file) in durable_protocol_scan_files(&src) {
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
fn structural_call_scanner_distinguishes_raw_dataset_accessor_from_logical_lookup() {
    let ast = parse_rust_source(
        r#"
        fn exercise(raw: &RawOwner, snapshot: &Snapshot) {
            raw.dataset();
            raw.tags();
            RawOwner::dataset(raw);
            snapshot.dataset("node:Person");
            Snapshot::dataset(snapshot, "node:Person");
        }
        "#,
        "scanner dataset arity self-test",
    );
    let inventory = call_inventory(&ast);
    assert_eq!(inventory.counts.get("dataset"), Some(&2));
    assert_eq!(inventory.counts.get("tags"), Some(&1));
}

#[test]
fn structural_call_scanner_skips_test_module_but_keeps_later_production() {
    let ast = parse_rust_source(
        r#"
        #[cfg(test)]
        mod tests {
            fn helper(storage: &Storage, object_store: &ObjectStore, upload: &mut Upload) {
                storage.commit_staged_exact();
                object_store.put_multipart(location);
                upload.put_part(payload);
                upload.complete();
                upload.abort();
            }
        }

        fn production_after_tests(
            storage: &Storage,
            object_store: &ObjectStore,
            upload: &mut Upload,
        ) {
            storage.commit_staged_exact();
            object_store.put_multipart(location);
            upload.put_part(payload);
            upload.complete();
            upload.abort();
        }
        "#,
        "scanner cfg(test) self-test",
    );
    let inventory = call_inventory(&ast);
    assert_eq!(inventory.counts.get("commit_staged_exact"), Some(&1));
    assert_eq!(inventory.counts.get("put_multipart"), Some(&1));
    assert_eq!(inventory.counts.get("put_part"), Some(&1));
    assert_eq!(inventory.counts.get("complete"), Some(&1));
    assert_eq!(inventory.counts.get("abort"), Some(&1));
}

#[test]
fn structural_call_scanner_closes_raw_dataset_and_ufcs_routes() {
    let ast = parse_rust_source(
        r#"
        fn exercise(
            ds: &Dataset,
            scanner: &mut Scanner,
            storage: &Storage,
            audit: &mut RecoveryAudit,
            object_store: &ObjectStore,
            upload: &mut Upload,
        ) {
            ds.append(reader, params);
            ds.list_branches();
            scanner.order_by(ordering);
            Dataset::append(ds, reader, params);
            <Dataset>::merge_insert(ds, params);
            ds.add_columns(transforms, None);
            Dataset::write(reader, uri, params);
            CommitBuilder::new(ds);
            MergeInsertBuilder::try_new(ds, keys);
            TableStore::write_dataset(uri, batch);
            storage.delete(uri);
            StorageAdapter::write_text(storage, uri, contents);
            object_store.put(location, payload);
            object_store.put_opts(location, payload, options);
            object_store.put_multipart(location);
            upload.put_part(payload);
            upload.complete();
            upload.abort();
            object_store.rename(from, to);
            audit.append(RecoveryAuditRecord {});
            let _ = "put_multipart() put_part() complete() abort()";
            // upload.complete();
        }
        "#,
        "scanner raw Dataset/UFCS self-test",
    );
    let inventory = call_inventory(&ast);
    assert_eq!(inventory.counts.get(".raw_dataset_append("), Some(&2));
    assert_eq!(inventory.counts.get("list_branches"), Some(&1));
    assert_eq!(inventory.counts.get("order_by"), Some(&1));
    assert_eq!(inventory.counts.get("merge_insert"), Some(&1));
    assert_eq!(inventory.counts.get("add_columns"), Some(&1));
    assert_eq!(inventory.counts.get("Dataset::write("), Some(&1));
    assert_eq!(inventory.counts.get("CommitBuilder::new("), Some(&1));
    assert_eq!(
        inventory.counts.get("MergeInsertBuilder::try_new("),
        Some(&1)
    );
    assert_eq!(inventory.counts.get("TableStore::write_dataset("), Some(&1));
    assert_eq!(inventory.counts.get("delete"), Some(&1));
    assert_eq!(inventory.counts.get("write_text"), Some(&1));
    assert_eq!(inventory.counts.get("put"), Some(&1));
    assert_eq!(inventory.counts.get("put_opts"), Some(&1));
    assert_eq!(inventory.counts.get("put_multipart"), Some(&1));
    assert_eq!(inventory.counts.get("put_part"), Some(&1));
    assert_eq!(inventory.counts.get("complete"), Some(&1));
    assert_eq!(inventory.counts.get("abort"), Some(&1));
    assert_eq!(inventory.counts.get("rename"), Some(&1));
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
    assert!(!is_allow_listed(
        src,
        Path::new("/engine/src/nested/table_store.rs")
    ));
    assert!(!is_allow_listed(
        src,
        Path::new("/engine/src/table_store_extra.rs")
    ));
    assert!(ALLOW_LIST_FILES.contains(&"omnigraph-catalog/publisher.rs"));
}

#[test]
fn protocol_scan_exclusions_match_only_exact_test_files() {
    let src = Path::new("/engine/src");
    assert!(is_protocol_scan_excluded(
        src,
        Path::new("/engine/src/table_store/staged_tests.rs")
    ));
    assert!(is_protocol_scan_excluded(
        src,
        Path::new("/engine/src/db/catalog_tests.rs")
    ));
    assert!(!is_protocol_scan_excluded(
        src,
        Path::new("/engine/src/db/catalog_tests_helper.rs")
    ));
    assert!(PROTOCOL_SCAN_EXCLUDED_FILES.contains(&"omnigraph-catalog/tests.rs"));
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

    let facade = "db/snapshot.rs";
    let facade_contents = std::fs::read_to_string(guarded_path(&src, facade)).unwrap();
    let facade_ast = parse_rust_source(&facade_contents, facade);
    for owner in ["Snapshot", "SnapshotDataset", "SnapshotScanner"] {
        let structure = facade_ast.items.iter().find_map(|item| match item {
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

    for item in &facade_ast.items {
        let Item::Impl(implementation) = item else {
            continue;
        };
        if implementation.trait_.is_some()
            || !(is_named_type(&implementation.self_ty, "Snapshot")
                || is_named_type(&implementation.self_ty, "SnapshotDataset")
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
            for forbidden in [
                "Dataset",
                "Scanner",
                "ExecutionPlan",
                "SnapshotReadCaches",
                "CatalogSnapshot",
                "omnigraph_catalog",
                "omnigraph_core",
            ] {
                assert!(
                    !return_type_contains_identifier(&function.sig.output, forbidden),
                    "{owner}::{} must not return raw `{forbidden}` (a split-crate type or \
                     `SnapshotReadCaches` hands out the catalog `Dataset` the facade fences)",
                    function.sig.ident,
                    owner =
                        type_final_ident(&implementation.self_ty).expect("checked Snapshot owner")
                );
            }
        }
    }

    let mut dataset_entry_impls = 0usize;
    for (label, file) in sibling_crate_files(&src, "omnigraph-catalog") {
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &label);
        for item in &ast.items {
            let Item::Impl(implementation) = item else {
                continue;
            };
            if implementation.trait_.is_some()
                || !is_named_type(&implementation.self_ty, "DatasetEntry")
            {
                continue;
            }
            dataset_entry_impls += 1;
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
                        "{label}: DatasetEntry::{} must not return raw `{forbidden}`",
                        function.sig.ident
                    );
                }
            }
        }
    }
    assert!(
        dataset_entry_impls > 0,
        "no inherent `impl DatasetEntry` found in omnigraph-catalog/src; the return-type \
         check would pass vacuously"
    );

    let blob_contents = std::fs::read_to_string(src.join("blob.rs")).unwrap();
    let blob = parse_rust_source(&blob_contents, "blob.rs");
    let reader = blob.items.iter().find_map(|item| match item {
        Item::Struct(structure) if structure.ident == "BlobReader" => Some(structure),
        _ => None,
    });
    let reader = reader.expect("missing public BlobReader");
    assert!(matches!(reader.vis, Visibility::Public(_)));
    assert!(
        reader
            .fields
            .iter()
            .all(|field| matches!(field.vis, Visibility::Inherited)),
        "BlobReader fields must remain private so callers cannot recover BlobFile or Dataset"
    );
    for item in &blob.items {
        let Item::Impl(implementation) = item else {
            continue;
        };
        for item in &implementation.items {
            let syn::ImplItem::Fn(function) = item else {
                continue;
            };
            if !matches!(function.vis, Visibility::Public(_)) {
                continue;
            }
            for forbidden in ["BlobFile", "Dataset"] {
                assert!(
                    !return_type_contains_identifier(&function.sig.output, forbidden),
                    "public blob facade method {} must not return raw `{forbidden}`",
                    function.sig.ident
                );
            }
        }
    }
    let public_blob_reader_methods = blob
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(implementation)
                if implementation.trait_.is_none()
                    && is_named_type(&implementation.self_ty, "BlobReader") =>
            {
                Some(implementation)
            }
            _ => None,
        })
        .flat_map(|implementation| implementation.items.iter())
        .filter_map(|item| match item {
            syn::ImplItem::Fn(function) if matches!(function.vis, Visibility::Public(_)) => {
                Some(function.sig.ident.to_string())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        public_blob_reader_methods,
        ["is_empty", "len", "read_range"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        "BlobReader must remain a bounded range-only facade"
    );
}

/// `GraphCoordinator` stays crate-private, and the engine's `pub use` of a split-crate path
/// is exactly `SPLIT_CRATE_REEXPORTS`, naming no writer. Rustc refuses `pub use crate::db::manifest::X` of a
/// glob-imported `pub(crate)` item (E0364), not a path through a glob-imported module.
#[test]
fn graph_manifest_writer_methods_are_not_public_escape_hatches() {
    let src = engine_src_root();
    let coordinator = "db/graph_coordinator.rs";
    let file = guarded_path(&src, coordinator);
    let contents = std::fs::read_to_string(&file)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
    let coordinator_ast = parse_rust_source(&contents, coordinator);
    let visibility = coordinator_ast.items.iter().find_map(|item| match item {
        Item::Struct(item) if item.ident == "GraphCoordinator" => Some(&item.vis),
        _ => None,
    });
    assert!(
        matches!(
            visibility,
            Some(Visibility::Restricted(restricted)) if restricted.path.is_ident("crate")
        ),
        "{coordinator}::GraphCoordinator must remain crate-private"
    );

    let coordinator_methods = [
        "init_commit_with_session",
        "branch_create",
        "branch_delete_captured",
        "commit_updates_with_actor",
        "commit_updates_with_actor_with_expected",
        "commit_changes_with_intent_and_expected",
    ];
    for method in coordinator_methods {
        let visibilities = coordinator_ast
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
            "{coordinator}::{method} must resolve to exactly one inherent method, found {}",
            visibilities.len()
        );
        assert!(
            matches!(
                visibilities[0],
                Visibility::Restricted(restricted) if restricted.path.is_ident("crate")
            ),
            "{coordinator}::{method} is a graph-writer escape hatch; it must remain crate-private"
        );
    }

    let catalog_lib = "omnigraph-catalog/lib.rs";
    let file = guarded_path(&src, catalog_lib);
    let contents = std::fs::read_to_string(&file)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
    let catalog_ast = parse_rust_source(&contents, catalog_lib);
    for method in [
        "open",
        "init_commit",
        "commit_changes_with_lineage_and_precondition",
        "create_branch",
        "delete_branch",
        "delete_branch_with_expected",
    ] {
        let hidden = catalog_ast
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Impl(item) => Some(item),
                _ => None,
            })
            .flat_map(|implementation| implementation.items.iter())
            .filter_map(|item| match item {
                syn::ImplItem::Fn(function) if function.sig.ident == method => {
                    Some(has_doc_hidden(&function.attrs))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            hidden,
            [true],
            "{catalog_lib}::ManifestCoordinator::{method} is a production writer reachable \
             only by depending on the internal crate; it carries `#[doc(hidden)]`"
        );
    }

    let catalog_writer_methods = [
        "init_commit",
        "commit",
        "commit_with_expected",
        "commit_changes",
        "commit_changes_with_expected",
        "commit_changes_with_lineage",
        "commit_changes_with_lineage_and_precondition",
        "create_branch",
        "delete_branch",
        "delete_branch_with_expected",
    ];
    let shim = "db/manifest.rs";
    let file = guarded_path(&src, shim);
    let contents = std::fs::read_to_string(&file)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
    let shim_ast = parse_rust_source(&contents, shim);
    let crate_catalog_globs = shim_ast
        .items
        .iter()
        .filter(|item| {
            matches!(
                item,
                Item::Use(item)
                    if matches!(&item.vis, Visibility::Restricted(restricted) if restricted.path.is_ident("crate"))
                        && matches!(
                            &item.tree,
                            syn::UseTree::Path(path)
                                if path.ident == "omnigraph_catalog"
                                    && matches!(path.tree.as_ref(), syn::UseTree::Glob(_))
                        )
            )
        })
        .count();
    assert_eq!(
        crate_catalog_globs, 1,
        "{shim} must reach omnigraph_catalog through exactly one `pub(crate) use omnigraph_catalog::*;`"
    );

    let scopes = EngineScopes::parse(&src);
    let violations = split_reexport_violations(&scopes, SPLIT_CRATE_REEXPORTS);
    assert!(
        violations.is_empty(),
        "graph-writer escape hatch: the catalog's writers are `pub` in omnigraph-catalog, so \
         only the engine's re-exports fence them:\n  {}",
        violations.join("\n  ")
    );

    let labels = SPLIT_CRATE_REEXPORTS
        .iter()
        .filter_map(|entry| entry.split_once(": ").map(|(label, _)| label))
        .collect::<BTreeSet<_>>();
    for label in labels {
        let mut public_names = BTreeSet::new();
        for public_use in scopes
            .public_uses
            .iter()
            .filter(|public_use| public_use.label == *label)
        {
            public_names.extend(public_use.path.iter().cloned());
            public_names.extend(public_use.bound.iter().cloned());
        }
        for name in [
            "ManifestCoordinator",
            "GraphNamespacePublisher",
            "ManifestBatchPublisher",
            "PublishOutcome",
        ]
        .into_iter()
        .chain(catalog_writer_methods)
        .chain(coordinator_methods)
        {
            assert!(
                !public_names.contains(name),
                "{label} publicly re-exports `{name}`, a graph-writer escape hatch; \
                 it must stay crate-private at the engine boundary"
            );
        }
    }
}

/// Every plain `pub use` that reaches `omnigraph_core` or `omnigraph_catalog`, as
/// `file: path as written`: a crate or module export would publish its writers too.
const SPLIT_CRATE_REEXPORTS: &[&str] = &[
    "db/commit_graph.rs: omnigraph_catalog::commit_graph::CommitGraph",
    "db/commit_graph.rs: omnigraph_catalog::commit_graph::GraphCommit",
    "db/manifest.rs: omnigraph_catalog::DatasetEntry",
    "db/manifest.rs: omnigraph_catalog::DatasetUpdate",
    "db/manifest.rs: omnigraph_catalog::INTERNAL_MANIFEST_SCHEMA_VERSION",
    "db/manifest.rs: omnigraph_catalog::MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION",
    "db/manifest.rs: omnigraph_catalog::READ_REFRESH_POST_STATE_PRE_LINEAGE",
    "error.rs: omnigraph_core::error::CompletionEvidence",
    "error.rs: omnigraph_core::error::ManifestConflictDetails",
    "error.rs: omnigraph_core::error::ManifestError",
    "error.rs: omnigraph_core::error::ManifestErrorKind",
    "error.rs: omnigraph_core::error::MergeConflict",
    "error.rs: omnigraph_core::error::MergeConflictKind",
    "error.rs: omnigraph_core::error::OmniError",
    "error.rs: omnigraph_core::error::Result",
    "error.rs: omnigraph_core::error::StorageFailure",
    "error.rs: omnigraph_core::error::StorageFailureKind",
    "instrumentation.rs: omnigraph_core::instrumentation::CountingStorageAdapter",
    "instrumentation.rs: omnigraph_core::instrumentation::MergeTimingReading",
    "instrumentation.rs: omnigraph_core::instrumentation::MergeWriteProbes",
    "instrumentation.rs: omnigraph_core::instrumentation::ProbedStores",
    "instrumentation.rs: omnigraph_core::instrumentation::QueryBlockingPauseGuard",
    "instrumentation.rs: omnigraph_core::instrumentation::QueryExecutionMetrics",
    "instrumentation.rs: omnigraph_core::instrumentation::QueryIoProbes",
    "instrumentation.rs: omnigraph_core::instrumentation::QueryLadderReport",
    "instrumentation.rs: omnigraph_core::instrumentation::QueryMemoryProbes",
    "instrumentation.rs: omnigraph_core::instrumentation::RrfGateFallback",
    "instrumentation.rs: omnigraph_core::instrumentation::RrfGatePlan",
    "instrumentation.rs: omnigraph_core::instrumentation::RrfGateVerdict",
    "instrumentation.rs: omnigraph_core::instrumentation::StageWriteProbes",
    "instrumentation.rs: omnigraph_core::instrumentation::StorageReadCounts",
    "instrumentation.rs: omnigraph_core::instrumentation::with_merge_write_probes",
    "instrumentation.rs: omnigraph_core::instrumentation::with_query_io_probes",
    "instrumentation.rs: omnigraph_core::instrumentation::with_query_memory_limit",
    "instrumentation.rs: omnigraph_core::instrumentation::with_query_memory_probes",
    "instrumentation.rs: omnigraph_core::instrumentation::with_rrf_gate_subset_drop",
    "instrumentation.rs: omnigraph_core::instrumentation::with_stage_write_probes",
    "lib.rs: lance_access::object_store_seam",
    "lib.rs: lance_access::store_registry as dst_lance_store_registry",
    "lib.rs: omnigraph_core::dst_clock",
    "lib.rs: omnigraph_core::dst_gate",
    "lib.rs: omnigraph_core::dst_ids",
    "seams.rs: omnigraph_core::seams::Behavior",
    "seams.rs: omnigraph_core::seams::Counted",
    "seams.rs: omnigraph_core::seams::Decide",
    "seams.rs: omnigraph_core::seams::DecideSeam",
    "seams.rs: omnigraph_core::seams::Decision",
    "seams.rs: omnigraph_core::seams::Effect",
    "seams.rs: omnigraph_core::seams::FireAlways",
    "seams.rs: omnigraph_core::seams::FireOnceAt",
    "seams.rs: omnigraph_core::seams::Global",
    "seams.rs: omnigraph_core::seams::Hold",
    "seams.rs: omnigraph_core::seams::Installed",
    "seams.rs: omnigraph_core::seams::Observe",
    "seams.rs: omnigraph_core::seams::Op",
    "seams.rs: omnigraph_core::seams::PanicAt",
    "seams.rs: omnigraph_core::seams::Seam",
    "seams.rs: omnigraph_core::seams::SeamEntry",
    "seams.rs: omnigraph_core::seams::StoreEffect",
    "seams.rs: omnigraph_core::seams::ThreadLocal",
    "seams.rs: omnigraph_core::seams::decide_seam",
    "seams.rs: omnigraph_core::seams::effects_list",
    "seams.rs: omnigraph_core::seams::store_effects_list",
    "storage.rs: omnigraph_core::storage::DecorateStorage",
    "storage.rs: omnigraph_core::storage::ListDirBounds",
    "storage.rs: omnigraph_core::storage::ObjectStorageAdapter",
    "storage.rs: omnigraph_core::storage::STORAGE",
    "storage.rs: omnigraph_core::storage::StorageAdapter",
    "storage.rs: omnigraph_core::storage::StorageKind",
    "storage.rs: omnigraph_core::storage::join_uri",
    "storage.rs: omnigraph_core::storage::normalize_root_uri",
    "storage.rs: omnigraph_core::storage::redacted_storage_uri",
    "storage.rs: omnigraph_core::storage::storage_for_uri",
    "storage.rs: omnigraph_core::storage::storage_kind_for_uri",
    "table_store.rs: omnigraph_core::dataset_index::IndexCoverage",
];

/// The split-crate `pub use`s in `scopes` that differ from `pinned`, both ways.
fn split_reexport_violations(scopes: &EngineScopes, pinned: &[&str]) -> Vec<String> {
    let pinned = pinned
        .iter()
        .map(|entry| {
            let (label, spelled) = entry
                .split_once(": ")
                .unwrap_or_else(|| panic!("pin `{entry}` is not `file: path`"));
            (label.to_string(), spelled.to_string())
        })
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    let mut violations = Vec::new();
    for public_use in &scopes.public_uses {
        if !scopes.reaches_split(&public_use.module, &public_use.path, 0) {
            continue;
        }
        let mut spelled = public_use.path.join("::");
        match &public_use.bound {
            None => violations.push(format!(
                "{}: `pub use {spelled}` glob-publishes a split crate; name each re-export",
                public_use.label
            )),
            Some(bound) if public_use.path.last() != Some(bound) => {
                spelled = format!("{spelled} as {bound}");
            }
            Some(_) => {}
        }
        actual.insert((public_use.label.clone(), spelled));
    }
    for (label, spelled) in actual.difference(&pinned) {
        violations.push(format!(
            "{label}: `pub use {spelled}` is not in SPLIT_CRATE_REEXPORTS; re-export it \
             `pub(crate)`, or pin the exact item after review; a crate or module export \
             publishes every writer inside it"
        ));
    }
    for (label, spelled) in pinned.difference(&actual) {
        violations.push(format!(
            "{label}: pinned `pub use {spelled}` no longer exists; drop it from SPLIT_CRATE_REEXPORTS"
        ));
    }
    violations
}

#[test]
fn split_reexport_pin_refuses_crate_and_module_exports() {
    let dir = tempfile::tempdir().unwrap();
    let check = |source: &str, pinned: &[&str]| {
        std::fs::write(dir.path().join("lib.rs"), source).unwrap();
        split_reexport_violations(&EngineScopes::parse(dir.path()), pinned)
    };
    let item = ["lib.rs: omnigraph_catalog::Snapshot"];
    assert!(check("pub use omnigraph_catalog::Snapshot;\n", &item).is_empty());
    for escape in [
        "pub use omnigraph_catalog as catalog;\n",
        "pub use omnigraph_catalog::publisher;\n",
        "pub(crate) use omnigraph_catalog as cat;\npub use cat::publisher;\n",
        "pub use omnigraph_catalog::*;\n",
    ] {
        let source = format!("pub use omnigraph_catalog::Snapshot;\n{escape}");
        assert!(
            !check(&source, &item).is_empty(),
            "the pin accepted `{escape}`"
        );
    }
    assert!(!check("", &item).is_empty(), "a stale pin must fail");
}

const SPLIT_CRATE_ROOTS: &[&str] = &["omnigraph_core", "omnigraph_catalog"];

/// One engine module's names: what it defines, what its `use`s bind (target path
/// as written, whether the binding is plain `pub`), and what it glob-imports.
#[derive(Default)]
struct ModuleScope {
    items: BTreeSet<String>,
    bindings: BTreeMap<String, (Vec<String>, bool)>,
    globs: Vec<Vec<String>>,
}

/// A plain `pub use` (or `pub extern crate`) path, flattened, with its file,
/// its module path below `crate`, and the name it binds.
struct PublicUse {
    label: String,
    module: Vec<String>,
    path: Vec<String>,
    bound: Option<String>,
}

/// Name scopes of every engine module, parsed from `src`; a source resolver for
/// `use` paths only, so macro-generated re-exports stay outside it.
struct EngineScopes {
    modules: BTreeMap<Vec<String>, ModuleScope>,
    public_uses: Vec<PublicUse>,
}

impl EngineScopes {
    fn parse(src: &Path) -> Self {
        let mut scopes = Self {
            modules: BTreeMap::new(),
            public_uses: Vec::new(),
        };
        for path in walk_rust_files(src) {
            let label = relative_to_src(src, &path);
            let contents = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            let ast = parse_rust_source(&contents, &label);
            let stem = label.strip_suffix(".rs").unwrap_or(&label);
            let stem = stem.strip_suffix("/mod").unwrap_or(stem);
            let module = if stem == "lib" {
                Vec::new()
            } else {
                stem.split('/').map(str::to_string).collect()
            };
            scopes.record_items(&label, &module, &ast.items);
        }
        scopes
    }

    fn record_items(&mut self, label: &str, module: &[String], items: &[Item]) {
        for item in items {
            let (tree, public, leading) = match item {
                Item::Use(item) => (
                    item.tree.clone(),
                    matches!(item.vis, Visibility::Public(_)),
                    item.leading_colon.is_some(),
                ),
                Item::ExternCrate(item) => {
                    let name = syn::UseTree::Name(syn::UseName {
                        ident: item.ident.clone(),
                    });
                    let tree = match &item.rename {
                        Some((_, rename)) => syn::UseTree::Rename(syn::UseRename {
                            ident: item.ident.clone(),
                            as_token: Default::default(),
                            rename: rename.clone(),
                        }),
                        None => name,
                    };
                    (tree, matches!(item.vis, Visibility::Public(_)), true)
                }
                Item::Mod(inline) => {
                    self.scope(module).items.insert(inline.ident.to_string());
                    if let Some((_, content)) = &inline.content {
                        let mut child = module.to_vec();
                        child.push(inline.ident.to_string());
                        self.record_items(label, &child, content);
                    }
                    continue;
                }
                other => {
                    if let Some(ident) = item_ident(other) {
                        self.scope(module).items.insert(ident);
                    }
                    continue;
                }
            };
            let mut flattened = Vec::new();
            flatten_use_tree(&tree, &mut Vec::new(), &mut flattened);
            for (mut path, bound) in flattened {
                if leading {
                    path.insert(0, String::new());
                }
                match &bound {
                    None => self
                        .scope(module)
                        .globs
                        .push(path[..path.len() - 1].to_vec()),
                    Some(name) => {
                        let binding = self
                            .scope(module)
                            .bindings
                            .entry(name.clone())
                            .or_insert((path.clone(), false));
                        binding.1 |= public;
                    }
                }
                if public {
                    self.public_uses.push(PublicUse {
                        label: label.to_string(),
                        module: module.to_vec(),
                        path,
                        bound,
                    });
                }
            }
        }
    }

    fn scope(&mut self, module: &[String]) -> &mut ModuleScope {
        self.modules.entry(module.to_vec()).or_default()
    }

    /// Whether `path`, written in `module`, reaches a split-crate item through a
    /// crate-visible door: a split root, a non-`pub` binding of one, or a split glob.
    fn reaches_split(&self, module: &[String], path: &[String], depth: usize) -> bool {
        if depth > 16 {
            return true;
        }
        let (first, rest) = path.split_first().expect("a use path has a segment");
        let mut current = module.to_vec();
        let segments = match first.as_str() {
            "" => {
                return rest
                    .first()
                    .is_some_and(|root| SPLIT_CRATE_ROOTS.contains(&root.as_str()));
            }
            "crate" => {
                current.clear();
                rest.to_vec()
            }
            "self" => rest.to_vec(),
            "super" => {
                current.pop();
                let mut rest = rest;
                while rest.first().is_some_and(|segment| segment == "super") {
                    current.pop();
                    rest = &rest[1..];
                }
                rest.to_vec()
            }
            root if SPLIT_CRATE_ROOTS.contains(&root)
                && !self.modules.get(module).is_some_and(|scope| {
                    scope.items.contains(root) || scope.bindings.contains_key(root)
                }) =>
            {
                return true;
            }
            _ => path.to_vec(),
        };
        for (index, segment) in segments.iter().enumerate() {
            let Some(scope) = self.modules.get(&current) else {
                return false;
            };
            if segment == "*" {
                return scope
                    .globs
                    .iter()
                    .any(|glob| self.glob_reaches_split(&current, glob, depth))
                    || scope.bindings.iter().any(|(_, (target, public))| {
                        !public && self.reaches_split(&current, target, depth + 1)
                    });
            }
            if let Some((target, public)) = scope.bindings.get(segment) {
                if *public {
                    return false;
                }
                let mut resolved = target.clone();
                resolved.extend(segments[index + 1..].iter().cloned());
                return self.reaches_split(&current, &resolved, depth + 1);
            }
            if scope.items.contains(segment) {
                current.push(segment.clone());
                continue;
            }
            return scope
                .globs
                .iter()
                .any(|glob| self.glob_reaches_split(&current, glob, depth));
        }
        false
    }

    fn glob_reaches_split(&self, module: &[String], glob: &[String], depth: usize) -> bool {
        let mut target = glob.to_vec();
        target.push("*".to_string());
        self.reaches_split(module, &target, depth + 1)
    }
}

fn item_ident(item: &Item) -> Option<String> {
    let ident = match item {
        Item::Const(item) => &item.ident,
        Item::Enum(item) => &item.ident,
        Item::Fn(item) => &item.sig.ident,
        Item::Macro(item) => item.ident.as_ref()?,
        Item::Static(item) => &item.ident,
        Item::Struct(item) => &item.ident,
        Item::Trait(item) => &item.ident,
        Item::TraitAlias(item) => &item.ident,
        Item::Type(item) => &item.ident,
        Item::Union(item) => &item.ident,
        _ => return None,
    };
    Some(ident.to_string())
}

/// Every path a use tree imports as `(path, bound name)`; a glob ends in `*` and
/// binds nothing, and `{self}` binds its parent.
fn flatten_use_tree(
    tree: &syn::UseTree,
    prefix: &mut Vec<String>,
    out: &mut Vec<(Vec<String>, Option<String>)>,
) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, out);
            prefix.pop();
        }
        syn::UseTree::Name(name) if name.ident == "self" => {
            out.push((prefix.clone(), prefix.last().cloned()));
        }
        syn::UseTree::Name(name) => {
            let mut path = prefix.clone();
            path.push(name.ident.to_string());
            out.push((path, Some(name.ident.to_string())));
        }
        syn::UseTree::Rename(rename) => {
            let mut path = prefix.clone();
            if rename.ident != "self" {
                path.push(rename.ident.to_string());
            }
            out.push((path, Some(rename.rename.to_string())));
        }
        syn::UseTree::Glob(_) => {
            let mut path = prefix.clone();
            path.push("*".to_string());
            out.push((path, None));
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                flatten_use_tree(item, prefix, out);
            }
        }
    }
}

fn method_call_count(block: &syn::Block, method_name: &str) -> usize {
    struct Counter<'a> {
        method_name: &'a str,
        count: usize,
    }

    impl<'ast> Visit<'ast> for Counter<'_> {
        fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
            if node.method == self.method_name {
                self.count += 1;
            }
            visit::visit_expr_method_call(self, node);
        }
    }

    let mut counter = Counter {
        method_name,
        count: 0,
    };
    counter.visit_block(block);
    counter.count
}

#[test]
fn native_branch_controls_use_post_gate_captures_not_handle_refreshes() {
    let relative = "db/omnigraph.rs";
    let file = guarded_path(&engine_src_root(), relative);
    let contents = std::fs::read_to_string(&file)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
    let ast = parse_rust_source(&contents, relative);

    let mut functions = BTreeMap::new();
    for item in &ast.items {
        let Item::Impl(implementation) = item else {
            continue;
        };
        if !is_omnigraph_type(&implementation.self_ty) {
            continue;
        }
        for item in &implementation.items {
            let syn::ImplItem::Fn(function) = item else {
                continue;
            };
            functions.insert(function.sig.ident.to_string(), function);
        }
    }

    for function_name in [
        "branch_create_as",
        "branch_create_from_impl",
        "branch_delete_as",
        "delete_captured_branch_storage",
    ] {
        let function = functions
            .get(function_name)
            .unwrap_or_else(|| panic!("missing Omnigraph::{function_name}"));
        assert_eq!(
            method_call_count(&function.block, "refresh_coordinator_only"),
            0,
            "Omnigraph::{function_name} must not refresh the handle-local coordinator as native-ref authority"
        );
    }

    for (function_name, capture_method) in [
        ("branch_create_as", "capture_branch_control_source"),
        ("branch_create_from_impl", "capture_branch_control_source"),
        ("branch_delete_as", "open_coordinator_for_branch"),
    ] {
        let function = functions
            .get(function_name)
            .unwrap_or_else(|| panic!("missing Omnigraph::{function_name}"));
        assert_eq!(
            method_call_count(&function.block, capture_method),
            1,
            "Omnigraph::{function_name} must take exactly one post-gate operation-local control capture"
        );
        let capture_statement = function
            .block
            .stmts
            .iter()
            .position(|statement| {
                let block = syn::Block {
                    brace_token: function.block.brace_token,
                    stmts: vec![statement.clone()],
                };
                method_call_count(&block, capture_method) > 0
            })
            .expect("control capture must occur in a statement");
        let before_capture = syn::Block {
            brace_token: function.block.brace_token,
            stmts: function.block.stmts[..capture_statement].to_vec(),
        };
        assert_eq!(
            method_call_count(&before_capture, "acquire_many"),
            1,
            "Omnigraph::{function_name} must acquire its table gates before capture"
        );
    }

    let capture_helper = functions
        .get("capture_branch_control_source")
        .expect("missing Omnigraph::capture_branch_control_source");
    for required in ["probe_latest_incarnation", "validated_cached_coordinator"] {
        assert_eq!(
            method_call_count(&capture_helper.block, required),
            1,
            "source reuse must freshly verify bound authority and route misses through the verified cache"
        );
    }

    for function_name in ["branch_create_as", "branch_create_from_impl"] {
        let function = functions
            .get(function_name)
            .unwrap_or_else(|| panic!("missing Omnigraph::{function_name}"));
        assert_eq!(
            method_call_count(&function.block, "invalidate_read_caches"),
            1,
            "Omnigraph::{function_name} must invalidate derived caches after successful ref creation"
        );
    }
    let delete_helper = functions
        .get("delete_captured_branch_storage")
        .expect("missing Omnigraph::delete_captured_branch_storage");
    assert_eq!(
        method_call_count(&delete_helper.block, "invalidate_read_caches"),
        1,
        "captured branch deletion must invalidate derived caches after successful ref removal"
    );
}

fn function_blocks(ast: &syn::File, function_name: &str) -> Vec<syn::Block> {
    struct Finder<'a> {
        function_name: &'a str,
        blocks: Vec<syn::Block>,
    }

    impl<'ast> Visit<'ast> for Finder<'_> {
        fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
            if node.sig.ident == self.function_name {
                self.blocks.push((*node.block).clone());
            }
            visit::visit_item_fn(self, node);
        }

        fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
            if node.sig.ident == self.function_name {
                self.blocks.push(node.block.clone());
            }
            visit::visit_impl_item_fn(self, node);
        }
    }

    let mut finder = Finder {
        function_name,
        blocks: Vec::new(),
    };
    finder.visit_file(ast);
    finder.blocks
}

/// `omnigraph snapshot` and `GET /snapshot` resolve their target once and read
/// the stamp from that `Snapshot`; `internal_schema_version_of` would resolve
/// the target a second time, so the stamp could describe a newer graph version.
#[test]
fn snapshot_transports_read_the_stamp_from_the_snapshot_they_return() {
    let engine_src = engine_src_root();
    let mut violations = Vec::new();
    for (crate_name, relative, function_name) in [
        ("omnigraph-cli", "client.rs", "snapshot"),
        ("omnigraph-server", "handlers.rs", "server_snapshot"),
    ] {
        let file = sibling_crate_src(&engine_src, crate_name).join(relative);
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, relative);
        let blocks = function_blocks(&ast, function_name);
        let [block] = blocks.as_slice() else {
            panic!(
                "{crate_name}/{relative} must define exactly one `{function_name}`, found {}",
                blocks.len()
            );
        };
        for (method, expected) in [
            ("snapshot_of", 1),
            ("internal_schema_version_at", 1),
            ("internal_schema_version_of", 0),
        ] {
            let found = method_call_count(block, method);
            if found != expected {
                violations.push(format!(
                    "{crate_name}/{relative} `{function_name}`: {found} `{method}` calls, expected {expected}"
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "a snapshot transport must capture one `Snapshot` and read its stamp from it:\n{}",
        violations.join("\n")
    );
}

/// Lance's raw `Dataset::list_branches` is safe only behind the bounded retry
/// in `omnigraph-core/branch_control.rs`. OmniGraph's forwarding layers deliberately use
/// distinct method names, so the ordinary structural inventory can require
/// this to remain the sole production call.
#[test]
fn lance_branch_enumeration_stays_behind_retry_boundary() {
    let src = engine_src_root();
    let mut sites = Vec::new();
    for (relative, file) in labeled_scan_files(&src, true) {
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        let count = call_inventory(&ast)
            .counts
            .get("list_branches")
            .copied()
            .unwrap_or(0);
        if count > 0 {
            sites.push((relative, count));
        }
    }
    sites.sort();

    assert_eq!(
        sites,
        vec![("omnigraph-core/branch_control.rs".to_string(), 1)],
        "raw Lance branch enumeration must remain centralized in omnigraph-core/branch_control.rs"
    );

    let branch_control =
        std::fs::read_to_string(guarded_path(&src, "omnigraph-core/branch_control.rs")).expect(
            "read omnigraph-core/branch_control.rs for raw branch-enumeration owner signature",
        );
    let ast = parse_rust_source(&branch_control, "omnigraph-core/branch_control.rs");
    let owner = ast
        .items
        .iter()
        .find_map(|item| match item {
            Item::Fn(function) if function.sig.ident == "list_branch_contents" => Some(function),
            _ => None,
        })
        .expect("omnigraph-core/branch_control.rs must define list_branch_contents");
    assert_eq!(
        owner.sig.inputs.len(),
        1,
        "the raw branch-enumeration owner must accept only its Dataset handle"
    );
    let syn::FnArg::Typed(parameter) = &owner.sig.inputs[0] else {
        panic!("the raw branch-enumeration owner must accept dataset: &Dataset");
    };
    assert!(
        matches!(
            parameter.pat.as_ref(),
            syn::Pat::Ident(identifier) if identifier.ident == "dataset"
        ),
        "the raw branch-enumeration owner parameter must remain named `dataset`"
    );
    assert!(
        matches!(
            parameter.ty.as_ref(),
            Type::Reference(reference) if is_named_type(&reference.elem, "Dataset")
        ),
        "the raw branch-enumeration owner must accept dataset: &Dataset"
    );
    let mut owner_inventory = CallInventory::default();
    owner_inventory.visit_item_fn(owner);
    assert_eq!(
        owner_inventory.counts.get("list_branches"),
        Some(&1),
        "list_branch_contents(dataset: &Dataset) must own the one raw Lance call"
    );
}

/// PreparedScan owns raw ordering; the stream door selects its bounded executor.
/// The plan door supplies no ordering, and ScanTuning cannot add it.
#[test]
fn lance_ordering_stays_behind_bounded_scan_executor() {
    let src = engine_src_root();
    let mut sites = Vec::new();
    for (relative, file) in labeled_scan_files(&src, true) {
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        let count = call_inventory(&ast)
            .counts
            .get("order_by")
            .copied()
            .unwrap_or(0);
        if count > 0 {
            sites.push((relative, count));
        }
    }
    sites.sort();
    assert_eq!(
        sites,
        vec![("table_store.rs".to_string(), 1)],
        "raw Lance ordering must remain centralized in PreparedScan::configure"
    );

    let table_store = std::fs::read_to_string(src.join("table_store.rs"))
        .expect("read table_store.rs for ordered-scan owner signature");
    let ast = parse_rust_source(&table_store, "table_store.rs");
    let mut owner = None;
    let mut stream_door = None;
    let mut plan_door = None;
    let mut tuning_exposes_order_by = false;
    for item in &ast.items {
        let Item::Impl(implementation) = item else {
            continue;
        };
        if is_named_type(&implementation.self_ty, "PreparedScan") {
            owner = implementation.items.iter().find_map(|item| match item {
                syn::ImplItem::Fn(function) if function.sig.ident == "configure" => Some(function),
                _ => None,
            });
        }
        if is_named_type(&implementation.self_ty, "TableStore") {
            for item in &implementation.items {
                if let syn::ImplItem::Fn(function) = item {
                    match function.sig.ident.to_string().as_str() {
                        "scan_stream_with" => stream_door = Some(function),
                        "scan_plan_with" => plan_door = Some(function),
                        _ => {}
                    }
                }
            }
        }
        if is_named_type(&implementation.self_ty, "ScanTuning") {
            tuning_exposes_order_by = implementation.items.iter().any(|item| {
                matches!(item, syn::ImplItem::Fn(function) if function.sig.ident == "order_by")
            });
        }
    }
    let owner = owner.expect("PreparedScan::configure must own raw Lance ordering");
    assert_eq!(
        method_call_count(&owner.block, "order_by"),
        1,
        "PreparedScan::configure must own the one raw Lance order_by call"
    );
    let mut calls = CallInventory::default();
    calls.visit_block(&stream_door.expect("stream door exists").block);
    assert_eq!(calls.counts.get("execute_bounded_ordered_scan"), Some(&1));

    #[derive(Default)]
    struct UnorderedPlanCalls(usize);
    impl<'ast> Visit<'ast> for UnorderedPlanCalls {
        fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
            if final_path_ident(&node.func).as_deref() == Some("configure") {
                self.0 += 1;
                assert!(
                    matches!(node.args.iter().nth(3), Some(syn::Expr::Path(path))
                        if path.path.is_ident("None")),
                    "the plan door must not request Lance ordering"
                );
            }
            visit::visit_expr_call(self, node);
        }
    }
    let mut calls = UnorderedPlanCalls::default();
    calls.visit_block(&plan_door.expect("plan door exists").block);
    assert_eq!(
        calls.0, 1,
        "the plan door must configure exactly one scanner"
    );
    assert!(
        !tuning_exposes_order_by,
        "ScanTuning must not expose order_by after executor routing"
    );
}

/// Pins the per-file call counts of `read_blobs`, `read_blob_ranges` and
/// `with_io_buffer_size_bytes` in the production sources of the engine crate and the
/// `GUARDED_CRATES`.
#[test]
fn lance_batched_blob_read_call_counts_are_pinned() {
    let src = engine_src_root();
    let mut sites = Vec::new();
    for (relative, file) in labeled_scan_files(&src, true) {
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        let inventory = call_inventory(&ast);
        for method in [
            "read_blobs",
            "read_blob_ranges",
            "with_io_buffer_size_bytes",
        ] {
            let count = inventory.counts.get(method).copied().unwrap_or(0);
            if count > 0 {
                sites.push((method, relative.clone(), count));
            }
        }
    }
    sites.sort();
    assert_eq!(
        sites,
        vec![
            ("read_blobs", "table_store.rs".to_string(), 1),
            ("with_io_buffer_size_bytes", "table_store.rs".to_string(), 1),
        ],
        "the per-file call counts of read_blobs, read_blob_ranges and \
         with_io_buffer_size_bytes changed"
    );
}

#[test]
fn omni_error_has_only_reviewed_global_error_conversions() {
    use syn::{GenericArgument, PathArguments};

    fn type_segments(error_type: &Type) -> Option<Vec<String>> {
        let Type::Path(source) = error_type else {
            return None;
        };
        Some(
            source
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        )
    }

    let src = engine_src_root();
    let mut violations = Vec::new();
    let mut omni_error_enums = Vec::new();
    for (relative, file) in labeled_scan_files(&src, false) {
        let contents = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        let ast = parse_rust_source(&contents, &relative);
        for item in ast.items {
            if let Item::Enum(enumeration) = &item
                && enumeration.ident == "OmniError"
            {
                omni_error_enums.push(relative.clone());
                for variant in &enumeration.variants {
                    for field in &variant.fields {
                        if !field
                            .attrs
                            .iter()
                            .any(|attribute| attribute.path().is_ident("from"))
                        {
                            continue;
                        }
                        let segments = type_segments(&field.ty).unwrap_or_default();
                        let reviewed = segments == ["omnigraph_compiler", "error", "CompilerError"]
                            || segments == ["std", "io", "Error"];
                        if !reviewed {
                            violations.push(format!(
                                "{}: OmniError::{} derives From<{}>",
                                relative,
                                variant.ident,
                                segments.join("::")
                            ));
                        }
                    }
                }
            }

            let Item::Impl(implementation) = item else {
                continue;
            };
            let Type::Path(target) = implementation.self_ty.as_ref() else {
                continue;
            };
            if target
                .path
                .segments
                .last()
                .is_none_or(|segment| segment.ident != "OmniError")
            {
                continue;
            }
            let Some((_, trait_path, _)) = implementation.trait_ else {
                continue;
            };
            let Some(from) = trait_path.segments.last() else {
                continue;
            };
            if from.ident != "From" {
                continue;
            }
            let PathArguments::AngleBracketed(arguments) = &from.arguments else {
                continue;
            };
            let Some(GenericArgument::Type(Type::Path(source))) = arguments.args.first() else {
                continue;
            };
            let segments = source
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            // Keep every global conversion explicit. Requiring the exact
            // reviewed spelling also catches an upstream `Error` imported
            // under an alias, which type-name deny-lists miss.
            let reviewed = segments == ["omnigraph_storage", "StorageError"]
                || segments == ["ManifestInitError"]
                || segments == ["SidecarSchemaError"];
            if !reviewed {
                violations.push(format!(
                    "{}: impl From<{}> for OmniError",
                    relative,
                    segments.join("::")
                ));
            }
        }
    }

    assert_eq!(
        omni_error_enums,
        ["omnigraph-core/error.rs"],
        "the conversion review reads the one `OmniError` enum; a missing or moved enum \
         would pass it vacuously"
    );
    assert!(
        violations.is_empty(),
        "new OmniError conversions require a deliberate source-guard review; unreviewed global conversions found:\n  {}",
        violations.join("\n  ")
    );
}

#[test]
fn engine_code_does_not_call_forbidden_lance_apis() {
    let src = engine_src_root();
    let mut violations = Vec::new();

    // The final inline-commit storage escape hatch was retired with staged
    // full-table vector indexing. Pin its absence across the storage trait and
    // Omnigraph accessor so a future change cannot silently reopen it.
    for relative in ["storage_layer.rs", "db/omnigraph.rs"] {
        let file = guarded_path(&src, relative);
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

    let mut sibling_violations = Vec::new();
    for (relative, file) in labeled_scan_files(&src, false) {
        if ALLOW_LIST_FILES.contains(&relative.as_str()) {
            continue;
        }
        let violations = if GUARDED_CRATES
            .iter()
            .any(|crate_name| relative.starts_with(&format!("{crate_name}/")))
        {
            &mut sibling_violations
        } else {
            &mut violations
        };
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
                    violations.push(format!(
                        "{}:{}: forbidden pattern `{}` — {}",
                        relative,
                        idx + 1,
                        pattern,
                        line.trim()
                    ));
                }
            }
        }
    }

    let mut report = Vec::new();
    if !violations.is_empty() {
        report.push(format!(
            "{} violation(s) in engine code. Engine code MUST route through the \
             `TableStorage` trait (or its inherent counterparts on `TableStore`) instead \
             of calling Lance's inline-commit APIs directly.\n  {}",
            violations.len(),
            violations.join("\n  ")
        ));
    }
    if !sibling_violations.is_empty() {
        report.push(format!(
            "{} violation(s) in {}. The engine's storage layer is unreachable from \
             these crates: open datasets through \
             `omnigraph_core::instrumentation::{{open_dataset, open_pinned_dataset}}` \
             instead of calling Lance directly.\n  {}",
            sibling_violations.len(),
            GUARDED_CRATES.join(" / "),
            sibling_violations.join("\n  ")
        ));
    }
    if !report.is_empty() {
        panic!(
            "Forbidden-API guard found violations. If a use is genuinely justified, add \
             the comment `// forbidden-api-allow: <reason>` on the same line or the line \
             above.\n\n{}",
            report.join("\n\n")
        );
    }
}

/// `cfg(any(test, feature = "test-util"))` reads as test-only in both source guards,
/// which holds only while no production build enables a split crate's `test-util`.
#[test]
fn split_crate_test_util_is_enabled_only_by_dev_dependencies() {
    let manifests = workspace_manifests();
    let violations = test_util_violations(&manifests);
    assert!(
        violations.is_empty(),
        "a production build enables a split crate's `test-util`, so the \
         `cfg(any(test, feature = \"test-util\"))` code compiles into it; enable it from \
         [dev-dependencies] only (`REGULAR_TEST_UTIL_ENABLES` names the two test crates \
         exempt):\n  {}",
        violations.join("\n  ")
    );
}

/// The engine facade fences the split crates only while every other crate reaches
/// them through `omnigraph`; the reference engine reads them directly because it
/// may not depend on the engine.
#[test]
fn split_crates_are_dependencies_of_the_engine_only() {
    let violations = split_crate_dependents(&workspace_manifests());
    assert!(
        violations.is_empty(),
        "a crate other than the engine or the reference engine depends on omnigraph-core or \
         omnigraph-catalog directly and bypasses the engine facade; depend on `omnigraph` \
         instead:\n  {}",
        violations.join("\n  ")
    );
}

#[test]
fn split_crate_dependents_pin_resolves_renames_and_workspace_aliases() {
    let manifest = |label: &str, text: &str| (label.to_string(), text.to_string());
    let root = manifest(
        "Cargo.toml",
        "[workspace]\nmembers = []\n[workspace.dependencies]\n\
         shared = { package = \"omnigraph-core\", path = \"crates/core\" }\n",
    );
    let allowed = [
        manifest(
            "engine/Cargo.toml",
            "[package]\nname = \"omnigraph-engine\"\n[dependencies]\n\
             omnigraph-core = { path = \"../core\" }\nomnigraph-catalog = { path = \"../catalog\" }\n\
             [dev-dependencies]\nomnigraph-catalog = { path = \"../catalog\", features = [\"test-util\"] }\n",
        ),
        manifest(
            "catalog/Cargo.toml",
            "[package]\nname = \"omnigraph-catalog\"\n[dependencies]\nomnigraph-core = { path = \"../core\" }\n",
        ),
    ];
    let reference = manifest(
        "reference/Cargo.toml",
        "[package]\nname = \"omnigraph-reference-engine\"\n[dependencies]\n\
         omnigraph-core = { path = \"../core\" }\nomnigraph-catalog = { path = \"../catalog\" }\n",
    );
    assert_eq!(
        split_crate_dependents(&[
            root.clone(),
            allowed[0].clone(),
            allowed[1].clone(),
            reference
        ]),
        Vec::<String>::new(),
        "the engine and the reference engine may depend on both split crates and the catalog on core"
    );
    let offenders = [
        manifest(
            "server/Cargo.toml",
            "[package]\nname = \"server\"\n[dependencies]\nomnigraph-catalog = { path = \"../catalog\" }\n",
        ),
        manifest(
            "cli/Cargo.toml",
            "[package]\nname = \"cli\"\n[dev-dependencies]\nshared = { workspace = true }\n",
        ),
        manifest(
            "dst/Cargo.toml",
            "[package]\nname = \"dst\"\n[target.'cfg(unix)'.dependencies]\n\
             renamed = { package = \"omnigraph-catalog\", path = \"../catalog\" }\n",
        ),
    ];
    assert_eq!(
        split_crate_dependents(&[
            root,
            offenders[0].clone(),
            offenders[1].clone(),
            offenders[2].clone()
        ]),
        [
            "server/Cargo.toml: [dependencies] `omnigraph-catalog` depends on omnigraph-catalog",
            "cli/Cargo.toml: [dev-dependencies] `shared` depends on omnigraph-core",
            "dst/Cargo.toml: [target.cfg(unix).dependencies] `renamed` depends on omnigraph-catalog",
        ],
        "a plain, dev, per-target, renamed or workspace-aliased dependency on a split crate \
         from any other crate is refused"
    );
}

/// The frozen engine v1 GQT's `expect same as v1` compares engine v2 against.
const REFERENCE_ENGINE: &str = "omnigraph-reference-engine";

/// v1 reaches no production build while GQT alone names it and it names
/// neither v2's engine nor its planner. It is not in `GUARDED_CRATES`: it is
/// test-only, and its own Lance scan copy would trip the production chokepoints.
#[test]
fn reference_engine_is_a_dependency_of_gqt_only() {
    let violations = reference_engine_edges(&workspace_manifests());
    assert!(
        violations.is_empty(),
        "only omnigraph-gqt may depend on omnigraph-reference-engine, and the reference \
         engine may depend on neither omnigraph-engine nor omnigraph-planner (v1 is \
         independent of the production engine and planner; the compiler, core and catalog \
         crates are shared):\n  {}",
        violations.join("\n  ")
    );
}

#[test]
fn reference_engine_edges_pin_resolves_renames_and_workspace_aliases() {
    let manifest = |label: &str, text: &str| (label.to_string(), text.to_string());
    let root = manifest(
        "Cargo.toml",
        "[workspace]\nmembers = []\n[workspace.dependencies]\n\
         planner = { package = \"omnigraph-planner\", path = \"crates/planner\" }\n",
    );
    let allowed = [
        manifest(
            "gqt/Cargo.toml",
            "[package]\nname = \"omnigraph-gqt\"\n[dependencies]\n\
             omnigraph-reference-engine = { path = \"../reference\" }\n\
             omnigraph = { package = \"omnigraph-engine\", path = \"../engine\" }\n",
        ),
        manifest(
            "reference/Cargo.toml",
            "[package]\nname = \"omnigraph-reference-engine\"\n[dependencies]\n\
             omnigraph-core = { path = \"../core\" }\nomnigraph-catalog = { path = \"../catalog\" }\n",
        ),
    ];
    assert_eq!(
        reference_engine_edges(&[root.clone(), allowed[0].clone(), allowed[1].clone()]),
        Vec::<String>::new(),
        "GQT may depend on the reference engine, and the reference engine on the split crates"
    );
    let declared_root = manifest(
        "Cargo.toml",
        "[workspace]\nmembers = []\n[workspace.dependencies]\n\
         v1 = { package = \"omnigraph-reference-engine\", path = \"crates/reference\" }\n",
    );
    let inheriting_gqt = manifest(
        "gqt/Cargo.toml",
        "[package]\nname = \"omnigraph-gqt\"\n[dependencies]\nv1 = { workspace = true }\n",
    );
    assert_eq!(
        reference_engine_edges(&[
            declared_root.clone(),
            inheriting_gqt.clone(),
            allowed[1].clone()
        ]),
        Vec::<String>::new(),
        "a root [workspace.dependencies] declaration consumed only by GQT is no violation"
    );
    let inheriting_server = manifest(
        "server/Cargo.toml",
        "[package]\nname = \"server\"\n[dependencies]\nv1 = { workspace = true }\n",
    );
    assert_eq!(
        reference_engine_edges(&[declared_root, inheriting_gqt, inheriting_server]),
        ["server/Cargo.toml: [dependencies] `v1` depends on omnigraph-reference-engine"],
        "the same declaration consumed outside GQT is refused at the consumer"
    );
    let offenders = [
        manifest(
            "server/Cargo.toml",
            "[package]\nname = \"server\"\n[dev-dependencies]\n\
             v1 = { package = \"omnigraph-reference-engine\", path = \"../reference\" }\n",
        ),
        manifest(
            "dst/Cargo.toml",
            "[package]\nname = \"dst\"\n[target.'cfg(unix)'.dependencies]\n\
             omnigraph-reference-engine = { path = \"../reference\" }\n",
        ),
        manifest(
            "reference/Cargo.toml",
            "[package]\nname = \"omnigraph-reference-engine\"\n[dependencies]\n\
             omnigraph = { package = \"omnigraph-engine\", path = \"../engine\" }\n\
             [dev-dependencies]\nplanner = { workspace = true }\n",
        ),
    ];
    assert_eq!(
        reference_engine_edges(&[
            root,
            offenders[0].clone(),
            offenders[1].clone(),
            offenders[2].clone()
        ]),
        [
            "server/Cargo.toml: [dev-dependencies] `v1` depends on omnigraph-reference-engine",
            "dst/Cargo.toml: [target.cfg(unix).dependencies] `omnigraph-reference-engine` depends on omnigraph-reference-engine",
            "reference/Cargo.toml: [dependencies] `omnigraph` depends on omnigraph-engine",
            "reference/Cargo.toml: [dev-dependencies] `planner` depends on omnigraph-planner",
        ],
        "a dev, per-target or renamed dependency on the reference engine outside GQT, and a \
         plain or workspace-aliased engine or planner dependency of the reference engine, are refused"
    );
}

/// Every dependency on the reference engine outside GQT, and every dependency
/// of the reference engine on the engine or the planner.
fn reference_engine_edges(manifests: &[(String, String)]) -> Vec<String> {
    let mut violations = Vec::new();
    for (label, name, section, key, package) in dependency_entries(manifests) {
        let named_outside_gqt =
            package == REFERENCE_ENGINE && name.as_deref() != Some("omnigraph-gqt");
        let reaches_v2 = name.as_deref() == Some(REFERENCE_ENGINE)
            && matches!(package.as_str(), "omnigraph-engine" | "omnigraph-planner");
        if named_outside_gqt || reaches_v2 {
            violations.push(format!("{label}: [{section}] `{key}` depends on {package}"));
        }
    }
    violations
}

/// Every dependency table entry of every package as `(label, package name,
/// section, key, resolved package)`, `package =` renames and `workspace = true`
/// aliases resolved. A `[workspace.dependencies]` declaration consumes nothing.
fn dependency_entries(
    manifests: &[(String, String)],
) -> Vec<(String, Option<String>, String, String, String)> {
    let parsed = manifests
        .iter()
        .map(|(label, text)| {
            let value = toml::from_str::<toml::Value>(text)
                .unwrap_or_else(|error| panic!("{label} is not TOML: {error}"));
            (label.as_str(), value)
        })
        .collect::<Vec<_>>();
    let sections = ["dependencies", "build-dependencies", "dev-dependencies"];
    let workspace_packages = parsed
        .iter()
        .filter_map(|(_, manifest)| {
            manifest
                .get("workspace")?
                .get("dependencies")?
                .as_table()
                .cloned()
        })
        .flatten()
        .map(|(key, spec)| {
            let package = spec
                .get("package")
                .and_then(toml::Value::as_str)
                .unwrap_or(&key)
                .to_string();
            (key, package)
        })
        .collect::<BTreeMap<_, _>>();
    let mut entries = Vec::new();
    for (label, manifest) in &parsed {
        let name = manifest
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
            .map(str::to_string);
        let mut tables = Vec::new();
        for section in sections {
            if let Some(table) = manifest.get(section).and_then(toml::Value::as_table) {
                tables.push((section.to_string(), table.clone()));
            }
        }
        for (target, spec) in manifest
            .get("target")
            .and_then(toml::Value::as_table)
            .into_iter()
            .flatten()
        {
            for section in sections {
                if let Some(table) = spec.get(section).and_then(toml::Value::as_table) {
                    tables.push((format!("target.{target}.{section}"), table.clone()));
                }
            }
        }
        for (section, table) in tables {
            for (key, spec) in &table {
                let inherited = spec.get("workspace").and_then(toml::Value::as_bool) == Some(true);
                let package = spec
                    .get("package")
                    .and_then(toml::Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        inherited
                            .then(|| workspace_packages.get(key).cloned())
                            .flatten()
                    })
                    .unwrap_or_else(|| key.to_string());
                entries.push((
                    label.to_string(),
                    name.clone(),
                    section.clone(),
                    key.clone(),
                    package,
                ));
            }
        }
    }
    entries
}

/// v1 is independent of the production engine and planner: its source never
/// names the engine's modules (`engine::`) or the planner crate.
#[test]
fn reference_engine_source_names_neither_the_engine_nor_the_planner() {
    let files = sibling_crate_files(&engine_src_root(), REFERENCE_ENGINE)
        .into_iter()
        .map(|(label, path)| {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {label}: {error}"));
            (label, text)
        })
        .collect::<Vec<_>>();
    assert!(
        files.iter().any(|(label, _)| label.ends_with("/lib.rs")),
        "the walk found no reference-engine source: {files:?}"
    );
    let hits = v2_mentions(&files);
    assert!(
        hits.is_empty(),
        "the reference engine names v2's code; v1 reads through omnigraph-catalog and \
         omnigraph-core only:\n  {}",
        hits.join("\n  ")
    );
    let fixture = [(
        "fixture.rs".to_string(),
        "use crate::query;\nuse omnigraph::engine::lower;\nuse omnigraph_planner::Plan;\n"
            .to_string(),
    )];
    assert_eq!(
        v2_mentions(&fixture),
        [
            "fixture.rs:2 names `engine::`",
            "fixture.rs:3 names `omnigraph_planner`"
        ],
        "the lexical scan flags both spellings and nothing else"
    );
}

/// Each `(label, text)` line that names `engine::` or `omnigraph_planner`.
fn v2_mentions(files: &[(String, String)]) -> Vec<String> {
    let mut hits = Vec::new();
    for (label, text) in files {
        for (index, line) in text.lines().enumerate() {
            for needle in ["engine::", "omnigraph_planner"] {
                if line.contains(needle) {
                    hits.push(format!("{label}:{} names `{needle}`", index + 1));
                }
            }
        }
    }
    hits
}

/// Every dependency table entry outside the engine that resolves to a split crate,
/// `package =` renames and `workspace = true` aliases included; the catalog's own
/// dependency on core and the reference engine's on both are the other permitted edges.
fn split_crate_dependents(manifests: &[(String, String)]) -> Vec<String> {
    let parsed = manifests
        .iter()
        .map(|(label, text)| {
            let value = toml::from_str::<toml::Value>(text)
                .unwrap_or_else(|error| panic!("{label} is not TOML: {error}"));
            (label.as_str(), value)
        })
        .collect::<Vec<_>>();
    let sections = ["dependencies", "build-dependencies", "dev-dependencies"];
    let workspace_packages = parsed
        .iter()
        .filter_map(|(_, manifest)| {
            manifest
                .get("workspace")?
                .get("dependencies")?
                .as_table()
                .cloned()
        })
        .flatten()
        .map(|(key, spec)| {
            let package = spec
                .get("package")
                .and_then(toml::Value::as_str)
                .unwrap_or(&key)
                .to_string();
            (key, package)
        })
        .collect::<BTreeMap<_, _>>();
    let mut violations = Vec::new();
    for (label, manifest) in &parsed {
        let name = manifest
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str);
        if name == Some("omnigraph-engine") || name == Some(REFERENCE_ENGINE) {
            continue;
        }
        let mut tables = Vec::new();
        for section in sections {
            if let Some(table) = manifest.get(section).and_then(toml::Value::as_table) {
                tables.push((section.to_string(), table.clone()));
            }
        }
        for (target, spec) in manifest
            .get("target")
            .and_then(toml::Value::as_table)
            .into_iter()
            .flatten()
        {
            for section in sections {
                if let Some(table) = spec.get(section).and_then(toml::Value::as_table) {
                    tables.push((format!("target.{target}.{section}"), table.clone()));
                }
            }
        }
        for (section, table) in tables {
            for (key, spec) in &table {
                let inherited = spec.get("workspace").and_then(toml::Value::as_bool) == Some(true);
                let package = spec
                    .get("package")
                    .and_then(toml::Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        inherited
                            .then(|| workspace_packages.get(key).cloned())
                            .flatten()
                    })
                    .unwrap_or_else(|| key.to_string());
                let catalog_on_core =
                    package == "omnigraph-core" && name == Some("omnigraph-catalog");
                if SPLIT_CRATE_PACKAGES.contains(&package.as_str()) && !catalog_on_core {
                    violations.push(format!("{label}: [{section}] `{key}` depends on {package}"));
                }
            }
        }
    }
    violations
}

/// The workspace `Cargo.toml` and every member manifest, as `(label, text)`.
fn workspace_manifests() -> Vec<(String, String)> {
    let root = engine_src_root()
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("the engine crate lives two levels below the workspace root")
        .to_path_buf();
    let read = |label: &str| {
        let path = root.join(label);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        (label.to_string(), text)
    };
    let workspace = read("Cargo.toml");
    let workspace_manifest = toml::from_str::<toml::Value>(&workspace.1)
        .expect("the workspace Cargo.toml parses as TOML");
    let members = workspace_manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
        .expect("the workspace Cargo.toml lists [workspace] members");
    let mut manifests = vec![workspace];
    for member in members.iter().filter_map(toml::Value::as_str) {
        let directories = match member.strip_suffix("/*") {
            Some(parent) => {
                let mut children = std::fs::read_dir(root.join(parent))
                    .unwrap_or_else(|error| panic!("failed to list {parent}: {error}"))
                    .flatten()
                    .filter(|entry| entry.path().join("Cargo.toml").is_file())
                    .map(|entry| format!("{parent}/{}", entry.file_name().to_string_lossy()))
                    .collect::<Vec<_>>();
                children.sort();
                children
            }
            None => vec![member.to_string()],
        };
        manifests.extend(
            directories
                .iter()
                .map(|directory| read(&format!("{directory}/Cargo.toml"))),
        );
    }
    assert!(
        manifests.len() > 3,
        "the member walk found no manifests: {manifests:?}"
    );
    manifests
}

#[test]
fn test_util_pin_refuses_production_enables() {
    let manifest = |label: &str, text: &str| (label.to_string(), text.to_string());
    let catalog = manifest(
        "catalog/Cargo.toml",
        "[package]\nname = \"omnigraph-catalog\"\n[features]\n\
         test-util = [\"omnigraph-core/test-util\"]\n\
         [dependencies]\nomnigraph-core = { path = \"../core\" }\n\
         [dev-dependencies]\nomnigraph-core = { path = \"../core\", features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(std::slice::from_ref(&catalog)),
        Vec::<String>::new(),
        "a dev-dependency enable and a `test-util` feature forwarding to one are allowed"
    );
    let offender = manifest(
        "engine/Cargo.toml",
        "[package]\nname = \"engine\"\n[features]\n\
         helpers = [\"omnigraph-catalog/test-util\"]\n\
         maybe = [\"renamed?/test-util\"]\n\
         test-util = [\"omnigraph-core/test-util\"]\n\
         [dependencies]\nomnigraph-core = { path = \"../core\", features = [\"test-util\"] }\n\
         renamed = { package = \"omnigraph-catalog\", path = \"../catalog\", optional = true }\n\
         [build-dependencies]\nomnigraph-catalog = { path = \"../catalog\", features = [\"test-util\"] }\n\
         [target.'cfg(unix)'.dependencies]\nomnigraph-core = { path = \"../core\", features = [\"test-util\"] }\n",
    );
    let downstream = manifest(
        "server/Cargo.toml",
        "[package]\nname = \"server\"\n[dependencies]\nengine = { path = \"../engine\", features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(&[catalog, offender, downstream]),
        [
            "engine/Cargo.toml: [dependencies] `omnigraph-core` enables test-util",
            "engine/Cargo.toml: [build-dependencies] `omnigraph-catalog` enables test-util",
            "engine/Cargo.toml: [target.cfg(unix).dependencies] `omnigraph-core` enables test-util",
            "engine/Cargo.toml: [features] `helpers` enables `omnigraph-catalog/test-util`",
            "engine/Cargo.toml: [features] `maybe` enables `renamed?/test-util`",
            "server/Cargo.toml: [dependencies] `engine` enables test-util",
        ],
        "a production dependency table (plain, build, per-target, renamed), a non-`test-util` \
         feature (plain or `?/`) and a crate whose own `test-util` forwards to a split crate \
         are all refused"
    );
}

/// Every production enable of a split crate's `test-util`, and of any crate whose
/// own `test-util` feature forwards to one, across `(label, Cargo.toml text)` pairs.
fn test_util_violations(manifests: &[(String, String)]) -> Vec<String> {
    let parsed = manifests
        .iter()
        .map(|(label, text)| {
            let value = toml::from_str::<toml::Value>(text)
                .unwrap_or_else(|error| panic!("{label} is not TOML: {error}"));
            (label.as_str(), value)
        })
        .collect::<Vec<_>>();
    let dependency_tables = |manifest: &toml::Value| {
        let mut tables = Vec::new();
        for section in ["dependencies", "build-dependencies", "dev-dependencies"] {
            if let Some(table) = manifest.get(section).and_then(toml::Value::as_table) {
                tables.push((
                    section.to_string(),
                    table.clone(),
                    section == "dev-dependencies",
                ));
            }
        }
        if let Some(table) = manifest
            .get("workspace")
            .and_then(|workspace| workspace.get("dependencies"))
            .and_then(toml::Value::as_table)
        {
            tables.push(("workspace.dependencies".to_string(), table.clone(), false));
        }
        for (target, spec) in manifest
            .get("target")
            .and_then(toml::Value::as_table)
            .into_iter()
            .flatten()
        {
            for section in ["dependencies", "build-dependencies", "dev-dependencies"] {
                if let Some(table) = spec.get(section).and_then(toml::Value::as_table) {
                    tables.push((
                        format!("target.{target}.{section}"),
                        table.clone(),
                        section == "dev-dependencies",
                    ));
                }
            }
        }
        tables
    };
    let workspace_packages = parsed
        .iter()
        .filter_map(|(_, manifest)| {
            manifest
                .get("workspace")?
                .get("dependencies")?
                .as_table()
                .cloned()
        })
        .flatten()
        .map(|(key, spec)| {
            let package = spec
                .get("package")
                .and_then(toml::Value::as_str)
                .unwrap_or(&key)
                .to_string();
            (key, package)
        })
        .collect::<BTreeMap<_, _>>();
    let package_of = |key: &str, spec: &toml::Value| {
        let inherited = spec.get("workspace").and_then(toml::Value::as_bool) == Some(true);
        spec.get("package")
            .and_then(toml::Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                inherited
                    .then(|| workspace_packages.get(key).cloned())
                    .flatten()
            })
            .unwrap_or_else(|| key.to_string())
    };
    let reachable = |manifest: &toml::Value, feature: &str| {
        let table = manifest.get("features").and_then(toml::Value::as_table);
        let mut seen = BTreeSet::new();
        let mut pending = vec![feature.to_string()];
        let mut entries = BTreeSet::new();
        while let Some(name) = pending.pop() {
            if !seen.insert(name.clone()) {
                continue;
            }
            for entry in table
                .and_then(|table| table.get(&name))
                .and_then(toml::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(toml::Value::as_str)
            {
                entries.insert(entry.to_string());
                if !entry.contains('/') && !entry.starts_with("dep:") {
                    pending.push(entry.to_string());
                }
            }
        }
        entries
    };
    let forwarded = |manifest: &toml::Value, entry: &str, crates: &BTreeSet<String>| {
        let Some((dependency, feature)) = entry.split_once('/') else {
            return false;
        };
        let dependency = dependency.trim_end_matches('?');
        feature == "test-util"
            && dependency_tables(manifest).iter().any(|(_, table, _)| {
                table
                    .get(dependency)
                    .is_some_and(|spec| crates.contains(&package_of(dependency, spec)))
            })
    };
    let package_name = |manifest: &toml::Value| {
        manifest
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
            .map(str::to_string)
    };

    let mut crates = SPLIT_CRATE_PACKAGES
        .iter()
        .chain(TEST_UTIL_SEAM_PACKAGES)
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    loop {
        let before = crates.len();
        for (_, manifest) in &parsed {
            let Some(name) = package_name(manifest) else {
                continue;
            };
            if reachable(manifest, "test-util")
                .iter()
                .any(|entry| forwarded(manifest, entry, &crates))
            {
                crates.insert(name);
            }
        }
        if crates.len() == before {
            break;
        }
    }

    let mut violations = Vec::new();
    for (label, manifest) in &parsed {
        for (section, table, dev) in dependency_tables(manifest) {
            for (key, spec) in &table {
                let enables = spec
                    .get("features")
                    .and_then(toml::Value::as_array)
                    .is_some_and(|features| {
                        features
                            .iter()
                            .any(|feature| feature.as_str() == Some("test-util"))
                    });
                let package = package_of(key, spec);
                let exempt = package_name(manifest).is_some_and(|name| {
                    REGULAR_TEST_UTIL_ENABLES.contains(&(name.as_str(), package.as_str()))
                });
                if !dev && enables && crates.contains(&package) && !exempt {
                    violations.push(format!("{label}: [{section}] `{key}` enables test-util"));
                }
            }
        }
        let own_test_util = package_name(manifest).is_some_and(|name| crates.contains(&name));
        let feature_names = manifest
            .get("features")
            .and_then(toml::Value::as_table)
            .into_iter()
            .flat_map(|table| table.keys())
            .filter(|feature| *feature != "test-util");
        for feature in feature_names {
            for entry in reachable(manifest, feature) {
                if (own_test_util && entry == "test-util") || forwarded(manifest, &entry, &crates) {
                    violations.push(format!("{label}: [features] `{feature}` enables `{entry}`"));
                }
            }
        }
    }
    violations
}

#[test]
fn test_util_pin_follows_local_features_and_inherited_aliases() {
    let manifest = |label: &str, text: &str| (label.to_string(), text.to_string());
    let root = manifest(
        "Cargo.toml",
        "[workspace]\nmembers = []\n[workspace.dependencies]\n\
         shared = { package = \"omnigraph-core\", path = \"crates/core\" }\n",
    );
    let core = manifest(
        "core/Cargo.toml",
        "[package]\nname = \"omnigraph-core\"\n[features]\n\
         default = [\"helpers\"]\nhelpers = [\"test-util\"]\ntest-util = []\n",
    );
    let tests_only = manifest(
        "gqt/Cargo.toml",
        "[package]\nname = \"gqt\"\n[dev-dependencies]\n\
         shared = { workspace = true, features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(&[root.clone(), tests_only.clone()]),
        Vec::<String>::new(),
        "an inherited alias enabled from [dev-dependencies] is allowed"
    );
    let server = manifest(
        "server/Cargo.toml",
        "[package]\nname = \"server\"\n[features]\n\
         extra = [\"chain\"]\nchain = [\"shared/test-util\"]\n\
         [dependencies]\nshared = { workspace = true, features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(&[root, core, tests_only, server]),
        [
            "core/Cargo.toml: [features] `default` enables `test-util`",
            "core/Cargo.toml: [features] `helpers` enables `test-util`",
            "server/Cargo.toml: [dependencies] `shared` enables test-util",
            "server/Cargo.toml: [features] `chain` enables `shared/test-util`",
            "server/Cargo.toml: [features] `extra` enables `shared/test-util`",
        ],
        "a default or other local feature reaching `test-util`, a production enable through a \
         `workspace = true` alias, and a forward reached through local features are all refused"
    );
}

const SPLIT_CRATE_PACKAGES: &[&str] = &["omnigraph-core", "omnigraph-catalog"];

/// Crates outside the split whose own `test-util` gates a test seam
/// (`omnigraph-cluster`: `read_serving_snapshot_with_display_root`).
const TEST_UTIL_SEAM_PACKAGES: &[&str] = &["omnigraph-cluster"];

#[test]
fn test_util_pin_covers_the_cluster_seam() {
    let manifest = |label: &str, text: &str| (label.to_string(), text.to_string());
    let cluster = manifest(
        "cluster/Cargo.toml",
        "[package]\nname = \"omnigraph-cluster\"\n[features]\ntest-util = []\n",
    );
    let tests_only = manifest(
        "server/Cargo.toml",
        "[package]\nname = \"omnigraph-server\"\n[dev-dependencies]\n\
         omnigraph-cluster = { path = \"../cluster\", features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(&[cluster.clone(), tests_only]),
        Vec::<String>::new()
    );
    let production = manifest(
        "server/Cargo.toml",
        "[package]\nname = \"omnigraph-server\"\n[features]\n\
         helpers = [\"omnigraph-cluster/test-util\"]\n[dependencies]\n\
         omnigraph-cluster = { path = \"../cluster\", features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(&[cluster, production]),
        [
            "server/Cargo.toml: [dependencies] `omnigraph-cluster` enables test-util",
            "server/Cargo.toml: [features] `helpers` enables `omnigraph-cluster/test-util`",
        ]
    );
}

/// The only regular `test-util` enables, `(enabler, enabled)`: both enablers are
/// unpublished test crates, and `reference_engine_is_a_dependency_of_gqt_only`
/// keeps the reference engine out of every other dependency graph.
const REGULAR_TEST_UTIL_ENABLES: &[(&str, &str)] = &[
    ("omnigraph-reference-engine", "omnigraph-catalog"),
    ("omnigraph-gqt", "omnigraph-engine"),
];

#[test]
fn test_util_pin_allows_only_the_two_test_crate_enables() {
    let manifest = |label: &str, text: &str| (label.to_string(), text.to_string());
    let catalog = manifest(
        "catalog/Cargo.toml",
        "[package]\nname = \"omnigraph-catalog\"\n[features]\ntest-util = []\n",
    );
    let engine = manifest(
        "engine/Cargo.toml",
        "[package]\nname = \"omnigraph-engine\"\n[features]\n\
         test-util = [\"omnigraph-catalog/test-util\"]\n\
         [dependencies]\nomnigraph-catalog = { path = \"../catalog\" }\n",
    );
    let reference = manifest(
        "reference/Cargo.toml",
        "[package]\nname = \"omnigraph-reference-engine\"\n[dependencies]\n\
         omnigraph-catalog = { path = \"../catalog\", features = [\"test-util\"] }\n",
    );
    let gqt = manifest(
        "gqt/Cargo.toml",
        "[package]\nname = \"omnigraph-gqt\"\n[dependencies]\n\
         omnigraph = { package = \"omnigraph-engine\", path = \"../engine\", features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(&[catalog.clone(), engine.clone(), reference, gqt]),
        Vec::<String>::new(),
        "the reference engine may enable the catalog's test-util and GQT the engine's"
    );
    let swapped_reference = manifest(
        "reference/Cargo.toml",
        "[package]\nname = \"omnigraph-reference-engine\"\n[dependencies]\n\
         omnigraph = { package = \"omnigraph-engine\", path = \"../engine\", features = [\"test-util\"] }\n",
    );
    let swapped_gqt = manifest(
        "gqt/Cargo.toml",
        "[package]\nname = \"omnigraph-gqt\"\n[dependencies]\n\
         omnigraph-catalog = { path = \"../catalog\", features = [\"test-util\"] }\n",
    );
    let server = manifest(
        "server/Cargo.toml",
        "[package]\nname = \"server\"\n[dependencies]\n\
         omnigraph = { package = \"omnigraph-engine\", path = \"../engine\", features = [\"test-util\"] }\n",
    );
    assert_eq!(
        test_util_violations(&[catalog, engine, swapped_reference, swapped_gqt, server]),
        [
            "reference/Cargo.toml: [dependencies] `omnigraph` enables test-util",
            "gqt/Cargo.toml: [dependencies] `omnigraph-catalog` enables test-util",
            "server/Cargo.toml: [dependencies] `omnigraph` enables test-util",
        ],
        "each allowance names one enabler and one enabled crate, and every other crate is refused"
    );
}
