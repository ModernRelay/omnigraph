//! Internal schema versioning for the `__manifest` Lance dataset.
//!
//! ## Why this exists
//!
//! The on-disk shape of `__manifest` evolves alongside the engine. This module
//! is the *single* place where on-disk shape is reconciled with what the binary
//! expects:
//!
//! - One constant `INTERNAL_MANIFEST_SCHEMA_VERSION` declares the shape this
//!   binary writes.
//! - One stamp `omnigraph:internal_schema_version` in the manifest dataset's
//!   schema-level metadata records the on-disk shape.
//! - One guard `refuse_if_stamp_unsupported` rejects any graph this binary
//!   cannot serve — in either direction — with a clear, actionable error.
//!
//! ## Served stamps and explicit conversion
//!
//! Normal open accepts `MIN_SUPPORTED..=CURRENT`, which since v10 is one
//! stamp: every served graph may carry table pins that name a detached Lance
//! version and no linear one (RFC "Detached-only tables"), which an older
//! binary would misread as reclaimed history. Both RFC 0040 system column
//! vintages (`id`/`src`/`dst` and `__id`/`__src`/`__dst`) live under that
//! stamp; the vintage is read from the schema IR, never from the stamp.
//! Normal open refuses an active storage-upgrade intent. The explicit
//! offline upgrade entry point converts supported v6 to v12 graphs to v13
//! before serving; its v10 step opens the graph through an engine handle,
//! admitted at the stamp it converts from by [`admit_conversion_source`],
//! before it fences. Retained v6 snapshots use the legacy decoder after root
//! admission; normal open never runs conversion or lowers MIN_SUPPORTED.
//! Fresh graphs receive their stamp atomically in the manifest Create commit.
//!
//! ## Forward-version protection
//!
//! A stamp *higher* than this binary's version triggers a clear "upgrade
//! omnigraph first" error. An old binary cannot clobber a newer schema by
//! silently treating "unknown stamp" as "missing stamp".

use std::collections::{HashMap, HashSet};

use lance::Dataset;
use lance::dataset::refs::BranchIdentifier;
use lance::dataset::transaction::{Operation, Transaction, UpdateMap};
use omnigraph_compiler::{SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_V3, SystemColumns};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

#[cfg(any(test, feature = "test-util", feature = "dst"))]
use crate::commit::commit_overwrite;
use crate::error::{OmniError, Result};
use crate::record::{
    RECORD_COLUMN, compact_to_storage, flat_manifest_schema, manifest_storage_schema,
};
#[cfg(any(test, feature = "test-util", feature = "dst"))]
use crate::record::{expand_from_storage, flat_to_storage};
use crate::state::{SchemaContractHead, SchemaContractRow, entries_to_batch, read_publish_scan};

/// The internal schema version this binary writes for a new-vintage graph and
/// the ceiling it serves.
///
/// History:
/// - v1 — implicit (pre-stamp). `__manifest.object_id` carried no
///   `lance-schema:unenforced-primary-key` annotation.
/// - v2 — `__manifest.object_id` carries the unenforced-PK annotation,
///   engaging Lance's bloom-filter conflict resolver at commit time.
/// - v3 — one-time sweep of legacy `__run__<id>` staging branches left on the
///   `__manifest` dataset by the pre-v0.4.0 Run state machine.
/// - v4 — RFC-013 Phase 7 folds graph lineage into `__manifest` as
///   `graph_commit`/`graph_head` rows written in the publish CAS (no
///   `_graph_commits.lance`).
/// - v5 — RFC-028 adds non-zero stable-table/incarnation identity columns;
///   table registration, version, tombstone, fold, OCC, and physical paths are
///   keyed by that immutable identity rather than the mutable table alias.
/// - v6 — RFC-023 makes every node/edge physical table keyed by exactly its
///   non-null `id` field using Lance's unenforced-primary-key metadata. The
///   annotation is present at dataset creation and preserved by overwrites;
///   older graphs cross this immutable boundary by export/init/load rebuild.
/// - v7 — RFC-0062 re-keys `__manifest` registration and tombstone rows on
///   `(identity, manifest_version)`, the `__manifest` version that wrote the
///   row, carried as the row key's trailing segment, and projects a table's
///   current registration by the greatest manifest version instead of the
///   greatest per-native-ref Lance version. The unreleased v7–v19 stamps of the
///   rejected MemWAL experiment never shipped; v7 is reused.
/// - v8 — native graph refs retain ancestry through versioned retirement metadata.
///   Old readers must refuse rather than expose retired refs as live branches.
/// - v9 — RFC-0040 spells the system columns `__id`/`__src`/`__dst`, freeing
///   `id`, `src`, `dst` for user properties. Stamped on every graph created
///   with the new spellings; a v8 graph keeps the legacy spellings and its
///   stamp. RFC 0040 Rollout step 3 defines the v8 → v9 upgrade.
/// - v10 — RFC 0067 lets a table registration name a detached Lance version
///   (`omnigraph.staged_version`, `omnigraph.transaction_uuid`) whose linear
///   target is published before it exists. An older binary would open the
///   absent target and misdiagnose a pending pin as reclaimed history, so
///   the stamp refuses it before any open. Both system column vintages are
///   stamped v10; the stamp no longer encodes the vintage.
/// - v11 — RFC "Detached-only tables": a pin is never promoted, so
///   `published_dataset_version` names no Lance version and a table's history
///   is its chain of detached commits. A registration carries
///   `omnigraph.last_linear_version`, the highest linear version a v10 pin
///   reached; rows at or below it keep the v10 twin rule, rows above it open
///   their detached version directly. A v10 binary would read a v11 pin's
///   target as reclaimed history, so the stamp refuses it before any open.
/// - v12 — the `__manifest` rows are stored as `object_id`, `object_type` and
///   one packed `record` struct (`record.rs`); `base_objects` is dropped.
/// - v13 — the schema contract is one live `schema_contract` row in main's
///   `__manifest`: the `.pg` source and the IR text in the `schema_source`
///   and `schema_ir` columns beside `record`, the IR hash and the identity
///   version and domain in the row's `metadata`. A publish that applies a
///   schema replaces the row in the same commit as the table rows and the
///   head. A v12 binary would open a v13 graph without reading the row, so
///   the stamp refuses it before any open.
///
/// v1–v12 graphs are not served by this binary (see `MIN_SUPPORTED`); the
/// history is kept for provenance and to document what each stamp value meant.
pub const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = 13;

/// The first stamp whose `__manifest` rows are stored as `object_id`,
/// `object_type` and one packed `record` struct (`record.rs`). A stamp-11
/// manifest keeps its flat columns until a publish rewrites it.
pub const PACKED_RECORD_STAMP: u32 = 12;

/// The last stamp stored flat, and the oldest stamp a publish rewrites into
/// the packed shape at [`INTERNAL_MANIFEST_SCHEMA_VERSION`]; a publish over a
/// lower stamp keeps the flat shape and the source stamp, the state a storage
/// conversion reads while it runs.
pub const LAST_FLAT_STAMP: u32 = PACKED_RECORD_STAMP - 1;

/// The oldest main-manifest stamp accepted by normal open: v13, the stamp
/// every graph is born with since the schema contract moved into
/// `__manifest`. Explicit conversion and retained-snapshot decoding do not
/// lower this gate.
pub const MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION: u32 = 13;

/// The stamp a fresh graph of the given system column vintage is born with:
/// CURRENT for both `id`/`src`/`dst` and `__id`/`__src`/`__dst` since v10.
/// The vintage itself is read from the schema IR's feature set, never from
/// the stamp; the stamp is the storage-format fence old binaries refuse on.
/// An unknown vintage is still refused here so a new spelling cannot be born
/// without a decision about its stamp.
pub fn stamp_for_system_columns(system_columns: SystemColumns) -> Result<u32> {
    if system_columns == SYSTEM_COLUMNS_LEGACY || system_columns == SYSTEM_COLUMNS_V3 {
        Ok(INTERNAL_MANIFEST_SCHEMA_VERSION)
    } else {
        Err(OmniError::manifest_internal(format!(
            "system column spellings '{}'/'{}'/'{}' belong to no known vintage",
            system_columns.id, system_columns.src, system_columns.dst
        )))
    }
}

/// The omnigraph release or exact development build that wrote a given
/// internal-schema stamp. The
/// open-refusal uses it to tell an operator exactly which binary to use to
/// export a graph stamped below `MIN_SUPPORTED` (the export side of the
/// strand-model upgrade — see `docs/user/operations/upgrade.md`). Ranges are
/// the release tags that stamped each version (verify with
/// `git show vX.Y.Z:crates/omnigraph/src/db/manifest/migrations.rs`):
/// v1 ≤ 0.3.1, v2 0.4.1–0.6.1, v3 0.6.2–0.7.2, v4 0.8.x, v5 was
/// unreleased (final source commit pinned below), v6 is 0.9.x–0.10.x,
/// v7 an unreleased development build, v8 and v9 are the two 0.11.x
/// system column vintages, v10 is the 0.11.x line after RFC 0067 (the
/// stamp of every graph the 0.11.0 crate version wrote with detached table
/// commits), and v11 and v12 are the 0.11.x development line before the
/// schema contract moved into `__manifest`. The fallback keeps this map total.
pub fn release_for_internal_schema_version(stamp: u32) -> &'static str {
    match stamp {
        1 => "0.3.1 or earlier",
        2 => "0.4.1 to 0.6.1",
        3 => "0.6.2 to 0.7.2",
        4 => "0.8.x",
        5 => {
            "built from unreleased final-v5 source commit 46b6d9084fb629b88d4ac9e8c546e0a30d213d19"
        }
        6 => "0.9.x or 0.10.x",
        7 => "an unreleased v7 development build",
        8 => "0.11.x (legacy system column spellings)",
        9 => "0.11.x",
        10 => "0.11.x (detached table commits)",
        11 => "0.11.x (detached-only tables)",
        12 => "0.11.x (packed catalog record)",
        _ => "an unrecognized older release",
    }
}

/// Roots a storage conversion may open through an engine handle at the stamp
/// it converts from, keyed by `__manifest` URI; `guard_stamp` admits exactly
/// that stamp for an admitted root and refuses everything else as before.
static CONVERSION_ADMISSIONS: std::sync::Mutex<Vec<(String, u32)>> =
    std::sync::Mutex::new(Vec::new());

/// One admission, withdrawn when dropped.
pub struct ConversionAdmission {
    manifest_uri: String,
    stamp: u32,
}

fn conversion_admissions() -> std::sync::MutexGuard<'static, Vec<(String, u32)>> {
    CONVERSION_ADMISSIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Admit `root_uri`'s `__manifest`, and every branch of it, at `stamp` for
/// the lifetime of the returned guard.
pub fn admit_conversion_source(root_uri: &str, stamp: u32) -> ConversionAdmission {
    let manifest_uri = super::manifest_uri(root_uri);
    conversion_admissions().push((manifest_uri.clone(), stamp));
    ConversionAdmission {
        manifest_uri,
        stamp,
    }
}

impl Drop for ConversionAdmission {
    fn drop(&mut self) {
        let mut admissions = conversion_admissions();
        if let Some(index) = admissions
            .iter()
            .position(|(uri, stamp)| *uri == self.manifest_uri && *stamp == self.stamp)
        {
            admissions.remove(index);
        }
    }
}

/// The stamp `dataset`'s root is admitted at, when a conversion holds one.
fn admitted_conversion_source(dataset: &Dataset) -> Option<u32> {
    let uri = dataset.uri();
    conversion_admissions()
        .iter()
        .find(|(manifest_uri, _)| {
            uri == manifest_uri || uri.starts_with(&format!("{manifest_uri}/"))
        })
        .map(|(_, stamp)| *stamp)
}

pub const INTERNAL_SCHEMA_VERSION_KEY: &str = "omnigraph:internal_schema_version";

/// The schema-metadata entry stamping a fresh manifest at `stamp`. Folded into
/// the Arrow schema of init's `Dataset::write` so the stamp lands in the same
/// Lance commit that creates `__manifest` — the atomic-birth half of the
/// torn-init fix (the other half is `guard_stamp`'s absent arm).
pub(crate) fn stamp_entry(stamp: u32) -> (String, String) {
    (INTERNAL_SCHEMA_VERSION_KEY.to_string(), stamp.to_string())
}

/// Read the on-disk stamp from `__manifest`'s schema-level metadata for
/// display surfaces (`omnigraph snapshot`). `None` covers both an absent key
/// and an unparseable value; the open paths never use this — they go through
/// `guard_stamp`, which distinguishes those shapes and refuses each with its
/// own diagnosis instead of flooring to a version.
pub fn read_stamp(dataset: &Dataset) -> Option<u32> {
    dataset
        .schema()
        .metadata
        .get(INTERNAL_SCHEMA_VERSION_KEY)
        .and_then(|s| s.parse().ok())
}

/// The single stamp gate for every open path: read the stamp and refuse
/// anything this binary cannot serve, with an honest diagnosis for each shape.
///
/// - A parseable stamp — the ordinary floor/ceiling refusal
///   (`refuse_if_stamp_unsupported`).
/// - A stamp key whose value is not a version number — refused naming the
///   raw value. Never classified as absent: a corrupt stamp must not flow
///   into the delete-and-re-init advice below.
/// - No stamp key on a manifest with the modern layout — not a genuine
///   pre-stamp (v1) manifest, because the RFC-028 identity columns arrived at
///   v5, after stamping began. This can be an older binary's init interrupted
///   between the `__manifest` Create commit and its separate stamp commit, or
///   damaged/externally modified metadata on a graph that progressed further.
///   Those cases are indistinguishable from the remaining metadata, so the
///   guard fails closed. Delete-and-re-init is advised only when the operator
///   independently knows initialization never completed; otherwise the root
///   must be preserved for investigation or recovery.
/// - No stamp key on a pre-modern layout — the genuine pre-stamp world:
///   treated as v1 and refused through the ordinary sub-floor message naming
///   the 0.3.1 export path.
pub fn guard_stamp(dataset: &Dataset) -> Result<u32> {
    if dataset.schema().metadata.contains_key(UPGRADE_PENDING_KEY) {
        return Err(OmniError::manifest(recovery_guidance(dataset)));
    }
    match dataset.schema().metadata.get(INTERNAL_SCHEMA_VERSION_KEY) {
        Some(value) => match value.parse::<u32>() {
            Ok(stamp) => {
                if admitted_conversion_source(dataset) != Some(stamp) {
                    refuse_if_stamp_unsupported(stamp)?;
                }
                Ok(stamp)
            }
            Err(_) => Err(OmniError::manifest(format!(
                "__manifest carries an internal-schema stamp that is not a version \
                 number ('{value}'). The stamp metadata may be corrupt; refusing to \
                 open rather than guess the storage format.",
            ))),
        },
        None if manifest_layout_is_modern(dataset) => Err(OmniError::manifest(
            "__manifest has the current manifest layout but no internal-schema stamp. \
             This may be an interrupted `omnigraph init` from an older binary, which \
             stamped `__manifest` in a separate commit, or damaged or externally \
             modified metadata. OmniGraph cannot safely distinguish those cases and \
             will not open the graph. If you know initialization never completed, \
             delete the graph root and run `omnigraph init` again. Otherwise preserve \
             the root and investigate or restore from a known-good backup; do not \
             reinitialize it in place.",
        )),
        None => {
            refuse_if_stamp_unsupported(1)?;
            Ok(1)
        }
    }
}

/// Whether `__manifest`'s schema carries the RFC-028 stable-identity columns
/// (v5+). Distinguishes an unstamped modern manifest (possible interrupted init
/// or metadata damage) from a genuine pre-stamp v1 store — free, since the
/// schema is already in memory when the stamp is read.
fn manifest_layout_is_modern(dataset: &Dataset) -> bool {
    (dataset.schema().field("stable_table_id").is_some()
        && dataset.schema().field("table_incarnation_id").is_some())
        || dataset.schema().field(RECORD_COLUMN).is_some()
}

/// Whether this binary serves a `__manifest` stamped `stamp`: the served range
/// is one predicate, so the upgrade and respelling preflights cannot drift from
/// the open guard.
pub fn is_served_stamp(stamp: u32) -> bool {
    (MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION..=INTERNAL_MANIFEST_SCHEMA_VERSION).contains(&stamp)
}

/// Refuse to open a manifest whose stamp this binary cannot serve — in either
/// direction — with a clear, actionable path. Shared by every open path (the
/// read-write open guard, the read-only open guard, and the publisher), so a new
/// stamp-reading caller gets the floor and the ceiling together and cannot
/// half-enforce.
///
/// - `stamp > CURRENT`: the graph was written by a newer binary — upgrade omnigraph.
/// - `stamp < MIN_SUPPORTED`: the graph was made by an older omnigraph whose
///   storage format this binary does not read — use a registered offline
///   upgrade route when available, otherwise rebuild via export/import.
/// - `MIN_SUPPORTED..=CURRENT` is served as-is and never migrated on open.
pub fn refuse_if_stamp_unsupported(stamp: u32) -> Result<()> {
    if stamp > INTERNAL_MANIFEST_SCHEMA_VERSION {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{} but this binary reads only v{} to v{} \
             — upgrade omnigraph before opening this graph",
            stamp, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION, INTERNAL_MANIFEST_SCHEMA_VERSION,
        )));
    }
    if stamp < MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION {
        let explicit_upgrade = if matches!(stamp, 6..=12) {
            " A registered in-place route is also available: stop all readers, writers and maintenance, retain a verified backup, and run `omnigraph upgrade <graph> --check` before execution; the route keeps the graph's branches and system column spellings."
        } else {
            ""
        };
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{stamp}, but this omnigraph reads only v{min} to v{current}. \
             This graph was created by omnigraph {release}. Rebuild it: with an omnigraph {release} binary run \
             `omnigraph export <graph> > graph.jsonl`, relocate each record's `data.id` to the top-level `id`, \
             then with this binary run \
             `omnigraph init --schema <schema.pg> <new-graph>` and \
             `omnigraph load --mode overwrite --data graph.jsonl <new-graph>`. \
             (Data, vectors, and blobs are preserved; commit history and branches are not.) \
             See docs/user/operations/upgrade.md.{explicit_upgrade}",
            min = MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
            current = INTERNAL_MANIFEST_SCHEMA_VERSION,
            release = release_for_internal_schema_version(stamp),
        )));
    }
    Ok(())
}

pub const UPGRADE_PENDING_KEY: &str = "omnigraph:storage_upgrade_pending";
pub const UPGRADE_RECEIPT_KEY: &str = "omnigraph:storage_upgrade_receipt";
pub const MAX_BRANCHES: usize = 1024;
pub const MAX_INTENT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceBranch {
    pub native: Option<String>,
    pub identity: BranchIdentifier,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpgradeIntent {
    pub protocol: u32,
    pub attempt: String,
    pub source_format: u32,
    pub target_format: u32,
    pub graph_identity: String,
    pub branches: Vec<SourceBranch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_contract: Option<UpgradeSchemaContract>,
}

/// Protocol 5 binds semantic identity and exact text across branch conversions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpgradeSchemaContract {
    pub identity: SchemaContractHead,
    pub source_sha256: String,
    pub ir_sha256: String,
}

impl UpgradeSchemaContract {
    pub fn from_row(row: &SchemaContractRow) -> Self {
        Self {
            identity: row.head.clone(),
            source_sha256: format!("{:x}", Sha256::digest(row.source.as_bytes())),
            ir_sha256: format!("{:x}", Sha256::digest(row.ir.as_bytes())),
        }
    }

    pub fn validate_row(&self, row: &SchemaContractRow) -> Result<()> {
        if *self != Self::from_row(row) {
            return Err(invalid(
                "upgrade schema contract identity or exact text changed",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchReceipt {
    protocol: u32,
    attempt: String,
    source: SourceBranch,
}

pub fn invalid(message: impl Into<String>) -> OmniError {
    OmniError::manifest(message)
}

fn valid_sha256(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn intent_from(dataset: &Dataset) -> Result<Option<UpgradeIntent>> {
    let Some(json) = dataset.schema().metadata.get(UPGRADE_PENDING_KEY) else {
        return Ok(None);
    };
    if json.len() > MAX_INTENT_BYTES {
        return Err(invalid(
            "storage upgrade intent exceeds the metadata budget",
        ));
    }
    let intent: UpgradeIntent = serde_json::from_str(json)
        .map_err(|e| invalid(format!("unrecognized upgrade ownership: {e}")))?;
    let contract_valid = match &intent.schema_contract {
        Some(contract) => {
            intent.protocol == 5
                && contract.identity.schema_identity_domain == intent.graph_identity
                && contract.identity.schema_identity_version != 0
                && contract
                    .identity
                    .schema_ir_hash
                    .strip_prefix("sha256:")
                    .is_some_and(valid_sha256)
                && [&contract.source_sha256, &contract.ir_sha256]
                    .iter()
                    .all(|hash| valid_sha256(hash))
        }
        None => intent.protocol != 5,
    };
    let mut names = HashSet::new();
    if !matches!(
        (intent.protocol, intent.source_format, intent.target_format),
        (1, 6, 7) | (2, 7, 8) | (3, 8, 10) | (3, 9, 10) | (4, 10, 11) | (5, 11, 13) | (5, 12, 13)
    ) || !contract_valid
        || intent.attempt.parse::<ulid::Ulid>().is_err()
        || intent.graph_identity.is_empty()
        || intent.branches.is_empty()
        || intent.branches.len() > MAX_BRANCHES
        || intent
            .branches
            .last()
            .is_none_or(|branch| branch.native.is_some())
        || intent
            .branches
            .iter()
            .any(|branch| branch.version == 0 || !names.insert(branch.native.clone()))
    {
        return Err(invalid("unsupported or ambiguous storage upgrade intent"));
    }
    Ok(Some(intent))
}

pub fn recovery_guidance(dataset: &Dataset) -> String {
    match intent_from(dataset) {
        Ok(Some(intent)) => format!(
            "storage upgrade recovery required: stop all writers and maintenance and rerun `omnigraph upgrade <graph> --to-format {}` with this upgrade-capable executable; preserve the existing attempt.{}",
            intent.target_format,
            if intent.target_format < INTERNAL_MANIFEST_SCHEMA_VERSION { " After completion, run `omnigraph upgrade <graph>` to reach the served format before opening with this executable." } else { "" },
        ),
        _ => "storage upgrade ownership is unknown: preserve the graph and original upgrade options; use this upgrade-capable executable for read-only `omnigraph upgrade <graph> --check` diagnostics before recovery".into(),
    }
}

pub fn receipt(branch: &SourceBranch, intent: &UpgradeIntent) -> BranchReceipt {
    BranchReceipt {
        protocol: intent.protocol,
        attempt: intent.attempt.clone(),
        source: branch.clone(),
    }
}

pub fn branch_completed(
    dataset: &Dataset,
    source: &SourceBranch,
    intent: &UpgradeIntent,
) -> Result<bool> {
    let Some(raw) = dataset.schema().metadata.get(UPGRADE_RECEIPT_KEY) else {
        return Ok(false);
    };
    if raw.len() > MAX_INTENT_BYTES {
        return Err(invalid(
            "storage upgrade receipt exceeds the metadata budget",
        ));
    }
    let found: BranchReceipt =
        serde_json::from_str(raw).map_err(|e| invalid(format!("invalid upgrade receipt: {e}")))?;
    if found != receipt(source, intent) {
        let source_head = source
            .version
            .checked_add(u64::from(
                source.native.is_none()
                    && dataset.schema().metadata.contains_key(UPGRADE_PENDING_KEY),
            ))
            .ok_or_else(|| invalid("upgrade version overflow"))?;
        if intent.protocol > found.protocol
            && found.attempt != intent.attempt
            && dataset.version().version == source_head
        {
            return Ok(false);
        }
        return Err(invalid("foreign upgrade receipt"));
    }
    if read_stamp(dataset) != Some(intent.target_format) {
        return Err(invalid("upgrade receipt has an incompatible format"));
    }
    let expected = source
        .version
        .checked_add(if source.native.is_none() { 2 } else { 1 })
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    let activated =
        source.native.is_none() && !dataset.schema().metadata.contains_key(UPGRADE_PENDING_KEY);
    let expected = expected
        .checked_add(u64::from(activated))
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    if dataset.version().version != expected {
        return Err(invalid(
            "upgraded branch moved while conversion was incomplete",
        ));
    }
    Ok(true)
}

pub async fn historical_source(snapshot: Dataset, source_format: u32) -> Result<Dataset> {
    if !matches!(source_format, 7 | 13)
        || !snapshot.schema().metadata.contains_key(UPGRADE_PENDING_KEY)
    {
        return Ok(snapshot);
    }
    let intent =
        intent_from(&snapshot)?.ok_or_else(|| invalid("historical upgrade intent disappeared"))?;
    if !matches!(intent.protocol, 1 | 5) || snapshot.manifest().branch.is_some() {
        return Err(invalid("unsupported historical upgrade ownership"));
    }
    let main = intent
        .branches
        .last()
        .ok_or_else(|| invalid("historical upgrade has no main source"))?;
    if branch_completed(&snapshot, main, &intent)? {
        return Ok(snapshot);
    }
    let expected = main
        .version
        .checked_add(1)
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    let transaction = snapshot
        .read_transaction()
        .await
        .map_err(OmniError::storage)?
        .ok_or_else(|| invalid("historical upgrade fence has no transaction proof"))?;
    let json = snapshot
        .schema()
        .metadata
        .get(UPGRADE_PENDING_KEY)
        .ok_or_else(|| invalid("historical upgrade intent disappeared"))?
        .clone();
    let operation = Transaction::new(
        main.version,
        fence_operation(json, intent.target_format),
        None,
    );
    if snapshot.version().version != expected
        || read_stamp(&snapshot) != Some(intent.target_format)
        || transaction.read_version != main.version
        || lance_table::format::pb::Transaction::from(&transaction).operation
            != lance_table::format::pb::Transaction::from(&operation).operation
        || crate::branch_control::dataset_branch_identifier(&snapshot)
            .await
            .map_err(OmniError::storage)?
            != main.identity
    {
        return Err(invalid(
            "historical upgrade fence does not match its exact source",
        ));
    }
    let source = snapshot
        .checkout_version(main.version)
        .await
        .map_err(OmniError::storage)?;
    if read_stamp(&source) != Some(intent.source_format) {
        return Err(invalid(
            "historical upgrade fence source format differs from the intent",
        ));
    }
    Ok(source)
}

pub fn fence_operation(intent: String, target: u32) -> Operation {
    Operation::UpdateConfig {
        config_updates: None,
        table_metadata_updates: None,
        field_metadata_updates: HashMap::new(),
        schema_metadata_updates: Some(UpdateMap {
            update_entries: vec![
                (INTERNAL_SCHEMA_VERSION_KEY.to_string(), target.to_string()).into(),
                (UPGRADE_PENDING_KEY.to_string(), intent).into(),
            ],
            replace: false,
        }),
    }
}

#[cfg(any(test, feature = "test-util"))]
pub async fn set_stamp(dataset: &mut Dataset, version: u32) -> Result<()> {
    dataset
        .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY.to_string(), version.to_string())])
        .await
        .map_err(OmniError::storage)?;
    Ok(())
}

/// Test-only: force the on-disk internal-schema stamp to `version`. The minimal
/// seam used to synthesize a sub-CURRENT graph and assert the open path refuses
/// it. Its callers are the refusal tests here and in the engine, so it compiles
/// only under `test` or the `test-util` feature.
/// It changes the metadata only, so packed rows stay under the new stamp: below
/// the served floor the guard refuses the manifest, at v11 the flat projection
/// fails; a fixture that must READ uses [`restamp_flat_for_test`].
#[cfg(any(test, feature = "test-util"))]
pub async fn set_stamp_for_test(dataset: &mut Dataset, version: u32) -> Result<()> {
    set_stamp(dataset, version).await
}

/// Test and DST fixture: a stored batch of either shape laid out flat (the stamps 5 to 11
/// shape) under `metadata`, less any `schema_contract` row, which no flat
/// manifest ever carried.
#[cfg(any(test, feature = "test-util", feature = "dst"))]
pub fn flat_batch_for_test(
    batch: &RecordBatch,
    metadata: HashMap<String, String>,
) -> Result<RecordBatch> {
    let logical = if batch.column_by_name(RECORD_COLUMN).is_some() {
        expand_from_storage(batch)?
    } else {
        batch.clone()
    };
    let keep = arrow_array::BooleanArray::from_iter(
        crate::state::string_column(&logical, "object_type")?
            .iter()
            .map(|object_type| Some(object_type != Some(crate::OBJECT_TYPE_SCHEMA_CONTRACT))),
    );
    let logical = datafusion::arrow::compute::filter_record_batch(&logical, &keep)
        .map_err(OmniError::arrow_internal)?;
    let schema = Arc::new(Schema::new_with_metadata(
        flat_manifest_schema().fields().clone(),
        metadata,
    ));
    flat_to_storage(&logical, &schema)
}

/// Validate the exact source layout without interpreting a restamped current row as legacy.
pub fn validate_schema_contract_source(dataset: &Dataset) -> Result<()> {
    let expected = match read_stamp(dataset) {
        Some(11) => flat_manifest_schema(),
        Some(12) => packed_v12_schema(dataset.schema().metadata.clone())?,
        _ => {
            return Err(invalid(
                "schema-contract conversion requires format 11 or 12",
            ));
        }
    };
    let actual: Schema = dataset.schema().into();
    if actual.fields() != expected.fields() {
        return Err(invalid(
            "source manifest columns do not match its registered format",
        ));
    }
    Ok(())
}

/// Read a converted branch without admitting its pending root for ordinary serving.
pub async fn read_upgrade_schema_contract(dataset: &Dataset) -> Result<SchemaContractRow> {
    let state = crate::state::read_manifest_state(dataset).await?;
    crate::state::read_schema_contract_row(dataset, state.schema_contract.as_ref()).await
}

fn packed_v12_schema(metadata: HashMap<String, String>) -> Result<SchemaRef> {
    let current = manifest_storage_schema(metadata.clone())?;
    Ok(Arc::new(Schema::new_with_metadata(
        current.fields().iter().take(3).cloned().collect::<Vec<_>>(),
        metadata,
    )))
}

/// Preserve every live source row while adding the contract in the current packed shape.
pub async fn schema_contract_conversion_batches(
    source: &Dataset,
    metadata: HashMap<String, String>,
    contract: &SchemaContractRow,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    validate_schema_contract_source(source)?;
    let scan = read_publish_scan(source).await?;
    if scan.schema_contract.is_some() {
        return Err(invalid(
            "legacy source unexpectedly carries a schema contract row",
        ));
    }
    let schema = manifest_storage_schema(metadata)?;
    let mut rows = scan.live_rows;
    rows.push(entries_to_batch(&[], &HashMap::new(), &[], Some(contract))?);
    let batches = rows
        .iter()
        .map(|batch| compact_to_storage(batch, &schema))
        .collect::<Result<Vec<_>>>()?;
    Ok((schema, batches))
}

/// Test-only: remove the contract and content columns, preserving the actual v12 packed layout.
#[cfg(any(test, feature = "test-util"))]
pub async fn restamp_packed_v12_for_test(dataset: &mut Dataset) -> Result<()> {
    let live_rows = read_publish_scan(dataset).await?.live_rows;
    let mut metadata = dataset.schema().metadata.clone();
    metadata.insert(INTERNAL_SCHEMA_VERSION_KEY.to_string(), "12".to_string());
    let schema = packed_v12_schema(metadata.clone())?;
    let current_schema = manifest_storage_schema(metadata.clone())?;
    let batches = live_rows
        .iter()
        .map(|batch| {
            let flat = flat_batch_for_test(batch, metadata.clone())?;
            let packed = compact_to_storage(&flat, &current_schema)?;
            RecordBatch::try_new(
                schema.clone(),
                packed.columns().iter().take(3).cloned().collect(),
            )
            .map_err(OmniError::arrow_internal)
        })
        .collect::<Result<Vec<_>>>()?;
    *dataset = commit_overwrite(dataset.clone(), batches).await?;
    Ok(())
}

/// Test and DST fixture: rewrite the live rows flat and stamp `version` in one commit,
/// the `__manifest` a pre-stamp-12 binary left behind; a restamp alone leaves
/// packed rows no flat reader can project.
#[cfg(any(test, feature = "test-util", feature = "dst"))]
pub async fn restamp_flat_for_test(dataset: &mut Dataset, version: u32) -> Result<()> {
    let live_rows = read_publish_scan(dataset).await?.live_rows;
    let mut metadata = dataset.schema().metadata.clone();
    metadata.insert(INTERNAL_SCHEMA_VERSION_KEY.to_string(), version.to_string());
    let batches = live_rows
        .iter()
        .map(|batch| flat_batch_for_test(batch, metadata.clone()))
        .collect::<Result<Vec<_>>>()?;
    *dataset = commit_overwrite(dataset.clone(), batches).await?;
    Ok(())
}

/// Test-only: overwrite the internal-schema stamp with a raw (possibly
/// non-numeric) value. Used to pin `guard_stamp`'s unreadable-stamp arm.
#[cfg(test)]
pub async fn set_raw_stamp_for_test(dataset: &mut Dataset, value: &str) -> Result<()> {
    dataset
        .update_schema_metadata([(INTERNAL_SCHEMA_VERSION_KEY.to_string(), value.to_string())])
        .await
        .map_err(OmniError::storage)?;
    Ok(())
}

/// Test-only: strip the internal-schema stamp entirely, synthesizing the torn
/// state a pre-atomic-stamp binary left when init died between the `__manifest`
/// Create commit and the stamp commit. Used to pin `guard_stamp`'s absent arm.
#[cfg(test)]
pub async fn remove_stamp_for_test(dataset: &mut Dataset) -> Result<()> {
    let remaining: Vec<(String, String)> = dataset
        .schema()
        .metadata
        .iter()
        .filter(|(k, _)| k.as_str() != INTERNAL_SCHEMA_VERSION_KEY)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    dataset
        .update_schema_metadata(remaining)
        .replace()
        .await
        .map_err(OmniError::storage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guard accepts exactly the served range, one stamp since v10 that
    /// both system column vintages are born with, and refuses anything below
    /// the floor or above the ceiling.
    #[test]
    fn unsupported_guard_accepts_exactly_the_supported_range() {
        assert_eq!(stamp_for_system_columns(SYSTEM_COLUMNS_LEGACY).unwrap(), 13);
        assert_eq!(stamp_for_system_columns(SYSTEM_COLUMNS_V3).unwrap(), 13);
        assert!(stamp_for_system_columns(omnigraph_compiler::SYSTEM_COLUMNS_META).is_err());
        assert_eq!(
            (
                MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
                INTERNAL_MANIFEST_SCHEMA_VERSION
            ),
            (13, 13)
        );
        assert_eq!((LAST_FLAT_STAMP, PACKED_RECORD_STAMP), (11, 12));
        for stamp in MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION..=INTERNAL_MANIFEST_SCHEMA_VERSION {
            assert!(
                refuse_if_stamp_unsupported(stamp).is_ok(),
                "stamp v{stamp} is within [MIN, CURRENT] and must be accepted"
            );
        }
        let below = refuse_if_stamp_unsupported(MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION - 1)
            .expect_err("a sub-floor stamp must be refused")
            .to_string();
        assert!(below.contains("reads only v13 to v13"), "got: {below}");
        assert!(
            below.contains("0.11.x (packed catalog record)"),
            "got: {below}"
        );
        assert!(below.contains("omnigraph export"), "got: {below}");
        let legacy = refuse_if_stamp_unsupported(7)
            .expect_err("a v7 stamp must be refused")
            .to_string();
        assert!(
            legacy.contains("an unreleased v7 development build"),
            "got: {legacy}"
        );
        assert!(legacy.contains("omnigraph upgrade"), "got: {legacy}");
        for stamp in 6..=12 {
            let refusal = refuse_if_stamp_unsupported(stamp)
                .expect_err("conversion requires explicit offline upgrade")
                .to_string();
            assert!(refusal.contains("omnigraph upgrade"), "v{stamp}: {refusal}");
        }
        let future_stamp = INTERNAL_MANIFEST_SCHEMA_VERSION + 1;
        let future = refuse_if_stamp_unsupported(future_stamp)
            .expect_err("the first unsupported future stamp must be refused")
            .to_string();
        assert!(future.contains("internal schema v14"), "got: {future}");
        assert!(future.contains("reads only v13 to v13"), "got: {future}");
        assert!(future.contains("upgrade omnigraph"), "got: {future}");
    }

    /// The refusal names the release line that wrote each stamp so an operator
    /// knows which binary to use for the export step; unknown stamps fall back
    /// without panicking.
    #[test]
    fn release_names_the_writing_line_for_each_stamp() {
        assert_eq!(release_for_internal_schema_version(3), "0.6.2 to 0.7.2");
        assert_eq!(release_for_internal_schema_version(4), "0.8.x");
        assert!(release_for_internal_schema_version(5).contains("unreleased final-v5"));
        assert!(release_for_internal_schema_version(5).contains("46b6d908"));
        assert_eq!(release_for_internal_schema_version(6), "0.9.x or 0.10.x");
        assert_eq!(
            release_for_internal_schema_version(7),
            "an unreleased v7 development build"
        );
        assert_eq!(
            release_for_internal_schema_version(99),
            "an unrecognized older release"
        );
        // The sub-CURRENT refusal embeds the named release.
        let err = refuse_if_stamp_unsupported(3).unwrap_err().to_string();
        assert!(err.contains("0.6.2 to 0.7.2"), "got: {err}");
        assert!(err.contains("omnigraph export"), "got: {err}");
    }
}
