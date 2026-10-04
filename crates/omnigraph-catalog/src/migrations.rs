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
//! ## Served stamps
//!
//! Normal open accepts `MIN_SUPPORTED..=CURRENT`, which since v10 is one
//! stamp: every served graph may carry table pins that name a detached Lance
//! version and no linear one (RFC "Detached-only tables"), which an older
//! binary would misread as reclaimed history. Both RFC 0040 system column
//! vintages (`id`/`src`/`dst` and `__id`/`__src`/`__dst`) live under that
//! stamp; the vintage is read from the schema IR, never from the stamp.
//! Normal open never converts: a v8, v9 or v13 standalone root is converted
//! by the offline `omnigraph upgrade`, whose intent, receipts and fence live
//! here; a graph at any other stamp below v14 is rebuilt by export and load,
//! and normal open refuses a graph that carries a pending storage-upgrade
//! intent.
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
use lance::dataset::transaction::{Operation, UpdateMap};
use omnigraph_compiler::{SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_V3, SystemColumns};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{OmniError, Result};
use crate::history::LegacyLayout;
use crate::legacy::LegacyPlan;
use crate::record::RECORD_COLUMN;
use crate::state::{SchemaContractHead, SchemaContractRow};

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
/// - v14 — a branch's `__manifest` holds the current state, the head commit
///   and a byte-bounded buffer of its latest ancestors; older commits are
///   immutable Lance files under `__history` (`history.rs`), addressed by
///   the commit id, and each commit names its archived schema content under
///   `__history/schemas/`. A v13 binary would read the buffer as the whole
///   history, so the stamp refuses it before any open. Two unreleased
///   prototype layouts were stamped 14 and 15 in development trees only;
///   neither shipped and neither has a conversion route.
///
/// v1–v13 graphs are not served by this binary (see `MIN_SUPPORTED`); the
/// history is kept for provenance and to document what each stamp value meant.
/// A v8, v9 or v13 standalone root reaches v14 through the offline
/// `omnigraph upgrade`.
pub const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = 14;

/// Normal open serves only format 14. A v8, v9 or v13 standalone root is
/// converted by the offline `omnigraph upgrade`; a graph at any other lower
/// stamp, and a cluster-managed graph, is rebuilt by export and load.
pub const MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION: u32 = 14;

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
/// system column vintages. No release tag stamped v10 to v13: main
/// development builds wrote them, each from the day its stamp landed to the
/// day the next one did, and every such build reports itself as 0.11.0, so
/// the refusal names the build dates. The fallback keeps this map total.
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
        10 => "a main development build from 2026-09-21 to 2026-09-25 (detached table commits)",
        11 => "a main development build from 2026-09-25 to 2026-09-28 (detached-only tables)",
        12 => "a main development build from 2026-09-28 to 2026-10-01 (packed catalog record)",
        13 => {
            "a main development build from 2026-10-01 to 2026-10-04 (schema contract in manifest)"
        }
        _ => "an unrecognized older release",
    }
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
                refuse_if_stamp_unsupported(stamp)?;
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
pub(crate) fn guard_row_layout(dataset: &Dataset) -> Result<()> {
    refuse_if_stamp_unsupported(guard_stamp(dataset)?)
}

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
/// - `is_upgrade_source(stamp)` (v8, v9, v13): a standalone root is converted
///   by the offline `omnigraph upgrade`; a cluster-managed graph is rebuilt.
/// - any other `stamp < MIN_SUPPORTED`: the graph was made by an older
///   omnigraph whose storage format this binary does not read — rebuild via
///   export/import.
/// - `MIN_SUPPORTED..=CURRENT` is served as-is and never migrated on open.
pub fn refuse_if_stamp_unsupported(stamp: u32) -> Result<()> {
    if stamp > INTERNAL_MANIFEST_SCHEMA_VERSION {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{} but this binary reads only v{} to v{} \
             — upgrade omnigraph before opening this graph",
            stamp, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION, INTERNAL_MANIFEST_SCHEMA_VERSION,
        )));
    }
    if is_upgrade_source(stamp) {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{stamp}, but this omnigraph reads only v{min} to v{current}. \
             This graph was created by omnigraph {release}. For a standalone root, stop all readers, writers and \
             maintenance, retain a verified backup of the whole root, and run `omnigraph upgrade <graph> --check` \
             and then `omnigraph upgrade <graph>`; the upgrade keeps the graph's branches and commit history. \
             A cluster-managed graph has no in-place route yet: with {release} run \
             `omnigraph export <graph> > graph.jsonl`, then with this binary run \
             `omnigraph init --schema <schema.pg> <new-graph>` and \
             `omnigraph load --mode overwrite --data graph.jsonl <new-graph>`. \
             (Data, vectors, and blobs are preserved by the rebuild; commit history and branches are not.) \
             See docs/user/operations/upgrade.md.",
            min = MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
            current = INTERNAL_MANIFEST_SCHEMA_VERSION,
            release = release_for_internal_schema_version(stamp),
        )));
    }
    if stamp < MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{stamp}, but this omnigraph reads only v{min} to v{current}. \
             This graph was created by omnigraph {release}. Rebuild it: with an omnigraph {release} binary run \
             `omnigraph export <graph> > graph.jsonl`, relocate each record's `data.id` to the top-level `id`, \
             then with this binary run \
             `omnigraph init --schema <schema.pg> <new-graph>` and \
             `omnigraph load --mode overwrite --data graph.jsonl <new-graph>`. \
             (Data, vectors, and blobs are preserved; commit history and branches are not.) \
             See docs/user/operations/upgrade.md.",
            min = MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
            current = INTERNAL_MANIFEST_SCHEMA_VERSION,
            release = release_for_internal_schema_version(stamp),
        )));
    }
    Ok(())
}

/// Schema-metadata key the offline storage upgrade sets on main's `__manifest`
/// from its fence to its activation; the value is the [`UpgradeIntent`] JSON.
/// Normal open refuses a graph that carries it.
pub const UPGRADE_PENDING_KEY: &str = "omnigraph:storage_upgrade_pending";

/// Schema-metadata key holding the [`BranchReceipt`] JSON a conversion leaves
/// on every ref it converted; it stays after activation.
#[doc(hidden)]
pub const UPGRADE_RECEIPT_KEY: &str = "omnigraph:storage_upgrade_receipt";

/// The most live refs, main included, one upgrade converts.
#[doc(hidden)]
pub const MAX_BRANCHES: usize = 1024;

/// The byte budget of the intent JSON and of a receipt JSON.
#[doc(hidden)]
pub const MAX_INTENT_BYTES: usize = 1024 * 1024;

/// The one protocol this binary runs, as the intent carries it; every source
/// format converts under it.
#[doc(hidden)]
pub const UPGRADE_PROTOCOL: u32 = 6;

/// A storage format the upgrade converts from: the two stamps of release
/// 0.11.x, whose schema contract is the three schema objects at the graph
/// root, and 13, whose contract is the `schema_contract` row of `__manifest`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeSource {
    Stamp8,
    Stamp9,
    Stamp13,
}

impl UpgradeSource {
    /// Every source, oldest first.
    pub const ALL: [Self; 3] = [Self::Stamp8, Self::Stamp9, Self::Stamp13];

    /// The stamp the live heads of this source carry.
    pub const fn stamp(self) -> u32 {
        match self {
            Self::Stamp8 => 8,
            Self::Stamp9 => 9,
            Self::Stamp13 => 13,
        }
    }

    /// The source whose live heads are stamped `stamp`, if the upgrade converts one.
    pub fn from_stamp(stamp: u32) -> Option<Self> {
        Self::ALL.into_iter().find(|source| source.stamp() == stamp)
    }

    /// Whether the contract is a row of `__manifest` (13) or the root objects (8, 9).
    pub const fn contract_in_row(self) -> bool {
        matches!(self, Self::Stamp13)
    }
}

/// The stamps the upgrade converts from, oldest first.
#[doc(hidden)]
pub const UPGRADE_SOURCE_FORMATS: [u32; 3] = [
    UpgradeSource::Stamp8.stamp(),
    UpgradeSource::Stamp9.stamp(),
    UpgradeSource::Stamp13.stamp(),
];

/// Whether a standalone root stamped `stamp` is converted by the upgrade.
#[doc(hidden)]
pub fn is_upgrade_source(stamp: u32) -> bool {
    UpgradeSource::from_stamp(stamp).is_some()
}

/// A live ref pinned at the version the upgrade converts; `parent_version`
/// is 0 for main and the fork version for a named ref. A receipt an earlier
/// route left carries no `parent_version` and reads as 0.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceBranch {
    pub native: Option<String>,
    pub identity: BranchIdentifier,
    pub version: u64,
    #[serde(default)]
    pub parent_version: u64,
}

/// A live ref of the source the upgrade retires once main is fenced, as the
/// branch delete that created it would have: the `__schema_apply_lock__` ref
/// a 0.11.x schema apply did not release.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceLock {
    pub native: String,
    pub identity: BranchIdentifier,
}

/// What the fence binds: the live refs at their source versions, main last,
/// the refs retired after the fence, the schema contract and the plan of the
/// legacy objects.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpgradeIntent {
    pub protocol: u32,
    pub attempt: String,
    pub source_format: u32,
    pub target_format: u32,
    pub graph_identity: String,
    pub branches: Vec<SourceBranch>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retire: Vec<SourceLock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_contract: Option<UpgradeSchemaContract>,
    pub legacy: LegacyPlan,
}

/// The identity and the exact texts of main's schema contract at the fence,
/// and the name of its archive under `__history/schemas/`, written before the
/// fence: a fenced rerun reads the contract from there.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpgradeSchemaContract {
    pub identity: SchemaContractHead,
    pub source_sha256: String,
    pub ir_sha256: String,
    pub content_sha256: String,
}

impl UpgradeSchemaContract {
    pub fn from_row(row: &SchemaContractRow) -> Result<Self> {
        Ok(Self {
            identity: row.head.clone(),
            source_sha256: format!("{:x}", Sha256::digest(row.source.as_bytes())),
            ir_sha256: format!("{:x}", Sha256::digest(row.ir.as_bytes())),
            content_sha256: crate::history::schema_content_hash(row)?,
        })
    }

    pub fn validate_row(&self, row: &SchemaContractRow) -> Result<()> {
        if *self != Self::from_row(row)? {
            return Err(invalid(
                "upgrade schema contract identity or exact text changed",
            ));
        }
        Ok(())
    }
}

/// What a converted ref carries under [`UPGRADE_RECEIPT_KEY`]: a pure
/// function of the intent and the ref's source pin.
#[doc(hidden)]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchReceipt {
    protocol: u32,
    attempt: String,
    source: SourceBranch,
}

fn invalid(message: impl Into<String>) -> OmniError {
    OmniError::manifest(message)
}

fn valid_sha256(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl UpgradeIntent {
    /// Refuse an intent this binary cannot own: another route, a legacy
    /// layout version it does not know, or pins that do not name one main
    /// last among distinct live refs.
    pub fn validate(&self) -> Result<()> {
        let known = LegacyLayout::CURRENT.version;
        if self.legacy.layout.version != known {
            return Err(invalid(format!(
                "storage upgrade intent names legacy layout version {}, which this build does \
                 not know (it writes layout version {known}); finish the upgrade with the \
                 executable that fenced the graph",
                self.legacy.layout.version
            )));
        }
        let route = self.protocol == UPGRADE_PROTOCOL
            && is_upgrade_source(self.source_format)
            && self.target_format == INTERNAL_MANIFEST_SCHEMA_VERSION;
        let contract_valid = self.schema_contract.as_ref().is_some_and(|contract| {
            contract.identity.schema_identity_domain == self.graph_identity
                && contract.identity.schema_identity_version != 0
                && contract
                    .identity
                    .schema_ir_hash
                    .strip_prefix("sha256:")
                    .is_some_and(valid_sha256)
                && valid_sha256(&contract.source_sha256)
                && valid_sha256(&contract.ir_sha256)
                && valid_sha256(&contract.content_sha256)
        });
        let mut names = HashSet::new();
        let branches_valid = !self.branches.is_empty()
            && self.branches.len() <= MAX_BRANCHES
            && self
                .branches
                .last()
                .is_some_and(|branch| branch.native.is_none())
            && self.branches.iter().all(|branch| {
                branch.version != 0
                    && branch.native.is_none() == (branch.parent_version == 0)
                    && branch.parent_version <= branch.version
                    && names.insert(branch.native.as_deref())
            });
        let retire_valid = self
            .retire
            .iter()
            .all(|lock| !lock.native.is_empty() && names.insert(Some(lock.native.as_str())));
        if !route
            || !contract_valid
            || !branches_valid
            || !retire_valid
            || !valid_sha256(&self.legacy.directory_sha256)
            || self.attempt.parse::<ulid::Ulid>().is_err()
            || self.graph_identity.is_empty()
        {
            return Err(invalid("unsupported or ambiguous storage upgrade intent"));
        }
        Ok(())
    }
}

/// The intent under [`UPGRADE_PENDING_KEY`], `None` when `dataset` carries no
/// key. An intent this binary cannot own is an error, never `None`.
#[doc(hidden)]
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
    intent.validate()?;
    Ok(Some(intent))
}

/// The refusal for a graph that carries [`UPGRADE_PENDING_KEY`]: how to resume
/// an intent this binary owns, or how to preserve one it cannot read.
pub fn recovery_guidance(dataset: &Dataset) -> String {
    let unknown = match intent_from(dataset) {
        Ok(Some(intent)) => {
            return format!(
                "storage upgrade recovery required: this graph carries the pending storage \
                 conversion of attempt {} to format v{}. Keep the graph offline: stop all \
                 readers, writers and maintenance, retain the backup taken before the attempt, \
                 and rerun `omnigraph upgrade <graph>` without `--check` with this executable; \
                 the upgrade resumes the existing attempt.",
                intent.attempt, intent.target_format
            );
        }
        Ok(None) => "the marker is absent".to_string(),
        Err(error) => error.to_string(),
    };
    format!(
        "storage upgrade recovery required: this graph carries a pending storage conversion \
         whose ownership this executable cannot establish ({unknown}). Keep the graph offline: \
         stop all readers, writers and maintenance, preserve the graph root and its backup, \
         run `omnigraph upgrade <graph> --check` for read-only diagnostics, and finish the \
         conversion with the omnigraph executable that started it. Never delete the marker."
    )
}

#[doc(hidden)]
pub fn receipt(branch: &SourceBranch, intent: &UpgradeIntent) -> BranchReceipt {
    BranchReceipt {
        protocol: intent.protocol,
        attempt: intent.attempt.clone(),
        source: branch.clone(),
    }
}

/// Whether `dataset`, the head of the ref `source` pins, already carries this
/// intent's conversion: its receipt, the target stamp, and exactly the
/// versions the protocol adds (one on a named ref; the fence and the
/// conversion on main, plus the activation once the key is gone). A receipt
/// an earlier route left on an unconverted head reads as not completed; any
/// other receipt, stamp or version is refused.
#[doc(hidden)]
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
    let fenced = dataset.schema().metadata.contains_key(UPGRADE_PENDING_KEY);
    let main = source.native.is_none();
    if found != receipt(source, intent) {
        let source_head = source
            .version
            .checked_add(u64::from(main && fenced))
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
        .checked_add(if main { 2 } else { 1 })
        .and_then(|converted| converted.checked_add(u64::from(main && !fenced)))
        .ok_or_else(|| invalid("upgrade version overflow"))?;
    if dataset.version().version != expected {
        return Err(invalid(
            "upgraded branch moved while conversion was incomplete",
        ));
    }
    Ok(true)
}

/// The fence: one metadata-only transaction that stamps main `target` and
/// sets the intent, keeping every other key and every row.
#[doc(hidden)]
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
/// It changes the metadata only: the rows keep the current layout under the
/// new stamp, which is enough for the guard, which reads the stamp alone.
#[cfg(any(test, feature = "test-util"))]
pub async fn set_stamp_for_test(dataset: &mut Dataset, version: u32) -> Result<()> {
    set_stamp(dataset, version).await
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
        assert_eq!(stamp_for_system_columns(SYSTEM_COLUMNS_LEGACY).unwrap(), 14);
        assert_eq!(stamp_for_system_columns(SYSTEM_COLUMNS_V3).unwrap(), 14);
        assert!(stamp_for_system_columns(omnigraph_compiler::SYSTEM_COLUMNS_META).is_err());
        assert_eq!(
            (
                MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION,
                INTERNAL_MANIFEST_SCHEMA_VERSION
            ),
            (14, 14)
        );
        for stamp in MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION..=INTERNAL_MANIFEST_SCHEMA_VERSION {
            assert!(
                refuse_if_stamp_unsupported(stamp).is_ok(),
                "stamp v{stamp} is within [MIN, CURRENT] and must be accepted"
            );
        }
        let below = refuse_if_stamp_unsupported(MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION - 1)
            .expect_err("a sub-floor stamp must be refused")
            .to_string();
        assert!(below.contains("reads only v14 to v14"), "got: {below}");
        assert!(
            below.contains(release_for_internal_schema_version(
                MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION - 1
            )),
            "got: {below}"
        );
        assert!(
            below.contains(
                "For a standalone root, stop all readers, writers and maintenance, retain a \
                 verified backup of the whole root, and run `omnigraph upgrade <graph> --check` \
                 and then `omnigraph upgrade <graph>`"
            ),
            "got: {below}"
        );
        assert!(
            below.contains(
                "A cluster-managed graph has no in-place route yet: with a main development \
                 build from 2026-10-01 to 2026-10-04 (schema contract in manifest) run \
                 `omnigraph export <graph> > graph.jsonl`"
            ),
            "got: {below}"
        );
        let legacy = refuse_if_stamp_unsupported(7)
            .expect_err("a v7 stamp must be refused")
            .to_string();
        assert!(
            legacy.contains("an unreleased v7 development build"),
            "got: {legacy}"
        );
        assert_eq!(UPGRADE_PROTOCOL, 6);
        assert_eq!(
            UPGRADE_SOURCE_FORMATS,
            [8, 9, MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION - 1]
        );
        for stamp in 1..INTERNAL_MANIFEST_SCHEMA_VERSION {
            let refusal = refuse_if_stamp_unsupported(stamp)
                .expect_err("a sub-floor stamp is refused")
                .to_string();
            assert!(refusal.contains("omnigraph export"), "v{stamp}: {refusal}");
            assert!(
                refusal.contains(release_for_internal_schema_version(stamp)),
                "v{stamp}: {refusal}"
            );
            if [8, 9, 13].contains(&stamp) {
                assert!(is_upgrade_source(stamp), "v{stamp}");
                assert!(
                    refusal.contains(
                        "run `omnigraph upgrade <graph> --check` and then `omnigraph upgrade \
                         <graph>`"
                    ),
                    "v{stamp}: {refusal}"
                );
            } else {
                assert!(!is_upgrade_source(stamp), "v{stamp}");
                assert!(
                    !refusal.contains("omnigraph upgrade") && !refusal.contains("in-place"),
                    "v{stamp}: {refusal}"
                );
            }
        }
        assert!(!is_upgrade_source(0) && !is_upgrade_source(INTERNAL_MANIFEST_SCHEMA_VERSION));
        let released = refuse_if_stamp_unsupported(9)
            .expect_err("a v9 stamp must be refused")
            .to_string();
        assert!(
            released.contains(
                "This graph was created by omnigraph 0.11.x. For a standalone root, stop all \
                 readers, writers and maintenance"
            ) && released.contains(
                "A cluster-managed graph has no in-place route yet: with 0.11.x run `omnigraph \
                 export <graph> > graph.jsonl`"
            ),
            "got: {released}"
        );
        let future_stamp = INTERNAL_MANIFEST_SCHEMA_VERSION + 1;
        let future = refuse_if_stamp_unsupported(future_stamp)
            .expect_err("the first unsupported future stamp must be refused")
            .to_string();
        assert!(
            future.contains(&format!("internal schema v{future_stamp}")),
            "got: {future}"
        );
        assert!(future.contains("reads only v14 to v14"), "got: {future}");
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
            [10, 11, 12, 13].map(release_for_internal_schema_version),
            [
                "a main development build from 2026-09-21 to 2026-09-25 (detached table commits)",
                "a main development build from 2026-09-25 to 2026-09-28 (detached-only tables)",
                "a main development build from 2026-09-28 to 2026-10-01 (packed catalog record)",
                "a main development build from 2026-10-01 to 2026-10-04 (schema contract in manifest)",
            ]
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
