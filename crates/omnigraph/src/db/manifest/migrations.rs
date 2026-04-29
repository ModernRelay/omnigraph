//! Internal schema migrations for the `__manifest` Lance dataset.
//!
//! ## Why this exists
//!
//! The on-disk shape of `__manifest` evolves alongside the engine. We do not
//! want healing hooks scattered through every read/write path that ask
//! "is this an old shape? am I supposed to upgrade it?" — that pattern
//! accrues liability with every change. Instead this module is the *single*
//! place where on-disk shape is reconciled with what the binary expects:
//!
//! - One constant `INTERNAL_MANIFEST_SCHEMA_VERSION` declares the shape this
//!   binary writes.
//! - One stamp `omnigraph:internal_schema_version` in the manifest dataset's
//!   schema-level metadata records the on-disk shape.
//! - One dispatcher `migrate_internal_schema` walks the on-disk stamp forward
//!   to the expected version via `match`-arm steps. Each future change adds
//!   one arm + one test, never a new branch in unrelated code paths.
//!
//! After the dispatcher runs, the rest of the engine assumes current shape.
//! No code outside this module should ever inspect the stamp.
//!
//! ## When it runs
//!
//! Only on open-for-write paths (the publisher's `load_publish_state`).
//! Reads are side-effect-free by contract; an old-shape `__manifest` reads
//! fine, it just lacks the protections introduced by later versions.
//! `init_manifest_repo` stamps the current version at creation, so newly
//! initialized repos never need migration.
//!
//! ## Forward-version protection
//!
//! A stamp *higher* than this binary's known version triggers a clear
//! "upgrade omnigraph first" error. An old binary cannot clobber a newer
//! schema by silently treating "unknown stamp" as "missing stamp".

use lance::Dataset;

use crate::error::{OmniError, Result};

/// Current internal schema version this binary expects to find on disk.
///
/// History:
/// - v1 — implicit (pre-stamp). `__manifest.object_id` carried no
///   `lance-schema:unenforced-primary-key` annotation; the publisher had
///   no row-level CAS protection (see `.context/merge-insert-cas-granularity.md`).
/// - v2 — `__manifest.object_id` carries the unenforced-PK annotation,
///   engaging Lance's bloom-filter conflict resolver at commit time. Added
///   alongside `expected_table_versions` OCC on `ManifestBatchPublisher::publish`.
pub(super) const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = 2;

const INTERNAL_SCHEMA_VERSION_KEY: &str = "omnigraph:internal_schema_version";
const OBJECT_ID_PK_KEY: &str = "lance-schema:unenforced-primary-key";

/// Read the on-disk stamp from `__manifest`'s schema-level metadata.
/// Absent ⇒ v1 (pre-stamp world).
pub(super) fn read_stamp(dataset: &Dataset) -> u32 {
    dataset
        .schema()
        .metadata
        .get(INTERNAL_SCHEMA_VERSION_KEY)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

/// Stamp a freshly-initialized manifest with the current internal schema
/// version. Idempotent — safe to call on an already-stamped dataset.
pub(super) async fn stamp_current_version(dataset: &mut Dataset) -> Result<()> {
    set_stamp(dataset, INTERNAL_MANIFEST_SCHEMA_VERSION).await
}

/// Apply any pending internal-schema migrations to the manifest dataset.
///
/// Idempotent: when the on-disk stamp matches the binary, this is a single
/// metadata read with no writes.
pub(super) async fn migrate_internal_schema(dataset: &mut Dataset) -> Result<()> {
    let mut current = read_stamp(dataset);

    if current > INTERNAL_MANIFEST_SCHEMA_VERSION {
        return Err(OmniError::manifest(format!(
            "__manifest is stamped at internal schema v{} but this binary expects v{} \
             — upgrade omnigraph before opening this repo for writes",
            current, INTERNAL_MANIFEST_SCHEMA_VERSION,
        )));
    }

    while current < INTERNAL_MANIFEST_SCHEMA_VERSION {
        match current {
            1 => {
                migrate_v1_to_v2(dataset).await?;
                current = 2;
            }
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "no internal-schema migration registered for v{} → v{}",
                    other,
                    other + 1,
                )));
            }
        }
    }
    Ok(())
}

/// v1 → v2: annotate `__manifest.object_id` as Lance's unenforced primary key
/// so the merge-insert conflict resolver enforces row-level CAS at commit
/// time, then bump the stamp.
///
/// Both steps are idempotent under retry: re-applying the field annotation
/// at its current value is a no-op-ish bump in Lance, and the stamp is a
/// simple key-value write. A crash between the two leaves the field set
/// without a stamp; the next open re-runs this fn and only the stamp lands.
async fn migrate_v1_to_v2(dataset: &mut Dataset) -> Result<()> {
    dataset
        .update_field_metadata()
        .update("object_id", [(OBJECT_ID_PK_KEY.to_string(), "true".to_string())])
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    set_stamp(dataset, 2).await
}

async fn set_stamp(dataset: &mut Dataset, version: u32) -> Result<()> {
    dataset
        .update_schema_metadata([(
            INTERNAL_SCHEMA_VERSION_KEY.to_string(),
            version.to_string(),
        )])
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(())
}
