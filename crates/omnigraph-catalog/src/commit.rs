//! The copy-on-write publish of `__manifest`.
//!
//! A publish writes the branch's hot row set into a new file and commits it
//! with zero retries. Lance then writes
//! `_versions/{read_version + 1}.manifest` only if it is absent and never
//! rebases over a concurrent commit, so the version number is the put-if-absent
//! CAS. Earlier versions stay readable, which keeps point-in-time reads and
//! lost-acknowledgement read-back unchanged.
//!
//! The stored row shape lives in `record`.
//!
//! Fragments. A version's hot fragment holds its head commit's record, the
//! buffer, the `table` rows, the schema contract and up to
//! [`REGISTRY_HOT_ROWS`] block registry rows; it is the fragment with the
//! highest id and is rewritten by every publish. Its cold fragments hold the
//! older registry rows, each written once by the publish that pushed them out
//! of the hot fragment and named again by every later version: the commit is
//! a Lance `Overwrite` whose fragment list carries the previous version's cold
//! fragments by reference, so their files are neither read nor written by an
//! ordinary publish. The overwrite gives every listed fragment a fresh id, the
//! hot one last, so the hot fragment is found by its id and a cold file is
//! known by its path. Cold fragments are repacked by the git commit-graph
//! chain rule when a push joins them ([`fragments_to_repack`]), which keeps
//! their count logarithmic in the registry. The overwrite with zero retries is
//! Lance's strict overwrite, the one commit it never rebases over a concurrent
//! version (`lance/src/io/commit.rs`, `strict_overwrite`); an `Update` would
//! be rebased over a compatible foreign commit and land past the CAS.
//!
//! Costs. Without a cold push, the bytes written and decoded per publish do
//! not depend on released history; a push also reads and writes the cold
//! fragments it repacks. Each version keeps its own hot copy and nothing
//! prunes `__manifest` versions today, so retained bytes grow with the
//! publication count until version retention exists.
//!
//! Lance's auto-cleanup hook is skipped: `__manifest` versions are the snapshot
//! and time-travel authority.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use lance::Dataset;
use lance::dataset::transaction::{Operation, TransactionBuilder};
use lance::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use lance_table::format::Fragment;

use crate::error::{OmniError, Result};
use crate::migrations::{
    INTERNAL_MANIFEST_SCHEMA_VERSION, guard_row_layout, read_stamp, stamp_entry,
};
use crate::publisher::map_lance_publish_error;
use crate::record::{compact_to_storage, manifest_storage_schema};
use crate::state::{self, LineageRegistry, ManifestRows};

/// Registry rows the hot fragment holds before a publish moves every one of
/// them into a cold fragment of their own. A row is about 100 bytes, so the
/// bound costs an ordinary publish at most a few kilobytes; a cold fragment
/// is read by a merge, never by an open.
pub(crate) const REGISTRY_HOT_ROWS: usize = 64;

/// Where a publish of the whole row set lands.
pub(crate) enum Placement {
    /// `dataset`'s version + 1 on its linear history: the version number is
    /// the CAS, so Lance retries nothing.
    Linear,
    /// A detached version Lance names after the rows are encoded: no CAS, the
    /// only retry is the one around the random version id; the properties ride
    /// the transaction (an unpublished graph commit's record).
    Detached { properties: HashMap<String, String> },
}

/// How many random detached ids Lance may try before it gives up; zero
/// retries never commit a detached version at all.
const DETACHED_VERSION_RETRIES: u32 = 20;

/// Write `rows` as the hot row set of `dataset`'s version + 1, and `cold` as
/// a cold fragment of that version. The schema metadata carries over from
/// `dataset`, a served stamp, and the new version is stamped with the format
/// this binary writes: a v14 branch becomes v15 in the CAS that writes its
/// first registry rows. Returns the hot batch beside the new version.
pub(crate) async fn overwrite(
    dataset: Dataset,
    rows: &ManifestRows,
    cold: Option<&LineageRegistry>,
) -> Result<(Dataset, RecordBatch)> {
    overwrite_placed(dataset, rows, cold, Placement::Linear).await
}

/// Write `rows` as the hot row set of a detached version of `dataset`'s
/// branch, with `properties` on its transaction; the branch's cold registry
/// fragments are named again, unchanged. The row clocks are the caller's;
/// the version Lance allocates is read from the returned dataset.
pub(crate) async fn overwrite_detached(
    dataset: Dataset,
    rows: &ManifestRows,
    properties: HashMap<String, String>,
) -> Result<(Dataset, RecordBatch)> {
    overwrite_placed(dataset, rows, None, Placement::Detached { properties }).await
}

async fn overwrite_placed(
    dataset: Dataset,
    rows: &ManifestRows,
    cold: Option<&LineageRegistry>,
    placement: Placement,
) -> Result<(Dataset, RecordBatch)> {
    guard_row_layout(&dataset)?;
    let mut metadata = dataset.schema().metadata.clone();
    let (stamp_key, stamp_value) = stamp_entry(INTERNAL_MANIFEST_SCHEMA_VERSION);
    metadata.insert(stamp_key, stamp_value);
    let schema = manifest_storage_schema(metadata);
    let batch = compact_to_storage(&rows.to_batch()?, &schema)?;
    let dataset = Arc::new(dataset);
    let mut fragments = match read_stamp(&dataset) == Some(INTERNAL_MANIFEST_SCHEMA_VERSION) {
        true => state::cold_fragments(&dataset),
        false => Vec::new(),
    };
    if let Some(cold) = cold {
        let repacked = fragments_to_repack(&fragments, cold.len());
        let mut cold = cold.clone();
        cold.merge(&state::read_cold_registry(&dataset, &repacked).await?);
        fragments.retain(|kept| !repacked.iter().any(|joined| joined.id == kept.id));
        let (written, _) = stage(
            &dataset,
            compact_to_storage(&cold.to_batch()?, &schema)?,
            None,
        )
        .await?;
        fragments.extend(written);
    }
    let hot_rows = batch.num_rows();
    if u32::try_from(hot_rows).is_err() {
        return Err(OmniError::manifest_internal(
            "manifest hot rows exceed the Lance fragment limit",
        ));
    }
    let (hot, schema) = stage(&dataset, batch.clone(), Some(hot_rows.max(1))).await?;
    if hot.len() != 1 {
        return Err(OmniError::manifest_internal(
            "manifest hot rows must occupy exactly one fragment",
        ));
    }
    fragments.extend(hot);
    let operation = Operation::Overwrite {
        fragments,
        schema,
        config_upsert_values: None,
        initial_bases: None,
    };
    let mut transaction = TransactionBuilder::new(dataset.version().version, operation).build();
    let builder = match placement {
        Placement::Linear => CommitBuilder::new(dataset).with_max_retries(0),
        Placement::Detached { properties } => {
            transaction.transaction_properties = Some(Arc::new(properties));
            CommitBuilder::new(dataset)
                .with_detached(true)
                .with_max_retries(DETACHED_VERSION_RETRIES)
        }
    };
    let committed = builder
        .with_skip_auto_cleanup(true)
        .execute(transaction)
        .await
        .map_err(map_lance_publish_error)?;
    Ok((committed, batch))
}

/// Write `batch` as a data file of `dataset` without committing it: the
/// fragment, and the stored schema it was written under, the same for every
/// file of `__manifest` so one version's fragments agree on field ids.
async fn stage(
    dataset: &Arc<Dataset>,
    batch: RecordBatch,
    max_rows_per_file: Option<usize>,
) -> Result<(Vec<Fragment>, lance_core::datatypes::Schema)> {
    let mut params = WriteParams {
        mode: WriteMode::Overwrite,
        auto_cleanup: None,
        skip_auto_cleanup: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    if let Some(max_rows) = max_rows_per_file {
        params.max_rows_per_file = max_rows;
    }
    let transaction = InsertBuilder::new(dataset.clone())
        .with_params(&params)
        .execute_uncommitted(vec![batch])
        .await
        .map_err(map_lance_publish_error)?;
    match transaction.operation {
        Operation::Overwrite {
            fragments, schema, ..
        } => Ok((fragments, schema)),
        other => Err(OmniError::manifest_internal(format!(
            "an uncommitted `__manifest` write staged a {other} operation"
        ))),
    }
}

/// The cold fragments that join a cold fragment of `rows_written` rows being
/// written: smallest first while a fragment holds fewer than twice the rows
/// joined so far (git's `commit-graph` chain rule), an unrecorded count joins.
fn fragments_to_repack(cold: &[Fragment], rows_written: usize) -> Vec<Fragment> {
    let mut candidates: Vec<&Fragment> = cold.iter().collect();
    candidates.sort_by_key(|fragment| fragment.physical_rows.unwrap_or(0));
    let mut joined = rows_written;
    let mut repack = Vec::new();
    for fragment in candidates {
        let rows = fragment.physical_rows.unwrap_or(0);
        if rows >= joined.saturating_mul(2) {
            break;
        }
        joined = joined.saturating_add(rows);
        repack.push(fragment.clone());
    }
    repack
}

/// Create `uri` with `batch` as its first version, stamped by the batch's schema
/// metadata: the birth of a `__manifest` fixture of an older stamp.
#[cfg(any(test, feature = "test-util"))]
pub(crate) async fn create_for_test(uri: &str, batch: RecordBatch) -> Result<Dataset> {
    let schema = batch.schema();
    let params = WriteParams {
        mode: WriteMode::Create,
        store_params: Some(crate::storage::lance_store_params_for_uri(uri)?),
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        auto_cleanup: None,
        skip_auto_cleanup: true,
        ..Default::default()
    };
    let params = crate::lance_clone::write_params(uri, params)
        .await
        .map_err(crate::error::OmniError::storage)?;
    Dataset::write(
        arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        Some(params),
    )
    .await
    .map_err(crate::error::OmniError::storage)
}

/// Commit `batches` as the whole stored row set at `dataset`'s version + 1, in
/// one fragment (the module doc: zero retries, the version number is the CAS).
#[cfg(any(test, feature = "test-util"))]
pub(crate) async fn commit_overwrite(
    dataset: Dataset,
    batches: Vec<RecordBatch>,
) -> Result<Dataset> {
    let params = WriteParams {
        mode: WriteMode::Overwrite,
        auto_cleanup: None,
        skip_auto_cleanup: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let dataset = Arc::new(dataset);
    let transaction = InsertBuilder::new(dataset.clone())
        .with_params(&params)
        .execute_uncommitted(batches)
        .await
        .map_err(map_lance_publish_error)?;
    CommitBuilder::new(dataset)
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(transaction)
        .await
        .map_err(map_lance_publish_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cold(id: u64, rows: usize) -> Fragment {
        Fragment {
            physical_rows: Some(rows),
            ..Fragment::new(id)
        }
    }

    /// Joining stops at the first fragment holding at least twice the rows
    /// joined so far, counted smallest first, and an unsized fragment joins.
    #[test]
    fn repack_follows_the_commit_graph_chain_rule() {
        let ids = |fragments: &[Fragment]| fragments.iter().map(|f| f.id).collect::<Vec<_>>();
        assert_eq!(ids(&fragments_to_repack(&[], 10)), Vec::<u64>::new());
        assert_eq!(
            ids(&fragments_to_repack(
                &[cold(1, 1000), cold(2, 15), cold(3, 60)],
                10
            )),
            [2]
        );
        assert_eq!(
            ids(&fragments_to_repack(
                &[cold(1, 1000), cold(2, 15), cold(3, 49)],
                10
            )),
            [2, 3]
        );
        assert_eq!(
            ids(&fragments_to_repack(&[cold(1, 20)], 10)),
            Vec::<u64>::new()
        );
        assert_eq!(ids(&fragments_to_repack(&[cold(1, 19)], 10)), [1]);
        let mut unsized_fragment = Fragment::new(4);
        unsized_fragment.physical_rows = None;
        assert_eq!(
            ids(&fragments_to_repack(&[cold(1, 1000), unsized_fragment], 10)),
            [4]
        );
    }
}
