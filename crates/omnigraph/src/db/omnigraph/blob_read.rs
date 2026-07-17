//! Public blob read facade.
//!
//! Descriptor-first classification: one blob-v2 descriptor row is fetched
//! through the public take path and classified BEFORE any payload API is
//! touched. An `External` reference therefore never resolves its object store
//! server-side (Lance's `take_blobs` eagerly builds a store client for the
//! URI and issues a remote HEAD when the recorded size is zero), and internal
//! blobs are wrapped in an engine-owned [`BlobReader`] so
//! `lance::dataset::BlobFile` does not leak through the public API — the same
//! boundary shape as `SnapshotScanner`.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::StructArray;
use lance::dataset::{BlobFile, Dataset};

use crate::blob_descriptor::{BlobDescriptor, decode_blob_descriptor};
use crate::db::ReadTarget;
use crate::error::{OmniError, Result};

use super::Omnigraph;

/// Identity-derived version facts for one blob cell, suitable for a strong
/// HTTP `ETag`. The stable-table/incarnation pair (not the mutable
/// `table_key`) makes the tag rename-stable and drop/re-add ABA-safe; the
/// pinned table version plus stable row id make it content-stable within a
/// table lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobVersionTag {
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
    pub table_version: u64,
    pub row_id: u64,
}

/// How one blob cell's content is reachable.
pub enum BlobContent {
    /// Internal storage (inline / packed / dedicated): bytes stream through
    /// the engine-owned reader with bounded range reads.
    Streamed(BlobReader),
    /// External reference: the descriptor's absolute URI. The engine performs
    /// no server-side resolution of the external location — callers redirect
    /// or fetch it themselves.
    ExternalRef { uri: String },
}

/// One resolved blob cell at one pinned snapshot.
pub struct BlobRead {
    pub content: BlobContent,
    /// Total payload size in bytes. `None` only for an external reference
    /// whose descriptor recorded no size.
    pub size: Option<u64>,
    pub version_tag: BlobVersionTag,
}

/// Engine-owned, range-capable reader over one internal blob payload.
///
/// Every read is bounded by the caller-supplied range; nothing here
/// materializes the whole payload unless the caller asks for `0..size()`.
pub struct BlobReader {
    file: BlobFile,
}

impl BlobReader {
    /// Total payload size in bytes, resolved from the descriptor with no
    /// payload I/O.
    pub fn size(&self) -> u64 {
        self.file.size()
    }

    /// Read one blob-local byte range. The range is validated against the
    /// known size before any storage read; an out-of-bounds range is a typed
    /// bad-request, never a substrate error surfaced by text.
    pub async fn read_range(&self, range: Range<u64>) -> Result<bytes::Bytes> {
        if range.start > range.end || range.end > self.file.size() {
            return Err(OmniError::manifest(format!(
                "blob range {}..{} is outside payload size {}",
                range.start,
                range.end,
                self.file.size()
            )));
        }
        self.file
            .read_range(range)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    /// Convenience full read (`0..size()`); prefer `read_range` chunks for
    /// large payloads.
    pub async fn read_all(&self) -> Result<bytes::Bytes> {
        self.read_range(0..self.file.size()).await
    }
}

impl Omnigraph {
    /// Resolve one blob property cell at one pinned snapshot of `target`.
    ///
    /// Classification is descriptor-first: external references return
    /// [`BlobContent::ExternalRef`] with zero external I/O, internal blobs
    /// return a streaming [`BlobReader`]. A null blob cell and a missing row
    /// are typed not-found errors; a non-Blob property is a typed
    /// bad-request.
    ///
    /// ```ignore
    /// use omnigraph::db::{BlobContent, ReadTarget};
    /// let blob = db
    ///     .read_blob_at(ReadTarget::branch("main"), "Document", "readme", "content")
    ///     .await?;
    /// if let BlobContent::Streamed(reader) = blob.content {
    ///     let head = reader.read_range(0..16).await?;
    /// }
    /// ```
    pub async fn read_blob_at(
        &self,
        target: impl Into<ReadTarget>,
        type_name: &str,
        id: &str,
        property: &str,
    ) -> Result<BlobRead> {
        let (resolved, catalog) = self.capture_read_view(target).await?;
        let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
            OmniError::manifest_not_found(format!("unknown node type '{}'", type_name))
        })?;
        if !node_type.blob_properties.contains(property) {
            return Err(OmniError::manifest(format!(
                "property '{}' on type '{}' is not a Blob",
                property, type_name
            )));
        }

        let table_key = format!("node:{}", type_name);
        let entry = resolved.snapshot.entry(&table_key).ok_or_else(|| {
            OmniError::manifest_not_found(format!(
                "no table for type '{}' in this snapshot",
                type_name
            ))
        })?;
        let mut version_tag = BlobVersionTag {
            stable_table_id: entry.identity.stable_table_id,
            table_incarnation_id: entry.identity.table_incarnation_id,
            table_version: entry.table_version,
            row_id: 0,
        };

        let handle = self
            .storage()
            .open_snapshot_at_table(&resolved.snapshot, &table_key)
            .await?;
        let row_id = self
            .storage()
            .first_row_id_for_id(&handle, id)
            .await?
            .ok_or_else(|| {
                OmniError::manifest_not_found(format!(
                    "no {} with id '{}' found",
                    type_name, id
                ))
            })?;
        version_tag.row_id = row_id;

        // `take_builder`/`take_blobs` are Lance-specific accessors not lifted
        // onto the sealed `TableStorage` trait — reach the inner
        // `Arc<Dataset>` via the `pub(crate)` accessor for these read-only
        // calls.
        let ds = handle.into_arc();
        match take_blob_descriptor(&ds, row_id, property).await? {
            BlobDescriptor::Null => Err(OmniError::manifest_not_found(format!(
                "blob '{}' on {} '{}' is null",
                property, type_name, id
            ))),
            BlobDescriptor::External { uri, size, base_id } => {
                // OmniGraph writers only mint absolute external URIs
                // (`base_id == 0`); a base-relative reference would produce a
                // relative redirect target and is outside the supported
                // writer topology.
                if base_id != 0 {
                    return Err(OmniError::manifest_internal(format!(
                        "external blob '{}' on {} '{}' is base-relative (base_id {}); \
                         only absolute external URIs are supported",
                        property, type_name, id, base_id
                    )));
                }
                Ok(BlobRead {
                    content: BlobContent::ExternalRef { uri },
                    size: (size > 0).then_some(size),
                    version_tag,
                })
            }
            BlobDescriptor::Inline { .. }
            | BlobDescriptor::Packed { .. }
            | BlobDescriptor::Dedicated { .. } => {
                let mut blobs = ds
                    .take_blobs(&[row_id], property)
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
                let file = blobs.pop().ok_or_else(|| {
                    OmniError::manifest_not_found(format!(
                        "blob '{}' on {} '{}' returned no data",
                        property, type_name, id
                    ))
                })?;
                let size = file.size();
                Ok(BlobRead {
                    content: BlobContent::Streamed(BlobReader { file }),
                    size: Some(size),
                    version_tag,
                })
            }
        }
    }
}

/// Fetch and decode one row's blob descriptor through the public take path.
async fn take_blob_descriptor(
    ds: &Arc<Dataset>,
    row_id: u64,
    property: &str,
) -> Result<BlobDescriptor> {
    let projection = ds
        .schema()
        .project(&[property])
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let batch = ds
        .take_builder(&[row_id], projection)
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .execute()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    if batch.num_rows() != 1 {
        return Err(OmniError::manifest_not_found(format!(
            "blob descriptor take returned {} rows for one row id",
            batch.num_rows()
        )));
    }
    let descriptions = batch
        .column_by_name(property)
        .and_then(|col| col.as_any().downcast_ref::<StructArray>())
        .ok_or_else(|| {
            OmniError::Lance(format!(
                "blob column '{}' did not read back as a descriptor struct",
                property
            ))
        })?;
    decode_blob_descriptor(descriptions, 0)
}
