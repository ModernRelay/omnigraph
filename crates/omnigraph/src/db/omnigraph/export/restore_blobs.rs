//! Payload verification for one immutable, destination-confined restore walk.
use super::*;
use lance::dataset::BlobFile;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sha2::{Digest, Sha256};

const MAX_RANGES: usize = 1_000_000;
const MAX_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

#[derive(Default)]
pub(in crate::db::omnigraph) struct RestoreBlobCache {
    // This cache belongs to a single verify_restored_history call on a frozen
    // backend. Its keys are the resolved object path and exact byte range;
    // paths are never included in the portable witness.
    ranges: HashMap<(Path, u64, u64), [u8; 32]>,
    pub(in crate::db::omnigraph) read_bytes: u64,
    pub(in crate::db::omnigraph) hits: u64,
}

impl RestoreBlobCache {
    pub(super) async fn verified_values(
        &mut self,
        dataset: &Dataset,
        batch: &RecordBatch,
        properties: &HashSet<String>,
    ) -> Result<HashMap<String, Vec<Option<LogicalBlobValue>>>> {
        let row_ids = batch
            .column_by_name("_rowid")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| OmniError::manifest("restored Blob batch has no stable row ids"))?;
        let root = dataset
            .branch_location()
            .find_main()
            .map_err(OmniError::storage)?
            .path;
        let store = dataset
            .object_store(None)
            .await
            .map_err(OmniError::storage)?;
        let dataset = Arc::new(dataset.clone());
        let mut values = HashMap::new();
        for name in properties {
            let descriptors = batch
                .column_by_name(name)
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .ok_or_else(|| OmniError::blob_integrity("restore requires Blob descriptors"))?;
            let (cells, ids) = classify_blob_cells(
                name,
                descriptors,
                row_ids.values(),
                RangedExternalBlobs::RequireManaged,
            )?;
            let mut files = crate::blob::managed_blob_files(&dataset, &ids, name)
                .await?
                .into_iter();
            let mut column = Vec::with_capacity(cells.len());
            for cell in cells {
                column.push(match cell {
                    ClassifiedBlobCell::Decided(None) => None,
                    ClassifiedBlobCell::Managed { length } => {
                        let file = files.next().ok_or_else(|| {
                            OmniError::blob_integrity("missing restored Blob handle")
                        })?;
                        let hash = self
                            .digest(&file, length, &root, store.inner.as_ref())
                            .await?;
                        let hash = sha2::digest::Output::<Sha256>::from(hash);
                        Some(LogicalBlobValue::Text(format!("sha256:{length}:{hash:x}")))
                    }
                    ClassifiedBlobCell::Decided(Some(_)) => {
                        return Err(OmniError::blob_integrity(
                            "restore encountered an external Blob",
                        ));
                    }
                });
            }
            values.insert(name.clone(), column);
        }
        Ok(values)
    }

    async fn digest(
        &mut self,
        file: &BlobFile,
        length: u64,
        root: &Path,
        store: &dyn object_store::ObjectStore,
    ) -> Result<[u8; 32]> {
        let path = file.data_path();
        if !path.prefix_matches(root) || file.size() != length {
            return Err(OmniError::blob_integrity(
                "restored Blob is outside its table root or has an inconsistent length",
            ));
        }
        let key = (path.clone(), file.position(), length);
        if let Some(hash) = self.ranges.get(&key) {
            self.hits += 1;
            return Ok(*hash);
        }
        if self.ranges.len() >= MAX_RANGES {
            return Err(OmniError::resource_limit(
                "restore_blob_ranges",
                MAX_RANGES as u64,
                self.ranges.len() as u64 + 1,
            ));
        }
        let bytes = self.read_bytes.saturating_add(length);
        if bytes > MAX_BYTES {
            return Err(OmniError::resource_limit(
                "restore_blob_bytes",
                MAX_BYTES,
                bytes,
            ));
        }
        // An empty managed value still references a real object. Check the
        // resolved range against that object even when there is nothing to GET.
        let meta = store
            .head(path)
            .await
            .map_err(|err| OmniError::storage(err.into()))?;
        if file
            .position()
            .checked_add(length)
            .is_none_or(|end| end > meta.size)
        {
            return Err(OmniError::blob_integrity(
                "restored Blob range exceeds its object",
            ));
        }
        let mut sha = Sha256::new();
        let mut offset = 0;
        while offset < length {
            let end = (offset + crate::blob::BLOB_READ_RANGE_MAX_BYTES).min(length);
            let chunk = file
                .read_range(offset..end)
                .await
                .map_err(OmniError::storage)?;
            if chunk.len() as u64 != end - offset {
                return Err(OmniError::blob_integrity(
                    "restored Blob range returned short data",
                ));
            }
            sha.update(chunk);
            offset = end;
        }
        let hash: [u8; 32] = sha.finalize().into();
        self.ranges.insert(key, hash);
        self.read_bytes = bytes;
        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_the_same_verified_object_and_range_can_reuse_a_digest() {
        let store = Arc::new(lance_io::object_store::ObjectStore::memory());
        let root = Path::from("table");
        let first = root.clone().join("first");
        let second = root.clone().join("second");
        store
            .inner
            .put(&first, b"abcd".as_slice().into())
            .await
            .unwrap();
        store
            .inner
            .put(&second, b"efgh".as_slice().into())
            .await
            .unwrap();
        let handle = |path: &Path, start, size| {
            BlobFile::new_packed(store.clone(), path.clone(), start, size)
        };
        let mut cache = RestoreBlobCache::default();
        for (path, offset, expected) in [
            (&first, 0, b"ab"),
            (&first, 0, b"ab"),
            (&first, 2, b"cd"),
            (&second, 0, b"ef"),
        ] {
            assert_eq!(
                cache
                    .digest(&handle(path, offset, 2), 2, &root, store.inner.as_ref())
                    .await
                    .unwrap(),
                <[u8; 32]>::from(Sha256::digest(expected))
            );
        }
        assert_eq!(
            (cache.read_bytes, cache.hits, cache.ranges.len()),
            (6, 1, 3)
        );
        for (path, offset, length) in [
            (Path::from("table-elsewhere"), 0, 2),
            (first.clone(), 4, 2),
            (root.clone().join("missing"), 0, 0),
        ] {
            assert!(
                cache
                    .digest(
                        &handle(&path, offset, length),
                        length,
                        &root,
                        store.inner.as_ref()
                    )
                    .await
                    .is_err()
            );
        }
    }
}
