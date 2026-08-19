//! Per-operation-class object-store call counting for run records (RFC 0039:
//! "the harness counts the run's storage calls per operation class, adopting
//! RFC-031's operation vocabulary unchanged (get, put, put_part, head, list,
//! delete, copy, rename, and the multipart operations), into the same record
//! beside the timings").
//!
//! [`CallCounter`] is a [`WrappingObjectStore`] in the exact shape of the
//! engine cost harness's `PrefixCounter` (`tests/helpers/cost.rs`): it is
//! handed to the engine through the public `QueryIoProbes` task-local, the
//! `open_dataset` chokepoint attaches it to every dataset opened under the
//! probe scope, and every object-store request on those handles is counted.
//! Counts are build-profile-independent (RFC 0039 rule 2). Per rule 6 they
//! are a measurement column and never gate anything, at any CI stage.
//!
//! Unobserved classes still serialize as zero — a class is never omitted, so
//! a reader can distinguish "no rename happened" from "rename was not
//! counted".
//!
//! Completeness: the count covers every store opened inside the probe scope.
//! The bench run opens the graph inside the scope, so cached handles reused by
//! later repetitions still carry the wrapper. Non-Lance control-plane calls go
//! through the engine's own public `CountingStorageAdapter` instead (wired in
//! `run.rs`); the two are complementary, not overlapping.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OSResult,
    UploadPart,
};

/// One take of the per-class call tally (calls since the previous take), in
/// RFC-031's operation vocabulary. Every class always serializes, zero
/// included.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CallCounts {
    pub get: u64,
    pub put: u64,
    pub put_part: u64,
    pub head: u64,
    pub list: u64,
    pub delete: u64,
    pub copy: u64,
    pub rename: u64,
    /// Multipart-upload initiations (`put_multipart_opts`).
    pub put_multipart: u64,
    pub multipart_complete: u64,
    pub multipart_abort: u64,
}

#[derive(Debug, Default)]
struct Tally {
    get: AtomicU64,
    put: AtomicU64,
    put_part: AtomicU64,
    head: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
    rename: AtomicU64,
    put_multipart: AtomicU64,
    multipart_complete: AtomicU64,
    multipart_abort: AtomicU64,
}

/// Shared per-class object-store call counter. Clone freely: every clone (and
/// every store wrapped through [`WrappingObjectStore::wrap`]) feeds one tally.
#[derive(Debug, Default, Clone)]
pub struct CallCounter(Arc<Tally>);

impl CallCounter {
    /// Read-and-reset: the calls recorded since the previous `take`. The run
    /// loop takes once immediately before each measured merge (discarding the
    /// prepare/divergence calls) and once after (the merge's own counts).
    pub fn take(&self) -> CallCounts {
        CallCounts {
            get: self.0.get.swap(0, Ordering::Relaxed),
            put: self.0.put.swap(0, Ordering::Relaxed),
            put_part: self.0.put_part.swap(0, Ordering::Relaxed),
            head: self.0.head.swap(0, Ordering::Relaxed),
            list: self.0.list.swap(0, Ordering::Relaxed),
            delete: self.0.delete.swap(0, Ordering::Relaxed),
            copy: self.0.copy.swap(0, Ordering::Relaxed),
            rename: self.0.rename.swap(0, Ordering::Relaxed),
            put_multipart: self.0.put_multipart.swap(0, Ordering::Relaxed),
            multipart_complete: self.0.multipart_complete.swap(0, Ordering::Relaxed),
            multipart_abort: self.0.multipart_abort.swap(0, Ordering::Relaxed),
        }
    }
}

impl WrappingObjectStore for CallCounter {
    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CountingStore {
            target,
            counter: self.clone(),
        })
    }
}

/// The wrapped `ObjectStore` recording each call into a [`CallCounter`].
/// Implements the required core methods (object_store 0.13) — the convenience
/// surface (`get`/`put`/`head`/`delete`/`rename`/...) is an extension-trait
/// blanket impl routing through these core methods, so every request is still
/// counted. `rename_opts` is a provided method whose default decomposes into
/// copy-plus-delete; the wrapper overrides it (count one `rename`, forward to
/// the target's own `rename_opts`) so an atomic backend rename stays atomic
/// and lands in the `rename` class instead of masquerading as copy+delete.
#[derive(Debug)]
struct CountingStore {
    target: Arc<dyn ObjectStore>,
    counter: CallCounter,
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.target)
    }
}

/// Counts `put_part` and the multipart completion/abort on a wrapped upload.
#[derive(Debug)]
struct CountingMultipart {
    inner: Box<dyn MultipartUpload>,
    counter: CallCounter,
}

#[async_trait]
impl MultipartUpload for CountingMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.counter.0.put_part.fetch_add(1, Ordering::Relaxed);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> OSResult<PutResult> {
        self.counter
            .0
            .multipart_complete
            .fetch_add(1, Ordering::Relaxed);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> OSResult<()> {
        self.counter
            .0
            .multipart_abort
            .fetch_add(1, Ordering::Relaxed);
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.counter.0.put.fetch_add(1, Ordering::Relaxed);
        self.target.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        self.counter.0.put_multipart.fetch_add(1, Ordering::Relaxed);
        let inner = self.target.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingMultipart {
            inner,
            counter: self.counter.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        if options.head {
            self.counter.0.head.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counter.0.get.fetch_add(1, Ordering::Relaxed);
        }
        self.target.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        // Counted per stream invocation, not per deleted object.
        self.counter.0.delete.fetch_add(1, Ordering::Relaxed);
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counter.0.list.fetch_add(1, Ordering::Relaxed);
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.counter.0.list.fetch_add(1, Ordering::Relaxed);
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.counter.0.list.fetch_add(1, Ordering::Relaxed);
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OSResult<()> {
        self.counter.0.copy.fetch_add(1, Ordering::Relaxed);
        self.target.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OSResult<()> {
        // Forward to the target's own rename (atomic on local FS) instead of
        // inheriting the copy-plus-delete default, so the rename class is
        // observed rather than decomposed.
        self.counter.0.rename.fetch_add(1, Ordering::Relaxed);
        self.target.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_resets_the_tally() {
        let counter = CallCounter::default();
        counter.0.get.fetch_add(3, Ordering::Relaxed);
        counter.0.put.fetch_add(1, Ordering::Relaxed);
        counter.0.put_part.fetch_add(2, Ordering::Relaxed);
        let first = counter.take();
        assert_eq!(first.get, 3);
        assert_eq!(first.put, 1);
        assert_eq!(first.put_part, 2);
        assert_eq!(counter.take(), CallCounts::default());
    }

    #[test]
    fn unobserved_classes_serialize_as_zero() {
        let json = serde_json::to_value(CallCounts::default()).unwrap();
        for class in [
            "get",
            "put",
            "put_part",
            "head",
            "list",
            "delete",
            "copy",
            "rename",
            "put_multipart",
            "multipart_complete",
            "multipart_abort",
        ] {
            assert_eq!(json[class], 0, "class {class} must appear even at zero");
        }
    }
}
