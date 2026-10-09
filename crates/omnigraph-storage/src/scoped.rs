//! Explicit accounting for one owner's storage mutations.
//!
//! This is process-local settlement evidence, never a writer fence. Every
//! wrapped remote client must disable retries before construction: a successful
//! retry cannot establish what happened to an earlier, unacknowledged request.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use tokio::sync::Notify;

/// Shared mutation accounting for an explicitly owned storage lifetime.
///
/// Failed or abandoned requests permanently make the scope uncertain. Closing
/// it refuses new mutations, while already admitted multipart uploads may still
/// finish or abort. Reads remain available. An idle scope is not proof of
/// settlement unless it is also unpoisoned and its logical owners have drained.
#[derive(Clone, Debug, Default)]
pub struct StorageIoScope {
    inner: Arc<ScopeInner>,
}

#[derive(Debug, Default)]
struct ScopeInner {
    state: Mutex<ScopeState>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct ScopeState {
    pending: usize,
    closed: bool,
    uncertain: bool,
}

impl StorageIoScope {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_uncertain(&self) -> bool {
        self.inner.state.lock().unwrap().uncertain
    }

    pub fn pending(&self) -> usize {
        self.inner.state.lock().unwrap().pending
    }

    /// Irreversibly refuse new mutation admission.
    pub fn close(&self) {
        self.inner.state.lock().unwrap().closed = true;
    }

    /// Wait until all admitted requests and multipart lifetimes have settled
    /// or poisoned the scope. The caller owns the shutdown deadline and must
    /// separately check [`Self::is_uncertain`].
    pub async fn wait_idle(&self) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.pending() == 0 {
                return;
            }
            changed.await;
        }
    }

    /// Wrap a client whose mutating requests perform at most one attempt.
    ///
    /// The caller must configure that client (and any outer retry layer) before
    /// wrapping it. Scoped adapter constructors do so for their own clients.
    pub fn wrap_object_store(&self, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(ScopedObjectStore {
            inner,
            scope: self.clone(),
        })
    }

    pub(crate) fn begin(&self) -> Result<Mutation> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(closed_error());
        }
        if state.uncertain {
            return Err(uncertain_error());
        }
        state.pending += 1;
        Ok(Mutation {
            scope: self.clone(),
            settled: false,
        })
    }

    fn begin_child(&self, cleanup: bool) -> Result<Mutation> {
        // Only an admitted multipart lifetime can call this. Its outstanding
        // guard prevents an idle observation before the child is registered.
        let mut state = self.inner.state.lock().unwrap();
        if state.uncertain && !cleanup {
            return Err(uncertain_error());
        }
        state.pending += 1;
        Ok(Mutation {
            scope: self.clone(),
            settled: false,
        })
    }
}

fn closed_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "owned storage",
        source: std::io::Error::other("storage mutation admission is closed").into(),
    }
}

fn uncertain_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "owned storage",
        source: std::io::Error::other("a prior storage mutation has an uncertain outcome").into(),
    }
}

#[derive(Debug)]
pub(crate) struct Mutation {
    scope: StorageIoScope,
    settled: bool,
}

impl Mutation {
    pub(crate) fn finish<T>(mut self, result: Result<T>, conditional: bool) -> Result<T> {
        self.settled = result.is_ok()
            || (conditional
                && matches!(
                    result,
                    Err(object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. })
                ));
        result
    }

    pub(crate) fn settle(mut self) {
        self.settled = true;
    }
}

impl Drop for Mutation {
    fn drop(&mut self) {
        let mut state = self.scope.inner.state.lock().unwrap();
        if !self.settled {
            state.uncertain = true;
        }
        state.pending -= 1;
        drop(state);
        self.scope.inner.changed.notify_waiters();
    }
}

#[derive(Debug)]
struct ScopedObjectStore {
    inner: Arc<dyn ObjectStore>,
    scope: StorageIoScope,
}

impl fmt::Display for ScopedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scoped({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ScopedObjectStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let conditional = !matches!(opts.mode, object_store::PutMode::Overwrite);
        let mutation = self.scope.begin()?;
        mutation.finish(self.inner.put_opts(path, payload, opts).await, conditional)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let lifetime = self.scope.begin()?;
        match self.inner.put_multipart_opts(path, opts).await {
            Ok(inner) => Ok(Box::new(ScopedUpload {
                inner,
                lifetime: Some(lifetime),
                scope: self.scope.clone(),
            })),
            Err(error) => lifetime.finish(Err(error), false),
        }
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(path, opts).await
    }

    async fn get_ranges(&self, path: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(path, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let scope = self.scope.clone();
        // Track each one-key request to its acknowledgement. In particular,
        // ObjectStoreExt::delete consumes one item then drops this stream.
        locations
            .map(move |path| {
                let inner = inner.clone();
                let scope = scope.clone();
                async move {
                    let path = path?;
                    let mutation = scope.begin()?;
                    mutation.finish(inner.delete(&path).await, false)?;
                    Ok(path)
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        let conditional = matches!(opts.mode, object_store::CopyMode::Create);
        let mutation = self.scope.begin()?;
        mutation.finish(self.inner.copy_opts(from, to, opts).await, conditional)
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> Result<()> {
        let mutation = self.scope.begin()?;
        if opts.target_mode == object_store::RenameTargetMode::Create {
            // The standard conditional rename is copy-Create then delete.
            // Keep its phases visible: a copy conflict is known no-effect,
            // whereas an error after a successful copy must not inherit that
            // classification. The outer owner spans both requests, even if
            // new admission closes between them.
            let copy = CopyOptions {
                mode: object_store::CopyMode::Create,
                extensions: opts.extensions,
            };
            match self.inner.copy_opts(from, to, copy).await {
                Err(error) => mutation.finish(Err(error), true),
                Ok(()) => mutation.finish(self.inner.delete(from).await, false),
            }
        } else {
            // Preserve the backend's optimized atomic overwrite rename.
            mutation.finish(self.inner.rename_opts(from, to, opts).await, false)
        }
    }
}

#[derive(Debug)]
struct ScopedUpload {
    inner: Box<dyn MultipartUpload>,
    lifetime: Option<Mutation>,
    scope: StorageIoScope,
}

#[async_trait]
impl MultipartUpload for ScopedUpload {
    fn put_part(&mut self, payload: PutPayload) -> BoxFuture<'static, Result<()>> {
        if self.lifetime.is_none() {
            return Box::pin(async { Err(closed_error()) });
        }
        // Register before invoking the synchronous backend method: a backend
        // may start work there, before its returned future is first polled.
        let mutation = match self.scope.begin_child(false) {
            Ok(mutation) => mutation,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let part = self.inner.put_part(payload);
        Box::pin(async move { mutation.finish(part.await, false) })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.lifetime.is_none() {
            return Err(closed_error());
        }
        let mutation = self.scope.begin_child(false)?;
        let result = mutation.finish(self.inner.complete().await, false);
        if result.is_ok() {
            self.lifetime.take().unwrap().settle();
        }
        result
    }

    async fn abort(&mut self) -> Result<()> {
        if self.lifetime.is_none() {
            return Err(closed_error());
        }
        let mutation = self.scope.begin_child(true)?;
        let result = mutation.finish(self.inner.abort().await, false);
        if result.is_ok() {
            self.lifetime.take().unwrap().settle();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use std::time::Duration;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Point {
        Put,
        Delete,
        Copy,
        Rename,
        Multipart,
        Part,
        Complete,
        Abort,
    }

    #[derive(Debug)]
    struct Fault {
        point: Point,
        lost_ack: bool,
        entered: Notify,
        resume: Notify,
    }

    impl Fault {
        async fn after<T>(&self, point: Point, result: Result<T>) -> Result<T> {
            if point == self.point && result.is_ok() {
                self.entered.notify_one();
                if self.lost_ack {
                    return Err(object_store::Error::Generic {
                        store: "fault fixture",
                        source: std::io::Error::other("acknowledgement lost after effect").into(),
                    });
                }
                self.resume.notified().await;
            }
            result
        }
    }

    #[derive(Debug)]
    struct FaultStore {
        inner: Arc<InMemory>,
        fault: Arc<Fault>,
    }

    impl fmt::Display for FaultStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FaultStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FaultStore {
        async fn put_opts(
            &self,
            p: &Path,
            data: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.fault
                .after(Point::Put, self.inner.put_opts(p, data, opts).await)
                .await
        }

        async fn put_multipart_opts(
            &self,
            p: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            let inner = self
                .fault
                .after(
                    Point::Multipart,
                    self.inner.put_multipart_opts(p, opts).await,
                )
                .await?;
            Ok(Box::new(FaultUpload {
                inner,
                fault: self.fault.clone(),
            }))
        }

        async fn get_opts(&self, p: &Path, opts: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(p, opts).await
        }

        fn delete_stream(
            &self,
            paths: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            let inner = self.inner.clone();
            let fault = self.fault.clone();
            paths
                .then(move |p| {
                    let inner = inner.clone();
                    let fault = fault.clone();
                    async move {
                        let p = p?;
                        fault.after(Point::Delete, inner.delete(&p).await).await?;
                        Ok(p)
                    }
                })
                .boxed()
        }

        fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(p)
        }

        async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
            self.fault
                .after(Point::Copy, self.inner.copy_opts(from, to, opts).await)
                .await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> Result<()> {
            self.fault
                .after(Point::Rename, self.inner.rename_opts(from, to, opts).await)
                .await
        }
    }

    #[derive(Debug)]
    struct FaultUpload {
        inner: Box<dyn MultipartUpload>,
        fault: Arc<Fault>,
    }

    #[async_trait]
    impl MultipartUpload for FaultUpload {
        fn put_part(&mut self, p: PutPayload) -> BoxFuture<'static, Result<()>> {
            let part = self.inner.put_part(p);
            let fault = self.fault.clone();
            Box::pin(async move { fault.after(Point::Part, part.await).await })
        }
        async fn complete(&mut self) -> Result<PutResult> {
            self.fault
                .after(Point::Complete, self.inner.complete().await)
                .await
        }
        async fn abort(&mut self) -> Result<()> {
            self.fault
                .after(Point::Abort, self.inner.abort().await)
                .await
        }
    }

    async fn fixture(
        point: Point,
        lost_ack: bool,
    ) -> (StorageIoScope, Arc<dyn ObjectStore>, Arc<Fault>) {
        let scope = StorageIoScope::new();
        let fault = Arc::new(Fault {
            point,
            lost_ack,
            entered: Notify::new(),
            resume: Notify::new(),
        });
        let inner = Arc::new(InMemory::new());
        inner
            .put(&Path::from("source"), "original".into())
            .await
            .unwrap();
        let store = scope.wrap_object_store(Arc::new(FaultStore {
            inner,
            fault: fault.clone(),
        }));
        (scope, store, fault)
    }

    async fn mutate(store: Arc<dyn ObjectStore>, point: Point) -> Result<()> {
        let p = Path::from("target");
        match point {
            Point::Put => store.put(&p, "data".into()).await.map(|_| ()),
            Point::Delete => store.delete(&Path::from("source")).await,
            Point::Copy => store.copy(&Path::from("source"), &p).await,
            Point::Rename => store.rename(&Path::from("source"), &p).await,
            Point::Multipart | Point::Part | Point::Complete | Point::Abort => {
                let mut upload = store.put_multipart(&p).await?;
                upload.put_part("part".into()).await?;
                if point == Point::Abort {
                    upload.abort().await
                } else {
                    upload.complete().await.map(|_| ())
                }
            }
        }
    }

    const POINTS: [Point; 8] = [
        Point::Put,
        Point::Delete,
        Point::Copy,
        Point::Rename,
        Point::Multipart,
        Point::Part,
        Point::Complete,
        Point::Abort,
    ];

    #[tokio::test]
    async fn cancellation_of_every_mutation_boundary_poisoned_before_idle() {
        for point in POINTS {
            let (scope, store, fault) = fixture(point, false).await;
            let task = tokio::spawn(mutate(store, point));
            tokio::time::timeout(Duration::from_secs(2), fault.entered.notified())
                .await
                .unwrap();
            assert!(scope.pending() > 0, "{point:?}");
            scope.close();
            assert!(
                tokio::time::timeout(Duration::from_millis(1), scope.wait_idle())
                    .await
                    .is_err()
            );
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            scope.wait_idle().await;
            assert!(scope.is_uncertain(), "cancelled {point:?} must poison");
        }
    }

    #[tokio::test]
    async fn lost_ack_is_sticky_even_when_caller_swallows_error() {
        for point in POINTS {
            let (scope, store, _) = fixture(point, true).await;
            let _swallowed = mutate(store.clone(), point).await;
            scope.wait_idle().await;
            assert!(
                scope.is_uncertain(),
                "lost {point:?} acknowledgement must poison"
            );
            // A higher layer cannot turn its retry into apparent success.
            assert!(
                store
                    .put(&Path::from("later"), "later".into())
                    .await
                    .is_err()
            );
            assert!(store.get(&Path::from("later")).await.is_err());
        }
    }

    #[tokio::test]
    async fn never_polled_put_or_delete_has_no_effect_or_poison() {
        let scope = StorageIoScope::new();
        let store = scope.wrap_object_store(Arc::new(InMemory::new()));
        let path = Path::from("key");
        drop(store.put(&path, "value".into()));
        drop(store.delete_stream(futures::stream::iter([Ok(path)]).boxed()));
        assert_eq!(scope.pending(), 0);
        assert!(!scope.is_uncertain());
    }

    #[tokio::test]
    async fn immutable_reuse_and_single_conditional_conflicts_settle() {
        let scope = StorageIoScope::new();
        let store = scope.wrap_object_store(Arc::new(InMemory::new()));
        let path = Path::from("content");
        store
            .put_opts(&path, "exact".into(), object_store::PutMode::Create.into())
            .await
            .unwrap();
        assert!(matches!(
            store
                .put_opts(&path, "exact".into(), object_store::PutMode::Create.into())
                .await,
            Err(object_store::Error::AlreadyExists { .. })
        ));
        let update = object_store::PutMode::Update(object_store::UpdateVersion {
            e_tag: Some("wrong".into()),
            version: None,
        });
        assert!(matches!(
            store.put_opts(&path, "changed".into(), update.into()).await,
            Err(object_store::Error::Precondition { .. })
        ));
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "exact"
        );
        store.copy(&path, &Path::from("copy")).await.unwrap();
        assert!(matches!(
            store.rename_if_not_exists(&Path::from("copy"), &path).await,
            Err(object_store::Error::AlreadyExists { .. })
        ));
        assert!(!scope.is_uncertain(), "initial copy conflict is settled");
        assert!(store.get(&Path::from("copy")).await.is_ok());
        store
            .rename(&Path::from("copy"), &Path::from("renamed"))
            .await
            .unwrap();
        store.delete(&Path::from("renamed")).await.unwrap();
        scope.close();
        scope.wait_idle().await;
        assert!(!scope.is_uncertain());
        assert!(store.put(&path, "late".into()).await.is_err());
        assert!(store.delete(&path).await.is_err());
        assert!(store.copy(&path, &Path::from("late")).await.is_err());
        assert!(store.rename(&path, &Path::from("late")).await.is_err());
        assert!(store.put_multipart(&path).await.is_err());
        assert_eq!(
            store.get_ranges(&path, &[0..2, 2..5]).await.unwrap(),
            [Bytes::from("ex"), Bytes::from("act")]
        );
        assert_eq!(
            store
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            !scope.is_uncertain(),
            "pre-effect closed refusals are settled"
        );
    }

    #[tokio::test]
    async fn multipart_lifetime_covers_deferred_abort_and_preexisting_parts() {
        let (scope, store, fault) = fixture(Point::Abort, false).await;
        let mut upload = store.put_multipart(&Path::from("upload")).await.unwrap();
        let part = upload.put_part("data".into());
        assert_eq!(scope.pending(), 2);
        scope.close();
        part.await.unwrap();
        let abort = tokio::spawn(async move { upload.abort().await });
        fault.entered.notified().await;
        assert!(scope.pending() > 0);
        fault.resume.notify_one();
        abort.await.unwrap().unwrap();
        scope.wait_idle().await;
        assert!(!scope.is_uncertain());

        let scope = StorageIoScope::new();
        let store = scope.wrap_object_store(Arc::new(InMemory::new()));
        let mut upload = store.put_multipart(&Path::from("abandoned")).await.unwrap();
        let part = upload.put_part("data".into());
        drop(upload);
        assert!(scope.is_uncertain());
        assert_eq!(
            scope.pending(),
            1,
            "part remains owned after multipart drop"
        );
        drop(part);
        scope.wait_idle().await;
        assert!(scope.is_uncertain());
    }

    #[tokio::test]
    async fn multipart_can_complete_after_closing_new_admission() {
        let scope = StorageIoScope::new();
        let store = scope.wrap_object_store(Arc::new(InMemory::new()));
        let path = Path::from("upload");
        let mut upload = store.put_multipart(&path).await.unwrap();
        scope.close();
        upload.put_part("body".into()).await.unwrap();
        upload.complete().await.unwrap();
        scope.wait_idle().await;
        assert!(!scope.is_uncertain());
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "body"
        );
    }

    #[tokio::test]
    async fn scoped_local_adapter_cannot_bypass_closed_scope_with_directory_delete() {
        let root = tempfile::tempdir().unwrap();
        let graph = root.path().join("graph");
        std::fs::create_dir(&graph).unwrap();
        std::fs::write(graph.join("file"), "retained").unwrap();
        let scope = StorageIoScope::new();
        let handle =
            crate::storage_handle_for_uri_scoped(root.path().to_str().unwrap(), scope.clone())
                .unwrap();
        let adapter = handle.adapter();
        assert!(adapter.io_scope().is_some());
        scope.close();
        assert!(
            adapter
                .delete_prefix(graph.to_str().unwrap())
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(graph.join("file")).unwrap(),
            "retained"
        );
        assert!(!scope.is_uncertain());
    }

    #[tokio::test]
    async fn failed_part_can_abort_but_cannot_publish_after_uncertainty() {
        let (scope, store, _) = fixture(Point::Part, true).await;
        let mut upload = store.put_multipart(&Path::from("upload")).await.unwrap();
        assert!(upload.put_part("data".into()).await.is_err());
        assert!(scope.is_uncertain());
        assert!(upload.complete().await.is_err());
        upload.abort().await.unwrap();
        scope.wait_idle().await;
        assert!(scope.is_uncertain());
        assert!(store.get(&Path::from("upload")).await.is_err());
    }

    #[tokio::test]
    async fn conditional_rename_does_not_treat_failed_delete_as_copy_conflict() {
        let (scope, store, _) = fixture(Point::Delete, true).await;
        let target = Path::from("renamed");
        assert!(
            store
                .rename_if_not_exists(&Path::from("source"), &target)
                .await
                .is_err()
        );
        assert!(scope.is_uncertain());
        assert_eq!(
            store.get(&target).await.unwrap().bytes().await.unwrap(),
            "original"
        );
        scope.wait_idle().await;
    }

    async fn single_attempt_http(fail_status: Option<u16>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::Instant;

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::task::spawn_blocking(move || {
            let mut deadline = Instant::now() + Duration::from_secs(5);
            let mut requests = 0;
            loop {
                let (mut socket, _) = match listener.accept() {
                    Ok(socket) => socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept fixture: {error}"),
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buf = [0; 4096];
                let header_end = loop {
                    let read = socket.read(&mut buf).unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&buf[..read]);
                    assert!(request.len() <= 16 * 1024);
                    if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                assert!(request.starts_with(b"PUT "));
                let headers = std::str::from_utf8(&request[..header_end]).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(length <= 1024);
                while request.len() < header_end + length {
                    let read = socket.read(&mut buf).unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&buf[..read]);
                }
                requests += 1;
                if requests == 1 {
                    deadline = Instant::now() + Duration::from_millis(500);
                    // Both cases represent a request whose body was accepted.
                    // Any illicit second request would receive apparent success.
                    if let Some(status) = fail_status {
                        write!(socket, "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    }
                } else {
                    write!(socket, "HTTP/1.1 200 OK\r\nETag: \"second-attempt\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                }
                // None deliberately drops the connection without an ack.
            }
            requests
        });
        let inner = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("test-bucket")
            .with_region("us-east-1")
            .with_access_key_id("fixture-access")
            .with_secret_access_key("fixture-secret")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_retry(crate::single_attempt_retry_config())
            .build()
            .unwrap();
        let scope = StorageIoScope::new();
        let store = scope.wrap_object_store(Arc::new(inner));
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            store.put(&Path::from("key"), "body".into()),
        )
        .await
        .unwrap();
        assert!(
            result.is_err(),
            "one uncertain attempt must never become success"
        );
        assert!(scope.is_uncertain());
        assert_eq!(server.await.unwrap(), 1, "mutating transport retried");
    }

    #[tokio::test]
    async fn single_attempt_transport_keeps_5xx_and_lost_ack_uncertain() {
        single_attempt_http(Some(500)).await;
        single_attempt_http(Some(503)).await;
        single_attempt_http(None).await;
    }
}
