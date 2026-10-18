//! Explicit accounting for one owner's storage mutations.
//!
//! This is process-local settlement evidence, never a writer fence. Every
//! wrapped remote client must disable retries before construction: a successful
//! retry cannot establish what happened to an earlier, unacknowledged request.
//!
//! What an unacknowledged request can still do decides whether it poisons the
//! scope, and that is a property of the key it names ([`KeyClass`]):
//!
//! - Lance writes its table files once under fresh names (`data/*.lance` and
//!   the Blob sidecars below them, `_transactions/*.txn`, `_deletions/*`,
//!   `_indices/<uuid>/*`, detached `_versions/d*.manifest`) and its numbered
//!   `_versions/*.manifest` put-if-absent. A request to one of those keys
//!   that lands late or twice changes nothing a `__manifest` publication
//!   resolves: a file nothing names is reclaimable garbage, a detached
//!   version's random id is never reused, and a numbered manifest's
//!   precondition admits one copy, which the next reader observes. Such a
//!   request settles with its definite error.
//! - Every other key (a Lance ref under `_refs/`, the version hint, a
//!   control object under `__cluster/`, a path this module does not know)
//!   may be rewritten or removed by a request that lands after the owner
//!   gave up on it, so an unsettled request there permanently makes the
//!   scope uncertain. The first such request is logged with its key and
//!   error; nothing else records the cause.
//! - A manifest put the store did not acknowledge may still land. Lance
//!   reads the version back once, takes one "not found" as definitive, and
//!   deletes the attempt's transaction file; a manifest landing after that
//!   would name a file that is gone. So after such a put the scope refuses
//!   every delete under that dataset's `_transactions/` for its own
//!   lifetime: the file stays as reclaimable garbage, and a late manifest
//!   stays complete. A conditional conflict is a definite refusal and holds
//!   nothing.
//! - A multipart upload to an immutable key settles whatever happens to its
//!   `complete`, its `abort` or the upload itself: the object either exists
//!   whole under its fresh name or never does, and leftover parts are the
//!   bucket's to expire.

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
    /// `_transactions/` directories whose files may be named by a manifest
    /// the store did not acknowledge; deletes under them are refused.
    held_transaction_dirs: Vec<Path>,
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

    pub(crate) fn begin(&self, request: Request) -> Result<Mutation> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(closed_error());
        }
        if state.uncertain {
            return Err(uncertain_error());
        }
        if request.op == "delete"
            && let Some(parent) = parent_dir(&request.path)
            && state.held_transaction_dirs.contains(&parent)
        {
            // A settled refusal: Lance's best-effort cleanup logs it and
            // returns the commit's own error; the collector reclaims the
            // file later from another process.
            tracing::warn!(
                path = %request.path,
                "refusing to delete a transaction file a late manifest may name"
            );
            return Err(transaction_file_held_error());
        }
        state.pending += 1;
        Ok(Mutation {
            scope: self.clone(),
            settled: false,
            hold_transactions: request.transactions_dir.is_some(),
            request,
        })
    }

    fn begin_child(&self, request: Request, cleanup: bool) -> Result<Mutation> {
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
            hold_transactions: false,
            request,
        })
    }
}

/// The directory a key sits in, or `None` for a one-segment key.
fn parent_dir(path: &Path) -> Option<Path> {
    let parts: Vec<_> = path.parts().collect();
    let (_, dirs) = parts.split_last()?;
    Some(Path::from_iter(dirs.iter().cloned()))
}

/// For a put of `<dataset>/_versions/<name>.manifest`, the dataset's
/// `_transactions/` directory, whose files that manifest may name.
fn manifest_transactions_dir(path: &Path) -> Option<Path> {
    let parts: Vec<_> = path.parts().collect();
    let [dataset @ .., versions, name] = parts.as_slice() else {
        return None;
    };
    (versions.as_ref() == "_versions" && name.as_ref().ends_with(".manifest")).then(|| {
        Path::from_iter(dataset.iter().cloned().chain(std::iter::once(
            object_store::path::PathPart::from("_transactions"),
        )))
    })
}

/// Whether a request to this key that lands after its owner gave up on it,
/// or lands twice, can change what a later owner reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyClass {
    /// A Lance table file written once under a fresh name, or a manifest
    /// version written put-if-absent: a late or duplicate landing is
    /// unreachable garbage or a refused duplicate.
    Immutable,
    /// A key a later owner may read or rewrite in place.
    Mutable,
}

/// One mutating request as the scope accounts for it.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    op: &'static str,
    path: Path,
    /// A put-if-absent or if-match request, whose conflict is a definite
    /// refusal and whose late landing applies at most once.
    conditional: bool,
    class: KeyClass,
    /// For a manifest put, the `_transactions/` directory whose files the
    /// manifest may name.
    transactions_dir: Option<Path>,
}

impl Request {
    pub(crate) fn new(op: &'static str, path: &Path, conditional: bool) -> Self {
        Self {
            op,
            path: path.clone(),
            conditional,
            class: key_class(path, conditional),
            transactions_dir: (op == "put")
                .then(|| manifest_transactions_dir(path))
                .flatten(),
        }
    }
}

/// Classify a key by Lance's table layout, anchored at the tail of the path
/// so a root prefix never matches. A numbered manifest is immutable only
/// through a conditional request; the version hint beside it, every ref and
/// every unknown path is mutable.
pub(crate) fn key_class(path: &Path, conditional: bool) -> KeyClass {
    let owned: Vec<String> = path.parts().map(|part| part.as_ref().to_owned()).collect();
    let parts: Vec<&str> = owned.iter().map(String::as_str).collect();
    let [.., parent, name] = parts.as_slice() else {
        return KeyClass::Mutable;
    };
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    let immutable = match *parent {
        "data" => name.ends_with(".lance"),
        "_transactions" => name.ends_with(".txn"),
        "_deletions" => name.ends_with(".arrow") || name.ends_with(".bin"),
        // Managed Blob payloads of the current Lance layout; 11.0.0 writes
        // them beside their data file instead (below).
        "_blobs" => name.ends_with(".blob"),
        "_versions" => match name.strip_suffix(".manifest") {
            Some(version) => match version.strip_prefix('d') {
                Some(detached) => digits(detached),
                None => conditional && digits(version),
            },
            None => false,
        },
        _ => false,
    };
    if immutable {
        return KeyClass::Immutable;
    }
    if let [.., grandparent, _, _] = parts.as_slice() {
        // Blob v2 sidecars: `data/<data file key>/<blob id>.blob`.
        if *grandparent == "data" && name.ends_with(".blob") {
            return KeyClass::Immutable;
        }
        // Fragment tree objects, content-addressed under `_bt/`.
        if *grandparent == "_bt"
            && matches!(
                (*parent, name.rsplit_once('.').map(|(_, ext)| ext)),
                ("root", Some("root")) | ("node", Some("node")) | ("leaf", Some("lance"))
            )
        {
            return KeyClass::Immutable;
        }
    }
    // Index files: anything below `_indices/<uuid>/`.
    let index_dir = parts[..parts.len() - 1]
        .windows(2)
        .any(|pair| pair[0] == "_indices" && is_uuid(pair[1]));
    if index_dir {
        return KeyClass::Immutable;
    }
    KeyClass::Mutable
}

/// A hyphenated UUID as Lance names index directories: `8-4-4-4-12` hex.
fn is_uuid(text: &str) -> bool {
    let groups: Vec<&str> = text.split('-').collect();
    groups.len() == 5
        && groups
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(group, len)| group.len() == len && group.bytes().all(|b| b.is_ascii_hexdigit()))
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

fn transaction_file_held_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "owned storage",
        source: std::io::Error::other(
            "a manifest put with an unknown outcome may name this transaction file; it stays until cleanup",
        )
        .into(),
    }
}

#[derive(Debug)]
pub(crate) struct Mutation {
    scope: StorageIoScope,
    settled: bool,
    /// Set for a manifest put until the store acknowledges it or refuses it
    /// definitively: on drop, the dataset's transaction files are held.
    hold_transactions: bool,
    request: Request,
}

impl Mutation {
    /// Settle on success, on a definite refusal (a conditional request's
    /// conflict, a delete's "not found"), and on any failure of an immutable
    /// key. Everything else leaves the scope uncertain when this guard
    /// drops, and is logged here because the error may be swallowed above
    /// and the later refusals name only the scope.
    pub(crate) fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        let Err(error) = &result else {
            self.settled = true;
            self.hold_transactions = false;
            return result;
        };
        let conflict = self.request.conditional
            && matches!(
                error,
                object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. }
            );
        let already_gone =
            self.request.op == "delete" && matches!(error, object_store::Error::NotFound { .. });
        self.settled = conflict || already_gone || self.request.class == KeyClass::Immutable;
        // A definite refusal is the caller's own outcome and is not logged.
        if conflict || already_gone {
            self.hold_transactions = false;
            return result;
        }
        if self.settled {
            tracing::warn!(
                op = self.request.op,
                path = %self.request.path,
                conditional = self.request.conditional,
                error = %error,
                "storage request failed on an immutable key; the outcome is definite"
            );
        } else {
            tracing::error!(
                op = self.request.op,
                path = %self.request.path,
                conditional = self.request.conditional,
                error = %error,
                "storage request outcome unknown on a mutable key; the owned storage scope is now uncertain"
            );
        }
        result
    }

    pub(crate) fn settle(mut self) {
        self.settled = true;
    }

    /// Account the second phase of a two-request mutation under its own key.
    fn with_request(mut self, request: Request) -> Self {
        self.request = request;
        self
    }
}

impl Drop for Mutation {
    fn drop(&mut self) {
        let mut state = self.scope.inner.state.lock().unwrap();
        if !self.settled {
            // A request never finished was cancelled in flight. On an
            // immutable key that is one more unreachable or refused landing;
            // anywhere else it is the uncertainty the scope exists to keep.
            if self.request.class == KeyClass::Immutable {
                tracing::warn!(
                    op = self.request.op,
                    path = %self.request.path,
                    "storage request abandoned on an immutable key; the outcome is definite"
                );
            } else {
                tracing::error!(
                    op = self.request.op,
                    path = %self.request.path,
                    "storage request abandoned on a mutable key; the owned storage scope is now uncertain"
                );
                state.uncertain = true;
            }
        }
        if self.hold_transactions
            && let Some(dir) = &self.request.transactions_dir
            && !state.held_transaction_dirs.contains(dir)
        {
            state.held_transaction_dirs.push(dir.clone());
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
        let mutation = self.scope.begin(Request::new("put", path, conditional))?;
        mutation.finish(self.inner.put_opts(path, payload, opts).await)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let request = Request::new("multipart", path, false);
        let lifetime = self.scope.begin(request.clone())?;
        match self.inner.put_multipart_opts(path, opts).await {
            Ok(inner) => Ok(Box::new(ScopedUpload {
                inner,
                lifetime: Some(lifetime),
                scope: self.scope.clone(),
                request,
            })),
            Err(error) => lifetime.finish(Err(error)),
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
                    let mutation = scope.begin(Request::new("delete", &path, false))?;
                    mutation.finish(inner.delete(&path).await)?;
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
        let mutation = self.scope.begin(Request::new("copy", to, conditional))?;
        mutation.finish(self.inner.copy_opts(from, to, opts).await)
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> Result<()> {
        if opts.target_mode == object_store::RenameTargetMode::Create {
            // The standard conditional rename is copy-Create then delete.
            // Keep its phases visible: a copy conflict is known no-effect,
            // whereas an error after a successful copy must not inherit that
            // classification. The outer owner spans both requests, even if
            // new admission closes between them.
            let mutation = self.scope.begin(Request::new("rename", to, true))?;
            let copy = CopyOptions {
                mode: object_store::CopyMode::Create,
                extensions: opts.extensions,
            };
            match self.inner.copy_opts(from, to, copy).await {
                Err(error) => mutation.finish(Err(error)),
                Ok(()) => {
                    let delete = self.inner.delete(from).await;
                    mutation
                        .with_request(Request::new("rename source delete", from, false))
                        .finish(delete)
                }
            }
        } else {
            // Preserve the backend's optimized atomic overwrite rename.
            let mutation = self.scope.begin(Request::new("rename", to, false))?;
            mutation.finish(self.inner.rename_opts(from, to, opts).await)
        }
    }
}

#[derive(Debug)]
struct ScopedUpload {
    inner: Box<dyn MultipartUpload>,
    lifetime: Option<Mutation>,
    scope: StorageIoScope,
    request: Request,
}

impl ScopedUpload {
    fn child(&self, op: &'static str) -> Request {
        Request {
            op,
            ..self.request.clone()
        }
    }
}

#[async_trait]
impl MultipartUpload for ScopedUpload {
    fn put_part(&mut self, payload: PutPayload) -> BoxFuture<'static, Result<()>> {
        if self.lifetime.is_none() {
            return Box::pin(async { Err(closed_error()) });
        }
        // Register before invoking the synchronous backend method: a backend
        // may start work there, before its returned future is first polled.
        let mutation = match self.scope.begin_child(self.child("multipart part"), false) {
            Ok(mutation) => mutation,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let part = self.inner.put_part(payload);
        Box::pin(async move { mutation.finish(part.await) })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.lifetime.is_none() {
            return Err(closed_error());
        }
        let mutation = self
            .scope
            .begin_child(self.child("multipart complete"), false)?;
        let result = mutation.finish(self.inner.complete().await);
        if result.is_ok() {
            self.lifetime.take().unwrap().settle();
        }
        result
    }

    async fn abort(&mut self) -> Result<()> {
        if self.lifetime.is_none() {
            return Err(closed_error());
        }
        let mutation = self
            .scope
            .begin_child(self.child("multipart abort"), true)?;
        let result = mutation.finish(self.inner.abort().await);
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

    /// Lance's table layout, anchored at the tail of a key: the files it
    /// writes once and the manifest versions it writes put-if-absent are
    /// immutable; refs, the version hint, control objects and unknown keys
    /// are mutable. A root prefix that happens to spell a layout name does
    /// not match.
    #[test]
    fn key_class_follows_lance_table_layout_issue_1067() {
        let immutable = [
            ("g/node_Person/data/3f2c4d9e-1a2b.lance", false),
            ("g/node_Person/data/3f2c4d9e-1a2b/7.blob", false),
            (
                "g/node_Person/_transactions/12770445047132701458-0c41-c45b.txn",
                false,
            ),
            ("g/node_Person/_deletions/4-1733.arrow", false),
            ("g/node_Person/_deletions/4-1733.bin", false),
            (
                "g/node_Person/_indices/1b4e28ba-2fa1-11d2-883f-0016d3cca427/index.idx",
                false,
            ),
            (
                "g/node_Person/_indices/1b4e28ba-2fa1-11d2-883f-0016d3cca427/part/0.idx",
                false,
            ),
            (
                "g/node_Person/_versions/d12770445047132701458.manifest",
                false,
            ),
            ("g/__manifest/_versions/18446744073709551602.manifest", true),
            ("g/__manifest/_versions/14.manifest", true),
        ];
        for (key, conditional) in immutable {
            assert_eq!(
                key_class(&Path::from(key), conditional),
                KeyClass::Immutable,
                "{key}"
            );
        }
        for key in [
            "g/node_Person/_bt/root/1b4e28ba-2fa1-11d2-883f-0016d3cca427.root",
            "g/node_Person/_bt/node/1b4e28ba-2fa1-11d2-883f-0016d3cca427.node",
            "g/node_Person/_bt/leaf/1b4e28ba-2fa1-11d2-883f-0016d3cca427.lance",
            "g/node_Person/_blobs/1b4e28ba-2fa1-11d2-883f-0016d3cca427.blob",
        ] {
            assert_eq!(
                key_class(&Path::from(key), false),
                KeyClass::Immutable,
                "{key}"
            );
        }
        let mutable = [
            ("g/__manifest/_versions/14.manifest", false),
            ("g/__manifest/_versions/latest_version_hint.json", false),
            ("g/__manifest/_refs/branches/main", true),
            ("g/__manifest/_refs/tags/unpublished_01J.json", false),
            ("g/__cluster/lock.json", true),
            ("g/__cluster/state.json", true),
            ("g/__graph_index/csr-current.bin", false),
            ("data/graphs/g/node_Person/schema.pg", false),
            ("data/x.lance/state.json", false),
            ("g/node_Person/_indices/not-a-uuid/index.idx", false),
            (
                "g/node_Person/_indices/zzzzzzzz-2fa1-11d2-883f-0016d3cca427/index.idx",
                false,
            ),
            (
                "g/node_Person/_bt/other/1b4e28ba-2fa1-11d2-883f-0016d3cca427.root",
                false,
            ),
            ("g/node_Person/_versions/dx.manifest", false),
            ("g/node_Person/_versions/d.manifest", false),
            ("lance", false),
        ];
        for (key, conditional) in mutable {
            assert_eq!(
                key_class(&Path::from(key), conditional),
                KeyClass::Mutable,
                "{key}"
            );
        }
    }

    /// A lost acknowledgement on an immutable key fails the request but
    /// leaves the scope settled: a late landing is unreachable garbage or a
    /// refused duplicate, so later mutations are admitted and a clean release
    /// still holds. The same loss on a mutable key still poisons.
    #[tokio::test]
    async fn lost_ack_on_an_immutable_key_fails_without_poison_issue_1067() {
        let immutable_puts = [
            (
                "g/node_Person/data/3f2c4d9e.lance",
                object_store::PutMode::Overwrite,
            ),
            (
                "g/node_Person/_transactions/12770445047132701458-0c41.txn",
                object_store::PutMode::Overwrite,
            ),
            (
                "g/node_Person/_versions/d12770445047132701458.manifest",
                object_store::PutMode::Create,
            ),
            (
                "g/__manifest/_versions/18446744073709551602.manifest",
                object_store::PutMode::Create,
            ),
        ];
        for (key, mode) in immutable_puts {
            let (scope, store, _) = fixture(Point::Put, true).await;
            let error = store
                .put_opts(&Path::from(key), "bytes".into(), mode.into())
                .await
                .expect_err("the lost acknowledgement is still the caller's error");
            assert!(error.to_string().contains("acknowledgement lost"), "{key}");
            scope.wait_idle().await;
            assert!(!scope.is_uncertain(), "{key} must not poison the scope");
            // The fixture loses every put's acknowledgement; what matters is
            // that the scope still admits the request instead of refusing it.
            let later = store
                .put(
                    &Path::from("g/node_Person/data/later.lance"),
                    "later".into(),
                )
                .await
                .unwrap_err();
            assert!(
                later.to_string().contains("acknowledgement lost"),
                "{key}: later mutations stay admitted: {later}"
            );
            scope.close();
            scope.wait_idle().await;
            assert!(!scope.is_uncertain(), "{key}: clean release still holds");
        }
        for key in [
            "g/node_Person/data/3f2c4d9e.lance",
            "g/node_Person/_versions/d12770445047132701458.manifest",
        ] {
            let (scope, store, _) = fixture(Point::Delete, true).await;
            store.put(&Path::from(key), "bytes".into()).await.unwrap();
            assert!(store.delete(&Path::from(key)).await.is_err());
            scope.wait_idle().await;
            assert!(!scope.is_uncertain(), "a lost delete of {key} settles");
        }
        {
            let (scope, store, _) = fixture(Point::Part, true).await;
            let mut upload = store
                .put_multipart(&Path::from("g/node_Person/data/3f2c4d9e.lance"))
                .await
                .unwrap();
            assert!(upload.put_part("data".into()).await.is_err());
            assert!(!scope.is_uncertain(), "a lost data-file part settles");
            upload.abort().await.unwrap();
            scope.wait_idle().await;
            assert!(!scope.is_uncertain());
        }
        for (key, mode) in [
            (
                "g/__manifest/_versions/14.manifest",
                object_store::PutMode::Overwrite,
            ),
            (
                "g/__manifest/_versions/latest_version_hint.json",
                object_store::PutMode::Overwrite,
            ),
            (
                "g/__manifest/_refs/branches/main",
                object_store::PutMode::Create,
            ),
            ("g/__cluster/lock.json", object_store::PutMode::Create),
        ] {
            let (scope, store, _) = fixture(Point::Put, true).await;
            assert!(
                store
                    .put_opts(&Path::from(key), "bytes".into(), mode.into())
                    .await
                    .is_err()
            );
            scope.wait_idle().await;
            assert!(
                scope.is_uncertain(),
                "a lost acknowledgement on {key} poisons"
            );
            let refused = store
                .put(
                    &Path::from("g/node_Person/data/later.lance"),
                    "later".into(),
                )
                .await
                .unwrap_err();
            assert!(
                refused.to_string().contains("uncertain outcome"),
                "{key}: the poisoned scope admits nothing: {refused}"
            );
        }
        let (scope, store, _) = fixture(Point::Delete, true).await;
        store
            .put(&Path::from("g/__manifest/_refs/tags/t.json"), "tag".into())
            .await
            .unwrap();
        assert!(
            store
                .delete(&Path::from("g/__manifest/_refs/tags/t.json"))
                .await
                .is_err()
        );
        scope.wait_idle().await;
        assert!(scope.is_uncertain(), "a lost ref delete poisons");
    }

    /// After a manifest put the store did not acknowledge, the dataset's
    /// transaction files cannot be deleted through the scope: Lance's own
    /// cleanup is refused with a settled error and the file stays for a late
    /// manifest to name. A definite conflict on the put holds nothing, and
    /// another dataset's transaction files are untouched.
    #[tokio::test]
    async fn unacknowledged_manifest_put_holds_the_transaction_file_issue_1067() {
        let manifest = Path::from("g/node_Person/_versions/d12770445047132701458.manifest");
        let transaction = Path::from("g/node_Person/_transactions/12770445047132701457-0c41.txn");
        let other = Path::from("g/node_Knows/_transactions/7-abcd.txn");
        for mode in [
            object_store::PutMode::Create,
            object_store::PutMode::Overwrite,
        ] {
            let (scope, store, _) = fixture(Point::Put, true).await;
            // The fixture loses every put's acknowledgement; the objects exist.
            assert!(store.put(&transaction, "txn".into()).await.is_err());
            assert!(store.put(&other, "txn".into()).await.is_err());
            assert!(
                store
                    .put_opts(&manifest, "manifest".into(), mode.clone().into())
                    .await
                    .is_err()
            );
            assert!(!scope.is_uncertain(), "{mode:?}: the manifest put settles");
            let refused = store.delete(&transaction).await.unwrap_err();
            assert!(
                refused.to_string().contains("transaction file"),
                "{mode:?}: {refused}"
            );
            assert!(
                store.get(&transaction).await.is_ok(),
                "{mode:?}: the file stays"
            );
            store
                .delete(&other)
                .await
                .expect("another dataset's transaction files are not held");
            assert!(
                !scope.is_uncertain(),
                "{mode:?}: a held delete is a settled refusal"
            );
            scope.close();
            scope.wait_idle().await;
            assert!(!scope.is_uncertain(), "{mode:?}: clean release still holds");
        }

        let scope = StorageIoScope::new();
        let store = scope.wrap_object_store(Arc::new(InMemory::new()));
        store.put(&transaction, "txn".into()).await.unwrap();
        store
            .put_opts(
                &manifest,
                "first".into(),
                object_store::PutMode::Create.into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            store
                .put_opts(
                    &manifest,
                    "second".into(),
                    object_store::PutMode::Create.into()
                )
                .await,
            Err(object_store::Error::AlreadyExists { .. })
        ));
        store
            .delete(&transaction)
            .await
            .expect("a definite conflict holds nothing");
        assert!(!scope.is_uncertain());
    }

    /// A multipart upload to an immutable key settles whatever its `complete`,
    /// its `abort` or an abandonment does: the object exists whole under its
    /// fresh name or never does. The same losses on a mutable key keep
    /// poisoning, as the other multipart tests pin.
    #[tokio::test]
    async fn multipart_upload_to_an_immutable_key_settles_on_every_outcome_issue_1067() {
        let path = Path::from("g/node_Person/data/3f2c4d9e.lance");
        {
            let (scope, store, _) = fixture(Point::Complete, true).await;
            let mut upload = store.put_multipart(&path).await.unwrap();
            upload.put_part("data".into()).await.unwrap();
            let lost = upload.complete().await.unwrap_err();
            assert!(lost.to_string().contains("acknowledgement lost"), "{lost}");
            drop(upload);
            scope.wait_idle().await;
            assert!(!scope.is_uncertain(), "a lost complete settles");
            assert_eq!(
                store.get(&path).await.unwrap().bytes().await.unwrap(),
                "data",
                "the object the store completed is whole"
            );
        }
        {
            let (scope, store, _) = fixture(Point::Abort, true).await;
            let mut upload = store.put_multipart(&path).await.unwrap();
            upload.put_part("data".into()).await.unwrap();
            assert!(upload.abort().await.is_err());
            drop(upload);
            scope.wait_idle().await;
            assert!(!scope.is_uncertain(), "a lost abort settles");
        }
        {
            let scope = StorageIoScope::new();
            let store = scope.wrap_object_store(Arc::new(InMemory::new()));
            let mut upload = store.put_multipart(&path).await.unwrap();
            upload.put_part("data".into()).await.unwrap();
            drop(upload);
            scope.wait_idle().await;
            assert_eq!(scope.pending(), 0);
            assert!(!scope.is_uncertain(), "an abandoned upload settles");
        }
        {
            // A request cancelled in flight on an immutable key settles too.
            let (scope, store, fault) = fixture(Point::Put, false).await;
            let task = tokio::spawn(async move { store.put(&path, "data".into()).await });
            tokio::time::timeout(Duration::from_secs(2), fault.entered.notified())
                .await
                .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            scope.wait_idle().await;
            assert!(!scope.is_uncertain(), "a cancelled immutable put settles");
        }
    }

    /// A delete the store answers "not found" is definite on any key, and a
    /// local root answers it for a missing file.
    #[tokio::test]
    async fn not_found_delete_is_definite_on_a_mutable_key_issue_1067() {
        let root = tempfile::tempdir().unwrap();
        let scope = StorageIoScope::new();
        let handle =
            crate::storage_handle_for_uri_scoped(root.path().to_str().unwrap(), scope.clone())
                .unwrap();
        let adapter = handle.adapter();
        let missing = format!("{}/__cluster/missing.json", root.path().display());
        let outcome = adapter.delete(&missing).await;
        scope.wait_idle().await;
        assert!(!scope.is_uncertain(), "{outcome:?}");
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
