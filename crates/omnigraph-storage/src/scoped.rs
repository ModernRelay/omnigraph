//! Explicit accounting for one owner's storage mutations.
//!
//! This is process-local settlement evidence, never a writer fence. Every
//! wrapped remote client must disable retries before construction: a successful
//! retry cannot establish what happened to an earlier, unacknowledged request.
//!
//! Settlement depends on the request's operation and the Lance 11 key it names:
//!
//! - Lance writes fresh table files (`data/*.lance`, their Blob sidecars,
//!   `_transactions/*.txn`, `_deletions/*.arrow|*.bin`, flat files in
//!   `_indices/<uuid>/`, detached `_versions/d*.manifest`) and creates numbered
//!   manifest versions only if absent. Late fresh files remain unreachable;
//!   a conditional create cannot replace an existing publication. These
//!   requests settle at the physical scope even after an error or cancellation.
//!   This is not evidence of graph non-publication: the catalogue still owns
//!   the result of an unacknowledged numbered-manifest publication, and its
//!   uncertainty must reach the logical owner independently of this scope.
//! - Other keys (refs, version hints, control objects and unknown paths),
//!   if-match replacements and recursive deletes stay conservative. An
//!   unacknowledged request leaves its key unsettled even if its caller
//!   consumes the error: a later request to that key, which the earlier one
//!   could still overwrite by landing late, is refused, and the scope refuses
//!   a clean release. Other keys are unaffected. Rename accounts for its
//!   source deletion too.
//! - A manifest write the store did not acknowledge may still land after
//!   Lance reads it back as absent and tries to delete its transaction file.
//!   One sticky flag therefore refuses transaction-file deletion throughout
//!   this scope, including rename source deletion and recursive removal. This
//!   deliberately over-retains other datasets' transaction files without an
//!   unbounded path list. A replacement scope may reclaim them only through
//!   the collector's normal reachability, authority and age checks. Definite
//!   conditional conflicts hold nothing.
//! - Multipart uploads to fresh immutable keys settle even if completion or
//!   abort fails or the handle is abandoned: the object exists whole or not at
//!   all. Leftover parts are the bucket's to expire. Unacknowledged manifest
//!   completion retains transaction files by the same rule above.
//!
//! Requests are routed by class. Reads and unconditional writes of fresh
//! immutable keys go to a retrying client: a retry re-sends the same bytes
//! to a name only this request uses, so its late copies are the ones a lost
//! acknowledgement already allows. Conditional requests and mutable keys go
//! to a single-attempt client: a conditional request is resolved by its
//! owner's re-issue and read-back, which needs every attempt to be its own,
//! and a mutable key is the class whose late landing matters. A multipart
//! upload follows its key's class, and its parts, completion and abort follow
//! the upload.
//!
//! [`StorageIoScope::fence`] is stronger than closing. It is for an owner
//! that has lost its right to write, when a successor may act: it refuses
//! every later request, including the parts, completion and abort of an
//! admitted multipart upload, and it cancels every admitted request still
//! running. A cancelled request's future is dropped, so its client sends no
//! further attempt and no retry. A request already sent may still be
//! applied by the store, as after a crash, and its caller sees an ambiguous
//! error rather than this scope's definite refusal.

use std::collections::BTreeSet;
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
/// Failed or abandoned requests to mutable keys make the scope uncertain. Closing
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
    /// Wakes every admitted request when the scope is fenced.
    fence: Notify,
}

#[derive(Debug, Default)]
struct ScopeState {
    pending: usize,
    closed: bool,
    /// The owner lost its right to write: nothing more is sent.
    fenced: bool,
    /// Mutable keys whose last request has an unknown outcome. Bounded by
    /// [`MAX_UNSETTLED_KEYS`]; past the bound `unsettled_overflow` refuses
    /// every mutation rather than growing.
    unsettled: BTreeSet<Path>,
    unsettled_overflow: bool,
    /// An unacknowledged manifest write may still land. Retain transaction
    /// files for this scope's lifetime with bounded, conservative state.
    hold_transaction_deletes: bool,
}

/// How many unsettled mutable keys one scope remembers. A store that leaves
/// this many mutable requests unanswered is failing; past the bound the
/// scope refuses every mutation, and reads continue.
pub const MAX_UNSETTLED_KEYS: usize = 4096;

impl ScopeState {
    /// The first unsettled key `request` touches: its own key, or for a
    /// recursive delete any key under its prefix.
    fn unsettled_conflict(&self, request: &Request) -> bool {
        if request.op == "delete_prefix" {
            let prefix = request.path.as_ref();
            return self.unsettled.iter().any(|key| {
                key.as_ref() == prefix || key.as_ref().starts_with(&format!("{prefix}/"))
            });
        }
        self.unsettled.contains(&request.path)
    }

    fn unsettle(&mut self, path: &Path) {
        if self.unsettled.len() < MAX_UNSETTLED_KEYS {
            self.unsettled.insert(path.clone());
        } else if !self.unsettled.contains(path) {
            self.unsettled_overflow = true;
        }
    }
}

impl StorageIoScope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether some mutable key's last request has an unknown outcome. A
    /// clean release requires this to be false; serving does not.
    pub fn is_uncertain(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
        !state.unsettled.is_empty() || state.unsettled_overflow
    }

    /// How many mutable keys this scope holds unsettled.
    pub fn unsettled_keys(&self) -> usize {
        self.inner.state.lock().unwrap().unsettled.len()
    }

    pub fn pending(&self) -> usize {
        self.inner.state.lock().unwrap().pending
    }

    /// Irreversibly refuse new mutation admission.
    pub fn close(&self) {
        self.inner.state.lock().unwrap().closed = true;
    }

    /// Irreversibly stop sending: refuse every later request, including the
    /// parts, completion and abort of an admitted multipart upload, and
    /// cancel every admitted request still running (see the module
    /// documentation). For an owner whose lease fenced, so that it sends
    /// nothing after a successor may act, however long the process lives.
    pub fn fence(&self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.closed = true;
            state.fenced = true;
        }
        self.inner.fence.notify_waiters();
    }

    pub fn is_fenced(&self) -> bool {
        self.inner.state.lock().unwrap().fenced
    }

    /// Resolves once the scope is fenced.
    async fn fenced(&self) {
        loop {
            let fence = self.inner.fence.notified();
            tokio::pin!(fence);
            fence.as_mut().enable();
            if self.is_fenced() {
                return;
            }
            fence.await;
        }
    }

    /// Run an admitted request until it answers or the scope is fenced. A
    /// fence drops the request's future, so its client sends nothing more.
    async fn until_fenced<T>(
        &self,
        request: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::select! {
            biased;
            result = request => result,
            () = self.fenced() => Err(fenced_in_flight_error()),
        }
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

    /// Wrap a client whose mutating requests perform at most one attempt,
    /// for every request class.
    ///
    /// The caller must configure that client (and any outer retry layer) before
    /// wrapping it.
    pub fn wrap_object_store(&self, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        self.wrap_object_stores(Arc::clone(&inner), inner)
    }

    /// Wrap two clients of one store: `retrying` serves reads and fresh
    /// immutable writes with its own bounded retries; `single` performs at
    /// most one attempt per request and serves conditional requests and
    /// mutable keys.
    pub fn wrap_object_stores(
        &self,
        retrying: Arc<dyn ObjectStore>,
        single: Arc<dyn ObjectStore>,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(ScopedObjectStore {
            inner: retrying,
            single,
            scope: self.clone(),
        })
    }

    pub(crate) fn begin(&self, request: Request) -> Result<Mutation> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(closed_error());
        }
        if state.unsettled_overflow {
            return Err(unsettled_overflow_error());
        }
        if state.unsettled_conflict(&request) {
            return Err(unsettled_error(&request.path));
        }
        if state.hold_transaction_deletes && request.deletes_transactions() {
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
            hold_transactions: request.writes_manifest,
            request,
        })
    }

    fn begin_child(&self, request: Request, cleanup: bool) -> Result<Mutation> {
        // Only an admitted multipart lifetime can call this. Its outstanding
        // guard prevents an idle observation before the child is registered.
        let mut state = self.inner.state.lock().unwrap();
        // A close lets an admitted upload finish or abort; a fence sends
        // nothing more, its cleanup included. Leftover parts are the
        // bucket's to expire.
        if state.fenced {
            return Err(fenced_error());
        }
        if !cleanup && state.unsettled_overflow {
            return Err(unsettled_overflow_error());
        }
        if !cleanup && state.unsettled_conflict(&request) {
            return Err(unsettled_error(&request.path));
        }
        state.pending += 1;
        Ok(Mutation {
            scope: self.clone(),
            settled: false,
            // Completion can publish before returning or being cancelled.
            // A later successful abort cannot erase that earlier evidence.
            hold_transactions: request.writes_manifest && request.op == "multipart complete",
            request,
        })
    }
}

/// Match a file in a known Lance directory at the tail of its key.
fn file_in_directory(path: &Path, directory: &str, suffix: &str) -> bool {
    let parts: Vec<_> = path.parts().collect();
    matches!(parts.as_slice(), [.., parent, name]
        if parent.as_ref() == directory && name.as_ref().ends_with(suffix))
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
    /// This request may write a manifest which names a transaction file.
    writes_manifest: bool,
}

impl Request {
    pub(crate) fn new(op: &'static str, path: &Path, conditional: bool) -> Self {
        Self {
            op,
            path: path.clone(),
            conditional,
            class: if op == "delete_prefix" {
                // A directory name never proves its descendants immutable.
                KeyClass::Mutable
            } else {
                key_class(path, conditional)
            },
            writes_manifest: matches!(op, "put" | "copy" | "rename" | "multipart")
                && file_in_directory(path, "_versions", ".manifest"),
        }
    }

    fn deletes_transactions(&self) -> bool {
        self.op == "delete_prefix"
            || (matches!(self.op, "delete" | "rename source delete")
                && file_in_directory(&self.path, "_transactions", ".txn"))
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
        // Lance 11 writes each index's files directly in its UUID directory.
        // Unknown nested paths stay mutable: a root prefix can itself contain
        // `_indices/<uuid>/` before an unrelated control object.
        if *grandparent == "_indices" && is_uuid(parent) {
            return KeyClass::Immutable;
        }
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

/// The `store` of every error the scope returns for a request it refuses
/// before sending it. A caller may read such an error as definite: no copy of
/// the request can land.
pub const OWNED_STORE_NAME: &str = "owned storage";

fn closed_error() -> object_store::Error {
    object_store::Error::Generic {
        store: OWNED_STORE_NAME,
        source: std::io::Error::other("storage mutation admission is closed").into(),
    }
}

fn fenced_error() -> object_store::Error {
    object_store::Error::Generic {
        store: OWNED_STORE_NAME,
        source: std::io::Error::other("storage mutation is fenced; nothing more is sent").into(),
    }
}

/// The `store` of the error a request cancelled by a fence returns. Not
/// [`OWNED_STORE_NAME`]: the request may already have been sent, so its
/// outcome is unknown.
const FENCED_REQUEST_STORE_NAME: &str = "fenced storage request";

fn fenced_in_flight_error() -> object_store::Error {
    object_store::Error::Generic {
        store: FENCED_REQUEST_STORE_NAME,
        source: std::io::Error::other(
            "storage mutation was fenced while this request was in flight; it may still land",
        )
        .into(),
    }
}

fn unsettled_overflow_error() -> object_store::Error {
    object_store::Error::Generic {
        store: OWNED_STORE_NAME,
        source: std::io::Error::other(format!(
            "more than {MAX_UNSETTLED_KEYS} keys in this storage scope have requests with unknown \
             outcomes; it refuses every mutation until a replacement scope"
        ))
        .into(),
    }
}

fn unsettled_error(path: &Path) -> object_store::Error {
    object_store::Error::Generic {
        store: OWNED_STORE_NAME,
        source: std::io::Error::other(format!(
            "a prior request to '{path}' has an unknown outcome and could still land; \
             this key stays refused until a replacement scope"
        ))
        .into(),
    }
}

fn transaction_file_held_error() -> object_store::Error {
    object_store::Error::Generic {
        store: OWNED_STORE_NAME,
        source: std::io::Error::other(
            "a manifest write with an unknown outcome may name this transaction file; it stays until cleanup from a replacement scope",
        )
        .into(),
    }
}

#[derive(Debug)]
pub(crate) struct Mutation {
    scope: StorageIoScope,
    settled: bool,
    /// Set for a manifest write until the store acknowledges it or refuses it
    /// definitively: on drop, this scope holds transaction-file deletion.
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
        let already_gone = matches!(self.request.op, "delete" | "rename source delete")
            && matches!(error, object_store::Error::NotFound { .. });
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
                "storage request failed on an immutable key; the physical scope remains settled"
            );
        } else {
            tracing::error!(
                op = self.request.op,
                path = %self.request.path,
                conditional = self.request.conditional,
                error = %error,
                "storage request outcome unknown on a mutable key; this key is now unsettled"
            );
        }
        result
    }

    pub(crate) fn settle(mut self) {
        self.settled = true;
        self.hold_transactions = false;
    }

    /// The previous request was acknowledged. Account the next request before
    /// it can be polled, even when new admission closed between the phases.
    fn with_request(mut self, request: Request) -> Result<Self> {
        self.settled = true;
        self.hold_transactions = false;
        if self
            .scope
            .inner
            .state
            .lock()
            .unwrap()
            .hold_transaction_deletes
            && request.deletes_transactions()
        {
            return Err(transaction_file_held_error());
        }
        self.hold_transactions = request.writes_manifest;
        self.request = request;
        self.settled = false;
        Ok(self)
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
                    "storage request abandoned on an immutable key; the physical scope remains settled"
                );
            } else {
                tracing::error!(
                    op = self.request.op,
                    path = %self.request.path,
                    "storage request abandoned on a mutable key; this key is now unsettled"
                );
                state.unsettle(&self.request.path);
            }
        }
        state.hold_transaction_deletes |= self.hold_transactions;
        state.pending -= 1;
        drop(state);
        self.scope.inner.changed.notify_waiters();
    }
}

#[derive(Debug)]
struct ScopedObjectStore {
    /// Reads and fresh immutable writes; retries within its own budget.
    inner: Arc<dyn ObjectStore>,
    /// Conditional requests and mutable keys; one attempt per request.
    single: Arc<dyn ObjectStore>,
    scope: StorageIoScope,
}

impl ScopedObjectStore {
    /// The client a request goes to (see the module documentation).
    fn client(&self, request: &Request) -> &Arc<dyn ObjectStore> {
        if request.conditional || request.class == KeyClass::Mutable {
            &self.single
        } else {
            &self.inner
        }
    }
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
        let mut request = Request::new("put", path, conditional);
        if matches!(opts.mode, object_store::PutMode::Update(_)) {
            // If-match can replace existing bytes, including a publication
            // key. Only Create has the numbered-manifest exemption.
            request.class = KeyClass::Mutable;
        }
        let client = Arc::clone(self.client(&request));
        let mutation = self.scope.begin(request)?;
        mutation.finish(
            self.scope
                .until_fenced(client.put_opts(path, payload, opts))
                .await,
        )
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let request = Request::new("multipart", path, false);
        let client = Arc::clone(self.client(&request));
        let lifetime = self.scope.begin(request.clone())?;
        match self
            .scope
            .until_fenced(client.put_multipart_opts(path, opts))
            .await
        {
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
        let retrying = self.inner.clone();
        let single = self.single.clone();
        let scope = self.scope.clone();
        // Track each one-key request to its acknowledgement. In particular,
        // ObjectStoreExt::delete consumes one item then drops this stream.
        locations
            .map(move |path| {
                let retrying = retrying.clone();
                let single = single.clone();
                let scope = scope.clone();
                async move {
                    let path = path?;
                    let request = Request::new("delete", &path, false);
                    let client = match request.class {
                        KeyClass::Immutable => retrying,
                        KeyClass::Mutable => single,
                    };
                    let mutation = scope.begin(request)?;
                    mutation.finish(scope.until_fenced(client.delete(&path)).await)?;
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
        let request = Request::new("copy", to, conditional);
        let client = Arc::clone(self.client(&request));
        let mutation = self.scope.begin(request)?;
        mutation.finish(
            self.scope
                .until_fenced(client.copy_opts(from, to, opts))
                .await,
        )
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> Result<()> {
        if self
            .scope
            .inner
            .state
            .lock()
            .unwrap()
            .hold_transaction_deletes
            && file_in_directory(from, "_transactions", ".txn")
        {
            return Err(transaction_file_held_error());
        }
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
            match self
                .scope
                .until_fenced(self.single.copy_opts(from, to, copy))
                .await
            {
                Err(error) => mutation.finish(Err(error)),
                Ok(()) => {
                    if self.scope.is_fenced() {
                        // The copy landed, so this is no refusal; the source
                        // delete is never sent.
                        return mutation.finish(Err(fenced_in_flight_error()));
                    }
                    let mutation =
                        mutation.with_request(Request::new("rename source delete", from, false))?;
                    mutation.finish(self.scope.until_fenced(self.single.delete(from)).await)
                }
            }
        } else {
            // Preserve the backend's optimized atomic overwrite rename.
            let mut request = Request::new("rename", to, false);
            if key_class(from, false) == KeyClass::Mutable {
                // An atomic rename changes both keys; a safe destination
                // cannot establish settlement of a mutable source removal.
                request.class = KeyClass::Mutable;
            }
            let client = Arc::clone(self.client(&request));
            let mutation = self.scope.begin(request)?;
            mutation.finish(
                self.scope
                    .until_fenced(client.rename_opts(from, to, opts))
                    .await,
            )
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
        let scope = self.scope.clone();
        Box::pin(async move { mutation.finish(scope.until_fenced(part).await) })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.lifetime.is_none() {
            return Err(closed_error());
        }
        let mutation = self
            .scope
            .begin_child(self.child("multipart complete"), false)?;
        let result = mutation.finish(self.scope.until_fenced(self.inner.complete()).await);
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
        let result = mutation.finish(self.scope.until_fenced(self.inner.abort()).await);
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
        let (scope, store, fault, _) = fixture_with_inner(point, lost_ack).await;
        (scope, store, fault)
    }

    async fn fixture_with_inner(
        point: Point,
        lost_ack: bool,
    ) -> (
        StorageIoScope,
        Arc<dyn ObjectStore>,
        Arc<Fault>,
        Arc<InMemory>,
    ) {
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
            inner: inner.clone(),
            fault: fault.clone(),
        }));
        (scope, store, fault, inner)
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
                "lost {point:?} acknowledgement must leave its key unsettled"
            );
            // A higher layer cannot turn its retry into apparent success: a
            // later request to the same key is refused before it is sent.
            let key = Path::from(if point == Point::Delete {
                "source"
            } else {
                "target"
            });
            let before = store.get(&key).await.is_ok();
            let refused = store.put(&key, "retried".into()).await.unwrap_err();
            assert!(
                refused.to_string().contains("unknown outcome"),
                "{point:?}: {refused}"
            );
            assert_eq!(
                store.get(&key).await.is_ok(),
                before,
                "{point:?}: the refused retry wrote nothing"
            );
        }
    }

    /// An unsettled mutable key refuses later requests to itself, including
    /// a recursive delete over it, and nothing else: other mutable keys,
    /// immutable keys and reads stay admitted. A clean release still refuses.
    #[tokio::test]
    async fn an_unsettled_key_refuses_only_itself_issue_1067() {
        let (scope, store, _) = fixture(Point::Delete, true).await;
        let tag = Path::from("g/__manifest/_refs/tags/t.json");
        store.put(&tag, "tag".into()).await.unwrap();
        assert!(
            store.delete(&tag).await.is_err(),
            "the delete's answer is lost"
        );
        assert!(scope.is_uncertain());
        assert_eq!(scope.unsettled_keys(), 1);

        let refused = store.put(&tag, "recreated".into()).await.unwrap_err();
        assert!(refused.to_string().contains("unknown outcome"), "{refused}");
        let deleted = store
            .delete_stream(futures::stream::iter(vec![Ok(tag.clone())]).boxed())
            .next()
            .await
            .unwrap();
        assert!(deleted.is_err(), "a delete of the same key is refused too");

        let other = Path::from("g/__manifest/_refs/tags/u.json");
        store
            .put(&other, "another key".into())
            .await
            .expect("another mutable key is admitted");
        store
            .put(&Path::from("g/node_Person/data/f.lance"), "data".into())
            .await
            .expect("an immutable key is admitted");
        assert!(store.get(&other).await.is_ok(), "reads continue");
        scope.close();
        scope.wait_idle().await;
        assert!(scope.is_uncertain(), "a clean release still refuses");
    }

    /// Counts the requests one client of a store serves.
    #[derive(Debug)]
    struct Counted {
        inner: Arc<InMemory>,
        served: Arc<Mutex<Vec<String>>>,
        name: &'static str,
    }

    impl fmt::Display for Counted {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.name)
        }
    }

    impl Counted {
        fn note(&self, op: &str, path: &Path) {
            self.served
                .lock()
                .unwrap()
                .push(format!("{} {op} {path}", self.name));
        }
    }

    #[async_trait]
    impl ObjectStore for Counted {
        async fn put_opts(&self, p: &Path, d: PutPayload, o: PutOptions) -> Result<PutResult> {
            self.note("put", p);
            self.inner.put_opts(p, d, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &Path,
            o: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.note("multipart", p);
            self.inner.put_multipart_opts(p, o).await
        }
        async fn get_opts(&self, p: &Path, o: GetOptions) -> Result<GetResult> {
            self.note("get", p);
            self.inner.get_opts(p, o).await
        }
        fn delete_stream(
            &self,
            paths: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            let served = Arc::clone(&self.served);
            let name = self.name;
            self.inner.delete_stream(
                paths
                    .inspect(move |path| {
                        if let Ok(path) = path {
                            served.lock().unwrap().push(format!("{name} delete {path}"));
                        }
                    })
                    .boxed(),
            )
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, o: CopyOptions) -> Result<()> {
            self.note("copy", to);
            self.inner.copy_opts(from, to, o).await
        }
    }

    /// Reads and fresh immutable writes go to the retrying client; a
    /// conditional request and every request to a mutable key go to the
    /// single-attempt client, whose every attempt the owner sees.
    #[tokio::test]
    async fn requests_route_by_class_issue_1067() {
        let inner = Arc::new(InMemory::new());
        let served = Arc::new(Mutex::new(Vec::new()));
        let client = |name| {
            Arc::new(Counted {
                inner: Arc::clone(&inner),
                served: Arc::clone(&served),
                name,
            }) as Arc<dyn ObjectStore>
        };
        let scope = StorageIoScope::new();
        let store = scope.wrap_object_stores(client("retrying"), client("single"));
        let data = Path::from("g/node_Person/data/f.lance");
        let txn = Path::from("g/node_Person/_transactions/1-0c41.txn");
        let manifest = Path::from("g/__manifest/_versions/2.manifest");
        let lock = Path::from("g/__cluster/lock.json");
        let tag = Path::from("g/__manifest/_refs/tags/t.json");
        store.put(&data, "data".into()).await.unwrap();
        store.put(&txn, "txn".into()).await.unwrap();
        store
            .put_opts(
                &manifest,
                "manifest".into(),
                object_store::PutMode::Create.into(),
            )
            .await
            .unwrap();
        store.put(&lock, "lock".into()).await.unwrap();
        store.put(&tag, "tag".into()).await.unwrap();
        store.get(&lock).await.unwrap();
        store.delete(&txn).await.unwrap();
        store.delete(&tag).await.unwrap();
        assert_eq!(
            *served.lock().unwrap(),
            [
                "retrying put g/node_Person/data/f.lance",
                "retrying put g/node_Person/_transactions/1-0c41.txn",
                "single put g/__manifest/_versions/2.manifest",
                "single put g/__cluster/lock.json",
                "single put g/__manifest/_refs/tags/t.json",
                "retrying get g/__cluster/lock.json",
                "retrying delete g/node_Person/_transactions/1-0c41.txn",
                "single delete g/__manifest/_refs/tags/t.json",
            ]
        );
    }

    /// The scope remembers a bounded number of unsettled keys. Past the bound
    /// it refuses every mutation rather than grow, and reads continue.
    #[tokio::test]
    async fn unsettled_keys_are_bounded_issue_1067() {
        let (scope, store, _) = fixture(Point::Put, true).await;
        for n in 0..MAX_UNSETTLED_KEYS {
            let key = Path::from(format!("g/__cluster/key-{n}.json"));
            assert!(store.put(&key, "x".into()).await.is_err());
        }
        assert_eq!(scope.unsettled_keys(), MAX_UNSETTLED_KEYS);
        let fresh = Path::from("g/node_Person/data/fresh.lance");
        let admitted = store.put(&fresh, "x".into()).await.unwrap_err();
        assert!(
            admitted.to_string().contains("acknowledgement lost"),
            "below the bound other keys are admitted: {admitted}"
        );
        assert!(
            store
                .put(&Path::from("g/__cluster/one-more.json"), "x".into())
                .await
                .is_err()
        );
        let refused = store
            .put(&Path::from("g/node_Person/data/next.lance"), "x".into())
            .await
            .unwrap_err();
        assert!(
            refused.to_string().contains("unknown outcomes"),
            "past the bound every mutation is refused: {refused}"
        );
        assert!(store.get(&fresh).await.is_ok(), "reads continue");
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
        let mutable = [
            (
                "g/node_Person/_bt/root/1b4e28ba-2fa1-11d2-883f-0016d3cca427.root",
                false,
            ),
            (
                "g/node_Person/_bt/node/1b4e28ba-2fa1-11d2-883f-0016d3cca427.node",
                false,
            ),
            (
                "g/node_Person/_bt/leaf/1b4e28ba-2fa1-11d2-883f-0016d3cca427.lance",
                false,
            ),
            (
                "g/node_Person/_blobs/1b4e28ba-2fa1-11d2-883f-0016d3cca427.blob",
                false,
            ),
            (
                "g/node_Person/_indices/1b4e28ba-2fa1-11d2-883f-0016d3cca427/part/0.idx",
                false,
            ),
            (
                "_indices/1b4e28ba-2fa1-11d2-883f-0016d3cca427/cluster/__cluster/lock.json",
                true,
            ),
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
            (
                "_indices/1b4e28ba-2fa1-11d2-883f-0016d3cca427/cluster/__cluster/lock.json",
                object_store::PutMode::Create,
            ),
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
                "a lost acknowledgement on {key} leaves it unsettled"
            );
            let refused = store
                .put(&Path::from(key), "again".into())
                .await
                .unwrap_err();
            assert!(
                refused.to_string().contains("unknown outcome"),
                "{key}: a later request to the unsettled key is refused: {refused}"
            );
            // The fixture loses every put's acknowledgement; what matters is
            // that another key is still admitted rather than refused.
            let admitted = store
                .put(
                    &Path::from("g/node_Person/data/later.lance"),
                    "later".into(),
                )
                .await
                .unwrap_err();
            assert!(
                admitted.to_string().contains("acknowledgement lost"),
                "{key}: other keys stay admitted: {admitted}"
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

    /// After a manifest put the store did not acknowledge, transaction files
    /// cannot be deleted through this scope. One sticky flag intentionally
    /// retains other datasets' transactions too, bounding the bookkeeping.
    /// The refusal is settled; a definite conflict on the put holds nothing.
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
            let refused = store.delete(&other).await.unwrap_err();
            assert!(refused.to_string().contains("transaction file"));
            assert!(
                store.get(&other).await.is_ok(),
                "the bounded hold covers every dataset"
            );
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

    /// A rename also removes its source. Cancellation must use that source's
    /// classification as soon as its delete begins, including when the target
    /// is one of Lance's immutable files.
    async fn rename_to_immutable_file_keeps_mutable_source_uncertainty(conditional: bool) {
        for lost_ack in [false, true] {
            let point = if conditional {
                Point::Delete
            } else {
                Point::Rename
            };
            let (scope, store, fault) = fixture(point, lost_ack).await;
            let target = Path::from("g/node_Person/data/renamed.lance");
            let task = {
                let store = store.clone();
                let target = target.clone();
                tokio::spawn(async move {
                    store
                        .rename_opts(
                            &Path::from("source"),
                            &target,
                            RenameOptions {
                                target_mode: if conditional {
                                    object_store::RenameTargetMode::Create
                                } else {
                                    object_store::RenameTargetMode::Overwrite
                                },
                                ..Default::default()
                            },
                        )
                        .await
                })
            };
            if lost_ack {
                assert!(task.await.unwrap().is_err());
            } else {
                tokio::time::timeout(Duration::from_secs(2), fault.entered.notified())
                    .await
                    .unwrap();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            }
            scope.wait_idle().await;
            assert!(
                scope.is_uncertain(),
                "conditional={conditional}, lost_ack={lost_ack}: a mutable source delete stays uncertain"
            );
            assert!(store.get(&target).await.is_ok(), "the target copy landed");
        }
    }

    #[tokio::test]
    async fn conditional_rename_keeps_mutable_source_uncertainty_issue_1067() {
        rename_to_immutable_file_keeps_mutable_source_uncertainty(true).await;
    }

    #[tokio::test]
    async fn overwrite_rename_keeps_mutable_source_uncertainty_issue_1067() {
        rename_to_immutable_file_keeps_mutable_source_uncertainty(false).await;
    }

    /// Windows' Lance RenameCommitHandler publishes with conditional rename;
    /// the wrapper implements that as a conditional copy followed by delete.
    /// A lost or cancelled copy acknowledgement must retain transaction files
    /// just as an unacknowledged manifest put does.
    #[tokio::test]
    async fn unacknowledged_manifest_copy_or_rename_holds_transactions_issue_1067() {
        for rename in [false, true] {
            for lost_ack in [false, true] {
                for name in ["14.manifest", "d12770445047132701458.manifest"] {
                    let (scope, store, fault, inner) =
                        fixture_with_inner(Point::Copy, lost_ack).await;
                    let manifest = Path::from(format!("g/node_Person/_versions/{name}"));
                    let transaction = Path::from("g/node_Person/_transactions/13-attempt.txn");
                    store.put(&transaction, "transaction".into()).await.unwrap();
                    let task = {
                        let store = store.clone();
                        let manifest = manifest.clone();
                        tokio::spawn(async move {
                            if rename {
                                store
                                    .rename_opts(
                                        &Path::from("source"),
                                        &manifest,
                                        RenameOptions {
                                            target_mode: object_store::RenameTargetMode::Create,
                                            ..Default::default()
                                        },
                                    )
                                    .await
                            } else {
                                store
                                    .copy_opts(
                                        &Path::from("source"),
                                        &manifest,
                                        CopyOptions {
                                            mode: object_store::CopyMode::Create,
                                            ..Default::default()
                                        },
                                    )
                                    .await
                            }
                        })
                    };
                    if lost_ack {
                        assert!(task.await.unwrap().is_err());
                    } else {
                        tokio::time::timeout(Duration::from_secs(2), fault.entered.notified())
                            .await
                            .unwrap();
                        task.abort();
                        assert!(task.await.unwrap_err().is_cancelled());
                    }
                    scope.wait_idle().await;
                    assert!(
                        !scope.is_uncertain(),
                        "rename={rename}, lost_ack={lost_ack}, {name}"
                    );
                    assert!(
                        store.get(&manifest).await.is_ok(),
                        "the copied manifest landed"
                    );
                    let refused = store.delete(&transaction).await.unwrap_err();
                    assert!(
                        refused.to_string().contains("transaction file"),
                        "{refused}"
                    );
                    assert!(store.get(&transaction).await.is_ok());
                    scope.close();
                    scope.wait_idle().await;
                    assert!(!scope.is_uncertain(), "the retention refusal is settled");

                    // The hold is local to the old scope. A replacement owner
                    // can delete after its collector establishes the ordinary
                    // reachability/authority/age predicates; this fixture has
                    // no graph references and removes the manifest first.
                    let fresh_scope = StorageIoScope::new();
                    let fresh = fresh_scope.wrap_object_store(inner);
                    fresh.delete(&manifest).await.unwrap();
                    fresh.delete(&transaction).await.unwrap();
                    assert!(fresh.get(&transaction).await.is_err());
                    assert!(!fresh_scope.is_uncertain());
                }
            }
        }
    }

    #[tokio::test]
    async fn replacing_a_numbered_manifest_is_not_create_only_issue_1067() {
        let (scope, store, _) = fixture(Point::Put, true).await;
        let manifest = Path::from("g/__manifest/_versions/14.manifest");
        assert!(
            store
                .put_opts(
                    &manifest,
                    "first".into(),
                    object_store::PutMode::Create.into()
                )
                .await
                .is_err()
        );
        assert!(!scope.is_uncertain());
        let version = store.head(&manifest).await.unwrap();
        let update = object_store::PutMode::Update(object_store::UpdateVersion {
            e_tag: version.e_tag,
            version: version.version,
        });
        assert!(
            store
                .put_opts(&manifest, "replacement".into(), update.into())
                .await
                .is_err()
        );
        assert!(
            scope.is_uncertain(),
            "if-match can replace an existing publication key"
        );
    }

    #[test]
    fn recursive_delete_never_inherits_a_file_key_class_issue_1067() {
        let scope = StorageIoScope::new();
        let mutation = scope
            .begin(Request::new(
                "delete_prefix",
                &Path::from("data/x.lance"),
                false,
            ))
            .unwrap();
        drop(mutation);
        assert!(
            scope.is_uncertain(),
            "a recursive removal can also delete mutable descendants"
        );
    }

    #[tokio::test]
    async fn held_transaction_delete_cannot_hide_inside_rename_issue_1067() {
        let (scope, store, _) = fixture(Point::Put, true).await;
        let transaction = Path::from("g/node_Person/_transactions/13-attempt.txn");
        let manifest = Path::from("g/node_Person/_versions/d12770445047132701458.manifest");
        assert!(store.put(&transaction, "transaction".into()).await.is_err());
        assert!(store.put(&manifest, "manifest".into()).await.is_err());
        assert!(!scope.is_uncertain());
        for target_mode in [
            object_store::RenameTargetMode::Create,
            object_store::RenameTargetMode::Overwrite,
        ] {
            let target = Path::from("g/node_Person/data/moved.lance");
            let refused = store
                .rename_opts(
                    &transaction,
                    &target,
                    RenameOptions {
                        target_mode,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                refused.to_string().contains("transaction file"),
                "{refused}"
            );
            assert!(store.get(&transaction).await.is_ok());
            assert!(
                store.get(&target).await.is_err(),
                "refusal precedes the copy"
            );
            assert!(!scope.is_uncertain());
        }
        let refused = scope
            .begin(Request::new(
                "delete_prefix",
                &Path::from("g/node_Person"),
                false,
            ))
            .unwrap_err();
        assert!(
            refused.to_string().contains("transaction file"),
            "{refused}"
        );
        assert!(
            !scope.is_uncertain(),
            "retention refusals do not poison the scope"
        );
    }

    #[tokio::test]
    async fn definite_manifest_copy_or_rename_conflict_holds_nothing_issue_1067() {
        for rename in [false, true] {
            let (scope, store, _) = fixture(Point::Copy, true).await;
            let manifest = Path::from("g/node_Person/_versions/14.manifest");
            let transaction = Path::from("g/node_Person/_transactions/13-attempt.txn");
            store.put(&transaction, "transaction".into()).await.unwrap();
            store
                .put_opts(
                    &manifest,
                    "winner".into(),
                    object_store::PutMode::Create.into(),
                )
                .await
                .unwrap();
            let result = if rename {
                store
                    .rename_opts(
                        &Path::from("source"),
                        &manifest,
                        RenameOptions {
                            target_mode: object_store::RenameTargetMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
            } else {
                store
                    .copy_opts(
                        &Path::from("source"),
                        &manifest,
                        CopyOptions {
                            mode: object_store::CopyMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
            };
            assert!(matches!(
                result,
                Err(object_store::Error::AlreadyExists { .. })
            ));
            store
                .delete(&transaction)
                .await
                .expect("a definite conflict never holds transaction cleanup");
            assert!(!scope.is_uncertain());
        }
    }

    #[tokio::test]
    async fn unacknowledged_manifest_completion_holds_transactions_after_abort_issue_1067() {
        for lost_ack in [false, true] {
            let (scope, store, fault) = fixture(Point::Complete, lost_ack).await;
            let manifest = Path::from("g/node_Person/_versions/d12770445047132701458.manifest");
            let transaction = Path::from("g/node_Person/_transactions/13-attempt.txn");
            store.put(&transaction, "transaction".into()).await.unwrap();
            let mut upload = store.put_multipart(&manifest).await.unwrap();
            upload.put_part("manifest".into()).await.unwrap();
            if lost_ack {
                assert!(upload.complete().await.is_err());
            } else {
                let complete = upload.complete();
                tokio::pin!(complete);
                tokio::select! {
                    result = &mut complete => panic!("completion acknowledgement should be parked: {result:?}"),
                    reached = tokio::time::timeout(Duration::from_secs(2), fault.entered.notified()) => reached.unwrap(),
                }
                // Dropping this borrowed completion future cancels it while
                // retaining the upload handle, so abort can still succeed.
            }
            upload.abort().await.unwrap();
            drop(upload);
            scope.wait_idle().await;
            assert!(!scope.is_uncertain());
            assert!(
                store.get(&manifest).await.is_ok(),
                "complete published the object"
            );
            let refused = store.delete(&transaction).await.unwrap_err();
            assert!(
                refused.to_string().contains("transaction file"),
                "{refused}"
            );
            assert!(store.get(&transaction).await.is_ok());
            assert!(
                !scope.is_uncertain(),
                "successful abort cannot erase a prior unknown completion"
            );
        }
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

    /// A fence cancels every admitted request still running, so its client
    /// sends nothing more, at every mutation boundary. The cancelled request
    /// may already have been sent, so its error is not the scope's definite
    /// refusal. Everything later is refused, including the parts, completion
    /// and abort of an upload admitted before the fence; reads continue.
    #[tokio::test]
    async fn a_fence_stops_admitted_requests_and_later_parts_issue_1067() {
        for point in POINTS {
            let (scope, store, fault) = fixture(point, false).await;
            let task = tokio::spawn(mutate(store, point));
            tokio::time::timeout(Duration::from_secs(2), fault.entered.notified())
                .await
                .unwrap();
            scope.fence();
            let error = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap_or_else(|_| panic!("{point:?}: a fence ends an admitted request"))
                .unwrap()
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    object_store::Error::Generic { store, .. } if store == FENCED_REQUEST_STORE_NAME
                ),
                "{point:?}: {error}"
            );
            scope.wait_idle().await;
            assert!(scope.is_fenced());
        }

        let scope = StorageIoScope::new();
        let inner = Arc::new(InMemory::new());
        inner
            .put(&Path::from("source"), "original".into())
            .await
            .unwrap();
        let store = scope.wrap_object_store(inner);
        let path = Path::from("g/node_Person/data/3f2c4d9e.lance");
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload.put_part("data".into()).await.unwrap();
        scope.fence();
        let refusals = [
            upload.put_part("more".into()).await.unwrap_err(),
            upload.complete().await.map(|_| ()).unwrap_err(),
            upload.abort().await.unwrap_err(),
            store
                .put(&path, "data".into())
                .await
                .map(|_| ())
                .unwrap_err(),
        ];
        for refused in refusals {
            assert!(
                matches!(
                    refused,
                    object_store::Error::Generic { store, .. } if store == OWNED_STORE_NAME
                ),
                "nothing is sent after a fence: {refused}"
            );
        }
        drop(upload);
        scope.wait_idle().await;
        assert!(
            store.get(&path).await.is_err(),
            "the upload never completed"
        );
        assert_eq!(
            store
                .get(&Path::from("source"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "original",
            "reads continue"
        );
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

        // InMemory treats a missing delete as success. Exercise the explicit
        // NotFound response at the same source-delete guard boundary: the copy
        // was acknowledged and the source is definitely absent.
        let scope = StorageIoScope::new();
        let result: Result<()> = scope
            .begin(Request::new(
                "rename source delete",
                &Path::from("source"),
                false,
            ))
            .unwrap()
            .finish(Err(object_store::Error::NotFound {
                path: "source".into(),
                source: std::io::Error::from(std::io::ErrorKind::NotFound).into(),
            }));
        assert!(matches!(result, Err(object_store::Error::NotFound { .. })));
        assert!(
            !scope.is_uncertain(),
            "the missing source is a definite delete outcome"
        );
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
