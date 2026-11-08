//! Scoped clone-source location reuse and local manifest identity normalization.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::io::ObjectStore;
use lance_core::{Error, Result};
use lance_table::format::{IndexMetadata, Manifest, Transaction};
use lance_table::io::commit::{
    CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme, ManifestWriter,
    PredecessorIdentity,
};
use object_store::ObjectStore as RawObjectStore;
use object_store::path::Path;

use omnigraph_storage::conditional::{
    Answer as RequestAnswer, ConditionalRequest, Resolution, Step,
};

use crate::manifest_request::{
    FailedRequest, ManifestRequest, ManifestRequestAttempt, ManifestRequestProbe,
    classify_failed_request, request_error_class,
};

#[cfg(test)]
pub(crate) use tests::fresh_dataset;

tokio::task_local! {
    static CLONE_CONTEXT: CloneContext;
}

struct CloneContext {
    source_branch: Option<String>,
    source: Path,
    source_version: u64,
    source_location: ManifestLocation,
}

impl CloneContext {
    fn capture(source: &Dataset, source_version: u64) -> Result<Self> {
        if source.version().version != source_version {
            return Err(Error::invalid_input(format!(
                "clone source version changed: expected {source_version}, got {}",
                source.version().version,
            )));
        }
        Ok(Self {
            source_branch: source.manifest().branch.clone(),
            source: source.branch_location().path,
            source_version,
            source_location: source.manifest_location().clone(),
        })
    }

    /// Reuse only canonical, immutable main-version metadata within this clone.
    /// Lance still reads the source bytes and verifies existence before ref publication.
    fn main_source_location(&self, base_path: &Path, version: u64) -> Option<ManifestLocation> {
        let location = &self.source_location;
        (self.source_branch.is_none()
            && self.source == *base_path
            && self.source_version == version
            && location.version == version
            && !lance_table::format::is_detached_version(version)
            && location.naming_scheme == ManifestNamingScheme::V2
            && location.path == ManifestNamingScheme::V2.manifest_path(base_path, version)
            && location.size.is_some()
            && location.e_tag.is_some())
        .then(|| location.clone())
    }
}

/// The handler every OmniGraph dataset commits through. It reuses a clone
/// source's location, normalizes local discovery tags, and resolves the
/// manifest request of every commit (`manifest_request`).
#[derive(Debug)]
struct ManifestLocationCommitHandler {
    inner: Arc<dyn CommitHandler>,
    reuse_main_source_location: bool,
    normalize_local_discovery_etag: bool,
    probe: Option<ManifestRequestProbe>,
}

// Lance 13's direct local discovery still uses its unquoted inode-mtime-size
// helper, while object_store 0.14 returns that same generation token quoted.
// Normalize only this known native representation; opaque provider tokens and
// missing or malformed tokens keep their exact meaning.
fn normalize_local_discovery_etag(location: &mut ManifestLocation) {
    let Some(tag) = location.e_tag.as_deref() else {
        return;
    };
    let mut parts = tag.split('-');
    if (0..3).all(|_| {
        parts.next().is_some_and(|part| {
            !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    }) && parts.next().is_none()
    {
        location.e_tag = Some(format!("\"{tag}\""));
    }
}

pub fn wrap_commit_handler(inner: Arc<dyn CommitHandler>) -> Arc<dyn CommitHandler> {
    Arc::new(wrapped(inner, None))
}

fn wrapped(
    inner: Arc<dyn CommitHandler>,
    probe: Option<ManifestRequestProbe>,
) -> ManifestLocationCommitHandler {
    ManifestLocationCommitHandler {
        inner,
        reuse_main_source_location: false,
        normalize_local_discovery_etag: false,
        probe,
    }
}

pub async fn configured_commit_handler(
    uri: &str,
    params: &Option<lance::io::ObjectStoreParams>,
    existing: Option<Arc<dyn CommitHandler>>,
) -> Result<Arc<dyn CommitHandler>> {
    Ok(Arc::new(
        configured_handler(uri, params, existing, None).await?,
    ))
}

/// The configured handler for one commit of `dataset`, recording what its
/// manifest requests establish in `probe`. Pass it to that commit's
/// `CommitBuilder::with_commit_handler`; the dataset keeps its own handler.
pub async fn probed_commit_handler(
    dataset: &Dataset,
    probe: ManifestRequestProbe,
) -> Result<Arc<dyn CommitHandler>> {
    let params = dataset.store_params().cloned();
    Ok(Arc::new(
        configured_handler(dataset.uri(), &params, None, Some(probe)).await?,
    ))
}

async fn configured_handler(
    uri: &str,
    params: &Option<lance::io::ObjectStoreParams>,
    existing: Option<Arc<dyn CommitHandler>>,
    probe: Option<ManifestRequestProbe>,
) -> Result<ManifestLocationCommitHandler> {
    if let Some(handler) = existing {
        return Ok(wrapped(handler, probe));
    }
    let reuse_main_source_location = url::Url::parse(uri).map_or(true, |url| {
        matches!(
            url.scheme(),
            "file"
                | "file-object-store"
                | "s3"
                | "gs"
                | "az"
                | "abfss"
                | "memory"
                | "oss"
                | "tos"
                | "shared-memory"
                | "goosefs"
        )
    });
    let normalize_local_discovery_etag = url::Url::parse(uri).map_or(true, |url| {
        matches!(url.scheme(), "file" | "file-object-store")
            || (cfg!(windows) && url.scheme().len() == 1)
    });
    Ok(ManifestLocationCommitHandler {
        inner: lance_table::io::commit::commit_handler_from_url(uri, params).await?,
        reuse_main_source_location,
        normalize_local_discovery_etag,
        probe,
    })
}

pub async fn write_params(
    uri: &str,
    mut params: lance::dataset::WriteParams,
) -> Result<lance::dataset::WriteParams> {
    params.commit_handler = Some(
        configured_commit_handler(uri, &params.store_params, params.commit_handler.take()).await?,
    );
    Ok(params)
}

/// Create `branch` of `source` at `source_version`. `binding` is the captured
/// destination of the handle that opened `source`, if any: Lance's
/// `create_branch` probes the new branch location through a client of its own,
/// built on a default session from the store parameters it is given, so a
/// bound handle passes its captured options there or the probe reaches the
/// ambient provider configuration.
pub async fn create_branch(
    source: &mut Dataset,
    branch: &str,
    source_version: u64,
    binding: Option<&crate::storage::CapturedStorageBinding>,
) -> Result<Dataset> {
    let store_params = binding.map(|binding| {
        let mut params = source.store_params().cloned().unwrap_or_default();
        params.storage_options_accessor = Some(Arc::new(
            lance::io::StorageOptionsAccessor::with_static_options(binding.lance_options()),
        ));
        params
    });
    let context = CloneContext::capture(source, source_version)?;
    CLONE_CONTEXT
        .scope(
            context,
            source.create_branch(branch, source_version, store_params),
        )
        .await
}

impl ManifestLocationCommitHandler {
    /// Send the commit's manifest put-if-absent until the store answers it,
    /// by the shared conditional-request policy
    /// ([`omnigraph_storage::conditional`]).
    ///
    /// A version the store holds goes back to Lance as a conflict, which Lance
    /// reads back by its complete transaction: this commit's own earlier
    /// request is then a success, another commit's a conflict. An ambiguous
    /// failure is re-issued with the same manifest and the same precondition,
    /// so at most one copy applies. A request that a re-issue's conflict or
    /// the last lost answer ends goes back to Lance as it failed, recorded as
    /// unresolved for the committer: the earlier copy may still land.
    #[allow(clippy::too_many_arguments)]
    async fn resolve_manifest_request(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        let attempt = self.probe.as_ref().map(ManifestRequestProbe::attempt);
        let finish = |attempt: Option<ManifestRequestAttempt>, outcome| {
            if let Some(attempt) = attempt {
                attempt.finish(outcome);
            }
        };
        // Lance reports a held version as a conflict and reads it back, so
        // the first copy's conflict ends the request.
        let mut request = ConditionalRequest::new(true);
        loop {
            let answer = match self
                .inner
                .commit(
                    manifest,
                    indices.clone(),
                    base_path,
                    object_store,
                    manifest_writer,
                    naming_scheme,
                    transaction.clone(),
                )
                .await
            {
                Ok(location) => RequestAnswer::Applied(location),
                Err(CommitError::CommitConflict) => {
                    RequestAnswer::Conflict(CommitError::CommitConflict)
                }
                Err(CommitError::OtherError(error)) => match classify_failed_request(&error) {
                    FailedRequest::Refused => {
                        RequestAnswer::Refused(CommitError::OtherError(error))
                    }
                    FailedRequest::Ambiguous => RequestAnswer::Lost(CommitError::OtherError(error)),
                },
            };
            let resolution = match request.answer(answer) {
                Step::Done(resolution) => resolution,
                Step::Reissue {
                    error,
                    reissue,
                    pause,
                } => {
                    tracing::warn!(
                        version = manifest.version,
                        reissue,
                        error_class = commit_error_class(&error),
                        "manifest request outcome unknown; re-issuing the identical conditional \
                         request"
                    );
                    tokio::time::sleep(pause).await;
                    continue;
                }
            };
            return match resolution {
                Resolution::Applied {
                    value: location,
                    reissues,
                } => {
                    if reissues > 0 {
                        tracing::info!(
                            version = manifest.version,
                            reissues,
                            "manifest request resolved by re-issue: the store holds this commit"
                        );
                    }
                    finish(attempt, ManifestRequest::Landed);
                    Ok(location)
                }
                Resolution::Conflict(error) => {
                    finish(attempt, ManifestRequest::Held);
                    Err(error)
                }
                Resolution::Refused(error) => {
                    finish(attempt, ManifestRequest::Refused);
                    Err(error)
                }
                Resolution::Unsettled {
                    error,
                    conflict,
                    reissues,
                } => {
                    if conflict {
                        tracing::warn!(
                            version = manifest.version,
                            reissues,
                            "manifest version held after a re-issue; the earlier copy may hold \
                             it or still be in flight, so its absence on read-back proves nothing"
                        );
                    } else {
                        tracing::error!(
                            version = manifest.version,
                            reissues,
                            error_class = commit_error_class(&error),
                            "manifest request outcome unknown after every re-issue; the version \
                             may still hold this commit"
                        );
                    }
                    finish(attempt, ManifestRequest::Unresolved);
                    Err(error)
                }
            };
        }
    }
}

/// A log-safe class of a failed manifest commit.
fn commit_error_class(error: &CommitError) -> &'static str {
    match error {
        CommitError::CommitConflict => "commit_conflict",
        CommitError::OtherError(error) => request_error_class(error),
    }
}

#[async_trait]
impl CommitHandler for ManifestLocationCommitHandler {
    /// Whether a version read back as missing after a failed commit proves
    /// the commit absent, which lets Lance delete the attempt's transaction
    /// file. Only the commit's own request record can say so: a handler built
    /// for one commit answers from its probe, and a request that may still
    /// land keeps the file its manifest would name. A dataset's shared handler
    /// holds no record of which attempt Lance is reading back, so it never
    /// calls a missing version definitive; Lance then reports the commit
    /// status unknown and leaves the file to the collector's orphan sweep.
    fn is_version_not_found_definitive(&self) -> bool {
        match &self.probe {
            Some(probe) => {
                probe.outcome() != ManifestRequest::Unresolved
                    && self.inner.is_version_not_found_definitive()
            }
            None => false,
        }
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        self.inner.propagate_commit_error_after_success()
    }

    async fn resolve_latest_location(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
    ) -> Result<ManifestLocation> {
        let mut location = self
            .inner
            .resolve_latest_location(base_path, object_store)
            .await?;
        if self.normalize_local_discovery_etag && object_store.has_direct_local_paths() {
            normalize_local_discovery_etag(&mut location);
        }
        Ok(location)
    }

    async fn resolve_version_location(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn RawObjectStore,
    ) -> Result<ManifestLocation> {
        if self.reuse_main_source_location
            && let Ok(Some(location)) =
                CLONE_CONTEXT.try_with(|context| context.main_source_location(base_path, version))
        {
            return Ok(location);
        }
        self.inner
            .resolve_version_location(base_path, version, object_store)
            .await
    }

    async fn version_exists(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn RawObjectStore,
        naming_scheme: ManifestNamingScheme,
    ) -> Result<bool> {
        self.inner
            .version_exists(base_path, version, object_store, naming_scheme)
            .await
    }

    fn list_detached_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.inner
            .list_detached_manifest_locations(base_path, object_store)
    }

    fn list_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        sorted_descending: bool,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.inner
            .list_manifest_locations(base_path, object_store, sorted_descending)
    }

    fn list_manifest_locations_since<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        since_version: u64,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.inner
            .list_manifest_locations_since(base_path, object_store, since_version)
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        self.resolve_manifest_request(
            manifest,
            indices,
            base_path,
            object_store,
            manifest_writer,
            naming_scheme,
            transaction,
        )
        .await
    }

    fn supports_predecessor_condition(&self) -> bool {
        self.inner.supports_predecessor_condition()
    }

    async fn resolve_latest_identity(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
    ) -> Result<Option<PredecessorIdentity>> {
        self.inner
            .resolve_latest_identity(base_path, object_store)
            .await
    }

    async fn resolve_identity(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
        version: u64,
    ) -> Result<Option<PredecessorIdentity>> {
        self.inner
            .resolve_identity(base_path, object_store, version)
            .await
    }

    async fn commit_after(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
        predecessor: &PredecessorIdentity,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        self.inner
            .commit_after(
                manifest,
                indices,
                base_path,
                object_store,
                manifest_writer,
                naming_scheme,
                transaction,
                predecessor,
            )
            .await
    }

    async fn forget_version(&self, base_path: &Path, version: u64, identity: &str) -> Result<()> {
        self.inner
            .forget_version(base_path, version, identity)
            .await
    }

    async fn delete(&self, base_path: &Path) -> Result<()> {
        self.inner.delete(base_path).await
    }
}

#[cfg(test)]
mod tests {
    #[derive(Debug)]
    struct ConfiguredBackend;

    #[async_trait]
    impl CommitHandler for ConfiguredBackend {
        fn is_version_not_found_definitive(&self) -> bool {
            true
        }
        fn propagate_commit_error_after_success(&self) -> bool {
            false
        }

        fn supports_predecessor_condition(&self) -> bool {
            true
        }

        async fn resolve_latest_identity(
            &self,
            _base_path: &Path,
            _object_store: &ObjectStore,
        ) -> Result<Option<PredecessorIdentity>> {
            Ok(Some(PredecessorIdentity {
                version: 7,
                identity: "configured latest".to_owned(),
            }))
        }

        async fn resolve_identity(
            &self,
            _base_path: &Path,
            _object_store: &ObjectStore,
            version: u64,
        ) -> Result<Option<PredecessorIdentity>> {
            Ok(Some(PredecessorIdentity {
                version,
                identity: "configured version".to_owned(),
            }))
        }

        async fn commit_after(
            &self,
            _manifest: &mut Manifest,
            _indices: Option<Vec<IndexMetadata>>,
            _base_path: &Path,
            _object_store: &ObjectStore,
            _writer: ManifestWriter,
            _scheme: ManifestNamingScheme,
            _transaction: Option<Transaction>,
            predecessor: &PredecessorIdentity,
        ) -> std::result::Result<ManifestLocation, CommitError> {
            assert_eq!(predecessor.version, 7);
            assert_eq!(predecessor.identity, "configured latest");
            Err(Error::io("configured conditional commit").into())
        }

        async fn forget_version(
            &self,
            _base_path: &Path,
            version: u64,
            identity: &str,
        ) -> Result<()> {
            assert_eq!(version, 3);
            assert_eq!(identity, "configured version");
            Err(Error::io("configured retirement"))
        }

        async fn resolve_latest_location(
            &self,
            base_path: &Path,
            _object_store: &ObjectStore,
        ) -> Result<ManifestLocation> {
            Ok(ManifestLocation {
                version: 7,
                path: ManifestNamingScheme::V1.manifest_path(base_path, 7),
                size: Some(128),
                naming_scheme: ManifestNamingScheme::V1,
                e_tag: Some("1a-2b-3c".to_string()),
                identity: Some("configured latest".to_string()),
            })
        }

        async fn resolve_version_location(
            &self,
            _base_path: &Path,
            _version: u64,
            _object_store: &dyn RawObjectStore,
        ) -> Result<ManifestLocation> {
            Err(Error::io("configured resolver"))
        }

        async fn commit(
            &self,
            _manifest: &mut Manifest,
            _indices: Option<Vec<IndexMetadata>>,
            _base_path: &Path,
            _object_store: &ObjectStore,
            _writer: ManifestWriter,
            _scheme: ManifestNamingScheme,
            _transaction: Option<Transaction>,
        ) -> std::result::Result<ManifestLocation, CommitError> {
            Err(Error::io("configured commit").into())
        }
    }

    #[test]
    fn local_discovery_only_quotes_the_native_generation_token() {
        for (before, after) in [
            (None, None),
            (Some(""), Some("")),
            (Some("1a-2b-3c"), Some("\"1a-2b-3c\"")),
            (Some("\"1a-2b-3c\""), Some("\"1a-2b-3c\"")),
            (Some("W/\"1a-2b-3c\""), Some("W/\"1a-2b-3c\"")),
            (Some("1a-2b"), Some("1a-2b")),
            (Some("1a-2b-3c-4d"), Some("1a-2b-3c-4d")),
            (Some("1a--3c"), Some("1a--3c")),
            (Some("1a-2b-nothex"), Some("1a-2b-nothex")),
        ] {
            let mut location = ManifestLocation {
                version: 7,
                path: "dataset/_versions/7.manifest".into(),
                size: Some(128),
                naming_scheme: ManifestNamingScheme::V1,
                e_tag: before.map(str::to_string),
                identity: Some("retained record".to_string()),
            };
            let mut expected = location.clone();
            expected.e_tag = after.map(str::to_string);
            normalize_local_discovery_etag(&mut location);
            assert_eq!(
                (
                    location.version,
                    &location.path,
                    location.size,
                    location.naming_scheme,
                    &location.e_tag,
                    &location.identity,
                ),
                (
                    expected.version,
                    &expected.path,
                    expected.size,
                    expected.naming_scheme,
                    &expected.e_tag,
                    &expected.identity,
                ),
                "token={before:?}"
            );
        }
    }

    /// A backend that answers each manifest commit with the next scripted
    /// result and counts the requests it was sent.
    #[derive(Debug)]
    struct ScriptedBackend {
        answers: std::sync::Mutex<std::collections::VecDeque<Answer>>,
        sent: std::sync::atomic::AtomicUsize,
    }

    #[derive(Debug, Clone, Copy)]
    enum Answer {
        Landed,
        Held,
        Ambiguous,
        Refused,
        LandedWithLocalError,
        NativeConditionalPut,
    }

    impl ScriptedBackend {
        fn new(answers: &[Answer]) -> Self {
            Self {
                answers: std::sync::Mutex::new(answers.iter().copied().collect()),
                sent: Default::default(),
            }
        }
    }

    #[async_trait]
    impl CommitHandler for ScriptedBackend {
        // As the conditional-put handler does: a missing version is absent.
        fn is_version_not_found_definitive(&self) -> bool {
            true
        }

        async fn resolve_version_location(
            &self,
            _base_path: &Path,
            _version: u64,
            _object_store: &dyn RawObjectStore,
        ) -> Result<ManifestLocation> {
            Err(Error::io("scripted resolver"))
        }

        async fn commit(
            &self,
            manifest: &mut Manifest,
            indices: Option<Vec<IndexMetadata>>,
            base_path: &Path,
            object_store: &ObjectStore,
            writer: ManifestWriter,
            naming_scheme: ManifestNamingScheme,
            transaction: Option<Transaction>,
        ) -> std::result::Result<ManifestLocation, CommitError> {
            self.sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let answer = self.answers.lock().unwrap().pop_front().expect("scripted");
            let store_error = |store: &'static str| {
                CommitError::OtherError(
                    object_store::Error::Generic {
                        store,
                        source: std::io::Error::other("scripted").into(),
                    }
                    .into(),
                )
            };
            match answer {
                Answer::Landed => Ok(ManifestLocation {
                    version: manifest.version,
                    path: naming_scheme.manifest_path(base_path, manifest.version),
                    size: None,
                    naming_scheme,
                    e_tag: None,
                    identity: None,
                }),
                Answer::Held => Err(CommitError::CommitConflict),
                Answer::Ambiguous => Err(store_error("S3")),
                Answer::Refused => Err(store_error(omnigraph_storage::OWNED_STORE_NAME)),
                Answer::LandedWithLocalError | Answer::NativeConditionalPut => {
                    let result = lance_table::io::commit::ConditionalPutCommitHandler
                        .commit(
                            manifest,
                            indices,
                            base_path,
                            object_store,
                            writer,
                            naming_scheme,
                            transaction,
                        )
                        .await;
                    if matches!(answer, Answer::LandedWithLocalError) {
                        // Windows' rename handler can create the destination
                        // before source cleanup fails. Persist a real manifest
                        // here so the re-issue meets its native precondition.
                        result.expect("the manifest must exist before the local error");
                        Err(store_error("LocalFileSystem"))
                    } else {
                        result
                    }
                }
            }
        }
    }

    /// Run one manifest commit through the configured handler over `answers`;
    /// returns whether it succeeded, how many requests were sent and what the
    /// probe recorded.
    async fn resolve(answers: &[Answer]) -> (bool, usize, ManifestRequest) {
        let backend = Arc::new(ScriptedBackend::new(answers));
        let probe = ManifestRequestProbe::new();
        let handler = wrapped(backend.clone(), Some(probe.clone()));
        let succeeded = commit_once(&handler).await;
        let outcome = probe.outcome();
        // Lance reads the version back through this flag after a failure: a
        // missing version is proof of absence unless the request may still
        // land, which keeps the transaction file its manifest would name.
        assert_eq!(
            handler.is_version_not_found_definitive(),
            outcome != ManifestRequest::Unresolved,
            "{answers:?}"
        );
        (
            succeeded,
            backend.sent.load(std::sync::atomic::Ordering::SeqCst),
            outcome,
        )
    }

    /// Send one manifest commit through `handler`; returns whether it succeeded.
    async fn commit_once(handler: &ManifestLocationCommitHandler) -> bool {
        let directory = tempfile::tempdir().unwrap();
        let dataset = fresh_dataset(directory.path().to_str().unwrap()).await;
        let mut manifest = dataset.manifest().clone();
        manifest.version += 1;
        let store = dataset.object_store(None).await.unwrap();
        let result = handler
            .commit(
                &mut manifest,
                None,
                &dataset.branch_location().path,
                &store,
                lance_table::io::commit::write_manifest_file_to_path,
                ManifestNamingScheme::V2,
                None,
            )
            .await;
        result.is_ok()
    }

    /// The handler re-issues an ambiguous manifest request until the store
    /// answers it; a definite refusal, a held version and a landed manifest
    /// end it at once; and a request no answer settles stays unresolved, as
    /// does one whose re-issue meets a held version.
    #[tokio::test]
    async fn manifest_request_is_re_issued_until_the_store_answers_issue_1067() {
        use Answer::*;
        assert_eq!(resolve(&[Landed]).await, (true, 1, ManifestRequest::Landed));
        assert_eq!(
            resolve(&[Held]).await,
            (false, 1, ManifestRequest::Held),
            "a held version goes to Lance's read-back, which tells this commit from another"
        );
        assert_eq!(
            resolve(&[LandedWithLocalError, NativeConditionalPut]).await,
            (false, 2, ManifestRequest::Unresolved),
            "a local error after manifest creation must not be called a refusal"
        );
        assert_eq!(
            resolve(&[Ambiguous, Ambiguous, Landed]).await,
            (true, 3, ManifestRequest::Landed),
            "a re-issue the store applies is this commit"
        );
        assert_eq!(
            resolve(&[Ambiguous, Held]).await,
            (false, 2, ManifestRequest::Unresolved),
            "a re-issue's conflict can answer the earlier copy still in flight (S3's 409): \
             Lance's read-back decides, and a missing version is not proof of absence"
        );
        assert_eq!(
            resolve(&[Refused]).await,
            (false, 1, ManifestRequest::Refused),
            "a definite refusal is not re-issued"
        );
        assert_eq!(
            resolve(&[Ambiguous, Refused, Landed]).await,
            (true, 3, ManifestRequest::Landed),
            "after an ambiguous request, a refusal does not settle the key"
        );
        let unanswered = [Ambiguous; 1 + omnigraph_storage::CONDITIONAL_REQUEST_REISSUES as usize];
        assert_eq!(
            resolve(&unanswered).await,
            (false, unanswered.len(), ManifestRequest::Unresolved)
        );
    }

    /// A dataset's own handler is shared by concurrent commits and holds no
    /// record of the attempt Lance reads back, so it never calls a missing
    /// version definitive, whatever its last commit did: a request that may
    /// still land keeps the transaction file its manifest would name.
    #[tokio::test]
    async fn a_shared_handler_never_calls_a_missing_version_definitive_issue_1067() {
        let unanswered =
            [Answer::Ambiguous; 1 + omnigraph_storage::CONDITIONAL_REQUEST_REISSUES as usize];
        for answers in [&[Answer::Landed][..], &[Answer::Refused], &unanswered] {
            let handler = wrapped(Arc::new(ScriptedBackend::new(answers)), None);
            assert!(!handler.is_version_not_found_definitive(), "{answers:?}");
            commit_once(&handler).await;
            assert!(!handler.is_version_not_found_definitive(), "{answers:?}");
        }
    }

    #[tokio::test]
    async fn creation_preserves_configured_handler_session_and_failure_semantics() {
        let session = Arc::new(Session::default());
        let params = write_params(
            "custom-commit-provider://dataset",
            WriteParams {
                commit_handler: Some(Arc::new(ConfiguredBackend)),
                session: Some(session.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(params.session.as_ref().unwrap(), &session));
        let handler = params.commit_handler.unwrap();
        // A shared handler holds no record of the attempt Lance reads back,
        // so it never calls a missing version definitive, whatever the
        // configured backend says.
        assert!(!handler.is_version_not_found_definitive());
        assert!(!handler.propagate_commit_error_after_success());
        assert!(handler.supports_predecessor_condition());
        let path = Path::from("dataset");
        // A custom local handler owns opaque tokens too: an ETag that happens
        // to resemble the native representation must remain byte-identical.
        let custom_local = configured_commit_handler(
            "file:///configured",
            &None,
            Some(Arc::new(ConfiguredBackend)),
        )
        .await
        .unwrap();
        let location = custom_local
            .resolve_latest_location(&path, &ObjectStore::local())
            .await
            .unwrap();
        assert_eq!(location.e_tag.as_deref(), Some("1a-2b-3c"));
        assert_eq!(location.identity.as_deref(), Some("configured latest"));
        let store = ObjectStore::from_uri("memory://delegation")
            .await
            .unwrap()
            .0;
        let latest_identity = handler
            .resolve_latest_identity(&path, &store)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            latest_identity,
            PredecessorIdentity {
                version: 7,
                identity: "configured latest".to_owned(),
            }
        );
        let version_identity = handler
            .resolve_identity(&path, &store, 3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            version_identity,
            PredecessorIdentity {
                version: 3,
                identity: "configured version".to_owned(),
            }
        );
        let error = handler
            .forget_version(&path, version_identity.version, &version_identity.identity)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("configured retirement"));
        let backend = object_store::memory::InMemory::new();
        let error = handler
            .resolve_version_location(&Path::from("dataset"), 1, &backend)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("configured resolver"));
        let dir = tempfile::tempdir().unwrap();
        let mut source = fresh_dataset(dir.path().join("init.lance").to_str().unwrap()).await;
        let context = CloneContext::capture(&source, source.version().version).unwrap();
        let location = source.manifest_location();
        assert!(
            context
                .main_source_location(&source.branch_location().path, location.version)
                .is_some()
        );
        let error = CLONE_CONTEXT
            .scope(
                context,
                handler.resolve_version_location(
                    &source.branch_location().path,
                    location.version,
                    &backend,
                ),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("configured resolver"));
        let error = handler
            .commit(
                Arc::make_mut(&mut source.manifest),
                None,
                &path,
                &store,
                lance_table::io::commit::write_manifest_file_to_path,
                ManifestNamingScheme::V1,
                None,
            )
            .await
            .unwrap_err();
        assert!(Error::from(error).to_string().contains("configured commit"));
        let error = handler
            .commit_after(
                Arc::make_mut(&mut source.manifest),
                None,
                &path,
                &store,
                lance_table::io::commit::write_manifest_file_to_path,
                ManifestNamingScheme::V1,
                None,
                &latest_identity,
            )
            .await
            .unwrap_err();
        assert!(
            Error::from(error)
                .to_string()
                .contains("configured conditional commit")
        );
    }

    #[tokio::test]
    async fn newly_created_dataset_can_fork_twice_without_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("fresh.lance");
        let uri = uri.to_str().unwrap();
        let mut source = fresh_dataset(uri).await;
        source
            .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
        let version = source.version().version;
        let mut first = create_branch(&mut source, "first", version, None)
            .await
            .unwrap();
        let version = first.version().version;
        let second = create_branch(&mut first, "second", version, None)
            .await
            .unwrap();
        for dataset in [&source, &first, &second] {
            let store = dataset.object_store(None).await.unwrap();
            let handler =
                configured_commit_handler(dataset.uri(), &dataset.store_params().cloned(), None)
                    .await
                    .unwrap();
            let discovered = handler
                .resolve_latest_location(&dataset.branch_location().path, &store)
                .await
                .unwrap();
            let object = store.inner.head(&discovered.path).await.unwrap();
            assert_eq!(discovered.e_tag, object.e_tag);
            assert_eq!(discovered.identity, dataset.manifest_location().identity);
            if let Some(published) = dataset.manifest_location().e_tag.as_ref() {
                assert_eq!(discovered.e_tag.as_ref(), Some(published));
            }
        }
        let mut scan = second.scan();
        scan.filter("value = 1").unwrap();
        assert!(
            scan.explain_plan(true)
                .await
                .unwrap()
                .contains("ScalarIndexQuery")
        );
        assert_eq!(scan.try_into_batch().await.unwrap().num_rows(), 1);
    }
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::builder::DatasetBuilder;
    use lance::dataset::{WriteMode, WriteParams};
    use lance::index::DatasetIndexExt;
    use lance::session::Session;
    use lance_file::version::LanceFileVersion;
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;
    use object_store::ObjectStoreExt;
    pub(crate) async fn fresh_dataset(uri: &str) -> Dataset {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let params = write_params(uri, params).await.unwrap();
        Dataset::write(reader, uri, Some(params)).await.unwrap()
    }

    async fn append_guard_row(dataset: &mut Dataset, id: &str, value: i32) {
        let schema = Arc::new(Schema::from(dataset.schema()));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(Int32Array::from(vec![value])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        dataset
            .append(
                reader,
                Some(WriteParams {
                    mode: WriteMode::Append,
                    enable_stable_row_ids: true,
                    data_storage_version: Some(LanceFileVersion::V2_2),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }
    async fn engine_open(uri: &str) -> Dataset {
        crate::instrumentation::open_dataset(
            uri,
            crate::instrumentation::VersionResolution::Latest,
            Some(&Arc::new(Session::default())),
            None,
        )
        .await
        .unwrap()
    }

    async fn indexed_source(uri: &str) -> Dataset {
        let mut dataset = fresh_dataset(uri).await;
        dataset
            .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
        engine_open(uri).await
    }

    #[tokio::test]
    async fn source_version_mismatch_refuses_before_native_create() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("versioned.lance");
        let mut source = fresh_dataset(uri.to_str().unwrap()).await;
        let version = source.version().version;
        let error = create_branch(&mut source, "refused", version + 1, None)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(source.list_branches().await.unwrap().is_empty());
        let target = source
            .branch_location()
            .find_branch(Some("refused"))
            .unwrap();
        assert!(!std::path::Path::new(&target.uri).exists());
        assert_eq!(source.version().version, version);
    }

    #[tokio::test]
    async fn concurrent_clone_contexts_keep_each_source_location() {
        async fn branch_twice(uri: String) -> Dataset {
            let mut source = fresh_dataset(&uri).await;
            append_guard_row(&mut source, &uri, 3).await;
            let version = source.version().version;
            let mut first = create_branch(&mut source, "first", version, None)
                .await
                .unwrap();
            tokio::task::yield_now().await;
            let version = first.version().version;
            let second = create_branch(&mut first, "second", version, None)
                .await
                .unwrap();
            assert!(CLONE_CONTEXT.try_with(|_| ()).is_err());
            let mut scan = second.scan();
            scan.filter("value = 3").unwrap();
            let batch = scan.try_into_batch().await.unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                uri
            );
            second
        }
        let dir = tempfile::tempdir().unwrap();
        let left_uri = dir.path().join("left.lance").to_str().unwrap().to_string();
        let right_uri = dir.path().join("right.lance").to_str().unwrap().to_string();
        let (left, right) = tokio::join!(
            tokio::spawn(branch_twice(left_uri)),
            tokio::spawn(branch_twice(right_uri))
        );
        assert_ne!(left.unwrap().uri(), right.unwrap().uri());
    }

    #[tokio::test]
    async fn source_capture_performs_no_io_and_append_cost_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let empty_uri = dir.path().join("empty.lance");
        fresh_dataset(empty_uri.to_str().unwrap()).await;
        let empty = engine_open(empty_uri.to_str().unwrap()).await;
        let store = empty.object_store(None).await.unwrap();
        store.io_stats_incremental();
        CloneContext::capture(&empty, empty.version().version).unwrap();
        let empty_capture = store.io_stats_incremental();
        assert_eq!(empty_capture.read_iops, 0, "{empty_capture:?}");
        assert_eq!(empty_capture.write_iops, 0, "{empty_capture:?}");

        let indexed_uri = dir.path().join("indexed.lance");
        let indexed = indexed_source(indexed_uri.to_str().unwrap()).await;
        let store = indexed.object_store(None).await.unwrap();
        store.io_stats_incremental();
        CloneContext::capture(&indexed, indexed.version().version).unwrap();
        let indexed_capture = store.io_stats_incremental();
        assert_eq!(
            indexed_capture.read_iops, 0,
            "capture must not inspect index metadata: {indexed_capture:?}"
        );
        assert_eq!(indexed_capture.write_iops, 0, "{indexed_capture:?}");

        let raw_uri = dir.path().join("raw.lance");
        let engine_uri = dir.path().join("eng.lance");
        fresh_dataset(raw_uri.to_str().unwrap()).await;
        fresh_dataset(engine_uri.to_str().unwrap()).await;
        let mut raw = DatasetBuilder::from_uri(raw_uri.to_str().unwrap())
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap();
        let mut engine = engine_open(engine_uri.to_str().unwrap()).await;
        let raw_store = raw.object_store(None).await.unwrap();
        let engine_store = engine.object_store(None).await.unwrap();
        raw_store.io_stats_incremental();
        engine_store.io_stats_incremental();
        append_guard_row(&mut raw, "carol", 3).await;
        let raw_cost = raw_store.io_stats_incremental();
        append_guard_row(&mut engine, "carol", 3).await;
        let engine_cost = engine_store.io_stats_incremental();
        assert_eq!(
            (engine_cost.read_iops, engine_cost.write_iops),
            (raw_cost.read_iops, raw_cost.write_iops),
            "raw={raw_cost:?}, engine={engine_cost:?}"
        );
        assert!(engine.manifest_location().e_tag.is_some());
        raw_store.io_stats_incremental();
        engine_store.io_stats_incremental();
        let raw_child = raw
            .create_branch("read_cost", raw.version().version, None)
            .await
            .unwrap();
        let raw_clone_cost = raw_store.io_stats_incremental();
        let version = engine.version().version;
        let engine_child = create_branch(&mut engine, "read_cost", version, None)
            .await
            .unwrap();
        let engine_clone_cost = engine_store.io_stats_incremental();
        assert_eq!(
            engine_clone_cost.read_iops + 2,
            raw_clone_cost.read_iops,
            "raw={raw_clone_cost:?}, engine={engine_clone_cost:?}"
        );
        assert_eq!(engine_clone_cost.write_iops, raw_clone_cost.write_iops);
        assert_eq!(raw_child.count_rows(None).await.unwrap(), 3);
        assert_eq!(engine_child.count_rows(None).await.unwrap(), 3);
        assert!(CLONE_CONTEXT.try_with(|_| ()).is_err());
    }

    #[tokio::test]
    async fn scoped_main_source_location_keeps_fallbacks_and_missing_source_failure() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("source.lance");
        let uri = uri.to_str().unwrap();
        let mut source = fresh_dataset(uri).await;
        let version = source.version().version;
        let base = source.branch_location().path;
        let mut context = CloneContext::capture(&source, version).unwrap();
        let original = context.source_location.clone();
        assert!(context.main_source_location(&base, version).is_some());
        assert!(
            context
                .main_source_location(&Path::from("other"), version)
                .is_none()
        );
        assert!(context.main_source_location(&base, version + 1).is_none());
        context.source_location.e_tag = None;
        assert!(context.main_source_location(&base, version).is_none());
        context.source_location = original.clone();
        context.source_location.size = None;
        assert!(context.main_source_location(&base, version).is_none());
        context.source_location = original.clone();
        context.source_location.naming_scheme = ManifestNamingScheme::V1;
        context.source_location.path = ManifestNamingScheme::V1.manifest_path(&base, version);
        assert!(context.main_source_location(&base, version).is_none());
        context.source_location = original.clone();
        context.source_location.path = Path::from("custom/manifest");
        assert!(context.main_source_location(&base, version).is_none());
        context.source_location = original.clone();
        for native in ["legacy", "feature.01ARZ3NDEKTSV4RRFFQ69G5FAV"] {
            context.source_branch = Some(native.to_string());
            assert!(context.main_source_location(&base, version).is_none());
        }
        let store = source.object_store(None).await.unwrap();
        store.inner.delete(&original.path).await.unwrap();
        let error = create_branch(&mut source, "missing_source", version, None)
            .await
            .unwrap_err();
        assert!(error.is_not_found(), "{error:?}");
        assert!(source.list_branches().await.unwrap().is_empty());
    }
}
