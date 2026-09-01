use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fmt::{self, Debug};
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{DynObjectStore, GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use url::Url;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageError>;

/// The typed condition established at a storage-substrate boundary.
///
/// This classifies the evidence carried by the failure. It does not say
/// whether an operation is safe to replay; that decision remains local to the
/// operation that can prove its effect boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFailureKind {
    /// Positive evidence of timeout, throttling, cancellation, or transport
    /// interruption.
    Transient,
    /// Authentication, permission, unsupported operation, malformed
    /// input/location, or an operator-actionable capacity constraint.
    Configuration,
    /// The requested object, dataset, ref, version, index, or namespace entity
    /// is absent.
    NotFound,
    /// An already-exists, not-modified, CAS/concurrency, stale-authority, or
    /// fenced-authority condition requires state to be re-evaluated.
    Precondition,
    /// Positive evidence of corruption, an invariant failure, panic, or a
    /// substrate-internal failure.
    Permanent,
    /// Typed evidence is insufficient. Neither retry nor permanent escalation
    /// is implied.
    Unknown,
}

/// A classified storage failure whose message is already the complete
/// operator-facing diagnostic.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct StorageFailure {
    pub kind: StorageFailureKind,
    pub message: String,
}

impl StorageFailure {
    pub fn new(kind: StorageFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// True only for positive transient evidence. This is not a replay
    /// decision.
    pub fn is_transient(&self) -> bool {
        self.kind == StorageFailureKind::Transient
    }
}

/// Inspect at most eight source links. A typed cause beyond this bound, a
/// cyclic chain, or a source-free opaque wrapper remains `Unknown`.
const MAX_STORAGE_SOURCE_DEPTH: usize = 8;

/// Callback used by an owning crate to add typed storage wrappers without
/// duplicating the bounded source-chain traversal.
#[doc(hidden)]
pub type StorageSourceClassifier = for<'a> fn(
    &'a (dyn std::error::Error + 'static),
) -> std::ops::ControlFlow<
    StorageFailureKind,
    Option<&'a (dyn std::error::Error + 'static)>,
>;

/// Classify a storage-owned I/O error, recursively consulting a typed inner
/// source only when the outer `ErrorKind` carries no decision.
fn classify_io_error(error: &std::io::Error) -> StorageFailureKind {
    find_storage_source_kind_with(error, no_additional_storage_source)
        .unwrap_or(StorageFailureKind::Unknown)
}

/// Classify an `object_store` 0.13 failure by its typed variant. Opaque
/// `Generic` values are not assumed transient merely because the provider has
/// already exhausted its own retries.
fn classify_object_store_error(error: &object_store::Error) -> StorageFailureKind {
    find_storage_source_kind_with(error, no_additional_storage_source)
        .unwrap_or(StorageFailureKind::Unknown)
}

/// The single bounded typed-source traversal shared by the storage adapter
/// and engine-specific storage wrappers.
/// `Break(kind)` supplies typed engine-owned evidence,
/// `Continue(Some(source))` supplies a recognized wrapper's typed source, and
/// `Continue(None)` means the node is not owned by the extension.
#[doc(hidden)]
pub fn find_storage_source_kind_with(
    source: &(dyn std::error::Error + 'static),
    classify_additional: StorageSourceClassifier,
) -> Option<StorageFailureKind> {
    let mut current = source;
    let mut current_depth = 0;
    let mut saw_storage_wrapper = false;
    loop {
        if current_depth >= MAX_STORAGE_SOURCE_DEPTH {
            return saw_storage_wrapper.then_some(StorageFailureKind::Unknown);
        }
        let evidence = match classify_builtin_storage_source(current) {
            std::ops::ControlFlow::Continue(None) => classify_additional(current),
            evidence => evidence,
        };
        match evidence {
            std::ops::ControlFlow::Break(kind) => return Some(kind),
            std::ops::ControlFlow::Continue(Some(inner)) => {
                saw_storage_wrapper = true;
                current = inner;
                current_depth += 1;
                continue;
            }
            std::ops::ControlFlow::Continue(None) => {}
        }
        let Some(inner) = current.source() else {
            return saw_storage_wrapper.then_some(StorageFailureKind::Unknown);
        };
        current = inner;
        current_depth += 1;
    }
}

fn no_additional_storage_source<'a>(
    _source: &'a (dyn std::error::Error + 'static),
) -> std::ops::ControlFlow<StorageFailureKind, Option<&'a (dyn std::error::Error + 'static)>> {
    std::ops::ControlFlow::Continue(None)
}

fn classify_builtin_storage_source<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> std::ops::ControlFlow<StorageFailureKind, Option<&'a (dyn std::error::Error + 'static)>> {
    use std::io::ErrorKind;
    use std::ops::ControlFlow::{Break, Continue};

    if let Some(error) = source.downcast_ref::<object_store::client::HttpError>() {
        return Break(match error.kind() {
            object_store::client::HttpErrorKind::Connect
            | object_store::client::HttpErrorKind::Timeout
            | object_store::client::HttpErrorKind::Interrupted => StorageFailureKind::Transient,
            // `Request` also wraps request-construction/serialization failures,
            // and `Decode` does not prove whether the bad response is stable.
            // Neither public kind is positive retry or permanence evidence.
            object_store::client::HttpErrorKind::Request
            | object_store::client::HttpErrorKind::Decode
            | object_store::client::HttpErrorKind::Unknown => StorageFailureKind::Unknown,
            _ => StorageFailureKind::Unknown,
        });
    }
    if let Some(error) = source.downcast_ref::<object_store::Error>() {
        return match error {
            object_store::Error::NotFound { .. } => Break(StorageFailureKind::NotFound),
            object_store::Error::NotModified { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. } => Break(StorageFailureKind::Precondition),
            object_store::Error::InvalidPath { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::UnknownConfigurationKey { .. } => {
                Break(StorageFailureKind::Configuration)
            }
            object_store::Error::JoinError { source } if source.is_cancelled() => {
                Break(StorageFailureKind::Transient)
            }
            object_store::Error::JoinError { source } if source.is_panic() => {
                Break(StorageFailureKind::Permanent)
            }
            object_store::Error::JoinError { .. } => Break(StorageFailureKind::Unknown),
            object_store::Error::Generic { source, .. } => Continue(Some(source.as_ref())),
            _ => Break(StorageFailureKind::Unknown),
        };
    }
    if let Some(error) = source.downcast_ref::<std::io::Error>() {
        return match error.kind() {
            ErrorKind::NotFound => Break(StorageFailureKind::NotFound),
            ErrorKind::AlreadyExists => Break(StorageFailureKind::Precondition),
            ErrorKind::PermissionDenied
            | ErrorKind::InvalidInput
            | ErrorKind::Unsupported
            | ErrorKind::StorageFull
            | ErrorKind::QuotaExceeded
            | ErrorKind::ReadOnlyFilesystem
            | ErrorKind::FileTooLarge => Break(StorageFailureKind::Configuration),
            ErrorKind::TimedOut
            | ErrorKind::Interrupted
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
            | ErrorKind::HostUnreachable
            | ErrorKind::NetworkUnreachable
            | ErrorKind::WouldBlock => Break(StorageFailureKind::Transient),
            ErrorKind::InvalidData => Break(StorageFailureKind::Permanent),
            _ if error.get_ref().is_some() => Continue(
                error
                    .get_ref()
                    .map(|source| source as &(dyn std::error::Error + 'static)),
            ),
            _ => Break(StorageFailureKind::Unknown),
        };
    }
    Continue(None)
}

/// Resource envelope for one suffix-filtered directory listing.
///
/// The bounds apply while the backend listing stream is consumed, before the
/// adapter can accumulate an unbounded `Vec`. `max_irrelevant_entries` counts
/// both direct children that do not match the requested suffix and recursive
/// descendants, which keeps unrelated prefix residue from turning a small
/// filtered inventory into an unbounded scan. `max_uri_bytes` counts the
/// input-anchored URI bytes for every encountered object, matching or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListDirBounds {
    /// Maximum direct children that may match the requested suffix.
    pub max_matching_entries: usize,
    /// Maximum other objects encountered beneath the directory prefix.
    pub max_irrelevant_entries: usize,
    /// Maximum cumulative input-anchored URI bytes across all encountered objects.
    pub max_uri_bytes: u64,
}

/// Backend-neutral failure from the shared control-object storage boundary.
///
/// `Internal` preserves the engine's historical manifest-internal message
/// verbatim when converted by the compatibility facade. `Backend` carries the
/// complete historical diagnostic plus typed substrate evidence; `Io` also
/// retains the original structured filesystem source for embedded callers.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    Backend(StorageFailure),
    /// A storage-owned filesystem failure. The classified failure drives
    /// engine/HTTP semantics while the original I/O value remains available
    /// to embedded callers through the source chain.
    #[error("{failure}")]
    Io {
        failure: StorageFailure,
        #[source]
        source: std::io::Error,
    },
    #[error("storage resource '{resource}' for '{uri}' exceeds limit {limit} (actual {actual})")]
    ResourceLimit {
        resource: String,
        limit: u64,
        actual: u64,
        uri: String,
    },
}

impl StorageError {
    fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    fn backend(kind: StorageFailureKind, message: impl Into<String>) -> Self {
        Self::Backend(StorageFailure::new(kind, message))
    }

    fn io(error: std::io::Error) -> Self {
        let kind = classify_io_error(&error);
        Self::Io {
            failure: StorageFailure::new(kind, format!("io: {error}")),
            source: error,
        }
    }

    fn io_context(error: std::io::Error, message: impl Into<String>) -> Self {
        let kind = classify_io_error(&error);
        Self::Io {
            failure: StorageFailure::new(kind, message),
            source: error,
        }
    }
}

/// Preserve the pre-v0.10 `?` conversion while enriching the resulting I/O
/// variant with typed storage evidence.
impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        Self::io(error)
    }
}

const FILE_SCHEME_PREFIX: &str = "file://";
const S3_SCHEME_PREFIX: &str = "s3://";
const AZURE_SCHEME_PREFIX: &str = "az://";
/// The DST harness's opaque in-memory scheme (Lance's shared-memory
/// provider). Named beside its siblings; every user (the classification
/// arm, the URI normalizer, the Memory-codec strip) is `dst`-gated, so
/// the const is too.
#[cfg(feature = "dst")]
const SHARED_MEMORY_SCHEME_PREFIX: &str = "shared-memory://";
const DEFAULT_AZURITE_BLOB_STORAGE_URL: &str = "http://127.0.0.1:10000";
// Keep the Azure GET -> multipart PUT rename envelope bounded. Five MiB is
// accepted as a non-final multipart part by every object_store backend and
// leaves schema promotion with a small, fixed peak copy buffer.
const AZURE_RENAME_PART_BYTES: u64 = 5 * 1024 * 1024;
// Public, well-known Azurite development key. Never use this for Azure.
const DEFAULT_AZURITE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const AZURE_ADMISSION_PREFIX: &str = "__omnigraph_azure_admission/v1";

/// Render a storage location for diagnostics without exposing URI credentials.
///
/// This is deliberately not a canonicalization API: callers must use the
/// original URI for I/O. The returned label removes userinfo, query strings,
/// and fragments, and falls back to a scheme-only label when a malformed URI
/// cannot be safely decomposed.
pub fn redacted_storage_uri(uri: &str) -> String {
    if !has_uri_scheme(uri) || is_windows_drive_path(uri) {
        return uri.to_string();
    }

    let Ok(mut url) = Url::parse(uri) else {
        let scheme = uri
            .split_once(':')
            .map(|(scheme, _)| scheme)
            .filter(|scheme| {
                !scheme.is_empty()
                    && scheme.as_bytes()[0].is_ascii_alphabetic()
                    && scheme.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.')
                    })
            })
            .unwrap_or("storage");
        return format!("{scheme}://<invalid or redacted>");
    };

    let had_userinfo = !url.username().is_empty() || url.password().is_some();
    let had_query = url.query().is_some();
    let had_fragment = url.fragment().is_some();
    if had_userinfo && (url.set_password(None).is_err() || url.set_username("").is_err()) {
        return format!("{}://<credentials redacted>", url.scheme());
    }
    url.set_query(None);
    url.set_fragment(None);

    let mut redacted = Vec::new();
    if had_userinfo {
        redacted.push("userinfo");
    }
    if had_query {
        redacted.push("query");
    }
    if had_fragment {
        redacted.push("fragment");
    }
    if redacted.is_empty() {
        url.to_string()
    } else {
        format!("{} [{} redacted]", url, redacted.join(", "))
    }
}

/// Process-wide Azure location and identity selection captured before the
/// first Azure client is built.
///
/// The complete object-store configuration is captured so control-object and
/// Lance clients cannot silently select different accounts, endpoints, or
/// static credentials after the process has started. Upstream managed-identity
/// providers may still refresh short-lived credentials after construction.
#[derive(Clone, PartialEq, Eq)]
pub struct AzureStorageConfig {
    account_name: String,
    endpoint: Option<String>,
    emulator_url: Option<String>,
    use_emulator: bool,
    client_id: Option<String>,
    identity_endpoint: Option<String>,
    identity_header: Option<String>,
    azure_options: BTreeMap<String, String>,
    environment_snapshot: Option<AzureEnvironmentSnapshot>,
}

impl fmt::Debug for AzureStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureStorageConfig")
            .field("account_name", &self.account_name)
            .field("endpoint", &self.endpoint.as_ref().map(|_| "<configured>"))
            .field(
                "emulator_url",
                &self.emulator_url.as_ref().map(|_| "<configured>"),
            )
            .field("use_emulator", &self.use_emulator)
            .field("client_id", &self.client_id)
            .field(
                "identity_endpoint",
                &self.identity_endpoint.as_ref().map(|_| "<configured>"),
            )
            .field(
                "identity_header",
                &self.identity_header.as_ref().map(|_| "<redacted>"),
            )
            .field("azure_options", &RedactedAzureOptions(&self.azure_options))
            .field(
                "environment_snapshot",
                &self.environment_snapshot.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
struct AzureEnvironmentSnapshot {
    values: BTreeMap<String, String>,
}

impl fmt::Debug for AzureEnvironmentSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureEnvironmentSnapshot")
            .field("keys", &self.values.keys().collect::<Vec<_>>())
            .field("values", &"<redacted>")
            .finish()
    }
}

struct RedactedAzureOptions<'a>(&'a BTreeMap<String, String>);

impl fmt::Debug for RedactedAzureOptions<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|key| (key, "<redacted>")))
            .finish()
    }
}

/// Captured secret material consumed only by the narrow Azure lease wrapper.
///
/// This is deliberately hidden from generated documentation. Its `Debug`
/// implementation never prints credential values, and no general raw-secret
/// getter is exposed by [`CanonicalAzureRoot`].
#[doc(hidden)]
#[derive(Clone, PartialEq, Eq)]
pub enum AzureAdmissionCredential {
    BearerToken(String),
    SharedKey {
        account: String,
        encoded_key: String,
    },
    ManagedIdentity {
        endpoint: String,
        secret_header: String,
        client_id: Option<String>,
    },
}

impl fmt::Debug for AzureAdmissionCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BearerToken(_) => f.write_str("BearerToken(<redacted>)"),
            Self::SharedKey { account, .. } => f
                .debug_struct("SharedKey")
                .field("account", account)
                .field("encoded_key", &"<redacted>")
                .finish(),
            Self::ManagedIdentity {
                endpoint,
                client_id,
                ..
            } => f
                .debug_struct("ManagedIdentity")
                .field("endpoint", endpoint)
                .field("secret_header", &"<redacted>")
                .field("client_id", client_id)
                .finish(),
        }
    }
}

impl AzureStorageConfig {
    /// Construct an explicit production configuration.
    ///
    /// Account and endpoint selection come from this value. Supported Azure
    /// credential and HTTP-client environment variables are still captured
    /// once when the root is constructed, so explicit selection does not
    /// weaken the upstream authentication chain.
    pub fn new(account_name: impl Into<String>) -> Self {
        Self {
            account_name: account_name.into(),
            endpoint: None,
            emulator_url: None,
            use_emulator: false,
            client_id: None,
            identity_endpoint: None,
            identity_header: None,
            azure_options: BTreeMap::new(),
            environment_snapshot: None,
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn with_emulator(mut self, emulator_url: impl Into<String>) -> Self {
        self.use_emulator = true;
        self.emulator_url = Some(emulator_url.into());
        self
    }

    pub fn with_default_emulator(mut self) -> Self {
        self.use_emulator = true;
        self.emulator_url = None;
        self
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_identity_endpoint(mut self, identity_endpoint: impl Into<String>) -> Self {
        self.identity_endpoint = Some(identity_endpoint.into());
        self
    }

    /// Test/deployment seam for the App Service managed-identity secret
    /// header. The value remains redacted from `Debug` and errors.
    #[doc(hidden)]
    pub fn with_identity_header(mut self, identity_header: impl Into<String>) -> Self {
        self.identity_header = Some(identity_header.into());
        self
    }

    /// Test/deployment seam for static Shared Key authentication. Production
    /// should prefer managed identity.
    #[doc(hidden)]
    pub fn with_account_key(mut self, encoded_key: impl Into<String>) -> Self {
        self.azure_options.insert(
            AzureConfigKey::AccessKey.as_ref().to_string(),
            encoded_key.into(),
        );
        self
    }

    /// Test/deployment seam for a pre-acquired bearer token.
    #[doc(hidden)]
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.azure_options
            .insert(AzureConfigKey::Token.as_ref().to_string(), token.into());
        self
    }

    fn capture_from_env() -> Result<Self> {
        let (environment_snapshot, azure_options) = capture_azure_environment()?;
        let account_name = required_option(
            &azure_options,
            AzureConfigKey::AccountName,
            "AZURE_STORAGE_ACCOUNT_NAME",
        )?;
        let endpoint = option(&azure_options, AzureConfigKey::Endpoint);
        let client_id = option(&azure_options, AzureConfigKey::ClientId);
        let use_emulator = option(&azure_options, AzureConfigKey::UseEmulator)
            .map(|value| parse_bool("AZURE_STORAGE_USE_EMULATOR", &value))
            .transpose()?
            .unwrap_or(false);
        let emulator_url = environment_snapshot
            .values
            .get("AZURITE_BLOB_STORAGE_URL")
            .cloned();
        let identity_endpoint = option(&azure_options, AzureConfigKey::MsiEndpoint);
        let identity_header = environment_snapshot.values.get("IDENTITY_HEADER").cloned();
        Ok(Self {
            account_name,
            endpoint,
            emulator_url,
            use_emulator,
            client_id,
            identity_endpoint,
            identity_header,
            azure_options,
            environment_snapshot: Some(environment_snapshot),
        })
    }

    fn verify_environment_unchanged(&self) -> Result<()> {
        if let Some(expected) = &self.environment_snapshot {
            expected.verify_unchanged()?;
        }
        Ok(())
    }
}

impl AzureEnvironmentSnapshot {
    fn verify_unchanged(&self) -> Result<()> {
        let (current, _) = capture_azure_environment()?;
        self.verify_matches(&current)
    }

    fn verify_matches(&self, current: &Self) -> Result<()> {
        if current == self {
            return Ok(());
        }
        let changed = self
            .values
            .keys()
            .chain(current.values.keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| self.values.get(*key) != current.values.get(*key))
            .cloned()
            .collect::<Vec<_>>();
        Err(StorageError::backend(
            StorageFailureKind::Configuration,
            format!(
                "Azure storage environment changed after process capture (changed keys: {}); restart the process",
                changed.join(", ")
            ),
        ))
    }
}

/// Strict, canonical Azure root plus one process-captured backend selection.
///
/// The public URI deliberately contains only container and object prefix. The
/// account and service endpoint are snapshotted separately from environment so
/// the same URI cannot drift between control-object and Lance clients while a
/// process is alive.
#[derive(Clone, PartialEq, Eq)]
pub struct CanonicalAzureRoot {
    canonical_uri: String,
    account_name: String,
    container: String,
    prefix: String,
    service_url: Url,
    storage_endpoint: String,
    use_emulator: bool,
    client_id: Option<String>,
    identity_endpoint: Option<String>,
    identity_header: Option<String>,
    azure_options: BTreeMap<String, String>,
    environment_snapshot: Option<AzureEnvironmentSnapshot>,
    root_digest_hex: String,
    admission_blob_path: String,
    admission_blob_uri: String,
}

impl fmt::Debug for CanonicalAzureRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CanonicalAzureRoot")
            .field("canonical_uri", &self.canonical_uri)
            .field("account_name", &self.account_name)
            .field("container", &self.container)
            .field("prefix", &self.prefix)
            .field("service_url", &self.service_url)
            .field("storage_endpoint", &self.storage_endpoint)
            .field("use_emulator", &self.use_emulator)
            .field("client_id", &self.client_id)
            .field("identity_endpoint", &self.identity_endpoint)
            .field(
                "identity_header",
                &self.identity_header.as_ref().map(|_| "<redacted>"),
            )
            .field("azure_options", &RedactedAzureOptions(&self.azure_options))
            .field(
                "environment_snapshot",
                &self.environment_snapshot.as_ref().map(|_| "<redacted>"),
            )
            .field("root_digest_hex", &self.root_digest_hex)
            .field("admission_blob_path", &self.admission_blob_path)
            .field("admission_blob_uri", &self.admission_blob_uri)
            .finish()
    }
}

impl CanonicalAzureRoot {
    /// Parse an Azure root using the process-wide Azure selection snapshot.
    ///
    /// The first call, including a failed call, fixes the selection for the
    /// process. Operators must restart after changing Azure location variables.
    pub fn from_env(root_uri: &str) -> Result<Self> {
        // URI validity is independent of environment and is checked before a
        // missing account can obscure a malformed or unsafe root.
        let canonical_uri = parse_azure_uri(root_uri)?.canonical_uri;
        static AZURE_CONFIG: OnceLock<std::result::Result<AzureStorageConfig, StorageFailure>> =
            OnceLock::new();
        let config = AZURE_CONFIG.get_or_init(|| {
            AzureStorageConfig::capture_from_env().map_err(|error| {
                StorageFailure::new(StorageFailureKind::Configuration, error.to_string())
            })
        });
        match config {
            Ok(config) => {
                config.verify_environment_unchanged()?;
                Self::from_config(&canonical_uri, config.clone())
            }
            Err(failure) => Err(StorageError::Backend(failure.clone())),
        }
    }

    /// Parse an Azure root against an explicit, immutable selection.
    pub fn from_config(root_uri: &str, config: AzureStorageConfig) -> Result<Self> {
        let (current_environment, ambient_options) = capture_azure_environment()?;
        if let Some(expected) = &config.environment_snapshot {
            expected.verify_matches(&current_environment)?;
        }
        let location = parse_azure_uri(root_uri)?;
        let account_name = validate_azure_account_name(&config.account_name)?;
        if config.use_emulator && config.endpoint.is_some() {
            return Err(StorageError::backend(
                StorageFailureKind::Configuration,
                "Azure emulator configuration cannot also set AZURE_STORAGE_ENDPOINT",
            ));
        }

        let service_url = if config.use_emulator {
            let value = config
                .emulator_url
                .as_deref()
                .unwrap_or(DEFAULT_AZURITE_BLOB_STORAGE_URL);
            parse_azure_service_url(value, "Azurite Blob service")?
        } else if let Some(endpoint) = config.endpoint.as_deref() {
            let endpoint = parse_azure_service_url(endpoint, "Azure Blob service endpoint")?;
            if endpoint.scheme() != "https" {
                return Err(StorageError::backend(
                    StorageFailureKind::Configuration,
                    "invalid Azure Blob service endpoint: HTTPS is required outside Azurite mode",
                ));
            }
            endpoint
        } else {
            let value = format!("https://{account_name}.blob.core.windows.net");
            parse_azure_service_url(&value, "Azure Blob service endpoint")?
        };

        // object_store 0.13.2's emulator branch has no endpoint option: it
        // rereads AZURITE_BLOB_STORAGE_URL at build time. Model Azurite as an
        // ordinary explicit HTTP endpoint instead, including the account path
        // that the emulator branch would otherwise append. This gives control
        // objects and Lance the exact same immutable endpoint.
        let storage_endpoint = if config.use_emulator {
            azure_service_url_with_segment(&service_url, &account_name)?.to_string()
        } else {
            service_url.to_string()
        };

        let mut azure_options = ambient_options;
        azure_options.extend(config.azure_options);
        azure_options.insert(
            AzureConfigKey::AccountName.as_ref().to_string(),
            account_name.clone(),
        );
        azure_options.insert(
            AzureConfigKey::Endpoint.as_ref().to_string(),
            storage_endpoint.clone(),
        );
        azure_options.insert(
            AzureConfigKey::UseEmulator.as_ref().to_string(),
            "false".to_string(),
        );
        azure_options.insert(
            AzureConfigKey::UseFabricEndpoint.as_ref().to_string(),
            "false".to_string(),
        );
        validate_azure_http_policy(
            config.use_emulator,
            &current_environment.values,
            &azure_options,
        )?;
        if config.use_emulator {
            azure_options.insert("allow_http".to_string(), "true".to_string());
        }
        let client_id = nonempty_optional(
            config
                .client_id
                .or_else(|| option(&azure_options, AzureConfigKey::ClientId)),
            "Azure client id",
        )?;
        if let Some(client_id) = &client_id {
            azure_options.insert(
                AzureConfigKey::ClientId.as_ref().to_string(),
                client_id.clone(),
            );
        }
        let identity_endpoint = config
            .identity_endpoint
            .or_else(|| option(&azure_options, AzureConfigKey::MsiEndpoint))
            .as_deref()
            .map(|value| parse_azure_service_url(value, "Azure managed-identity endpoint"))
            .transpose()?
            .map(|url| url.to_string());
        if let Some(identity_endpoint) = &identity_endpoint {
            azure_options.insert(
                AzureConfigKey::MsiEndpoint.as_ref().to_string(),
                identity_endpoint.clone(),
            );
        }

        let has_emulator_credential = [
            AzureConfigKey::AccessKey,
            AzureConfigKey::Token,
            AzureConfigKey::SasKey,
        ]
        .iter()
        .any(|key| azure_options.contains_key(key.as_ref()));
        let uses_default_emulator_key = config.use_emulator && !has_emulator_credential;
        if uses_default_emulator_key {
            azure_options.insert(
                AzureConfigKey::AccessKey.as_ref().to_string(),
                DEFAULT_AZURITE_ACCOUNT_KEY.to_string(),
            );
        }

        let digest_input = format!(
            "omnigraph-azure-root-v1\nservice={}\naccount={}\ncontainer={}\nroot={}",
            service_url.as_str(),
            account_name,
            location.container,
            location.canonical_uri
        );
        let root_digest_hex = sha256_hex(digest_input.as_bytes());
        let admission_blob_path = format!("{AZURE_ADMISSION_PREFIX}/{root_digest_hex}/writer.lock");
        let admission_blob_uri = format!("az://{}/{}", location.container, admission_blob_path);

        Ok(Self {
            canonical_uri: location.canonical_uri,
            account_name,
            container: location.container,
            prefix: location.key,
            service_url,
            storage_endpoint,
            use_emulator: config.use_emulator,
            client_id,
            identity_endpoint,
            identity_header: nonempty_optional(
                config
                    .identity_header
                    .or_else(|| current_environment.values.get("IDENTITY_HEADER").cloned()),
                "Azure managed-identity secret header",
            )?,
            azure_options,
            environment_snapshot: Some(current_environment),
            root_digest_hex,
            admission_blob_path,
            admission_blob_uri,
        })
    }

    pub fn canonical_uri(&self) -> &str {
        &self.canonical_uri
    }

    pub fn account_name(&self) -> &str {
        &self.account_name
    }

    pub fn container(&self) -> &str {
        &self.container
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Canonical Blob service base URL. In emulator mode this is the Azurite
    /// base before the required `/account/container` path components.
    pub fn endpoint(&self) -> &Url {
        &self.service_url
    }

    pub fn use_emulator(&self) -> bool {
        self.use_emulator
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn identity_endpoint(&self) -> Option<&str> {
        self.identity_endpoint.as_deref()
    }

    /// Refuse to construct another Azure client if any upstream-recognized
    /// Azure setting changed since the process snapshot was captured.
    pub fn verify_environment_unchanged(&self) -> Result<()> {
        if let Some(expected) = &self.environment_snapshot {
            expected.verify_unchanged()?;
        }
        Ok(())
    }

    pub fn root_digest_hex(&self) -> &str {
        &self.root_digest_hex
    }

    pub fn admission_blob_path(&self) -> &str {
        &self.admission_blob_path
    }

    pub fn admission_blob_uri(&self) -> &str {
        &self.admission_blob_uri
    }

    /// Exact REST URL for the reserved admission Blob, including Azurite's
    /// account path component when emulator mode is selected.
    pub fn admission_blob_url(&self) -> Result<Url> {
        self.object_url(self.admission_blob_path())
    }

    /// Exact REST URL for one container-relative object key.
    pub fn object_url(&self, object_path: &str) -> Result<Url> {
        let object_path = validate_relative_object_path(object_path)?;
        let mut url = self.service_url.clone();
        let mut segments = url.path_segments_mut().map_err(|_| {
            StorageError::backend(
                StorageFailureKind::Permanent,
                format!(
                    "Azure Blob service URL cannot be a base: '{}'",
                    self.service_url
                ),
            )
        })?;
        segments.pop_if_empty();
        if self.use_emulator {
            segments.push(&self.account_name);
        }
        segments.push(&self.container);
        for segment in object_path.split('/') {
            segments.push(segment);
        }
        drop(segments);
        Ok(url)
    }

    /// Complete captured options for constructing Lance's redacted
    /// [`StorageOptionsAccessor`](https://docs.rs/lance-io/latest/lance_io/object_store/struct.StorageOptionsAccessor.html).
    ///
    /// This narrow cross-crate seam intentionally includes credentials: if
    /// Lance filled them from its independent environment cache, its data
    /// client could authenticate differently from the control-object client.
    /// Callers must pass the map directly into the accessor and must never log
    /// or otherwise expose it. `StorageOptionsAccessor`'s own `Debug`
    /// implementation does not render option values.
    #[doc(hidden)]
    pub fn lance_storage_options(&self) -> Result<HashMap<String, String>> {
        self.verify_environment_unchanged()?;
        Ok(self
            .azure_options
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    /// Return the captured authentication shape for the dedicated Blob lease
    /// wrapper without exposing general credential getters.
    #[doc(hidden)]
    pub fn admission_credential(&self) -> Result<AzureAdmissionCredential> {
        self.verify_environment_unchanged()?;
        if self.use_emulator {
            let encoded_key = self
                .azure_options
                .get(AzureConfigKey::AccessKey.as_ref())
                .cloned()
                .ok_or_else(|| {
                    StorageError::backend(
                        StorageFailureKind::Configuration,
                        "Azure admission requires a Shared Key in emulator mode",
                    )
                })?;
            return Ok(AzureAdmissionCredential::SharedKey {
                account: self.account_name.clone(),
                encoded_key,
            });
        }
        if let Some(token) = self.azure_options.get(AzureConfigKey::Token.as_ref()) {
            return Ok(AzureAdmissionCredential::BearerToken(token.clone()));
        }
        if let Some(encoded_key) = self.azure_options.get(AzureConfigKey::AccessKey.as_ref()) {
            return Ok(AzureAdmissionCredential::SharedKey {
                account: self.account_name.clone(),
                encoded_key: encoded_key.clone(),
            });
        }
        let endpoint = self.identity_endpoint.clone().ok_or_else(|| {
            StorageError::backend(
                StorageFailureKind::Configuration,
                "IDENTITY_ENDPOINT is required for managed-identity admission",
            )
        })?;
        let secret_header = self.identity_header.clone().ok_or_else(|| {
            StorageError::backend(
                StorageFailureKind::Configuration,
                "IDENTITY_HEADER is required for managed-identity admission",
            )
        })?;
        Ok(AzureAdmissionCredential::ManagedIdentity {
            endpoint,
            secret_header,
            client_id: self.client_id.clone(),
        })
    }
}

#[async_trait]
pub trait StorageAdapter: Debug + Send + Sync {
    async fn read_text(&self, uri: &str) -> Result<String>;
    /// Read a text object if it exists using one backend GET.
    ///
    /// Returns `Ok(None)` only when the object store reports `NotFound`.
    /// Every other transport, permission, body-read, or UTF-8 failure remains
    /// a loud error. Callers must use this instead of `exists()` followed by
    /// `read_text()` when disappearance between the probe and read is a valid
    /// concurrent outcome.
    async fn read_text_if_exists(&self, uri: &str) -> Result<Option<String>>;
    /// Read at most `max_bytes + 1` bytes and refuse an oversized object
    /// before materializing its full body. `None` is reserved for NotFound.
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> Result<Option<String>>;
    /// Byte sibling of [`Self::read_text_if_exists_bounded`]: read a binary
    /// object of at most `max_bytes` in one backend GET; `None` is reserved
    /// for NotFound, and an oversized object is refused before its full body
    /// materializes. For binary artifacts (the graph-index adjacency) where
    /// UTF-8 decoding would be wrong, not just wasteful.
    async fn read_bytes_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> Result<Option<Vec<u8>>>;
    async fn write_text(&self, uri: &str, contents: &str) -> Result<()>;
    /// Byte sibling of [`Self::write_text`]: one PUT of a binary body under
    /// the same atomic-visibility backend contract (a reader sees the old
    /// object or the new one, never a truncated in-progress write).
    async fn write_bytes(&self, uri: &str, contents: &[u8]) -> Result<()>;
    /// Write a text object only if no object exists at `uri`.
    ///
    /// Returns `Ok(true)` when this call created the object, `Ok(false)`
    /// when the object already existed, and propagates every other storage
    /// error. Callers use this to establish ownership before running
    /// best-effort cleanup on partial failure.
    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool>;
    /// Return whether an exact object or any object recursively below this
    /// URI exists. The prefix probe stops after its first result and
    /// propagates every listing failure; only a successful empty listing is
    /// absence. This also supports directory-shaped dataset roots without
    /// relying on synthetic directory objects.
    async fn exists(&self, uri: &str) -> Result<bool>;
    /// Move a file from `from_uri` to `to_uri`, replacing any existing file at
    /// `to_uri`. Atomic on local POSIX; on S3 implemented as copy + delete
    /// (NOT atomic — callers that depend on atomicity for crash recovery must
    /// tolerate "both source and destination exist after a crash").
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()>;
    /// Remove a file. Returns Ok(()) if the file does not exist.
    async fn delete(&self, uri: &str) -> Result<()>;
    /// List all files (non-recursively, files only) directly under `dir_uri`.
    /// Returns full URIs (same scheme as `dir_uri`). The result is unordered.
    /// Returns Ok(empty) if the directory does not exist or is empty.
    /// Consumers must tolerate non-payload residue appearing in storage
    /// (backend staging files are filtered by the backend, but crash residue
    /// of any future producer may not be) — filter by suffix, never assume
    /// every entry is yours.
    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>>;
    /// Stream a non-recursive directory inventory and retain only direct files
    /// whose names end with `matching_suffix`.
    ///
    /// The first entry beyond any [`ListDirBounds`] member fails with typed
    /// [`StorageError::ResourceLimit`]; the method never returns a truncated
    /// success. Backends may retain one implementation-defined listing page,
    /// but this adapter does not collect the complete prefix before enforcing
    /// the bounds. Results use the same input-anchored URI shape and unordered
    /// contract as [`StorageAdapter::list_dir`]. A missing directory is empty.
    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: ListDirBounds,
    ) -> Result<Vec<String>>;
    /// Read a text object together with its backend version token (stores
    /// with conditional-update support: the object's ETag; local: sha256 of
    /// the content). The token is opaque — valid only for
    /// `write_text_if_match` against the same adapter.
    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)>;
    /// Replace the object at `uri` only if its current version still matches
    /// `expected_version` (obtained from a prior versioned read/write on this
    /// adapter). Returns `Ok(Some(new_version))` on success and `Ok(None)`
    /// when the precondition failed (a concurrent writer won — the CAS-lost
    /// case callers must surface, never swallow). Stores with conditional
    /// updates (S3, in-memory) use a true conditional put (If-Match); the
    /// local filesystem has no such primitive (`PutMode::Update` is
    /// unimplemented upstream), so local compares content then replaces via
    /// an atomic staged write — the same single-machine semantics the
    /// callers had before this trait, safe under the callers' own lock
    /// protocol but not a cross-process barrier by itself (see the Known
    /// Gaps entry in docs/dev/invariants.md).
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>>;
    /// Recursively delete every object under `prefix_uri`. Returns Ok(())
    /// when nothing exists there (idempotent). Local: `remove_dir_all`
    /// (directories are a local-FS concept; list+delete would leave empty
    /// directory skeletons that local existence probes report as present);
    /// object stores: list + delete (NOT atomic — callers must tolerate
    /// partial prefixes on crash, which the cluster delete protocol does by
    /// retry).
    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()>;
}

/// Version token for local files: content identity. The local filesystem
/// backend reports mtime-derived ETags too coarse for CAS (sub-granularity
/// rewrites collide); sha256 is stable, cheap at these object sizes, and
/// already the cluster ledger's CAS vocabulary.
fn local_version_token(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Local,
    S3,
    Azure,
}

/// Concrete storage selection for control-plane authority checks.
///
/// Unlike [`StorageAdapter`], this handle cannot be implemented by a caller:
/// it is minted only after this crate selects and initializes a real local,
/// S3, or Azure backend. Authority factories accept this concrete handle so an
/// in-memory/custom adapter cannot manufacture persisted cluster evidence.
#[derive(Debug, Clone)]
pub struct StorageHandle {
    adapter: Arc<ObjectStorageAdapter>,
    kind: StorageKind,
}

impl StorageHandle {
    pub fn kind(&self) -> StorageKind {
        self.kind
    }

    #[doc(hidden)]
    pub fn adapter(&self) -> Arc<dyn StorageAdapter> {
        self.adapter.clone()
    }
}

/// The one storage implementation: every backend is an
/// [`object_store::ObjectStore`], so the semantics (atomic-visibility puts,
/// conditional creates, path-delimited listing) are upstream-maintained and
/// identical across backends by construction. The per-backend residue is
/// confined to [`UriCodec`] (URI ↔ object path mapping) and the
/// `supports_conditional_update` capability flag (false only for the local
/// filesystem, where upstream `PutMode::Update` is unimplemented).
#[derive(Debug)]
pub struct ObjectStorageAdapter {
    store: Arc<DynObjectStore>,
    codec: UriCodec,
    /// Whether the backend implements `PutMode::Update` (ETag-conditioned
    /// put). Gates BOTH the version-token source in `read_text_versioned`
    /// and the `write_text_if_match` strategy — the two must agree or every
    /// CAS loses.
    supports_conditional_update: bool,
    #[cfg(test)]
    omit_read_etag: bool,
    #[cfg(test)]
    omit_write_etag: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UriCodec {
    /// Plain absolute/relative paths or `file://` URIs, mapped onto a
    /// root-anchored [`LocalFileSystem`].
    Local,
    /// `s3://{bucket}/{key}` URIs, mapped onto a bucket-scoped store.
    S3 { bucket: String },
    /// `az://{container}/{key}` URIs, mapped onto a container-scoped store.
    Azure { container: String },
    /// Opaque keys for the in-memory test/embedded backend; leading
    /// slashes are stripped.
    Memory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Location {
    bucket: String,
    key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AzureLocation {
    container: String,
    key: String,
    canonical_uri: String,
}

impl ObjectStorageAdapter {
    /// Local-filesystem backend rooted at `/`. URIs are plain paths or
    /// `file://` URIs; relative paths are lexically absolutized against the
    /// current working directory.
    pub fn local() -> Self {
        Self {
            store: Arc::new(LocalFileSystem::new()),
            codec: UriCodec::Local,
            supports_conditional_update: false,
            #[cfg(test)]
            omit_read_etag: false,
            #[cfg(test)]
            omit_write_etag: false,
        }
    }

    /// S3 backend scoped to the bucket named in `root_uri`. Credentials and
    /// endpoint come from the standard `AWS_*` environment variables (the
    /// same ones Lance reads for its dataset stores).
    pub fn s3_from_root_uri(root_uri: &str) -> Result<Self> {
        let location = parse_s3_uri(root_uri)?;
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&location.bucket);

        if let Some(endpoint) = env::var("AWS_ENDPOINT_URL_S3")
            .ok()
            .or_else(|| env::var("AWS_ENDPOINT_URL").ok())
        {
            builder = builder.with_endpoint(&endpoint);
            if endpoint.starts_with("http://") || env_var_truthy("AWS_ALLOW_HTTP") {
                builder = builder.with_allow_http(true);
            }
        }

        if env_var_truthy("AWS_S3_FORCE_PATH_STYLE") {
            builder = builder.with_virtual_hosted_style_request(false);
        }

        let store = builder.build().map_err(|err| {
            let kind = classify_object_store_error(&err);
            StorageError::backend(
                kind,
                format!(
                    "failed to initialize s3 storage for '{}': {}",
                    root_uri, err
                ),
            )
        })?;

        Ok(Self {
            store: Arc::new(store),
            codec: UriCodec::S3 {
                bucket: location.bucket,
            },
            supports_conditional_update: true,
            #[cfg(test)]
            omit_read_etag: false,
            #[cfg(test)]
            omit_write_etag: false,
        })
    }

    /// Azure backend selected from the process-wide canonical root snapshot.
    pub fn azure_from_root_uri(root_uri: &str) -> Result<Self> {
        let root = CanonicalAzureRoot::from_env(root_uri)?;
        Self::azure_from_root(&root)
    }

    /// Azure backend scoped to the canonical root's container.
    pub fn azure_from_root(root: &CanonicalAzureRoot) -> Result<Self> {
        root.verify_environment_unchanged()?;
        // Starting from `new` is load-bearing: `from_env` would refresh live
        // process state and could move control objects away from Lance after
        // CanonicalAzureRoot captured the backend selection.
        let mut builder = MicrosoftAzureBuilder::new();
        for (key, value) in &root.azure_options {
            let key = AzureConfigKey::from_str(key).map_err(|_| {
                StorageError::backend(
                    StorageFailureKind::Configuration,
                    format!("captured Azure option key '{key}' is not supported by object_store"),
                )
            })?;
            builder = builder.with_config(key, value.clone());
        }
        builder = builder
            .with_account(root.account_name())
            .with_container_name(root.container())
            .with_endpoint(root.storage_endpoint.clone())
            .with_use_emulator(false);
        let store = builder.build().map_err(|_| {
            StorageError::backend(
                StorageFailureKind::Configuration,
                format!(
                    "failed to initialize Azure storage for '{}'; verify the captured Azure configuration",
                    root.canonical_uri()
                ),
            )
        })?;
        Ok(Self {
            store: Arc::new(store),
            codec: UriCodec::Azure {
                container: root.container().to_string(),
            },
            supports_conditional_update: true,
            #[cfg(test)]
            omit_read_etag: false,
            #[cfg(test)]
            omit_write_etag: false,
        })
    }

    /// In-memory backend for tests and embedded experiments. Implements the
    /// FULL contract including true conditional updates (unlike the local
    /// filesystem), so contract tests exercise the strong-CAS path without a
    /// bucket. State lives only as long as the adapter.
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            codec: UriCodec::Memory,
            supports_conditional_update: true,
            #[cfg(test)]
            omit_read_etag: false,
            #[cfg(test)]
            omit_write_etag: false,
        }
    }

    fn object_path(&self, uri: &str) -> Result<ObjectPath> {
        match &self.codec {
            UriCodec::Local => {
                let path = absolutize_lexically(local_path_from_uri(uri)?)?;
                ObjectPath::from_absolute_path(&path).map_err(|err| {
                    StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!("invalid local object path for '{}': {}", uri, err),
                    )
                })
            }
            UriCodec::S3 { bucket } => {
                let location = parse_s3_uri(uri)?;
                if &location.bucket != bucket {
                    return Err(StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!(
                            "s3 storage bucket mismatch for '{}': expected '{}', found '{}'",
                            uri, bucket, location.bucket
                        ),
                    ));
                }
                if location.key.is_empty() {
                    return Err(StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!("s3 storage path is empty for '{}'", uri),
                    ));
                }
                ObjectPath::parse(&location.key).map_err(|err| {
                    StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!("invalid s3 object path for '{}': {}", uri, err),
                    )
                })
            }
            UriCodec::Azure { container } => {
                let location = parse_azure_uri(uri)?;
                if &location.container != container {
                    return Err(StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!(
                            "Azure storage container mismatch for '{}': expected '{}', found '{}'",
                            uri, container, location.container
                        ),
                    ));
                }
                if location.key.is_empty() {
                    return Err(StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!("Azure storage path is empty for '{}'", uri),
                    ));
                }
                ObjectPath::parse(&location.key).map_err(|err| {
                    StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!("invalid Azure object path for '{}': {}", uri, err),
                    )
                })
            }
            UriCodec::Memory => {
                // DST: accept the harness's scheme-carrying roots
                // (shared-memory://name/key) — that scheme is namespacing
                // only; the authority+path becomes the opaque key. Only
                // this one scheme, and only under `dst`: default builds
                // keep the base behavior (scheme-carrying keys error in
                // ObjectPath::parse).
                #[cfg(feature = "dst")]
                let key = uri.strip_prefix(SHARED_MEMORY_SCHEME_PREFIX).unwrap_or(uri);
                #[cfg(not(feature = "dst"))]
                let key = uri;
                ObjectPath::parse(key.trim_start_matches('/')).map_err(|err| {
                    StorageError::backend(
                        StorageFailureKind::Configuration,
                        format!("invalid memory object path for '{}': {}", uri, err),
                    )
                })
            }
        }
    }

    async fn read_azure_rename_part(
        &self,
        from: &ObjectPath,
        from_uri: &str,
        source_etag: &str,
        start: u64,
        end: u64,
    ) -> Result<PutPayload> {
        let expected = end.checked_sub(start).ok_or_else(|| {
            StorageError::backend(
                StorageFailureKind::Permanent,
                format!(
                    "storage rename_read failed for '{}': invalid range {start}..{end}",
                    redacted_storage_uri(from_uri)
                ),
            )
        })?;
        let result = self
            .store
            .get_opts(
                from,
                GetOptions::new()
                    .with_if_match(Some(source_etag.to_string()))
                    .with_range(Some(start..end)),
            )
            .await
            .map_err(|err| storage_backend_error("rename_read", from_uri, err))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|err| storage_backend_error("rename_read", from_uri, err))?;
        let actual = u64::try_from(bytes.len()).map_err(|_| {
            StorageError::backend(
                StorageFailureKind::Permanent,
                format!(
                    "storage rename_read failed for '{}': response length exceeds u64",
                    redacted_storage_uri(from_uri)
                ),
            )
        })?;
        if actual != expected {
            return Err(StorageError::backend(
                StorageFailureKind::Permanent,
                format!(
                    "storage rename_read failed for '{}': expected {expected} bytes for range \
                     {start}..{end}, received {actual}",
                    redacted_storage_uri(from_uri)
                ),
            ));
        }
        Ok(PutPayload::from(bytes))
    }

    async fn abort_azure_rename_after_error(
        upload: &mut Box<dyn object_store::MultipartUpload>,
        to_uri: &str,
        primary: StorageError,
    ) -> StorageError {
        match upload.abort().await {
            Ok(()) => primary,
            Err(abort_error) => StorageError::backend(
                StorageFailureKind::Unknown,
                format!(
                    "{primary}; storage rename_abort failed for '{}': {abort_error}",
                    redacted_storage_uri(to_uri)
                ),
            ),
        }
    }

    /// Map a non-already-exists `PutMode::Create` failure. The local backend
    /// publishes create-if-absent via `std::fs::hard_link` (omnigraph#453),
    /// so probing the destination directory distinguishes "this filesystem
    /// cannot do create-if-absent" from a generic backend failure.
    fn create_if_absent_error(&self, uri: &str, err: object_store::Error) -> StorageError {
        if matches!(self.codec, UriCodec::Local)
            && let Ok(path) = local_path_from_uri(uri)
            && let Ok(path) = absolutize_lexically(path)
            && let Some(dir) = path.parent()
            && let Some(link_error) = hard_link_refusal_in(dir)
        {
            let message = format!(
                "the filesystem at '{}' does not support hard links, which the local storage backend requires for atomic create-if-absent writes (seen on Android app storage, FAT/exFAT, and some network mounts); move the graph to a filesystem with hard-link support or use an S3-compatible backend: {}",
                dir.display(),
                link_error
            );
            return StorageError::io_context(link_error, message);
        }
        storage_backend_error("write_if_absent", uri, err)
    }

    /// DST-only bottom-count listing: every key currently held by the
    /// backing store, flat, no prefix filter. This reads BELOW every
    /// harness wrapper (the store itself is the one surface a bypass
    /// writer cannot avoid), so the DST write census can reconcile
    /// wrapper-recorded writes against ground truth. Hidden like the
    /// engine's dst seams; not part of the storage contract.
    ///
    /// UNBOUNDED and unscoped by design (census roots are universe-sized;
    /// a partial listing would hide exactly the bypass writes it exists
    /// to find) — census use on universe-scoped roots only; on a
    /// local-filesystem adapter it walks the store's whole root.
    ///
    /// # Errors
    /// When the backing store's listing fails.
    #[doc(hidden)]
    #[cfg(feature = "dst")]
    pub async fn dst_list_all_keys(&self) -> Result<Vec<String>> {
        let listing: Vec<object_store::ObjectMeta> = self
            .store
            .list(None)
            .try_collect()
            .await
            .map_err(|err| storage_backend_error("dst_list_all_keys", "<all>", err))?;
        Ok(listing
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }
}

/// Probe whether `dir` accepts new files but refuses `hard_link(2)` — the
/// signature of a filesystem without hard-link support. Returns the link
/// error only in that case; setup failures and "name taken" outcomes are
/// `None`, and callers keep their original error. Cleanup is best-effort;
/// leftover probe files fall under `list_dir`'s foreign-residue tolerance.
fn hard_link_refusal_in(dir: &Path) -> Option<std::io::Error> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static PROBE_SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = PROBE_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let src = dir.join(format!("__hardlink_probe_{pid}_{seq}_src"));
    let dst = dir.join(format!("__hardlink_probe_{pid}_{seq}_dst"));
    // O_EXCL creation: never follows a pre-placed entry at the predictable
    // path (symlink attack), which instead lands in the inconclusive arm.
    if std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&src)
        .is_err()
    {
        return None;
    }
    let outcome = std::fs::hard_link(&src, &dst);
    // Remove only entries this probe created: on a failed link, `dst` is
    // either absent or a foreign pre-placed entry that must survive.
    if outcome.is_ok() {
        let _ = std::fs::remove_file(&dst);
    }
    let _ = std::fs::remove_file(&src);
    match outcome {
        Ok(()) => None,
        Err(err) if is_hard_link_capability_refusal(&err) => Some(err),
        Err(_) => None,
    }
}

/// Errors that prove the destination directory cannot provide the hard-link
/// primitive used by the local backend's atomic create-if-absent path.
/// Everything else is inconclusive and must preserve the original backend
/// failure rather than replacing it with a filesystem-capability diagnosis.
fn is_hard_link_capability_refusal(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::Unsupported
    )
}

#[async_trait]
impl StorageAdapter for ObjectStorageAdapter {
    async fn read_text(&self, uri: &str) -> Result<String> {
        let location = self.object_path(uri)?;
        let bytes = self
            .store
            .get(&location)
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?
            .bytes()
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?;

        decode_storage_text(uri, bytes.as_ref())
    }

    async fn read_text_if_exists(&self, uri: &str) -> Result<Option<String>> {
        let location = self.object_path(uri)?;
        let result = match self.store.get(&location).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(storage_backend_error("read", uri, err)),
        };
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(storage_backend_error("read", uri, err)),
        };
        let text = decode_storage_text(uri, bytes.as_ref())?;
        Ok(Some(text))
    }

    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> Result<Option<String>> {
        let location = self.object_path(uri)?;
        let end = max_bytes.checked_add(1).ok_or_else(|| {
            StorageError::internal(format!(
                "bounded storage read limit overflows for '{uri}': {max_bytes}"
            ))
        })?;
        let bytes = match self.store.get_range(&location, 0..end).await {
            Ok(bytes) => bytes,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(storage_backend_error("bounded_read", uri, err)),
        };
        let actual = u64::try_from(bytes.len()).map_err(|_| {
            StorageError::internal(format!(
                "bounded storage read length exceeds u64 for '{uri}'"
            ))
        })?;
        if actual > max_bytes {
            return Err(StorageError::ResourceLimit {
                resource: "storage_text_bytes".to_string(),
                limit: max_bytes,
                actual,
                uri: uri.to_string(),
            });
        }
        let text = decode_storage_text(uri, bytes.as_ref())?;
        Ok(Some(text))
    }

    async fn read_bytes_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> Result<Option<Vec<u8>>> {
        let location = self.object_path(uri)?;
        let end = max_bytes.checked_add(1).ok_or_else(|| {
            StorageError::internal(format!(
                "bounded storage read limit overflows for '{uri}': {max_bytes}"
            ))
        })?;
        let bytes = match self.store.get_range(&location, 0..end).await {
            Ok(bytes) => bytes,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(storage_backend_error("bounded_read", uri, err)),
        };
        let actual = u64::try_from(bytes.len()).map_err(|_| {
            StorageError::internal(format!(
                "bounded storage read length exceeds u64 for '{uri}'"
            ))
        })?;
        if actual > max_bytes {
            return Err(StorageError::ResourceLimit {
                resource: "storage_bytes".to_string(),
                limit: max_bytes,
                actual,
                uri: uri.to_string(),
            });
        }
        Ok(Some(bytes.to_vec()))
    }

    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        // Atomic visibility is the backend's contract: object stores via
        // PutObject; LocalFileSystem via an internal staged-temp + rename
        // (a reader sees the old object or the new one, never a truncated
        // in-progress write). Callers (sidecar protocol, cluster state)
        // assume it.
        let location = self.object_path(uri)?;
        self.store
            .put(&location, PutPayload::from(contents.as_bytes().to_vec()))
            .await
            .map_err(|err| storage_backend_error("write", uri, err))?;
        Ok(())
    }

    async fn write_bytes(&self, uri: &str, contents: &[u8]) -> Result<()> {
        // Same atomic-visibility contract as `write_text`, binary body.
        let location = self.object_path(uri)?;
        self.store
            .put(&location, PutPayload::from(contents.to_vec()))
            .await
            .map_err(|err| storage_backend_error("write", uri, err))?;
        Ok(())
    }

    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool> {
        // PutMode::Create: atomic no-replace publish on every backend —
        // exactly one of N concurrent claimants wins, and the winner's
        // object is fully readable at the instant it becomes visible
        // (LocalFileSystem stages the temp file completely, then
        // hard_links it; pinned by
        // `local_write_text_if_absent_is_read_visible_on_return`).
        let location = self.object_path(uri)?;
        match self
            .store
            .put_opts(
                &location,
                PutPayload::from(contents.as_bytes().to_vec()),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => Ok(false),
            Err(err) => Err(self.create_if_absent_error(uri, err)),
        }
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        // head() answers for objects; the list fallback answers for
        // "directory-shaped" URIs (e.g. a Lance dataset root, whose
        // `_versions/*.manifest` makes any committed dataset non-empty).
        // Object-store semantics throughout: only objects exist —
        // an EMPTY local directory does not (callers that probe local
        // directories use std::fs directly).
        let location = self.object_path(uri)?;
        match self.store.head(&location).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => {
                let mut entries = self.store.list(Some(&location));
                let has_prefix_entries = entries
                    .try_next()
                    .await
                    .map_err(|err| storage_backend_error("exists", uri, err))?
                    .is_some();
                Ok(has_prefix_entries)
            }
            Err(err) => Err(storage_backend_error("exists", uri, err)),
        }
    }

    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
        let from = self.object_path(from_uri)?;
        let to = self.object_path(to_uri)?;
        if matches!(self.codec, UriCodec::Azure { .. }) {
            // Azure Copy Blob may complete asynchronously. Control-object
            // rename instead performs ETag-pinned bounded range GETs, a
            // visibility-complete PUT, and then DELETE. It is still
            // intentionally non-atomic: a crash after PUT can leave both
            // objects, which recovery owns. Multipart blocks are provider-
            // invisible until complete; every earlier failure aborts them.
            let source = self
                .store
                .head(&from)
                .await
                .map_err(|err| storage_backend_error("rename_read", from_uri, err))?;
            let source_etag = required_remote_etag("rename_read", from_uri, source.e_tag.clone())?;

            if source.size == 0 {
                self.store
                    .put(&to, PutPayload::default())
                    .await
                    .map_err(|err| storage_backend_error("rename_write", to_uri, err))?;
            } else if source.size <= AZURE_RENAME_PART_BYTES {
                let payload = self
                    .read_azure_rename_part(&from, from_uri, &source_etag, 0, source.size)
                    .await?;
                self.store
                    .put(&to, payload)
                    .await
                    .map_err(|err| storage_backend_error("rename_write", to_uri, err))?;
            } else {
                let mut upload = self
                    .store
                    .put_multipart(&to)
                    .await
                    .map_err(|err| storage_backend_error("rename_write", to_uri, err))?;
                let mut start = 0_u64;
                while start < source.size {
                    let end = match start
                        .checked_add(AZURE_RENAME_PART_BYTES)
                        .map(|end| end.min(source.size))
                    {
                        Some(end) => end,
                        None => {
                            let error = StorageError::backend(
                                StorageFailureKind::Permanent,
                                format!(
                                    "storage rename_read failed for '{}': source range arithmetic \
                                     overflow at {start} bytes",
                                    redacted_storage_uri(from_uri)
                                ),
                            );
                            return Err(Self::abort_azure_rename_after_error(
                                &mut upload,
                                to_uri,
                                error,
                            )
                            .await);
                        }
                    };
                    let payload = match self
                        .read_azure_rename_part(&from, from_uri, &source_etag, start, end)
                        .await
                    {
                        Ok(payload) => payload,
                        Err(error) => {
                            return Err(Self::abort_azure_rename_after_error(
                                &mut upload,
                                to_uri,
                                error,
                            )
                            .await);
                        }
                    };
                    if let Err(error) = upload.put_part(payload).await {
                        let error = storage_backend_error("rename_write", to_uri, error);
                        return Err(Self::abort_azure_rename_after_error(
                            &mut upload,
                            to_uri,
                            error,
                        )
                        .await);
                    }
                    start = end;
                }
                if let Err(error) = upload.complete().await {
                    let error = storage_backend_error("rename_write", to_uri, error);
                    return Err(
                        Self::abort_azure_rename_after_error(&mut upload, to_uri, error).await,
                    );
                }
            }
            self.store
                .delete(&from)
                .await
                .map_err(|err| storage_backend_error("rename_delete", from_uri, err))?;
            return Ok(());
        }
        // LocalFileSystem overrides rename with atomic fs::rename. S3 uses
        // copy + delete and may leave both names after a crash.
        self.store
            .rename(&from, &to)
            .await
            .map_err(|err| storage_backend_error("rename", from_uri, err))?;
        Ok(())
    }

    async fn delete(&self, uri: &str) -> Result<()> {
        let location = self.object_path(uri)?;
        match self.store.delete(&location).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(storage_backend_error("delete", uri, err)),
        }
    }

    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
        // list_with_delimiter is non-recursive and path-delimited on every
        // backend (no sibling-prefix bleed: listing `__recovery` cannot
        // match `__recovery_log/...`), and returns Ok(empty) for a missing
        // directory. Output URIs are anchored on the INPUT `dir_uri` plus
        // the entry filename, so the strings round-trip byte-identically
        // into read_text/delete regardless of scheme (plain path, file://,
        // s3://).
        let anchor = dir_uri.trim_end_matches('/');
        let prefix = self.object_path(anchor)?;
        let listing = self
            .store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|err| storage_backend_error("list_dir", dir_uri, err))?;
        let mut out = Vec::with_capacity(listing.objects.len());
        for meta in listing.objects {
            if let Some(name) = meta.location.filename() {
                out.push(format!("{}/{}", anchor, name));
            }
        }
        Ok(out)
    }

    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: ListDirBounds,
    ) -> Result<Vec<String>> {
        // `list_with_delimiter` collects every result page before returning.
        // The recursive `list` stream lets us stop at the first over-limit
        // entry instead. Descendants are classified as irrelevant so nested
        // residue cannot evade the scan bounds; only direct suffix matches
        // reach the returned inventory.
        let anchor = dir_uri.trim_end_matches('/');
        let prefix = self.object_path(anchor)?;
        let mut listing = self.store.list(Some(&prefix));
        let mut matching_entries = 0_u64;
        let mut irrelevant_entries = 0_u64;
        let mut uri_bytes = 0_u64;
        let max_matching_entries = u64::try_from(bounds.max_matching_entries).unwrap_or(u64::MAX);
        let max_irrelevant_entries =
            u64::try_from(bounds.max_irrelevant_entries).unwrap_or(u64::MAX);
        let limit_error = |resource: &str, limit: u64, actual: u64| StorageError::ResourceLimit {
            resource: resource.to_string(),
            limit,
            actual,
            uri: dir_uri.to_string(),
        };
        let anchor_bytes = u64::try_from(anchor.len())
            .map_err(|_| limit_error("storage_list_uri_bytes", bounds.max_uri_bytes, u64::MAX))?;
        let prefix_bytes = u64::try_from(prefix.as_ref().len())
            .map_err(|_| limit_error("storage_list_uri_bytes", bounds.max_uri_bytes, u64::MAX))?;
        let mut out = Vec::with_capacity(bounds.max_matching_entries.min(1024));

        while let Some(meta) = listing
            .try_next()
            .await
            .map_err(|err| storage_backend_error("bounded_list_dir", dir_uri, err))?
        {
            let mut parts = meta.location.prefix_match(&prefix).ok_or_else(|| {
                StorageError::internal(format!(
                    "bounded directory list for '{dir_uri}' returned out-of-prefix object '{}'",
                    meta.location
                ))
            })?;
            let Some(first_part) = parts.next() else {
                // ObjectStore::list excludes the exact prefix itself. Stay
                // defensive if a custom backend violates that contract
                // without charging or returning a phantom child.
                continue;
            };
            let is_direct_child = parts.next().is_none();
            let location_bytes = u64::try_from(meta.location.as_ref().len()).map_err(|_| {
                limit_error("storage_list_uri_bytes", bounds.max_uri_bytes, u64::MAX)
            })?;
            let relative_bytes = location_bytes
                .checked_sub(prefix_bytes)
                .and_then(|bytes| {
                    if prefix_bytes == 0 {
                        Some(bytes)
                    } else {
                        bytes.checked_sub(1)
                    }
                })
                .ok_or_else(|| {
                    StorageError::internal(format!(
                        "bounded directory list for '{dir_uri}' returned malformed child '{}'",
                        meta.location
                    ))
                })?;

            let entry_uri_bytes = anchor_bytes
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(relative_bytes))
                .ok_or_else(|| {
                    limit_error("storage_list_uri_bytes", bounds.max_uri_bytes, u64::MAX)
                })?;
            uri_bytes = uri_bytes.checked_add(entry_uri_bytes).ok_or_else(|| {
                limit_error("storage_list_uri_bytes", bounds.max_uri_bytes, u64::MAX)
            })?;
            if uri_bytes > bounds.max_uri_bytes {
                return Err(limit_error(
                    "storage_list_uri_bytes",
                    bounds.max_uri_bytes,
                    uri_bytes,
                ));
            }

            let direct_matching_name = is_direct_child
                .then_some(first_part.as_ref())
                .filter(|name| name.ends_with(matching_suffix));
            if let Some(name) = direct_matching_name {
                matching_entries = matching_entries.checked_add(1).ok_or_else(|| {
                    limit_error(
                        "storage_list_matching_entries",
                        max_matching_entries,
                        u64::MAX,
                    )
                })?;
                if matching_entries > max_matching_entries {
                    return Err(limit_error(
                        "storage_list_matching_entries",
                        max_matching_entries,
                        matching_entries,
                    ));
                }
                out.push(format!("{anchor}/{name}"));
            } else {
                irrelevant_entries = irrelevant_entries.checked_add(1).ok_or_else(|| {
                    limit_error(
                        "storage_list_irrelevant_entries",
                        max_irrelevant_entries,
                        u64::MAX,
                    )
                })?;
                if irrelevant_entries > max_irrelevant_entries {
                    return Err(limit_error(
                        "storage_list_irrelevant_entries",
                        max_irrelevant_entries,
                        irrelevant_entries,
                    ));
                }
            }
        }
        Ok(out)
    }

    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)> {
        let location = self.object_path(uri)?;
        let result = self
            .store
            .get(&location)
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?;
        let etag = result.meta.e_tag.clone();
        #[cfg(test)]
        let etag = if self.omit_read_etag { None } else { etag };
        let bytes = result
            .bytes()
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?;
        // The token SOURCE must agree with the write_text_if_match strategy
        // below: conditional-update backends compare ETags server-side, so
        // the token is the ETag; the local emulation compares content, so
        // the token is the content hash. Mixing them makes every CAS lose.
        let version = if self.supports_conditional_update {
            required_remote_etag("read", uri, etag)?
        } else {
            local_version_token(&bytes)
        };
        let text = decode_storage_text(uri, bytes.as_ref())?;
        Ok((text, version))
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>> {
        let location = self.object_path(uri)?;
        if self.supports_conditional_update {
            let mode = PutMode::Update(object_store::UpdateVersion {
                e_tag: Some(expected_version.to_string()),
                version: None,
            });
            return match self
                .store
                .put_opts(
                    &location,
                    PutPayload::from(contents.as_bytes().to_vec()),
                    mode.into(),
                )
                .await
            {
                Ok(result) => {
                    let etag = result.e_tag;
                    #[cfg(test)]
                    let etag = if self.omit_write_etag { None } else { etag };
                    Ok(Some(required_remote_etag("write_if_match", uri, etag)?))
                }
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(err) => Err(storage_backend_error("write_if_match", uri, err)),
            };
        }
        // Local emulation: content-compare then atomic replace. NOT a
        // cross-process CAS (check-then-act gap) — safe under the callers'
        // lock protocol only; tracked in docs/dev/invariants.md Known Gaps.
        let current = match self.store.get(&location).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|err| storage_backend_error("read", uri, err))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(storage_backend_error("read", uri, err)),
        };
        if local_version_token(&current) != expected_version {
            return Ok(None);
        }
        self.store
            .put(&location, PutPayload::from(contents.as_bytes().to_vec()))
            .await
            .map_err(|err| storage_backend_error("write_if_match", uri, err))?;
        Ok(Some(local_version_token(contents.as_bytes())))
    }

    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()> {
        // Directories are a local-FS concept: a list+delete loop would
        // leave empty directory skeletons that local existence probes
        // (cluster graph_root_exists uses std Path::exists) report as
        // still-present. remove_dir_all reclaims them in one call.
        if self.codec == UriCodec::Local {
            let path = absolutize_lexically(local_path_from_uri(prefix_uri)?)?;
            return match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(StorageError::io(err)),
            };
        }
        let prefix = self.object_path(prefix_uri.trim_end_matches('/'))?;
        let mut entries = self.store.list(Some(&prefix));
        let mut locations = Vec::new();
        while let Some(meta) = entries
            .try_next()
            .await
            .map_err(|err| storage_backend_error("delete_prefix", prefix_uri, err))?
        {
            locations.push(meta.location);
        }
        for location in locations {
            match self.store.delete(&location).await {
                Ok(()) => {}
                Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(storage_backend_error("delete_prefix", prefix_uri, err)),
            }
        }
        Ok(())
    }
}

pub fn storage_kind_for_uri(uri: &str) -> Result<StorageKind> {
    // `shared-memory://` is the DST harness's in-memory scheme (Lance's
    // shared-memory provider). Classified Local: universes need Local
    // layout/probe semantics while all IO is served by the injected
    // adapter. Gated on `dst`, so production builds refuse the scheme
    // like any unknown one.
    #[cfg(feature = "dst")]
    if uri.starts_with(SHARED_MEMORY_SCHEME_PREFIX) {
        return Ok(StorageKind::Local);
    }
    if uri.starts_with(S3_SCHEME_PREFIX) {
        Ok(StorageKind::S3)
    } else if uri.starts_with(AZURE_SCHEME_PREFIX) {
        Ok(StorageKind::Azure)
    } else if uri.starts_with(FILE_SCHEME_PREFIX)
        || !has_uri_scheme(uri)
        || is_windows_drive_path(uri)
    {
        Ok(StorageKind::Local)
    } else {
        let scheme = uri.split_once(':').map(|(scheme, _)| scheme).unwrap_or(uri);
        let diagnostic_uri = redacted_storage_uri(uri);
        Err(StorageError::backend(
            StorageFailureKind::Configuration,
            format!("unsupported storage URI scheme '{scheme}' in '{diagnostic_uri}'"),
        ))
    }
}

pub fn storage_for_uri(uri: &str) -> Result<Arc<dyn StorageAdapter>> {
    Ok(storage_handle_for_uri(uri)?.adapter())
}

/// Select a concrete backend for control-plane authority reads and locks.
pub fn storage_handle_for_uri(uri: &str) -> Result<StorageHandle> {
    match storage_kind_for_uri(uri)? {
        StorageKind::Local => Ok(StorageHandle {
            adapter: Arc::new(ObjectStorageAdapter::local()),
            kind: StorageKind::Local,
        }),
        StorageKind::S3 => Ok(StorageHandle {
            adapter: Arc::new(ObjectStorageAdapter::s3_from_root_uri(uri)?),
            kind: StorageKind::S3,
        }),
        StorageKind::Azure => Ok(StorageHandle {
            adapter: Arc::new(ObjectStorageAdapter::azure_from_root_uri(uri)?),
            kind: StorageKind::Azure,
        }),
    }
}

pub fn normalize_root_uri(uri: &str) -> Result<String> {
    // DST: Lance's `shared-memory://` scheme is opaque —
    // normalized like other object-store URIs, never as a local path.
    // Gated like the classification arm below, so production builds
    // refuse the scheme at every step.
    #[cfg(feature = "dst")]
    if uri.starts_with(SHARED_MEMORY_SCHEME_PREFIX) {
        return Ok(trim_trailing_slashes(uri));
    }
    match storage_kind_for_uri(uri)? {
        StorageKind::Local => {
            let path = local_path_from_uri(uri)?;
            Ok(normalize_local_path(&path))
        }
        StorageKind::S3 => Ok(trim_trailing_slashes(uri)),
        StorageKind::Azure => Ok(parse_azure_uri(uri)?.canonical_uri),
    }
}

/// Process-local identity used to share writer queues across handles for the
/// same graph root.
///
/// The storage URI remains unchanged: this identity is used only as the key in
/// the in-process queue registry. Local paths are first made absolute
/// lexically, then the deepest canonicalizable ancestor is resolved so aliases
/// through symlinks converge. Any suffix that does not exist yet is appended
/// unchanged, which makes the identity safe to compute before `init` creates
/// the graph directory. Object-store and caller-defined URI schemes are opaque
/// and retain their normalized spelling.
#[doc(hidden)]
pub fn write_queue_root_identity(normalized_root: &str) -> Result<String> {
    let local_path = if normalized_root.starts_with(FILE_SCHEME_PREFIX) {
        local_path_from_file_uri(normalized_root)?
    } else if Path::new(normalized_root).is_absolute() {
        PathBuf::from(normalized_root)
    } else if has_uri_scheme(normalized_root) {
        return Ok(normalized_root.to_string());
    } else {
        PathBuf::from(normalized_root)
    };

    let absolute = absolutize_lexically(local_path)?;
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();

    loop {
        if let Ok(canonical) = std::fs::canonicalize(ancestor) {
            let mut identity = canonical;
            for component in suffix.iter().rev() {
                identity.push(component);
            }
            return Ok(normalize_local_path(&identity));
        }

        let Some(name) = ancestor.file_name() else {
            // Filesystem roots should always canonicalize. Falling back to the
            // lexical absolute path preserves queue availability on unusual
            // platforms/filesystems without changing the storage root.
            return Ok(normalize_local_path(&absolute));
        };
        suffix.push(name.to_os_string());
        let Some(parent) = ancestor.parent() else {
            return Ok(normalize_local_path(&absolute));
        };
        ancestor = parent;
    }
}

pub fn join_uri(root_uri: &str, relative_path: &str) -> String {
    let relative_path = relative_path.trim_start_matches('/');
    match storage_kind_for_uri(root_uri) {
        Ok(StorageKind::S3 | StorageKind::Azure) => {
            let root = trim_trailing_slashes(root_uri);
            if root.is_empty() {
                relative_path.to_string()
            } else {
                format!("{}/{}", root, relative_path)
            }
        }
        Ok(StorageKind::Local) => {
            let root = if root_uri.starts_with(FILE_SCHEME_PREFIX) {
                local_path_from_file_uri(root_uri)
                    .map(|path| normalize_local_path(&path))
                    .unwrap_or_else(|_| trim_trailing_slashes(root_uri))
            } else {
                normalize_local_path(Path::new(root_uri))
            };
            let joined = Path::new(&root).join(relative_path);
            normalize_local_path(&joined)
        }
        Err(_) => {
            // Joining is intentionally infallible for legacy internal callers,
            // but an unknown URI is never reinterpreted as a filesystem path.
            // Backend selection/normalization remains the fail-closed gate.
            let root = trim_trailing_slashes(root_uri);
            format!("{root}/{relative_path}")
        }
    }
}

fn local_path_from_uri(uri: &str) -> Result<PathBuf> {
    if uri.starts_with(FILE_SCHEME_PREFIX) {
        return local_path_from_file_uri(uri);
    }
    Ok(PathBuf::from(uri))
}

fn has_uri_scheme(value: &str) -> bool {
    let Some(colon) = value.find(':') else {
        return false;
    };
    let scheme = &value[..colon];
    !scheme.is_empty()
        && scheme.as_bytes()[0].is_ascii_alphabetic()
        && scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

fn is_windows_drive_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

/// Lexically absolutize a local path: join relative paths onto the current
/// working directory and fold `.` / `..` components, without touching the
/// filesystem. Required because `object_store::path::Path` rejects
/// relative and dot segments, while callers (the CLI in particular) pass
/// paths like `./graph.omni` verbatim.
fn absolutize_lexically(path: PathBuf) -> Result<PathBuf> {
    let joined = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|err| {
                let message = format!(
                    "cannot resolve relative storage path '{}': {}",
                    path.display(),
                    err
                );
                StorageError::io_context(err, message)
            })?
            .join(path)
    };
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    Ok(out)
}

fn local_path_from_file_uri(uri: &str) -> Result<PathBuf> {
    let url = Url::parse(uri).map_err(|err| {
        StorageError::backend(
            StorageFailureKind::Configuration,
            format!("invalid file uri '{}': {}", uri, err),
        )
    })?;
    url.to_file_path().map_err(|_| {
        StorageError::backend(
            StorageFailureKind::Configuration,
            format!("invalid file uri '{}'", uri),
        )
    })
}

fn parse_s3_uri(uri: &str) -> Result<S3Location> {
    let url = Url::parse(uri).map_err(|err| {
        StorageError::backend(
            StorageFailureKind::Configuration,
            format!("invalid s3 uri '{}': {}", uri, err),
        )
    })?;
    if url.scheme() != "s3" {
        return Err(StorageError::backend(
            StorageFailureKind::Configuration,
            format!("unsupported s3 uri '{}'", uri),
        ));
    }
    let bucket = url.host_str().ok_or_else(|| {
        StorageError::backend(
            StorageFailureKind::Configuration,
            format!("missing s3 bucket in '{}'", uri),
        )
    })?;
    Ok(S3Location {
        bucket: bucket.to_string(),
        key: url.path().trim_start_matches('/').to_string(),
    })
}

fn storage_backend_error(action: &str, uri: &str, err: object_store::Error) -> StorageError {
    let kind = classify_object_store_error(&err);
    StorageError::backend(
        kind,
        format!("storage {} failed for '{}': {}", action, uri, err),
    )
}

fn decode_storage_text(uri: &str, bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|error| {
        StorageError::backend(
            StorageFailureKind::Permanent,
            format!("storage read failed for '{uri}': {error}"),
        )
    })
}

fn azure_configuration_error(message: impl Into<String>) -> StorageError {
    StorageError::backend(StorageFailureKind::Configuration, message)
}

fn parse_azure_uri(uri: &str) -> Result<AzureLocation> {
    let diagnostic_uri = redacted_storage_uri(uri);
    let remainder = uri.strip_prefix(AZURE_SCHEME_PREFIX).ok_or_else(|| {
        azure_configuration_error(format!(
            "unsupported Azure URI '{}': expected az://<container>[/<prefix>]",
            diagnostic_uri
        ))
    })?;
    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let raw_authority = &remainder[..authority_end];
    if raw_authority.is_empty() {
        return Err(azure_configuration_error(format!(
            "missing Azure container in '{}'",
            diagnostic_uri
        )));
    }
    let raw_path = &remainder[authority_end..];
    if raw_path.contains('\\') {
        return Err(azure_configuration_error(format!(
            "Azure URI path contains a backslash in '{}'",
            diagnostic_uri
        )));
    }

    let url = Url::parse(uri).map_err(|err| {
        azure_configuration_error(format!("invalid Azure URI '{}': {}", diagnostic_uri, err))
    })?;
    if url.scheme() != "az" {
        return Err(azure_configuration_error(format!(
            "unsupported Azure URI scheme in '{}'",
            diagnostic_uri
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(azure_configuration_error(format!(
            "Azure URI must not contain userinfo in '{}'",
            diagnostic_uri
        )));
    }
    if url.port().is_some() {
        return Err(azure_configuration_error(format!(
            "Azure URI must not contain a port in '{}'",
            diagnostic_uri
        )));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(azure_configuration_error(format!(
            "Azure URI must not contain a query or fragment in '{}'",
            diagnostic_uri
        )));
    }
    let container = url.host_str().ok_or_else(|| {
        azure_configuration_error(format!("missing Azure container in '{}'", diagnostic_uri))
    })?;
    // URL parsing normalizes hosts. Requiring the raw authority to equal the
    // validated container rejects account-qualified, case, IDNA, and escaped
    // aliases instead of giving one container multiple accepted spellings.
    if raw_authority != container {
        return Err(azure_configuration_error(format!(
            "Azure container authority is not canonical in '{}'",
            diagnostic_uri
        )));
    }
    validate_azure_container(container)?;

    let raw_path = raw_path.strip_prefix('/').unwrap_or(raw_path);
    let mut raw_segments = raw_path.split('/').collect::<Vec<_>>();
    if raw_segments.last() == Some(&"") {
        raw_segments.pop();
    }
    if raw_segments.iter().any(|segment| segment.is_empty()) {
        return Err(azure_configuration_error(format!(
            "Azure URI path contains an empty segment in '{}'",
            diagnostic_uri
        )));
    }

    let mut decoded_segments = Vec::with_capacity(raw_segments.len());
    for raw_segment in raw_segments {
        let segment = percent_decode_uri_segment(raw_segment, &diagnostic_uri)?;
        validate_azure_path_segment(&segment, &diagnostic_uri)?;
        decoded_segments.push(segment);
    }

    let mut canonical = Url::parse(&format!("az://{container}")).expect("validated Azure base");
    if !decoded_segments.is_empty() {
        let mut segments = canonical
            .path_segments_mut()
            .expect("az URLs are hierarchical");
        segments.pop_if_empty();
        for segment in &decoded_segments {
            segments.push(segment);
        }
    }
    let canonical_uri = canonical.as_str().trim_end_matches('/').to_string();
    Ok(AzureLocation {
        container: container.to_string(),
        key: decoded_segments.join("/"),
        canonical_uri,
    })
}

fn validate_azure_container(container: &str) -> Result<()> {
    let bytes = container.as_bytes();
    let valid = (3..=63).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        && !container.contains("--");
    if valid {
        Ok(())
    } else {
        Err(azure_configuration_error(format!(
            "invalid Azure container '{}': expected 3-63 lowercase letters, digits, or single hyphens, starting and ending with a letter or digit",
            container
        )))
    }
}

fn validate_azure_account_name(account: &str) -> Result<String> {
    let account = account.trim();
    let valid = (3..=24).contains(&account.len())
        && account
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
    if valid {
        Ok(account.to_string())
    } else {
        Err(azure_configuration_error(format!(
            "invalid AZURE_STORAGE_ACCOUNT_NAME '{}': expected 3-24 lowercase letters or digits",
            account
        )))
    }
}

fn percent_decode_uri_segment(segment: &str, uri: &str) -> Result<String> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(azure_configuration_error(format!(
                    "Azure URI contains an incomplete percent encoding in '{}'",
                    uri
                )));
            }
            let high = hex_value(bytes[index + 1]).ok_or_else(|| {
                azure_configuration_error(format!(
                    "Azure URI contains an invalid percent encoding in '{}'",
                    uri
                ))
            })?;
            let low = hex_value(bytes[index + 2]).ok_or_else(|| {
                azure_configuration_error(format!(
                    "Azure URI contains an invalid percent encoding in '{}'",
                    uri
                ))
            })?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| {
        azure_configuration_error(format!("Azure URI path is not valid UTF-8 in '{}'", uri))
    })
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn validate_azure_path_segment(segment: &str, uri: &str) -> Result<()> {
    if segment == "." || segment == ".." {
        return Err(azure_configuration_error(format!(
            "Azure URI path contains a dot segment in '{}'",
            uri
        )));
    }
    if segment.contains(['/', '\\']) {
        return Err(azure_configuration_error(format!(
            "Azure URI path contains an encoded separator in '{}'",
            uri
        )));
    }
    if segment.chars().any(char::is_control) {
        return Err(azure_configuration_error(format!(
            "Azure URI path contains a control character in '{}'",
            uri
        )));
    }
    Ok(())
}

fn validate_relative_object_path(path: &str) -> Result<&str> {
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        || path.contains('\\')
        || path.chars().any(char::is_control)
    {
        return Err(azure_configuration_error(format!(
            "invalid Azure container-relative object path '{}'",
            path
        )));
    }
    Ok(path)
}

fn parse_azure_service_url(value: &str, label: &str) -> Result<Url> {
    let mut url = Url::parse(value)
        .map_err(|_| azure_configuration_error(format!("invalid {label}: malformed URL")))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(azure_configuration_error(format!(
            "invalid {label}: expected an HTTP(S) base URL without credentials, query, or fragment"
        )));
    }
    let normalized_path = url.path().trim_end_matches('/').to_string();
    url.set_path(&normalized_path);
    Ok(url)
}

fn azure_service_url_with_segment(base: &Url, segment: &str) -> Result<Url> {
    let mut url = base.clone();
    let mut segments = url.path_segments_mut().map_err(|_| {
        azure_configuration_error("invalid Azure service endpoint: URL cannot be a base")
    })?;
    segments.pop_if_empty();
    segments.push(segment);
    drop(segments);
    Ok(url)
}

fn capture_azure_environment() -> Result<(AzureEnvironmentSnapshot, BTreeMap<String, String>)> {
    capture_azure_environment_values(env::vars_os().filter_map(|(raw_key, raw_value)| {
        Some((raw_key.into_string().ok()?, raw_value.into_string().ok()?))
    }))
}

fn capture_azure_environment_values(
    environment: impl IntoIterator<Item = (String, String)>,
) -> Result<(AzureEnvironmentSnapshot, BTreeMap<String, String>)> {
    let mut values = BTreeMap::new();
    let mut unsupported_aliases = Vec::new();
    for (key, value) in environment {
        let direct = matches!(
            key.as_str(),
            "AZURITE_BLOB_STORAGE_URL"
                | "IDENTITY_ENDPOINT"
                | "IDENTITY_HEADER"
                // Azure Container Apps may inject this deprecated alias
                // alongside IDENTITY_ENDPOINT. Capture it so normalization
                // can prove both clients received one identical endpoint.
                | "MSI_ENDPOINT"
                // Lance reads these outside AzureConfigKey. Capture them so
                // later client construction can detect drift and mirror the
                // effective HTTP allowance in the control adapter.
                | "AZURE_STORAGE_ALLOW_HTTP"
                | "AZURE_STORAGE_USE_HTTP"
                | "AWS_ALLOW_HTTP"
                | "OBJECT_STORE_CLIENT_MAX_RETRIES"
                | "OBJECT_STORE_CLIENT_RETRY_TIMEOUT"
        );
        let recognized = AzureConfigKey::from_str(&key.to_ascii_lowercase()).is_ok();
        if direct || (key.starts_with("AZURE_") && recognized) {
            values.insert(key, value);
        } else if recognized {
            // Lance accepts generic aliases such as TOKEN and ENDPOINT while
            // object_store::MicrosoftAzureBuilder::from_env deliberately does
            // not. Refuse them instead of letting unrelated process variables
            // become Azure credentials or silently split the two clients.
            unsupported_aliases.push(key);
        }
    }
    if values.contains_key("MSI_ENDPOINT") && !values.contains_key("IDENTITY_ENDPOINT") {
        // object_store's Azure builder reads IDENTITY_ENDPOINT directly while
        // Lance also recognizes MSI_ENDPOINT. Accept the legacy alias only as
        // a matching platform duplicate, never as an independent selector.
        unsupported_aliases.push("MSI_ENDPOINT".to_string());
    }
    if !unsupported_aliases.is_empty() {
        unsupported_aliases.sort();
        unsupported_aliases.dedup();
        return Err(azure_configuration_error(format!(
            "unsupported unprefixed Azure environment aliases detected: {}; use the documented AZURE_* names",
            unsupported_aliases.join(", ")
        )));
    }

    let normalized = normalize_azure_environment(&values)?;

    Ok((AzureEnvironmentSnapshot { values }, normalized))
}

fn normalize_azure_environment(
    values: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let mut normalized = BTreeMap::<String, (String, String)>::new();
    for (source, value) in values {
        let Ok(key) = AzureConfigKey::from_str(&source.to_ascii_lowercase()) else {
            continue;
        };
        let canonical = key.as_ref().to_string();
        if let Some((previous_source, previous_value)) = normalized.get(&canonical)
            && previous_value != value
        {
            return Err(azure_configuration_error(format!(
                "conflicting Azure storage environment aliases: {previous_source} and {source} differ"
            )));
        }
        normalized.insert(canonical, (source.clone(), value.clone()));
    }
    let mut normalized = normalized
        .into_iter()
        .map(|(key, (_, value))| (key, value))
        .collect::<BTreeMap<_, _>>();
    if let Some(allow_http) = lance_allow_http_override(values)? {
        normalized.insert("allow_http".to_string(), allow_http.to_string());
    }
    Ok(normalized)
}

fn lance_allow_http_override(values: &BTreeMap<String, String>) -> Result<Option<bool>> {
    let mut effective = None;
    // This is Lance StorageOptions::new's load order. Later values win.
    for key in [
        "AZURE_STORAGE_ALLOW_HTTP",
        "AZURE_STORAGE_USE_HTTP",
        "AWS_ALLOW_HTTP",
    ] {
        if let Some(value) = values.get(key) {
            effective = Some(parse_bool(key, value)?);
        }
    }
    Ok(effective)
}

fn validate_azure_http_policy(
    use_emulator: bool,
    environment: &BTreeMap<String, String>,
    options: &BTreeMap<String, String>,
) -> Result<()> {
    let process_override = lance_allow_http_override(environment)?;
    if use_emulator {
        if process_override == Some(false) {
            return Err(azure_configuration_error(
                "Azurite requires HTTP, but a process-wide Lance HTTP override disables it",
            ));
        }
        return Ok(());
    }

    let configured_override = options
        .get("allow_http")
        .map(|value| parse_bool("allow_http", value))
        .transpose()?;
    if process_override == Some(true) || configured_override == Some(true) {
        return Err(azure_configuration_error(
            "production Azure storage forbids HTTP allowances; unset AZURE_STORAGE_ALLOW_HTTP, \
             AZURE_STORAGE_USE_HTTP, and AWS_ALLOW_HTTP",
        ));
    }
    Ok(())
}

fn option(options: &BTreeMap<String, String>, key: AzureConfigKey) -> Option<String> {
    options.get(key.as_ref()).cloned()
}

fn required_option(
    options: &BTreeMap<String, String>,
    key: AzureConfigKey,
    environment_name: &str,
) -> Result<String> {
    option(options, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            azure_configuration_error(format!("{environment_name} is required for Azure storage"))
        })
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" | "y" => Ok(true),
        "0" | "false" | "off" | "no" | "n" => Ok(false),
        _ => Err(azure_configuration_error(format!(
            "invalid boolean value for {key}"
        ))),
    }
}

fn nonempty_optional(value: Option<String>, label: &str) -> Result<Option<String>> {
    match value {
        Some(value) if value.trim().is_empty() => Err(azure_configuration_error(format!(
            "{label} must not be empty"
        ))),
        value => Ok(value),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn required_remote_etag(action: &str, uri: &str, etag: Option<String>) -> Result<String> {
    etag.filter(|etag| !etag.is_empty()).ok_or_else(|| {
        StorageError::backend(
            StorageFailureKind::Permanent,
            format!(
                "storage {action} failed for '{uri}': remote backend omitted the required ETag"
            ),
        )
    })
}

fn normalize_local_path(path: &Path) -> String {
    let raw = path.as_os_str().to_string_lossy();
    if raw == "/" {
        return raw.to_string();
    }
    trim_trailing_slashes(&raw)
}

fn trim_trailing_slashes(value: &str) -> String {
    let trimmed = value.trim_end_matches('/');
    if trimmed.is_empty() {
        value.to_string()
    } else {
        trimmed.to_string()
    }
}

fn env_var_truthy(key: &str) -> bool {
    matches!(
        env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "True" | "yes" | "YES" | "on" | "ON")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;
    use std::fmt;

    #[derive(Debug)]
    struct OpaqueError;

    impl fmt::Display for OpaqueError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("opaque")
        }
    }

    impl StdError for OpaqueError {}

    #[derive(Debug)]
    struct SourceLink {
        source: Box<dyn StdError + Send + Sync>,
    }

    impl fmt::Display for SourceLink {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("source link")
        }
    }

    impl StdError for SourceLink {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(self.source.as_ref())
        }
    }

    #[derive(Debug)]
    struct CyclicError;

    impl fmt::Display for CyclicError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("cycle")
        }
    }

    impl StdError for CyclicError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(self)
        }
    }

    fn boxed_io(kind: std::io::ErrorKind) -> Box<dyn StdError + Send + Sync> {
        Box::new(std::io::Error::new(kind, "typed source"))
    }

    fn source_chain(
        links: usize,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Box<dyn StdError + Send + Sync> {
        (0..links).fold(source, |source, _| Box::new(SourceLink { source }))
    }

    fn generic(source: Box<dyn StdError + Send + Sync>) -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source,
        }
    }

    fn spawn_status_server(
        status: u16,
        response_count: usize,
    ) -> (String, tokio::task::JoinHandle<usize>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::{Duration, Instant};

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind local status server");
        listener
            .set_nonblocking(true)
            .expect("configure local status server");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("local server address")
        );
        let server = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut served = 0;
            while served < response_count {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "status server accept timed out");
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("status server accept failed: {error}"),
                };
                stream
                    .set_nonblocking(false)
                    .expect("configure blocking status connection");
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("configure status server read timeout");
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .expect("configure status server write timeout");

                let mut request = [0_u8; 16 * 1024];
                let mut request_len = 0;
                loop {
                    assert!(
                        request_len < request.len(),
                        "status server request headers exceeded test bound"
                    );
                    let read = stream
                        .read(&mut request[request_len..])
                        .expect("read local status request");
                    assert!(read != 0, "status request ended before its headers");
                    request_len += read;
                    if request[..request_len]
                        .windows(4)
                        .any(|window| window == b"\r\n\r\n")
                    {
                        break;
                    }
                }
                assert!(
                    request[..request_len].starts_with(b"GET "),
                    "status evidence must come from a real object-store GET"
                );
                write!(
                    stream,
                    "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .expect("write local status response");
                stream.flush().expect("flush local status response");
                served += 1;
            }
            served
        });
        (endpoint, server)
    }

    async fn exhausted_s3_status_error(status: u16) -> object_store::Error {
        use std::time::Duration;

        const EXPECTED_REQUESTS: usize = 2;
        let (endpoint, server) = spawn_status_server(status, EXPECTED_REQUESTS);
        let store = AmazonS3Builder::new()
            .with_bucket_name("test-bucket")
            .with_region("us-east-1")
            .with_access_key_id("test-access-key")
            .with_secret_access_key("test-secret-key")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_retry(object_store::RetryConfig {
                backoff: object_store::BackoffConfig {
                    init_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(1),
                    base: 2.0,
                },
                max_retries: 1,
                retry_timeout: Duration::from_secs(2),
            })
            .build()
            .expect("build local S3 status client");
        let error = store
            .get(&ObjectPath::from("key"))
            .await
            .expect_err("status response must fail the object-store GET");
        assert_eq!(
            server.await.expect("join local status server"),
            EXPECTED_REQUESTS,
            "object_store must exhaust its configured retry before surfacing status {status}"
        );
        error
    }

    #[test]
    fn storage_failure_is_transient_only_for_transient_kind() {
        for kind in [
            StorageFailureKind::Configuration,
            StorageFailureKind::NotFound,
            StorageFailureKind::Precondition,
            StorageFailureKind::Permanent,
            StorageFailureKind::Unknown,
        ] {
            assert!(!StorageFailure::new(kind, "diagnostic").is_transient());
        }
        assert!(StorageFailure::new(StorageFailureKind::Transient, "diagnostic").is_transient());
    }

    #[test]
    fn io_error_kind_matrix_is_narrow_and_exhaustive_for_the_contract() {
        use std::io::ErrorKind;

        for kind in [
            ErrorKind::TimedOut,
            ErrorKind::Interrupted,
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::BrokenPipe,
            ErrorKind::NotConnected,
            ErrorKind::HostUnreachable,
            ErrorKind::NetworkUnreachable,
            ErrorKind::WouldBlock,
        ] {
            assert_eq!(
                classify_io_error(&std::io::Error::new(kind, "test")),
                StorageFailureKind::Transient,
                "{kind:?}"
            );
        }
        for kind in [
            ErrorKind::PermissionDenied,
            ErrorKind::InvalidInput,
            ErrorKind::Unsupported,
            ErrorKind::StorageFull,
            ErrorKind::QuotaExceeded,
            ErrorKind::ReadOnlyFilesystem,
            ErrorKind::FileTooLarge,
        ] {
            assert_eq!(
                classify_io_error(&std::io::Error::new(kind, "test")),
                StorageFailureKind::Configuration,
                "{kind:?}"
            );
        }
        assert_eq!(
            classify_io_error(&std::io::Error::new(ErrorKind::NotFound, "test")),
            StorageFailureKind::NotFound
        );
        assert_eq!(
            classify_io_error(&std::io::Error::new(ErrorKind::AlreadyExists, "test")),
            StorageFailureKind::Precondition
        );
        assert_eq!(
            classify_io_error(&std::io::Error::new(ErrorKind::InvalidData, "test")),
            StorageFailureKind::Permanent
        );
        assert_eq!(
            classify_io_error(&std::io::Error::other("test")),
            StorageFailureKind::Unknown
        );
    }

    #[test]
    fn local_io_adapter_failures_keep_their_complete_message_and_kind() {
        use std::io::ErrorKind;

        for (error_kind, failure_kind) in [
            (ErrorKind::TimedOut, StorageFailureKind::Transient),
            (
                ErrorKind::PermissionDenied,
                StorageFailureKind::Configuration,
            ),
            (ErrorKind::NotFound, StorageFailureKind::NotFound),
            (ErrorKind::AlreadyExists, StorageFailureKind::Precondition),
            (ErrorKind::InvalidData, StorageFailureKind::Permanent),
            (ErrorKind::Other, StorageFailureKind::Unknown),
        ] {
            let error = StorageError::from(std::io::Error::new(error_kind, "local failure"));
            let StorageError::Io { failure, source } = error else {
                panic!("local storage I/O must retain its structured source")
            };
            assert_eq!(failure.kind, failure_kind, "{error_kind:?}");
            assert_eq!(failure.message, "io: local failure");
            assert_eq!(failure.to_string(), "io: local failure");
            assert_eq!(source.kind(), error_kind);
        }

        let raw = std::io::Error::from_raw_os_error(28);
        let expected_kind = classify_io_error(&raw);
        let error = StorageError::io(raw);
        let StorageError::Io { failure, source } = error else {
            panic!("local storage I/O must retain its structured source")
        };
        assert_eq!(failure.kind, expected_kind);
        assert_eq!(source.raw_os_error(), Some(28));
    }

    #[test]
    fn object_store_variant_matrix_uses_only_typed_evidence() {
        use object_store::Error;

        let cases = [
            (
                Error::NotFound {
                    path: "key".to_string(),
                    source: boxed_io(std::io::ErrorKind::Other),
                },
                StorageFailureKind::NotFound,
            ),
            (
                Error::NotModified {
                    path: "key".to_string(),
                    source: boxed_io(std::io::ErrorKind::Other),
                },
                StorageFailureKind::Precondition,
            ),
            (
                Error::Precondition {
                    path: "key".to_string(),
                    source: boxed_io(std::io::ErrorKind::Other),
                },
                StorageFailureKind::Precondition,
            ),
            (
                Error::AlreadyExists {
                    path: "key".to_string(),
                    source: boxed_io(std::io::ErrorKind::Other),
                },
                StorageFailureKind::Precondition,
            ),
            (
                Error::InvalidPath {
                    source: object_store::path::Error::EmptySegment {
                        path: "bad//path".to_string(),
                    },
                },
                StorageFailureKind::Configuration,
            ),
            (
                Error::NotSupported {
                    source: boxed_io(std::io::ErrorKind::Other),
                },
                StorageFailureKind::Configuration,
            ),
            (
                Error::NotImplemented {
                    operation: "op".to_string(),
                    implementer: "test".to_string(),
                },
                StorageFailureKind::Configuration,
            ),
            (
                Error::PermissionDenied {
                    path: "key".to_string(),
                    source: boxed_io(std::io::ErrorKind::Other),
                },
                StorageFailureKind::Configuration,
            ),
            (
                Error::Unauthenticated {
                    path: "key".to_string(),
                    source: boxed_io(std::io::ErrorKind::Other),
                },
                StorageFailureKind::Configuration,
            ),
            (
                Error::UnknownConfigurationKey {
                    store: "test",
                    key: "unknown".to_string(),
                },
                StorageFailureKind::Configuration,
            ),
            (
                generic(boxed_io(std::io::ErrorKind::TimedOut)),
                StorageFailureKind::Transient,
            ),
            (
                generic(Box::new(object_store::client::HttpError::new(
                    object_store::client::HttpErrorKind::Connect,
                    OpaqueError,
                ))),
                StorageFailureKind::Transient,
            ),
            (
                generic(Box::new(object_store::client::HttpError::new(
                    object_store::client::HttpErrorKind::Request,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "inner timeout"),
                ))),
                StorageFailureKind::Unknown,
            ),
            (
                generic(Box::new(object_store::client::HttpError::new(
                    object_store::client::HttpErrorKind::Timeout,
                    OpaqueError,
                ))),
                StorageFailureKind::Transient,
            ),
            (
                generic(Box::new(object_store::client::HttpError::new(
                    object_store::client::HttpErrorKind::Interrupted,
                    OpaqueError,
                ))),
                StorageFailureKind::Transient,
            ),
            (
                generic(Box::new(object_store::client::HttpError::new(
                    object_store::client::HttpErrorKind::Decode,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "inner timeout"),
                ))),
                StorageFailureKind::Unknown,
            ),
            (
                generic(Box::new(object_store::client::HttpError::new(
                    object_store::client::HttpErrorKind::Unknown,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "inner timeout"),
                ))),
                StorageFailureKind::Unknown,
            ),
            (generic(Box::new(OpaqueError)), StorageFailureKind::Unknown),
        ];

        for (error, expected) in cases {
            assert_eq!(classify_object_store_error(&error), expected, "{error}");
        }
    }

    #[tokio::test]
    async fn object_store_join_errors_distinguish_cancellation_and_panic() {
        let cancelled_task = tokio::spawn(async { std::future::pending::<()>().await });
        cancelled_task.abort();
        let cancelled = object_store::Error::JoinError {
            source: cancelled_task.await.unwrap_err(),
        };
        assert_eq!(
            classify_object_store_error(&cancelled),
            StorageFailureKind::Transient
        );

        let panicked_task = tokio::spawn(async { panic!("typed test panic") });
        let panicked = object_store::Error::JoinError {
            source: panicked_task.await.unwrap_err(),
        };
        assert_eq!(
            classify_object_store_error(&panicked),
            StorageFailureKind::Permanent
        );
    }

    #[tokio::test]
    async fn exhausted_http_status_wrappers_remain_unknown_without_public_typed_evidence() {
        // object_store 0.13.2 retains these status codes in a private retry
        // wrapper. The controlled server proves the status supplied to the
        // public client; classification must not recover it from display text.
        for status in [408, 429, 500, 502, 503, 504] {
            let error = exhausted_s3_status_error(status).await;
            assert!(
                matches!(&error, object_store::Error::Generic { .. }),
                "status {status} must exercise object_store's opaque exhausted-retry wrapper"
            );
            assert_eq!(
                classify_object_store_error(&error),
                StorageFailureKind::Unknown,
                "private status evidence must remain unknown for {status}"
            );
        }
    }

    #[test]
    fn typed_source_walk_accepts_depth_seven_and_bounds_depth_eight_and_cycles() {
        let classify = |source: &(dyn std::error::Error + 'static)| {
            find_storage_source_kind_with(source, no_additional_storage_source)
                .unwrap_or(StorageFailureKind::Unknown)
        };
        let depth_seven = source_chain(7, boxed_io(std::io::ErrorKind::TimedOut));
        assert_eq!(
            classify(depth_seven.as_ref()),
            StorageFailureKind::Transient
        );

        let depth_eight = source_chain(8, boxed_io(std::io::ErrorKind::TimedOut));
        assert_eq!(classify(depth_eight.as_ref()), StorageFailureKind::Unknown);
        let http_depth_seven = source_chain(
            7,
            Box::new(object_store::client::HttpError::new(
                object_store::client::HttpErrorKind::Timeout,
                OpaqueError,
            )),
        );
        assert_eq!(
            classify(http_depth_seven.as_ref()),
            StorageFailureKind::Transient
        );
        let http_depth_eight = source_chain(
            8,
            Box::new(object_store::client::HttpError::new(
                object_store::client::HttpErrorKind::Timeout,
                OpaqueError,
            )),
        );
        assert_eq!(
            classify(http_depth_eight.as_ref()),
            StorageFailureKind::Unknown
        );
        assert_eq!(classify(&CyclicError), StorageFailureKind::Unknown);
        assert_eq!(
            classify_object_store_error(&generic(Box::new(CyclicError))),
            StorageFailureKind::Unknown
        );
    }

    #[test]
    fn adapter_failure_keeps_complete_historical_message() {
        let error = storage_backend_error(
            "read",
            "memory://graph/object",
            object_store::Error::NotFound {
                path: "object".to_string(),
                source: Box::new(OpaqueError),
            },
        );
        let StorageError::Backend(failure) = error else {
            panic!("backend failures must be typed")
        };
        assert_eq!(failure.kind, StorageFailureKind::NotFound);
        assert_eq!(
            failure.message,
            "storage read failed for 'memory://graph/object': Object at location object not found: opaque"
        );
        assert_eq!(failure.to_string(), failure.message);
    }
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutResult, UploadPart,
    };
    use std::ops::Range;
    use std::sync::Mutex;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum AzureRenameFault {
        None,
        MissingHeadEtag,
        ChangeSourceBeforeSecondRange,
        FailPart(usize),
        FailComplete,
        FailList,
    }

    #[derive(Debug, Default)]
    struct AzureRenameProbe {
        ranges: Mutex<Vec<(Range<u64>, Option<String>)>>,
        multipart_creates: std::sync::atomic::AtomicUsize,
        aborts: Arc<std::sync::atomic::AtomicUsize>,
        completes: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[derive(Debug)]
    struct AzureRenameFaultStore {
        inner: Arc<InMemory>,
        fault: AzureRenameFault,
        probe: Arc<AzureRenameProbe>,
    }

    impl std::fmt::Display for AzureRenameFaultStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("azure-rename-fault-store")
        }
    }

    #[derive(Debug)]
    struct AzureRenameFaultUpload {
        inner: Box<dyn MultipartUpload>,
        fault: AzureRenameFault,
        part_index: usize,
        aborts: Arc<std::sync::atomic::AtomicUsize>,
        completes: Arc<std::sync::atomic::AtomicUsize>,
    }

    fn injected_object_store_error(operation: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "azure-rename-fault-store",
            source: Box::new(std::io::Error::other(format!(
                "injected {operation} failure"
            ))),
        }
    }

    #[async_trait]
    impl MultipartUpload for AzureRenameFaultUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let index = self.part_index;
            self.part_index += 1;
            if self.fault == AzureRenameFault::FailPart(index) {
                return Box::pin(async { Err(injected_object_store_error("multipart part")) });
            }
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.completes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self.fault == AzureRenameFault::FailComplete {
                return Err(injected_object_store_error("multipart complete"));
            }
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.abort().await
        }
    }

    #[async_trait]
    impl ObjectStore for AzureRenameFaultStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.probe
                .multipart_creates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let inner = self.inner.put_multipart_opts(location, options).await?;
            Ok(Box::new(AzureRenameFaultUpload {
                inner,
                fault: self.fault,
                part_index: 0,
                aborts: Arc::clone(&self.probe.aborts),
                completes: Arc::clone(&self.probe.completes),
            }))
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let is_head = options.head;
            let bounded_range = match options.range.as_ref() {
                Some(GetRange::Bounded(range)) => Some(range.clone()),
                _ => None,
            };
            if let Some(range) = bounded_range {
                let change_source = {
                    let mut ranges = self.probe.ranges.lock().unwrap();
                    ranges.push((range, options.if_match.clone()));
                    self.fault == AzureRenameFault::ChangeSourceBeforeSecondRange
                        && ranges.len() == 2
                };
                if change_source {
                    let replacement =
                        vec![b'z'; usize::try_from(AZURE_RENAME_PART_BYTES + 17).unwrap()];
                    self.inner
                        .put(location, PutPayload::from(replacement))
                        .await?;
                }
            }
            let mut result = self.inner.get_opts(location, options).await?;
            if is_head && self.fault == AzureRenameFault::MissingHeadEtag {
                result.meta.e_tag = None;
            }
            Ok(result)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            if self.fault == AzureRenameFault::FailList {
                return Box::pin(futures::stream::once(async {
                    Err(injected_object_store_error("list"))
                }));
            }
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn azure_rename_fault_adapter(
        fault: AzureRenameFault,
    ) -> (ObjectStorageAdapter, Arc<AzureRenameProbe>) {
        let probe = Arc::new(AzureRenameProbe::default());
        let store: Arc<DynObjectStore> = Arc::new(AzureRenameFaultStore {
            inner: Arc::new(InMemory::new()),
            fault,
            probe: Arc::clone(&probe),
        });
        (
            ObjectStorageAdapter {
                store,
                codec: UriCodec::Azure {
                    container: "container".to_string(),
                },
                supports_conditional_update: true,
                omit_read_etag: false,
                omit_write_etag: false,
            },
            probe,
        )
    }

    #[tokio::test]
    async fn directory_shaped_exists_propagates_listing_failure() {
        let (adapter, _) = azure_rename_fault_adapter(AzureRenameFault::FailList);
        let error = adapter
            .exists("az://container/graphs/knowledge.omni")
            .await
            .expect_err("a failed recursive probe must not become absence");
        let StorageError::Backend(failure) = error else {
            panic!("object-store listing failures must stay typed");
        };
        assert_eq!(failure.kind, StorageFailureKind::Unknown);
        assert!(failure.message.contains("injected list failure"));
    }

    /// The executable backend contract: every assertion here must hold for
    /// EVERY backend (the divergence class this adapter closed was "two
    /// implementations, one prose contract, no referee"). The S3 variant
    /// runs bucket-gated in `tests/s3_storage.rs`
    /// (`s3_adapter_conditional_writes_contract`).
    async fn contract_suite(adapter: &dyn StorageAdapter, root: &str) {
        // Write/read round-trip; replace is in-place and atomic.
        let a = format!("{root}/contract/a.json");
        adapter.write_text(&a, "v1").await.unwrap();
        assert_eq!(adapter.read_text(&a).await.unwrap(), "v1");
        assert_eq!(
            adapter.read_text_if_exists(&a).await.unwrap().as_deref(),
            Some("v1")
        );
        adapter.write_text(&a, "v2").await.unwrap();
        assert_eq!(adapter.read_text(&a).await.unwrap(), "v2");
        assert_eq!(
            adapter
                .read_text_if_exists(&format!("{root}/contract/missing.json"))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            adapter
                .read_text_if_exists_bounded(&a, 2)
                .await
                .unwrap()
                .as_deref(),
            Some("v2")
        );
        let bounded = adapter
            .read_text_if_exists_bounded(&a, 1)
            .await
            .expect_err("the bounded reader must refuse before reading the full body");
        assert!(matches!(
            bounded,
            StorageError::ResourceLimit {
                ref resource,
                limit: 1,
                actual: 2,
                ref uri,
            } if resource == "storage_text_bytes" && uri == &a
        ));
        assert_eq!(
            adapter
                .read_text_if_exists_bounded(&format!("{root}/contract/missing-bounded.json"), 2,)
                .await
                .unwrap(),
            None
        );

        // exists: object yes; missing no; non-empty prefix yes (the
        // directory-shaped probe Lance dataset roots rely on).
        assert!(adapter.exists(&a).await.unwrap());
        assert!(
            !adapter
                .exists(&format!("{root}/contract/missing.json"))
                .await
                .unwrap()
        );
        assert!(adapter.exists(&format!("{root}/contract")).await.unwrap());

        // A recursive prefix probe must remain path-component delimited. A
        // sibling with a longer, byte-sharing name is not the requested
        // dataset root.
        let sibling_only = format!("{root}/graphical/__manifest/latest");
        adapter.write_text(&sibling_only, "manifest").await.unwrap();
        assert!(!adapter.exists(&format!("{root}/graph")).await.unwrap());

        // if_absent: exactly one claim wins; the loser leaves the winner's
        // object untouched.
        let claim = format!("{root}/contract/claim.json");
        assert!(adapter.write_text_if_absent(&claim, "first").await.unwrap());
        assert!(
            !adapter
                .write_text_if_absent(&claim, "second")
                .await
                .unwrap()
        );
        assert_eq!(adapter.read_text(&claim).await.unwrap(), "first");

        // Versioned CAS: fresh token wins, stale token loses with Ok(None)
        // (never a silent overwrite), missing object can't match.
        let state = format!("{root}/contract/state.json");
        adapter.write_text(&state, "s1").await.unwrap();
        let (text, v1) = adapter.read_text_versioned(&state).await.unwrap();
        assert_eq!(text, "s1");
        let v2 = adapter
            .write_text_if_match(&state, "s2", &v1)
            .await
            .unwrap()
            .expect("fresh token must win");
        assert_ne!(v2, v1);
        assert!(
            adapter
                .write_text_if_match(&state, "s3", &v1)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(adapter.read_text(&state).await.unwrap(), "s2");
        assert!(
            adapter
                .write_text_if_match(&format!("{root}/contract/absent.json"), "x", &v1)
                .await
                .unwrap()
                .is_none()
        );

        // rename: destination is replaced; source is gone.
        let src = format!("{root}/contract/src.json");
        adapter.write_text(&src, "moved").await.unwrap();
        adapter.rename_text(&src, &a).await.unwrap();
        assert_eq!(adapter.read_text(&a).await.unwrap(), "moved");
        assert!(!adapter.exists(&src).await.unwrap());

        // list_dir: direct children only, no sibling-prefix bleed, output
        // URIs round-trip verbatim into read_text, missing dir is empty.
        let dir_uri = format!("{root}/contract/list");
        adapter
            .write_text(&format!("{dir_uri}/one.json"), "1")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{dir_uri}/two.json"), "2")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{dir_uri}/sub/three.json"), "3")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{root}/contract/list_log/x.json"), "x")
            .await
            .unwrap();
        let mut listed = adapter.list_dir(&dir_uri).await.unwrap();
        listed.sort();
        assert_eq!(
            listed,
            vec![format!("{dir_uri}/one.json"), format!("{dir_uri}/two.json")]
        );
        for uri in &listed {
            adapter.read_text(uri).await.unwrap();
        }
        assert!(
            adapter
                .list_dir(&format!("{root}/contract/nope"))
                .await
                .unwrap()
                .is_empty()
        );

        // delete: idempotent.
        adapter.delete(&claim).await.unwrap();
        assert_eq!(adapter.read_text_if_exists(&claim).await.unwrap(), None);
        adapter.delete(&claim).await.unwrap();
        assert!(!adapter.exists(&claim).await.unwrap());

        // delete_prefix: recursive + idempotent; nothing under the prefix
        // (including local directory skeletons) survives.
        adapter
            .delete_prefix(&format!("{root}/contract"))
            .await
            .unwrap();
        assert!(!adapter.exists(&a).await.unwrap());
        assert!(!adapter.exists(&format!("{root}/contract")).await.unwrap());
        adapter
            .delete_prefix(&format!("{root}/contract"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn contract_suite_local() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        contract_suite(&adapter, dir.path().to_str().unwrap()).await;
    }

    #[tokio::test]
    async fn contract_suite_in_memory() {
        // InMemory implements true conditional updates, so this runs the
        // strong-CAS path (ETag tokens + PutMode::Update) without a bucket.
        let adapter = ObjectStorageAdapter::in_memory();
        contract_suite(&adapter, "mem-root").await;
    }

    #[tokio::test]
    async fn contract_suite_azure_when_configured() {
        let Ok(container) = env::var("OMNIGRAPH_AZURE_TEST_CONTAINER") else {
            eprintln!("skipping Azure storage contract: OMNIGRAPH_AZURE_TEST_CONTAINER is not set");
            return;
        };
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root_uri = format!(
            "az://{container}/omnigraph-storage-contract-{}-{nonce}",
            std::process::id()
        );
        let root = CanonicalAzureRoot::from_env(&root_uri).unwrap();
        let adapter = Arc::new(ObjectStorageAdapter::azure_from_root(&root).unwrap());
        contract_suite(adapter.as_ref(), root.canonical_uri()).await;

        // The provider-level race is part of Azure's contract: exactly one
        // If-None-Match claimant wins and the losing payload is never visible.
        let claim = format!("{}/concurrent-claim.json", root.canonical_uri());
        let first = {
            let adapter = Arc::clone(&adapter);
            let claim = claim.clone();
            tokio::spawn(async move { adapter.write_text_if_absent(&claim, "first").await })
        };
        let second = {
            let adapter = Arc::clone(&adapter);
            let claim = claim.clone();
            tokio::spawn(async move { adapter.write_text_if_absent(&claim, "second").await })
        };
        let outcomes = [
            first.await.unwrap().unwrap(),
            second.await.unwrap().unwrap(),
        ];
        assert_eq!(outcomes.into_iter().filter(|won| *won).count(), 1);
        assert!(matches!(
            adapter.read_text(&claim).await.unwrap().as_str(),
            "first" | "second"
        ));

        // Cross the multipart threshold against the actual Azure adapter,
        // not only the in-memory fault seam used by the focused unit tests.
        let large_source = format!("{}/large-rename-source.json", root.canonical_uri());
        let large_destination = format!("{}/large-rename-destination.json", root.canonical_uri());
        let large_payload = "x".repeat(usize::try_from(AZURE_RENAME_PART_BYTES + 17).unwrap());
        adapter
            .write_text(&large_source, &large_payload)
            .await
            .unwrap();
        adapter
            .rename_text(&large_source, &large_destination)
            .await
            .unwrap();
        assert_eq!(
            adapter.read_text(&large_destination).await.unwrap(),
            large_payload
        );
        assert!(!adapter.exists(&large_source).await.unwrap());
        adapter.delete_prefix(root.canonical_uri()).await.unwrap();
    }

    #[tokio::test]
    async fn bounded_list_returns_only_direct_suffix_matches() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        let dir_uri = format!("{}/bounded-filter", dir.path().display());
        let matching = format!("{dir_uri}/one.json");
        let irrelevant = format!("{dir_uri}/residue.tmp");
        adapter.write_text(&matching, "1").await.unwrap();
        adapter.write_text(&irrelevant, "r").await.unwrap();
        adapter
            .write_text(&format!("{dir_uri}/nested/two.json"), "2")
            .await
            .unwrap();

        let listed = adapter
            .list_dir_bounded(
                &dir_uri,
                ".json",
                ListDirBounds {
                    max_matching_entries: 1,
                    max_irrelevant_entries: 2,
                    max_uri_bytes: u64::MAX,
                },
            )
            .await
            .unwrap();
        assert_eq!(listed, vec![matching.clone()]);

        // The existing unbounded API keeps its direct-child, unfiltered
        // contract; introducing the bounded path must not change it.
        let mut unbounded = adapter.list_dir(&dir_uri).await.unwrap();
        unbounded.sort();
        assert_eq!(unbounded, vec![matching, irrelevant]);
        assert!(
            adapter
                .list_dir_bounded(
                    &format!("{}/bounded-filter-missing", dir.path().display()),
                    ".json",
                    ListDirBounds {
                        max_matching_entries: 0,
                        max_irrelevant_entries: 0,
                        max_uri_bytes: 0,
                    },
                )
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn bounded_list_refuses_the_first_excess_matching_entry() {
        let adapter = ObjectStorageAdapter::in_memory();
        let dir_uri = "bounded-matches";
        for index in 0..257 {
            let name = format!("{index:03}.json");
            adapter
                .write_text(&format!("{dir_uri}/{name}"), &name)
                .await
                .unwrap();
        }

        let error = adapter
            .list_dir_bounded(
                dir_uri,
                ".json",
                ListDirBounds {
                    max_matching_entries: 256,
                    max_irrelevant_entries: 0,
                    max_uri_bytes: u64::MAX,
                },
            )
            .await
            .expect_err("the 257th match must refuse instead of returning a truncated list");
        assert!(matches!(
            error,
            StorageError::ResourceLimit {
                ref resource,
                limit: 256,
                actual: 257,
                ref uri,
            } if resource == "storage_list_matching_entries" && uri == dir_uri
        ));
        assert_eq!(adapter.list_dir(dir_uri).await.unwrap().len(), 257);
    }

    #[tokio::test]
    async fn bounded_list_counts_direct_and_nested_residue_as_irrelevant() {
        let adapter = ObjectStorageAdapter::in_memory();
        let dir_uri = "bounded-residue";
        adapter
            .write_text(&format!("{dir_uri}/keep.json"), "k")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{dir_uri}/direct.tmp"), "d")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{dir_uri}/nested/residue.json"), "n")
            .await
            .unwrap();

        let error = adapter
            .list_dir_bounded(
                dir_uri,
                ".json",
                ListDirBounds {
                    max_matching_entries: 1,
                    max_irrelevant_entries: 1,
                    max_uri_bytes: u64::MAX,
                },
            )
            .await
            .expect_err("nested objects must not evade the irrelevant-entry bound");
        assert!(matches!(
            error,
            StorageError::ResourceLimit {
                ref resource,
                limit: 1,
                actual: 2,
                ref uri,
            } if resource == "storage_list_irrelevant_entries" && uri == dir_uri
        ));
    }

    #[tokio::test]
    async fn bounded_list_caps_cumulative_input_anchored_uri_bytes() {
        let adapter = ObjectStorageAdapter::in_memory();
        let dir_uri = "bounded-uri-bytes";
        let first = format!("{dir_uri}/one.json");
        let second = format!("{dir_uri}/two.json");
        adapter.write_text(&first, "1").await.unwrap();
        adapter.write_text(&second, "2").await.unwrap();
        let exact_bytes = u64::try_from(first.len() + second.len()).unwrap();
        let bounds = |max_uri_bytes| ListDirBounds {
            max_matching_entries: 2,
            max_irrelevant_entries: 0,
            max_uri_bytes,
        };

        let mut listed = adapter
            .list_dir_bounded(dir_uri, ".json", bounds(exact_bytes))
            .await
            .unwrap();
        listed.sort();
        assert_eq!(listed, vec![first, second]);

        let error = adapter
            .list_dir_bounded(dir_uri, ".json", bounds(exact_bytes - 1))
            .await
            .expect_err("the aggregate URI bytes must include every encountered object");
        assert!(matches!(
            error,
            StorageError::ResourceLimit {
                ref resource,
                limit,
                actual,
                ref uri,
            } if resource == "storage_list_uri_bytes"
                && limit == exact_bytes - 1
                && actual == exact_bytes
                && uri == dir_uri
        ));
    }

    async fn assert_bounded_list_prefix_and_uri_contract(
        adapter: &ObjectStorageAdapter,
        dir_uri: &str,
        sibling_dir_uri: &str,
    ) {
        let matching = format!("{dir_uri}/one.json");
        let direct_residue = format!("{dir_uri}/direct.tmp");
        let nested_residue = format!("{dir_uri}/nested/two.json");
        let sibling = format!("{sibling_dir_uri}/must-not-bleed.json");
        adapter.write_text(&matching, "1").await.unwrap();
        adapter.write_text(&direct_residue, "d").await.unwrap();
        adapter.write_text(&nested_residue, "n").await.unwrap();
        adapter.write_text(&sibling, "s").await.unwrap();

        // Every object genuinely below the requested directory is charged in
        // its input-anchored URI form. The similarly named sibling is outside
        // the path-delimited prefix and must neither be returned nor consume
        // either bound.
        let exact_uri_bytes =
            u64::try_from(matching.len() + direct_residue.len() + nested_residue.len()).unwrap();
        let bounds = |max_uri_bytes| ListDirBounds {
            max_matching_entries: 1,
            max_irrelevant_entries: 2,
            max_uri_bytes,
        };
        let listed = adapter
            .list_dir_bounded(dir_uri, ".json", bounds(exact_uri_bytes))
            .await
            .unwrap();
        assert_eq!(listed, vec![matching.clone()]);
        assert_eq!(adapter.read_text(&listed[0]).await.unwrap(), "1");

        let error = adapter
            .list_dir_bounded(dir_uri, ".json", bounds(exact_uri_bytes - 1))
            .await
            .expect_err("the exact input-anchored URI-byte boundary must be enforced");
        assert!(matches!(
            error,
            StorageError::ResourceLimit {
                ref resource,
                limit,
                actual,
                ref uri,
            } if resource == "storage_list_uri_bytes"
                && limit == exact_uri_bytes - 1
                && actual == exact_uri_bytes
                && uri == dir_uri
        ));
    }

    #[tokio::test]
    async fn bounded_list_is_path_delimited_and_uri_exact_for_local_file_and_s3_shapes() {
        let plain_dir = tempfile::tempdir().unwrap();
        let plain_adapter = ObjectStorageAdapter::local();
        let plain_root = plain_dir.path().join("plain");
        assert_bounded_list_prefix_and_uri_contract(
            &plain_adapter,
            &format!("{}/__recovery", plain_root.display()),
            &format!("{}/__recovery_log", plain_root.display()),
        )
        .await;

        let file_dir = tempfile::tempdir().unwrap();
        let file_adapter = ObjectStorageAdapter::local();
        let file_root = file_dir.path().join("file scheme with space");
        assert_bounded_list_prefix_and_uri_contract(
            &file_adapter,
            &format!("file://{}/__recovery", file_root.display()),
            &format!("file://{}/__recovery_log", file_root.display()),
        )
        .await;

        // Exercise the S3 URI codec without credentials/network. InMemory has
        // the same component-aware ObjectStore::list contract; the production
        // S3 implementation additionally sends the wire prefix with a trailing
        // slash before streaming pages.
        let s3_adapter = ObjectStorageAdapter {
            store: Arc::new(InMemory::new()),
            codec: UriCodec::S3 {
                bucket: "bounded-list-bucket".to_string(),
            },
            supports_conditional_update: true,
            omit_read_etag: false,
            omit_write_etag: false,
        };
        assert_bounded_list_prefix_and_uri_contract(
            &s3_adapter,
            "s3://bounded-list-bucket/graph/__recovery",
            "s3://bounded-list-bucket/graph/__recovery_log",
        )
        .await;
    }

    #[tokio::test]
    async fn read_text_if_exists_keeps_non_not_found_errors_loud() {
        let adapter = ObjectStorageAdapter::in_memory();
        let uri = "invalid-utf8.json";
        let location = adapter.object_path(uri).unwrap();
        adapter
            .store
            .put(&location, PutPayload::from(vec![0xff]))
            .await
            .unwrap();

        let error = adapter
            .read_text_if_exists(uri)
            .await
            .expect_err("invalid UTF-8 is not absence and must remain loud");
        assert!(
            error.to_string().contains("storage read failed"),
            "unexpected optional-read error: {error}"
        );
    }

    /// `write_text_if_absent` must make the contents visible to any
    /// subsequent reader before it returns — callers acknowledge
    /// success the moment it resolves (cluster state bootstrap reads
    /// the file back; init ownership claims depend on it).
    /// Regression: the previous hand-rolled local adapter wrote through a
    /// buffered `tokio::fs::File` without flushing, so the bytes could
    /// still be in flight on the blocking pool while a reader saw an empty
    /// or partial file. Reads back through `std::fs` deliberately —
    /// cross-API visibility is the point.
    #[tokio::test]
    async fn local_write_text_if_absent_is_read_visible_on_return() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        let payload = "x".repeat(8 * 1024);
        for i in 0..1000 {
            let path = dir.path().join(format!("obj-{i}.json"));
            let uri = format!("{}", path.display());
            assert!(adapter.write_text_if_absent(&uri, &payload).await.unwrap());
            let read = std::fs::read_to_string(&path).unwrap();
            assert_eq!(
                read.len(),
                payload.len(),
                "iteration {i}: write_text_if_absent returned before its \
                 contents reached the file"
            );
        }
    }

    /// Regression for the write_text_if_absent buffering bug, via the
    /// `storage_for_uri` + `file://` construction path and a multi-thread
    /// runtime (complements `local_write_text_if_absent_is_read_visible_-
    /// on_return`, which uses the direct constructor and plain paths): a
    /// reader immediately after Ok(true) must never see the created file
    /// empty or short.
    #[tokio::test(flavor = "multi_thread")]
    async fn write_text_if_absent_is_read_consistent_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = storage_for_uri(&format!("file://{}", dir.path().display())).unwrap();
        let payload = "x".repeat(64 * 1024);
        for i in 0..200 {
            let uri = format!("file://{}/f{}.json", dir.path().display(), i);
            assert!(adapter.write_text_if_absent(&uri, &payload).await.unwrap());
            let read = std::fs::read_to_string(dir.path().join(format!("f{i}.json"))).unwrap();
            assert_eq!(read.len(), payload.len(), "iteration {i}: short read");
        }
    }

    /// Object-store semantics on the local filesystem: only objects exist.
    /// An empty directory is not an object and not a non-empty prefix —
    /// callers that genuinely probe local directories use std::fs.
    #[tokio::test]
    async fn local_exists_is_object_semantics_for_directories() {
        let dir = tempfile::tempdir().unwrap();
        let probe = dir.path().join("maybe-dataset");
        let adapter = ObjectStorageAdapter::local();
        std::fs::create_dir(&probe).unwrap();
        assert!(
            !adapter.exists(probe.to_str().unwrap()).await.unwrap(),
            "an empty directory is not an object"
        );
        std::fs::write(probe.join("1.manifest"), "m").unwrap();
        assert!(
            adapter.exists(probe.to_str().unwrap()).await.unwrap(),
            "a non-empty prefix exists (the Lance dataset-root probe shape)"
        );
    }

    /// list_dir output is anchored on the INPUT dir_uri, so `file://`
    /// anchors and paths with spaces round-trip byte-identically into
    /// read_text — the cluster store passes file://-schemed roots.
    #[tokio::test]
    async fn local_list_round_trips_file_scheme_and_spaces() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("with space");
        let adapter = ObjectStorageAdapter::local();
        let plain = format!("{}/x.json", root.display());
        adapter.write_text(&plain, "x").await.unwrap();

        let listed = adapter.list_dir(root.to_str().unwrap()).await.unwrap();
        assert_eq!(listed, vec![plain.clone()]);
        assert_eq!(adapter.read_text(&listed[0]).await.unwrap(), "x");

        let file_anchor = format!("file://{}", root.display());
        let listed = adapter.list_dir(&file_anchor).await.unwrap();
        assert_eq!(listed, vec![format!("{file_anchor}/x.json")]);
        assert_eq!(adapter.read_text(&listed[0]).await.unwrap(), "x");
    }

    /// Relative and dot-segment paths are lexically absolutized before
    /// hitting the object-path layer (which rejects them) — the CLI passes
    /// `./graph.omni`-shaped URIs verbatim.
    #[tokio::test]
    async fn local_paths_with_dot_segments_are_absolutized() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        let uri = format!("{}/sub/../dotted.json", dir.path().display());
        adapter.write_text(&uri, "x").await.unwrap();
        assert_eq!(adapter.read_text(&uri).await.unwrap(), "x");
        assert!(dir.path().join("dotted.json").exists());
    }

    /// Upstream local rename creates missing destination parents — more
    /// lenient than the previous bare fs::rename; pinned so an upstream
    /// regression is loud.
    #[tokio::test]
    async fn local_rename_creates_missing_destination_parents() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        let src = format!("{}/src.json", dir.path().display());
        adapter.write_text(&src, "x").await.unwrap();
        let dst = format!("{}/new-sub/dst.json", dir.path().display());
        adapter.rename_text(&src, &dst).await.unwrap();
        assert_eq!(adapter.read_text(&dst).await.unwrap(), "x");
    }

    #[tokio::test]
    async fn azure_rename_uses_bounded_etag_pinned_ranges_and_handles_empty() {
        let (adapter, probe) = azure_rename_fault_adapter(AzureRenameFault::None);
        let source = "az://container/source.json";
        let destination = "az://container/destination.json";
        let payload = "x".repeat(usize::try_from(AZURE_RENAME_PART_BYTES + 17).unwrap());
        adapter.write_text(source, &payload).await.unwrap();

        adapter.rename_text(source, destination).await.unwrap();

        assert_eq!(adapter.read_text(destination).await.unwrap(), payload);
        assert!(!adapter.exists(source).await.unwrap());
        let ranges = probe.ranges.lock().unwrap().clone();
        assert_eq!(
            ranges
                .iter()
                .map(|(range, _)| range.clone())
                .collect::<Vec<_>>(),
            vec![
                0..AZURE_RENAME_PART_BYTES,
                AZURE_RENAME_PART_BYTES..AZURE_RENAME_PART_BYTES + 17,
            ]
        );
        assert!(ranges.iter().all(|(range, etag)| range.end - range.start
            <= AZURE_RENAME_PART_BYTES
            && etag.as_deref().is_some_and(|etag| !etag.is_empty())));
        assert!(ranges.windows(2).all(|pair| pair[0].1 == pair[1].1));
        assert_eq!(
            probe
                .multipart_creates
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            probe.completes.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(probe.aborts.load(std::sync::atomic::Ordering::Relaxed), 0);

        let empty_source = "az://container/empty-source.json";
        let empty_destination = "az://container/empty-destination.json";
        adapter.write_text(empty_source, "").await.unwrap();
        adapter
            .rename_text(empty_source, empty_destination)
            .await
            .unwrap();
        assert_eq!(adapter.read_text(empty_destination).await.unwrap(), "");
        assert!(!adapter.exists(empty_source).await.unwrap());
        assert_eq!(
            probe
                .multipart_creates
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "zero-byte rename must use one atomic empty PUT, not multipart"
        );
    }

    #[tokio::test]
    async fn azure_rename_requires_head_etag_before_writing() {
        let (adapter, probe) = azure_rename_fault_adapter(AzureRenameFault::MissingHeadEtag);
        let source = "az://container/source.json";
        let destination = "az://container/destination.json";
        adapter.write_text(source, "payload").await.unwrap();

        let error = adapter.rename_text(source, destination).await.unwrap_err();

        assert!(error.to_string().contains("omitted the required ETag"));
        assert!(adapter.exists(source).await.unwrap());
        assert!(!adapter.exists(destination).await.unwrap());
        assert!(probe.ranges.lock().unwrap().is_empty());
        assert_eq!(
            probe
                .multipart_creates
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn azure_rename_aborts_when_source_changes_between_ranges() {
        let (adapter, probe) =
            azure_rename_fault_adapter(AzureRenameFault::ChangeSourceBeforeSecondRange);
        let source = "az://container/source.json";
        let destination = "az://container/destination.json";
        let payload = "x".repeat(usize::try_from(AZURE_RENAME_PART_BYTES + 17).unwrap());
        adapter.write_text(source, &payload).await.unwrap();

        let error = adapter.rename_text(source, destination).await.unwrap_err();

        assert!(error.to_string().contains("rename_read"));
        assert_eq!(
            adapter.read_text(source).await.unwrap().as_bytes()[0],
            b'z',
            "the concurrent source replacement must survive the failed rename"
        );
        assert!(!adapter.exists(destination).await.unwrap());
        assert_eq!(probe.ranges.lock().unwrap().len(), 2);
        assert_eq!(probe.aborts.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(
            probe.completes.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn azure_rename_aborts_part_and_complete_failures_before_delete() {
        for fault in [
            AzureRenameFault::FailPart(1),
            AzureRenameFault::FailComplete,
        ] {
            let (adapter, probe) = azure_rename_fault_adapter(fault);
            let source = "az://container/source.json";
            let destination = "az://container/destination.json";
            let payload = "x".repeat(usize::try_from(AZURE_RENAME_PART_BYTES + 17).unwrap());
            adapter.write_text(source, &payload).await.unwrap();

            let error = adapter.rename_text(source, destination).await.unwrap_err();

            assert!(error.to_string().contains("rename_write"));
            assert!(adapter.exists(source).await.unwrap());
            assert!(
                !adapter.exists(destination).await.unwrap(),
                "the injected {fault:?} failure happens before the fake upload publishes"
            );
            assert_eq!(probe.aborts.load(std::sync::atomic::Ordering::Relaxed), 1);
            assert_eq!(
                probe.completes.load(std::sync::atomic::Ordering::Relaxed),
                usize::from(fault == AzureRenameFault::FailComplete)
            );
        }
    }

    #[test]
    fn storage_backend_selection_is_scheme_aware() {
        assert_eq!(
            storage_kind_for_uri("/tmp/graph").unwrap(),
            StorageKind::Local
        );
        assert_eq!(
            storage_kind_for_uri("file:///tmp/graph").unwrap(),
            StorageKind::Local
        );
        assert_eq!(
            storage_kind_for_uri(r"C:\omnigraph\graph").unwrap(),
            StorageKind::Local
        );
        assert_eq!(
            storage_kind_for_uri("s3://omnigraph-preview/graph").unwrap(),
            StorageKind::S3
        );
        assert_eq!(
            storage_kind_for_uri("az://omnigraph/graph").unwrap(),
            StorageKind::Azure
        );
        let error = storage_kind_for_uri("https://example.com/graph")
            .expect_err("unknown schemes must never fall through to local storage");
        assert!(error.to_string().contains("unsupported storage URI scheme"));
    }

    #[test]
    fn storage_uri_diagnostics_redact_credentials() {
        let query_secret = "TOPSECRET-QUERY";
        let query_error = normalize_root_uri(&format!(
            "az://container/path?sv=2026-01-01&sig={query_secret}"
        ))
        .unwrap_err()
        .to_string();
        assert!(!query_error.contains(query_secret));
        assert!(query_error.contains("az://container/path"));
        assert!(query_error.contains("query redacted"));

        let password_secret = "TOPSECRET-PASSWORD";
        let userinfo_error =
            normalize_root_uri(&format!("az://operator:{password_secret}@container/path"))
                .unwrap_err()
                .to_string();
        assert!(!userinfo_error.contains(password_secret));
        assert!(userinfo_error.contains("az://container/path"));
        assert!(userinfo_error.contains("userinfo redacted"));

        let https_secret = "TOPSECRET-HTTPS-SAS";
        let unsupported_error = storage_kind_for_uri(&format!(
            "https://account.blob.core.windows.net/container?sig={https_secret}"
        ))
        .unwrap_err()
        .to_string();
        assert!(!unsupported_error.contains(https_secret));
        assert!(unsupported_error.contains("https://account.blob.core.windows.net/container"));

        let malformed_secret = "TOPSECRET-MALFORMED-SAS";
        let malformed_error = normalize_root_uri(&format!("az://[invalid?sig={malformed_secret}"))
            .unwrap_err()
            .to_string();
        assert!(!malformed_error.contains(malformed_secret));
        assert!(malformed_error.contains("az://<invalid or redacted>"));
    }

    #[test]
    fn normalize_root_uri_preserves_local_s3_and_azure_shapes() {
        assert_eq!(
            normalize_root_uri("/tmp/omnigraph/").unwrap(),
            "/tmp/omnigraph"
        );
        assert_eq!(
            normalize_root_uri("file:///tmp/omnigraph/").unwrap(),
            "/tmp/omnigraph"
        );
        assert_eq!(
            normalize_root_uri("s3://bucket/prefix/").unwrap(),
            "s3://bucket/prefix"
        );
        assert_eq!(
            normalize_root_uri("az://container/prefix%20with%20space/").unwrap(),
            "az://container/prefix%20with%20space"
        );
        assert!(normalize_root_uri("custom://root/path").is_err());
    }

    #[test]
    fn write_queue_identity_keeps_remote_and_custom_schemes_opaque() {
        for root in [
            "s3://bucket/prefix",
            "memory://write-queue/custom",
            "custom+transport:opaque-root",
        ] {
            assert_eq!(write_queue_root_identity(root).unwrap(), root);
        }
    }

    #[test]
    fn join_uri_handles_local_file_s3_and_azure_roots() {
        assert_eq!(
            join_uri("/tmp/omnigraph", "_schema.pg"),
            "/tmp/omnigraph/_schema.pg"
        );
        assert_eq!(
            join_uri("file:///tmp/omnigraph", "_schema.pg"),
            "/tmp/omnigraph/_schema.pg"
        );
        assert_eq!(
            join_uri("s3://bucket/prefix", "_schema.pg"),
            "s3://bucket/prefix/_schema.pg"
        );
        assert_eq!(
            join_uri("az://container/prefix", "_schema.pg"),
            "az://container/prefix/_schema.pg"
        );
        assert_eq!(
            join_uri("custom://opaque/root", "_schema.pg"),
            "custom://opaque/root/_schema.pg",
            "an unsupported scheme may remain opaque during joining but must never become a local path"
        );
    }

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        let location = parse_s3_uri("s3://bucket/graph/_schema.pg").unwrap();
        assert_eq!(location.bucket, "bucket");
        assert_eq!(location.key, "graph/_schema.pg");
    }

    #[test]
    fn canonical_azure_root_owns_identity_options_and_admission_location() {
        let config = AzureStorageConfig::new("companybrainprod")
            .with_endpoint("https://companybrainprod.blob.core.windows.net/")
            .with_client_id("00000000-0000-0000-0000-000000000001")
            .with_identity_endpoint("http://127.0.0.1:42342/msi/token");
        let root =
            CanonicalAzureRoot::from_config("az://omnigraph/clusters/company%20brain/", config)
                .unwrap();
        assert_eq!(
            root.canonical_uri(),
            "az://omnigraph/clusters/company%20brain"
        );
        assert_eq!(root.account_name(), "companybrainprod");
        assert_eq!(root.container(), "omnigraph");
        assert_eq!(root.prefix(), "clusters/company brain");
        assert!(!root.use_emulator());
        assert_eq!(
            root.client_id(),
            Some("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(
            root.identity_endpoint(),
            Some("http://127.0.0.1:42342/msi/token")
        );
        assert_eq!(root.root_digest_hex().len(), 64);
        assert_eq!(
            root.admission_blob_uri(),
            format!(
                "az://omnigraph/{AZURE_ADMISSION_PREFIX}/{}/writer.lock",
                root.root_digest_hex()
            )
        );
        assert_eq!(
            root.admission_blob_url().unwrap().as_str(),
            format!(
                "https://companybrainprod.blob.core.windows.net/omnigraph/{AZURE_ADMISSION_PREFIX}/{}/writer.lock",
                root.root_digest_hex()
            )
        );
        let options = root.lance_storage_options().unwrap();
        assert_eq!(
            options
                .get("azure_storage_account_name")
                .map(String::as_str),
            Some("companybrainprod")
        );
        assert_eq!(
            options.get("azure_storage_endpoint").map(String::as_str),
            Some("https://companybrainprod.blob.core.windows.net/")
        );
        assert_eq!(
            options.get("azure_storage_client_id").map(String::as_str),
            Some("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(
            options.get("azure_msi_endpoint").map(String::as_str),
            Some("http://127.0.0.1:42342/msi/token")
        );

        let same = CanonicalAzureRoot::from_config(
            "az://omnigraph/clusters/company%20brain",
            AzureStorageConfig::new("companybrainprod")
                .with_endpoint("https://companybrainprod.blob.core.windows.net"),
        )
        .unwrap();
        assert_eq!(same.root_digest_hex(), root.root_digest_hex());
        let other = CanonicalAzureRoot::from_config(
            "az://omnigraph/clusters/other",
            AzureStorageConfig::new("companybrainprod"),
        )
        .unwrap();
        assert_ne!(other.root_digest_hex(), root.root_digest_hex());
    }

    #[test]
    fn canonical_azure_root_builds_exact_azurite_url() {
        let root = CanonicalAzureRoot::from_config(
            "az://omnigraph/clusters/test",
            AzureStorageConfig::new("devstoreaccount1").with_emulator("http://127.0.0.1:10000/"),
        )
        .unwrap();
        assert_eq!(
            root.admission_blob_url().unwrap().as_str(),
            format!(
                "http://127.0.0.1:10000/devstoreaccount1/omnigraph/{AZURE_ADMISSION_PREFIX}/{}/writer.lock",
                root.root_digest_hex()
            )
        );
        let options = root.lance_storage_options().unwrap();
        assert_eq!(
            options
                .get("azure_storage_use_emulator")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            options.get("azure_storage_endpoint").map(String::as_str),
            Some("http://127.0.0.1:10000/devstoreaccount1")
        );
        assert_eq!(options.get("allow_http").map(String::as_str), Some("true"));
        assert_eq!(
            options.get("azure_storage_account_key").map(String::as_str),
            Some(DEFAULT_AZURITE_ACCOUNT_KEY)
        );
        ObjectStorageAdapter::azure_from_root(&root).unwrap();
    }

    #[test]
    fn canonical_azure_root_rejects_production_http_without_leaking_secrets() {
        let token = "TOPSECRET-BEARER-TOKEN";
        let endpoint_path_secret = "TOPSECRET-ENDPOINT-PATH";
        let error = CanonicalAzureRoot::from_config(
            "az://omnigraph/clusters/production",
            AzureStorageConfig::new("companybrainprod")
                .with_endpoint(format!("http://127.0.0.1/{endpoint_path_secret}"))
                .with_bearer_token(token),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("HTTPS is required outside Azurite mode"));
        assert!(!error.contains(token));
        assert!(!error.contains(endpoint_path_secret));

        let endpoint_query_secret = "TOPSECRET-ENDPOINT-QUERY";
        let query_error = CanonicalAzureRoot::from_config(
            "az://omnigraph/clusters/production",
            AzureStorageConfig::new("companybrainprod")
                .with_endpoint(format!("http://127.0.0.1/?sig={endpoint_query_secret}"))
                .with_bearer_token(token),
        )
        .unwrap_err()
        .to_string();
        assert!(!query_error.contains(token));
        assert!(!query_error.contains(endpoint_query_secret));
    }

    #[test]
    fn canonical_azure_root_seals_default_endpoint_and_fabric_selection() {
        let mut config = AzureStorageConfig::new("companybrainprod");
        config.azure_options.insert(
            AzureConfigKey::Endpoint.as_ref().to_string(),
            "https://ambient.invalid".to_string(),
        );
        config.azure_options.insert(
            AzureConfigKey::UseFabricEndpoint.as_ref().to_string(),
            "true".to_string(),
        );

        let root =
            CanonicalAzureRoot::from_config("az://omnigraph/graphs/knowledge", config).unwrap();
        let options = root.lance_storage_options().unwrap();
        assert_eq!(
            options
                .get(AzureConfigKey::Endpoint.as_ref())
                .map(String::as_str),
            Some("https://companybrainprod.blob.core.windows.net/")
        );
        assert_eq!(
            options
                .get(AzureConfigKey::UseFabricEndpoint.as_ref())
                .map(String::as_str),
            Some("false")
        );
        ObjectStorageAdapter::azure_from_root(&root).unwrap();
    }

    #[test]
    fn azure_environment_aliases_are_normalized_or_rejected() {
        let mut values = BTreeMap::from([
            (
                "AZURE_STORAGE_ENDPOINT".to_string(),
                "https://one.example".to_string(),
            ),
            (
                "AZURE_ENDPOINT".to_string(),
                "https://one.example".to_string(),
            ),
        ]);
        let normalized = normalize_azure_environment(&values).unwrap();
        assert_eq!(
            normalized
                .get(AzureConfigKey::Endpoint.as_ref())
                .map(String::as_str),
            Some("https://one.example")
        );

        values.insert(
            "AZURE_ENDPOINT".to_string(),
            "https://secret-value-that-must-not-render.example".to_string(),
        );
        let message = normalize_azure_environment(&values)
            .unwrap_err()
            .to_string();
        assert!(message.contains("AZURE_STORAGE_ENDPOINT"));
        assert!(message.contains("AZURE_ENDPOINT"));
        assert!(!message.contains("secret-value-that-must-not-render"));

        let managed_identity_endpoint = "http://127.0.0.1:42342/msi/token";
        let (snapshot, normalized) = capture_azure_environment_values([
            (
                "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
                "companybrainprod".to_string(),
            ),
            (
                "IDENTITY_ENDPOINT".to_string(),
                managed_identity_endpoint.to_string(),
            ),
            (
                "MSI_ENDPOINT".to_string(),
                managed_identity_endpoint.to_string(),
            ),
            (
                "IDENTITY_HEADER".to_string(),
                "identity-header-secret".to_string(),
            ),
        ])
        .unwrap();
        assert_eq!(
            normalized
                .get(AzureConfigKey::MsiEndpoint.as_ref())
                .map(String::as_str),
            Some(managed_identity_endpoint)
        );
        assert!(snapshot.values.contains_key("IDENTITY_ENDPOINT"));
        assert!(snapshot.values.contains_key("MSI_ENDPOINT"));

        let conflicting_identity_endpoint = "http://127.0.0.1:42343/msi/token";
        let message = capture_azure_environment_values([
            (
                "IDENTITY_ENDPOINT".to_string(),
                managed_identity_endpoint.to_string(),
            ),
            (
                "MSI_ENDPOINT".to_string(),
                conflicting_identity_endpoint.to_string(),
            ),
        ])
        .unwrap_err()
        .to_string();
        assert!(message.contains("IDENTITY_ENDPOINT"));
        assert!(message.contains("MSI_ENDPOINT"));
        assert!(!message.contains(managed_identity_endpoint));
        assert!(!message.contains(conflicting_identity_endpoint));

        let legacy_only_endpoint = "http://legacy-only.invalid/msi/token";
        let message = capture_azure_environment_values([(
            "MSI_ENDPOINT".to_string(),
            legacy_only_endpoint.to_string(),
        )])
        .unwrap_err()
        .to_string();
        assert!(message.contains("MSI_ENDPOINT"));
        assert!(!message.contains(legacy_only_endpoint));
    }

    #[test]
    fn production_azure_rejects_every_process_wide_http_allowance() {
        for key in [
            "AZURE_STORAGE_ALLOW_HTTP",
            "AZURE_STORAGE_USE_HTTP",
            "AWS_ALLOW_HTTP",
        ] {
            let environment = BTreeMap::from([(key.to_string(), "true".to_string())]);
            let (_, options) = capture_azure_environment_values(environment.clone()).unwrap();
            let error = validate_azure_http_policy(false, &environment, &options)
                .unwrap_err()
                .to_string();
            assert!(error.contains("production Azure storage forbids HTTP allowances"));
            assert!(error.contains(key));
        }

        let mut options = BTreeMap::new();
        options.insert("allow_http".to_string(), "true".to_string());
        assert!(validate_azure_http_policy(false, &BTreeMap::new(), &options).is_err());
        assert!(validate_azure_http_policy(true, &BTreeMap::new(), &options).is_ok());

        let mut config = AzureStorageConfig::new("companybrainprod");
        config
            .azure_options
            .insert("allow_http".to_string(), "true".to_string());
        assert!(
            CanonicalAzureRoot::from_config("az://omnigraph/clusters/production", config)
                .unwrap_err()
                .to_string()
                .contains("production Azure storage forbids HTTP allowances")
        );
    }

    #[test]
    fn azure_environment_rejects_lance_only_unprefixed_aliases() {
        let message = capture_azure_environment_values([
            (
                "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
                "companybrainprod".to_string(),
            ),
            ("TOKEN".to_string(), "unrelated-ci-secret".to_string()),
            (
                "ENDPOINT".to_string(),
                "https://unrelated.example".to_string(),
            ),
        ])
        .unwrap_err()
        .to_string();
        assert!(message.contains("TOKEN"));
        assert!(message.contains("ENDPOINT"));
        assert!(!message.contains("unrelated-ci-secret"));
        assert!(!message.contains("unrelated.example"));
    }

    #[test]
    fn azure_environment_snapshot_detects_drift_without_leaking_values() {
        let (current, _) = capture_azure_environment().unwrap();
        let mut captured = current;
        let changed_key = "AZURE_FABRIC_SESSION_TOKEN";
        let changed_value = format!(
            "{}-secret-drift-value",
            captured
                .values
                .get(changed_key)
                .map(String::as_str)
                .unwrap_or("captured")
        );
        captured
            .values
            .insert(changed_key.to_string(), changed_value.clone());

        let message = captured.verify_unchanged().unwrap_err().to_string();
        assert!(message.contains(changed_key));
        assert!(!message.contains(&changed_value));
    }

    #[test]
    fn azure_config_root_and_credentials_redact_every_secret() {
        let secrets = [
            "shared-key-secret-value",
            "bearer-token-secret-value",
            "client-secret-value",
            "sas-secret-value",
            "fabric-session-secret-value",
            "identity-header-secret-value",
        ];
        let mut config = AzureStorageConfig::new("companybrainprod")
            .with_account_key(secrets[0])
            .with_bearer_token(secrets[1])
            .with_identity_header(secrets[5]);
        config.azure_options.insert(
            AzureConfigKey::ClientSecret.as_ref().to_string(),
            secrets[2].to_string(),
        );
        config.azure_options.insert(
            AzureConfigKey::SasKey.as_ref().to_string(),
            secrets[3].to_string(),
        );
        config.azure_options.insert(
            AzureConfigKey::FabricSessionToken.as_ref().to_string(),
            secrets[4].to_string(),
        );

        let config_debug = format!("{config:?}");
        let unvalidated_config_debug = format!(
            "{:?}",
            AzureStorageConfig::new("companybrainprod")
                .with_endpoint("https://example.invalid/?sig=config-debug-secret")
                .with_identity_endpoint("http://127.0.0.1/msi/token?secret=identity-debug-secret",)
        );
        let root =
            CanonicalAzureRoot::from_config("az://omnigraph/graphs/knowledge", config).unwrap();
        let root_debug = format!("{root:?}");
        let credential_debug = format!("{:?}", root.admission_credential().unwrap());
        for secret in secrets {
            assert!(!config_debug.contains(secret));
            assert!(!root_debug.contains(secret));
            assert!(!credential_debug.contains(secret));
        }
        assert!(!unvalidated_config_debug.contains("config-debug-secret"));
        assert!(!unvalidated_config_debug.contains("identity-debug-secret"));

        // The hidden handoff must carry the exact captured credential set to
        // Lance; callers pass this directly into its redacted accessor.
        let lance_options = root.lance_storage_options().unwrap();
        assert_eq!(
            lance_options
                .get(AzureConfigKey::AccessKey.as_ref())
                .map(String::as_str),
            Some(secrets[0])
        );
        assert_eq!(
            lance_options
                .get(AzureConfigKey::Token.as_ref())
                .map(String::as_str),
            Some(secrets[1])
        );

        let malformed = CanonicalAzureRoot::from_config(
            "az://omnigraph/graphs/knowledge",
            AzureStorageConfig::new("companybrainprod")
                .with_endpoint("https://example.invalid/?sig=endpoint-secret-value"),
        )
        .unwrap_err()
        .to_string();
        assert!(!malformed.contains("endpoint-secret-value"));
    }

    #[test]
    fn azure_uri_parser_rejects_aliases_and_unsafe_paths() {
        for uri in [
            "az://",
            "az://ab/path",
            "az://Uppercase/path",
            "az://container@account/path",
            "az://container:443/path",
            "az://container/path?sig=secret",
            "az://container/path#fragment",
            "az://container/a//b",
            "az://container/a/./b",
            "az://container/a/../b",
            "az://container/%2e",
            "az://container/%2E%2E",
            "az://container/a%2Fb",
            "az://container/a%5Cb",
            "az://container/a\\b",
            "az://container/%00",
        ] {
            assert!(
                parse_azure_uri(uri).is_err(),
                "unsafe or aliased Azure URI unexpectedly accepted: {uri}"
            );
        }
    }

    #[test]
    fn azure_codec_refuses_cross_container_and_empty_object_access() {
        let adapter = ObjectStorageAdapter {
            store: Arc::new(InMemory::new()),
            codec: UriCodec::Azure {
                container: "container-one".to_string(),
            },
            supports_conditional_update: true,
            omit_read_etag: false,
            omit_write_etag: false,
        };
        assert!(
            adapter
                .object_path("az://container-two/path.json")
                .unwrap_err()
                .to_string()
                .contains("container mismatch")
        );
        assert!(adapter.object_path("az://container-one").is_err());
        assert_eq!(
            adapter
                .object_path("az://container-one/a%20b.json")
                .unwrap()
                .as_ref(),
            "a b.json"
        );
    }

    #[tokio::test]
    async fn remote_conditional_updates_require_backend_etags() {
        for action in ["read", "write_if_match"] {
            let error = required_remote_etag(action, "az://container/state.json", None)
                .expect_err("a remote content hash must never replace a missing ETag");
            assert!(error.to_string().contains("omitted the required ETag"));
        }
        assert_eq!(
            required_remote_etag(
                "read",
                "az://container/state.json",
                Some("etag-1".to_string())
            )
            .unwrap(),
            "etag-1"
        );

        let uri = "remote-etag/state.json";
        let mut missing_read = ObjectStorageAdapter::in_memory();
        missing_read.write_text(uri, "v1").await.unwrap();
        missing_read.omit_read_etag = true;
        let error = missing_read
            .read_text_versioned(uri)
            .await
            .expect_err("a remote versioned read without an ETag must fail closed");
        assert!(error.to_string().contains("omitted the required ETag"));

        let mut missing_write = ObjectStorageAdapter::in_memory();
        missing_write.write_text(uri, "v1").await.unwrap();
        let (_, version) = missing_write.read_text_versioned(uri).await.unwrap();
        missing_write.omit_write_etag = true;
        let error = missing_write
            .write_text_if_match(uri, "v2", &version)
            .await
            .expect_err("a successful remote CAS without its new ETag must fail closed");
        assert!(error.to_string().contains("omitted the required ETag"));
        assert_eq!(
            missing_write.read_text(uri).await.unwrap(),
            "v2",
            "the error is post-effect ambiguity, never a claim that the write was absent"
        );
    }

    /// Where hard links work the probe is negative and cleans up after
    /// itself. (The positive branch needs a filesystem that refuses
    /// `hard_link(2)`, e.g. FAT32, which this suite cannot assume.)
    #[test]
    fn hard_link_probe_negative_where_links_work_and_leaves_no_residue() {
        let dir = tempfile::tempdir().unwrap();
        assert!(hard_link_refusal_in(dir.path()).is_none());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        assert!(is_hard_link_capability_refusal(&std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "hard links denied",
        )));
        assert!(is_hard_link_capability_refusal(&std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "hard links unsupported",
        )));
        for transient in [
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::OutOfMemory,
            std::io::ErrorKind::Other,
        ] {
            assert!(
                !is_hard_link_capability_refusal(&std::io::Error::new(
                    transient,
                    "transient probe failure",
                )),
                "{transient:?} must preserve the original backend error"
            );
        }
    }

    /// A missing directory is an inconclusive probe, not a capability
    /// verdict: callers keep their original error.
    #[test]
    fn hard_link_probe_inconclusive_on_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent");
        assert!(hard_link_refusal_in(&missing).is_none());
    }

    /// A generic create-if-absent failure on a link-capable filesystem keeps
    /// the original backend error: the enriched diagnostic fires only when
    /// the probe proves hard links are refused.
    #[tokio::test]
    async fn create_if_absent_failure_keeps_backend_error_where_links_work() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        // A destination whose parent is a regular FILE makes PutMode::Create
        // fail at staging, before any hard link. The probe on the parent is
        // inconclusive (unwritable dir), so the original error survives.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "not a directory").unwrap();
        let uri = format!("{}/child.txt", blocker.display());
        let err = StorageAdapter::write_text_if_absent(&adapter, &uri, "x")
            .await
            .expect_err("staging under a regular file must fail");
        assert!(
            matches!(
                err,
                StorageError::Backend(ref failure)
                    if failure.message.contains("write_if_absent")
            ),
            "got: {err}"
        );
    }
}
