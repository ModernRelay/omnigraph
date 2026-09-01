//! Azure Blob lease admission for the supported single-writer deployment.
//!
//! This crate is deliberately not a storage backend. It can address exactly
//! one reserved admission Blob derived by `omnigraph-storage`, and exposes only
//! the lease operations needed to admit and retire an OmniGraph process.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use hmac::{Hmac, Mac};
use omnigraph_storage::{AzureAdmissionCredential, CanonicalAzureRoot};
use reqwest::header::{
    AUTHORIZATION, CONTENT_LENGTH, HeaderMap, HeaderName, HeaderValue, IF_NONE_MATCH,
};
use reqwest::redirect::Policy;
use reqwest::{Method, Response, StatusCode};
use serde::Deserialize;
use sha2::Sha256;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const AZURE_API_VERSION: &str = "2023-11-03";
const STORAGE_RESOURCE: &str = "https://storage.azure.com/";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const BREAK_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const BREAK_CONFIRM_POLL: Duration = Duration::from_millis(100);
const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;
const RESERVED_ADMISSION_NAMESPACE: &str = "__omnigraph_azure_admission";
type HmacSha256 = Hmac<Sha256>;

pub type Result<T> = std::result::Result<T, AdmissionError>;

/// Typed failure from the narrow admission protocol.
#[derive(Debug, Error)]
pub enum AdmissionError {
    #[error("invalid Azure admission configuration: {0}")]
    Configuration(String),
    #[error("Azure managed-identity request failed with HTTP {status}")]
    IdentityStatus { status: u16 },
    #[error("Azure managed-identity response was invalid: {0}")]
    IdentityResponse(String),
    #[error("Azure admission {operation} transport failed: {source}")]
    Transport {
        operation: &'static str,
        #[source]
        source: reqwest::Error,
    },
    #[error(
        "Azure admission {operation} response body exceeds {limit} bytes (observed at least {actual})"
    )]
    ResponseBodyTooLarge {
        operation: &'static str,
        limit: usize,
        actual: u64,
    },
    #[error("Azure admission {operation} response body read failed: {source}")]
    ResponseBodyRead {
        operation: &'static str,
        #[source]
        source: reqwest::Error,
    },
    #[error("Azure admission {operation} failed with HTTP {status}{code}")]
    Azure {
        operation: &'static str,
        status: u16,
        code: AzureCodeDisplay,
    },
    #[error("could not construct Azure admission authorization: {0}")]
    Authorization(String),
    #[error("Azure admission lease break was not observed as unlocked before timeout")]
    BreakNotObserved,
}

/// Redacted formatting helper for an optional Azure error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureCodeDisplay(Option<String>);

impl fmt::Display for AzureCodeDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(code) => write!(f, " ({code})"),
            None => Ok(()),
        }
    }
}

/// Exact lease identifier returned by Azure.
#[derive(Clone, PartialEq, Eq)]
pub struct LeaseId(String);

impl LeaseId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        Uuid::from_str(&value).map_err(|_| {
            AdmissionError::Configuration("Azure lease id must be a UUID".to_string())
        })?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for LeaseId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for LeaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LeaseId").field(&"<redacted>").finish()
    }
}

/// Result of one acquire attempt with one proposed lease id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    Acquired(LeaseId),
    /// Another lease currently owns the admission Blob.
    Held,
    /// The acquire response was lost and exact-id renewal could not prove
    /// ownership. Callers must not start a child or try a different id.
    Ambiguous(LeaseId),
}

/// Result of releasing an exact owned lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    Released,
    /// Azure may or may not have applied the release. Callers make no claim;
    /// every successor still needs a positively confirmed acquire.
    Ambiguous,
}

/// Observable server-side state of the reserved admission Blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseState {
    Missing,
    Present {
        status: Option<String>,
        state: Option<String>,
        duration: Option<String>,
    },
}

#[derive(Clone)]
enum AdmissionAuth {
    ManagedIdentity(ManagedIdentity),
    StaticBearer(String),
    SharedKey { account: String, key: Vec<u8> },
}

impl fmt::Debug for AdmissionAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ManagedIdentity(_) => f.write_str("ManagedIdentity(<redacted>)"),
            Self::StaticBearer(_) => f.write_str("StaticBearer(<redacted>)"),
            Self::SharedKey { account, .. } => f
                .debug_struct("SharedKey")
                .field("account", account)
                .field("key", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Clone)]
struct ManagedIdentity {
    endpoint: Url,
    secret_header: String,
    client_id: Option<String>,
}

impl fmt::Debug for ManagedIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedIdentity")
            .field("endpoint", &self.endpoint)
            .field("secret_header", &"<redacted>")
            .field("client_id", &self.client_id)
            .finish()
    }
}

/// Client for exactly one root-derived Azure admission Blob.
#[derive(Clone)]
pub struct AdmissionClient {
    http: reqwest::Client,
    root: CanonicalAzureRoot,
    blob_url: Url,
    auth: AdmissionAuth,
}

impl fmt::Debug for AdmissionClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmissionClient")
            .field("canonical_root", &self.root.canonical_uri())
            .field("root_digest", &self.root.root_digest_hex())
            .field("blob_url", &self.blob_url)
            .field("auth", &self.auth)
            .finish()
    }
}

impl AdmissionClient {
    /// Construct from the same process-start environment captured by the
    /// shared Azure storage root.
    pub fn from_env(root_uri: &str) -> Result<Self> {
        let root = CanonicalAzureRoot::from_env(root_uri)
            .map_err(|err| AdmissionError::Configuration(err.to_string()))?;
        Self::from_root_and_env(root)
    }

    pub fn from_root_and_env(root: CanonicalAzureRoot) -> Result<Self> {
        validate_deployment_root(&root)?;
        root.verify_environment_unchanged()
            .map_err(|err| AdmissionError::Configuration(err.to_string()))?;
        let blob_url = root
            .admission_blob_url()
            .map_err(|err| AdmissionError::Configuration(err.to_string()))?;
        let auth = auth_from_root(&root)?;
        let http = admission_http_client()?;
        Ok(Self {
            http,
            root,
            blob_url,
            auth,
        })
    }

    #[doc(hidden)]
    pub fn with_static_bearer_for_test(
        root: CanonicalAzureRoot,
        token: impl Into<String>,
    ) -> Result<Self> {
        validate_deployment_root(&root)?;
        let blob_url = root
            .admission_blob_url()
            .map_err(|err| AdmissionError::Configuration(err.to_string()))?;
        Ok(Self {
            http: admission_http_client()?,
            root,
            blob_url,
            auth: AdmissionAuth::StaticBearer(token.into()),
        })
    }

    pub fn canonical_root(&self) -> &CanonicalAzureRoot {
        &self.root
    }

    pub fn root_digest_hex(&self) -> &str {
        self.root.root_digest_hex()
    }

    pub fn admission_blob_uri(&self) -> &str {
        self.root.admission_blob_uri()
    }

    /// Create the permanent zero-byte admission Blob iff it is absent.
    pub async fn ensure_admission_blob(&self) -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-ms-blob-type"),
            HeaderValue::from_static("BlockBlob"),
        );
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        let response = self
            .send(
                "create admission blob",
                Method::PUT,
                self.blob_url.clone(),
                headers,
            )
            .await
            .map_err(|err| match err {
                SendFailure::Definite(err) | SendFailure::Ambiguous(err) => err,
            })?;
        if response.status() == StatusCode::CREATED {
            return Ok(());
        }
        let failure = response_failure("create admission blob", response).await?;
        if matches!(failure.status, 409 | 412)
            && matches!(
                failure.code.as_deref(),
                Some("BlobAlreadyExists" | "ConditionNotMet" | "LeaseIdMissing")
            )
        {
            return Ok(());
        }
        Err(failure.into_error())
    }

    /// Attempt an infinite lease using one caller-owned proposed UUID.
    pub async fn try_acquire(&self, proposed: LeaseId) -> Result<AcquireOutcome> {
        self.ensure_admission_blob().await?;
        let headers = lease_headers("acquire", Some(&proposed), Some("-1"))?;
        let url = lease_url(&self.blob_url);
        let response = match self.send("acquire lease", Method::PUT, url, headers).await {
            Ok(response) => response,
            Err(SendFailure::Definite(err)) => return Err(err),
            Err(SendFailure::Ambiguous(_)) => {
                return Ok(self.resolve_uncertain_acquire(proposed).await);
            }
        };

        if response.status() == StatusCode::CREATED {
            let returned = response
                .headers()
                .get("x-ms-lease-id")
                .and_then(|value| value.to_str().ok());
            if returned != Some(proposed.as_str()) {
                // The acquire may already have taken effect. Only an exact-id
                // renew can establish ownership; never try a fresh id.
                return Ok(self.resolve_uncertain_acquire(proposed).await);
            }
            return Ok(AcquireOutcome::Acquired(proposed));
        }
        if response.status().is_success() {
            // A non-conforming 2xx may still hide an applied acquire. It is
            // never positive ownership proof; only exact-id renew may prove it.
            return Ok(self.resolve_uncertain_acquire(proposed).await);
        }

        let failure = response_failure("acquire lease", response).await?;
        if matches!(failure.status, 409 | 412)
            && matches!(
                failure.code.as_deref(),
                Some(
                    "LeaseAlreadyPresent"
                        | "LeaseAlreadyBroken"
                        | "LeaseIsBreakingAndCannotBeAcquired"
                        | "LeaseIdMismatchWithLeaseOperation"
                )
            )
        {
            return Ok(AcquireOutcome::Held);
        }
        if response_status_may_hide_mutation(failure.status) {
            return Ok(self.resolve_uncertain_acquire(proposed).await);
        }
        Err(failure.into_error())
    }

    async fn resolve_uncertain_acquire(&self, proposed: LeaseId) -> AcquireOutcome {
        match self.renew(&proposed).await {
            Ok(true) => AcquireOutcome::Acquired(proposed),
            Ok(false) | Err(_) => AcquireOutcome::Ambiguous(proposed),
        }
    }

    /// Exact-id renew used only to prove ownership after a lost acquire reply.
    pub async fn renew(&self, lease_id: &LeaseId) -> Result<bool> {
        let headers = lease_headers("renew", Some(lease_id), None)?;
        let response = self
            .send(
                "renew lease",
                Method::PUT,
                lease_url(&self.blob_url),
                headers,
            )
            .await
            .map_err(|err| match err {
                SendFailure::Definite(err) | SendFailure::Ambiguous(err) => err,
            })?;
        if response.status() == StatusCode::OK {
            return Ok(true);
        }
        if response.status().is_success() {
            return Err(response_failure("renew lease", response)
                .await?
                .into_error());
        }
        let failure = response_failure("renew lease", response).await?;
        if matches!(failure.status, 409 | 412)
            && matches!(
                failure.code.as_deref(),
                Some(
                    "LeaseIdMismatchWithLeaseOperation"
                        | "LeaseNotPresentWithLeaseOperation"
                        | "LeaseLost"
                        | "LeaseIsBrokenAndCannotBeRenewed"
                        | "LeaseIsBreakingAndCannotBeRenewed"
                )
            )
        {
            return Ok(false);
        }
        Err(failure.into_error())
    }

    pub async fn release(&self, lease_id: &LeaseId) -> Result<ReleaseOutcome> {
        let headers = lease_headers("release", Some(lease_id), None)?;
        let response = match self
            .send(
                "release lease",
                Method::PUT,
                lease_url(&self.blob_url),
                headers,
            )
            .await
        {
            Ok(response) => response,
            Err(SendFailure::Ambiguous(_)) => return Ok(ReleaseOutcome::Ambiguous),
            Err(SendFailure::Definite(err)) => return Err(err),
        };
        if response.status() == StatusCode::OK {
            return Ok(ReleaseOutcome::Released);
        }
        if response.status().is_success() {
            return Ok(ReleaseOutcome::Ambiguous);
        }
        let failure = response_failure("release lease", response).await?;
        if response_status_may_hide_mutation(failure.status) {
            return Ok(ReleaseOutcome::Ambiguous);
        }
        Err(failure.into_error())
    }

    pub async fn inspect(&self) -> Result<LeaseState> {
        let response = self
            .send(
                "inspect admission blob",
                Method::HEAD,
                self.blob_url.clone(),
                HeaderMap::new(),
            )
            .await
            .map_err(|err| match err {
                SendFailure::Definite(err) | SendFailure::Ambiguous(err) => err,
            })?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(LeaseState::Missing);
        }
        if !response.status().is_success() {
            return Err(response_failure("inspect admission blob", response)
                .await?
                .into_error());
        }
        Ok(LeaseState::Present {
            status: header_string(response.headers(), "x-ms-lease-status"),
            state: header_string(response.headers(), "x-ms-lease-state"),
            duration: header_string(response.headers(), "x-ms-lease-duration"),
        })
    }

    /// Break the lease. The CLI owns the mandatory operator confirmations;
    /// ordinary serving code must never call this method.
    #[doc(hidden)]
    pub async fn break_after_operator_proof(&self) -> Result<()> {
        let headers = lease_headers("break", None, None)?;
        let response = self
            .send(
                "break lease",
                Method::PUT,
                lease_url(&self.blob_url),
                headers,
            )
            .await
            .map_err(|err| match err {
                SendFailure::Definite(err) | SendFailure::Ambiguous(err) => err,
            })?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(response_failure("break lease", response)
                .await?
                .into_error());
        }

        let deadline = tokio::time::Instant::now() + BREAK_CONFIRM_TIMEOUT;
        loop {
            if matches!(
                self.inspect().await?,
                LeaseState::Present {
                    status: Some(ref status),
                    ..
                } if status.eq_ignore_ascii_case("unlocked")
            ) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AdmissionError::BreakNotObserved);
            }
            tokio::time::sleep(BREAK_CONFIRM_POLL).await;
        }
    }

    async fn send(
        &self,
        operation: &'static str,
        method: Method,
        url: Url,
        mut headers: HeaderMap,
    ) -> std::result::Result<Response, SendFailure> {
        headers.insert(
            HeaderName::from_static("x-ms-version"),
            HeaderValue::from_static(AZURE_API_VERSION),
        );
        let now = httpdate::fmt_http_date(SystemTime::now());
        headers.insert(
            HeaderName::from_static("x-ms-date"),
            HeaderValue::from_str(&now).map_err(|err| {
                SendFailure::Definite(AdmissionError::Authorization(err.to_string()))
            })?,
        );
        if method == Method::PUT {
            headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
        }
        let authorization = self
            .authorization(&method, &url, &headers)
            .await
            .map_err(SendFailure::Definite)?;
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&authorization).map_err(|err| {
                SendFailure::Definite(AdmissionError::Authorization(err.to_string()))
            })?,
        );

        self.http
            .request(method, url)
            .headers(headers)
            .body(Vec::new())
            .send()
            .await
            .map_err(|source| {
                SendFailure::Ambiguous(AdmissionError::Transport { operation, source })
            })
    }

    async fn authorization(
        &self,
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
    ) -> Result<String> {
        match &self.auth {
            AdmissionAuth::StaticBearer(token) => Ok(format!("Bearer {token}")),
            AdmissionAuth::ManagedIdentity(identity) => {
                let token = identity.access_token(&self.http).await?;
                Ok(format!("Bearer {token}"))
            }
            AdmissionAuth::SharedKey { account, key } => {
                shared_key_authorization(account, key, method, url, headers)
            }
        }
    }
}

fn admission_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        // Blob lease and managed-identity responses prove facts about one exact
        // configured endpoint. Following a redirect would both weaken that
        // proof and forward custom secret-bearing headers that reqwest does not
        // classify as standard sensitive headers.
        .redirect(Policy::none())
        .build()
        .map_err(|err| AdmissionError::Configuration(err.to_string()))
}

fn validate_deployment_root(root: &CanonicalAzureRoot) -> Result<()> {
    let prefix = root.prefix();
    if prefix.is_empty() {
        return Err(AdmissionError::Configuration(
            "Azure process admission requires a non-empty dedicated cluster prefix".to_string(),
        ));
    }
    if prefix
        .split('/')
        .next()
        .is_some_and(|segment| segment == RESERVED_ADMISSION_NAMESPACE)
    {
        return Err(AdmissionError::Configuration(format!(
            "Azure cluster roots may not use the reserved '{RESERVED_ADMISSION_NAMESPACE}/' namespace"
        )));
    }
    Ok(())
}

impl ManagedIdentity {
    async fn access_token(&self, http: &reqwest::Client) -> Result<String> {
        let mut endpoint = self.endpoint.clone();
        {
            let mut query = endpoint.query_pairs_mut();
            query.append_pair("api-version", "2019-08-01");
            query.append_pair("resource", STORAGE_RESOURCE);
            if let Some(client_id) = &self.client_id {
                query.append_pair("client_id", client_id);
            }
        }
        let response = http
            .get(endpoint)
            .header("X-IDENTITY-HEADER", &self.secret_header)
            .send()
            .await
            .map_err(|source| AdmissionError::Transport {
                operation: "acquire managed-identity token",
                source,
            })?;
        if !response.status().is_success() {
            return Err(AdmissionError::IdentityStatus {
                status: response.status().as_u16(),
            });
        }
        let body = read_bounded_response_body("managed-identity token", response).await?;
        let token: ManagedIdentityToken = serde_json::from_slice(&body)
            .map_err(|err| AdmissionError::IdentityResponse(err.to_string()))?;
        if token.access_token.is_empty() {
            return Err(AdmissionError::IdentityResponse(
                "access_token is empty".to_string(),
            ));
        }
        Ok(token.access_token)
    }
}

#[derive(Deserialize)]
struct ManagedIdentityToken {
    access_token: String,
}

enum SendFailure {
    Definite(AdmissionError),
    Ambiguous(AdmissionError),
}

#[derive(Debug)]
struct AzureFailure {
    operation: &'static str,
    status: u16,
    code: Option<String>,
}

impl AzureFailure {
    fn into_error(self) -> AdmissionError {
        AdmissionError::Azure {
            operation: self.operation,
            status: self.status,
            code: AzureCodeDisplay(self.code),
        }
    }
}

async fn response_failure(operation: &'static str, response: Response) -> Result<AzureFailure> {
    let status = response.status().as_u16();
    let header_code = response
        .headers()
        .get("x-ms-error-code")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body_code = if header_code.is_none() {
        let body = read_bounded_response_body(operation, response).await?;
        std::str::from_utf8(&body)
            .ok()
            .and_then(|body| xml_tag(body, "Code"))
    } else {
        None
    };
    Ok(AzureFailure {
        operation,
        status,
        code: header_code.or(body_code),
    })
}

async fn read_bounded_response_body(
    operation: &'static str,
    mut response: Response,
) -> Result<Vec<u8>> {
    let limit_u64 = MAX_RESPONSE_BODY_BYTES as u64;
    if let Some(content_length) = response.content_length()
        && content_length > limit_u64
    {
        return Err(AdmissionError::ResponseBodyTooLarge {
            operation,
            limit: MAX_RESPONSE_BODY_BYTES,
            actual: content_length,
        });
    }

    let capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default();
    let mut body = Vec::with_capacity(capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| AdmissionError::ResponseBodyRead { operation, source })?
    {
        let actual = body
            .len()
            .checked_add(chunk.len())
            .and_then(|length| u64::try_from(length).ok())
            .unwrap_or(u64::MAX);
        if actual > limit_u64 {
            return Err(AdmissionError::ResponseBodyTooLarge {
                operation,
                limit: MAX_RESPONSE_BODY_BYTES,
                actual,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn xml_tag(body: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = body.find(&start_tag)? + start_tag.len();
    let end = body[start..].find(&end_tag)? + start;
    Some(body[start..end].to_string())
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn response_status_may_hide_mutation(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

fn lease_url(blob_url: &Url) -> Url {
    let mut url = blob_url.clone();
    url.query_pairs_mut().append_pair("comp", "lease");
    url
}

fn lease_headers(
    action: &'static str,
    lease_id: Option<&LeaseId>,
    duration: Option<&'static str>,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-ms-lease-action"),
        HeaderValue::from_static(action),
    );
    if let Some(lease_id) = lease_id {
        let name = if action == "acquire" {
            "x-ms-proposed-lease-id"
        } else {
            "x-ms-lease-id"
        };
        headers.insert(
            HeaderName::from_static(name),
            HeaderValue::from_str(lease_id.as_str())
                .map_err(|err| AdmissionError::Configuration(err.to_string()))?,
        );
    }
    if let Some(duration) = duration {
        headers.insert(
            HeaderName::from_static("x-ms-lease-duration"),
            HeaderValue::from_static(duration),
        );
    }
    Ok(headers)
}

fn auth_from_root(root: &CanonicalAzureRoot) -> Result<AdmissionAuth> {
    let credential = root
        .admission_credential()
        .map_err(|err| AdmissionError::Configuration(err.to_string()))?;
    match credential {
        AzureAdmissionCredential::BearerToken(token) => {
            if token.is_empty() {
                return Err(AdmissionError::Configuration(
                    "Azure storage bearer token is empty".to_string(),
                ));
            }
            Ok(AdmissionAuth::StaticBearer(token))
        }
        AzureAdmissionCredential::SharedKey {
            account,
            encoded_key,
        } => {
            let key = BASE64_STANDARD.decode(encoded_key).map_err(|_| {
                AdmissionError::Configuration("invalid Azure storage account key".to_string())
            })?;
            Ok(AdmissionAuth::SharedKey { account, key })
        }
        AzureAdmissionCredential::ManagedIdentity {
            endpoint,
            secret_header,
            client_id,
        } => {
            if secret_header.is_empty() {
                return Err(AdmissionError::Configuration(
                    "IDENTITY_HEADER is empty".to_string(),
                ));
            }
            let endpoint = Url::parse(&endpoint).map_err(|_| {
                AdmissionError::Configuration("invalid IDENTITY_ENDPOINT".to_string())
            })?;
            Ok(AdmissionAuth::ManagedIdentity(ManagedIdentity {
                endpoint,
                secret_header,
                client_id,
            }))
        }
    }
}

fn shared_key_authorization(
    account: &str,
    key: &[u8],
    method: &Method,
    url: &Url,
    headers: &HeaderMap,
) -> Result<String> {
    let canonical_headers = canonicalized_x_ms_headers(headers)?;
    let canonical_resource = canonicalized_resource(account, url);
    let fields = [
        method.as_str().to_string(),
        header(headers, "content-encoding")?,
        header(headers, "content-language")?,
        canonical_content_length(headers)?,
        header(headers, "content-md5")?,
        header(headers, "content-type")?,
        header(headers, "date")?,
        header(headers, "if-modified-since")?,
        header(headers, "if-match")?,
        header(headers, "if-none-match")?,
        header(headers, "if-unmodified-since")?,
        header(headers, "range")?,
    ];
    let string_to_sign = format!(
        "{}\n{}{}",
        fields.join("\n"),
        canonical_headers,
        canonical_resource
    );
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|err| AdmissionError::Authorization(err.to_string()))?;
    mac.update(string_to_sign.as_bytes());
    let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    Ok(format!("SharedKey {account}:{signature}"))
}

fn canonicalized_x_ms_headers(headers: &HeaderMap) -> Result<String> {
    let mut values = BTreeMap::<String, String>::new();
    for (name, value) in headers {
        let name = name.as_str().to_ascii_lowercase();
        if name.starts_with("x-ms-") {
            let value = value
                .to_str()
                .map_err(|err| AdmissionError::Authorization(err.to_string()))?;
            values.insert(
                name,
                value.split_ascii_whitespace().collect::<Vec<_>>().join(" "),
            );
        }
    }
    let mut out = String::new();
    for (name, value) in values {
        out.push_str(&name);
        out.push(':');
        out.push_str(&value);
        out.push('\n');
    }
    Ok(out)
}

fn canonicalized_resource(account: &str, url: &Url) -> String {
    let mut out = format!("/{account}{}", url.path());
    let mut query = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in url.query_pairs() {
        query
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.into_owned());
    }
    for (name, mut values) in query {
        values.sort();
        out.push('\n');
        out.push_str(&name);
        out.push(':');
        out.push_str(&values.join(","));
    }
    out
}

fn canonical_content_length(headers: &HeaderMap) -> Result<String> {
    let value = header(headers, "content-length")?;
    if value == "0" {
        Ok(String::new())
    } else {
        Ok(value)
    }
}

fn header(headers: &HeaderMap, name: &str) -> Result<String> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(ToOwned::to_owned)
                .map_err(|err| AdmissionError::Authorization(err.to_string()))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_storage::AzureStorageConfig;
    use std::env;
    use std::io::ErrorKind;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::JoinHandle;
    use std::time::Instant;

    enum ScriptedResponse {
        Status(&'static str),
        Raw(&'static str),
        Bytes(Vec<u8>),
        DropReply,
    }

    fn scripted_blob_service(
        responses: Vec<ScriptedResponse>,
    ) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..read]);
                    if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                match response {
                    ScriptedResponse::Status(status) => {
                        write!(
                            stream,
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .unwrap();
                        stream.flush().unwrap();
                    }
                    ScriptedResponse::Raw(raw) => {
                        stream.write_all(raw.as_bytes()).unwrap();
                        stream.flush().unwrap();
                    }
                    ScriptedResponse::Bytes(bytes) => {
                        stream.write_all(&bytes).unwrap();
                        stream.flush().unwrap();
                    }
                    ScriptedResponse::DropReply => {}
                }
            }
            requests
        });
        (endpoint, handle)
    }

    fn redirect_target(response: Vec<u8>) -> (String, JoinHandle<Option<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        let mut bytes = Vec::new();
                        let mut buffer = [0_u8; 1024];
                        loop {
                            let read = stream.read(&mut buffer).unwrap();
                            if read == 0 {
                                break;
                            }
                            bytes.extend_from_slice(&buffer[..read]);
                            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                        stream.write_all(&response).unwrap();
                        stream.flush().unwrap();
                        return Some(String::from_utf8(bytes).unwrap());
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return None;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("redirect target accept failed: {error}"),
                }
            }
        });
        (endpoint, handle)
    }

    fn scripted_client(endpoint: &str) -> AdmissionClient {
        let root = CanonicalAzureRoot::from_config(
            "az://omnigraph/clusters/scripted",
            AzureStorageConfig::new("companybrainprod"),
        )
        .unwrap();
        AdmissionClient {
            http: admission_http_client().unwrap(),
            root,
            blob_url: Url::parse(&format!("{endpoint}/admission/blob")).unwrap(),
            auth: AdmissionAuth::StaticBearer("test-token".to_string()),
        }
    }

    #[test]
    fn lease_ids_are_uuid_values() {
        let lease = LeaseId::new();
        assert_eq!(LeaseId::parse(lease.as_str()).unwrap(), lease);
        assert!(LeaseId::parse("not-a-uuid").is_err());
        assert!(!format!("{lease:?}").contains(lease.as_str()));
    }

    #[test]
    fn azure_error_code_is_bounded_to_the_code_element() {
        assert_eq!(
            xml_tag(
                "<?xml version=\"1.0\"?><Error><Code>LeaseAlreadyPresent</Code><Message>details</Message></Error>",
                "Code"
            )
            .as_deref(),
            Some("LeaseAlreadyPresent")
        );
    }

    #[tokio::test]
    async fn managed_identity_rejects_oversized_content_length_before_reading() {
        let oversized = MAX_RESPONSE_BODY_BYTES + 1;
        let response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {oversized}\r\nConnection: close\r\n\r\n");
        let (endpoint, server) =
            scripted_blob_service(vec![ScriptedResponse::Bytes(response.into_bytes())]);
        let identity = ManagedIdentity {
            endpoint: Url::parse(&endpoint).unwrap(),
            secret_header: "test-secret".to_string(),
            client_id: None,
        };

        let error = identity
            .access_token(&admission_http_client().unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            AdmissionError::ResponseBodyTooLarge {
                operation: "managed-identity token",
                limit: MAX_RESPONSE_BODY_BYTES,
                actual,
            } if actual == oversized as u64
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn managed_identity_redirect_never_forwards_the_secret_header() {
        let token_body = br#"{"access_token":"redirected-token"}"#;
        let target_response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            token_body.len(),
            std::str::from_utf8(token_body).unwrap()
        )
        .into_bytes();
        let (target_endpoint, target) = redirect_target(target_response);
        let redirect_response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {target_endpoint}/token\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let (identity_endpoint, redirector) = scripted_blob_service(vec![ScriptedResponse::Bytes(
            redirect_response.into_bytes(),
        )]);
        let identity = ManagedIdentity {
            endpoint: Url::parse(&identity_endpoint).unwrap(),
            secret_header: "TOPSECRET-IDENTITY-HEADER".to_string(),
            client_id: None,
        };

        let result = identity
            .access_token(&admission_http_client().unwrap())
            .await;
        redirector.join().unwrap();
        let forwarded = target.join().unwrap();

        assert!(matches!(
            result,
            Err(AdmissionError::IdentityStatus { status: 307 })
        ));
        assert!(
            forwarded.is_none(),
            "managed-identity redirect reached the second listener"
        );
    }

    #[tokio::test]
    async fn acquire_redirect_cannot_prove_ownership_from_another_endpoint() {
        let proposed = LeaseId::new();
        let target_response = format!(
            "HTTP/1.1 201 Created\r\nx-ms-lease-id: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            proposed.as_str()
        )
        .into_bytes();
        let (target_endpoint, target) = redirect_target(target_response);
        let redirect_response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {target_endpoint}/lease\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let (endpoint, redirector) = scripted_blob_service(vec![
            ScriptedResponse::Status("201 Created"),
            ScriptedResponse::Bytes(redirect_response.into_bytes()),
        ]);
        let client = scripted_client(&endpoint);

        let result = client.try_acquire(proposed).await;
        redirector.join().unwrap();
        let forwarded = target.join().unwrap();

        assert!(matches!(
            result,
            Err(AdmissionError::Azure {
                operation: "acquire lease",
                status: 307,
                ..
            })
        ));
        assert!(
            forwarded.is_none(),
            "lease redirect reached the echo endpoint"
        );
    }

    #[tokio::test]
    async fn azure_error_rejects_chunked_body_at_limit_plus_one() {
        let first_chunk = vec![b'x'; MAX_RESPONSE_BODY_BYTES];
        let mut response = format!(
            "HTTP/1.1 409 Conflict\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n",
            first_chunk.len()
        )
        .into_bytes();
        response.extend_from_slice(&first_chunk);
        response.extend_from_slice(b"\r\n1\r\ny\r\n0\r\n\r\n");
        let (endpoint, server) = scripted_blob_service(vec![ScriptedResponse::Bytes(response)]);
        let response = reqwest::get(endpoint).await.unwrap();

        let error = response_failure("chunked test", response)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            AdmissionError::ResponseBodyTooLarge {
                operation: "chunked test",
                limit: MAX_RESPONSE_BODY_BYTES,
                actual,
            } if actual == (MAX_RESPONSE_BODY_BYTES + 1) as u64
        ));
        server.join().unwrap();
    }

    #[test]
    fn lease_url_adds_only_the_lease_component() {
        let url = Url::parse("https://a.blob.core.windows.net/c/p/writer.lock").unwrap();
        assert_eq!(
            lease_url(&url).as_str(),
            "https://a.blob.core.windows.net/c/p/writer.lock?comp=lease"
        );
    }

    #[test]
    fn canonical_resource_sorts_query_values() {
        let url = Url::parse("https://a.blob.core.windows.net/c/p/writer.lock?z=2&comp=lease&z=1")
            .unwrap();
        assert_eq!(
            canonicalized_resource("a", &url),
            "/a/c/p/writer.lock\ncomp:lease\nz:1,2"
        );
    }

    #[test]
    fn shared_key_signature_never_contains_the_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-ms-date"),
            HeaderValue::from_static("Fri, 22 Aug 2026 00:00:00 GMT"),
        );
        headers.insert(
            HeaderName::from_static("x-ms-version"),
            HeaderValue::from_static(AZURE_API_VERSION),
        );
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
        let key = b"this-is-not-logged";
        let authorization = shared_key_authorization(
            "account",
            key,
            &Method::PUT,
            &Url::parse("https://account.blob.core.windows.net/c/blob").unwrap(),
            &headers,
        )
        .unwrap();
        assert!(authorization.starts_with("SharedKey account:"));
        assert!(!authorization.contains("this-is-not-logged"));
    }

    #[test]
    fn deployment_admission_requires_a_dedicated_non_reserved_prefix() {
        let config = AzureStorageConfig::new("companybrainprod");
        let empty = CanonicalAzureRoot::from_config("az://omnigraph", config.clone()).unwrap();
        assert!(
            AdmissionClient::with_static_bearer_for_test(empty, "test-token")
                .unwrap_err()
                .to_string()
                .contains("non-empty dedicated cluster prefix")
        );

        let reserved = CanonicalAzureRoot::from_config(
            "az://omnigraph/__omnigraph_azure_admission/cluster",
            config.clone(),
        )
        .unwrap();
        assert!(
            AdmissionClient::with_static_bearer_for_test(reserved, "test-token")
                .unwrap_err()
                .to_string()
                .contains("reserved '__omnigraph_azure_admission/' namespace")
        );

        let valid =
            CanonicalAzureRoot::from_config("az://omnigraph/clusters/company-brain", config)
                .unwrap();
        AdmissionClient::with_static_bearer_for_test(valid, "test-token").unwrap();
    }

    #[tokio::test]
    async fn lost_acquire_reply_requires_exact_id_renewal_proof() {
        let (endpoint, server) = scripted_blob_service(vec![
            ScriptedResponse::Status("201 Created"),
            ScriptedResponse::DropReply,
            ScriptedResponse::Status("200 OK"),
        ]);
        let client = scripted_client(&endpoint);
        let proposed = LeaseId::new();
        assert_eq!(
            client.try_acquire(proposed.clone()).await.unwrap(),
            AcquireOutcome::Acquired(proposed)
        );
        let requests = server.join().unwrap();
        assert!(requests[1].contains("x-ms-lease-action: acquire"));
        assert!(requests[2].contains("x-ms-lease-action: renew"));
    }

    #[tokio::test]
    async fn malformed_success_requires_exact_id_renewal_proof() {
        for acquire_response in [
            ScriptedResponse::Status("201 Created"),
            ScriptedResponse::Raw(
                "HTTP/1.1 201 Created\r\nx-ms-lease-id: 00000000-0000-0000-0000-000000000001\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ),
        ] {
            let (endpoint, server) = scripted_blob_service(vec![
                ScriptedResponse::Status("201 Created"),
                acquire_response,
                ScriptedResponse::Status("200 OK"),
            ]);
            let client = scripted_client(&endpoint);
            let proposed = LeaseId::new();
            assert_eq!(
                client.try_acquire(proposed.clone()).await.unwrap(),
                AcquireOutcome::Acquired(proposed)
            );
            let requests = server.join().unwrap();
            assert!(requests[2].contains("x-ms-lease-action: renew"));
        }
    }

    #[tokio::test]
    async fn leased_blob_create_precondition_still_reaches_held_acquire() {
        let (endpoint, server) = scripted_blob_service(vec![
            ScriptedResponse::Raw(
                "HTTP/1.1 412 Precondition Failed\r\nConnection: close\r\n\r\n<Error><Code>LeaseIdMissing</Code></Error>",
            ),
            ScriptedResponse::Raw(
                "HTTP/1.1 409 Conflict\r\nConnection: close\r\n\r\n<Error><Code>LeaseAlreadyPresent</Code></Error>",
            ),
        ]);
        let client = scripted_client(&endpoint);
        assert_eq!(
            client.try_acquire(LeaseId::new()).await.unwrap(),
            AcquireOutcome::Held
        );
        let requests = server.join().unwrap();
        assert!(requests[0].contains("if-none-match: *"));
        assert!(requests[1].contains("x-ms-lease-action: acquire"));
    }

    #[tokio::test]
    async fn unexpected_success_statuses_never_prove_lease_state() {
        let (endpoint, server) = scripted_blob_service(vec![
            ScriptedResponse::Status("201 Created"),
            ScriptedResponse::Status("204 No Content"),
            ScriptedResponse::Status("204 No Content"),
        ]);
        let client = scripted_client(&endpoint);
        let proposed = LeaseId::new();
        assert_eq!(
            client.try_acquire(proposed.clone()).await.unwrap(),
            AcquireOutcome::Ambiguous(proposed)
        );
        server.join().unwrap();

        let (endpoint, server) =
            scripted_blob_service(vec![ScriptedResponse::Status("204 No Content")]);
        let client = scripted_client(&endpoint);
        assert_eq!(
            client.release(&LeaseId::new()).await.unwrap(),
            ReleaseOutcome::Ambiguous
        );
        server.join().unwrap();

        let (endpoint, server) =
            scripted_blob_service(vec![ScriptedResponse::Status("204 No Content")]);
        let client = scripted_client(&endpoint);
        assert!(client.renew(&LeaseId::new()).await.is_err());
        server.join().unwrap();

        let (endpoint, server) = scripted_blob_service(vec![ScriptedResponse::Status("200 OK")]);
        let client = scripted_client(&endpoint);
        assert!(client.break_after_operator_proof().await.is_err());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn break_returns_only_after_unlocked_state_is_observed() {
        let (endpoint, server) = scripted_blob_service(vec![
            ScriptedResponse::Status("202 Accepted"),
            ScriptedResponse::Raw(
                "HTTP/1.1 200 OK\r\nx-ms-lease-status: unlocked\r\nx-ms-lease-state: broken\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ),
        ]);
        let client = scripted_client(&endpoint);
        client.break_after_operator_proof().await.unwrap();
        let requests = server.join().unwrap();
        assert!(requests[0].contains("x-ms-lease-action: break"));
        assert!(requests[1].starts_with("HEAD "));
    }

    #[tokio::test]
    async fn mutation_statuses_are_ambiguous_without_exact_renewal_proof() {
        for status in [
            "408 Request Timeout",
            "429 Too Many Requests",
            "500 Internal Server Error",
            "503 Service Unavailable",
        ] {
            let (endpoint, server) = scripted_blob_service(vec![
                ScriptedResponse::Status("201 Created"),
                ScriptedResponse::Status(status),
                ScriptedResponse::Status("412 Precondition Failed"),
            ]);
            let client = scripted_client(&endpoint);
            let proposed = LeaseId::new();
            assert_eq!(
                client.try_acquire(proposed.clone()).await.unwrap(),
                AcquireOutcome::Ambiguous(proposed)
            );
            let requests = server.join().unwrap();
            assert!(requests[2].contains("x-ms-lease-action: renew"));

            let (endpoint, server) = scripted_blob_service(vec![ScriptedResponse::Status(status)]);
            let client = scripted_client(&endpoint);
            assert_eq!(
                client.release(&LeaseId::new()).await.unwrap(),
                ReleaseOutcome::Ambiguous
            );
            server.join().unwrap();
        }
    }

    #[tokio::test]
    async fn unproved_lost_acquire_and_lost_release_are_ambiguous() {
        let (endpoint, server) = scripted_blob_service(vec![
            ScriptedResponse::Status("201 Created"),
            ScriptedResponse::DropReply,
            ScriptedResponse::DropReply,
        ]);
        let client = scripted_client(&endpoint);
        let proposed = LeaseId::new();
        assert_eq!(
            client.try_acquire(proposed.clone()).await.unwrap(),
            AcquireOutcome::Ambiguous(proposed)
        );
        server.join().unwrap();

        let (endpoint, server) = scripted_blob_service(vec![ScriptedResponse::DropReply]);
        let client = scripted_client(&endpoint);
        assert_eq!(
            client.release(&LeaseId::new()).await.unwrap(),
            ReleaseOutcome::Ambiguous
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn configured_azurite_enforces_one_lease_owner_and_explicit_break() {
        let Ok(container) = env::var("OMNIGRAPH_AZURE_TEST_CONTAINER") else {
            eprintln!("skipping Azurite admission test: OMNIGRAPH_AZURE_TEST_CONTAINER is not set");
            return;
        };
        let root_uri = format!(
            "az://{container}/admission-tests/{}",
            Uuid::new_v4().simple()
        );
        let client = AdmissionClient::from_env(&root_uri).unwrap();

        let contenders = 8;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(contenders));
        let mut tasks = Vec::new();
        for _ in 0..contenders {
            let client = client.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let proposed = LeaseId::new();
                barrier.wait().await;
                (proposed.clone(), client.try_acquire(proposed).await)
            }));
        }

        let mut owner = None;
        let mut held = 0;
        for task in tasks {
            let (proposed, outcome) = task.await.unwrap();
            match outcome.unwrap() {
                AcquireOutcome::Acquired(acquired) => {
                    assert_eq!(acquired, proposed);
                    assert!(owner.replace(acquired).is_none(), "more than one lease won");
                }
                AcquireOutcome::Held => held += 1,
                AcquireOutcome::Ambiguous(id) => {
                    panic!("Azurite acquire was unexpectedly ambiguous for {id:?}")
                }
            }
        }
        assert_eq!(held, contenders - 1);
        let owner = owner.expect("one contender must acquire the lease");
        assert!(matches!(
            client.inspect().await.unwrap(),
            LeaseState::Present {
                status: Some(ref status),
                ..
            } if status == "locked"
        ));
        assert_eq!(
            client.release(&owner).await.unwrap(),
            ReleaseOutcome::Released
        );

        let replacement = LeaseId::new();
        assert_eq!(
            client.try_acquire(replacement.clone()).await.unwrap(),
            AcquireOutcome::Acquired(replacement.clone())
        );
        client.break_after_operator_proof().await.unwrap();
        assert!(!client.renew(&replacement).await.unwrap());
    }
}
