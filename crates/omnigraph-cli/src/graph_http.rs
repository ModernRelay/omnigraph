//! Admission for the graph HTTP protocol, before any response body is exposed.

use std::time::Duration;

use color_eyre::Result;
use color_eyre::eyre::bail;
use omnigraph_api_types::{
    ErrorCode, HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER, IF_ACTOR_HEADER,
};
use reqwest::{Method, RequestBuilder, Response, StatusCode, Url};
use serde::Serialize;

use crate::helpers::MissingCredential;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// The schemes of a server address. `http` and `https` reach the server over
/// TCP; `unix` carries the same HTTP/1.1 protocol over a Unix-domain socket.
const SERVER_SCHEMES: [&str; 3] = ["http", "https", "unix"];

/// The origin every request over a Unix socket names. The socket alone
/// selects the server: no DNS lookup or proxy sees this host.
#[cfg(unix)]
const UNIX_SOCKET_ORIGIN: &str = "http://localhost";

const SERVER_ADDRESS_SHAPES: &str = "http://HOST[:PORT][/PREFIX], https://HOST[:PORT][/PREFIX] or \
     unix:///ABSOLUTE/PATH.sock";

/// The server scheme a `scheme://…` value names, matched case-insensitively
/// as URL schemes are. `None` for every other value, storage URIs included.
pub(crate) fn server_scheme(value: &str) -> Option<&'static str> {
    let (scheme, _) = value.split_once("://")?;
    SERVER_SCHEMES
        .into_iter()
        .find(|known| scheme.eq_ignore_ascii_case(known))
}

/// How requests reach a server root, decided once from its scheme.
#[derive(Debug, Clone)]
pub(crate) enum ServerTransport {
    /// `http://` or `https://`: each request is sent to its composed URL.
    Tcp,
    /// `unix:///absolute/path.sock`: HTTP/1.1 over the socket at `socket`.
    /// The root's whole path names the socket, so a request's HTTP target is
    /// what its composed URL carries after `root`, the root's URL form.
    #[cfg(unix)]
    Unix {
        root: String,
        socket: std::path::PathBuf,
    },
}

impl ServerTransport {
    /// The one parser of a server address: a captured server target URL, a
    /// configured target endpoint, or a managed endpoint. A value with any
    /// other scheme is refused as a server address, never tried as storage.
    pub(crate) fn parse(root: &str) -> Result<Self> {
        match server_scheme(root) {
            Some("http" | "https") => Ok(Self::Tcp),
            Some(_) => unix_transport(root.trim_end_matches('/')),
            None => match root.split_once("://") {
                Some((scheme, _)) => bail!(
                    "invalid server address: `{scheme}://` is not a server scheme; a server \
                     address is {SERVER_ADDRESS_SHAPES}"
                ),
                None => {
                    bail!("invalid server address: a server address is {SERVER_ADDRESS_SHAPES}")
                }
            },
        }
    }

    /// The URL a request composed under this root is sent to.
    fn http_url(&self, composed: &str) -> Result<Url> {
        let composed = Url::parse(composed)?;
        match self {
            Self::Tcp => Ok(composed),
            #[cfg(unix)]
            Self::Unix { root, .. } => {
                let target = composed
                    .as_str()
                    .strip_prefix(root.as_str())
                    .filter(|target| target.is_empty() || target.starts_with(['/', '?']))
                    .ok_or_else(|| {
                        color_eyre::eyre::eyre!(
                            "request URL is outside the Unix-socket server root; nothing was sent"
                        )
                    })?;
                Ok(Url::parse(&format!("{UNIX_SOCKET_ORIGIN}{target}"))?)
            }
        }
    }
}

/// Where a request composed under a server root goes, as the transport
/// derives it: the endpoint it connects to and the HTTP path it sends, with
/// `.` and `..` segments already resolved. A credential is matched against
/// this, never against the address as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestTarget {
    endpoint: Endpoint,
    /// The sent path's segments, without the empty one a trailing slash
    /// leaves.
    segments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Endpoint {
    /// Scheme, host and port, the scheme's default port filled in.
    Tcp {
        scheme: String,
        host: String,
        port: Option<u16>,
    },
    /// The socket path the transport connects to.
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}

impl RequestTarget {
    /// The target of `url`, a URL composed under `server_root`.
    pub(crate) fn of(server_root: &str, url: &str) -> Result<Self> {
        let transport = ServerTransport::parse(server_root)?;
        let sent = transport.http_url(url)?;
        Ok(Self::from_sent(&transport, &sent))
    }

    /// The request after transport conversion, retaining the selected Unix socket.
    fn from_sent(transport: &ServerTransport, sent: &Url) -> Self {
        let endpoint = match transport {
            ServerTransport::Tcp => Endpoint::Tcp {
                scheme: sent.scheme().to_owned(),
                host: sent.host_str().unwrap_or_default().to_owned(),
                port: sent.port_or_known_default(),
            },
            #[cfg(unix)]
            ServerTransport::Unix { socket, .. } => Endpoint::Unix(socket.clone()),
        };
        let mut segments: Vec<String> = sent
            .path_segments()
            .map(|segments| segments.map(str::to_owned).collect())
            .unwrap_or_default();
        if segments.last().is_some_and(String::is_empty) {
            segments.pop();
        }
        Self { endpoint, segments }
    }

    /// Whether this target is `base` or lies under it: the same endpoint, and
    /// a path whose leading segments are all of `base`'s.
    pub(crate) fn is_under(&self, base: &Self) -> bool {
        self.endpoint == base.endpoint && self.segments.starts_with(&base.segments)
    }

    /// How many path segments the target names; a deeper base is a more
    /// specific match.
    pub(crate) fn depth(&self) -> usize {
        self.segments.len()
    }
}

/// `unix:///absolute/path.sock`: no host, port, user, query or fragment, and
/// a path that names a socket, never a directory (a `.` or `..` segment that
/// normalizes to one included). The path is percent-decoded, so a socket
/// under a directory with spaces may be written either way.
#[cfg(unix)]
fn unix_transport(root: &str) -> Result<ServerTransport> {
    use std::os::unix::ffi::OsStrExt;

    let shape = || {
        color_eyre::eyre::eyre!(
            "invalid server address: a Unix-socket server is unix:///ABSOLUTE/PATH.sock, with \
             no host, query or fragment"
        )
    };
    let url = Url::parse(root).map_err(|_| shape())?;
    if url.host().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.path().starts_with('/')
        || url.path().ends_with('/')
    {
        return Err(shape());
    }
    let socket: Vec<u8> = percent_encoding::percent_decode_str(url.path()).collect();
    if socket.contains(&0) {
        bail!("invalid server address: a Unix-socket path cannot contain a NUL byte");
    }
    Ok(ServerTransport::Unix {
        socket: std::ffi::OsStr::from_bytes(&socket).into(),
        root: url.into(),
    })
}

#[cfg(not(unix))]
fn unix_transport(_root: &str) -> Result<ServerTransport> {
    bail!("invalid server address: a unix:// server needs a Unix platform")
}

/// A failure establishing the contract for this request. Earlier requests in a
/// compound command may already have effects; this marker says nothing about them.
#[derive(Debug, Serialize)]
pub(crate) struct ApiContractError {
    pub(crate) error: String,
    pub(crate) code: ErrorCode,
    pub(crate) http_status: Option<u16>,
    pub(crate) request_dispatched: bool,
}

impl ApiContractError {
    fn discovery(status: Option<StatusCode>) -> Self {
        Self {
            error: format!(
                "could not establish {HTTP_API_CONTRACT_HEADER}: {HTTP_API_CONTRACT} through successful server discovery; this data request was not sent; configure the server root and select the graph separately"
            ),
            code: ErrorCode::ApiContractMismatch,
            http_status: status.map(|status| status.as_u16()),
            request_dispatched: false,
        }
    }

    fn discovery_transport(error: reqwest::Error, transport: &ServerTransport) -> Self {
        // Classify the typed cause instead of rendering its source chain:
        // connector errors can contain URLs or reflected credentials.
        let cause = if error.is_timeout() {
            "server discovery timed out"
        } else if error.is_connect() {
            match transport {
                ServerTransport::Tcp => {
                    "server discovery connection failed (DNS, TCP, proxy, or TLS)"
                }
                #[cfg(unix)]
                ServerTransport::Unix { .. } => "server discovery connection failed (Unix socket)",
            }
        } else {
            "server discovery transport failed"
        };
        Self {
            error: format!(
                "{cause}; could not establish {HTTP_API_CONTRACT_HEADER}: {HTTP_API_CONTRACT}; this data request was not sent"
            ),
            code: ErrorCode::ApiContractMismatch,
            http_status: error.status().map(|status| status.as_u16()),
            request_dispatched: false,
        }
    }

    fn response(status: StatusCode) -> Self {
        Self {
            error: format!(
                "server response did not carry exactly one {HTTP_API_CONTRACT_HEADER}: {HTTP_API_CONTRACT} header; this request's effects are unknown; do not retry automatically"
            ),
            code: ErrorCode::ApiContractMismatch,
            http_status: Some(status.as_u16()),
            request_dispatched: true,
        }
    }
}

impl std::fmt::Display for ApiContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.error)?;
        if let Some(status) = self.http_status {
            write!(f, " (HTTP {status})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiContractError {}

/// The explicit server root owns discovery, including any proxy path prefix.
/// Never derive it from a graph URL: a proxy prefix may itself contain `graphs`.
pub(crate) struct GraphHttpClient {
    operator_transport: Option<crate::operator_transport::CapturedTransport>,
    client: reqwest::Client,
    transport: ServerTransport,
    health_url: Url,
    /// Why this client's requests carry no credential, attached to a 401
    /// response so its refusal says how to bind one.
    missing_credential: Option<Box<MissingCredential>>,
    /// The actor `--as` asserts, sent as `omnigraph-if-actor` on every
    /// data request, so the server refuses a credential that resolves to
    /// another actor before anything runs.
    expected_actor: Option<String>,
    /// Self-hosted requests retain the selected owner across every composed route.
    /// Managed authority is independent and never uses operator server declarations.
    request_scope: Option<crate::targets::ServerRequestScope>,
    /// A bearer is a bound credential with a lifetime, not only a header value.
    /// Public discovery may outlive its validity.
    credential: Option<crate::credential_sources::AcquiredCredential>,
}

impl GraphHttpClient {
    pub(crate) fn new(server_root: &str) -> Result<Self> {
        Self::with_deadline(server_root, false)
    }

    pub(crate) fn managed(server_root: &str) -> Result<Self> {
        Self::with_deadline(server_root, true)
    }

    /// Capture self-hosted endpoint ownership and anonymous-401 guidance together.
    pub(crate) fn server_target(
        input: &crate::operator::InvocationConfig,
        target: &crate::targets::ResolvedTarget,
    ) -> Result<Self> {
        use crate::targets::{CredentialOwner, ServerAuth, TargetBinding};
        let TargetBinding::Server { endpoint, auth, .. } = &target.binding else {
            bail!("server HTTP transport requires a server target");
        };
        let scope = crate::targets::capture_server_scope(input, auth, endpoint)?;
        let mut client = Self::new(endpoint)?.with_request_scope(scope);
        if matches!(auth, ServerAuth::Anonymous) {
            let request = RequestTarget::of(endpoint, endpoint)?;
            let missing = match input
                .get(&mut std::io::sink())?
                .credential_owner_for(&request)
            {
                Some(CredentialOwner::Anonymous { target }) => {
                    MissingCredential::AnonymousTarget(target.to_owned())
                }
                _ => MissingCredential::Unbound {
                    server_root: endpoint.clone(),
                    config: input.config_path(),
                },
            };
            client = client.with_missing_credential(missing);
        } else {
            let credential = crate::operator_routing::AUTH
                .try_with(|selected| {
                    selected
                        .as_ref()
                        .ok_or_else(|| color_eyre::eyre::eyre!("no frozen credential binding"))?
                        .cached_credential(endpoint)
                })??
                .ok_or_else(|| {
                    color_eyre::eyre::eyre!("selected server credential is unavailable")
                })?;
            client = client.with_credential(credential)?;
        }
        Ok(client)
    }

    /// Every client of one root shares its transport; only the managed JSON
    /// deadlines differ.
    fn build(
        transport: &ServerTransport,
        managed: bool,
        captured: Option<&crate::operator_transport::CapturedTransport>,
    ) -> Result<reqwest::Client> {
        let builder = reqwest::Client::builder()
            .no_proxy()
            .tls_built_in_webpki_certs(false)
            .tls_built_in_native_certs(true)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
        let builder = if let Some(captured) = captured {
            captured.apply(builder)?
        } else {
            builder
        };
        let builder = match transport {
            ServerTransport::Tcp => builder,
            // Proxies and DNS do not apply to the socket.
            #[cfg(unix)]
            ServerTransport::Unix { socket, .. } => builder.unix_socket(socket.as_path()),
        };
        let builder = if managed {
            builder
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
        } else {
            builder
        };
        Ok(builder.build()?)
    }

    fn with_deadline(server_root: &str, managed: bool) -> Result<Self> {
        let operator_transport = crate::operator_routing::transport(server_root)?;
        let transport = ServerTransport::parse(server_root)?;
        let mut health_url =
            transport.http_url(&crate::helpers::remote_url(server_root, &["healthz"], &[])?)?;
        // Discovery is public. Never propagate URL-embedded basic credentials.
        let _ = health_url.set_username("");
        let _ = health_url.set_password(None);
        Ok(Self {
            client: Self::build(&transport, managed, operator_transport.as_ref())?,
            operator_transport,
            transport,
            health_url,
            missing_credential: None,
            expected_actor: None,
            request_scope: None,
            credential: None,
        })
    }

    pub(crate) fn with_credential(
        mut self,
        credential: crate::credential_sources::AcquiredCredential,
    ) -> Result<Self> {
        use crate::credential_sources::{CredentialError, Purpose};
        let binding = credential.request();
        let endpoint = RequestTarget::of(binding.endpoint(), binding.endpoint())?;
        if binding.purpose() != Purpose::Data
            || !RequestTarget::from_sent(&self.transport, &self.health_url).is_under(&endpoint)
        {
            return Err(CredentialError::BindingMismatch.into());
        }
        self.credential = Some(credential);
        Ok(self)
    }

    pub(crate) fn with_request_scope(mut self, scope: crate::targets::ServerRequestScope) -> Self {
        self.request_scope = Some(scope);
        self
    }

    fn validate_scope(&self, url: &Url) -> Result<()> {
        if let Some(scope) = &self.request_scope {
            scope.validate(&RequestTarget::from_sent(&self.transport, url))?;
        }
        Ok(())
    }

    pub(crate) fn with_missing_credential(mut self, missing: MissingCredential) -> Self {
        self.missing_credential = Some(Box::new(missing));
        self
    }

    /// Assert `actor` on every data request this client sends.
    pub(crate) fn with_expected_actor(mut self, actor: Option<&str>) -> Self {
        self.expected_actor = actor.map(str::to_owned);
        self
    }

    /// Blob delivery retains its existing streaming deadline instead of
    /// inheriting the managed JSON request's total thirty-second deadline.
    pub(crate) fn blob_delivery(&self) -> Result<Self> {
        Ok(Self {
            client: Self::build(&self.transport, false, self.operator_transport.as_ref())?,
            operator_transport: self.operator_transport.clone(),
            transport: self.transport.clone(),
            health_url: self.health_url.clone(),
            missing_credential: self.missing_credential.clone(),
            expected_actor: self.expected_actor.clone(),
            request_scope: self.request_scope.clone(),
            credential: self.credential.clone(),
        })
    }

    /// A request to `url`, composed under this client's root with
    /// [`crate::helpers::remote_url`].
    pub(crate) fn request(&self, method: Method, url: String) -> Result<RequestBuilder> {
        let url = self.transport.http_url(&url)?;
        self.validate_scope(&url)?;
        Ok(self.client.request(method, url))
    }

    pub(crate) async fn send(&self, request: RequestBuilder) -> Result<Response> {
        let request = self.discovered(request).await?;
        let bearer = request
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_owned);
        if let Some(credential) = &self.credential {
            let binding = credential.request();
            let endpoint = RequestTarget::of(binding.endpoint(), binding.endpoint())?;
            if !RequestTarget::from_sent(&self.transport, request.url()).is_under(&endpoint)
                || Some(
                    credential
                        .bearer_for(binding, time::OffsetDateTime::now_utc())?
                        .expose(),
                ) != bearer.as_deref()
            {
                return Err(crate::credential_sources::CredentialError::BindingMismatch.into());
            }
        }
        crate::command_outcome::dispatched(request.method());
        let mut response = self.client.execute(request).await?;
        crate::command_outcome::response(response.status(), response.headers(), bearer.as_deref());
        if !has_contract(&response) {
            return Err(ApiContractError::response(response.status()).into());
        }
        if response.status() == StatusCode::UNAUTHORIZED
            && bearer.is_none()
            && let Some(missing) = &self.missing_credential
        {
            response
                .extensions_mut()
                .insert(MissingCredential::clone(missing));
        }
        Ok(response)
    }

    /// [`Self::send`] for `/mcp`, which follows the MCP transport instead of
    /// the graph contract: the request is scoped, asserts its actor and is
    /// preceded by discovery as every other is, but the server stamps no
    /// contract header on an MCP answer, so none is required. Every answer,
    /// a 401 included, is the caller's to read.
    pub(crate) async fn send_mcp(&self, request: RequestBuilder) -> Result<Response> {
        let request = self.discovered(request).await?;
        Ok(self.client.execute(request).await?)
    }

    /// The request with its contract and actor headers, once its route lies
    /// in the captured scope and a fresh discovery probe established the
    /// contract. Nothing of the request has been sent when this fails.
    async fn discovered(&self, request: RequestBuilder) -> Result<reqwest::Request> {
        let mut request = request.header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT);
        if let Some(actor) = &self.expected_actor {
            request = request.header(IF_ACTOR_HEADER, actor);
        }
        let request = request.build()?;
        // Validate the exact route before even public discovery. A nested endpoint
        // declaration cannot silently replace the already captured credential owner.
        self.validate_scope(request.url())?;
        self.validate_scope(&self.health_url)?;
        // No bearer and no body consumption. A fresh probe precedes each data
        // request; it is discovery, not a fence against a changed backend.
        let discovery = self
            .client
            .head(self.health_url.clone())
            .timeout(DISCOVERY_TIMEOUT)
            .send()
            .await
            .map_err(|error| ApiContractError::discovery_transport(error, &self.transport))?;
        if !discovery.status().is_success() || !has_contract(&discovery) {
            return Err(ApiContractError::discovery(Some(discovery.status())).into());
        }
        Ok(request)
    }
}

fn has_contract(response: &Response) -> bool {
    let mut values = response.headers().get_all(HTTP_API_CONTRACT_HEADER).iter();
    matches!(values.next(), Some(value) if value.as_bytes() == HTTP_API_CONTRACT.as_bytes())
        && values.next().is_none()
}
