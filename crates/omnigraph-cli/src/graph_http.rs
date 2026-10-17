//! Admission for the graph HTTP protocol, before any response body is exposed.

use std::time::Duration;

use color_eyre::Result;
use color_eyre::eyre::bail;
use omnigraph_api_types::{ErrorCode, HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
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
    /// The one parser of a server address: a literal `--server` URL, a
    /// configured `servers.<name>.url`, or a managed endpoint. A value with any
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
    client: reqwest::Client,
    transport: ServerTransport,
    health_url: Url,
    /// Why this client's requests carry no credential, attached to a 401
    /// response so its refusal says how to bind one.
    missing_credential: Option<Box<MissingCredential>>,
}

impl GraphHttpClient {
    pub(crate) fn new(server_root: &str) -> Result<Self> {
        Self::with_deadline(server_root, false)
    }

    pub(crate) fn managed(server_root: &str) -> Result<Self> {
        Self::with_deadline(server_root, true)
    }

    /// Every client of one root shares its transport; only the managed JSON
    /// deadlines differ.
    fn build(transport: &ServerTransport, managed: bool) -> Result<reqwest::Client> {
        let builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
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
        let transport = ServerTransport::parse(server_root)?;
        let mut health_url =
            transport.http_url(&crate::helpers::remote_url(server_root, &["healthz"], &[])?)?;
        // Discovery is public. Never propagate URL-embedded basic credentials.
        let _ = health_url.set_username("");
        let _ = health_url.set_password(None);
        Ok(Self {
            client: Self::build(&transport, managed)?,
            transport,
            health_url,
            missing_credential: None,
        })
    }

    pub(crate) fn with_missing_credential(mut self, missing: MissingCredential) -> Self {
        self.missing_credential = Some(Box::new(missing));
        self
    }

    /// Blob delivery retains its existing streaming deadline instead of
    /// inheriting the managed JSON request's total thirty-second deadline.
    pub(crate) fn blob_delivery(&self) -> Result<Self> {
        Ok(Self {
            client: Self::build(&self.transport, false)?,
            transport: self.transport.clone(),
            health_url: self.health_url.clone(),
            missing_credential: self.missing_credential.clone(),
        })
    }

    /// A request to `url`, composed under this client's root with
    /// [`crate::helpers::remote_url`].
    pub(crate) fn request(&self, method: Method, url: String) -> Result<RequestBuilder> {
        Ok(self.client.request(method, self.transport.http_url(&url)?))
    }

    pub(crate) async fn send(&self, request: RequestBuilder) -> Result<Response> {
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
        drop(discovery);
        let request = request
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .build()?;
        let bearer = request
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_owned);
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
}

fn has_contract(response: &Response) -> bool {
    let mut values = response.headers().get_all(HTTP_API_CONTRACT_HEADER).iter();
    matches!(values.next(), Some(value) if value.as_bytes() == HTTP_API_CONTRACT.as_bytes())
        && values.next().is_none()
}
