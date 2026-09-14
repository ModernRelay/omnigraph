//! The provider SDK owns OAuth encoding; this transport owns our I/O bounds.
use super::*;
use std::sync::Arc;
use workos::transport::{HttpRequest, HttpResponse, HttpTransport, TransportError};
use workos::user_management::{
    AuthenticateWithDeviceCodeParams, AuthenticateWithRefreshTokenParams, CreateDeviceParams,
};

const AUTHORIZE: &str = "https://api.workos.com/user_management/authorize";
const DEVICE: &str = "https://api.workos.com/user_management/authorize/device";
const TOKEN: &str = "https://api.workos.com/user_management/authenticate";
const MAX_PROVIDER_BODY: usize = 64 * 1024;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub version: u8,
    pub provider: String,
    pub client_id: String,
    pub issuer: String,
    pub organization_id: String,
    pub authorization_endpoint: String,
    pub device_authorization_endpoint: String,
    pub token_endpoint: String,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let issuer = url::Url::parse(&self.issuer).map_err(|_| Failure::protocol())?;
        if self.version != 1
            || self.provider != "workos_authkit"
            || !self.client_id.starts_with("client_")
            || !self.organization_id.starts_with("org_")
            || [&self.client_id, &self.organization_id].iter().any(|s| {
                s.len() > 128 || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            })
            || self.issuer.len() > 2048
            || issuer.scheme() != "https"
            || issuer.host_str().is_none()
            || !issuer.username().is_empty()
            || issuer.password().is_some()
            || issuer.query().is_some()
            || issuer.fragment().is_some()
            || self.authorization_endpoint != AUTHORIZE
            || self.device_authorization_endpoint != DEVICE
            || self.token_endpoint != TOKEN
        {
            return Err(Failure::refused(
                "auth_profile_invalid",
                "the API authentication profile is unsupported or invalid",
            ));
        }
        Ok(())
    }

    pub async fn discover(origin: &str) -> Result<Self> {
        let body = Api::new(origin.into(), None)?
            .request(Method::GET, "/v1/auth/config", None, None)
            .await?;
        let config: Self =
            serde_json::from_value(body["data"].clone()).map_err(|_| Failure::protocol())?;
        config.validate()?;
        Ok(config)
    }
}

struct BoundedTransport(reqwest::Client);

#[async_trait::async_trait]
impl HttpTransport for BoundedTransport {
    async fn execute(
        &self,
        request: HttpRequest,
    ) -> std::result::Result<HttpResponse, TransportError> {
        // Never send credentials to a discovery-supplied endpoint or SDK redirect.
        if request.method != Method::POST
            || ![DEVICE, TOKEN].contains(&request.url.as_str())
            || request.headers.contains_key(reqwest::header::AUTHORIZATION)
            || request
                .body
                .as_ref()
                .is_some_and(|body| body.len() > 32 * 1024)
        {
            return Err(TransportError::other(std::io::Error::other(
                "provider request refused",
            )));
        }
        let bounded = async {
            let mut outgoing = self
                .0
                .request(request.method, &request.url)
                .headers(request.headers);
            if let Some(body) = request.body {
                outgoing = outgoing.body(body);
            }
            let mut response = outgoing.send().await.map_err(|_| {
                TransportError::other(std::io::Error::other("provider transport failed"))
            })?;
            if response.status().is_redirection()
                || response
                    .content_length()
                    .is_some_and(|n| n > MAX_PROVIDER_BODY as u64)
            {
                return Err(TransportError::other(std::io::Error::other(
                    "provider response refused",
                )));
            }
            let status = response.status();
            let headers = response.headers().clone();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| {
                TransportError::other(std::io::Error::other("provider response failed"))
            })? {
                if chunk.len() > MAX_PROVIDER_BODY.saturating_sub(bytes.len()) {
                    return Err(TransportError::other(std::io::Error::other(
                        "provider response too large",
                    )));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(HttpResponse {
                status,
                headers,
                body: bytes.into(),
            })
        };
        tokio::time::timeout(Duration::from_secs(25), bounded)
            .await
            .map_err(|_| {
                TransportError::timeout(std::io::Error::other("provider deadline exceeded"))
            })?
    }
}

pub(super) struct Provider(pub workos::Client);

impl Provider {
    pub fn new(config: &Config) -> Result<Self> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(25))
            .build()
            .map_err(|_| Failure::protocol())?;
        Self::with_transport(config, Arc::new(BoundedTransport(http)))
    }

    pub(super) fn with_transport(
        config: &Config,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<Self> {
        config.validate()?;
        workos::Client::builder()
            .client_id(&config.client_id)
            .max_retries(0)
            .transport(transport)
            .try_build()
            .map(Self)
            .map_err(|_| Failure::protocol())
    }

    pub async fn device(
        &self,
        config: &Config,
    ) -> std::result::Result<workos::DeviceAuthorizationResponse, workos::Error> {
        self.0
            .user_management()
            .create_device_with_options(
                CreateDeviceParams::new(workos::SSODeviceAuthorizationRequest {
                    client_id: config.client_id.clone(),
                }),
                Some(&workos::RequestOptions::new().strategy(workos::RequestStrategy::Once)),
            )
            .await
    }

    pub async fn poll(
        &self,
        code: &str,
    ) -> std::result::Result<workos::AuthenticateResponse, workos::Error> {
        self.0
            .user_management()
            .authenticate_with_device_code_with_options(
                AuthenticateWithDeviceCodeParams::new(code),
                Some(&workos::RequestOptions::new().strategy(workos::RequestStrategy::Once)),
            )
            .await
    }

    pub async fn refresh(
        &self,
        token: &str,
        config: &Config,
    ) -> std::result::Result<workos::AuthenticateResponse, workos::Error> {
        let mut params = AuthenticateWithRefreshTokenParams::new(token);
        params.organization_id = Some(config.organization_id.clone());
        self.0
            .user_management()
            .authenticate_with_refresh_token_with_options(
                params,
                Some(&workos::RequestOptions::new().strategy(workos::RequestStrategy::Once)),
            )
            .await
    }
}

pub(super) fn failure(error: &workos::Error) -> Failure {
    // Provider error bodies can reflect secrets. Only expose our fixed vocabulary.
    match error.code() {
        Some("invalid_grant" | "invalid_refresh_token" | "session_revoked" | "session_expired") => {
            login_required()
        }
        Some("access_denied") => Failure::refused("device_denied", "device sign-in was denied"),
        Some("expired_token") => Failure::refused(
            "device_expired",
            "device sign-in expired; run login --api again",
        ),
        _ => Failure::new(
            "identity_provider_unavailable",
            "the identity provider request failed; no credentials or provider response are displayed",
            1,
        ),
    }
}
