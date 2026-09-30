//! Admission for the graph HTTP protocol, before any response body is exposed.

use std::time::Duration;

use color_eyre::Result;
use omnigraph_api_types::{ErrorCode, HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
use reqwest::{Method, RequestBuilder, Response, StatusCode, Url};
use serde::Serialize;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// A failure establishing the contract for this request. Earlier requests in a
/// compound command may already have effects; this marker says nothing about them.
#[derive(Debug, Serialize)]
pub(crate) struct ApiContractError {
    pub(crate) error: &'static str,
    pub(crate) code: ErrorCode,
    pub(crate) http_status: Option<u16>,
    pub(crate) request_dispatched: bool,
}

impl ApiContractError {
    fn discovery(status: Option<StatusCode>) -> Self {
        Self {
            error: "could not establish Omnigraph-Http-Api: 0.12 through successful server discovery; this data request was not sent; configure the server root and select the graph separately",
            code: ErrorCode::ApiContractMismatch,
            http_status: status.map(|status| status.as_u16()),
            request_dispatched: false,
        }
    }

    fn response(status: StatusCode) -> Self {
        Self {
            error: "server response did not carry exactly one Omnigraph-Http-Api: 0.12 header; this request's effects are unknown; do not retry automatically",
            code: ErrorCode::ApiContractMismatch,
            http_status: Some(status.as_u16()),
            request_dispatched: true,
        }
    }
}

impl std::fmt::Display for ApiContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.error)?;
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
    health_url: Url,
}

impl GraphHttpClient {
    pub(crate) fn new(server_root: &str) -> Result<Self> {
        Self::with_deadline(server_root, false)
    }

    pub(crate) fn managed(server_root: &str) -> Result<Self> {
        Self::with_deadline(server_root, true)
    }

    fn builder() -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
    }

    fn with_deadline(server_root: &str, managed: bool) -> Result<Self> {
        let mut health_url =
            Url::parse(&crate::helpers::remote_url(server_root, &["healthz"], &[])?)?;
        // Discovery is public. Never propagate URL-embedded basic credentials.
        let _ = health_url.set_username("");
        let _ = health_url.set_password(None);
        let builder = Self::builder();
        let builder = if managed {
            builder
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
        } else {
            builder
        };
        Ok(Self {
            client: builder.build()?,
            health_url,
        })
    }

    /// Blob delivery retains its existing streaming deadline instead of
    /// inheriting the managed JSON request's total thirty-second deadline.
    pub(crate) fn blob_delivery(&self) -> Result<Self> {
        Ok(Self {
            client: Self::builder().build()?,
            health_url: self.health_url.clone(),
        })
    }

    pub(crate) fn request(&self, method: Method, url: String) -> RequestBuilder {
        self.client.request(method, url)
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
            .map_err(|error| ApiContractError::discovery(error.status()))?;
        if !discovery.status().is_success() || !has_contract(&discovery) {
            return Err(ApiContractError::discovery(Some(discovery.status())).into());
        }
        drop(discovery);
        let response = request
            .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
            .send()
            .await?;
        if !has_contract(&response) {
            return Err(ApiContractError::response(response.status()).into());
        }
        Ok(response)
    }
}

fn has_contract(response: &Response) -> bool {
    let mut values = response.headers().get_all(HTTP_API_CONTRACT_HEADER).iter();
    matches!(values.next(), Some(value) if value.as_bytes() == HTTP_API_CONTRACT.as_bytes())
        && values.next().is_none()
}
