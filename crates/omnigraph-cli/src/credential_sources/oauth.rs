//! One noninteractive OAuth client-credentials exchange at an explicit endpoint.
use std::time::Duration;

use serde::Deserialize;
use time::OffsetDateTime;

use super::{
    AcquiredCredential, BearerSecret, CredentialError, CredentialRequest, MAX_CREDENTIAL_BYTES,
    Purpose, Result,
};
use crate::operator_schema::ClientSecretSource;

pub(crate) fn validate_client_credentials(
    endpoint: &str,
    client_id: &str,
    scope: Option<&str>,
) -> Result<()> {
    let url = url::Url::parse(endpoint).map_err(|_| CredentialError::InvalidClientCredentials)?;
    if endpoint.len() > 4096
        || url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || endpoint.contains('\\')
        || endpoint
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        || client_id.is_empty()
        || client_id.len() > 512
        || !client_id.bytes().all(|c| (0x21..=0x7e).contains(&c))
        || scope.is_some_and(|s| {
            s.is_empty()
                || s.len() > 4096
                || s.split(' ').any(|part| {
                    part.is_empty()
                        || !part.bytes().all(|b| {
                            b == 0x21 || (0x23..=0x5b).contains(&b) || (0x5d..=0x7e).contains(&b)
                        })
                })
        })
    {
        return Err(CredentialError::InvalidClientCredentials);
    }
    Ok(())
}

fn secret(source: &ClientSecretSource) -> Result<String> {
    let value = match source {
        ClientSecretSource::Env { env } => {
            super::read_environment(env, |name| std::env::var_os(name))?
        }
        ClientSecretSource::File { file } => {
            let mut bytes = super::read_file(file)?;
            if bytes.last() == Some(&b'\n') {
                bytes.pop();
                if bytes.last() == Some(&b'\r') {
                    bytes.pop();
                }
            }
            String::from_utf8(bytes).map_err(|_| CredentialError::InvalidClientCredentials)?
        }
    };
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(CredentialError::InvalidClientCredentials);
    }
    Ok(value)
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

fn check_status(status: reqwest::StatusCode) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        Err(CredentialError::ClientCredentialsUnavailable)
    } else {
        Err(CredentialError::ClientCredentialsRefused)
    }
}

fn decode(
    request: &CredentialRequest,
    bytes: &[u8],
    started: OffsetDateTime,
) -> Result<AcquiredCredential> {
    if bytes.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::TooLarge);
    }
    let response: TokenResponse =
        serde_json::from_slice(bytes).map_err(|_| CredentialError::ClientCredentialsResponse)?;
    if !response.token_type.eq_ignore_ascii_case("bearer") || response.expires_in == 0 {
        return Err(CredentialError::ClientCredentialsResponse);
    }
    let seconds = i64::try_from(response.expires_in)
        .map_err(|_| CredentialError::ClientCredentialsResponse)?;
    let expires_at = started
        .checked_add(time::Duration::seconds(seconds))
        .ok_or(CredentialError::ClientCredentialsResponse)?;
    let credential = AcquiredCredential {
        request: request.clone(),
        secret: BearerSecret::new(response.access_token)?,
        expires_at: Some(expires_at),
    };
    credential.bearer_for(request, OffsetDateTime::now_utc())?;
    Ok(credential)
}

pub(crate) async fn acquire_client_credentials(
    request: &CredentialRequest,
    endpoint: &str,
    client_id: &str,
    source: &ClientSecretSource,
    scope: Option<&str>,
) -> Result<AcquiredCredential> {
    validate_client_credentials(endpoint, client_id, scope)?;
    if request.purpose() != Purpose::Control {
        return Err(CredentialError::InvalidBinding);
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .tls_built_in_webpki_certs(false)
        .tls_built_in_native_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| CredentialError::ClientCredentialsUnavailable)?;
    let secret = secret(source)?;
    let mut form = vec![
        ("grant_type", "client_credentials"),
        ("client_id", client_id),
        ("client_secret", secret.as_str()),
    ];
    if let Some(scope) = scope {
        form.push(("scope", scope));
    }
    let started = OffsetDateTime::now_utc();
    let exchange = async {
        let mut response = client
            .post(endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|_| CredentialError::ClientCredentialsUnavailable)?;
        check_status(response.status())?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_CREDENTIAL_BYTES as u64)
        {
            return Err(CredentialError::TooLarge);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CredentialError::ClientCredentialsUnavailable)?
        {
            if chunk.len() > MAX_CREDENTIAL_BYTES.saturating_sub(bytes.len()) {
                return Err(CredentialError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        decode(request, &bytes, started)
    };
    // One deadline covers response headers and every body chunk.
    tokio::time::timeout(Duration::from_secs(25), exchange)
        .await
        .map_err(|_| CredentialError::ClientCredentialsUnavailable)?
}

#[cfg(test)]
mod tests;
