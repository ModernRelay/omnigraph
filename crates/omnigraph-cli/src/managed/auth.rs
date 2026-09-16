//! Managed human credentials never share the legacy named-server token file.
use super::{Api, Failure, Method, Result, Value, canonical_origin, json};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::Instant;

mod coordination;
mod provider;
mod renewal;

const MAX_SECRET: usize = 16 * 1024;
// Metadata from another clock may be slightly ahead. Actual expiry never has grace.
const MAX_METADATA_CLOCK_SKEW: time::Duration = time::Duration::seconds(30);

pub(super) trait Store {
    fn get(&self, origin: &str) -> Result<Option<String>>;
    fn put(&self, origin: &str, value: &str) -> Result<()>;
    fn remove(&self, origin: &str) -> Result<()>;
}

pub(super) struct OsStore {
    service: &'static str,
}

const AUTHKIT_STORE: OsStore = OsStore {
    service: "omnigraph.workos-authkit.session.v1",
};
pub(super) const DATA_STORE: OsStore = OsStore {
    service: "omnigraph.data-plane.credential.v1",
};

fn keychain_failed() -> Failure {
    // Some keyring errors contain raw secret bytes: never render the error.
    Failure::refused(
        "keychain_unavailable",
        "the OS keychain is unavailable; no plaintext credential fallback is used",
    )
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
impl Store for OsStore {
    fn get(&self, origin: &str) -> Result<Option<String>> {
        let entry = keyring::Entry::new(self.service, origin).map_err(|_| keychain_failed())?;
        match entry.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(_) => Err(keychain_failed()),
        }
    }
    fn put(&self, origin: &str, value: &str) -> Result<()> {
        keyring::Entry::new(self.service, origin)
            .and_then(|entry| entry.set_password(value))
            .map_err(|_| keychain_failed())
    }
    fn remove(&self, origin: &str) -> Result<()> {
        let entry = keyring::Entry::new(self.service, origin).map_err(|_| keychain_failed())?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(_) => Err(keychain_failed()),
        }
    }
}

// keyring otherwise defaults to an in-memory mock on unsupported targets.
// Refuse explicitly rather than claiming the session was securely persisted.
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
)))]
impl Store for OsStore {
    fn get(&self, _: &str) -> Result<Option<String>> {
        Err(keychain_failed())
    }
    fn put(&self, _: &str, _: &str) -> Result<()> {
        Err(keychain_failed())
    }
    fn remove(&self, _: &str) -> Result<()> {
        Err(keychain_failed())
    }
}

fn validate_token(token: &str) -> Result<()> {
    if token.is_empty() || token.len() > MAX_SECRET || !token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(Failure::refused(
            "credential_invalid",
            "the credential is invalid",
        ));
    }
    Ok(())
}

fn login_required() -> Failure {
    Failure::refused(
        "login_required",
        "sign in with login --api before continuing",
    )
}

fn env(name: &str) -> Result<Option<String>> {
    std::env::var(name).map(Some).or_else(|error| match error {
        std::env::VarError::NotPresent => Ok(None),
        _ => Err(Failure::refused(
            "credential_invalid",
            "credential environment variables must be valid UTF-8",
        )),
    })
}

fn automation(origin: &str, token: Option<String>, api: Option<String>) -> Result<Option<String>> {
    match (token, api) {
        (None, None) => Ok(None),
        (Some(token), Some(api)) => {
            if canonical_origin(&api)? != origin {
                return Err(Failure::refused(
                    "credential_origin_mismatch",
                    "OMNIGRAPH_CONTROL_API does not match the selected API",
                ));
            }
            validate_token(&token)?;
            Ok(Some(token))
        }
        _ => Err(Failure::refused(
            "credential_origin_required",
            "OMNIGRAPH_CONTROL_TOKEN and OMNIGRAPH_CONTROL_API must be supplied together",
        )),
    }
}

pub(super) async fn credential(origin: &str) -> Result<String> {
    if let Some(token) = automation(
        origin,
        env("OMNIGRAPH_CONTROL_TOKEN")?,
        env("OMNIGRAPH_CONTROL_API")?,
    )? {
        return Ok(token);
    }
    let _lock = coordination::lock(origin).await?;
    renewal::credential(&AUTHKIT_STORE, origin).await
}

pub(super) async fn cache_lock(key: &str) -> Result<std::fs::File> {
    coordination::lock(key).await
}

async fn selected_principal_with(
    store: &dyn Store,
    origin: &str,
    token: Option<String>,
    api: Option<String>,
) -> Result<Option<String>> {
    if let Some(token) = automation(origin, token, api)? {
        let body = Api::new(origin.into(), Some(token.clone()))?
            .request(Method::GET, "/v1/auth/session", None, None)
            .await
            .map_err(|failure| scrub(failure, &token))?;
        return bounded_string(&body["data"], "principal_id", 1024)
            .map(|principal| Some(principal.into()));
    }
    Ok(renewal::load(store, origin)?.map(|saved| saved.identity.principal_id))
}

pub(super) async fn selected_principal(origin: &str) -> Result<Option<String>> {
    selected_principal_with(
        &AUTHKIT_STORE,
        origin,
        env("OMNIGRAPH_CONTROL_TOKEN")?,
        env("OMNIGRAPH_CONTROL_API")?,
    )
    .await
}

fn bounded_string<'a>(value: &'a Value, name: &str, max: usize) -> Result<&'a str> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= max && !s.chars().any(char::is_control))
        .ok_or_else(Failure::protocol)
}

fn verification_uri(uri: &str) -> Result<()> {
    let url = url::Url::parse(uri).map_err(|_| Failure::protocol())?;
    if uri.len() > 4096
        || url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Failure::protocol());
    }
    Ok(())
}

fn scrub(mut failure: Failure, secret: &str) -> Failure {
    scrub_value(&mut failure.body, secret);
    failure
}

pub(super) fn scrub_value(value: &mut Value, secret: &str) {
    match value {
        Value::String(s) => *s = s.replace(secret, "[redacted]"),
        Value::Array(items) => {
            for item in items {
                scrub_value(item, secret);
            }
        }
        Value::Object(items) => {
            for field in [
                "access_token",
                "refresh_token",
                "device_code",
                "id_token",
                "client_secret",
                "csrf_token",
            ] {
                items.remove(field);
            }
            let original = std::mem::take(items);
            for (key, mut item) in original {
                scrub_value(&mut item, secret);
                items.insert(key.replace(secret, "[redacted]"), item);
            }
        }
        _ => {}
    }
}

fn seconds(value: f64, minimum: u64, maximum: u64) -> Result<u64> {
    if !value.is_finite()
        || value.fract() != 0.0
        || value < minimum as f64
        || value > maximum as f64
    {
        return Err(Failure::protocol());
    }
    Ok(value as u64)
}

async fn login_with(
    store: &dyn Store,
    origin: &str,
    config: provider::Config,
    provider: &provider::Provider,
) -> Result<Value> {
    let _ = store.get(origin)?;
    if let Some(saved) = renewal::load(store, origin)?
        && saved.config == config
        && saved.state == renewal::RefreshState::Ready
        && renewal::timestamp(&saved.refresh_expires_at)? > OffsetDateTime::now_utc()
    {
        match renewal::credential_with(store, origin, provider).await {
            Ok(token) => {
                let verified = renewal::verify(origin, &config, &token, None).await;
                match verified {
                    Ok((identity, expiry, mut body)) => {
                        if identity != saved.identity {
                            return Err(Failure::refused(
                                "session_identity_mismatch",
                                "the stored sign-in identity changed",
                            ));
                        }
                        body["data"]["expires_at"] = json!(if renewal::timestamp(&expiry)?
                            > renewal::timestamp(&saved.refresh_expires_at)?
                        {
                            &saved.refresh_expires_at
                        } else {
                            &expiry
                        });
                        body["data"]["refresh_expires_at"] = json!(saved.refresh_expires_at);
                        return Ok(body);
                    }
                    Err(error)
                        if matches!(
                            error.body["type"].as_str(),
                            Some("login_required" | "unauthenticated")
                        ) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error)
                if matches!(
                    error.body["type"].as_str(),
                    Some("login_required" | "refresh_outcome_unknown")
                ) => {}
            Err(error) => return Err(error),
        }
    }
    let started = Instant::now();
    let device = provider
        .device(&config)
        .await
        .map_err(|error| provider::failure(&error))?;
    validate_token(&device.device_code)?;
    if device.user_code.is_empty()
        || device.user_code.len() > 128
        || device.user_code.chars().any(char::is_control)
    {
        return Err(Failure::protocol());
    }
    verification_uri(&device.verification_uri)?;
    let uri = device
        .verification_uri_complete
        .as_deref()
        .unwrap_or(&device.verification_uri);
    verification_uri(uri)?;
    if uri.contains(&device.device_code) || device.user_code.contains(&device.device_code) {
        return Err(Failure::protocol());
    }
    let lifetime = seconds(device.expires_in, 1, 600)?;
    let mut interval = seconds(device.interval.unwrap_or(5.0), 5, 600)?;
    let deadline = started + Duration::from_secs(lifetime);
    eprintln!("Open {uri}\nEnter code: {}", device.user_code);
    loop {
        let next = Instant::now() + Duration::from_secs(interval);
        if next >= deadline {
            return Err(Failure::refused(
                "device_expired",
                "device sign-in expired; run login --api again",
            ));
        }
        tokio::time::sleep_until(next).await;
        let reply = tokio::time::timeout_at(deadline, provider.poll(&device.device_code))
            .await
            .map_err(|_| {
                Failure::refused(
                    "device_expired",
                    "device sign-in expired; run login --api again",
                )
            })?;
        match reply {
            Ok(tokens) => {
                let (saved, body) = renewal::accept(origin, config, tokens, None).await?;
                if let Err(error) = renewal::save(store, &saved) {
                    // We cannot retain the new renewable session. Revoke it
                    // when possible without obscuring the custody failure.
                    saved.revoke().await;
                    return Err(error);
                }
                return Ok(body);
            }
            Err(error) if error.code() == Some("authorization_pending") => {}
            Err(error) if error.code() == Some("slow_down") => {
                interval = interval.saturating_add(5).min(600);
            }
            Err(error) => return Err(provider::failure(&error)),
        }
    }
}

pub(super) async fn login(origin: String) -> Result<Value> {
    let _lock = coordination::lock(&origin).await?;
    let _ = AUTHKIT_STORE.get(&origin)?;
    let config = provider::Config::discover(&origin).await?;
    let provider = provider::Provider::new(&config)?;
    login_with(&AUTHKIT_STORE, &origin, config, &provider).await
}

pub(super) async fn logout(origin: String) -> Result<Value> {
    let _lock = coordination::lock(&origin).await?;
    renewal::logout(&AUTHKIT_STORE, &origin).await
}

#[cfg(test)]
pub(super) mod tests;
