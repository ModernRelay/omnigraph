//! Managed human credentials never share the legacy named-server token file.
use super::{Api, Failure, Method, Result, Value, canonical_origin, json};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::Instant;

mod coordination;
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

pub(super) const CONTROL_STORE: OsStore = OsStore {
    service: "omnigraph.control-plane.session.v1",
};
const RENEWABLE_STORE: OsStore = OsStore {
    service: "omnigraph.control-plane.session.v2",
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    version: u8,
    access_token: String,
    expires_at: String,
}

fn validate_token(token: &str) -> Result<()> {
    if token.is_empty() || token.len() > MAX_SECRET || !token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(Failure::refused(
            "credential_invalid",
            "the managed credential is invalid",
        ));
    }
    Ok(())
}

fn session_record(value: &str) -> Result<Session> {
    session_record_at(value, OffsetDateTime::now_utc())
}

fn session_record_at(value: &str, now: OffsetDateTime) -> Result<Session> {
    if value.len() > MAX_SECRET {
        return Err(Failure::refused(
            "credential_invalid",
            "the saved managed session is invalid",
        ));
    }
    let session: Session = serde_json::from_str(value).map_err(|_| {
        Failure::refused("credential_invalid", "the saved managed session is invalid")
    })?;
    validate_token(&session.access_token)?;
    let expires = OffsetDateTime::parse(&session.expires_at, &Rfc3339).map_err(|_| {
        Failure::refused(
            "credential_invalid",
            "the saved managed session expiry is invalid",
        )
    })?;
    if session.version != 1 {
        return Err(Failure::refused(
            "credential_invalid",
            "the saved managed session version is unsupported",
        ));
    }
    if expires > now + time::Duration::minutes(15) + MAX_METADATA_CLOCK_SKEW {
        return Err(Failure::refused(
            "credential_invalid",
            "the saved managed session exceeds its 15-minute lifetime and 30-second clock tolerance",
        ));
    }
    Ok(session)
}

fn session(value: &str) -> Result<Session> {
    session_at(value, OffsetDateTime::now_utc())
}

fn session_at(value: &str, now: OffsetDateTime) -> Result<Session> {
    let session = session_record_at(value, now)?;
    let expires =
        OffsetDateTime::parse(&session.expires_at, &Rfc3339).map_err(|_| Failure::protocol())?;
    if expires <= now {
        return Err(Failure::refused(
            "login_required",
            "the managed session has expired; run login --api again",
        ));
    }
    Ok(session)
}

fn env(name: &str) -> Result<Option<String>> {
    std::env::var(name).map(Some).or_else(|err| match err {
        std::env::VarError::NotPresent => Ok(None),
        std::env::VarError::NotUnicode(_) => Err(Failure::refused(
            "credential_invalid",
            "managed credential environment variables must be valid UTF-8",
        )),
    })
}

fn credential_from(
    store: &dyn Store,
    origin: &str,
    token: Option<String>,
    api: Option<String>,
) -> Result<String> {
    match (token, api) {
        (Some(token), Some(api)) => {
            if canonical_origin(&api)? != origin {
                return Err(Failure::refused(
                    "credential_origin_mismatch",
                    "OMNIGRAPH_CONTROL_API does not match the selected API origin",
                ));
            }
            validate_token(&token)?;
            Ok(token)
        }
        (None, None) => {
            let value = store.get(origin)?.ok_or_else(|| {
                Failure::refused(
                    "login_required",
                    "no managed session is stored for this API; run login --api",
                )
            })?;
            Ok(session(&value)?.access_token)
        }
        _ => Err(Failure::refused(
            "credential_origin_required",
            "OMNIGRAPH_CONTROL_TOKEN and OMNIGRAPH_CONTROL_API must be supplied together",
        )),
    }
}

pub(super) async fn credential(origin: &str) -> Result<String> {
    let token = env("OMNIGRAPH_CONTROL_TOKEN")?;
    let api = env("OMNIGRAPH_CONTROL_API")?;
    if token.is_some() || api.is_some() {
        return credential_from(&CONTROL_STORE, origin, token, api);
    }
    let _lock = coordination::lock(origin).await?;
    renewal::credential(&CONTROL_STORE, &RENEWABLE_STORE, origin).await
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
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Failure::protocol());
    }
    canonical_origin(&url.origin().ascii_serialization()).map_err(|_| Failure::protocol())?;
    Ok(())
}

fn interval(body: &Value) -> Result<u64> {
    body.get("interval")
        .and_then(Value::as_u64)
        .filter(|n| (5..=600).contains(n))
        .ok_or_else(Failure::protocol)
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

async fn login_with(store: &dyn Store, current: &dyn Store, origin: String) -> Result<Value> {
    // Detect unsupported/unavailable keychains before asking the user to log in.
    let _ = store.get(&origin)?;
    if let Some(body) = renewal::cached_login(store, current, &origin).await? {
        return Ok(body);
    }
    let api = Api::new(origin.clone(), None)?;
    let started = Instant::now();
    let initial = api
        .request(Method::POST, "/v1/auth/device", Some(&json!({})), None)
        .await?;
    let data = &initial["data"];
    let code = bounded_string(data, "device_code", MAX_SECRET)?.to_string();
    let user_code = bounded_string(data, "user_code", 128)?;
    let uri = bounded_string(data, "verification_uri", 4096)?;
    verification_uri(uri)?;
    let complete = match data.get("verification_uri_complete") {
        None | Some(Value::Null) => uri,
        Some(Value::String(value)) if !value.is_empty() && value.len() <= 4096 => {
            verification_uri(value)?;
            value
        }
        _ => return Err(Failure::protocol()),
    };
    let expires = data
        .get("expires_in")
        .and_then(Value::as_u64)
        .filter(|n| (1..=600).contains(n))
        .ok_or_else(Failure::protocol)?;
    let deadline = started + Duration::from_secs(expires);
    let mut poll_interval = interval(data)?;
    if user_code.contains(&code) || complete.contains(&code) {
        return Err(Failure::protocol());
    }
    eprintln!("Open {complete}\nEnter code: {user_code}");
    let result = async {
        loop {
            let next = Instant::now() + Duration::from_secs(poll_interval);
            if next >= deadline {
                tokio::time::sleep_until(deadline).await;
                return Err(Failure::refused(
                    "device_expired",
                    "device authorization expired; start login again",
                ));
            }
            tokio::time::sleep_until(next).await;
            let response = tokio::time::timeout_at(
                deadline,
                api.raw(
                    Method::POST,
                    "/v1/auth/device/token",
                    Some(&json!({"device_code":code})),
                    None,
                ),
            )
            .await
            .map_err(|_| {
                Failure::refused(
                    "device_expired",
                    "device authorization expired; start login again",
                )
            })??;
            if response.status.is_success() {
                let mut body = match renewal::store_login(store, current, &origin, &response.body) {
                    Ok(body) => body,
                    Err(failure) => {
                        if let Some(token) = response.body["data"]["access_token"].as_str()
                            && validate_token(token).is_ok()
                        {
                            let revoke = Api::new(origin.clone(), Some(token.into()))?;
                            let _ = revoke
                                .request(Method::POST, "/v1/auth/logout", None, None)
                                .await;
                        }
                        return Err(failure);
                    }
                };
                scrub_value(&mut body, &code);
                return Ok(body);
            }
            match (response.status.as_u16(), response.body["type"].as_str()) {
                (428, Some("authorization_pending")) => {
                    poll_interval = poll_interval.max(interval(&response.body)?)
                }
                (429, Some("slow_down")) => {
                    poll_interval = (poll_interval + 5).min(600).max(interval(&response.body)?)
                }
                (409, Some("device_poll_in_progress")) => {
                    return Err(Failure::refused(
                        "device_poll_in_progress",
                        "device authorization may have been consumed; start login again",
                    ));
                }
                _ => {
                    return Err(Failure {
                        body: response.body,
                        exit: if response.status.is_client_error() {
                            2
                        } else {
                            1
                        },
                    });
                }
            }
        }
    }
    .await;
    result.map_err(|failure| scrub(failure, &code))
}

pub(super) async fn login(origin: String) -> Result<Value> {
    let _lock = coordination::lock(&origin).await?;
    login_with(&CONTROL_STORE, &RENEWABLE_STORE, origin).await
}

pub(super) async fn logout(origin: String) -> Result<Value> {
    let _lock = coordination::lock(&origin).await?;
    renewal::logout(&CONTROL_STORE, &RENEWABLE_STORE, &origin).await
}

#[cfg(test)]
pub(super) mod tests;
