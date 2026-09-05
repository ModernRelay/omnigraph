//! Managed human credentials never share the legacy named-server token file.
use super::{Api, Failure, Method, Result, Value, canonical_origin, json};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::Instant;

const MAX_SECRET: usize = 16 * 1024;

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

fn session(value: &str) -> Result<Session> {
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
    let now = OffsetDateTime::now_utc();
    if session.version != 1 || expires > now + time::Duration::minutes(15) {
        return Err(Failure::refused(
            "credential_invalid",
            "the saved managed session exceeds its 15-minute lifetime",
        ));
    }
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
    store: &impl Store,
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

pub(super) fn credential(store: &OsStore, origin: &str) -> Result<String> {
    credential_from(
        store,
        origin,
        env("OMNIGRAPH_CONTROL_TOKEN")?,
        env("OMNIGRAPH_CONTROL_API")?,
    )
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
            ] {
                items.remove(field);
            }
            for item in items.values_mut() {
                scrub_value(item, secret);
            }
        }
        _ => {}
    }
}

async fn login_with(store: &impl Store, origin: String) -> Result<Value> {
    // Detect unsupported/unavailable keychains before asking the user to log in.
    let _ = store.get(&origin)?;
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
                let mut body = response.body;
                let data = body
                    .get_mut("data")
                    .and_then(Value::as_object_mut)
                    .ok_or_else(Failure::protocol)?;
                let token = data
                    .remove("access_token")
                    .and_then(|v| v.as_str().map(str::to_string))
                    .ok_or_else(Failure::protocol)?;
                validate_token(&token)?;
                if data.get("token_type").and_then(Value::as_str) != Some("Bearer") {
                    return Err(Failure::protocol());
                }
                let expiry = data
                    .get("expires_at")
                    .and_then(Value::as_str)
                    .ok_or_else(Failure::protocol)?
                    .to_string();
                for field in ["principal_id", "subject", "account_id"] {
                    if !data
                        .get(field)
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty() && s.len() <= 1024)
                    {
                        return Err(Failure::protocol());
                    }
                }
                if !data.get("scopes").is_some_and(Value::is_object) {
                    return Err(Failure::protocol());
                }
                // Only the bounded, opaque service credential enters the keychain.
                let saved = serde_json::to_string(&Session {
                    version: 1,
                    access_token: token.clone(),
                    expires_at: expiry,
                })
                .map_err(|_| Failure::protocol())?;
                session(&saved)?;
                if let Err(failure) = store.put(&origin, &saved) {
                    let revoke = Api::new(origin.clone(), Some(token))?;
                    let _ = revoke
                        .request(Method::POST, "/v1/auth/logout", None, None)
                        .await;
                    return Err(failure);
                }
                // Provider/device credentials are not part of the public login result.
                scrub_value(&mut body, &token);
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

pub(super) async fn login(store: &OsStore, origin: String) -> Result<Value> {
    login_with(store, origin).await
}

async fn logout_with(store: &impl Store, origin: String) -> Result<Value> {
    let value = store.get(&origin)?.ok_or_else(|| {
        Failure::refused(
            "login_required",
            "no managed session is stored for this API",
        )
    })?;
    let parsed = session(&value);
    let result = match parsed {
        Ok(session) => {
            let api = Api::new(origin.clone(), Some(session.access_token.clone()))?;
            api.request(Method::POST, "/v1/auth/logout", None, None)
                .await
                .map_err(|e| scrub(e, &session.access_token))
        }
        Err(err) => Err(err),
    };
    store.remove(&origin)?;
    result.map_err(|mut err| {
        err.body["local_credential_removed"] = json!(true);
        err.body["revocation_confirmed"] = json!(false);
        err
    })
}

pub(super) async fn logout(store: &OsStore, origin: String) -> Result<Value> {
    logout_with(store, origin).await
}

#[cfg(test)]
pub(super) mod tests;
