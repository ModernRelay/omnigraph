//! Rotating session credentials remain exclusively in the OS credential store.
use super::*;

const MAX_CACHE: usize = 64 * 1024;
const RENEW_BEFORE: time::Duration = time::Duration::seconds(30);

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RefreshState {
    Ready,
    Pending,
    LoginRequired,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Identity {
    principal_id: String,
    subject: String,
    account_id: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Renewable {
    version: u8,
    access_token: String,
    expires_at: String,
    refresh_token: String,
    refresh_expires_at: String,
    identity: Identity,
    state: RefreshState,
}

fn invalid() -> Failure {
    invalid_reason("the saved renewable session format is invalid")
}

fn invalid_reason(detail: &'static str) -> Failure {
    Failure::refused("credential_invalid", detail)
}

fn timestamp(value: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| invalid_reason("the session timestamp is not valid RFC 3339"))
}

fn identity(data: &Value) -> Result<Identity> {
    Ok(Identity {
        principal_id: bounded_string(data, "principal_id", 1024)?.into(),
        subject: bounded_string(data, "subject", 1024)?.into(),
        account_id: bounded_string(data, "account_id", 1024)?.into(),
    })
}

fn parse(value: &str) -> Result<Renewable> {
    parse_at(value, OffsetDateTime::now_utc())
}

fn parse_at(value: &str, now: OffsetDateTime) -> Result<Renewable> {
    if value.len() > MAX_CACHE {
        return Err(invalid_reason("the saved renewable session exceeds 64 KiB"));
    }
    let saved: Renewable = serde_json::from_str(value).map_err(|_| invalid())?;
    validate_token(&saved.access_token)?;
    validate_refresh(&saved.refresh_token)?;
    let expiry = timestamp(&saved.expires_at)?;
    let refresh_expiry = timestamp(&saved.refresh_expires_at)?;
    if saved.version != 2 {
        return Err(invalid_reason(
            "the saved renewable session version is unsupported",
        ));
    }
    if expiry > now + time::Duration::minutes(15) + MAX_METADATA_CLOCK_SKEW {
        return Err(invalid_reason(
            "the access expiry exceeds its 15-minute lifetime and 30-second clock tolerance",
        ));
    }
    if refresh_expiry > now + time::Duration::hours(8) + MAX_METADATA_CLOCK_SKEW {
        return Err(invalid_reason(
            "the refresh deadline exceeds its 8-hour lifetime and 30-second clock tolerance",
        ));
    }
    if expiry > refresh_expiry {
        return Err(invalid_reason(
            "the access expiry is later than the session refresh deadline",
        ));
    }
    if identity(&serde_json::to_value(&saved.identity).map_err(|_| invalid())?).is_err() {
        return Err(invalid_reason(
            "the saved renewable session identity is invalid",
        ));
    }
    Ok(saved)
}

fn validate_refresh(token: &str) -> Result<()> {
    if token.is_empty() || token.len() > MAX_SECRET || token.chars().any(char::is_control) {
        return Err(invalid_reason(
            "the refresh credential must be nonempty, at most 16 KiB, and contain no control characters",
        ));
    }
    Ok(())
}

fn save(store: &dyn Store, origin: &str, saved: &Renewable) -> Result<()> {
    let value = serde_json::to_string(saved).map_err(|_| invalid())?;
    parse(&value)?;
    store.put(origin, &value)
}

fn unknown() -> Failure {
    Failure::refused(
        "refresh_outcome_unknown",
        "session renewal may have completed; run login --api explicitly to recover",
    )
}

fn login_required() -> Failure {
    Failure::refused("login_required", "the session requires login --api")
}

fn scrub_pair(mut failure: Failure, saved: &Renewable) -> Failure {
    scrub_value(&mut failure.body, &saved.access_token);
    scrub_value(&mut failure.body, &saved.refresh_token);
    failure
}

/// Both the original pair and the pending marker survive an uncertain exchange.
async fn refresh(store: &dyn Store, origin: &str, saved: &mut Renewable) -> Result<()> {
    let original = saved.clone();
    saved.state = RefreshState::Pending;
    save(store, origin, saved)?;
    let api = Api::new(origin.into(), Some(saved.access_token.clone()))?;
    let response = api
        .raw_with_timeout(
            Method::POST,
            "/v1/auth/refresh",
            Some(&json!({"refresh_token": saved.refresh_token})),
            None,
            Duration::from_secs(35),
        )
        .await
        .map_err(|_| unknown())?;
    if !response.status.is_success() {
        let failure = Failure {
            body: response.body,
            exit: if response.status.is_client_error() {
                2
            } else {
                1
            },
        };
        if response.status.as_u16() == 503
            && failure.body["type"] == "refresh_unavailable"
            && failure.body["exchange_started"] == false
        {
            save(store, origin, &original)?;
            *saved = original;
        } else if response.status.as_u16() == 401 && failure.body["type"] == "login_required" {
            saved.state = RefreshState::LoginRequired;
            save(store, origin, saved)?;
        }
        return Err(scrub_pair(failure, saved));
    }
    let (next, _) = from_response(&response.body, Some(saved)).map_err(|_| unknown())?;
    let next = next.ok_or_else(unknown)?;
    if let Err(failure) = save(store, origin, &next) {
        // The old pending entry remains authoritative if storing the rotation fails.
        let revoke = Api::new(origin.into(), Some(next.access_token.clone()))?;
        let _ = revoke
            .request(Method::POST, "/v1/auth/logout", None, None)
            .await;
        return Err(failure);
    }
    *saved = next;
    Ok(())
}

pub(super) async fn credential(
    legacy: &dyn Store,
    current: &dyn Store,
    origin: &str,
) -> Result<String> {
    let Some(value) = current.get(origin)? else {
        return credential_from(legacy, origin, None, None);
    };
    let mut saved = parse(&value)?;
    let now = OffsetDateTime::now_utc();
    if saved.state == RefreshState::LoginRequired || timestamp(&saved.refresh_expires_at)? <= now {
        return Err(login_required());
    }
    let valid = timestamp(&saved.expires_at)? > now;
    if saved.state == RefreshState::Pending {
        return if valid {
            Ok(saved.access_token)
        } else {
            Err(unknown())
        };
    }
    if timestamp(&saved.expires_at)? > now + RENEW_BEFORE {
        return Ok(saved.access_token);
    }
    match refresh(current, origin, &mut saved).await {
        Ok(()) => Ok(saved.access_token),
        Err(failure)
            if saved.state != RefreshState::LoginRequired
                && timestamp(&saved.expires_at)? > OffsetDateTime::now_utc() =>
        {
            // A failed renewal does not invalidate an unexpired access credential.
            let _ = failure;
            Ok(saved.access_token)
        }
        Err(failure) => Err(failure),
    }
}

/// Fresh metadata comes from the API; the cached identity only detects drift.
pub(super) async fn cached_login(
    legacy: &dyn Store,
    current: &dyn Store,
    origin: &str,
) -> Result<Option<Value>> {
    let expected = current.get(origin)?.map(|v| parse(&v)).transpose()?;
    if expected
        .as_ref()
        .is_some_and(|s| s.state != RefreshState::Ready)
    {
        return Ok(None);
    }
    let token = match credential(legacy, current, origin).await {
        Ok(token) => token,
        Err(failure) if failure.exit == 2 && failure.body["type"] == "login_required" => {
            return Ok(None);
        }
        Err(failure) => return Err(failure),
    };
    let active = current.get(origin)?.map(|v| parse(&v)).transpose()?;
    let response = Api::new(origin.into(), Some(token.clone()))?
        .raw(Method::GET, "/v1/auth/session", None, None)
        .await?;
    let mut body = match response.status.as_u16() {
        200..=299 => response.body,
        401 if matches!(
            response.body["type"].as_str(),
            Some("login_required" | "unauthenticated")
        ) =>
        {
            return Ok(None);
        }
        _ => {
            let failure = Failure {
                body: response.body,
                exit: if response.status.is_client_error() {
                    2
                } else {
                    1
                },
            };
            let mut failure = scrub(failure, &token);
            if let Some(saved) = &expected {
                failure = scrub_pair(failure, saved);
            }
            if let Some(saved) = &active {
                failure = scrub_pair(failure, saved);
            }
            return Err(failure);
        }
    };
    let actual = identity(&body["data"])?;
    if expected
        .as_ref()
        .is_some_and(|saved| saved.identity != actual)
    {
        return Err(Failure::refused(
            "session_identity_mismatch",
            "the session identity changed; log out before selecting another identity",
        ));
    }
    let expiry = timestamp(bounded_string(&body["data"], "expires_at", 128)?)?;
    if expiry <= OffsetDateTime::now_utc()
        || expiry
            > OffsetDateTime::now_utc() + time::Duration::minutes(15) + MAX_METADATA_CLOCK_SKEW
        || !body["data"]["scopes"].is_object()
    {
        return Err(Failure::protocol());
    }
    // Server metadata cannot establish that this client retained a usable pair.
    if !active
        .as_ref()
        .is_some_and(|saved| saved.state == RefreshState::Ready)
    {
        body["data"]
            .as_object_mut()
            .ok_or_else(Failure::protocol)?
            .remove("refresh_expires_at");
    }
    if let Some(refresh_expiry) = body["data"].get("refresh_expires_at") {
        let refresh_expiry = timestamp(refresh_expiry.as_str().ok_or_else(Failure::protocol)?)?;
        if refresh_expiry < expiry
            || refresh_expiry
                > OffsetDateTime::now_utc() + time::Duration::hours(8) + MAX_METADATA_CLOCK_SKEW
            || expected.as_ref().is_some_and(|saved| {
                timestamp(&saved.refresh_expires_at).ok() != Some(refresh_expiry)
            })
        {
            return Err(Failure::protocol());
        }
    }
    scrub_value(&mut body, &token);
    if let Some(saved) = expected {
        scrub_value(&mut body, &saved.refresh_token);
        scrub_value(&mut body, &saved.access_token);
    }
    if let Some(saved) = active {
        scrub_value(&mut body, &saved.refresh_token);
    }
    Ok(Some(body))
}

fn from_response(body: &Value, previous: Option<&Renewable>) -> Result<(Option<Renewable>, Value)> {
    let data = &body["data"];
    let token = bounded_string(data, "access_token", MAX_SECRET)?;
    validate_token(token)?;
    let identity = identity(data)?;
    if data["token_type"] != "Bearer" || !data["scopes"].is_object() {
        return Err(Failure::protocol());
    }
    let expires_at = bounded_string(data, "expires_at", 128)?.to_string();
    session(
        &serde_json::to_string(&Session {
            version: 1,
            access_token: token.into(),
            expires_at: expires_at.clone(),
        })
        .map_err(|_| Failure::protocol())?,
    )?;
    let mut public = body.clone();
    scrub_value(&mut public, token);
    if let Some(refresh) = data.get("refresh_token").and_then(Value::as_str) {
        scrub_value(&mut public, refresh);
    }
    let Some(refresh_expiry) = data.get("refresh_expires_at") else {
        return if previous.is_none() {
            Ok((None, public))
        } else {
            Err(Failure::protocol())
        };
    };
    let saved = Renewable {
        version: 2,
        access_token: token.into(),
        expires_at,
        refresh_token: bounded_string(data, "refresh_token", MAX_SECRET)?.into(),
        refresh_expires_at: refresh_expiry
            .as_str()
            .ok_or_else(Failure::protocol)?
            .into(),
        identity,
        state: RefreshState::Ready,
    };
    parse(&serde_json::to_string(&saved).map_err(|_| Failure::protocol())?)?;
    if timestamp(&saved.refresh_expires_at)? <= OffsetDateTime::now_utc()
        || previous.is_some_and(|old| {
            old.identity != saved.identity
                || timestamp(&old.refresh_expires_at).ok()
                    != timestamp(&saved.refresh_expires_at).ok()
                || old.access_token == saved.access_token
                || old.refresh_token == saved.refresh_token
        })
    {
        return Err(Failure::protocol());
    }
    Ok((Some(saved), public))
}

pub(super) fn store_login(
    legacy: &dyn Store,
    current: &dyn Store,
    origin: &str,
    body: &Value,
) -> Result<Value> {
    let (saved, public) = from_response(body, None)?;
    match saved {
        Some(saved) => save(current, origin, &saved)?,
        None => {
            let data = &body["data"];
            legacy.put(
                origin,
                &serde_json::to_string(&Session {
                    version: 1,
                    access_token: data["access_token"]
                        .as_str()
                        .ok_or_else(Failure::protocol)?
                        .into(),
                    expires_at: data["expires_at"]
                        .as_str()
                        .ok_or_else(Failure::protocol)?
                        .into(),
                })
                .map_err(|_| Failure::protocol())?,
            )?;
            current.remove(origin)?;
        }
    }
    Ok(public)
}

pub(super) async fn logout(legacy: &dyn Store, current: &dyn Store, origin: &str) -> Result<Value> {
    let renewable = current
        .get(origin)?
        .map(|value| parse(&value))
        .transpose()?;
    let token = match &renewable {
        Some(saved) => saved.access_token.clone(),
        None => {
            let value = legacy.get(origin)?.ok_or_else(login_required)?;
            session_record(&value)?.access_token
        }
    };
    let result = Api::new(origin.into(), Some(token.clone()))?
        .raw(Method::POST, "/v1/auth/logout", None, None)
        .await;
    match result {
        Ok(response)
            if response.status.is_success() && response.body["data"]["logged_out"] == true =>
        {
            let mut body = response.body;
            legacy.remove(origin)?;
            current.remove(origin)?;
            scrub_value(&mut body, &token);
            if let Some(saved) = renewable {
                scrub_value(&mut body, &saved.refresh_token);
            }
            Ok(body)
        }
        response => {
            let (terminal, mut failure) = match response {
                Ok(response) if response.status.is_success() => (false, Failure::protocol()),
                Ok(response) => (
                    response.status.as_u16() == 401
                        && matches!(
                            response.body["type"].as_str(),
                            Some("login_required" | "unauthenticated")
                        ),
                    Failure {
                        body: response.body,
                        exit: if response.status.is_client_error() {
                            2
                        } else {
                            1
                        },
                    },
                ),
                Err(failure) => (false, failure),
            };
            if terminal {
                legacy.remove(origin)?;
                current.remove(origin)?;
            }
            failure.body["local_credential_removed"] = json!(terminal);
            failure.body["revocation_confirmed"] = json!(false);
            let mut failure = scrub(failure, &token);
            if let Some(saved) = renewable {
                failure = scrub_pair(failure, &saved);
            }
            Err(failure)
        }
    }
}

#[cfg(test)]
mod tests;
