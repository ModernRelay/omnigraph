//! Provider credentials and their exact identity remain in the OS keychain.
use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest as _, Sha256};

const MAX_CACHE: usize = 64 * 1024;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum RefreshState {
    Ready,
    Pending,
    LoginRequired,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Identity {
    pub principal_id: String,
    account_id: String,
    subject: String,
    session_id: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Session {
    version: u8,
    origin: String,
    pub config: provider::Config,
    pub identity: Identity,
    access_token: String,
    expires_at: String,
    refresh_token: String,
    pub refresh_expires_at: String,
    pub state: RefreshState,
}

impl Session {
    pub(super) async fn revoke(&self) -> bool {
        revoke(&self.origin, self.access_token.clone()).await
    }
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    sid: String,
    org_id: String,
    client_id: String,
    iat: i64,
    exp: i64,
}

fn invalid() -> Failure {
    Failure::refused(
        "credential_invalid",
        "the saved AuthKit credential profile or identity is invalid",
    )
}

pub(super) fn timestamp(value: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| invalid())
}

fn claims(token: &str, config: &provider::Config, allow_expired: bool) -> Result<Claims> {
    validate_token(token)?;
    let parts: Vec<_> = token.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(invalid());
    }
    // Metadata checks do not authenticate a JWT. The resource server verifies it.
    let claims: Claims =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).map_err(|_| invalid())?)
            .map_err(|_| invalid())?;
    let now = OffsetDateTime::now_utc().unix_timestamp();
    if claims.iss != config.issuer
        || claims.client_id != config.client_id
        || claims.org_id != config.organization_id
        || [&claims.sub, &claims.sid].iter().any(|s| {
            s.is_empty() || s.len() > 1024 || s.chars().any(|c| c.is_control() || c.is_whitespace())
        })
        || claims.iat > now + 60
        || claims.exp <= claims.iat
        || claims
            .exp
            .checked_sub(claims.iat)
            .is_none_or(|lifetime| lifetime > 900)
        || (!allow_expired && claims.exp <= now)
    {
        return Err(invalid());
    }
    Ok(claims)
}

fn key(saved: &Session) -> Result<String> {
    let tuple = (
        &saved.origin,
        "workos_authkit",
        &saved.config.issuer,
        &saved.config.client_id,
        &saved.config.organization_id,
        &saved.identity.principal_id,
        &saved.identity.subject,
    );
    let bytes = serde_json::to_vec(&tuple).map_err(|_| invalid())?;
    Ok(format!("credential:{:x}", Sha256::digest(bytes)))
}

fn parse(raw: &str, origin: &str) -> Result<Session> {
    if raw.len() > MAX_CACHE {
        return Err(invalid());
    }
    let saved: Session = serde_json::from_str(raw).map_err(|_| invalid())?;
    saved.config.validate()?;
    let decoded = claims(&saved.access_token, &saved.config, true)?;
    validate_token(&saved.refresh_token)?;
    let expiry = timestamp(&saved.expires_at)?;
    let refresh_expiry = timestamp(&saved.refresh_expires_at)?;
    let now = OffsetDateTime::now_utc();
    if saved.version != 1
        || saved.origin != origin
        || canonical_origin(origin)? != origin
        || saved.identity.account_id != saved.config.organization_id
        || saved.identity.subject != decoded.sub
        || saved.identity.session_id != decoded.sid
        || saved.identity.principal_id.is_empty()
        || saved.identity.principal_id.len() > 1024
        || saved
            .identity
            .principal_id
            .chars()
            .any(|c| c.is_control() || c.is_whitespace())
        || expiry > OffsetDateTime::from_unix_timestamp(decoded.exp).map_err(|_| invalid())?
        || expiry > refresh_expiry
        || refresh_expiry > now + time::Duration::hours(8) + MAX_METADATA_CLOCK_SKEW
    {
        return Err(invalid());
    }
    Ok(saved)
}

pub(super) fn load(store: &dyn Store, origin: &str) -> Result<Option<Session>> {
    let Some(selected) = store.get(origin)? else {
        return Ok(None);
    };
    if !selected.starts_with("credential:") || selected.len() != "credential:".len() + 64 {
        return Err(invalid());
    }
    let raw = store.get(&selected)?.ok_or_else(invalid)?;
    let saved = parse(&raw, origin)?;
    if key(&saved)? != selected {
        return Err(invalid());
    }
    Ok(Some(saved))
}

pub(super) fn save(store: &dyn Store, saved: &Session) -> Result<()> {
    let value = serde_json::to_string(saved).map_err(|_| invalid())?;
    parse(&value, &saved.origin)?;
    let selected = key(saved)?;
    let previous = store.get(&saved.origin)?;
    // Both rotated secrets and the pending marker are one atomic keychain value.
    store.put(&selected, &value)?;
    if previous.as_deref() != Some(&selected) {
        if let Err(error) = store.put(&saved.origin, &selected) {
            let _ = store.remove(&selected);
            return Err(error);
        }
        if let Some(previous) = previous {
            store.remove(&previous)?;
        }
    }
    Ok(())
}

pub(super) async fn verify(
    origin: &str,
    config: &provider::Config,
    token: &str,
    expected_user: Option<&str>,
) -> Result<(Identity, String, Value)> {
    let decoded = claims(token, config, false)?;
    if expected_user.is_some_and(|id| id != decoded.sub) {
        return Err(invalid());
    }
    let body = Api::new(origin.into(), Some(token.into()))?
        .request(Method::GET, "/v1/auth/session", None, None)
        .await
        .map_err(|error| scrub(error, token))?;
    let data = &body["data"];
    let account = bounded_string(data, "account_id", 1024)?;
    let principal = bounded_string(data, "principal_id", 1024)?;
    let expires = bounded_string(data, "expires_at", 128)?;
    let expiry = timestamp(expires)?;
    if account != config.organization_id
        || data["kind"] != "human"
        || !data["scopes"].is_object()
        || expiry <= OffsetDateTime::now_utc()
        || expiry > OffsetDateTime::from_unix_timestamp(decoded.exp).map_err(|_| invalid())?
    {
        return Err(invalid());
    }
    let identity = Identity {
        principal_id: principal.into(),
        account_id: account.into(),
        subject: decoded.sub,
        session_id: decoded.sid,
    };
    // Provider/API extras never reach normal CLI output.
    let mut output = json!({"data":{"principal_id":principal,"account_id":account,"kind":"human","subject":identity.subject,"scopes":data["scopes"],"expires_at":expires},"meta":{"assurance":"verified_human"}});
    scrub_value(&mut output, token);
    Ok((identity, expires.into(), output))
}

pub(super) async fn accept(
    origin: &str,
    config: provider::Config,
    tokens: workos::AuthenticateResponse,
    previous: Option<&Session>,
) -> Result<(Session, Value)> {
    if tokens.organization_id.as_deref() != Some(&config.organization_id) {
        return Err(invalid());
    }
    let access = tokens.access_token.into_inner();
    let refresh = tokens.refresh_token.into_inner();
    validate_token(&refresh)?;
    let (identity, mut expires_at, mut output) =
        verify(origin, &config, &access, Some(&tokens.user.id))
            .await
            .map_err(|error| scrub(scrub(error, &access), &refresh))?;
    let refresh_expires_at = match previous {
        Some(previous) => {
            if previous.config != config
                || previous.identity != identity
                || previous.origin != origin
            {
                return Err(Failure::refused(
                    "session_identity_mismatch",
                    "renewal changed the selected identity or provider profile",
                ));
            }
            previous.refresh_expires_at.clone()
        }
        None => (OffsetDateTime::now_utc() + time::Duration::hours(8))
            .format(&Rfc3339)
            .map_err(|_| invalid())?,
    };
    if timestamp(&expires_at)? > timestamp(&refresh_expires_at)? {
        expires_at = refresh_expires_at.clone();
    }
    output["data"]["expires_at"] = json!(expires_at);
    output["data"]["refresh_expires_at"] = json!(refresh_expires_at);
    scrub_value(&mut output, &refresh);
    let saved = Session {
        version: 1,
        origin: origin.into(),
        config,
        identity,
        access_token: access,
        expires_at,
        refresh_token: refresh,
        refresh_expires_at,
        state: RefreshState::Ready,
    };
    Ok((saved, output))
}

fn unknown() -> Failure {
    Failure::refused(
        "refresh_outcome_unknown",
        "renewal may have completed; run login --api explicitly to recover",
    )
}

async fn refresh(
    store: &dyn Store,
    saved: &mut Session,
    provider: &provider::Provider,
) -> Result<()> {
    saved.state = RefreshState::Pending;
    save(store, saved)?;
    let outcome = tokio::time::timeout(Duration::from_secs(35), async {
        let tokens = provider
            .refresh(&saved.refresh_token, &saved.config)
            .await
            .map_err(|error| provider::failure(&error))?;
        accept(&saved.origin, saved.config.clone(), tokens, Some(saved)).await
    })
    .await;
    match outcome {
        Ok(Ok((next, _))) => {
            save(store, &next)?;
            *saved = next;
            Ok(())
        }
        Ok(Err(error)) if error.body["type"] == "login_required" => {
            saved.state = RefreshState::LoginRequired;
            save(store, saved)?;
            Err(error)
        }
        _ => Err(unknown()),
    }
}

pub(super) async fn credential(store: &dyn Store, origin: &str) -> Result<String> {
    let saved = load(store, origin)?.ok_or_else(login_required)?;
    let provider = provider::Provider::new(&saved.config)?;
    credential_with(store, origin, &provider).await
}

pub(super) async fn credential_with(
    store: &dyn Store,
    origin: &str,
    provider: &provider::Provider,
) -> Result<String> {
    let mut saved = load(store, origin)?.ok_or_else(login_required)?;
    let now = OffsetDateTime::now_utc();
    if saved.state == RefreshState::LoginRequired || timestamp(&saved.refresh_expires_at)? <= now {
        return Err(login_required());
    }
    if timestamp(&saved.expires_at)? > now + time::Duration::seconds(30) {
        return Ok(saved.access_token);
    }
    if saved.state == RefreshState::Pending {
        return if timestamp(&saved.expires_at)? > now {
            Ok(saved.access_token)
        } else {
            Err(unknown())
        };
    }
    match refresh(store, &mut saved, provider).await {
        Ok(()) => Ok(saved.access_token),
        Err(_)
            if saved.state != RefreshState::LoginRequired
                && timestamp(&saved.expires_at)? > OffsetDateTime::now_utc() =>
        {
            Ok(saved.access_token)
        }
        Err(error) => Err(error),
    }
}

async fn revoke(origin: &str, token: String) -> bool {
    let Ok(api) = Api::new(origin.into(), Some(token)) else {
        return false;
    };
    api.request(Method::POST, "/v1/auth/logout", None, None)
        .await
        .is_ok_and(|body| body["data"]["logged_out"] == true)
}

pub(super) async fn logout(store: &dyn Store, origin: &str) -> Result<Value> {
    let token = credential(store, origin).await.ok();
    let confirmed = if let Some(token) = token {
        revoke(origin, token).await
    } else {
        false
    };
    if let Some(selected) = store.get(origin)? {
        store.remove(origin)?;
        store.remove(&selected)?;
    }
    Ok(
        json!({"data":{"logged_out":true,"local_credentials_removed":true,"provider_revocation_confirmed":confirmed},"meta":{}}),
    )
}

#[cfg(test)]
mod tests;
