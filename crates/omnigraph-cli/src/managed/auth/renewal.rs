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
    pub account_id: String,
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
pub(super) struct Claims {
    iss: String,
    sub: String,
    sid: String,
    org_id: Option<String>,
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
    let claims = provider_claims(token, config, allow_expired)?;
    claims.org_id.as_ref().ok_or_else(invalid)?;
    Ok(claims)
}

// A fresh device session can be organizationless. Saved credentials and resource
// verification still use claims(), which requires an organization.
fn provider_claims(token: &str, config: &provider::Config, allow_expired: bool) -> Result<Claims> {
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
        || claims.org_id.as_ref().is_some_and(|org| {
            !org.starts_with("org_") || super::super::organization_id(org).is_err()
        })
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
        &saved.identity.account_id,
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
    if saved.version != 2
        || saved.origin != origin
        || canonical_origin(origin)? != origin
        || Some(saved.identity.account_id.as_str()) != decoded.org_id.as_deref()
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

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Index {
    version: u8,
    sessions: std::collections::BTreeMap<String, String>,
}

fn index(store: &dyn Store, origin: &str) -> Result<Index> {
    let Some(raw) = store.get(origin)? else {
        return Ok(Index {
            version: 2,
            sessions: Default::default(),
        });
    };
    if raw.len() > MAX_CACHE {
        return Err(invalid());
    }
    let index: Index = serde_json::from_str(&raw).map_err(|_| invalid())?;
    if index.version != 2
        || index.sessions.len() > 128
        || index.sessions.iter().any(|(org, key)| {
            super::super::organization_id(org).is_err()
                || !key.starts_with("credential:")
                || key.len() != 75
                || !key[11..].bytes().all(|b| b.is_ascii_hexdigit())
        })
    {
        return Err(invalid());
    }
    Ok(index)
}

pub(super) fn load_selected(
    store: &dyn Store,
    origin: &str,
    organization: Option<&str>,
) -> Result<Option<Session>> {
    if let Some(org) = organization {
        super::super::organization_id(org)?;
    }
    let index = index(store, origin)?;
    let selected = match organization {
        Some(org) => index.sessions.get(org).map(|key| (org, key)),
        None if index.sessions.len() > 1 => {
            return Err(Failure::refused(
                "organization_required",
                "multiple organizations are signed in; select --organization or a bound folder",
            ));
        }
        None => index
            .sessions
            .iter()
            .next()
            .map(|(org, key)| (org.as_str(), key)),
    };
    let Some((org, selected)) = selected else {
        return Ok(None);
    };
    let raw = store.get(selected)?.ok_or_else(invalid)?;
    let saved = parse(&raw, origin)?;
    if key(&saved)? != *selected || saved.identity.account_id != org {
        return Err(invalid());
    }
    Ok(Some(saved))
}

#[cfg(test)]
pub(super) fn load(store: &dyn Store, origin: &str) -> Result<Option<Session>> {
    load_selected(store, origin, None)
}

pub(super) fn save(store: &dyn Store, saved: &Session) -> Result<()> {
    let value = serde_json::to_string(saved).map_err(|_| invalid())?;
    parse(&value, &saved.origin)?;
    let selected = key(saved)?;
    let mut index = index(store, &saved.origin)?;
    let previous = index
        .sessions
        .insert(saved.identity.account_id.clone(), selected.clone());
    if index.sessions.len() > 128 {
        return Err(invalid());
    }
    // Rotation and its pending marker remain one atomic credential value. The
    // index never chooses a most-recent identity, nor copies refresh secrets.
    store.put(&selected, &value)?;
    if previous.as_ref() != Some(&selected) {
        let encoded = serde_json::to_string(&index).map_err(|_| invalid())?;
        if let Err(error) = store.put(&saved.origin, &encoded) {
            let _ = store.remove(&selected);
            return Err(error);
        }
        if let Some(previous) = previous {
            store.remove(&previous)?;
        }
    }
    Ok(())
}

pub(super) fn check_device_session(
    store: &dyn Store,
    origin: &str,
    config: &provider::Config,
    tokens: &workos::AuthenticateResponse,
) -> Result<Claims> {
    let initial = provider_claims(tokens.access_token.as_ref(), config, false)?;
    validate_token(tokens.refresh_token.as_ref())?;
    if tokens.user.id != initial.sub || tokens.organization_id != initial.org_id {
        return Err(invalid());
    }
    // A provider response must be a new session before it can be selected or
    // revoked. Existing token pairs stay untouched under the caller's origin lock.
    for (org, selected) in index(store, origin)?.sessions {
        let raw = store.get(&selected)?.ok_or_else(invalid)?;
        let cached = parse(&raw, origin)?;
        if key(&cached)? != selected || cached.identity.account_id != org {
            return Err(invalid());
        }
        if cached.config.issuer == config.issuer
            && cached.config.client_id == config.client_id
            && cached.identity.session_id == initial.sid
        {
            return Err(Failure::refused(
                "session_identity_mismatch",
                "device sign-in reused a cached session; start a fresh provider sign-in",
            ));
        }
    }
    Ok(initial)
}

pub(super) async fn select_device_organization(
    config: &provider::Config,
    provider: &provider::Provider,
    tokens: workos::AuthenticateResponse,
    initial: Claims,
    organization: Option<&str>,
) -> Result<workos::AuthenticateResponse> {
    let Some(organization) = organization else {
        return Ok(tokens);
    };
    super::super::organization_id(organization)?;
    if initial.org_id.as_deref() == Some(organization) {
        return Ok(tokens);
    }
    let selected = provider
        .refresh(tokens.refresh_token.as_ref(), organization)
        .await
        .map_err(|error| provider::failure(&error))?;
    let claims = claims(selected.access_token.as_ref(), config, false)?;
    if selected.user.id != initial.sub
        || claims.sub != initial.sub
        || claims.sid != initial.sid
        || selected.organization_id != claims.org_id
    {
        return Err(Failure::refused(
            "session_identity_mismatch",
            "organization selection changed the device sign-in identity",
        ));
    }
    super::check_account(Some(organization), claims.org_id.as_deref().unwrap())?;
    Ok(selected)
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
    if Some(account) != decoded.org_id.as_deref()
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
    let organization = tokens.organization_id.clone().ok_or_else(invalid)?;
    let access = tokens.access_token.into_inner();
    let refresh = tokens.refresh_token.into_inner();
    validate_token(&refresh)?;
    let (identity, mut expires_at, mut output) =
        verify(origin, &config, &access, Some(&tokens.user.id))
            .await
            .map_err(|error| scrub(scrub(error, &access), &refresh))?;
    if identity.account_id != organization {
        return Err(invalid());
    }
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
        version: 2,
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

pub(super) fn unknown() -> Failure {
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
            .refresh(&saved.refresh_token, &saved.identity.account_id)
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

pub(super) async fn credential(
    store: &dyn Store,
    origin: &str,
    identity: &Identity,
) -> Result<String> {
    let saved = selected_session(store, origin, identity)?;
    let provider = provider::Provider::new(&saved.config)?;
    credential_selected(store, origin, identity, &provider).await
}

fn selected_session(store: &dyn Store, origin: &str, identity: &Identity) -> Result<Session> {
    let saved =
        load_selected(store, origin, Some(&identity.account_id))?.ok_or_else(login_required)?;
    if saved.identity != *identity {
        return Err(Failure::refused(
            "session_identity_mismatch",
            "the selected sign-in identity changed during this invocation",
        ));
    }
    Ok(saved)
}

#[cfg(test)]
pub(super) async fn credential_with(
    store: &dyn Store,
    origin: &str,
    provider: &provider::Provider,
) -> Result<String> {
    let saved = load_selected(store, origin, None)?.ok_or_else(login_required)?;
    credential_selected(store, origin, &saved.identity, provider).await
}

pub(super) async fn credential_selected(
    store: &dyn Store,
    origin: &str,
    identity: &Identity,
    provider: &provider::Provider,
) -> Result<String> {
    let mut saved = selected_session(store, origin, identity)?;
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

pub(super) async fn logout(
    store: &dyn Store,
    origin: &str,
    organization: Option<&str>,
) -> Result<Value> {
    let saved = load_selected(store, origin, organization)?;
    let org = saved
        .as_ref()
        .map(|s| s.identity.account_id.as_str())
        .or(organization);
    let confirmed = if let Some(saved) = &saved {
        match credential(store, origin, &saved.identity).await {
            Ok(token) => revoke(origin, token).await,
            Err(_) => false,
        }
    } else {
        false
    };
    if let Some(org) = org {
        let mut index = index(store, origin)?;
        if let Some(selected) = index.sessions.remove(org) {
            if index.sessions.is_empty() {
                store.remove(origin)?;
            } else {
                store.put(
                    origin,
                    &serde_json::to_string(&index).map_err(|_| invalid())?,
                )?;
            }
            store.remove(&selected)?;
        }
    }
    Ok(
        json!({"data":{"logged_out":true,"organization_id":org,"local_credentials_removed":true,"provider_revocation_confirmed":confirmed},"meta":{}}),
    )
}

#[cfg(test)]
mod tests;
