//! Provider credentials and their exact identity remain in the OS keychain.
use super::*;
use crate::managed::{MAX_BODY, ResponseLimits};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest as _, Sha256};

const MAX_CACHE: usize = 64 * 1024;
const HUMAN_SESSION_LIFETIME: time::Duration = time::Duration::days(30);
const REFRESH_BUDGET: Duration = Duration::from_secs(20);
const REFRESH_ATTEMPT: Duration = Duration::from_secs(6);
const VERIFY_BUDGET: Duration = Duration::from_secs(15);
const VERIFY_ATTEMPT: Duration = Duration::from_secs(4);
const RETRY_BACKOFF: [Duration; 2] = [Duration::from_millis(250), Duration::from_secs(1)];
/// The seconds a fresh device token must still have for its verification:
/// [`VERIFY_BUDGET`] and the 30 seconds a saved token is renewed ahead of.
const VERIFY_MARGIN: i64 = 45;

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
    pub(super) fn access_token(&self) -> &str {
        &self.access_token
    }

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

impl Claims {
    /// The organization the token names, if the sign-in selected one.
    pub(super) fn organization(&self) -> Option<&str> {
        self.org_id.as_deref()
    }
}

/// A session, its index or its metadata, as this machine saved it, fails a
/// local check. `detail` names the check; it is a fixed text, never derived
/// from the saved material.
fn invalid(detail: &'static str) -> Failure {
    Failure::refused("credential_invalid", detail)
}

/// What the identity provider just returned does not fit the sign-in
/// configuration the API publishes, or is malformed. Nothing saved is at
/// fault, and signing in again does not help while the two disagree.
fn provider_invalid(detail: &'static str) -> Failure {
    Failure::refused("provider_response_invalid", detail)
}

/// A sign-in that selected no organization cannot become a session: every
/// session, saved or verified, belongs to one.
pub(super) fn organization_required() -> Failure {
    Failure::refused(
        "organization_required",
        "this sign-in selected no organization; run login again with --organization ORG_ID",
    )
}

/// Where a token under check came from, which decides its refusal's code.
#[derive(Clone, Copy)]
pub(super) enum Source {
    /// Read from this machine's secure store.
    Saved,
    /// Just returned by the identity provider.
    Provider,
}

impl Source {
    fn refuse(self, detail: &'static str) -> Failure {
        match self {
            Self::Saved => invalid(detail),
            Self::Provider => provider_invalid(detail),
        }
    }
}

pub(super) fn timestamp(value: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| invalid("a session time is not an RFC 3339 timestamp"))
}

fn claims(
    token: &str,
    config: &provider::Config,
    allow_expired: bool,
    source: Source,
) -> Result<Claims> {
    let claims = provider_claims(token, config, allow_expired, source)?;
    if claims.org_id.is_none() {
        return Err(source.refuse("the access token names no organization"));
    }
    Ok(claims)
}

// A fresh device session can be organizationless. Saved credentials and resource
// verification still use claims(), which requires an organization.
fn provider_claims(
    token: &str,
    config: &provider::Config,
    allow_expired: bool,
    source: Source,
) -> Result<Claims> {
    validate_token(token)
        .map_err(|_| source.refuse("the access token is empty, too long or not printable ASCII"))?;
    let parts: Vec<_> = token.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(source.refuse("the access token is not a three-part JWT"));
    }
    // Metadata checks do not authenticate a JWT. The resource server verifies it.
    let unreadable = || source.refuse("the access token's claims cannot be read");
    let claims: Claims =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).map_err(|_| unreadable())?)
            .map_err(|_| unreadable())?;
    let now = OffsetDateTime::now_utc().unix_timestamp();
    if claims.iss != config.issuer {
        return Err(source.refuse(
            "the access token's issuer is not the issuer the API's sign-in configuration names",
        ));
    }
    if claims.client_id != config.client_id {
        return Err(source.refuse(
            "the access token was issued to another client than the API's sign-in configuration names",
        ));
    }
    if claims
        .org_id
        .as_ref()
        .is_some_and(|org| !org.starts_with("org_") || super::super::organization_id(org).is_err())
    {
        return Err(source.refuse("the access token's organization ID is malformed"));
    }
    if [&claims.sub, &claims.sid].iter().any(|s| {
        s.is_empty() || s.len() > 1024 || s.chars().any(|c| c.is_control() || c.is_whitespace())
    }) {
        return Err(source.refuse(
            "the access token's subject or session ID is empty, too long or contains whitespace",
        ));
    }
    if claims.iat > now + 60 {
        return Err(source.refuse(
            "the access token was issued more than 60 seconds in the future; check this machine's clock",
        ));
    }
    if claims.exp <= claims.iat
        || claims
            .exp
            .checked_sub(claims.iat)
            .is_none_or(|lifetime| lifetime > 900)
    {
        return Err(source.refuse("the access token's lifetime is not between 1 and 900 seconds"));
    }
    if !allow_expired && claims.exp <= now {
        return Err(source.refuse("the access token has expired"));
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
        &saved.identity.session_id,
    );
    let bytes = serde_json::to_vec(&tuple)
        .map_err(|_| invalid("a session's storage key cannot be derived"))?;
    Ok(format!("credential:{:x}", Sha256::digest(bytes)))
}

fn parse(raw: &str, origin: &str) -> Result<Session> {
    if raw.len() > MAX_CACHE {
        return Err(invalid("a saved session is larger than 64 KiB"));
    }
    let saved: Session = serde_json::from_str(raw)
        .map_err(|_| invalid("a saved session is not in the format this CLI reads"))?;
    saved.config.validate()?;
    let decoded = claims(&saved.access_token, &saved.config, true, Source::Saved)?;
    validate_token(&saved.refresh_token)
        .map_err(|_| invalid("a saved refresh token is empty, too long or not printable ASCII"))?;
    let expiry = timestamp(&saved.expires_at)?;
    let refresh_expiry = timestamp(&saved.refresh_expires_at)?;
    let now = OffsetDateTime::now_utc();
    if saved.version != 2 {
        return Err(invalid("a saved session has another format version"));
    }
    if saved.origin != origin || canonical_origin(origin)? != origin {
        return Err(invalid("a saved session belongs to another API origin"));
    }
    if Some(saved.identity.account_id.as_str()) != decoded.org_id.as_deref() {
        return Err(invalid(
            "a saved session's organization differs from its access token's",
        ));
    }
    if saved.identity.subject != decoded.sub || saved.identity.session_id != decoded.sid {
        return Err(invalid(
            "a saved session's subject or session ID differs from its access token's",
        ));
    }
    if saved.identity.principal_id.is_empty()
        || saved.identity.principal_id.len() > 1024
        || saved
            .identity
            .principal_id
            .chars()
            .any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(invalid(
            "a saved session's principal ID is empty, too long or contains whitespace",
        ));
    }
    if expiry > token_expiry(&decoded)? || expiry > refresh_expiry {
        return Err(invalid(
            "a saved session expires later than its access token or its sign-in deadline",
        ));
    }
    if refresh_expiry > now + HUMAN_SESSION_LIFETIME + MAX_METADATA_CLOCK_SKEW {
        return Err(invalid(
            "a saved session's sign-in deadline is more than 30 days away; check this machine's clock",
        ));
    }
    Ok(saved)
}

fn token_expiry(claims: &Claims) -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(claims.exp)
        .map_err(|_| invalid("an access token's expiry is not a representable time"))
}

/// The saved index names a session the store does not hold.
fn missing() -> Failure {
    invalid("the saved session index names a session the secure store does not hold")
}

/// A saved session is not the one its index entry describes.
fn misfiled() -> Failure {
    invalid("a saved session is filed under another identity's key or organization")
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
        return Err(invalid("the saved session index is larger than 64 KiB"));
    }
    let index: Index = serde_json::from_str(&raw)
        .map_err(|_| invalid("the saved session index is not in the format this CLI reads"))?;
    if index.version != 2
        || index.sessions.len() > 128
        || index.sessions.iter().any(|(org, key)| {
            super::super::organization_id(org).is_err()
                || !key.starts_with("credential:")
                || key.len() != 75
                || !key[11..].bytes().all(|b| b.is_ascii_hexdigit())
        })
    {
        return Err(invalid(
            "the saved session index has another version or a malformed entry",
        ));
    }
    Ok(index)
}

/// Check the bounded cache without selecting an organization or renewing a session.
pub(super) fn validate_custody(store: &dyn Store, origin: &str) -> Result<Vec<Session>> {
    let index = index(store, origin)?;
    let mut sessions = Vec::new();
    for (organization, reference) in index.sessions {
        let raw = store.get(&reference)?.ok_or_else(missing)?;
        let saved = parse(&raw, origin)?;
        if key(&saved)? != reference || saved.identity.account_id != organization {
            return Err(misfiled());
        }
        sessions.push(saved);
    }
    Ok(sessions)
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
                "multiple organizations are signed in; select a target with an organization_id",
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
    let raw = store.get(selected)?.ok_or_else(missing)?;
    let saved = parse(&raw, origin)?;
    if key(&saved)? != *selected || saved.identity.account_id != org {
        return Err(misfiled());
    }
    Ok(Some(saved))
}

#[cfg(test)]
pub(super) fn load(store: &dyn Store, origin: &str) -> Result<Option<Session>> {
    load_selected(store, origin, None)
}

pub(super) fn save(store: &dyn Store, saved: &Session) -> Result<()> {
    let value = serde_json::to_string(saved)
        .map_err(|_| invalid("the session cannot be written in the saved format"))?;
    parse(&value, &saved.origin)?;
    let claim = session_claim(&saved.config, &saved.identity.session_id)?;
    let owner = crate::operator_routing::journal_owner().unwrap_or_else(|| "test-unbound".into());
    if store
        .session_owner(&claim)?
        .is_some_and(|previous| previous != owner)
    {
        return Err(Failure::refused(
            "session_identity_mismatch",
            "provider session belongs to another owner/profile",
        ));
    }
    store.claim_session(&claim, &owner)?;
    let selected = key(saved)?;
    let mut index = index(store, &saved.origin)?;
    let previous = index
        .sessions
        .insert(saved.identity.account_id.clone(), selected.clone());
    if index.sessions.len() > 128 {
        return Err(invalid(
            "this profile already holds sessions for 128 organizations",
        ));
    }
    // Rotation and its pending marker remain one atomic credential value. The
    // index never chooses a most-recent identity, nor copies refresh secrets.
    store.put(&selected, &value)?;
    if previous.as_ref() != Some(&selected) {
        let encoded = serde_json::to_string(&index).map_err(|_| unwritable_index())?;
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

/// The claims of a device sign-in's fresh tokens, checked against the
/// sign-in configuration and the provider's own response. Reads no saved
/// state, so it says nothing yet about whether the session is new.
pub(super) fn device_claims(
    config: &provider::Config,
    tokens: &workos::AuthenticateResponse,
) -> Result<Claims> {
    let initial = provider_claims(
        tokens.access_token.as_ref(),
        config,
        false,
        Source::Provider,
    )?;
    validate_token(tokens.refresh_token.as_ref()).map_err(|_| {
        provider_invalid("the provider's refresh token is empty, too long or not printable ASCII")
    })?;
    if tokens.user.id != initial.sub || tokens.organization_id != initial.org_id {
        return Err(provider_invalid(
            "the provider's response names another user or organization than its access token",
        ));
    }
    Ok(initial)
}

pub(super) fn check_device_session(
    store: &dyn Store,
    origin: &str,
    config: &provider::Config,
    tokens: &workos::AuthenticateResponse,
) -> Result<Claims> {
    let initial = device_claims(config, tokens)?;
    // A provider response must be a new session before it can be selected or
    // revoked. Existing token pairs stay untouched under the caller's origin lock.
    for (org, selected) in index(store, origin)?.sessions {
        let raw = store.get(&selected)?.ok_or_else(missing)?;
        let cached = parse(&raw, origin)?;
        if key(&cached)? != selected || cached.identity.account_id != org {
            return Err(misfiled());
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
    if store
        .session_owner(&session_claim(config, &initial.sid)?)?
        .is_some()
    {
        return Err(Failure::refused(
            "session_identity_mismatch",
            "device sign-in reused a previously claimed provider session; start a fresh sign-in",
        ));
    }
    Ok(initial)
}

fn session_claim(config: &provider::Config, sid: &str) -> Result<String> {
    let bytes = serde_json::to_vec(&(&config.issuer, &config.client_id, sid))
        .map_err(|_| invalid("a provider session's claim key cannot be derived"))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(super) fn check_replacement(
    store: &dyn Store,
    saved: &Session,
    confirmed: Option<&str>,
) -> Result<()> {
    if confirmed.is_some_and(|principal| principal != saved.identity.principal_id) {
        return Err(Failure::refused(
            "principal_replacement_mismatch",
            "verified new principal does not match --replace-principal",
        ));
    }
    if let Some(previous) = load_selected(store, &saved.origin, Some(&saved.identity.account_id))? {
        if previous.identity.principal_id != saved.identity.principal_id
            && confirmed != Some(saved.identity.principal_id.as_str())
        {
            return Err(Failure::refused(
                "principal_replacement_required",
                "profile belongs to another principal; confirm the new verified principal with login --replace-principal PRINCIPAL_ID",
            ));
        }
    }
    Ok(())
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
    // A token about to expire cannot outlast its verification, which happens
    // when a person took their time choosing: it is renewed for the same
    // organization, as one for another organization is exchanged.
    if initial.org_id.as_deref() == Some(organization)
        && initial.exp > OffsetDateTime::now_utc().unix_timestamp() + VERIFY_MARGIN
    {
        return Ok(tokens);
    }
    let selected = provider
        .refresh(tokens.refresh_token.as_ref(), organization)
        .await
        .map_err(|error| provider::failure(&error))?;
    let claims = claims(
        selected.access_token.as_ref(),
        config,
        false,
        Source::Provider,
    )?;
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
    source: Source,
) -> Result<(Identity, String, Value)> {
    let decoded = claims(token, config, false, source)?;
    if expected_user.is_some_and(|id| id != decoded.sub) {
        return Err(
            source.refuse("the access token names another user than the provider's response")
        );
    }
    let body = verify_session_response(&Api::new(origin.into(), Some(token.into()))?)
        .await
        .map_err(|error| scrub(error, token))?;
    let data = &body["data"];
    let account = bounded_string(data, "account_id", 1024)?;
    let principal = bounded_string(data, "principal_id", 1024)?;
    let expires = bounded_string(data, "expires_at", 128)?;
    let expiry = timestamp(expires)?;
    // What the API verified must be the session the token describes: a
    // person's, in the token's organization, for no longer than the token.
    let unlisted = || invalid("the API's verified session does not list valid service permissions");
    let permissions: Vec<String> =
        serde_json::from_value(data["service_permissions"].clone()).map_err(|_| unlisted())?;
    super::super::validate_service_permissions(&permissions).map_err(|_| unlisted())?;
    if Some(account) != decoded.org_id.as_deref() {
        return Err(invalid(
            "the API verified the session in another organization than its access token names",
        ));
    }
    if data["kind"] != "human" {
        return Err(invalid(
            "the API verified the session as another kind than a person's sign-in",
        ));
    }
    if expiry <= OffsetDateTime::now_utc() || expiry > token_expiry(&decoded)? {
        return Err(invalid(
            "the API's verified session has expired or outlasts its access token",
        ));
    }
    let identity = Identity {
        principal_id: principal.into(),
        account_id: account.into(),
        subject: decoded.sub,
        session_id: decoded.sid,
    };
    // Provider/API extras never reach normal CLI output.
    let mut output = json!({"data":{"principal_id":principal,"account_id":account,"kind":"human","subject":identity.subject,"service_permissions":permissions,"expires_at":expires},"meta":{"assurance":"verified_human"}});
    scrub_value(&mut output, token);
    Ok((identity, expires.into(), output))
}

pub(super) async fn accept(
    origin: &str,
    config: provider::Config,
    tokens: workos::AuthenticateResponse,
    previous: Option<&Session>,
) -> Result<(Session, Value)> {
    // The provider's tokens are fresh here, from a device sign-in or a
    // renewal: a defect in them is the provider's, not a saved session's.
    let organization = tokens
        .organization_id
        .clone()
        .ok_or_else(organization_required)?;
    let access = tokens.access_token.into_inner();
    let refresh = tokens.refresh_token.into_inner();
    validate_token(&refresh).map_err(|_| {
        provider_invalid("the provider's refresh token is empty, too long or not printable ASCII")
    })?;
    let (identity, mut expires_at, mut output) = verify(
        origin,
        &config,
        &access,
        Some(&tokens.user.id),
        Source::Provider,
    )
    .await
    .map_err(|error| scrub(scrub(error, &access), &refresh))?;
    if identity.account_id != organization {
        return Err(provider_invalid(
            "the provider's response names another organization than its access token",
        ));
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
        None => (OffsetDateTime::now_utc() + HUMAN_SESSION_LIFETIME)
            .format(&Rfc3339)
            .map_err(|_| invalid("the sign-in deadline is not a representable time"))?,
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
        "renewal may have completed; run login --target NAME explicitly to recover",
    )
}

fn retry_delay(
    retry_after: &[reqwest::header::HeaderValue],
    mut delay: Duration,
) -> Option<Duration> {
    match retry_after {
        [] => {}
        [value] => {
            let Ok(value) = value.to_str() else {
                return None;
            };
            let provider_delay = if let Ok(seconds) = value.trim().parse::<u64>() {
                Duration::from_secs(seconds)
            } else if let Ok(date) =
                OffsetDateTime::parse(value.trim(), &time::format_description::well_known::Rfc2822)
            {
                (date - OffsetDateTime::now_utc())
                    .max(time::Duration::ZERO)
                    .unsigned_abs()
            } else {
                return None;
            };
            delay = delay.max(provider_delay);
        }
        _ => return None,
    }
    Some(delay)
}

async fn retry_pause(
    attempt: usize,
    retry_after: &[reqwest::header::HeaderValue],
    deadline: Instant,
    next_attempt: Duration,
) -> bool {
    let Some(delay) = RETRY_BACKOFF
        .get(attempt)
        .and_then(|delay| retry_delay(retry_after, *delay))
    else {
        return false;
    };
    if Instant::now()
        .checked_add(delay)
        .and_then(|next| next.checked_add(next_attempt))
        .is_none_or(|next| next > deadline)
    {
        return false;
    }
    tokio::time::sleep(delay).await;
    true
}

// AuthKit's replay grace applies only to this exact token/organization pair in
// this invocation. Keep the whole loop comfortably inside its 30-second window.
async fn refresh_tokens(
    provider: &provider::Provider,
    saved: &Session,
) -> Result<workos::AuthenticateResponse> {
    let deadline = Instant::now() + REFRESH_BUDGET;
    for attempt in 0..3 {
        if Instant::now() + REFRESH_ATTEMPT > deadline {
            return Err(unknown());
        }
        let outcome = tokio::time::timeout(
            REFRESH_ATTEMPT,
            provider.refresh(&saved.refresh_token, &saved.identity.account_id),
        )
        .await;
        let headers = match outcome {
            Ok(Ok(tokens)) => return Ok(tokens),
            Ok(Err(error)) if provider::transient(&error) => error
                .api()
                .map(|error| {
                    error
                        .headers
                        .get_all(reqwest::header::RETRY_AFTER)
                        .iter()
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
            Ok(Err(error)) => return Err(provider::failure(&error)),
            Err(_) => Vec::new(),
        };
        if !retry_pause(attempt, &headers, deadline, REFRESH_ATTEMPT).await {
            return Err(unknown());
        }
    }
    Err(unknown())
}

// A successful rotation must never be repeated to recover a failed read. Reuse
// the returned bearer for bounded verification; only accept() installs the pair.
async fn verify_session_response(api: &Api) -> Result<Value> {
    let deadline = Instant::now() + VERIFY_BUDGET;
    for attempt in 0..3 {
        if Instant::now() + VERIFY_ATTEMPT > deadline {
            return Err(unknown());
        }
        let outcome = tokio::time::timeout(
            VERIFY_ATTEMPT,
            api.raw_bounded(
                Method::GET,
                "/v1/auth/session",
                None,
                None,
                ResponseLimits {
                    timeout: VERIFY_ATTEMPT,
                    max_body: MAX_BODY,
                    preserve_contents: false,
                    auth_verification_status: true,
                },
            ),
        )
        .await;
        let (failure, headers, transient) = match outcome {
            Ok(Ok(response)) if response.status.is_success() => return Ok(response.body),
            Ok(Ok(response)) => {
                let transient = matches!(response.status.as_u16(), 408 | 429 | 500..=599);
                (
                    Failure {
                        body: response.body,
                        exit: if response.status.is_client_error() {
                            2
                        } else {
                            1
                        },
                        response_status: Some(response.status),
                    },
                    response.retry_after,
                    transient,
                )
            }
            Ok(Err(error)) => {
                let transient =
                    error.response_status.is_none() && error.body["type"] == "transport_failed";
                (error, Vec::new(), transient)
            }
            Err(_) => (unknown(), Vec::new(), true),
        };
        if !transient || !retry_pause(attempt, &headers, deadline, VERIFY_ATTEMPT).await {
            return Err(failure);
        }
    }
    Err(unknown())
}

async fn refresh(
    store: &dyn Store,
    saved: &mut Session,
    provider: &provider::Provider,
) -> Result<()> {
    saved.state = RefreshState::Pending;
    save(store, saved)?;
    let outcome = tokio::time::timeout(Duration::from_secs(35), async {
        let tokens = refresh_tokens(provider, saved).await?;
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
    Ok(credential_loaded(store, saved, &provider)
        .await?
        .access_token)
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
    Ok(credential_loaded(store, saved, provider)
        .await?
        .access_token)
}

#[cfg(test)]
pub(super) async fn credential_selected(
    store: &dyn Store,
    origin: &str,
    identity: &Identity,
    provider: &provider::Provider,
) -> Result<String> {
    let saved = selected_session(store, origin, identity)?;
    Ok(credential_loaded(store, saved, provider)
        .await?
        .access_token)
}

pub(super) async fn credential_loaded(
    store: &dyn Store,
    mut saved: Session,
    provider: &provider::Provider,
) -> Result<Session> {
    let now = OffsetDateTime::now_utc();
    if saved.state == RefreshState::LoginRequired || timestamp(&saved.refresh_expires_at)? <= now {
        return Err(login_required());
    }
    if timestamp(&saved.expires_at)? > now + time::Duration::seconds(30) {
        return Ok(saved);
    }
    if saved.state == RefreshState::Pending {
        return if timestamp(&saved.expires_at)? > now {
            Ok(saved)
        } else {
            Err(unknown())
        };
    }
    match refresh(store, &mut saved, provider).await {
        Ok(()) => Ok(saved),
        Err(error)
            if error.body["type"] == "refresh_outcome_unknown"
                && saved.state != RefreshState::LoginRequired
                && timestamp(&saved.expires_at)? > OffsetDateTime::now_utc() =>
        {
            Ok(saved)
        }
        Err(error) => Err(error),
    }
}

/// The saved index, changed in memory, cannot be written back.
fn unwritable_index() -> Failure {
    invalid("the session index cannot be written in the saved format")
}

pub(super) async fn revoke(origin: &str, token: String) -> bool {
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
    if let Some(org) = org {
        let mut index = index(store, origin)?;
        if let Some(selected) = index.sessions.remove(org) {
            if index.sessions.is_empty() {
                store.remove(origin)?;
            } else {
                store.put(
                    origin,
                    &serde_json::to_string(&index).map_err(|_| unwritable_index())?,
                )?;
            }
            store.remove(&selected)?;
        }
    }
    let confirmed = if let Some(saved) = &saved {
        revoke(origin, saved.access_token.clone()).await
    } else {
        false
    };
    Ok(
        json!({"data":{"logged_out":true,"organization_id":org,"local_credentials_removed":true,"provider_revocation_confirmed":confirmed},"meta":{}}),
    )
}

#[cfg(test)]
mod tests;
