//! Managed human credentials never share the legacy named-server token file.
use super::{Api, Failure, Method, Result, Value, canonical_origin, json, write_diagnostic};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::Instant;

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
use crate::secure_store_error::{Operation, StoreFailure};

mod coordination;
mod provider;
mod renewal;

pub(crate) use coordination::LockGuard;

const MAX_SECRET: usize = 16 * 1024;
// Metadata from another clock may be slightly ahead. Actual expiry never has grace.
const MAX_METADATA_CLOCK_SKEW: time::Duration = time::Duration::seconds(30);

pub(super) trait Store {
    fn cache_without_control(&self) -> Result<bool> {
        Ok(false)
    }
    fn persistent(&self) -> bool {
        true
    }
    fn get(&self, origin: &str) -> Result<Option<String>>;
    fn put(&self, origin: &str, value: &str) -> Result<()>;
    fn remove(&self, origin: &str) -> Result<()>;
    fn session_owner(&self, session: &str) -> Result<Option<String>> {
        self.get(&format!("session-owner:{session}"))
    }
    fn claim_session(&self, session: &str, owner: &str) -> Result<()> {
        self.put(&format!("session-owner:{session}"), owner)
    }
}

pub(super) struct OsStore {
    service: &'static str,
}

impl OsStore {
    fn account(&self, key: &str) -> String {
        if self.service == SESSION_CLAIMS.service {
            key.to_owned()
        } else if self.service == DATA_STORE.service {
            crate::operator_routing::data_namespace(key)
        } else {
            crate::operator_routing::namespace(key)
        }
    }
}

const AUTHKIT_STORE: OsStore = OsStore {
    service: "omnigraph.workos-authkit.session.v2",
};
const SESSION_CLAIMS: OsStore = OsStore {
    service: "omnigraph.workos-authkit.session-owners.v1",
};
struct RawOsStore<'a>(&'a OsStore);

impl Store for OsStore {
    fn session_owner(&self, session: &str) -> Result<Option<String>> {
        SESSION_CLAIMS.get(session)
    }
    fn claim_session(&self, session: &str, owner: &str) -> Result<()> {
        SESSION_CLAIMS.put(session, owner)
    }
    fn get(&self, key: &str) -> Result<Option<String>> {
        RawOsStore(self).get(&self.account(key))
    }
    fn put(&self, key: &str, value: &str) -> Result<()> {
        let raw = RawOsStore(self);
        if self.service == DATA_STORE.service {
            let _lock = coordination::cache_index_lock()?;
            put_data(&raw, &self.account(key), value)
        } else {
            raw.put(&self.account(key), value)
        }
    }
    fn remove(&self, key: &str) -> Result<()> {
        RawOsStore(self).remove(&self.account(key))
    }
}

fn put_data(raw: &dyn Store, account: &str, value: &str) -> Result<()> {
    let auth = crate::operator_routing::AUTH
        .try_with(Clone::clone)
        .ok()
        .flatten()
        .ok_or_else(keychain_failed)?;
    auth.ensure_current().map_err(|_| {
        Failure::refused(
            "credential_generation_changed",
            "selected credential was replaced or removed; retry with a fresh invocation",
        )
    })?;
    DataIndex::selected()?.put(raw, account, value)
}

struct DataIndex {
    profile: String,
}

impl DataIndex {
    fn selected() -> Result<Self> {
        Ok(Self {
            profile: crate::operator_routing::data_profile_namespace()
                .ok_or_else(keychain_failed)?,
        })
    }

    fn account(&self) -> String {
        format!("{}:data-cache-index", self.profile)
    }

    fn owns(&self, account: &str) -> bool {
        let Some(binding) = account.strip_prefix(&format!("{}:binding:", self.profile)) else {
            return false;
        };
        let Some((binding, rest)) = binding.split_once(":generation:") else {
            return false;
        };
        let Some((generation, key)) = rest.split_once(":credential:") else {
            return false;
        };
        binding.len() == 64
            && binding
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && generation
                .parse::<u64>()
                .is_ok_and(|value| value.to_string() == generation)
            && !key.is_empty()
            && account.len() <= 4096
            && !key.chars().any(char::is_control)
    }

    fn load(&self, raw: &dyn Store) -> Result<std::collections::BTreeSet<String>> {
        let keys: std::collections::BTreeSet<String> = raw
            .get(&self.account())?
            .map(|value| serde_json::from_str(&value).map_err(|_| keychain_failed()))
            .transpose()?
            .unwrap_or_default();
        if keys.len() > 4096 || keys.iter().any(|key| !self.owns(key)) {
            return Err(keychain_failed());
        }
        Ok(keys)
    }

    fn put(&self, raw: &dyn Store, account: &str, value: &str) -> Result<()> {
        if !self.owns(account) {
            return Err(keychain_failed());
        }
        let mut keys = self.load(raw)?;
        keys.insert(account.to_owned());
        if keys.len() > 4096 {
            return Err(keychain_failed());
        }
        raw.put(
            &self.account(),
            &serde_json::to_string(&keys).map_err(|_| keychain_failed())?,
        )?;
        raw.put(account, value)
    }

    fn clear(&self, raw: &dyn Store) -> Result<()> {
        for key in self.load(raw)? {
            raw.remove(&key)?;
        }
        raw.remove(&self.account())
    }
}
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
fn keyring_failed(operation: Operation, error: keyring::Error) -> Failure {
    let diagnostic = StoreFailure::from_keyring(operation, &error);
    let mut failure = Failure::refused("keychain_unavailable", &diagnostic.to_string());
    failure.body["secure_store"] = json!(diagnostic);
    failure
}

#[cfg(all(
    test,
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "windows",
        target_os = "linux",
        target_os = "freebsd",
        target_os = "openbsd"
    )
))]
#[test]
fn keyring_failure_preserves_refusal_type_and_redacts_backend_payload() {
    let failure = keyring_failed(
        Operation::Read,
        keyring::Error::BadEncoding(b"synthetic-secret".to_vec()),
    );
    assert_eq!(failure.body["type"], "keychain_unavailable");
    assert_eq!(failure.body["secure_store"]["operation"], "read");
    assert_eq!(failure.body["secure_store"]["category"], "invalid_encoding");
    assert!(!failure.body.to_string().contains("synthetic-secret"));
}

#[cfg(all(test, target_os = "macos"))]
#[test]
fn keyring_failure_exposes_native_status_without_live_keychain_access() {
    let error = keyring::Error::PlatformFailure(Box::new(
        security_framework::base::Error::from_code(-25293),
    ));
    let failure = keyring_failed(Operation::Write, error);
    assert_eq!(failure.body["type"], "keychain_unavailable");
    assert_eq!(failure.body["secure_store"]["os_status"], -25293);
    assert_eq!(failure.body["secure_store"]["symbol"], "errSecAuthFailed");
    assert_eq!(failure.body["secure_store"]["operation"], "write");
}

#[cfg(target_os = "macos")]
use crate::macos_secure_store::Entry as RawStoreEntry;
#[cfg(any(
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
use keyring::Entry as RawStoreEntry;

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
impl Store for RawOsStore<'_> {
    fn get(&self, origin: &str) -> Result<Option<String>> {
        let entry = RawStoreEntry::new(self.0.service, origin)
            .map_err(|error| keyring_failed(Operation::Open, error))?;
        match entry.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(keyring_failed(Operation::Read, error)),
        }
    }
    fn put(&self, origin: &str, value: &str) -> Result<()> {
        RawStoreEntry::new(self.0.service, origin)
            .map_err(|error| keyring_failed(Operation::Open, error))?
            .set_password(value)
            .map_err(|error| keyring_failed(Operation::Write, error))
    }
    fn remove(&self, origin: &str) -> Result<()> {
        let entry = RawStoreEntry::new(self.0.service, origin)
            .map_err(|error| keyring_failed(Operation::Open, error))?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(keyring_failed(Operation::Delete, error)),
        }
    }
}

pub(super) fn clear_data() -> Result<()> {
    let _lock = coordination::cache_index_lock()?;
    DataIndex::selected()?.clear(&RawOsStore(&DATA_STORE))
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
impl Store for RawOsStore<'_> {
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
        "sign in with login --profile NAME before continuing",
    )
}

fn automation(origin: &str, token: Option<String>, api: Option<String>) -> Result<Option<String>> {
    match (token, api) {
        (None, None) => Ok(None),
        (Some(token), Some(api)) => {
            if canonical_origin(&api)? != origin {
                return Err(Failure::refused(
                    "credential_origin_mismatch",
                    "the declared credential owner does not match the selected API route",
                ));
            }
            validate_token(&token)?;
            Ok(Some(token))
        }
        _ => Err(Failure::refused(
            "credential_origin_required",
            "a control credential requires its explicit owner route",
        )),
    }
}

enum Source {
    Human(renewal::Identity),
    Automation(crate::credential_sources::AcquiredCredential),
}

pub(super) fn acquired_bearer<'a>(
    credential: &'a crate::credential_sources::AcquiredCredential,
    origin: &str,
) -> Result<&'a str> {
    let request = credential.request();
    if request.purpose() != crate::credential_sources::Purpose::Control
        || canonical_origin(request.endpoint())? != canonical_origin(origin)?
    {
        return Err(Failure::refused(
            "credential_origin_mismatch",
            "the captured credential belongs to another control destination",
        ));
    }
    credential
        .bearer_for(request, OffsetDateTime::now_utc())
        .map(|secret| secret.expose())
        .map_err(|error| match error {
            crate::credential_sources::CredentialError::Expired => Failure::refused(
                "credential_expired",
                "the captured control credential has expired; no request was sent",
            ),
            _ => Failure::refused(
                "credential_invalid",
                "the captured control credential is invalid",
            ),
        })
}

pub(super) struct Selection {
    pub account_id: String,
    pub principal_id: String,
    source: Source,
}

fn check_account(expected: Option<&str>, actual: &str) -> Result<()> {
    super::organization_id(actual)?;
    if expected.is_some_and(|org| org != actual) {
        return Err(Failure::refused(
            "organization_mismatch",
            "the verified account differs from the selected organization",
        ));
    }
    Ok(())
}

async fn select_with(
    store: &dyn Store,
    origin: &str,
    organization: Option<&str>,
    credential: Option<crate::credential_sources::AcquiredCredential>,
    api: Option<String>,
    mechanism: super::ControlAuth,
) -> Result<Option<Selection>> {
    if let Some(org) = organization {
        super::organization_id(org)?;
    }
    if mechanism == super::ControlAuth::AutomationEnv {
        let token = credential
            .as_ref()
            .map(|credential| acquired_bearer(credential, origin).map(str::to_owned))
            .transpose()?;
        let token = automation(origin, token, api)?.ok_or_else(|| {
            Failure::refused(
                "automation_credential_required",
                "automation targets require OMNIGRAPH_CONTROL_TOKEN and OMNIGRAPH_CONTROL_API",
            )
        })?;
        let credential = credential.ok_or_else(Failure::protocol)?;
        let body = Api::with_credential(origin.into(), credential.clone())?
            .request(Method::GET, "/v1/auth/session", None, None)
            .await
            .map_err(|failure| scrub(failure, &token))?;
        let account = bounded_string(&body["data"], "account_id", 128)?;
        check_account(organization, account)?;
        return Ok(Some(Selection {
            account_id: account.into(),
            principal_id: bounded_string(&body["data"], "principal_id", 1024)?.into(),
            source: Source::Automation(credential),
        }));
    }
    Ok(
        renewal::load_selected(store, origin, organization)?.map(|saved| Selection {
            account_id: saved.identity.account_id.clone(),
            principal_id: saved.identity.principal_id.clone(),
            source: Source::Human(saved.identity),
        }),
    )
}

async fn configured_token(
    origin: &str,
) -> Result<Option<crate::credential_sources::AcquiredCredential>> {
    let auth = crate::operator_routing::AUTH
        .try_with(Clone::clone)
        .ok()
        .flatten()
        .ok_or_else(|| {
            Failure::refused("credential_binding_required", "select a configured profile")
        })?;
    if canonical_origin(auth.owner.route())? != canonical_origin(origin)? {
        return Err(Failure::refused(
            "credential_origin_mismatch",
            "selected owner belongs to another service route",
        ));
    }
    auth.acquire_credential().await.map_err(|error| {
        if error.downcast_ref::<crate::credential_sources::CredentialError>()
            == Some(&crate::credential_sources::CredentialError::Expired)
        {
            return Failure::refused(
                "credential_expired",
                "the captured control credential has expired; no request was sent",
            );
        }
        Failure::refused(
            "credential_unavailable",
            "selected credential source failed; no fallback was attempted",
        )
    })
}

pub(super) async fn select(
    origin: &str,
    organization: Option<&str>,
    mechanism: super::ControlAuth,
) -> Result<Selection> {
    let _lock = if mechanism == super::ControlAuth::HumanDevice {
        Some(coordination::lock(origin).await?)
    } else {
        None
    };
    if mechanism == super::ControlAuth::HumanDevice {
        return select_human_with(&AUTHKIT_STORE, origin, organization).await;
    }
    let selected = select_with(
        &AUTHKIT_STORE,
        origin,
        organization,
        if mechanism == super::ControlAuth::AutomationEnv {
            configured_token(origin).await?
        } else {
            None
        },
        if mechanism == super::ControlAuth::AutomationEnv {
            Some(origin.to_owned())
        } else {
            None
        },
        mechanism,
    )
    .await?
    .ok_or_else(login_required)?;
    Ok(selected)
}

async fn select_human_with(
    store: &dyn Store,
    origin: &str,
    organization: Option<&str>,
) -> Result<Selection> {
    let saved = renewal::load_selected(store, origin, organization)?.ok_or_else(login_required)?;
    let provider = provider::Provider::new(&saved.config)?;
    let saved = renewal::credential_loaded(store, saved, &provider).await?;
    let (verified, _, _) =
        renewal::verify(origin, &saved.config, saved.access_token(), None).await?;
    if verified != saved.identity {
        return Err(Failure::refused(
            "session_identity_mismatch",
            "the verified sign-in identity changed",
        ));
    }
    Ok(Selection {
        account_id: saved.identity.account_id.clone(),
        principal_id: saved.identity.principal_id.clone(),
        source: Source::Human(saved.identity),
    })
}

impl Selection {
    pub(super) async fn credential(&self, origin: &str) -> Result<String> {
        match &self.source {
            Source::Automation(credential) => {
                acquired_bearer(credential, origin).map(str::to_owned)
            }
            Source::Human(identity) => {
                let _lock = coordination::lock(origin).await?;
                renewal::credential(&AUTHKIT_STORE, origin, identity).await
            }
        }
    }
}

pub(super) async fn cache_lock(key: &str) -> Result<LockGuard> {
    coordination::lock(key).await
}

pub(super) fn local_status(_origin: &str, _organization: Option<&str>) -> Value {
    json!({"availability":"unknown","identity":"unknown","reason":"noninteractive secure-store inspection is unavailable"})
}

async fn selected_principal_with(
    store: &dyn Store,
    origin: &str,
    organization: Option<&str>,
    credential: Option<crate::credential_sources::AcquiredCredential>,
    api: Option<String>,
    mechanism: super::ControlAuth,
) -> Result<Option<String>> {
    // A valid data credential keeps its independent lifetime after control
    // credentials are removed. Absence cannot fall back to human custody.
    if mechanism == super::ControlAuth::AutomationEnv && credential.is_none() && api.is_none() {
        return Ok(None);
    }
    Ok(
        select_with(store, origin, organization, credential, api, mechanism)
            .await?
            .map(|s| s.principal_id),
    )
}

pub(super) async fn selected_principal(
    origin: &str,
    organization: &str,
    mechanism: super::ControlAuth,
) -> Result<Option<String>> {
    let _lock = if mechanism == super::ControlAuth::HumanDevice {
        Some(coordination::lock(origin).await?)
    } else {
        None
    };
    selected_principal_with(
        &AUTHKIT_STORE,
        origin,
        Some(organization),
        if mechanism == super::ControlAuth::AutomationEnv {
            configured_token(origin).await?
        } else {
            None
        },
        if mechanism == super::ControlAuth::AutomationEnv {
            Some(origin.to_owned())
        } else {
            None
        },
        mechanism,
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

#[cfg(test)]
async fn login_with(
    store: &dyn Store,
    origin: &str,
    config: provider::Config,
    provider: &provider::Provider,
    organization: Option<&str>,
    stderr: &mut dyn Write,
) -> Result<Value> {
    let cached = renewal::validate_custody(store, origin)?;
    login_validated(
        store,
        origin,
        config,
        provider,
        organization,
        stderr,
        cached,
    )
    .await
}

async fn login_validated(
    store: &dyn Store,
    origin: &str,
    config: provider::Config,
    provider: &provider::Provider,
    organization: Option<&str>,
    stderr: &mut dyn Write,
    cached: Vec<renewal::Session>,
) -> Result<Value> {
    if replacement_principal().is_none()
        && organization.is_some()
        && let Some(saved) = cached
            .into_iter()
            .find(|saved| Some(saved.identity.account_id.as_str()) == organization)
        && saved.config == config
        && saved.state == renewal::RefreshState::Ready
        && renewal::timestamp(&saved.refresh_expires_at)? > OffsetDateTime::now_utc()
    {
        match renewal::credential_loaded(store, saved.clone(), provider).await {
            Ok(current) => {
                let verified = renewal::verify(origin, &config, current.access_token(), None).await;
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
    write_diagnostic(
        stderr,
        format_args!("Open {uri}\nEnter code: {}\n", device.user_code),
    )?;
    loop {
        let next = Instant::now() + Duration::from_secs(interval);
        if next >= deadline {
            return Err(Failure::refused(
                "device_expired",
                "device sign-in expired; run login --target NAME again",
            ));
        }
        tokio::time::sleep_until(next).await;
        let reply = tokio::time::timeout_at(deadline, provider.poll(&device.device_code))
            .await
            .map_err(|_| {
                Failure::refused(
                    "device_expired",
                    "device sign-in expired; run login --target NAME again",
                )
            })?;
        match reply {
            Ok(tokens) => {
                return accept_device(store, origin, config, provider, tokens, organization).await;
            }
            Err(error) if error.code() == Some("authorization_pending") => {}
            Err(error) if error.code() == Some("slow_down") => {
                interval = interval.saturating_add(5).min(600);
            }
            Err(error) => return Err(provider::failure(&error)),
        }
    }
}

async fn accept_device(
    store: &dyn Store,
    origin: &str,
    config: provider::Config,
    provider: &provider::Provider,
    tokens: workos::AuthenticateResponse,
    organization: Option<&str>,
) -> Result<Value> {
    let _claims_lock = if crate::operator_routing::journal_owner().is_some() {
        Some(coordination::global_session_lock().await?)
    } else {
        None
    };
    // Keychain reads can require interactive OS consent. Complete the cached
    // session collision check under the origin lock before timing network work.
    let initial = renewal::check_device_session(store, origin, &config, &tokens)?;
    let (saved, body) = tokio::time::timeout(Duration::from_secs(35), async {
        let tokens =
            renewal::select_device_organization(&config, provider, tokens, initial, organization)
                .await?;
        renewal::accept(origin, config, tokens, None).await
    })
    .await
    .map_err(|_| renewal::unknown())??;
    if let Err(error) = check_account(organization, &saved.identity.account_id) {
        saved.revoke().await;
        return Err(error);
    }
    if let Err(error) =
        renewal::check_replacement(store, &saved, replacement_principal().as_deref())
    {
        saved.revoke().await;
        return Err(error);
    }
    if replacement_principal().is_some() {
        if let Some(auth) = crate::operator_routing::AUTH
            .try_with(Clone::clone)
            .ok()
            .flatten()
        {
            let cleanup = auth
                .invalidate()
                .map_err(|_| keychain_failed())
                .and_then(|()| clear_data());
            if let Err(error) = cleanup {
                saved.revoke().await;
                return Err(error);
            }
        }
    }
    if let Err(error) = renewal::save(store, &saved) {
        // We cannot retain the new renewable session. Revoke it when possible
        // without obscuring the custody failure or touching another session.
        saved.revoke().await;
        return Err(error);
    }
    Ok(body)
}

tokio::task_local! { static REPLACE_PRINCIPAL: Option<String>; }

fn replacement_principal() -> Option<String> {
    REPLACE_PRINCIPAL.try_with(Clone::clone).ok().flatten()
}

pub(super) async fn login_replacing(
    origin: String,
    organization: Option<&str>,
    principal: Option<&str>,
    stderr: &mut dyn Write,
) -> Result<Value> {
    REPLACE_PRINCIPAL
        .scope(
            principal.map(str::to_owned),
            login(origin, organization, stderr),
        )
        .await
}

pub(super) async fn login(
    origin: String,
    organization: Option<&str>,
    stderr: &mut dyn Write,
) -> Result<Value> {
    let _lock = coordination::lock(&origin).await?;
    let cached = renewal::validate_custody(&AUTHKIT_STORE, &origin)?;
    let config = provider::Config::discover(&origin).await?;
    let provider = provider::Provider::new(&config)?;
    login_validated(
        &AUTHKIT_STORE,
        &origin,
        config,
        &provider,
        organization,
        stderr,
        cached,
    )
    .await
}

pub(super) async fn logout(origin: String, organization: Option<&str>) -> Result<Value> {
    let _lock = coordination::lock(&origin).await?;
    logout_after_data_cleanup(&AUTHKIT_STORE, &origin, organization, clear_data).await
}

async fn logout_after_data_cleanup(
    sessions: &dyn Store,
    origin: &str,
    organization: Option<&str>,
    cleanup: impl FnOnce() -> Result<()>,
) -> Result<Value> {
    cleanup()?;
    renewal::logout(sessions, origin, organization).await
}

#[cfg(test)]
pub(super) mod tests;
