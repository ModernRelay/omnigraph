use super::*;
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use workos::transport::{HttpRequest, HttpResponse, HttpTransport, TransportError};

fn automation_credential(
    origin: &str,
    token: &str,
) -> crate::credential_sources::AcquiredCredential {
    use crate::credential_sources::{CredentialRequest, OwnerBinding, OwnerKind, Purpose};
    let owner = OwnerBinding::new(
        OwnerKind::Profile,
        "test-owner".into(),
        "test-auth".into(),
        origin.into(),
        Some("org_test".into()),
    )
    .unwrap();
    let request =
        CredentialRequest::new(owner, Purpose::Control, origin.into(), None, None).unwrap();
    crate::credential_sources::acquire_environment_with(&request, "TEST_TOKEN", |_| {
        Some(token.into())
    })
    .unwrap()
}

#[test]
fn status_reports_unknown_without_requesting_keychain_custody() {
    assert_eq!(
        local_status("https://api.example/prefix", Some("org_test"))["availability"],
        "unknown"
    );
}

#[tokio::test]
async fn selected_control_token_matches_the_canonical_prefixed_route() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("token"), "declared-token\n").unwrap();
    std::fs::write(directory.path().join("config.yaml"), "version: 1\nprofiles:\n  work:\n    api: https://api.example/prefix/\n    organization_id: org_test\n    auth: {provider: token, source: {file: ./token}}\n").unwrap();
    let input = crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
    let cli =
        crate::invocation::parse_from(["omnigraph", "credentials", "status", "--profile", "work"])
            .unwrap()
            .cli;
    let target = crate::scope::resolve(&cli, &input, directory.path(), false, &mut Vec::new())
        .unwrap()
        .unwrap();
    let auth = crate::operator_routing::authentication(&cli, &input, Some(&target)).unwrap();
    crate::operator_routing::AUTH
        .scope(auth, async {
            assert_eq!(
                configured_token("https://api.example/prefix")
                    .await
                    .unwrap()
                    .as_ref()
                    .map(
                        |credential| acquired_bearer(credential, "https://api.example/prefix")
                            .unwrap()
                    ),
                Some("declared-token")
            );
            assert_eq!(
                configured_token("https://api.example/prefix/")
                    .await
                    .unwrap()
                    .as_ref()
                    .map(
                        |credential| acquired_bearer(credential, "https://api.example/prefix/")
                            .unwrap()
                    ),
                Some("declared-token")
            );
            assert!(
                configured_token("https://api.example/sibling")
                    .await
                    .is_err()
            );
        })
        .await;
}

#[cfg(unix)]
#[tokio::test]
async fn automation_credential_expiry_is_rechecked_before_each_control_request() {
    let directory = tempfile::tempdir().unwrap();
    let api = IntentApiFixture::new(vec![
        metadata(120, "principal_one"),
        IntentReply::json(200, json!({"data": {}, "meta": {}})),
        IntentReply::json(
            200,
            json!({"data": {"unexpected": "mutation dispatched"}, "meta": {}}),
        ),
    ]);
    let helper = directory.path().join("helper.sh");
    let response = directory.path().join("response.json");
    let calls = directory.path().join("calls");
    std::fs::write(
        &helper,
        "IFS= read -r request\nprintf 'called\\n' >> \"$2\"\ncat \"$1\"\n",
    )
    .unwrap();
    std::fs::write(
        directory.path().join("config.yaml"),
        json!({
            "version": 1, "profiles": {"work": {
                "api": api.origin, "organization_id": "org_test",
                "auth": {"provider": "exec", "command": ["/bin/sh", helper, response, calls]}
            }}
        })
        .to_string(),
    )
    .unwrap();
    let input = crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
    let cli = crate::invocation::parse_from(["omnigraph", "cluster", "list", "--profile", "work"])
        .unwrap()
        .cli;
    let target = crate::scope::resolve(&cli, &input, directory.path(), false, &mut Vec::new())
        .unwrap()
        .unwrap();
    let authentication =
        crate::operator_routing::authentication(&cli, &input, Some(&target)).unwrap();
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        crate::operator_routing::AUTH
            .scope(authentication, async {
                let expires = OffsetDateTime::now_utc() + time::Duration::seconds(2);
                std::fs::write(
                    &response,
                    json!({"version": 1,
                        "credential": {"type": "bearer", "token": "expiring-control-test-only"},
                        "expires_at": expires.format(&Rfc3339).unwrap()
                    })
                    .to_string(),
                )
                .unwrap();
                let client = Api::authenticated(
                    api.origin.clone(),
                    Some("org_test"),
                    super::super::ControlAuth::AutomationEnv,
                )
                .await
                .unwrap();
                client
                    .request(Method::GET, "/v1/clusters", None, None)
                    .await
                    .unwrap();
                assert_eq!(api.requests().len(), 2);
                while OffsetDateTime::now_utc() < expires {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let result = client
                    .request(
                        Method::POST,
                        "/v1/clusters",
                        Some(&json!({"name": "must-not-create"})),
                        None,
                    )
                    .await;
                assert_eq!(
                    api.requests().len(),
                    2,
                    "expired control credentials must not dispatch another request"
                );
                let error = result.unwrap_err();
                assert_eq!(error.body["type"], "credential_expired");
                assert!(
                    !error
                        .body
                        .to_string()
                        .contains("expiring-control-test-only")
                );
                assert_eq!(
                    std::fs::read_to_string(&calls).unwrap(),
                    "called\n",
                    "expiry must not rerun the helper"
                );
            })
            .await;
    })
    .await;
}

#[derive(Default)]
pub(in crate::managed) struct MemoryStore(pub RefCell<HashMap<String, String>>);
impl Store for MemoryStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.0.borrow().get(key).cloned())
    }
    fn put(&self, key: &str, value: &str) -> Result<()> {
        self.0.borrow_mut().insert(key.into(), value.into());
        Ok(())
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.0.borrow_mut().remove(key);
        Ok(())
    }
}

struct InterruptedCleanup<'a> {
    raw: &'a MemoryStore,
    remaining: Cell<usize>,
}

impl Store for InterruptedCleanup<'_> {
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.raw.get(key)
    }
    fn put(&self, key: &str, value: &str) -> Result<()> {
        self.raw.put(key, value)
    }
    fn remove(&self, key: &str) -> Result<()> {
        if self.remaining.get() == 0 {
            return Err(keychain_failed());
        }
        self.remaining.set(self.remaining.get() - 1);
        self.raw.remove(key)
    }
}

#[tokio::test]
async fn derived_index_survives_generations_route_changes_and_interrupted_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let input = crate::operator::InvocationConfig::at(Some(root.path().to_owned()), None);
    let binding = |profile, route| {
        crate::operator_routing::registration_auth(&input, profile, route).unwrap()
    };
    let first = binding("human", "https://first.example");
    let other = binding("other", "https://first.example");
    let raw = MemoryStore::default();
    let (index_account, old_key) = crate::operator_routing::AUTH
        .scope(Some(first.clone()), async {
            let key = crate::operator_routing::data_namespace("cluster-one");
            put_data(&raw, &key, "first-generation").unwrap();
            (DataIndex::selected().unwrap().account(), key)
        })
        .await;
    let (other_index, other_key) = crate::operator_routing::AUTH
        .scope(Some(other), async {
            let key = crate::operator_routing::data_namespace("cluster-one");
            put_data(&raw, &key, "other-profile").unwrap();
            (DataIndex::selected().unwrap().account(), key)
        })
        .await;
    first.invalidate().unwrap();
    crate::operator_routing::AUTH
        .scope(Some(first), async {
            assert_eq!(
                put_data(&raw, &old_key, "stale-writer").unwrap_err().body["type"],
                "credential_generation_changed"
            );
            assert_eq!(
                raw.get(&old_key).unwrap().as_deref(),
                Some("first-generation")
            );
        })
        .await;
    let second = binding("human", "https://second.example");
    let new_key = crate::operator_routing::AUTH
        .scope(Some(second.clone()), async {
            let index = DataIndex::selected().unwrap();
            assert_eq!(index.account(), index_account);
            let key = crate::operator_routing::data_namespace("cluster-two");
            assert_ne!(key, old_key);
            put_data(&raw, &key, "second-generation").unwrap();
            assert_eq!(index.load(&raw).unwrap().len(), 2);
            assert!(
                index
                    .clear(&InterruptedCleanup {
                        raw: &raw,
                        remaining: Cell::new(1)
                    })
                    .is_err()
            );
            assert!(raw.get(&index_account).unwrap().is_some());
            key
        })
        .await;
    second.invalidate().unwrap();
    let fresh = binding("human", "https://third.example");
    crate::operator_routing::AUTH
        .scope(Some(fresh), async {
            let index = DataIndex::selected().unwrap();
            assert_eq!(index.account(), index_account);
            index.clear(&raw).unwrap();
            assert!(raw.get(&old_key).unwrap().is_none());
            assert!(raw.get(&new_key).unwrap().is_none());
            assert!(raw.get(&index_account).unwrap().is_none());
            assert_eq!(
                raw.get(&other_key).unwrap().as_deref(),
                Some("other-profile")
            );
            assert!(raw.get(&other_index).unwrap().is_some());
            assert_eq!(raw.0.borrow().len(), 2);
        })
        .await;
}

#[test]
fn derived_index_rejects_foreign_or_malformed_keys_before_any_deletion() {
    let index = DataIndex {
        profile: "owner:one:profile:selected".into(),
    };
    let owned = format!(
        "{}:binding:{}:generation:1:credential:cluster",
        index.profile,
        "a".repeat(64)
    );
    let foreign = format!(
        "owner:one:profile:other:binding:{}:generation:1:credential:cluster",
        "a".repeat(64)
    );
    for invalid in [
        foreign,
        index.account(),
        format!(
            "{}:binding:invalid:generation:1:credential:cluster",
            index.profile
        ),
    ] {
        let raw = MemoryStore::default();
        raw.put(&owned, "selected").unwrap();
        raw.put(&invalid, "foreign").unwrap();
        raw.put(
            &index.account(),
            &serde_json::to_string(&[&owned, &invalid]).unwrap(),
        )
        .unwrap();
        let before = raw.0.borrow().clone();
        assert!(index.clear(&raw).is_err());
        assert!(index.put(&raw, &owned, "replacement").is_err());
        assert_eq!(*raw.0.borrow(), before);
    }
}

struct PromptingStore<'a> {
    inner: &'a MemoryStore,
    prompted: Cell<bool>,
}

impl Store for PromptingStore<'_> {
    fn get(&self, key: &str) -> Result<Option<String>> {
        if key.starts_with("credential:") && !self.prompted.replace(true) {
            // Model synchronous OS consent taking longer than the network budget.
            std::thread::sleep(Duration::from_secs(40));
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, value: &str) -> Result<()> {
        self.inner.put(key, value)
    }

    fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key)
    }
}

pub(super) fn config() -> provider::Config {
    provider::Config {
        version: 2,
        provider: "workos_authkit".into(),
        client_id: "client_test".into(),
        issuer: "https://issuer.example".into(),
        authorization_endpoint: "https://api.workos.com/user_management/authorize".into(),
        device_authorization_endpoint: "https://api.workos.com/user_management/authorize/device"
            .into(),
        token_endpoint: "https://api.workos.com/user_management/authenticate".into(),
    }
}

pub(super) fn jwt(seconds: i64, user: &str) -> String {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let expires = OffsetDateTime::now_utc().unix_timestamp() + seconds;
    let claims = json!({"iss":"https://issuer.example","sub":user,"sid":"session_one","org_id":"org_test","client_id":"client_test","iat":expires-300,"exp":expires});
    format!(
        "{}.{}.signature",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","kid":"key"}"#),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    )
}

pub(super) fn tokens(seconds: i64, user: &str) -> Value {
    json!({"user":{"object":"user","id":user,"email":"test@example.com","email_verified":true,"created_at":"2026-09-14T00:00:00Z","updated_at":"2026-09-14T00:00:00Z"},
        "organization_id":"org_test","access_token":jwt(seconds,user),"refresh_token":"rotated-refresh"})
}

pub(super) fn metadata(seconds: i64, principal: &str) -> IntentReply {
    metadata_for(seconds, principal, "org_test")
}

fn metadata_for(seconds: i64, principal: &str, organization: &str) -> IntentReply {
    IntentReply::json(
        200,
        json!({"data":{"principal_id":principal,"account_id":organization,"kind":"human","service_permissions":["source_read","status"],
        "expires_at":OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp()+seconds).unwrap().format(&Rfc3339).unwrap()},"meta":{}}),
    )
}

#[derive(Default)]
pub(super) struct MockTransport {
    pub requests: Mutex<Vec<HttpRequest>>,
    pub responses: Mutex<VecDeque<(u16, Value)>>,
    pending: bool,
}

impl MockTransport {
    pub fn with(responses: Vec<(u16, Value)>) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(responses.into()),
            ..Self::default()
        })
    }
    pub fn provider(self: &Arc<Self>) -> provider::Provider {
        provider::Provider::with_transport(&config(), self.clone()).unwrap()
    }
}

#[async_trait::async_trait]
impl HttpTransport for MockTransport {
    async fn execute(
        &self,
        request: HttpRequest,
    ) -> std::result::Result<HttpResponse, TransportError> {
        self.requests.lock().unwrap().push(request);
        if self.pending {
            return std::future::pending().await;
        }
        let (status, value) = self.responses.lock().unwrap().pop_front().ok_or_else(|| {
            TransportError::other(std::io::Error::other("unexpected provider call"))
        })?;
        Ok(HttpResponse {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            headers: reqwest::header::HeaderMap::new(),
            body: value.to_string().into(),
        })
    }
}

#[test]
fn explicit_automation_requires_its_exact_origin_and_never_uses_human_cache() {
    assert_eq!(
        automation(
            "https://api.example",
            Some("token".into()),
            Some("https://API.example:443".into())
        )
        .unwrap(),
        Some("token".into())
    );
    assert!(automation("https://api.example", Some("token".into()), None).is_err());
    assert!(
        automation(
            "https://api.example",
            None,
            Some("https://api.example".into())
        )
        .is_err()
    );
    assert!(
        automation(
            "https://api.example",
            Some("token".into()),
            Some("https://other.example".into())
        )
        .is_err()
    );
    assert_eq!(automation("https://api.example", None, None).unwrap(), None);
}

#[tokio::test]
async fn explicit_automation_resolves_its_own_principal_before_data_cache_reuse() {
    let api = IntentApiFixture::new(vec![metadata(120, "automation_principal")]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        // Even unreadable local human custody cannot override an explicit identity.
        store.put(&api.origin, "unrelated-human-record").unwrap();
        let principal = selected_principal_with(
            &store,
            &api.origin,
            Some("org_test"),
            Some(automation_credential(&api.origin, "automation-access")),
            Some(api.origin.clone()),
            super::super::ControlAuth::AutomationEnv,
        )
        .await
        .unwrap();
        assert_eq!(principal.as_deref(), Some("automation_principal"));
        assert_eq!(api.requests()[0].path, "/v1/auth/session");
        api.assert_complete();
    })
    .await;
}

#[test]
fn provider_configuration_rejects_credential_exfiltration_and_other_profiles() {
    config().validate().unwrap();
    for field in [
        "token_endpoint",
        "device_authorization_endpoint",
        "authorization_endpoint",
        "issuer",
    ] {
        let mut value = serde_json::to_value(config()).unwrap();
        value[field] = json!("http://127.0.0.1/steal");
        assert!(
            serde_json::from_value::<provider::Config>(value)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let mut wrong = config();
    wrong.provider = "connect".into();
    assert!(wrong.validate().is_err());
    for value in [0.0, 4.0, 601.0, 5.5, f64::INFINITY, f64::NAN] {
        assert!(seconds(value, 5, 600).is_err());
    }
}

#[test]
fn redaction_covers_secret_values_and_object_keys_without_provider_error_text() {
    let mut value =
        json!({"access_token":"bearer","nested":{"bearer-key":"bearer","refresh_token":"refresh"}});
    scrub_value(&mut value, "bearer");
    assert!(!value.to_string().contains("bearer"));
    assert!(!value.to_string().contains("refresh"));
    assert!(validate_token("token\nheader").is_err());
    assert!(verification_uri("http://auth.example/device").is_err());
}

#[tokio::test]
async fn explicit_api_credentials_are_scrubbed_from_success_and_error_responses() {
    let token = "explicit-automation-secret";
    let fixture = IntentApiFixture::new(vec![
        IntentReply::json(200, json!({"data":{"reflected":token},"meta":{}})),
        IntentReply::json(403, json!({"type":"permission_denied","detail":token})),
    ]);
    crate::operator_routing::test_transport_scope(&[&fixture.origin], async {
        let api = Api::new(fixture.origin.clone(), Some(token.into())).unwrap();
        let response = api
            .request(Method::GET, "/v1/auth/session", None, None)
            .await
            .unwrap();
        assert!(!response.to_string().contains(token));
        let error = api
            .request(Method::GET, "/v1/auth/session", None, None)
            .await
            .unwrap_err();
        assert_eq!(error.exit, 2);
        assert_eq!(error.body["type"], "permission_denied");
        assert!(!error.body.to_string().contains(token));
        for request in fixture.requests() {
            assert_eq!(request.headers["authorization"], format!("Bearer {token}"));
        }
        fixture.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn official_sdk_uses_public_client_device_and_refresh_without_secret_or_retry() {
    let transport = MockTransport::with(vec![
        (
            200,
            json!({"device_code":"secret-device","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":600,"interval":5}),
        ),
        (200, tokens(120, "user_one")),
        (
            503,
            json!({"error":"unavailable","message":"secret-provider-reflection"}),
        ),
    ]);
    let provider = transport.provider();
    provider.device(&config()).await.unwrap();
    provider.poll("secret-device").await.unwrap();
    let error = provider
        .refresh("secret-refresh", "org_test")
        .await
        .err()
        .unwrap();
    assert!(
        !provider::failure(&error)
            .body
            .to_string()
            .contains("secret-provider-reflection")
    );
    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for request in requests.iter() {
        assert!(!request.headers.contains_key(reqwest::header::AUTHORIZATION));
        let body: Value = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
        assert_eq!(body["client_id"], "client_test");
        assert!(body.get("client_secret").is_none());
    }
    assert_eq!(requests[0].url, config().device_authorization_endpoint);
    assert_eq!(requests[1].url, config().token_endpoint);
}

#[tokio::test]
async fn api_verifies_new_provider_identity_before_the_cache_accepts_it() {
    for permissions in [
        Some(json!(["source_read", "status"])),
        Some(json!([])),
        Some(json!(["status", "status"])),
        Some(json!(["status", "source_read"])),
        Some(json!(["bad\n"])),
        Some(Value::Null),
        None,
    ] {
        let valid = matches!(&permissions, Some(Value::Array(values)) if values.is_empty() || values == &vec![json!("source_read"), json!("status")]);
        let mut reply = metadata(120, "principal_one");
        let mut body: Value = serde_json::from_slice(&reply.body).unwrap();
        if let Some(permissions) = permissions {
            body["data"]["service_permissions"] = permissions;
        } else {
            body["data"]
                .as_object_mut()
                .unwrap()
                .remove("service_permissions");
            body["data"]["scopes"] = json!({});
        }
        reply.body = body.to_string().into_bytes();
        let api = IntentApiFixture::new(vec![reply]);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            let response = serde_json::from_value(tokens(120, "user_one")).unwrap();
            let result = renewal::accept(&api.origin, config(), response, None).await;
            if valid {
                let (saved, output) = result.unwrap();
                let store = MemoryStore::default();
                renewal::save(&store, &saved).unwrap();
                assert!(renewal::load(&store, &api.origin).unwrap().is_some());
                assert_eq!(
                    output["data"]["service_permissions"],
                    body["data"]["service_permissions"]
                );
                assert!(output["data"].get("scopes").is_none());
                assert!(!output.to_string().contains("rotated-refresh"));
                assert!(!output.to_string().contains(".signature"));
            } else {
                assert!(result.is_err());
            }
            let requests = api.requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].path, "/v1/auth/session");
            assert!(requests[0].headers["authorization"].starts_with("Bearer ey"));
            api.assert_complete();
        })
        .await;
    }
}

#[tokio::test]
async fn cached_login_does_not_restart_provider_authorization() {
    let api = IntentApiFixture::new(vec![
        metadata(120, "principal_one"),
        metadata(120, "principal_one"),
    ]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let tokens = serde_json::from_value(tokens(120, "user_one")).unwrap();
        let (saved, _) = renewal::accept(&api.origin, config(), tokens, None)
            .await
            .unwrap();
        let store = MemoryStore::default();
        renewal::save(&store, &saved).unwrap();
        let provider = MockTransport::with(vec![]);
        let output = login_with(
            &store,
            &api.origin,
            config(),
            &provider.provider(),
            Some("org_test"),
            &mut Vec::new(),
        )
        .await
        .unwrap();
        assert_eq!(output["data"]["principal_id"], "principal_one");
        assert!(output["data"]["refresh_expires_at"].is_string());
        assert!(provider.requests.lock().unwrap().is_empty());
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
#[ignore = "nightly: real 5 s device-flow poll intervals, 20 s in all"]
async fn device_login_polls_the_provider_and_never_the_old_broker() {
    let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
    let transport = MockTransport::with(vec![
        (
            200,
            json!({"device_code":"device-secret","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":30,"interval":5}),
        ),
        (400, json!({"error":"authorization_pending"})),
        (400, json!({"error":"slow_down"})),
        (200, tokens(120, "user_one")),
    ]);
    let store = MemoryStore::default();
    let output = login_with(
        &store,
        &api.origin,
        config(),
        &transport.provider(),
        None,
        &mut Vec::new(),
    )
    .await
    .unwrap();
    assert_eq!(output["data"]["principal_id"], "principal_one");
    assert_eq!(transport.requests.lock().unwrap().len(), 4);
    assert_eq!(api.requests()[0].path, "/v1/auth/session");
    api.assert_complete();
}).await;
}

#[tokio::test]
async fn rejected_resource_identity_never_becomes_a_cached_session() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        401,
        json!({"type":"unauthenticated"}),
    )]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let tokens = serde_json::from_value(tokens(120, "user_one")).unwrap();
        assert!(
            renewal::accept(&api.origin, config(), tokens, None)
                .await
                .is_err()
        );
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn device_deadline_prevents_a_poll_or_resource_request_after_expiry() {
    use crate::managed::tests::{FailedOutput, OutputFault};
    for fault in [
        None,
        Some(OutputFault::PartialWrite),
        Some(OutputFault::Flush),
    ] {
        let api = IntentApiFixture::new(vec![]);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let transport = MockTransport::with(vec![(
            200,
            json!({"device_code":"device-secret","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":if fault.is_some() {30} else {1},"interval":5}),
        )]);
        let store = MemoryStore::default();
        let mut delivered = Vec::new();
        let mut failed = FailedOutput::new(fault.unwrap_or(OutputFault::Flush), 0);
        let stderr: &mut dyn Write = if fault.is_some() {
            &mut failed
        } else {
            &mut delivered
        };
        let error = login_with(
            &store,
            &api.origin,
            config(),
            &transport.provider(),
            None,
            stderr,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.body["type"],
            if fault.is_some() {
                "output_delivery_failed"
            } else {
                "device_expired"
            }
        );
        if fault.is_none() {
            assert_eq!(
                delivered,
                b"Open https://auth.example/device\nEnter code: ABCD\n"
            );
        }
        assert!(!error.body.to_string().contains("device-secret"));
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
        assert!(store.0.borrow().is_empty());
        assert!(api.requests().is_empty());
    }).await;
    }
}

#[tokio::test]
#[ignore = "nightly: one real 5 s device-flow poll interval"]
async fn failed_login_custody_attempts_to_revoke_the_new_provider_session() {
    struct RefusingStore;
    impl Store for RefusingStore {
        fn get(&self, _: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn put(&self, _: &str, _: &str) -> Result<()> {
            Err(keychain_failed())
        }
        fn remove(&self, _: &str) -> Result<()> {
            Ok(())
        }
    }
    let api = IntentApiFixture::new(vec![
        metadata(120, "principal_one"),
        IntentReply::json(200, json!({"data":{"logged_out":true},"meta":{}})),
    ]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
    let transport = MockTransport::with(vec![
        (
            200,
            json!({"device_code":"device-secret","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":30,"interval":5}),
        ),
        (200, tokens(120, "user_one")),
    ]);
    let error = login_with(
        &RefusingStore,
        &api.origin,
        config(),
        &transport.provider(),
        None,
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.body["type"], "keychain_unavailable");
    assert_eq!(api.requests()[1].path, "/v1/auth/logout");
    assert!(api.requests()[1].headers["authorization"].starts_with("Bearer ey"));
    api.assert_complete();
}).await;
}

#[tokio::test]
async fn organization_discovery_starts_fresh_with_zero_one_or_multiple_cached_organizations() {
    for count in [0, 1, 2] {
        let organizations = ["org_test", "org_other"];
        let api = IntentApiFixture::new(
            organizations
                .iter()
                .take(count)
                .map(|org| metadata_for(120, "principal_one", org))
                .collect(),
        );
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        for org in organizations.iter().take(count) {
            let mut supplied = tokens(120, "user_one");
            supplied["organization_id"] = json!(org);
            rewrite_claim(&mut supplied, "org_id", json!(org));
            let (saved, _) = renewal::accept(
                &api.origin,
                config(),
                serde_json::from_value(supplied).unwrap(),
                None,
            )
            .await
            .unwrap();
            renewal::save(&store, &saved).unwrap();
        }
        let before = store.0.borrow().clone();
        let transport = MockTransport::with(vec![(
            200,
            json!({"device_code":"device-secret","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":1,"interval":5}),
        )]);
        let error = login_with(
            &store,
            &api.origin,
            config(),
            &transport.provider(),
            None,
            &mut Vec::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.body["type"], "device_expired");
        assert_eq!(*store.0.borrow(), before);
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, config().device_authorization_endpoint);
        let body: Value = serde_json::from_slice(requests[0].body.as_ref().unwrap()).unwrap();
        assert_eq!(body, json!({"client_id":"client_test"}));
        api.assert_complete();
    }).await;
    }
}

#[tokio::test]
async fn explicit_organization_must_match_the_verified_automation_account() {
    let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let result = select_with(
            &MemoryStore::default(),
            &api.origin,
            Some("org_other"),
            Some(automation_credential(&api.origin, "automation")),
            Some(api.origin.clone()),
            super::super::ControlAuth::AutomationEnv,
        )
        .await;
        assert_eq!(result.err().unwrap().body["type"], "organization_mismatch");
        assert_eq!(api.requests().len(), 1);
        api.assert_complete();
    })
    .await;
}

#[test]
fn discovery_v2_has_no_tenant_selector_and_rejects_v1() {
    let value = serde_json::to_value(config()).unwrap();
    assert_eq!(value["version"], 2);
    assert!(value.get("organization_id").is_none());
    let mut legacy = value.clone();
    legacy["version"] = json!(1);
    assert!(
        serde_json::from_value::<provider::Config>(legacy)
            .unwrap()
            .validate()
            .is_err()
    );
    let mut tenant = value;
    tenant["organization_id"] = json!("org_test");
    assert!(serde_json::from_value::<provider::Config>(tenant).is_err());
}

#[tokio::test]
#[ignore = "nightly: one real 5 s device-flow poll interval"]
async fn device_login_refuses_another_organization_before_saving_credentials() {
    let api = IntentApiFixture::new(vec![]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
    let transport = MockTransport::with(vec![
        (
            200,
            json!({"device_code":"device-secret","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":30,"interval":5}),
        ),
        (200, tokens(120, "user_one")),
        (200, tokens(120, "user_one")),
    ]);
    let store = MemoryStore::default();
    let error = login_with(
        &store,
        &api.origin,
        config(),
        &transport.provider(),
        Some("org_other"),
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.body["type"], "organization_mismatch");
    assert!(store.0.borrow().is_empty());
    assert!(api.requests().is_empty());
    for request in transport.requests.lock().unwrap().iter().take(2) {
        let body: Value = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
        assert!(body.get("organization_id").is_none());
    }
    api.assert_complete();
}).await;
}

fn rewrite_claim(tokens: &mut Value, field: &str, value: Value) {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let parts: Vec<_> = tokens["access_token"]
        .as_str()
        .unwrap()
        .split('.')
        .collect();
    let mut claims: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    claims[field] = value;
    tokens["access_token"] = json!(format!(
        "{}.{}.{}",
        parts[0],
        URL_SAFE_NO_PAD.encode(claims.to_string()),
        parts[2]
    ));
}

fn device_tokens(organization: Option<&str>, session: &str, refresh: &str) -> Value {
    let mut value = tokens(120, "user_one");
    value["organization_id"] = json!(organization);
    value["refresh_token"] = json!(refresh);
    rewrite_claim(&mut value, "org_id", json!(organization));
    rewrite_claim(&mut value, "sid", json!(session));
    value
}

async fn cache_original_session(store: &MemoryStore, origin: &str) {
    let original = device_tokens(Some("org_test"), "cached_session", "cached-refresh");
    let (saved, _) = renewal::accept(
        origin,
        config(),
        serde_json::from_value(original).unwrap(),
        None,
    )
    .await
    .unwrap();
    renewal::save(store, &saved).unwrap();
}

#[tokio::test]
async fn fresh_login_replaces_a_different_human_only_after_exact_confirmation() {
    for confirmed in [false, true] {
        let mut replies = vec![
            metadata(120, "principal_one"),
            metadata(120, "principal_other"),
        ];
        if !confirmed {
            replies.push(IntentReply::json(
                200,
                json!({"data":{"logged_out":true},"meta":{}}),
            ));
        }
        let api = IntentApiFixture::new(replies);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            let store = MemoryStore::default();
            cache_original_session(&store, &api.origin).await;
            let before = store.0.borrow().clone();
            let transport = MockTransport::with(vec![]);
            let mut supplied = device_tokens(Some("org_test"), "fresh_session", "fresh-refresh");
            supplied["user"]["id"] = json!("user_other");
            rewrite_claim(&mut supplied, "sub", json!("user_other"));
            let result = REPLACE_PRINCIPAL
                .scope(
                    confirmed.then(|| "principal_other".into()),
                    accept_device(
                        &store,
                        &api.origin,
                        config(),
                        &transport.provider(),
                        serde_json::from_value(supplied).unwrap(),
                        Some("org_test"),
                    ),
                )
                .await;
            if confirmed {
                result.unwrap();
                assert_eq!(
                    renewal::load_selected(&store, &api.origin, Some("org_test"))
                        .unwrap()
                        .unwrap()
                        .identity
                        .principal_id,
                    "principal_other"
                );
            } else {
                assert_eq!(
                    result.unwrap_err().body["type"],
                    "principal_replacement_required"
                );
                assert_eq!(*store.0.borrow(), before);
            }
            assert!(transport.requests.lock().unwrap().is_empty());
            api.assert_complete();
        })
        .await;
    }
}

#[tokio::test]
async fn fresh_device_selection_uses_only_new_credentials_and_preserves_cached_organization() {
    check_fresh_device_selection(false).await;
}

#[tokio::test]
#[ignore = "nightly: one real 40 s synchronous Keychain consent delay"]
async fn fresh_device_selection_excludes_keychain_consent_from_network_deadline() {
    check_fresh_device_selection(true).await;
}

async fn check_fresh_device_selection(wait_for_consent: bool) {
    for initial_org in [None, Some("org_unenrolled")] {
        let api = IntentApiFixture::new(vec![
            metadata(120, "principal_one"),
            metadata_for(120, "principal_one", "org_other"),
        ]);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        cache_original_session(&store, &api.origin).await;
        let before = store.0.borrow().clone();
        let prompting_store = PromptingStore {
            inner: &store,
            prompted: Cell::new(false),
        };
        // Only one case waits for consent; both fresh organization profiles
        // retain their existing custody and provider-request assertions.
        let device_store: &dyn Store = if wait_for_consent && initial_org.is_none() {
            &prompting_store
        } else {
            &store
        };
        let selected = device_tokens(Some("org_other"), "fresh_session", "selected-refresh");
        let transport = MockTransport::with(vec![(200, selected.clone())]);
        let result = accept_device(
            device_store,
            &api.origin,
            config(),
            &transport.provider(),
            serde_json::from_value(device_tokens(initial_org, "fresh_session", "fresh-refresh"))
                .unwrap(),
            Some("org_other"),
        )
        .await
        .unwrap();
        assert_eq!(
            prompting_store.prompted.get(),
            wait_for_consent && initial_org.is_none()
        );
        assert_eq!(result["data"]["account_id"], "org_other");
        assert_eq!(result["data"]["principal_id"], "principal_one");
        for (key, value) in before.iter().filter(|(key, _)| *key != &api.origin) {
            assert_eq!(store.0.borrow().get(key), Some(value));
        }
        let original_index: Value = serde_json::from_str(&before[&api.origin]).unwrap();
        let index: Value = serde_json::from_str(&store.0.borrow()[&api.origin]).unwrap();
        assert_eq!(
            index["sessions"]["org_test"],
            original_index["sessions"]["org_test"]
        );
        assert_eq!(index["sessions"].as_object().unwrap().len(), 2);
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(requests[0].body.as_ref().unwrap()).unwrap();
        assert_eq!(
            body,
            json!({"client_id":"client_test","grant_type":"refresh_token","refresh_token":"fresh-refresh","organization_id":"org_other","ip_address":null,"device_id":null,"user_agent":null})
        );
        assert_eq!(
            api.requests().len(),
            2,
            "only the selected device bearer reaches the API"
        );
        assert_eq!(
            api.requests()[1].headers["authorization"],
            format!("Bearer {}", selected["access_token"].as_str().unwrap())
        );
        assert!(!result.to_string().contains("selected-refresh"));
        assert!(!result.to_string().contains("fresh-refresh"));
        api.assert_complete();
    }).await;
    }
}

#[tokio::test]
#[ignore = "nightly: one real 35 s organization-selection network deadline"]
async fn fresh_device_selection_network_timeout_preserves_cached_custody_without_replay() {
    let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        cache_original_session(&store, &api.origin).await;
        let before = store.0.borrow().clone();
        let transport = Arc::new(MockTransport {
            pending: true,
            ..MockTransport::default()
        });
        let error = tokio::time::timeout(
            Duration::from_secs(40),
            accept_device(
                &store,
                &api.origin,
                config(),
                &transport.provider(),
                serde_json::from_value(device_tokens(None, "fresh_session", "fresh-refresh"))
                    .unwrap(),
                Some("org_other"),
            ),
        )
        .await
        .expect("network selection remains bounded")
        .unwrap_err();
        assert_eq!(error.body["type"], "refresh_outcome_unknown");
        assert_eq!(*store.0.borrow(), before);
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
        assert_eq!(api.requests().len(), 1, "no acceptance or revocation");
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn fresh_device_selection_refuses_substitutions_without_changing_cached_custody() {
    for change in [
        "organization",
        "user",
        "subject",
        "session",
        "issuer",
        "client",
        "expiry",
        "response_organization",
        "provider_denial",
        "uncertain_exchange",
        "target_denial",
    ] {
        let mut responses = vec![metadata(120, "principal_one")];
        if change == "target_denial" {
            responses.push(IntentReply::json(401, json!({"type":"unauthenticated"})));
        }
        let api = IntentApiFixture::new(responses);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            let store = MemoryStore::default();
            cache_original_session(&store, &api.origin).await;
            let before = store.0.borrow().clone();
            let mut selected =
                device_tokens(Some("org_other"), "fresh_session", "selected-refresh");
            let mut status = 200;
            match change {
                "organization" => {
                    selected["organization_id"] = json!("org_wrong");
                    rewrite_claim(&mut selected, "org_id", json!("org_wrong"));
                }
                "user" => selected["user"]["id"] = json!("user_other"),
                "subject" => rewrite_claim(&mut selected, "sub", json!("user_other")),
                "session" => rewrite_claim(&mut selected, "sid", json!("cached_session")),
                "issuer" => rewrite_claim(&mut selected, "iss", json!("https://other.example")),
                "client" => rewrite_claim(&mut selected, "client_id", json!("client_other")),
                "expiry" => rewrite_claim(&mut selected, "exp", json!(0)),
                "response_organization" => selected["organization_id"] = json!("org_wrong"),
                "provider_denial" => {
                    status = 400;
                    selected = json!({"error":"invalid_grant","message":"fresh-refresh"});
                }
                "uncertain_exchange" => {
                    status = 503;
                    selected = json!({"error":"unavailable","message":"fresh-refresh"});
                }
                "target_denial" => {}
                _ => unreachable!(),
            }
            let transport = MockTransport::with(vec![(status, selected)]);
            let error = accept_device(
                &store,
                &api.origin,
                config(),
                &transport.provider(),
                serde_json::from_value(device_tokens(
                    Some("org_test"),
                    "fresh_session",
                    "fresh-refresh",
                ))
                .unwrap(),
                Some("org_other"),
            )
            .await
            .unwrap_err();
            assert!(
                !error.body.to_string().contains("fresh-refresh"),
                "{change}"
            );
            assert!(
                !error.body.to_string().contains("selected-refresh"),
                "{change}"
            );
            assert_eq!(*store.0.borrow(), before, "{change}");
            assert_eq!(
                transport.requests.lock().unwrap().len(),
                1,
                "no replay or fallback: {change}"
            );
            assert_eq!(
                api.requests().len(),
                if change == "target_denial" { 2 } else { 1 },
                "no substituted bearer is accepted or revoked: {change}"
            );
            api.assert_complete();
        })
        .await;
    }
}

#[tokio::test]
async fn fresh_device_cannot_select_or_revoke_an_existing_cached_session() {
    for malformed in [false, true] {
        let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            let store = MemoryStore::default();
            cache_original_session(&store, &api.origin).await;
            if malformed {
                let key = store
                    .0
                    .borrow()
                    .keys()
                    .find(|key| key.starts_with("credential:"))
                    .unwrap()
                    .clone();
                store.0.borrow_mut().insert(key, "invalid".into());
            }
            let before = store.0.borrow().clone();
            let transport = MockTransport::with(vec![]);
            let error = accept_device(
                &store,
                &api.origin,
                config(),
                &transport.provider(),
                serde_json::from_value(device_tokens(
                    Some("org_test"),
                    "cached_session",
                    "fresh-refresh",
                ))
                .unwrap(),
                Some("org_other"),
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.body["type"],
                if malformed {
                    "credential_invalid"
                } else {
                    "session_identity_mismatch"
                }
            );
            assert_eq!(*store.0.borrow(), before);
            assert!(transport.requests.lock().unwrap().is_empty());
            assert_eq!(api.requests().len(), 1);
            api.assert_complete();
        })
        .await;
    }
}

#[tokio::test]
async fn selected_device_custody_failure_revokes_only_the_new_verified_session() {
    struct RefusingStore<'a>(&'a MemoryStore);
    impl Store for RefusingStore<'_> {
        fn get(&self, key: &str) -> Result<Option<String>> {
            self.0.get(key)
        }
        fn put(&self, _: &str, _: &str) -> Result<()> {
            Err(keychain_failed())
        }
        fn remove(&self, _: &str) -> Result<()> {
            panic!("existing custody must not be removed")
        }
    }
    let api = IntentApiFixture::new(vec![
        metadata(120, "principal_one"),
        metadata_for(120, "principal_one", "org_other"),
        IntentReply::json(200, json!({"data":{"logged_out":true},"meta":{}})),
    ]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        cache_original_session(&store, &api.origin).await;
        let before = store.0.borrow().clone();
        let selected = device_tokens(Some("org_other"), "fresh_session", "selected-refresh");
        let transport = MockTransport::with(vec![(200, selected.clone())]);
        let error = accept_device(
            &RefusingStore(&store),
            &api.origin,
            config(),
            &transport.provider(),
            serde_json::from_value(device_tokens(None, "fresh_session", "fresh-refresh")).unwrap(),
            Some("org_other"),
        )
        .await
        .unwrap_err();
        assert_eq!(error.body["type"], "keychain_unavailable");
        assert_eq!(*store.0.borrow(), before);
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
        let requests = api.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].path, "/v1/auth/logout");
        assert_eq!(
            requests[2].headers["authorization"],
            format!("Bearer {}", selected["access_token"].as_str().unwrap())
        );
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn target_mechanism_never_falls_back_to_another_credential_class() {
    let store = MemoryStore::default();
    let origin = "https://control.example";
    // A human target ignores foreign automation input.
    let human = select_with(
        &store,
        origin,
        Some("org_one"),
        Some(automation_credential(
            "https://foreign.example",
            "foreign-secret",
        )),
        Some("https://foreign.example".into()),
        super::super::ControlAuth::HumanDevice,
    )
    .await
    .unwrap();
    assert!(human.is_none());
    store.put(origin, "unreadable-human-session").unwrap();
    let error = select_with(
        &store,
        origin,
        Some("org_one"),
        None,
        None,
        super::super::ControlAuth::AutomationEnv,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.body["type"], "automation_credential_required");
    assert_eq!(
        selected_principal_with(
            &store,
            origin,
            Some("org_one"),
            None,
            None,
            super::super::ControlAuth::AutomationEnv
        )
        .await
        .unwrap(),
        None
    );
}
