use super::*;
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use workos::transport::{HttpRequest, HttpResponse, HttpTransport, TransportError};

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

pub(super) fn config() -> provider::Config {
    provider::Config {
        version: 1,
        provider: "workos_authkit".into(),
        client_id: "client_test".into(),
        issuer: "https://issuer.example".into(),
        organization_id: "org_test".into(),
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
    IntentReply::json(
        200,
        json!({"data":{"principal_id":principal,"account_id":"org_test","kind":"human","scopes":{},
        "expires_at":OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp()+seconds).unwrap().format(&Rfc3339).unwrap()},"meta":{}}),
    )
}

#[derive(Default)]
pub(super) struct MockTransport {
    pub requests: Mutex<Vec<HttpRequest>>,
    pub responses: Mutex<VecDeque<(u16, Value)>>,
}

impl MockTransport {
    pub fn with(responses: Vec<(u16, Value)>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(vec![]),
            responses: Mutex::new(responses.into()),
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
    let store = MemoryStore::default();
    // Even unreadable local human custody cannot override an explicit identity.
    store.put(&api.origin, "unrelated-human-record").unwrap();
    let principal = selected_principal_with(
        &store,
        &api.origin,
        Some("automation-access".into()),
        Some(api.origin.clone()),
    )
    .await
    .unwrap();
    assert_eq!(principal.as_deref(), Some("automation_principal"));
    assert_eq!(api.requests()[0].path, "/v1/auth/session");
    api.assert_complete();
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
        .refresh("secret-refresh", &config())
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
    let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
    let response = serde_json::from_value(tokens(120, "user_one")).unwrap();
    let (saved, output) = renewal::accept(&api.origin, config(), response, None)
        .await
        .unwrap();
    let store = MemoryStore::default();
    renewal::save(&store, &saved).unwrap();
    assert!(renewal::load(&store, &api.origin).unwrap().is_some());
    assert!(!output.to_string().contains("rotated-refresh"));
    assert!(!output.to_string().contains(".signature"));
    let requests = api.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/auth/session");
    assert!(requests[0].headers["authorization"].starts_with("Bearer ey"));
    api.assert_complete();
}

#[tokio::test]
async fn cached_login_does_not_restart_provider_authorization() {
    let api = IntentApiFixture::new(vec![
        metadata(120, "principal_one"),
        metadata(120, "principal_one"),
    ]);
    let tokens = serde_json::from_value(tokens(120, "user_one")).unwrap();
    let (saved, _) = renewal::accept(&api.origin, config(), tokens, None)
        .await
        .unwrap();
    let store = MemoryStore::default();
    renewal::save(&store, &saved).unwrap();
    let provider = MockTransport::with(vec![]);
    let output = login_with(&store, &api.origin, config(), &provider.provider())
        .await
        .unwrap();
    assert_eq!(output["data"]["principal_id"], "principal_one");
    assert!(output["data"]["refresh_expires_at"].is_string());
    assert!(provider.requests.lock().unwrap().is_empty());
    api.assert_complete();
}

#[tokio::test]
async fn device_login_polls_the_provider_and_never_the_old_broker() {
    let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
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
    let output = login_with(&store, &api.origin, config(), &transport.provider())
        .await
        .unwrap();
    assert_eq!(output["data"]["principal_id"], "principal_one");
    assert_eq!(transport.requests.lock().unwrap().len(), 4);
    assert_eq!(api.requests()[0].path, "/v1/auth/session");
    api.assert_complete();
}

#[tokio::test]
async fn rejected_resource_identity_never_becomes_a_cached_session() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        401,
        json!({"type":"unauthenticated"}),
    )]);
    let tokens = serde_json::from_value(tokens(120, "user_one")).unwrap();
    assert!(
        renewal::accept(&api.origin, config(), tokens, None)
            .await
            .is_err()
    );
    api.assert_complete();
}

#[tokio::test]
async fn device_deadline_prevents_a_poll_or_resource_request_after_expiry() {
    let api = IntentApiFixture::new(vec![]);
    let transport = MockTransport::with(vec![(
        200,
        json!({"device_code":"device-secret","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":1,"interval":5}),
    )]);
    let store = MemoryStore::default();
    let error = login_with(&store, &api.origin, config(), &transport.provider())
        .await
        .unwrap_err();
    assert_eq!(error.body["type"], "device_expired");
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
    assert!(store.0.borrow().is_empty());
    assert!(api.requests().is_empty());
}

#[tokio::test]
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
    let transport = MockTransport::with(vec![
        (
            200,
            json!({"device_code":"device-secret","user_code":"ABCD","verification_uri":"https://auth.example/device","expires_in":30,"interval":5}),
        ),
        (200, tokens(120, "user_one")),
    ]);
    let error = login_with(&RefusingStore, &api.origin, config(), &transport.provider())
        .await
        .unwrap_err();
    assert_eq!(error.body["type"], "keychain_unavailable");
    assert_eq!(api.requests()[1].path, "/v1/auth/logout");
    assert!(api.requests()[1].headers["authorization"].starts_with("Bearer ey"));
    api.assert_complete();
}
