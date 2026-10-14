use super::*;
use crate::managed::auth::tests::{MemoryStore, MockTransport, config, jwt, metadata, tokens};
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};

fn saved(origin: &str, seconds: i64) -> Session {
    let expires =
        OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + seconds)
            .unwrap();
    Session {
        version: 2,
        origin: origin.into(),
        config: config(),
        identity: Identity {
            principal_id: "principal_one".into(),
            account_id: "org_test".into(),
            subject: "user_one".into(),
            session_id: "session_one".into(),
        },
        access_token: jwt(seconds, "user_one"),
        expires_at: expires.format(&Rfc3339).unwrap(),
        refresh_token: "old-refresh".into(),
        refresh_expires_at: (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&Rfc3339)
            .unwrap(),
        state: RefreshState::Ready,
    }
}

#[tokio::test]
async fn refresh_rotates_one_keychain_record_after_resource_verifies_same_identity() {
    let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
    let store = MemoryStore::default();
    let original = saved(&api.origin, -1);
    save(&store, &original).unwrap();
    let transport = MockTransport::with(vec![(200, tokens(120, "user_one"))]);
    let token = credential_with(&store, &api.origin, &transport.provider())
        .await
        .unwrap();
    assert_ne!(token, original.access_token);
    let next = load(&store, &api.origin).unwrap().unwrap();
    assert_eq!(next.refresh_expires_at, original.refresh_expires_at);
    assert_eq!(next.refresh_token, "rotated-refresh");
    assert!(next.state == RefreshState::Ready);
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
    api.assert_complete();
}

#[tokio::test]
async fn unknown_exchange_keeps_pending_and_never_replays_the_refresh() {
    let store = MemoryStore::default();
    let origin = "https://api.example";
    save(&store, &saved(origin, -1)).unwrap();
    let transport = MockTransport::with(vec![(
        503,
        json!({"error":"unavailable","message":"old-refresh"}),
    )]);
    for _ in 0..2 {
        let error = credential_with(&store, origin, &transport.provider())
            .await
            .unwrap_err();
        assert_eq!(error.body["type"], "refresh_outcome_unknown");
        assert!(!error.body.to_string().contains("old-refresh"));
    }
    assert!(load(&store, origin).unwrap().unwrap().state == RefreshState::Pending);
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn invalid_refresh_is_terminal_and_cannot_reuse_still_valid_access() {
    let store = MemoryStore::default();
    let origin = "https://api.example";
    save(&store, &saved(origin, 20)).unwrap();
    let transport = MockTransport::with(vec![(400, json!({"error":"invalid_grant"}))]);
    assert_eq!(
        credential_with(&store, origin, &transport.provider())
            .await
            .unwrap_err()
            .body["type"],
        "login_required"
    );
    assert_eq!(
        credential_with(&store, origin, &transport.provider())
            .await
            .unwrap_err()
            .body["type"],
        "login_required"
    );
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn identity_drift_cannot_replace_cached_credentials_after_provider_consumption() {
    let api = IntentApiFixture::new(vec![metadata(120, "other_principal")]);
    let store = MemoryStore::default();
    save(&store, &saved(&api.origin, -1)).unwrap();
    let transport = MockTransport::with(vec![(200, tokens(120, "user_one"))]);
    assert_eq!(
        credential_with(&store, &api.origin, &transport.provider())
            .await
            .unwrap_err()
            .body["type"],
        "refresh_outcome_unknown"
    );
    let retained = load(&store, &api.origin).unwrap().unwrap();
    assert_eq!(retained.identity.principal_id, "principal_one");
    assert_eq!(retained.refresh_token, "old-refresh");
    assert!(retained.state == RefreshState::Pending);
    api.assert_complete();
}

#[test]
fn full_profile_cache_binding_rejects_cross_client_principal_and_origin_reuse() {
    let store = MemoryStore::default();
    let origin = "https://api.example";
    let original = saved(origin, 120);
    save(&store, &original).unwrap();
    let selected = store.get(origin).unwrap().unwrap();
    store.put("https://other.example", &selected).unwrap();
    assert!(load(&store, "https://other.example").is_err());
    let mut other = original.clone();
    other.config.client_id = "client_other".into();
    assert_ne!(key(&original).unwrap(), key(&other).unwrap());
    assert!(save(&store, &other).is_err());
    other = original.clone();
    other.identity.principal_id = "principal_other".into();
    assert_ne!(key(&original).unwrap(), key(&other).unwrap());
}

#[test]
fn access_and_absolute_session_bounds_reject_malformed_and_expired_profiles() {
    let original = saved("https://api.example", 120);
    let raw = serde_json::to_string(&original).unwrap();
    parse(&raw, &original.origin).unwrap();
    for field in ["client_id", "issuer", "organization_id"] {
        let mut value = serde_json::to_value(&original).unwrap();
        value["config"][field] = json!("unexpected");
        assert!(parse(&value.to_string(), &original.origin).is_err());
    }
    let mut over = original.clone();
    over.refresh_expires_at = (OffsetDateTime::now_utc() + time::Duration::hours(9))
        .format(&Rfc3339)
        .unwrap();
    assert!(parse(&serde_json::to_string(&over).unwrap(), &over.origin).is_err());
    assert!(parse("opaque-legacy-token", &original.origin).is_err());
}

#[tokio::test]
async fn absolute_expiry_does_not_call_provider_even_when_access_would_be_valid() {
    let origin = "https://api.example";
    let mut record = saved(origin, -1);
    record.refresh_expires_at = record.expires_at.clone();
    let store = MemoryStore::default();
    save(&store, &record).unwrap();
    let transport = MockTransport::with(vec![]);
    assert_eq!(
        credential_with(&store, origin, &transport.provider())
            .await
            .unwrap_err()
            .body["type"],
        "login_required"
    );
    assert!(transport.requests.lock().unwrap().is_empty());
}

struct RefusingStore(MemoryStore);
impl Store for RefusingStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.0.get(key)
    }
    fn put(&self, _: &str, _: &str) -> Result<()> {
        Err(Failure::refused("keychain_unavailable", "fixture"))
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.0.remove(key)
    }
}

#[tokio::test]
async fn pending_marker_failure_prevents_the_first_provider_effect() {
    let origin = "https://api.example";
    let store = MemoryStore::default();
    save(&store, &saved(origin, -1)).unwrap();
    let refusing = RefusingStore(store);
    let transport = MockTransport::with(vec![]);
    assert_eq!(
        credential_with(&refusing, origin, &transport.provider())
            .await
            .unwrap_err()
            .body["type"],
        "keychain_unavailable"
    );
    assert!(transport.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn logout_clears_local_custody_and_reports_unconfirmed_provider_revocation() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        503,
        json!({"type":"provider_unavailable"}),
    )]);
    let store = MemoryStore::default();
    save(&store, &saved(&api.origin, 120)).unwrap();
    let result = logout(&store, &api.origin, None).await.unwrap();
    assert_eq!(result["data"]["provider_revocation_confirmed"], false);
    assert!(store.0.borrow().is_empty());
    api.assert_complete();
}

#[tokio::test]
async fn concurrent_refresh_waiter_observes_one_completed_rotation() {
    let api = IntentApiFixture::with_response_delay(
        vec![metadata(120, "principal_one")],
        Duration::from_millis(75),
    );
    let store = MemoryStore::default();
    save(&store, &saved(&api.origin, -1)).unwrap();
    let transport = MockTransport::with(vec![(200, tokens(120, "user_one"))]);
    let provider = transport.provider();
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("locks");
    let run = || async {
        let _lock =
            super::super::coordination::lock_at(&directory, &api.origin, Duration::from_secs(2))
                .await
                .unwrap();
        credential_with(&store, &api.origin, &provider)
            .await
            .unwrap()
    };
    let (first, second) = tokio::join!(run(), run());
    assert_eq!(first, second);
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
    api.assert_complete();
}

fn in_organization(mut session: Session, organization: &str) -> Session {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let parts: Vec<_> = session.access_token.split('.').collect();
    let mut claims: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    claims["org_id"] = json!(organization);
    session.access_token = format!(
        "{}.{}.{}",
        parts[0],
        URL_SAFE_NO_PAD.encode(claims.to_string()),
        parts[2]
    );
    session.identity.account_id = organization.into();
    session.refresh_token = format!("refresh-{organization}");
    session
}

#[tokio::test]
async fn organization_custody_is_separate_and_logout_preserves_the_other_session() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        200,
        json!({"data":{"logged_out":true},"meta":{}}),
    )]);
    let store = MemoryStore::default();
    let first = in_organization(saved(&api.origin, 120), "org_one");
    let second = in_organization(saved(&api.origin, 120), "org_two");
    save(&store, &first).unwrap();
    save(&store, &second).unwrap();
    assert_eq!(
        load(&store, &api.origin).err().unwrap().body["type"],
        "organization_required"
    );
    assert_eq!(
        load_selected(&store, &api.origin, Some("org_one"))
            .unwrap()
            .unwrap()
            .refresh_token,
        "refresh-org_one"
    );
    assert_eq!(
        load_selected(&store, &api.origin, Some("org_two"))
            .unwrap()
            .unwrap()
            .refresh_token,
        "refresh-org_two"
    );
    assert!(logout(&store, &api.origin, None).await.is_err());
    assert!(api.requests().is_empty());
    let result = logout(&store, &api.origin, Some("org_one")).await.unwrap();
    assert_eq!(result["data"]["organization_id"], "org_one");
    assert_eq!(result["data"]["provider_revocation_confirmed"], true);
    assert!(
        load_selected(&store, &api.origin, Some("org_one"))
            .unwrap()
            .is_none()
    );
    let remaining = load(&store, &api.origin).unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(remaining).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    api.assert_complete();
}

#[tokio::test]
async fn invocation_pin_refuses_a_parallel_login_before_refresh_or_request() {
    let origin = "https://api.example";
    let store = MemoryStore::default();
    let first = saved(origin, -1);
    save(&store, &first).unwrap();
    let mut other = first.clone();
    other.identity.principal_id = "principal_other".into();
    save(&store, &other).unwrap();
    let transport = MockTransport::with(vec![]);
    let error = credential_selected(&store, origin, &first.identity, &transport.provider())
        .await
        .unwrap_err();
    assert_eq!(error.body["type"], "session_identity_mismatch");
    assert!(transport.requests.lock().unwrap().is_empty());
    assert_eq!(
        load(&store, origin).unwrap().unwrap().identity.principal_id,
        "principal_other"
    );
}

#[test]
fn legacy_singleton_index_and_context_cannot_select_an_organization() {
    let store = MemoryStore::default();
    store
        .put(
            "https://api.example",
            &format!("credential:{}", "a".repeat(64)),
        )
        .unwrap();
    assert!(load_selected(&store, "https://api.example", Some("org_test")).is_err());
}
