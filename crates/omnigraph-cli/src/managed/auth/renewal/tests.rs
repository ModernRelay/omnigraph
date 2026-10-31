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
    for (transient, verification_failure) in [
        (None, None),
        (Some(408), None),
        (Some(429), None),
        (Some(503), None),
        (None, Some(408)),
        (None, Some(429)),
        (None, Some(502)),
        (None, Some(503)),
    ] {
        let mut replies = Vec::new();
        if let Some(status) = verification_failure {
            replies.push(if status == 502 {
                IntentReply {
                    status,
                    headers: Vec::new(),
                    body: b"<html>Bad Gateway</html>".to_vec(),
                }
            } else if status == 503 {
                IntentReply::json(status, json!({"message":"temporarily unavailable"}))
            } else {
                IntentReply::json(status, json!({"type":"temporarily_unavailable"}))
            });
        }
        replies.push(metadata(120, "principal_one"));
        let api = IntentApiFixture::new(replies);
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            let store = MemoryStore::default();
            let mut original = saved(&api.origin, -1);
            original.refresh_expires_at = (OffsetDateTime::now_utc() + time::Duration::hours(8))
                .format(&Rfc3339)
                .unwrap();
            save(&store, &original).unwrap();
            let mut responses = Vec::new();
            if let Some(status) = transient {
                responses.push((status, json!({"error":"unavailable"})));
            }
            responses.push((200, tokens(120, "user_one")));
            let mut transport = MockTransport::with(responses);
            if transient == Some(503) {
                std::sync::Arc::get_mut(&mut transport)
                    .unwrap()
                    .raw_error_body = Some(b"<html>Service Unavailable</html>".to_vec());
            }
            let token = credential_with(&store, &api.origin, &transport.provider())
                .await
                .unwrap();
            assert_ne!(token, original.access_token);
            let next = load(&store, &api.origin).unwrap().unwrap();
            assert_eq!(next.refresh_expires_at, original.refresh_expires_at);
            assert_eq!(next.refresh_token, "rotated-refresh");
            assert!(next.state == RefreshState::Ready);
            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests.len(), if transient.is_some() { 2 } else { 1 });
            assert!(
                requests
                    .iter()
                    .all(|request| request.body == requests[0].body)
            );
            let verification = api.requests();
            assert_eq!(
                verification.len(),
                if verification_failure.is_some() { 2 } else { 1 }
            );
            assert!(verification.iter().all(|request| {
                request.path == "/v1/auth/session"
                    && request.headers["authorization"] == verification[0].headers["authorization"]
            }));
            api.assert_complete();
        })
        .await;
    }
}

#[tokio::test]
async fn retry_backoff_refuses_ambiguous_headers_and_attempts_outside_the_budget() {
    use reqwest::header::HeaderValue;
    let future = (OffsetDateTime::now_utc() + time::Duration::days(1))
        .format(&time::format_description::well_known::Rfc2822)
        .unwrap();
    let delay = retry_delay(
        &[HeaderValue::from_str(&future).unwrap()],
        Duration::from_millis(250),
    )
    .unwrap();
    assert!(delay > Duration::from_secs(23 * 3600) && delay <= Duration::from_secs(24 * 3600));
    assert_eq!(
        retry_delay(
            &[HeaderValue::from_static("Sun, 06 Nov 1994 08:49:37 GMT")],
            Duration::from_millis(250)
        ),
        Some(Duration::from_millis(250))
    );
    for (attempt, values, budget) in [
        (2, vec![], Duration::from_secs(20)),
        (0, vec![], Duration::from_secs(6)),
        (0, vec!["20"], Duration::from_secs(20)),
        (0, vec!["18446744073709551615"], Duration::from_secs(20)),
        (
            0,
            vec!["Thu, 01 Jan 2099 00:00:00 GMT"],
            Duration::from_secs(20),
        ),
        (0, vec!["invalid"], Duration::from_secs(20)),
        (0, vec!["0", "0"], Duration::from_secs(20)),
    ] {
        let headers = values
            .iter()
            .map(|value| HeaderValue::from_str(value).unwrap())
            .collect::<Vec<_>>();
        assert!(!retry_pause(attempt, &headers, Instant::now() + budget, REFRESH_ATTEMPT).await);
    }
}

#[tokio::test]
async fn verification_retry_after_outside_budget_preserves_original_custody() {
    let mut retry = IntentReply::json(429, json!({"type":"temporarily_unavailable"}));
    retry.headers.push(("Retry-After".into(), "30".into()));
    let api = IntentApiFixture::new(vec![retry]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        let original = saved(&api.origin, -1);
        save(&store, &original).unwrap();
        let transport = MockTransport::with(vec![(200, tokens(120, "user_one"))]);
        assert_eq!(
            credential_with(&store, &api.origin, &transport.provider())
                .await
                .unwrap_err()
                .body["type"],
            "refresh_outcome_unknown"
        );
        let retained = load(&store, &api.origin).unwrap().unwrap();
        assert_eq!(retained.refresh_token, original.refresh_token);
        assert_eq!(retained.refresh_expires_at, original.refresh_expires_at);
        assert!(retained.state == RefreshState::Pending);
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn unknown_exchange_keeps_pending_and_never_replays_the_refresh() {
    let store = MemoryStore::default();
    let origin = "https://api.example";
    save(&store, &saved(origin, -1)).unwrap();
    let transport = MockTransport::with(vec![
        (
            503,
            json!({"error":"unavailable","message":"old-refresh"}),
        );
        3
    ]);
    for _ in 0..2 {
        let error = credential_with(&store, origin, &transport.provider())
            .await
            .unwrap_err();
        assert_eq!(error.body["type"], "refresh_outcome_unknown");
        assert!(!error.body.to_string().contains("old-refresh"));
    }
    assert!(load(&store, origin).unwrap().unwrap().state == RefreshState::Pending);
    assert_eq!(transport.requests.lock().unwrap().len(), 3);
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
    let api = IntentApiFixture::new(vec![
        IntentReply::json(503, json!({"type":"temporarily_unavailable"})),
        metadata(120, "other_principal"),
    ]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
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
    })
    .await;
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
    other = original.clone();
    other.identity.session_id = "another_session".into();
    assert_ne!(key(&original).unwrap(), key(&other).unwrap());
}

#[test]
fn replacing_a_human_requires_confirmation_of_the_new_verified_principal() {
    let origin = "https://api.example";
    let store = MemoryStore::default();
    let original = saved(origin, 120);
    save(&store, &original).unwrap();
    let mut other = original.clone();
    other.identity.principal_id = "principal_other".into();
    assert!(check_replacement(&store, &other, None).is_err());
    assert!(check_replacement(&store, &other, Some("principal_one")).is_err());
    check_replacement(&store, &other, Some("principal_other")).unwrap();
    check_replacement(&store, &original, None).unwrap();
    assert_eq!(
        load(&store, origin).unwrap().unwrap().identity.principal_id,
        "principal_one"
    );
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
    over.refresh_expires_at = (OffsetDateTime::now_utc() + time::Duration::days(29))
        .format(&Rfc3339)
        .unwrap();
    parse(&serde_json::to_string(&over).unwrap(), &over.origin).unwrap();
    over.refresh_expires_at = (OffsetDateTime::now_utc() + time::Duration::days(31))
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

struct RefusingStore {
    inner: MemoryStore,
    fail_state: Option<RefreshState>,
    reads: std::cell::RefCell<std::collections::BTreeMap<String, usize>>,
    writes: std::cell::RefCell<std::collections::BTreeMap<String, usize>>,
}

impl RefusingStore {
    fn new(inner: MemoryStore, fail_state: Option<RefreshState>) -> Self {
        Self {
            inner,
            fail_state,
            reads: Default::default(),
            writes: Default::default(),
        }
    }
}

impl Store for RefusingStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        *self.reads.borrow_mut().entry(key.into()).or_default() += 1;
        self.inner.get(key)
    }
    fn put(&self, key: &str, value: &str) -> Result<()> {
        *self.writes.borrow_mut().entry(key.into()).or_default() += 1;
        if self.fail_state.as_ref().is_some_and(|state| {
            serde_json::from_str::<Session>(value).is_ok_and(|saved| saved.state == *state)
        }) {
            return Err(Failure::refused("keychain_unavailable", "fixture"));
        }
        self.inner.put(key, value)
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key)
    }
}

#[tokio::test]
async fn pending_marker_failure_prevents_the_first_provider_effect() {
    let origin = "https://api.example";
    let store = MemoryStore::default();
    let original = saved(origin, 10);
    let selected = key(&original).unwrap();
    save(&store, &original).unwrap();
    let refusing = RefusingStore::new(store, Some(RefreshState::Pending));
    let transport = MockTransport::with(vec![]);
    assert_eq!(
        credential_with(&refusing, origin, &transport.provider())
            .await
            .unwrap_err()
            .body["type"],
        "keychain_unavailable"
    );
    assert!(transport.requests.lock().unwrap().is_empty());
    assert_eq!(refusing.reads.borrow()[&selected], 1);
    assert_eq!(refusing.writes.borrow()[&selected], 1);
    assert!(load(&refusing.inner, origin).unwrap().unwrap().state == RefreshState::Ready);
}

#[tokio::test]
async fn selection_reads_one_session_and_next_credential_access_observes_other_writer() {
    let api = IntentApiFixture::new(vec![
        metadata(120, "principal_one"),
        metadata(120, "principal_one"),
    ]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let raw = MemoryStore::default();
        let mut original = saved(&api.origin, 180);
        let selected = key(&original).unwrap();
        save(&raw, &original).unwrap();
        let store = RefusingStore::new(raw, None);
        super::super::select_human_with(&store, &api.origin, Some("org_test"))
            .await
            .unwrap();
        assert_eq!(store.reads.borrow()[&api.origin], 1);
        assert_eq!(store.reads.borrow()[&selected], 1);
        assert!(store.writes.borrow().is_empty());
        let transport = MockTransport::with(vec![]);
        super::super::login_with(
            &store,
            &api.origin,
            config(),
            &transport.provider(),
            Some("org_test"),
            &mut Vec::new(),
        )
        .await
        .unwrap();
        assert_eq!(store.reads.borrow()[&api.origin], 2);
        assert_eq!(store.reads.borrow()[&selected], 2);
        assert!(transport.requests.lock().unwrap().is_empty());
        original.state = RefreshState::LoginRequired;
        save(&store.inner, &original).unwrap();
        let failure = credential(&store, &api.origin, &original.identity)
            .await
            .unwrap_err();
        assert_eq!(failure.body["type"], "login_required");
        assert_eq!(store.reads.borrow()[&api.origin], 3);
        assert_eq!(store.reads.borrow()[&selected], 3);
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn renewed_credential_write_failure_propagates_and_keeps_pending_without_replay() {
    let api = IntentApiFixture::new(vec![metadata(120, "principal_one")]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let raw = MemoryStore::default();
        save(&raw, &saved(&api.origin, 10)).unwrap();
        let store = RefusingStore::new(raw, Some(RefreshState::Ready));
        let transport = MockTransport::with(vec![(200, tokens(120, "user_one"))]);
        let failure = credential_with(&store, &api.origin, &transport.provider())
            .await
            .unwrap_err();
        assert_eq!(failure.body["type"], "keychain_unavailable");
        let mut pending = load(&store.inner, &api.origin).unwrap().unwrap();
        assert!(pending.state == RefreshState::Pending);
        assert_eq!(store.writes.borrow()[&key(&pending).unwrap()], 2);
        pending.access_token = jwt(-1, "user_one");
        pending.expires_at = OffsetDateTime::from_unix_timestamp(
            claims(&pending.access_token, &pending.config, true)
                .unwrap()
                .exp,
        )
        .unwrap()
        .format(&Rfc3339)
        .unwrap();
        save(&store.inner, &pending).unwrap();
        let failure = credential_with(&store, &api.origin, &transport.provider())
            .await
            .unwrap_err();
        assert_eq!(failure.body["type"], "refresh_outcome_unknown");
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn logout_clears_local_custody_and_reports_unconfirmed_provider_revocation() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        503,
        json!({"type":"provider_unavailable"}),
    )]);
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        save(&store, &saved(&api.origin, 120)).unwrap();
        let result = logout(&store, &api.origin, None).await.unwrap();
        assert_eq!(result["data"]["provider_revocation_confirmed"], false);
        assert!(
            store
                .0
                .borrow()
                .keys()
                .all(|key| key.starts_with("session-owner:"))
        );
        api.assert_complete();
    })
    .await;
}

#[tokio::test]
async fn logout_deletes_local_custody_before_a_frozen_transport_failure() {
    for proxy_conflict in [false, true] {
        let api = IntentApiFixture::new(vec![]);
        let directory = tempfile::tempdir().unwrap();
        let missing_ca = directory.path().join("missing.pem");
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            crate::operator_routing::test_broken_control_transport_scope(
                &api.origin,
                missing_ca.clone(),
                proxy_conflict,
                async {
                    let sessions = MemoryStore::default();
                    save(&sessions, &saved(&api.origin, 120)).unwrap();
                    let data = MemoryStore::default();
                    let index = super::super::DataIndex {
                        profile: "test-profile".into(),
                    };
                    index
                        .put(
                            &data,
                            &format!(
                                "test-profile:binding:{}:generation:0:credential:cluster-cache",
                                "a".repeat(64)
                            ),
                            "derived-credential",
                        )
                        .unwrap();
                    let cli =
                        crate::invocation::parse_from(["omnigraph", "logout", "--profile", "work"])
                            .unwrap()
                            .cli;
                    let target = crate::targets::ResolvedTarget::new(
                        crate::targets::TargetBinding::Managed(crate::managed::Binding {
                            api: api.origin.clone(),
                            organization_id: Some("org_test".into()),
                            cluster: None,
                            control_auth: crate::managed::ControlAuth::HumanDevice,
                        }),
                        crate::targets::TargetProvenance::Explicit {
                            name: "work".into(),
                        },
                    );
                    crate::operator_routing::freeze_transports(&cli, Some(&target)).unwrap();
                    if !proxy_conflict {
                        std::fs::copy(crate::tls_gateway::ca_path(), &missing_ca).unwrap();
                    }
                    let result = super::super::logout_after_data_cleanup(
                        &sessions,
                        &api.origin,
                        Some("org_test"),
                        || index.clear(&data),
                    )
                    .await
                    .unwrap();
                    assert_eq!(result["data"]["local_credentials_removed"], true);
                    assert_eq!(result["data"]["provider_revocation_confirmed"], false);
                    assert!(
                        load_selected(&sessions, &api.origin, Some("org_test"))
                            .unwrap()
                            .is_none()
                    );
                    assert!(data.0.borrow().is_empty());
                    assert!(api.requests().is_empty());
                },
            )
            .await;
        })
        .await;
        api.assert_complete();
    }
}

#[tokio::test]
async fn concurrent_refresh_waiter_observes_one_completed_rotation() {
    let api = IntentApiFixture::with_response_delay(
        vec![metadata(120, "principal_one")],
        Duration::from_millis(75),
    );
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
        let store = MemoryStore::default();
        save(&store, &saved(&api.origin, -1)).unwrap();
        let transport = MockTransport::with(vec![(200, tokens(120, "user_one"))]);
        let provider = transport.provider();
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("locks");
        let run = || async {
            let _lock = super::super::coordination::lock_at(
                &directory,
                &api.origin,
                Duration::from_secs(2),
            )
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
    })
    .await;
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
    crate::operator_routing::test_transport_scope(&[&api.origin], async {
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
    })
    .await;
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

#[tokio::test]
async fn two_profiles_for_the_same_human_keep_independent_refresh_custody() {
    for (principal, subject) in [
        ("principal_one", "user_one"),
        ("principal_other", "user_other"),
    ] {
        struct BoundStore(MemoryStore);
        impl Store for BoundStore {
            fn session_owner(&self, session: &str) -> Result<Option<String>> {
                self.0.session_owner(session)
            }
            fn claim_session(&self, session: &str, owner: &str) -> Result<()> {
                self.0.claim_session(session, owner)
            }
            fn get(&self, key: &str) -> Result<Option<String>> {
                self.0.get(&crate::operator_routing::namespace(key))
            }
            fn put(&self, key: &str, value: &str) -> Result<()> {
                self.0.put(&crate::operator_routing::namespace(key), value)
            }
            fn remove(&self, key: &str) -> Result<()> {
                self.0.remove(&crate::operator_routing::namespace(key))
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let input = crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
        let origin = "https://api.example";
        let first = crate::operator_routing::registration_auth(&input, "first", origin).unwrap();
        let second = crate::operator_routing::registration_auth(&input, "second", origin).unwrap();
        let store = BoundStore(MemoryStore::default());
        crate::operator_routing::AUTH
            .scope(Some(first.clone()), async {
                let mut session = saved(origin, 120);
                session.refresh_token = "first-refresh".into();
                save(&store, &session).unwrap();
            })
            .await;
        crate::operator_routing::AUTH
            .scope(Some(second.clone()), async {
                assert!(load(&store, origin).unwrap().is_none());
                let reused = serde_json::from_value(tokens(120, "user_one")).unwrap();
                assert!(check_device_session(&store, origin, &config(), &reused).is_err());
                let mut session = saved(origin, 120);
                session.identity.session_id = "session_two".into();
                session.identity.principal_id = principal.into();
                session.identity.subject = subject.into();
                let parts: Vec<_> = session.access_token.split('.').collect();
                let mut claims: Value =
                    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
                claims["sid"] = json!("session_two");
                claims["sub"] = json!(subject);
                session.access_token = format!(
                    "{}.{}.signature",
                    parts[0],
                    URL_SAFE_NO_PAD.encode(claims.to_string())
                );
                session.refresh_token = "second-refresh".into();
                save(&store, &session).unwrap();
            })
            .await;
        crate::operator_routing::AUTH
            .scope(Some(first), async {
                let mut session = load(&store, origin).unwrap().unwrap();
                assert_eq!(session.refresh_token, "first-refresh");
                session.state = RefreshState::Pending;
                save(&store, &session).unwrap();
            })
            .await;
        crate::operator_routing::AUTH
            .scope(Some(second), async {
                let session = load(&store, origin).unwrap().unwrap();
                assert_eq!(session.refresh_token, "second-refresh");
                assert!(session.state == RefreshState::Ready);
                assert_eq!(session.identity.principal_id, principal);
            })
            .await;
    }
}
