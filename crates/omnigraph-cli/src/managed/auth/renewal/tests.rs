use super::*;
use crate::managed::auth::tests::MemoryStore;
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};

fn expiry(seconds: i64) -> String {
    (OffsetDateTime::now_utc() + time::Duration::seconds(seconds))
        .format(&Rfc3339)
        .unwrap()
}

fn initial(seconds: i64) -> Renewable {
    Renewable {
        version: 2,
        access_token: "old-access-secret".into(),
        expires_at: expiry(seconds),
        refresh_token: "old-refresh-secret".into(),
        refresh_expires_at: expiry(7200),
        identity: Identity {
            principal_id: "principal-one".into(),
            subject: "subject-one".into(),
            account_id: "account-one".into(),
        },
        state: RefreshState::Ready,
    }
}

fn rotated(saved: &Renewable) -> Value {
    json!({"data":{"access_token":"new-access-secret","token_type":"Bearer","expires_at":expiry(120),"refresh_token":"new-refresh-secret","refresh_expires_at":saved.refresh_expires_at,"principal_id":saved.identity.principal_id,"subject":saved.identity.subject,"account_id":saved.identity.account_id,"scopes":{"actions":["read"]}},"meta":{"provenance":"service_db"}})
}

#[tokio::test]
async fn expired_access_rotates_once_then_reuses_the_exact_cached_pair() {
    let saved = initial(-1);
    let api = IntentApiFixture::new(vec![IntentReply::json(200, rotated(&saved))]);
    let legacy = MemoryStore::default();
    let current = MemoryStore::default();
    save(&current, &api.origin, &saved).unwrap();
    for _ in 0..2 {
        assert_eq!(
            credential(&legacy, &current, &api.origin).await.unwrap(),
            "new-access-secret"
        );
    }
    let requests = api.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/auth/refresh");
    assert_eq!(
        requests[0].headers["authorization"],
        "Bearer old-access-secret"
    );
    assert_eq!(
        requests[0].body,
        json!({"refresh_token":"old-refresh-secret"})
    );
    let next = parse(&current.get(&api.origin).unwrap().unwrap()).unwrap();
    assert_eq!(next.refresh_token, "new-refresh-secret");
    assert_eq!(next.refresh_expires_at, saved.refresh_expires_at);
    assert!(next.state == RefreshState::Ready);
    api.assert_complete();
}

#[tokio::test]
async fn pending_is_durable_before_submission_and_uncertain_exchange_never_repeats() {
    struct GuardedStore {
        inner: MemoryStore,
    }
    impl Store for GuardedStore {
        fn get(&self, origin: &str) -> Result<Option<String>> {
            self.inner.get(origin)
        }
        fn put(&self, origin: &str, value: &str) -> Result<()> {
            if parse(value)?.state == RefreshState::Pending {
                assert_eq!(parse(value)?.refresh_token, "old-refresh-secret");
            }
            self.inner.put(origin, value)
        }
        fn remove(&self, origin: &str) -> Result<()> {
            self.inner.remove(origin)
        }
    }
    for (status, kind) in [
        (503, "refresh_outcome_unknown"),
        (409, "refresh_in_progress"),
        (503, "refresh_unavailable"),
    ] {
        let api = IntentApiFixture::new(vec![IntentReply::json(
            status,
            json!({"type":kind,"detail":"old-access-secret old-refresh-secret","old-refresh-secret":"echo","old-access-secret":"echo"}),
        )]);
        let current = GuardedStore {
            inner: MemoryStore::default(),
        };
        save(&current, &api.origin, &initial(-1)).unwrap();
        let failure = credential(&MemoryStore::default(), &current, &api.origin)
            .await
            .unwrap_err();
        assert!(!failure.body.to_string().contains("secret"));
        let persisted = parse(&current.get(&api.origin).unwrap().unwrap()).unwrap();
        assert!(persisted.state == RefreshState::Pending);
        assert_eq!(persisted.access_token, "old-access-secret");
        assert_eq!(
            credential(&MemoryStore::default(), &current, &api.origin)
                .await
                .unwrap_err()
                .body["type"],
            "refresh_outcome_unknown"
        );
        api.assert_complete();
    }
}

#[tokio::test]
async fn only_an_exact_no_exchange_refusal_allows_a_later_refresh() {
    let saved = initial(-1);
    let api = IntentApiFixture::new(vec![
        IntentReply::json(
            503,
            json!({"type":"refresh_unavailable","exchange_started":false}),
        ),
        IntentReply::json(200, rotated(&saved)),
    ]);
    let current = MemoryStore::default();
    save(&current, &api.origin, &saved).unwrap();
    assert_eq!(
        credential(&MemoryStore::default(), &current, &api.origin)
            .await
            .unwrap_err()
            .body["type"],
        "refresh_unavailable"
    );
    assert!(
        parse(&current.get(&api.origin).unwrap().unwrap())
            .unwrap()
            .state
            == RefreshState::Ready
    );
    assert_eq!(
        credential(&MemoryStore::default(), &current, &api.origin)
            .await
            .unwrap(),
        "new-access-secret"
    );
    api.assert_complete();
}

#[tokio::test]
async fn unknown_refresh_keeps_unexpired_access_usable_without_another_exchange() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        503,
        json!({"type":"refresh_outcome_unknown"}),
    )]);
    let current = MemoryStore::default();
    save(&current, &api.origin, &initial(25)).unwrap();
    for _ in 0..2 {
        assert_eq!(
            credential(&MemoryStore::default(), &current, &api.origin)
                .await
                .unwrap(),
            "old-access-secret"
        );
    }
    api.assert_complete();
}

#[tokio::test]
async fn refresh_requires_same_identity_rotated_secrets_and_original_absolute_deadline() {
    for (field, altered) in [
        ("principal_id", json!("another-principal")),
        ("subject", json!("another-subject")),
        ("account_id", json!("another-account")),
        ("refresh_expires_at", json!(expiry(7201))),
        ("refresh_token", json!("old-refresh-secret")),
        ("access_token", json!("old-access-secret")),
        ("expires_at", json!(expiry(1000))),
    ] {
        let saved = initial(-1);
        let mut response = rotated(&saved);
        response["data"][field] = altered;
        let api = IntentApiFixture::new(vec![IntentReply::json(200, response)]);
        let current = MemoryStore::default();
        save(&current, &api.origin, &saved).unwrap();
        let failure = credential(&MemoryStore::default(), &current, &api.origin)
            .await
            .unwrap_err();
        assert_eq!(failure.body["type"], "refresh_outcome_unknown");
        assert!(
            parse(&current.get(&api.origin).unwrap().unwrap())
                .unwrap()
                .state
                == RefreshState::Pending
        );
        api.assert_complete();
    }
}

#[tokio::test]
async fn terminal_revocation_does_not_fallback_to_legacy_or_open_device_flow() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        401,
        json!({"type":"login_required"}),
    )]);
    let current = MemoryStore::default();
    let legacy = MemoryStore::default();
    legacy
        .put(
            &api.origin,
            &super::super::tests::saved("legacy-secret", 120),
        )
        .unwrap();
    save(&current, &api.origin, &initial(-1)).unwrap();
    for _ in 0..2 {
        assert_eq!(
            credential(&legacy, &current, &api.origin)
                .await
                .unwrap_err()
                .body["type"],
            "login_required"
        );
    }
    assert!(
        parse(&current.get(&api.origin).unwrap().unwrap())
            .unwrap()
            .state
            == RefreshState::LoginRequired
    );
    api.assert_complete();
}

#[tokio::test]
async fn renewal_uses_its_own_deadline_instead_of_the_ordinary_read_deadline() {
    let saved = initial(-1);
    let api = IntentApiFixture::with_response_delay(
        vec![IntentReply::json(200, rotated(&saved))],
        Duration::from_millis(10_100),
    );
    let current = MemoryStore::default();
    save(&current, &api.origin, &saved).unwrap();
    assert_eq!(
        credential(&MemoryStore::default(), &current, &api.origin)
            .await
            .unwrap(),
        "new-access-secret"
    );
    api.assert_complete();
}

#[test]
fn renewable_login_stores_only_credentials_and_verified_identity_not_arbitrary_metadata() {
    let saved = initial(120);
    let mut body = rotated(&saved);
    body["meta"]["extra"] = json!("new-access-secret new-refresh-secret");
    let legacy = MemoryStore::default();
    let current = MemoryStore::default();
    let public = store_login(&legacy, &current, "https://one.example", &body).unwrap();
    assert!(!public.to_string().contains("secret"));
    let raw = current.get("https://one.example").unwrap().unwrap();
    assert!(!raw.contains("extra"));
    assert!(raw.contains("new-refresh-secret"));
    assert!(legacy.get("https://one.example").unwrap().is_none());
}

#[test]
fn first_login_accepts_a_small_server_clock_offset_without_rewriting_expiry() {
    let saved = initial(120);
    let mut body = rotated(&saved);
    body["data"]["refresh_expires_at"] = json!(expiry(8 * 60 * 60 + 15));
    let current = MemoryStore::default();
    let public = store_login(
        &MemoryStore::default(),
        &current,
        "https://one.example",
        &body,
    )
    .unwrap();
    assert_eq!(
        public["data"]["refresh_expires_at"],
        body["data"]["refresh_expires_at"]
    );
}

#[test]
fn lifetime_metadata_clock_tolerance_has_exact_bounds_and_never_extends_expiry() {
    let now = OffsetDateTime::UNIX_EPOCH + time::Duration::days(20_000);
    for skew in [
        time::Duration::milliseconds(100),
        time::Duration::milliseconds(200),
        time::Duration::seconds(30),
        time::Duration::seconds(30) + time::Duration::nanoseconds(1),
    ] {
        let allowed = skew <= time::Duration::seconds(30);
        let access_expiry = (now + time::Duration::minutes(15) + skew)
            .format(&Rfc3339)
            .unwrap();
        let access = serde_json::to_string(&Session {
            version: 1,
            access_token: "access-secret".into(),
            expires_at: access_expiry.clone(),
        })
        .unwrap();
        let result = session_at(&access, now);
        assert_eq!(result.is_ok(), allowed);
        if let Ok(session) = result {
            assert_eq!(session.expires_at, access_expiry);
        }

        let mut saved = initial(120);
        saved.expires_at = (now + time::Duration::minutes(2)).format(&Rfc3339).unwrap();
        saved.refresh_expires_at = (now + time::Duration::hours(8) + skew)
            .format(&Rfc3339)
            .unwrap();
        let result = parse_at(&serde_json::to_string(&saved).unwrap(), now);
        assert_eq!(result.is_ok(), allowed);
        if let Ok(record) = result {
            assert_eq!(record.refresh_expires_at, saved.refresh_expires_at);
        }
        saved.refresh_expires_at = (now + time::Duration::hours(1)).format(&Rfc3339).unwrap();
        saved.expires_at = access_expiry;
        assert_eq!(
            parse_at(&serde_json::to_string(&saved).unwrap(), now).is_ok(),
            allowed
        );
    }
    for offset in [time::Duration::ZERO, -time::Duration::nanoseconds(1)] {
        let access = serde_json::to_string(&Session {
            version: 1,
            access_token: "expired-secret".into(),
            expires_at: (now + offset).format(&Rfc3339).unwrap(),
        })
        .unwrap();
        assert_eq!(
            session_at(&access, now).err().unwrap().body["type"],
            "login_required"
        );
    }
}

#[tokio::test]
async fn cached_login_accepts_clock_skewed_metadata_without_changing_absolute_deadlines() {
    let mut saved = initial(15 * 60 + 15);
    saved.refresh_expires_at = expiry(8 * 60 * 60 + 15);
    let response = json!({"data":{"principal_id":saved.identity.principal_id,"subject":saved.identity.subject,"account_id":saved.identity.account_id,"expires_at":saved.expires_at,"refresh_expires_at":saved.refresh_expires_at,"scopes":{}},"meta":{}});
    let api = IntentApiFixture::new(vec![IntentReply::json(200, response)]);
    let current = MemoryStore::default();
    save(&current, &api.origin, &saved).unwrap();
    let original = current.get(&api.origin).unwrap().unwrap();
    let public = cached_login(&MemoryStore::default(), &current, &api.origin)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(public["data"]["expires_at"], saved.expires_at);
    assert_eq!(
        public["data"]["refresh_expires_at"],
        saved.refresh_expires_at
    );
    assert_eq!(current.get(&api.origin).unwrap().unwrap(), original);
    assert_eq!(api.requests()[0].path, "/v1/auth/session");
    api.assert_complete();
}

#[test]
fn renewable_validation_diagnostics_identify_the_condition_without_secret_metadata() {
    let now = OffsetDateTime::now_utc();
    let base = initial(120);
    for (field, value, detail) in [
        (
            "refresh_token",
            json!("refresh-secret\n"),
            "refresh credential must be",
        ),
        (
            "refresh_expires_at",
            json!("refresh-secret"),
            "timestamp is not valid RFC 3339",
        ),
        (
            "refresh_expires_at",
            json!(
                (now + time::Duration::hours(8) + time::Duration::seconds(31))
                    .format(&Rfc3339)
                    .unwrap()
            ),
            "refresh deadline exceeds",
        ),
        (
            "expires_at",
            json!(
                (now + time::Duration::minutes(15) + time::Duration::seconds(31))
                    .format(&Rfc3339)
                    .unwrap()
            ),
            "access expiry exceeds",
        ),
        (
            "refresh_expires_at",
            json!(
                (now + time::Duration::seconds(60))
                    .format(&Rfc3339)
                    .unwrap()
            ),
            "access expiry is later",
        ),
        ("version", json!(3), "version is unsupported"),
        (
            "identity",
            json!({"principal_id":"","subject":"subject-secret","account_id":"account-secret"}),
            "identity is invalid",
        ),
    ] {
        let mut value_json = serde_json::to_value(&base).unwrap();
        value_json[field] = value;
        let failure = parse_at(&value_json.to_string(), now).err().unwrap();
        assert_eq!(failure.body["type"], "credential_invalid");
        assert!(failure.body["detail"].as_str().unwrap().contains(detail));
        assert!(!failure.body.to_string().contains("secret"));
    }
}

#[tokio::test]
async fn logout_can_revoke_expired_access_and_preserves_pair_until_confirmed() {
    let api = IntentApiFixture::new(vec![
        IntentReply::json(
            503,
            json!({"type":"service_unavailable","detail":"old-refresh-secret"}),
        ),
        IntentReply::json(200, json!({"data":{"logged_out":true},"meta":{}})),
    ]);
    let current = MemoryStore::default();
    let legacy = MemoryStore::default();
    save(&current, &api.origin, &initial(-1)).unwrap();
    let original = current.get(&api.origin).unwrap().unwrap();
    let failure = logout(&legacy, &current, &api.origin).await.unwrap_err();
    assert_eq!(failure.body["local_credential_removed"], false);
    assert!(!failure.body.to_string().contains("old-refresh-secret"));
    assert_eq!(current.get(&api.origin).unwrap().unwrap(), original);
    logout(&legacy, &current, &api.origin).await.unwrap();
    assert!(current.get(&api.origin).unwrap().is_none());
    assert!(
        api.requests().iter().all(|r| r.path == "/v1/auth/logout"
            && r.headers["authorization"] == "Bearer old-access-secret")
    );
    api.assert_complete();
}

#[tokio::test]
async fn malformed_current_cache_never_falls_back_and_expired_family_never_refreshes() {
    let legacy = MemoryStore::default();
    let current = MemoryStore::default();
    let api = IntentApiFixture::new(vec![]);
    legacy
        .put(
            &api.origin,
            &super::super::tests::saved("legacy-secret", 120),
        )
        .unwrap();
    current.put(&api.origin, "invalid-secret").unwrap();
    assert_eq!(
        credential(&legacy, &current, &api.origin)
            .await
            .unwrap_err()
            .body["type"],
        "credential_invalid"
    );
    let mut expired = initial(-20);
    expired.refresh_expires_at = expiry(-10);
    save(&current, &api.origin, &expired).unwrap();
    assert_eq!(
        credential(&legacy, &current, &api.origin)
            .await
            .unwrap_err()
            .body["type"],
        "login_required"
    );
    api.assert_complete();
}

#[tokio::test]
async fn failure_to_persist_pending_refuses_before_the_provider_exchange() {
    struct Unwritable(MemoryStore);
    impl Store for Unwritable {
        fn get(&self, origin: &str) -> Result<Option<String>> {
            self.0.get(origin)
        }
        fn put(&self, _: &str, _: &str) -> Result<()> {
            Err(keychain_failed())
        }
        fn remove(&self, origin: &str) -> Result<()> {
            self.0.remove(origin)
        }
    }
    let api = IntentApiFixture::new(vec![]);
    let current = MemoryStore::default();
    save(&current, &api.origin, &initial(-1)).unwrap();
    let failure = credential(&MemoryStore::default(), &Unwritable(current), &api.origin)
        .await
        .unwrap_err();
    assert_eq!(failure.body["type"], "keychain_unavailable");
    api.assert_complete();
}

#[tokio::test]
async fn malformed_or_redirected_refresh_response_stays_uncertain_without_retry() {
    for reply in [
        IntentReply {
            status: 200,
            headers: vec![],
            body: b"not json".to_vec(),
        },
        IntentReply {
            status: 307,
            headers: vec![("Location".into(), "https://other.example".into())],
            body: vec![],
        },
    ] {
        let api = IntentApiFixture::new(vec![reply]);
        let current = MemoryStore::default();
        save(&current, &api.origin, &initial(-1)).unwrap();
        for _ in 0..2 {
            assert_eq!(
                credential(&MemoryStore::default(), &current, &api.origin)
                    .await
                    .unwrap_err()
                    .body["type"],
                "refresh_outcome_unknown"
            );
        }
        api.assert_complete();
    }
}

#[tokio::test]
async fn cached_login_checks_fresh_identity_and_preserves_cache_on_transient_failure() {
    for (status, response) in [
        (
            503,
            json!({"type":"service_unavailable","detail":"old-access-secret old-refresh-secret","old-refresh-secret":"echo","old-access-secret":"echo"}),
        ),
        (
            200,
            json!({"data":{"principal_id":"different","subject":"subject-one","account_id":"account-one","expires_at":expiry(120),"scopes":{}},"meta":{}}),
        ),
    ] {
        let api = IntentApiFixture::new(vec![IntentReply::json(status, response)]);
        let current = MemoryStore::default();
        save(&current, &api.origin, &initial(120)).unwrap();
        let original = current.get(&api.origin).unwrap().unwrap();
        let failure = cached_login(&MemoryStore::default(), &current, &api.origin)
            .await
            .unwrap_err();
        assert!(!failure.body.to_string().contains("secret"));
        assert_eq!(current.get(&api.origin).unwrap().unwrap(), original);
        assert_eq!(api.requests()[0].path, "/v1/auth/session");
        api.assert_complete();
    }
}

#[tokio::test]
async fn only_terminal_http_status_allows_clearing_or_explicit_login_recovery() {
    for (status, terminal) in [(401, true), (503, false)] {
        for kind in ["unauthenticated", "login_required"] {
            let api = IntentApiFixture::new(vec![IntentReply::json(status, json!({"type":kind}))]);
            let current = MemoryStore::default();
            save(&current, &api.origin, &initial(120)).unwrap();
            let failure = logout(&MemoryStore::default(), &current, &api.origin)
                .await
                .unwrap_err();
            assert_eq!(failure.body["local_credential_removed"], terminal);
            assert_eq!(current.get(&api.origin).unwrap().is_none(), terminal);
            api.assert_complete();

            let api = IntentApiFixture::new(vec![IntentReply::json(status, json!({"type":kind}))]);
            save(&current, &api.origin, &initial(120)).unwrap();
            let result = cached_login(&MemoryStore::default(), &current, &api.origin).await;
            if terminal {
                assert!(result.unwrap().is_none());
            } else {
                assert!(result.is_err());
            }
            assert!(current.get(&api.origin).unwrap().is_some());
            api.assert_complete();
        }
    }
}

#[tokio::test]
async fn legacy_cache_does_not_advertise_a_refresh_credential_it_never_stored() {
    let saved = initial(120);
    let mut response = rotated(&saved);
    response["data"]
        .as_object_mut()
        .unwrap()
        .remove("access_token");
    response["data"]
        .as_object_mut()
        .unwrap()
        .remove("refresh_token");
    let api = IntentApiFixture::new(vec![IntentReply::json(200, response)]);
    let legacy = MemoryStore::default();
    let current = MemoryStore::default();
    legacy
        .put(
            &api.origin,
            &super::super::tests::saved("legacy-access", 120),
        )
        .unwrap();
    let original = legacy.get(&api.origin).unwrap().unwrap();
    let public = cached_login(&legacy, &current, &api.origin)
        .await
        .unwrap()
        .unwrap();
    assert!(public["data"].get("refresh_expires_at").is_none());
    assert_eq!(public["data"]["principal_id"], "principal-one");
    assert_eq!(legacy.get(&api.origin).unwrap().unwrap(), original);
    assert!(current.get(&api.origin).unwrap().is_none());
    assert_eq!(api.requests()[0].path, "/v1/auth/session");
    api.assert_complete();
}

#[tokio::test]
async fn partial_logout_cleanup_keeps_v2_authoritative_over_legacy_fallback() {
    struct Unremovable(MemoryStore);
    impl Store for Unremovable {
        fn get(&self, origin: &str) -> Result<Option<String>> {
            self.0.get(origin)
        }
        fn put(&self, origin: &str, value: &str) -> Result<()> {
            self.0.put(origin, value)
        }
        fn remove(&self, _: &str) -> Result<()> {
            Err(keychain_failed())
        }
    }
    let api = IntentApiFixture::new(vec![IntentReply::json(
        200,
        json!({"data":{"logged_out":true},"meta":{}}),
    )]);
    let current = MemoryStore::default();
    save(&current, &api.origin, &initial(120)).unwrap();
    let original = current.get(&api.origin).unwrap().unwrap();
    assert_eq!(
        logout(&Unremovable(MemoryStore::default()), &current, &api.origin)
            .await
            .unwrap_err()
            .body["type"],
        "keychain_unavailable"
    );
    assert_eq!(current.get(&api.origin).unwrap().unwrap(), original);
    api.assert_complete();
}
