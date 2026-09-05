use super::*;
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Default)]
struct MemoryStore(RefCell<HashMap<String, String>>);
impl Store for MemoryStore {
    fn get(&self, origin: &str) -> Result<Option<String>> {
        Ok(self.0.borrow().get(origin).cloned())
    }
    fn put(&self, origin: &str, value: &str) -> Result<()> {
        self.0
            .borrow_mut()
            .insert(origin.to_string(), value.to_string());
        Ok(())
    }
    fn remove(&self, origin: &str) -> Result<()> {
        self.0.borrow_mut().remove(origin);
        Ok(())
    }
}

fn saved(token: &str, seconds: i64) -> String {
    serde_json::to_string(&Session {
        version: 1,
        access_token: token.into(),
        expires_at: (OffsetDateTime::now_utc() + time::Duration::seconds(seconds))
            .format(&Rfc3339)
            .unwrap(),
    })
    .unwrap()
}

#[test]
fn origin_bound_automation_never_reads_human_or_data_plane_credentials() {
    let store = MemoryStore::default();
    store
        .put("https://control.example", &saved("human-secret", 60))
        .unwrap();
    assert_eq!(
        credential_from(
            &store,
            "https://control.example",
            Some("automation".into()),
            Some("https://CONTROL.example:443".into())
        )
        .unwrap(),
        "automation"
    );
    for (token, api) in [
        (Some("automation".into()), None),
        (None, Some("https://control.example".into())),
        (
            Some("automation".into()),
            Some("https://other.example".into()),
        ),
    ] {
        assert_eq!(
            credential_from(&store, "https://control.example", token, api)
                .unwrap_err()
                .exit,
            2
        );
    }
    assert_eq!(
        credential_from(&store, "https://control.example", None, None).unwrap(),
        "human-secret"
    );
    assert!(credential_from(&store, "https://other.example", None, None).is_err());
}

#[test]
fn expired_and_unbounded_sessions_are_refused_without_secret_diagnostics() {
    for value in [
        saved("opaque-secret", -1),
        saved("opaque-secret", 901),
        "opaque-secret".into(),
    ] {
        let failure = session(&value).err().unwrap();
        assert!(!failure.body.to_string().contains("opaque-secret"));
    }
    assert!(session(&saved("valid", 899)).is_ok());
    assert!(validate_token("secret\nheader").is_err());
}

#[test]
fn device_poll_intervals_are_bounded_and_secrets_are_scrubbed() {
    for n in [0, 4, 601] {
        assert!(interval(&json!({"interval":n})).is_err());
    }
    for n in [5, 10, 600] {
        assert_eq!(interval(&json!({"interval":n})).unwrap(), n);
    }
    let failure = scrub(
        Failure::new("device_expired", "device secret-code was consumed", 2),
        "secret-code",
    );
    assert!(!failure.body.to_string().contains("secret-code"));
    assert!(verification_uri("https://auth.example/device?user_code=ABCD").is_ok());
    assert!(verification_uri("javascript:alert(1)").is_err());
    assert!(verification_uri("http://127.0.0.1/verify").is_err());
}

fn device(expires: u64) -> IntentReply {
    IntentReply::json(
        200,
        json!({"data":{"device_code":"device-secret","user_code":"ABCD-EFGH","verification_uri":"https://auth.example/device","verification_uri_complete":null,"expires_in":expires,"interval":5},"meta":{"provenance":"service_db"}}),
    )
}

fn logged_in() -> IntentReply {
    IntentReply::json(
        200,
        json!({"data":{"access_token":"opaque-service-secret","token_type":"Bearer","expires_at":(OffsetDateTime::now_utc()+time::Duration::seconds(120)).format(&Rfc3339).unwrap(),"principal_id":"principal-one","subject":"actor-one","account_id":"account-one","scopes":{"actions":["plan","apply"]},"refresh_token":"must-never-persist"},"meta":{"provenance":"service_db","assurance":"verified_human","unexpected":"opaque-service-secret device-secret"}}),
    )
}

#[tokio::test]
async fn device_login_pending_slowdown_then_opaque_keychain_session() {
    let api = IntentApiFixture::new(vec![
        device(60),
        IntentReply::json(428, json!({"type":"authorization_pending","interval":5})),
        IntentReply::json(429, json!({"type":"slow_down","interval":10})),
        logged_in(),
    ]);
    let store = MemoryStore::default();
    let started = Instant::now();
    let output = login_with(&store, api.origin.clone()).await.unwrap();
    assert!(started.elapsed() >= Duration::from_secs(20));
    assert_eq!(output["data"]["principal_id"], "principal-one");
    for secret in [
        "device-secret",
        "opaque-service-secret",
        "must-never-persist",
    ] {
        assert!(!output.to_string().contains(secret));
    }
    let stored = store.get(&api.origin).unwrap().unwrap();
    assert_eq!(
        session(&stored).unwrap().access_token,
        "opaque-service-secret"
    );
    assert!(!stored.contains("must-never-persist"));
    assert!(!stored.contains("device-secret"));
    let requests = api.requests();
    assert_eq!(requests[0].path, "/v1/auth/device");
    assert_eq!(requests[0].body, json!({}));
    for request in &requests[1..] {
        assert_eq!(request.path, "/v1/auth/device/token");
        assert_eq!(request.body, json!({"device_code":"device-secret"}));
        assert!(!request.headers.contains_key("authorization"));
    }
    api.assert_complete();
}

#[tokio::test]
async fn uncertain_device_consumption_and_expiry_require_fresh_login() {
    let api = IntentApiFixture::new(vec![
        device(60),
        IntentReply::json(409, json!({"type":"device_poll_in_progress","interval":5})),
    ]);
    let store = MemoryStore::default();
    let failure = login_with(&store, api.origin.clone()).await.unwrap_err();
    assert_eq!(failure.body["type"], "device_poll_in_progress");
    assert!(
        failure.body["detail"]
            .as_str()
            .unwrap()
            .contains("start login again")
    );
    assert!(store.get(&api.origin).unwrap().is_none());
    api.assert_complete();
    let expired = IntentApiFixture::new(vec![device(1)]);
    assert_eq!(
        login_with(&store, expired.origin.clone())
            .await
            .unwrap_err()
            .body["type"],
        "device_expired"
    );
    expired.assert_complete();
}

#[tokio::test]
async fn keychain_failure_revokes_the_unstored_session_without_plaintext_fallback() {
    struct Unwritable;
    impl Store for Unwritable {
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
        device(60),
        logged_in(),
        IntentReply::json(200, json!({"data":{"logged_out":true},"meta":{}})),
    ]);
    assert_eq!(
        login_with(&Unwritable, api.origin.clone())
            .await
            .unwrap_err()
            .body["type"],
        "keychain_unavailable"
    );
    assert_eq!(api.requests()[2].path, "/v1/auth/logout");
    assert_eq!(
        api.requests()[2].headers["authorization"],
        "Bearer opaque-service-secret"
    );
    api.assert_complete();
}

#[tokio::test]
async fn logout_removes_only_selected_origin_even_when_revocation_fails() {
    let api = IntentApiFixture::new(vec![IntentReply::json(
        503,
        json!({"type":"provider_unavailable","detail":"temporary"}),
    )]);
    let store = MemoryStore::default();
    store
        .put(&api.origin, &saved("opaque-service-secret", 60))
        .unwrap();
    store
        .put("https://other.example", &saved("other-secret", 60))
        .unwrap();
    let failure = logout_with(&store, api.origin.clone()).await.unwrap_err();
    assert_eq!(failure.body["local_credential_removed"], true);
    assert_eq!(failure.body["revocation_confirmed"], false);
    assert!(store.get(&api.origin).unwrap().is_none());
    assert!(store.get("https://other.example").unwrap().is_some());
    assert_eq!(
        api.requests()[0].headers["authorization"],
        "Bearer opaque-service-secret"
    );
    api.assert_complete();
}
