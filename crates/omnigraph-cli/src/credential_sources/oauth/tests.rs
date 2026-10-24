use super::*;
use crate::credential_sources::{OwnerBinding, OwnerKind};
use serde_json::json;

fn request() -> CredentialRequest {
    let owner = OwnerBinding::new(
        OwnerKind::Profile,
        "owner".into(),
        "auth".into(),
        "https://api.example".into(),
        Some("org_one".into()),
    )
    .unwrap();
    CredentialRequest::new(
        owner,
        Purpose::Control,
        "https://api.example".into(),
        None,
        None,
    )
    .unwrap()
}

#[test]
fn explicit_endpoint_client_and_scope_are_strict() {
    for endpoint in [
        "http://auth.example/token",
        "https://user@auth.example/token",
        "https://auth.example/token?q=secret",
        "https://auth.example/token#token",
        "https://auth.example/ token",
        "https:\\auth.example/token",
        "\0https://auth.example/token",
    ] {
        assert_eq!(
            validate_client_credentials(endpoint, "client", None),
            Err(CredentialError::InvalidClientCredentials)
        );
    }
    for client in ["", "client\n", "client id"] {
        assert!(
            validate_client_credentials("https://auth.example/oauth2/token", client, None).is_err()
        );
    }
    for scope in [
        "",
        "read  write",
        " read",
        "read\nwrite",
        "read\"write",
        "read\\write",
    ] {
        assert!(
            validate_client_credentials("https://auth.example/oauth2/token", "client", Some(scope))
                .is_err()
        );
    }
    assert!(
        validate_client_credentials(
            "https://auth.example/oauth2/token",
            "client",
            Some("read write:graph")
        )
        .is_ok()
    );
}

#[test]
fn response_requires_a_bounded_bearer_and_finite_future_expiry() {
    let now = OffsetDateTime::now_utc();
    let body = json!({"access_token":"test-token", "token_type":"Bearer", "expires_in":120});
    let credential = decode(&request(), &serde_json::to_vec(&body).unwrap(), now).unwrap();
    assert_eq!(
        credential.expires_at(),
        Some(now + time::Duration::seconds(120))
    );
    assert_eq!(
        credential.bearer_for(&request(), now).unwrap().expose(),
        "test-token"
    );
    assert!(!format!("{credential:?}").contains("test-token"));
    for (field, value) in [
        ("access_token", json!("not a token")),
        ("token_type", json!("Basic")),
        ("expires_in", json!(0)),
        ("expires_in", json!(-1)),
        ("expires_in", json!(u64::MAX)),
        ("expires_in", json!("120")),
    ] {
        let mut invalid = body.clone();
        invalid[field] = value;
        assert!(
            decode(&request(), &serde_json::to_vec(&invalid).unwrap(), now).is_err(),
            "{field}"
        );
    }
    assert_eq!(
        decode(
            &request(),
            &serde_json::to_vec(&body).unwrap(),
            now - time::Duration::seconds(121)
        )
        .unwrap_err(),
        CredentialError::Expired
    );
    assert_eq!(
        decode(&request(), &vec![b'x'; MAX_CREDENTIAL_BYTES + 1], now).unwrap_err(),
        CredentialError::TooLarge
    );
}

#[test]
fn mounted_secret_allows_oauth_characters_without_logging_or_trimming_them() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("secret");
    let source = ClientSecretSource::File { file: file.clone() };
    for (raw, expected) in [
        ("test:&=+ secret", "test:&=+ secret"),
        ("test-secret\r\n", "test-secret"),
    ] {
        std::fs::write(&file, raw).unwrap();
        assert_eq!(secret(&source).unwrap(), expected);
    }
    for invalid in ["", "secret\n\n", "secret\r", "secret\0"] {
        std::fs::write(&file, invalid).unwrap();
        assert_eq!(
            secret(&source),
            Err(CredentialError::InvalidClientCredentials)
        );
    }
}
