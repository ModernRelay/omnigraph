use super::*;
use crate::credential_sources::{OwnerBinding, OwnerKind, ResourceIdentity};

fn request() -> CredentialRequest {
    let owner = OwnerBinding::new(
        OwnerKind::Server,
        "owner-id".into(),
        "profile-id".into(),
        "https://example.invalid/prefix/".into(),
        None,
    )
    .unwrap();
    CredentialRequest::new(
        owner,
        Purpose::Data,
        "https://example.invalid/prefix/".into(),
        None,
        None,
    )
    .unwrap()
}

#[test]
fn request_contains_only_captured_constraints_and_noninteractive_mode() {
    let owner = OwnerBinding::new(
        OwnerKind::Connection,
        "owner-id".into(),
        "profile-id".into(),
        "https://control.invalid/api/".into(),
        Some("org".into()),
    )
    .unwrap();
    let request = CredentialRequest::new(
        owner,
        Purpose::Data,
        "https://data.invalid/graphs/".into(),
        Some("audience".into()),
        Some(ResourceIdentity {
            cluster_id: "cluster".into(),
            cluster_incarnation: "generation".into(),
        }),
    )
    .unwrap();
    let encoded: serde_json::Value =
        serde_json::from_slice(&encode_request(&request).unwrap()).unwrap();
    assert_eq!(
        encoded,
        serde_json::json!({
            "version": 1, "purpose": "data", "endpoint": "https://data.invalid/graphs/",
            "interactive": false, "audience": "audience", "organization_id": "org",
            "cluster_id": "cluster", "cluster_incarnation": "generation"
        })
    );
}

#[test]
fn helper_response_is_strict_and_never_reports_raw_output() {
    let request = request();
    let valid = r#"{"version":1,"credential":{"type":"bearer","token":"fake-only"}}"#;
    let now = OffsetDateTime::UNIX_EPOCH;
    let result = decode_response(&request, valid.as_bytes(), now).unwrap();
    assert_eq!(
        result.bearer_for(&request, now).unwrap().expose(),
        "fake-only"
    );
    assert_eq!(result.expires_at(), None);
    for invalid in [
        "fake-only",
        "",
        "{}",
        r#"{"version":2,"credential":{"type":"bearer","token":"fake-only"}}"#,
        r#"{"version":1,"credential":{"type":"bearer","token":"fake-only"},"endpoint":"https://evil.invalid"}"#,
        r#"{"version":1,"credential":{"type":"bearer","token":"fake-only","header":"x"}}"#,
        r#"{"version":1,"credential":{"type":"certificate","token":"fake-only"}}"#,
        r#"{"version":1,"credential":{"type":"bearer","token":"fake-only"},"expires_at":null}"#,
        r#"{"version":1,"credential":{"type":"bearer","token":"fake-only"},"expires_at":"tomorrow"}"#,
        r#"{"version":1,"credential":{"type":"bearer","token":"fake-only"},"expires_at":123}"#,
        r#"{"version":1,"version":1,"credential":{"type":"bearer","token":"fake-only"}}"#,
        r#"{"version":1,"credential":{"type":"bearer","token":"fake-only","token":"other"}}"#,
    ] {
        let error = decode_response(&request, invalid.as_bytes(), now).unwrap_err();
        assert_eq!(error, CredentialError::HelperResponse);
        assert!(!format!("{error:?}: {error}").contains("fake-only"));
    }
    assert_eq!(
        decode_response(&request, format!("{valid}\n{valid}").as_bytes(), now).unwrap_err(),
        CredentialError::HelperResponse
    );
    assert_eq!(
        decode_response(&request, &vec![b' '; MAX_CREDENTIAL_BYTES + 1], now).unwrap_err(),
        CredentialError::HelperOutputLimit
    );
    for token in ["", "x\r\n", "a b"] {
        let bytes = serde_json::to_vec(
            &serde_json::json!({"version":1,"credential":{"type":"bearer","token":token}}),
        )
        .unwrap();
        assert_eq!(
            decode_response(&request, &bytes, now).unwrap_err(),
            CredentialError::InvalidToken
        );
    }
}

#[test]
fn helper_expiry_is_typed_and_must_be_in_the_future() {
    let request = request();
    let now = OffsetDateTime::parse("2030-01-01T00:00:00Z", &Rfc3339).unwrap();
    for (expiry, valid) in [
        ("2030-01-01T00:00:01Z", true),
        ("2030-01-01T01:00:00+01:00", false),
        ("2029-12-31T23:59:59Z", false),
    ] {
        let bytes = serde_json::to_vec(&serde_json::json!({"version":1,"credential":{"type":"bearer","token":"test"},"expires_at":expiry})).unwrap();
        let result = decode_response(&request, &bytes, now);
        if valid {
            assert_eq!(
                result.unwrap().expires_at(),
                Some(now + time::Duration::seconds(1))
            );
        } else {
            assert_eq!(result.unwrap_err(), CredentialError::Expired);
        }
    }
}

#[test]
fn command_and_timeout_validation_do_not_execute_anything() {
    for argv in [
        vec![],
        vec!["helper".into()],
        vec!["./helper".into()],
        vec!["/helper\0".into()],
        vec!["/helper".into(), "a\0b".into()],
    ] {
        assert_eq!(
            ExecSource::new(argv, "30s").unwrap_err(),
            CredentialError::InvalidCommand
        );
    }
    for timeout in [
        "",
        "0s",
        "121s",
        "1",
        "1.5s",
        "-1s",
        "+1s",
        " 1s",
        "1S",
        "99999999999999999999999s",
    ] {
        assert_eq!(
            ExecSource::new(vec!["/helper".into()], timeout).unwrap_err(),
            CredentialError::InvalidTimeout
        );
    }
    for timeout in ["1s", "30s", "120s"] {
        assert!(ExecSource::new(vec!["/helper".into()], timeout).is_ok());
    }
}

#[cfg(unix)]
fn helper(script: &str) -> (tempfile::TempDir, ExecSource) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("helper.sh");
    std::fs::write(&path, script).unwrap();
    let source =
        ExecSource::new(vec!["/bin/sh".into(), path.to_str().unwrap().into()], "2s").unwrap();
    (dir, source)
}

#[cfg(unix)]
#[tokio::test]
async fn helper_receives_json_and_literal_argv_and_returns_a_bound_secret() {
    let (_dir, mut source) = helper(
        r#"
IFS= read -r request
case "$request" in *'"interactive":false'*) ;; *) exit 1;; esac
case "$request" in *'"endpoint":"https://example.invalid/prefix/"'*) ;; *) exit 2;; esac
[ "$1" = '$(exit 99); * $HOME' ] || exit 3
printf '%s\n' '{"version":1,"credential":{"type":"bearer","token":"test-only-token"}}'
"#,
    );
    source.argv.push("$(exit 99); * $HOME".into());
    let request = request();
    let acquired = acquire_exec(&request, &source).await.unwrap();
    assert_eq!(
        acquired
            .bearer_for(&request, OffsetDateTime::now_utc())
            .unwrap()
            .expose(),
        "test-only-token"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn helper_nonzero_and_malformed_output_are_redacted() {
    for (script, expected) in [
        (
            "IFS= read -r request\nprintf 'leaked-secret' >&2\nexit 7",
            CredentialError::HelperFailed,
        ),
        (
            "IFS= read -r request\nprintf 'leaked-secret'",
            CredentialError::HelperResponse,
        ),
    ] {
        let (_dir, source) = helper(script);
        let error = acquire_exec(&request(), &source).await.unwrap_err();
        assert_eq!(error, expected);
        assert!(!format!("{error:?}: {error}").contains("leaked-secret"));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn both_output_streams_are_bounded_and_overflow_terminates_helper() {
    for stream in ["", ">&2"] {
        let script = format!("IFS= read -r request\nwhile :; do printf '%01024d' 0 {stream}; done");
        let (_dir, source) = helper(&script);
        assert_eq!(
            acquire_exec(&request(), &source).await.unwrap_err(),
            CredentialError::HelperOutputLimit
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn output_limit_accepts_exact_boundary_on_both_streams() {
    for retain in [false, true] {
        let input = vec![b'x'; MAX_CREDENTIAL_BYTES];
        let result = read_output(input.as_slice(), retain).await.unwrap();
        assert_eq!(result.len(), if retain { MAX_CREDENTIAL_BYTES } else { 0 });
        let input = vec![b'x'; MAX_CREDENTIAL_BYTES + 1];
        assert_eq!(
            read_output(input.as_slice(), retain).await.unwrap_err(),
            CredentialError::HelperOutputLimit
        );
    }
    let mut response =
        br#"{"version":1,"credential":{"type":"bearer","token":"fake-only"}}"#.to_vec();
    response.resize(MAX_CREDENTIAL_BYTES, b' ');
    assert!(decode_response(&request(), &response, OffsetDateTime::UNIX_EPOCH).is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn stdin_is_closed_and_output_streams_are_drained_concurrently() {
    let (_dir, source) = helper(
        r#"
while IFS= read -r request; do :; done
i=0
while [ "$i" -lt 32 ]; do
    printf '%01024d' 0 >&2
    printf '%1024s' ' '
    i=$((i + 1))
done
printf '%s\n' '{"version":1,"credential":{"type":"bearer","token":"fake-only"}}'
"#,
    );
    assert!(acquire_exec(&request(), &source).await.is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn timeout_covers_stdin_and_descendants_holding_pipes() {
    for script in [
        "exec /bin/sleep 30",
        "IFS= read -r request\n/bin/sleep 30 &\nexit 0",
    ] {
        let (_dir, mut source) = helper(script);
        source.timeout = Duration::from_millis(100);
        let result =
            tokio::time::timeout(Duration::from_secs(3), acquire_exec(&request(), &source))
                .await
                .unwrap();
        assert_eq!(result.unwrap_err(), CredentialError::HelperTimeout);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_and_timeout_kill_the_process_group() {
    for cancel in [false, true] {
        let (dir, mut source) = helper(
            "IFS= read -r request\n( /bin/sleep 0.5; printf survived > \"$1\" ) &\nprintf ready > \"$2\"\nwait",
        );
        let marker = dir.path().join("survived");
        let ready = dir.path().join("ready");
        source.argv.push(marker.to_str().unwrap().into());
        source.argv.push(ready.to_str().unwrap().into());
        source.timeout = Duration::from_millis(200);
        let task = tokio::spawn(async move { acquire_exec(&request(), &source).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(
                task.await.unwrap().unwrap_err(),
                CredentialError::HelperTimeout
            );
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(!marker.exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn missing_executable_does_not_expose_argv() {
    let dir = tempfile::tempdir().unwrap();
    let source = ExecSource::new(
        vec![
            dir.path().join("missing").to_str().unwrap().into(),
            "secret-in-argv".into(),
        ],
        "1s",
    )
    .unwrap();
    let error = acquire_exec(&request(), &source).await.unwrap_err();
    assert_eq!(error, CredentialError::HelperSpawn);
    assert!(!error.to_string().contains("secret-in-argv"));
}
