use super::*;

const SECRET: &str = "fixture-disclosed-machine-secret";

fn summary(id: &str) -> Value {
    json!({"id":id,"secret_hint":"ret","created_at":"2026-11-25T10:00:00Z","updated_at":"2026-11-25T10:00:00Z"})
}

fn secrets(ids: &[&str]) -> Vec<SecretSummary> {
    ids.iter()
        .map(|id| serde_json::from_value(summary(id)).unwrap())
        .collect()
}

fn machine() -> Machine {
    serde_json::from_value(json!({"application_id":"app_one","client_id":"client_one","organization_id":"org_one","name":"ci bot","scopes":["status"],"principal_id":"01PRINCIPAL","actor_id":"principal:01PRINCIPAL","created_at":"2026-11-25T10:00:00Z","updated_at":"2026-11-25T10:00:00Z"})).unwrap()
}

#[test]
fn a_disclosed_secret_leaves_the_body_before_validation() {
    let mut body = json!({"data":{"client_id":"client_one","secret":summary("sec_new"),"client_secret":SECRET,"secret_available":true},"meta":{}});
    let (secret, value) = take_secret(&mut body, "client_one").unwrap();
    assert_eq!(secret.id, "sec_new");
    assert_eq!(
        value.map(|ClientSecret(value)| value).as_deref(),
        Some(SECRET)
    );
    assert!(!body.to_string().contains(SECRET), "{body}");

    // An exact replay discloses nothing and says so.
    let mut body = json!({"data":{"client_id":"client_one","secret":summary("sec_new"),"client_secret":null,"secret_available":false},"meta":{}});
    assert!(take_secret(&mut body, "client_one").unwrap().1.is_none());

    // Every refusal is generic and the removed value appears in no failure.
    for (pointer, value) in [
        ("/data/secret_available", json!(false)),
        ("/data/client_id", json!("client_other")),
        ("/data/client_secret", json!(format!("{SECRET}\n"))),
        ("/data/client_secret", json!("")),
        ("/data/client_secret", json!(7)),
        ("/data/secret/id", json!("../sec")),
    ] {
        let mut body = json!({"data":{"client_id":"client_one","secret":summary("sec_new"),"client_secret":SECRET,"secret_available":true},"meta":{}});
        *body.pointer_mut(pointer).unwrap() = value;
        let failure = take_secret(&mut body, "client_one").err().unwrap();
        assert_eq!(failure.body["type"], "api_response_invalid", "{pointer}");
        assert!(!failure.body.to_string().contains(SECRET));
        assert!(!format!("{failure:?}").contains(SECRET));
    }
}

#[test]
fn rotation_revokes_only_an_unambiguous_or_named_earlier_secret() {
    assert!(revocation_targets(&[], &[]).unwrap().is_empty());
    assert_eq!(
        revocation_targets(&secrets(&["sec_a"]), &[]).unwrap(),
        ["sec_a"]
    );
    let two = secrets(&["sec_a", "sec_b"]);
    let ambiguous = revocation_targets(&two, &[]).unwrap_err();
    assert_eq!(ambiguous.body["type"], "machine_secret_ambiguous");
    let detail = ambiguous.body["detail"].as_str().unwrap();
    assert!(
        detail.contains("sec_a (hint ret") && detail.contains("sec_b"),
        "{detail}"
    );
    assert_eq!(
        revocation_targets(&two, &["sec_b".into()]).unwrap(),
        ["sec_b"]
    );
    assert_eq!(
        revocation_targets(&two, &["sec_a".into(), "sec_b".into()]).unwrap(),
        ["sec_a", "sec_b"]
    );
    let missing = revocation_targets(&two, &["sec_c".into()]).unwrap_err();
    assert_eq!(missing.body["type"], "machine_secret_not_found");
    assert!(missing.body["detail"].as_str().unwrap().contains("sec_a"));
}

#[cfg(unix)]
#[test]
fn the_secret_file_is_owner_only_atomic_and_never_a_link() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client-secret");
    let file = SecretFile::new(&path).unwrap();
    file.check(false).unwrap();
    file.deliver(file.stage().unwrap(), &ClientSecret(SECRET.into()), false)
        .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), SECRET);
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "{mode:o}");
    // Nothing but the destination is left in the directory.
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);

    assert_eq!(
        file.check(false).unwrap_err().body["type"],
        "secret_file_exists"
    );
    assert!(
        file.deliver(file.stage().unwrap(), &ClientSecret("other".into()), false)
            .is_err()
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), SECRET);
    file.check(true).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    file.deliver(file.stage().unwrap(), &ClientSecret("rotated".into()), true)
        .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "rotated");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "a replaced file is owner-only again");

    let link = directory.path().join("linked-secret");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    for replace in [false, true] {
        assert_eq!(
            SecretFile::new(&link)
                .unwrap()
                .check(replace)
                .unwrap_err()
                .body["type"],
            "secret_file_invalid"
        );
    }
    assert_eq!(
        SecretFile::new(directory.path())
            .unwrap()
            .check(true)
            .unwrap_err()
            .body["type"],
        "secret_file_invalid"
    );
    assert_eq!(
        SecretFile::new(&directory.path().join("missing/client-secret"))
            .err()
            .unwrap()
            .body["type"],
        "secret_file_invalid"
    );
    assert!(
        SecretFile::new(Path::new("relative-secret"))
            .unwrap()
            .path
            .is_absolute()
    );
}

#[test]
fn machines_must_belong_to_the_organization_and_name_their_principal() {
    let valid = machine();
    validate_machine(&valid, "org_one").unwrap();
    assert!(validate_machine(&valid, "org_two").is_err());
    for change in [
        (|m: &mut Machine| m.actor_id = "principal:other".into()) as fn(&mut Machine),
        |m| m.actor_id = "01PRINCIPAL".into(),
        |m| m.client_id = "client/one".into(),
        |m| m.name = " padded".into(),
        |m| m.scopes = vec!["status".into(), "status".into()],
        |m| m.created_at = "yesterday".into(),
    ] {
        let mut invalid = valid.clone();
        change(&mut invalid);
        assert!(validate_machine(&invalid, "org_one").is_err());
    }
}

#[test]
fn requested_scopes_are_canonical_and_bounded() {
    assert_eq!(
        validate_create_scopes(&["status".into(), "create".into()]).unwrap(),
        ["create", "status"]
    );
    assert!(validate_create_scopes(&[]).unwrap().is_empty());
    for scopes in [
        vec!["status", "status"],
        vec!["members_manage"],
        vec!["bad scope"],
        vec!["a", "b", "c", "d", "e", "f"],
    ] {
        let scopes: Vec<String> = scopes.into_iter().map(str::to_owned).collect();
        assert_eq!(
            validate_create_scopes(&scopes).unwrap_err().body["type"],
            "scopes_invalid"
        );
    }
}

#[test]
fn each_write_derives_its_own_key_from_one_bounded_request_key() {
    validate_key(&"k".repeat(MAX_KEY)).unwrap();
    assert_eq!(
        validate_key(&"k".repeat(MAX_KEY + 1)).unwrap_err().body["type"],
        "idempotency_key_invalid"
    );
    let key = "k".repeat(MAX_KEY);
    for step in ["machine", "secret", "revoke-127"] {
        let derived = step_key(&key, step);
        assert!(derived.len() <= 256 && derived.starts_with(&key));
    }
    assert_ne!(step_key("k", "machine"), step_key("k", "secret"));
}

#[test]
fn a_retained_request_records_progress_and_never_a_secret() {
    let request = Request::Rotate {
        profile: "ci".into(),
        revoke: vec!["sec_a".into()],
    };
    let progress = Progress::Rotate {
        client_id: "client_one".into(),
        token_endpoint: "https://auth.example/oauth2/token".into(),
        secret_file: "/run/ci-secret".into(),
        revoke: vec!["sec_a".into()],
        delivered: Some("sec_new".into()),
    };
    assert!(progress.matches(&request));
    assert!(!progress.matches(&Request::Revoke {
        selector: "ci".into()
    }));
    let record = serde_json::to_value(Record {
        version: 1,
        api: "https://api.example".into(),
        organization_id: "org_one".into(),
        principal_id: "principal_one".into(),
        owner_profile: "owner".into(),
        idempotency_key: "key".into(),
        request,
        progress,
    })
    .unwrap();
    assert_eq!(record["request"]["operation"], "rotate");
    assert_eq!(record["progress"]["state"], "rotate");
    assert_eq!(record["progress"]["delivered"], "sec_new");
    let mut unknown = record.clone();
    unknown["progress"]["client_secret"] = json!(SECRET);
    assert!(serde_json::from_value::<Record>(unknown).is_err());
}

#[test]
fn page_paths_encode_the_cursor_as_one_query_value() {
    assert_eq!(page_path(ROOT, None), ROOT);
    assert_eq!(
        page_path(ROOT, Some("a/b+c")),
        "/v1/organization/machines?after=a%2Fb%2Bc"
    );
}
