use super::*;
use crate::operator::{InvocationConfig, parse_operator_config};

#[test]
fn auth_selection_never_infers_a_sole_profile() {
    use crate::operator_schema::AuthProfile;
    let profiles = BTreeMap::from([("only".to_owned(), AuthProfile::Anonymous)]);
    assert!(crate::operator_routing::choose(&profiles, None, None).is_err());
    assert_eq!(
        crate::operator_routing::choose(&profiles, Some("only"), None)
            .unwrap()
            .0,
        "only"
    );
    assert_eq!(
        crate::operator_routing::choose(&profiles, None, Some("only"))
            .unwrap()
            .0,
        "only"
    );
    assert!(crate::operator_routing::choose(&profiles, Some("only"), Some("missing")).is_err());
}

#[test]
fn explicit_rename_and_principal_replacement_commands_parse_without_legacy_selectors() {
    for args in [
        vec!["config", "rename-connection", "old", "new"],
        vec!["config", "rename-target", "old", "new"],
        vec!["config", "rename-auth", "connection", "work", "old", "new"],
        vec!["config", "rename-auth", "server", "served", "old", "new"],
        vec!["config", "recover-rename"],
        vec![
            "login",
            "--connection",
            "work",
            "--auth",
            "me",
            "--replace-principal",
            "principal_new",
        ],
    ] {
        assert!(crate::invocation::parse_from(std::iter::once("omnigraph").chain(args)).is_ok());
    }
    assert!(
        crate::invocation::parse_from([
            "omnigraph",
            "login",
            "--connection",
            "work",
            "--replace-principal",
            "principal_new"
        ])
        .is_err()
    );
}

#[test]
fn standalone_server_and_auth_renames_preserve_the_secret_store_binding() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("config.yaml"), "version: 1\ntargets:\n  old:\n    kind: server\n    endpoint: https://example.test/prefix\n    auth: {first: {provider: token, source: {store: {}}}}\n    default_auth: first\n").unwrap();
    let resolve = |target: &str, profile: &str| {
        let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
        let cli = crate::invocation::parse_from([
            "omnigraph",
            "credentials",
            "status",
            "--target",
            target,
            "--auth",
            profile,
        ])
        .unwrap()
        .cli;
        let target = crate::scope::resolve(&cli, &input, directory.path(), false, &mut Vec::new())
            .unwrap()
            .unwrap();
        crate::operator_routing::authentication(&cli, &input, Some(&target))
            .unwrap()
            .unwrap()
    };
    let before = resolve("old", "first");
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    crate::operator_routing::rename(
        &input,
        crate::operator_routing::Rename::Target,
        "old",
        "new",
    )
    .unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    crate::operator_routing::rename(
        &input,
        crate::operator_routing::Rename::Auth {
            server: true,
            owner: "new".into(),
        },
        "first",
        "second",
    )
    .unwrap();
    assert_eq!(resolve("new", "second").owner, before.owner);
    assert!(before.ensure_current().is_ok());
}

#[tokio::test]
async fn anonymous_loopback_and_store_status_do_not_acquire_credentials() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("config.yaml"), "version: 1\ntargets:\n  public:\n    kind: server\n    endpoint: http://127.0.0.1:9000/prefix\n    auth: {guest: {provider: anonymous}}\n  private:\n    kind: server\n    endpoint: https://example.test/prefix\n    auth: {saved: {provider: token, source: {store: {}}}}\n").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    for (name, profile, expected) in [
        ("public", "guest", "anonymous"),
        ("private", "saved", "unknown"),
    ] {
        let cli = crate::invocation::parse_from([
            "omnigraph",
            "credentials",
            "status",
            "--json",
            "--target",
            name,
            "--auth",
            profile,
        ])
        .unwrap()
        .cli;
        let target = crate::scope::resolve(&cli, &input, directory.path(), false, &mut Vec::new())
            .unwrap()
            .unwrap();
        let auth = crate::operator_routing::authentication(&cli, &input, Some(&target)).unwrap();
        assert_eq!(auth.is_none(), name == "public");
        let mut output = Vec::new();
        crate::operator_routing::AUTH
            .scope(
                auth,
                crate::commands::targets::run_credential(
                    &target,
                    crate::commands::targets::CredentialCommand::Status { json: true },
                    profile,
                    false,
                    &mut std::io::empty(),
                    &mut output,
                ),
            )
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(result["availability"], expected);
    }
}

#[tokio::test]
async fn supported_owner_and_profile_renames_preserve_custody_and_references() {
    use crate::operator_routing::{Rename, registration_auth, rename};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.yaml");
    std::fs::write(&path, "version: 1\nconnections:\n  work:\n    api: https://api.example/prefix\n    organization_id: org_test\n    auth: {me: {provider: omnigraph}}\n    default_auth: me\ntargets:\n  cloud:\n    kind: managed\n    connection: work\n    cluster_id: cluster_one\n    cluster_incarnation: incarnation_one\naliases:\n  search: {target: cloud, graph: knowledge, query: search}\ndefaults: {connection: work, target: cloud}\n").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    let before = registration_auth(&input, "work", "me", "https://api.example/prefix").unwrap();
    rename(
        &input,
        Rename::Auth {
            server: false,
            owner: "work".into(),
        },
        "me",
        "human",
    )
    .unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    rename(&input, Rename::Connection, "work", "renamed").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    rename(&input, Rename::Target, "cloud", "production").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    let after =
        registration_auth(&input, "renamed", "human", "https://api.example/prefix").unwrap();
    assert_eq!(before.owner, after.owner);
    let old_namespace = crate::operator_routing::AUTH
        .scope(Some(before), async {
            crate::operator_routing::namespace("session")
        })
        .await;
    let new_namespace = crate::operator_routing::AUTH
        .scope(Some(after), async {
            crate::operator_routing::namespace("session")
        })
        .await;
    assert_eq!(old_namespace, new_namespace);
    let config = input.get(&mut Vec::new()).unwrap().schema.as_ref().unwrap();
    assert_eq!(config.defaults.connection.as_deref(), Some("renamed"));
    assert_eq!(config.defaults.target.as_deref(), Some("production"));
    assert_eq!(
        config.connections["renamed"].default_auth.as_deref(),
        Some("human")
    );
    assert_eq!(config.aliases["search"].target, "production");
    assert!(
        matches!(&config.targets["production"], crate::operator_schema::Target::Managed { connection, .. } if connection == "renamed")
    );
}

#[test]
fn whole_document_validation_rejects_legacy_and_unselected_invalid_entries() {
    for text in [
        "target_config_version: 1",
        "version: 2",
        "version: 1\noperator: {actor: me}",
        "version: 1\ndefaults: {target: missing}",
        "version: 1\ntargets: {bad: {kind: server}}",
        "version: 1\ntargets: {bad: {kind: direct, scope: graph, uri: /tmp/graph}}",
        "version: 1\ntargets: {bad: {kind: direct, scope: graph, uri: 's3://bucket/root'}}",
        "version: 1\ntargets: {bad: {kind: server, endpoint: 'https://example.com', credential: old}}",
    ] {
        assert!(parse_operator_config(text).is_err(), "accepted {text}");
    }
    let config = parse_operator_config(
        "version: 1\ntargets: {local: {kind: direct, scope: graph, uri: 'file:///tmp/graph'}}",
    )
    .unwrap();
    assert!(matches!(
        config.binding("local").unwrap(),
        TargetBinding::DirectGraph { .. }
    ));
}

#[test]
fn invocation_keeps_one_success_or_failure_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    std::fs::write(
        &path,
        "version: 1\ntargets: {local: {kind: direct, scope: graph, uri: 'file:///tmp/first'}}",
    )
    .unwrap();
    let input = InvocationConfig::at(Some(dir.path().to_owned()), None);
    assert!(
        input
            .get(&mut Vec::new())
            .unwrap()
            .targets
            .contains_key("local")
    );
    std::fs::write(&path, "version: 900").unwrap();
    assert!(
        input
            .get(&mut Vec::new())
            .unwrap()
            .targets
            .contains_key("local")
    );
    let bad = InvocationConfig::at(Some(dir.path().to_owned()), None);
    assert!(bad.get(&mut Vec::new()).is_err());
    std::fs::write(&path, "version: 1").unwrap();
    assert!(bad.get(&mut Vec::new()).is_err());
}

#[test]
fn explicit_operator_file_replaces_default_and_resolves_credential_paths() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("config.yaml"), "invalid: [").unwrap();
    std::fs::write(dir.path().join("chosen.yaml"), "version: 1\ntargets:\n  served:\n    kind: server\n    endpoint: https://example.test/prefix\n    auth: {me: {provider: token, source: {file: ./secret}}}\n").unwrap();
    let input = InvocationConfig::at(Some(dir.path().to_owned()), None)
        .with_path(Some("chosen.yaml"), dir.path());
    let config = input.get(&mut Vec::new()).unwrap();
    let crate::operator_schema::Target::Server { auth, .. } =
        &config.schema.as_ref().unwrap().targets["served"]
    else {
        panic!()
    };
    let crate::operator_schema::AuthProfile::Token {
        source: crate::operator_schema::TokenSource::File { file },
    } = &auth["me"]
    else {
        panic!()
    };
    assert_eq!(file, &dir.path().join("secret"));
}

#[tokio::test]
async fn owner_profiles_capture_one_credential_and_never_fall_back() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("config.yaml"), "version: 1\ntargets:\n  served:\n    kind: server\n    endpoint: https://example.test/prefix\n    default_graph: knowledge\n    auth:\n      first: {provider: token, source: {file: ./first}}\n      second: {provider: token, source: {file: ./second}}\n").unwrap();
    std::fs::write(dir.path().join("first"), "first-token\n").unwrap();
    let input = InvocationConfig::at(Some(dir.path().to_owned()), None);
    let resolve = |profile| {
        let cli = crate::invocation::parse_from([
            "omnigraph",
            "query",
            "-e",
            "q",
            "--target",
            "served",
            "--auth",
            profile,
        ])
        .unwrap()
        .cli;
        let target = crate::scope::resolve(&cli, &input, dir.path(), false, &mut Vec::new())
            .unwrap()
            .unwrap();
        crate::operator_routing::authentication(&cli, &input, Some(&target))
            .unwrap()
            .unwrap()
    };
    let first = resolve("first");
    let second = resolve("second");
    assert_ne!(first.owner, second.owner);
    assert_eq!(first.token().await.unwrap().as_deref(), Some("first-token"));
    std::fs::write(dir.path().join("first"), "rotated-token\n").unwrap();
    assert_eq!(first.token().await.unwrap().as_deref(), Some("first-token"));
    assert!(second.token().await.is_err());
    let next = resolve("first");
    assert_eq!(next.owner, first.owner);
    assert_eq!(
        next.token().await.unwrap().as_deref(),
        Some("rotated-token")
    );
    assert!(first.cached_token("https://example.test/sibling/").is_err());
    let old = crate::operator_routing::AUTH
        .scope(Some(first.clone()), async {
            (
                crate::operator_routing::data_namespace("data"),
                crate::operator_routing::journal_owner(),
            )
        })
        .await;
    first.invalidate().unwrap();
    let renewed = resolve("first");
    assert_eq!(renewed.owner, first.owner);
    let new = crate::operator_routing::AUTH
        .scope(Some(renewed), async {
            (
                crate::operator_routing::data_namespace("data"),
                crate::operator_routing::journal_owner(),
            )
        })
        .await;
    assert_ne!(old.0, new.0);
    assert_eq!(old.1, new.1);
}

#[test]
fn request_scope_cannot_leave_the_frozen_base_prefix() {
    let input = InvocationConfig::at(None, None);
    let auth = ServerAuth::Token {
        owner: "selected".into(),
    };
    let scope = capture_server_scope(&input, &auth, "https://example.test/one/").unwrap();
    assert!(
        scope
            .validate(
                &RequestTarget::of(
                    "https://example.test/one/",
                    "https://example.test/one/graphs/g"
                )
                .unwrap()
            )
            .is_ok()
    );
    assert!(
        scope
            .validate(
                &RequestTarget::of(
                    "https://example.test/one/",
                    "https://example.test/two/graphs/g"
                )
                .unwrap()
            )
            .is_err()
    );
}

#[test]
fn selected_root_profile_keeps_nested_boundaries_without_borrowing_other_profiles() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.yaml");
    std::fs::write(
        &path,
        r#"version: 1
targets:
  parent:
    kind: server
    endpoint: https://example.test/api
    default_auth: public
    auth:
      public: {provider: anonymous}
      private: {provider: token, source: {env: UNUSED_TOKEN}}
  alternate:
    kind: server
    endpoint: https://example.test/api
    default_auth: public
    auth: {public: {provider: anonymous}}
  child:
    kind: server
    endpoint: https://example.test/api/graphs/private
    default_auth: public
    auth: {public: {provider: anonymous}}
"#,
    )
    .unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    let scope = capture_server_scope(
        &input,
        &ServerAuth::Token {
            owner: "parent".into(),
        },
        "https://example.test/api/",
    )
    .unwrap();
    std::fs::write(path, "version: 1\n").unwrap();
    for route in ["healthz", "graphs/public/query"] {
        assert!(
            scope
                .validate(
                    &RequestTarget::of(
                        "https://example.test/api/",
                        &format!("https://example.test/api/{route}"),
                    )
                    .unwrap()
                )
                .is_ok()
        );
    }
    let failure = scope
        .validate(
            &RequestTarget::of(
                "https://example.test/api/",
                "https://example.test/api/graphs/private/query",
            )
            .unwrap(),
        )
        .unwrap_err();
    assert!(failure.to_string().contains("credential_scope_mismatch"));
}

#[test]
fn inline_provider_capture_has_one_destination_and_rejects_ambiguous_aliases() {
    let s3 = BTreeMap::from([
        ("AWS_REGION".into(), "us-west-2".into()),
        ("AWS_ENDPOINT_URL_S3".into(), "http://127.0.0.1:9000".into()),
        ("AWS_ENDPOINT_URL".into(), "https://ignored.example".into()),
        ("AWS_S3_FORCE_PATH_STYLE".into(), "true".into()),
    ]);
    assert_eq!(
        StorageProvider::capture_from("s3://bucket/graph", &s3).unwrap(),
        StorageProvider::S3 {
            region: "us-west-2".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            virtual_hosted_style: false,
        }
    );
    let azure = BTreeMap::from([
        (
            "AZURE_STORAGE_ACCOUNT_NAME".into(),
            "devstoreaccount1".into(),
        ),
        ("AZURE_STORAGE_USE_EMULATOR".into(), "true".into()),
        (
            "AZURITE_BLOB_STORAGE_URL".into(),
            "http://127.0.0.1:12345".into(),
        ),
    ]);
    assert_eq!(
        StorageProvider::capture_from("az://graphs/graph", &azure).unwrap(),
        StorageProvider::Azure {
            account_name: "devstoreaccount1".into(),
            endpoint: Some("http://127.0.0.1:12345".into()),
            emulator: true,
        }
    );
    for settings in [
        BTreeMap::from([("REGION".into(), "us-west-2".into())]),
        BTreeMap::from([
            ("AWS_REGION".into(), "us-west-2".into()),
            ("AWS_PROFILE".into(), "prod".into()),
        ]),
    ] {
        assert!(StorageProvider::capture_from("s3://bucket/graph", &settings).is_err());
    }
    let mut conflicting = azure;
    conflicting.insert("AZURE_ENDPOINT".into(), "https://other.example".into());
    assert!(StorageProvider::capture_from("az://graphs/graph", &conflicting).is_err());
}
#[test]
fn config_edit_lock_excludes_another_handle_and_releases_on_drop() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.yaml");
    let first = super::ConfigEditLock::acquire(&path).unwrap();
    #[cfg(unix)]
    let inherited = first.file.try_clone().unwrap();
    assert!(super::ConfigEditLock::acquire(&path).is_err());
    drop(first);
    let successor = super::ConfigEditLock::acquire(&path).unwrap();
    #[cfg(unix)]
    drop(inherited);
    assert!(super::ConfigEditLock::acquire(&path).is_err());
    drop(successor);
    assert!(super::ConfigEditLock::acquire(&path).is_ok());
}
