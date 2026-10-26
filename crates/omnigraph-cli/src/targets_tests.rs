use super::*;
use crate::operator::{InvocationConfig, parse_operator_config};

#[test]
fn explicit_rename_and_principal_replacement_commands_parse_without_legacy_selectors() {
    for args in [
        vec!["config", "rename-profile", "old", "new"],
        vec!["config", "rename-target", "old", "new"],
        vec!["config", "recover-rename"],
        vec![
            "login",
            "--profile",
            "work",
            "--replace-principal",
            "principal_new",
        ],
    ] {
        assert!(crate::invocation::parse_from(std::iter::once("omnigraph").chain(args)).is_ok());
    }
    for args in [
        vec!["config", "rename-connection", "old", "new"],
        vec!["config", "rename-auth", "server", "served", "old", "new"],
        vec!["config", "rename-auth", "connection", "work", "old", "new"],
        vec!["config", "rename-auth", "profile", "work", "old", "new"],
        vec!["login", "--connection", "work"],
    ] {
        assert!(crate::invocation::parse_from(std::iter::once("omnigraph").chain(args)).is_err());
    }
    for args in [
        vec!["login", "--profile", "work", "--auth", "me"],
        vec!["logout", "--profile", "work", "--auth", "me"],
        vec!["login", "--auth", "me"],
        vec!["login", "--replace-principal", "principal_new"],
    ] {
        if let Ok(parsed) = crate::invocation::parse_from(std::iter::once("omnigraph").chain(args))
        {
            assert!(crate::planes::guard_addressing(&parsed.cli).is_err());
        }
    }
}

#[test]
fn standalone_target_rename_preserves_the_secret_store_binding() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("config.yaml"), "version: 1\ntargets:\n  old:\n    kind: server\n    endpoint: https://example.test/prefix\n    auth: {provider: token, source: {store: {}}}\n").unwrap();
    let resolve = |target: &str| {
        let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
        let cli = crate::invocation::parse_from([
            "omnigraph",
            "credentials",
            "status",
            "--target",
            target,
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
    let before = resolve("old");
    assert_eq!(before.name, "@target");
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    crate::operator_routing::rename(
        &input,
        crate::operator_routing::Rename::Target,
        "old",
        "new",
    )
    .unwrap();
    assert_eq!(resolve("new").owner, before.owner);
    assert!(before.ensure_current().is_ok());
}

#[tokio::test]
async fn anonymous_loopback_and_store_status_do_not_acquire_credentials() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("config.yaml"), "version: 1\ntargets:\n  public:\n    kind: server\n    endpoint: http://127.0.0.1:9000/prefix\n    auth: {provider: anonymous}\n  private:\n    kind: server\n    endpoint: https://example.test/prefix\n    auth: {provider: token, source: {store: {}}}\n").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    for (name, expected) in [("public", "anonymous"), ("private", "unknown")] {
        let cli = crate::invocation::parse_from([
            "omnigraph",
            "credentials",
            "status",
            "--json",
            "--target",
            name,
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
                    name,
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
    std::fs::write(&path, "version: 1\nprofiles:\n  work:\n    api: https://api.example/prefix\n    organization_id: org_test\n    auth: {provider: omnigraph}\ntargets:\n  cloud:\n    kind: managed\n    profile: work\n    cluster_id: cluster_one\n    cluster_incarnation: incarnation_one\naliases:\n  search: {target: cloud, graph: knowledge, query: search}\ndefaults: {profile: work, target: cloud}\n").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    let before = registration_auth(&input, "work", "https://api.example/prefix").unwrap();
    assert_eq!(before.name, "auth");
    rename(&input, Rename::Profile, "work", "renamed").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    rename(&input, Rename::Target, "cloud", "production").unwrap();
    let input = InvocationConfig::at(Some(directory.path().to_owned()), None);
    let after = registration_auth(&input, "renamed", "https://api.example/prefix").unwrap();
    assert_eq!(before.owner, after.owner);
    assert_eq!(after.name, "auth");
    let bindings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(input.auth_state_path().unwrap()).unwrap()).unwrap();
    assert!(bindings["owners"].get("profile:work").is_none());
    let auth_bindings = bindings["owners"]["profile:renamed"]["profiles"]
        .as_object()
        .unwrap();
    assert_eq!(auth_bindings.len(), 1);
    assert!(auth_bindings.contains_key("auth"));
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
    assert_eq!(config.defaults.profile.as_deref(), Some("renamed"));
    assert_eq!(config.defaults.target.as_deref(), Some("production"));
    assert_eq!(
        config.profiles["renamed"].auth,
        crate::operator_schema::AuthProfile::Omnigraph
    );
    assert_eq!(config.aliases["search"].target, "production");
    assert!(
        matches!(&config.targets["production"], crate::operator_schema::Target::Managed { profile, .. } if profile == "renamed")
    );
}

#[test]
fn whole_document_validation_matches_discovery_protocols_issue_1127() {
    for text in [
        "target_config_version: 1",
        "target_config_version: 1\ntargets: {server: {kind: server, endpoint: 'https://example.com', credential: standalone}}",
        "version: 1\ntargets: {server: {kind: server, endpoint: 'https://example.com', auth: {operator: {provider: token, source: {store: {}}}}}}",
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

    // Published protocol-1 edge builds accepted the two incompatible server
    // documents above. Discovery must distinguish this inline-auth grammar;
    // its document version is independent of the cross-binary protocol.
    let config = parse_operator_config(
        "version: 1\ntargets: {server: {kind: server, endpoint: 'https://example.com', auth: {provider: token, source: {store: {}}}}}",
    )
    .unwrap();
    let document = config.schema.as_ref().unwrap();
    assert_eq!(document.version, 1);
    assert!(matches!(
        &document.targets["server"],
        crate::operator_schema::Target::Server {
            auth: crate::operator_schema::AuthProfile::Token {
                source: crate::operator_schema::TokenSource::Store { .. },
            },
            ..
        }
    ));
    let mut encoded = Vec::new();
    crate::commands::local::run_version(true, &mut encoded).unwrap();
    let discovery: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        discovery["protocols"],
        serde_json::json!({"studio": 1, "target": 2, "credential": 2, "access": 2}),
        "inline-auth configuration and credential sources require protocol 2"
    );
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
    std::fs::write(dir.path().join("chosen.yaml"), "version: 1\ntargets:\n  served:\n    kind: server\n    endpoint: https://example.test/prefix\n    auth: {provider: token, source: {file: ./secret}}\n").unwrap();
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
    } = auth
    else {
        panic!()
    };
    assert_eq!(file, &dir.path().join("secret"));
}

#[tokio::test]
async fn server_targets_capture_one_credential_and_never_fall_back() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("config.yaml"), "version: 1\ntargets:\n  first:\n    kind: server\n    endpoint: https://example.test/prefix\n    default_graph: knowledge\n    auth: {provider: token, source: {file: ./first}}\n  second:\n    kind: server\n    endpoint: https://example.test/prefix\n    default_graph: knowledge\n    auth: {provider: token, source: {file: ./second}}\n").unwrap();
    std::fs::write(dir.path().join("first"), "first-token\n").unwrap();
    let input = InvocationConfig::at(Some(dir.path().to_owned()), None);
    let resolve = |profile| {
        let cli =
            crate::invocation::parse_from(["omnigraph", "query", "-e", "q", "--target", profile])
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
    // A long-lived client reads the rotated source again for its endpoint
    // only, and the invocation's own credential is unchanged.
    let route = first.owner.route().to_owned();
    assert_eq!(
        first.reacquire(&route).await.unwrap().as_deref(),
        Some("rotated-token")
    );
    assert!(
        first
            .reacquire("https://example.test/sibling/")
            .await
            .is_err()
    );
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
fn selected_root_target_keeps_nested_boundaries_without_borrowing_other_targets() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.yaml");
    std::fs::write(
        &path,
        r#"version: 1
targets:
  parent:
    kind: server
    endpoint: https://example.test/api
    auth: {provider: token, source: {env: UNUSED_TOKEN}}
  alternate:
    kind: server
    endpoint: https://example.test/api
    auth: {provider: anonymous}
  child:
    kind: server
    endpoint: https://example.test/api/graphs/private
    auth: {provider: anonymous}
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
