use super::*;
use crate::operator::{InvocationConfig, parse_operator_config};

fn config(body: &str) -> OperatorConfig {
    parse_operator_config(&format!("target_config_version: 1\n{body}")).unwrap()
}
fn entry(json: &str) -> TargetEntry {
    serde_json::from_str(json).unwrap()
}

#[test]
fn structural_config_errors_refuse_but_unselected_incomplete_entries_do_not() {
    for text in [
        "servers: {}",
        "profiles: {}",
        "clusters: {}",
        "operator: {actor: me}",
        "target_config_version: 2",
        "target_config_version: 1\nservers: {}",
        "target_config_version: 1\ndefaults: {store: /tmp/g}",
        "target_config_version: 1\nunknown: true",
        "target_config_version: 1\ntargets: {a: {kind: direct, typo: true}}",
        "target_config_version: 1\ntargets:\n  a: {kind: direct}\n  a: {kind: server}",
        "target_config_version: 1\ntargets: {og_catalog: {kind: direct}}",
    ] {
        assert!(parse_operator_config(text).is_err(), "accepted {text}");
    }
    let config = config(
        "defaults: {target: missing}\ntargets:\n  good: {kind: direct, scope: graph, uri: /tmp/g}\n  incomplete: {kind: server}\n",
    );
    assert!(matches!(
        config.binding("good").unwrap(),
        TargetBinding::DirectGraph { .. }
    ));
    assert!(config.binding("incomplete").is_err());
    assert!(config.binding("missing").is_err());
    let unused = super::tests::config(
        "defaults: {target: 'https://example.com'}\naliases: {bad: {target: './old', query: q}}\ntargets: {good: {kind: direct, scope: graph, uri: /tmp/g}}\n",
    );
    assert!(unused.binding("good").is_ok());
}

#[test]
fn target_variants_require_complete_nonconflicting_bindings() {
    for json in [
        r#"{"kind":"direct","scope":"graph","uri":"/tmp/g","default_graph":"g"}"#,
        r#"{"kind":"direct","scope":"catalogue","uri":"/tmp/g"}"#,
        r#"{"kind":"direct","scope":"graph","uri":"s3://bucket/prefix"}"#,
        r#"{"kind":"direct","scope":"graph","uri":"s3:bucket/path"}"#,
        r#"{"kind":"direct","scope":"graph","uri":"https:host"}"#,
        r#"{"kind":"direct","scope":"graph","uri":"file:/tmp/g"}"#,
        r#"{"kind":"server","endpoint":"https://example.com"}"#,
        r#"{"kind":"server","endpoint":"https://u:secret@example.com","auth":"anonymous"}"#,
        r#"{"kind":"server","endpoint":"https://example.com?q=secret","auth":"anonymous"}"#,
        r#"{"kind":"server","endpoint":"https://example.com","credential":"auth","auth":"anonymous"}"#,
        r#"{"kind":"managed","scope":"account","api":"https://api.example/path","organization_id":"org","control_auth":"human-device"}"#,
        r#"{"kind":"managed","scope":"account","api":"https://api.example","control_auth":"automation-env"}"#,
        r#"{"kind":"managed","scope":"cluster","api":"https://api.example","organization_id":"org","control_auth":"human-device","cluster_id":"c"}"#,
    ] {
        assert!(entry(json).binding("test").is_err(), "accepted {json}");
    }
    assert!(matches!(
        entry(r#"{"kind":"direct","scope":"catalogue"}"#)
            .binding("catalog")
            .unwrap(),
        TargetBinding::DirectCatalogue
    ));
    assert!(matches!(entry(r#"{"kind":"managed","scope":"account","api":"https://api.example","control_auth":"human-device"}"#).binding("login").unwrap(),TargetBinding::Managed(_)));
    let cloud=entry(r#"{"kind":"direct","scope":"root","uri":"s3://bucket/prefix","provider":{"kind":"s3","region":"us-east-1","endpoint":"https://s3.example"}}"#).binding("cloud").unwrap();
    assert!(matches!(
        cloud,
        TargetBinding::DirectRoot {
            provider: StorageProvider::S3 { .. },
            ..
        }
    ));
}

#[test]
fn stored_relative_paths_follow_the_operator_file() {
    assert!(normalize_storage_uri("./path:with-colon", Path::new("/tmp")).is_ok());
    let home = tempfile::tempdir().unwrap();
    std::fs::write(home.path().join("config.yaml"),"target_config_version: 1\ntargets:\n  local: {kind: direct, scope: graph, uri: ./graph.omni}\n").unwrap();
    let input = InvocationConfig::at(Some(home.path().into()), None);
    let TargetBinding::DirectGraph { uri, .. } = input
        .get(&mut Vec::new())
        .unwrap()
        .binding("local")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        uri,
        omnigraph::storage::normalize_root_uri(&home.path().join("graph.omni").to_string_lossy())
            .unwrap()
    );
}

#[test]
fn invocation_keeps_one_success_or_failure_snapshot() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    let input = InvocationConfig::at(Some(home.path().into()), Some("retired".into()));
    assert!(input.has_profile());
    std::fs::write(&path, "broken: [").unwrap();
    let failure = input.get(&mut Vec::new()).unwrap_err();
    assert_eq!(
        failure.downcast_ref::<TargetError>().unwrap().code,
        "target_config_invalid"
    );
    let failure = failure.to_string();
    std::fs::write(
        &path,
        "target_config_version: 1\noperator: {actor: first}\n",
    )
    .unwrap();
    assert_eq!(input.get(&mut Vec::new()).unwrap_err().to_string(), failure);
    let fresh = InvocationConfig::at(Some(home.path().into()), None);
    assert_eq!(fresh.get(&mut Vec::new()).unwrap().actor(), Some("first"));
    std::fs::write(
        &path,
        "target_config_version: 1\noperator: {actor: second}\n",
    )
    .unwrap();
    assert_eq!(fresh.get(&mut Vec::new()).unwrap().actor(), Some("first"));
    let header = "target_config_version: 1\n#";
    for length in [
        crate::operator::OPERATOR_FILE_MAX_BYTES,
        crate::operator::OPERATOR_FILE_MAX_BYTES + 1,
    ] {
        std::fs::write(
            &path,
            format!("{header}{}", "x".repeat(length - header.len())),
        )
        .unwrap();
        let result = crate::operator::load_operator_config_at(&path);
        if length == crate::operator::OPERATOR_FILE_MAX_BYTES {
            assert!(result.is_ok());
        } else {
            assert!(result.unwrap_err().to_string().contains("exceeds 1 MiB"));
        }
    }
}

#[test]
fn credentials_match_endpoint_and_path_boundaries() {
    let cfg = config(
        "targets:\n  root: {kind: server, endpoint: 'https://example.com/api', credential: root-auth}\n  child: {kind: server, endpoint: 'https://example.com/api/private', credential: private-auth}\n  malformed-child: {kind: server, endpoint: 'https://example.com/api/incomplete', default_graph: 'bad/graph'}\n  anon: {kind: server, endpoint: 'https://example.com/public', auth: anonymous}\n",
    );
    for (url, owner) in [
        ("https://example.com/api/graphs/g", Some("root-auth")),
        (
            "https://example.com/api/private/graphs/g",
            Some("private-auth"),
        ),
        ("https://example.com/apievil", None),
        ("https://example.com/api/../other", None),
        ("https://evil.example/api", None),
    ] {
        let actual = cfg.credential_owner_for(&RequestTarget::of(url, url).unwrap());
        assert_eq!(actual, owner.map(CredentialOwner::Keyed));
    }
    assert_eq!(
        cfg.credential_owner_for(
            &RequestTarget::of(
                "https://example.com/api/incomplete",
                "https://example.com/api/incomplete/graphs/g"
            )
            .unwrap()
        ),
        Some(CredentialOwner::Anonymous {
            target: "malformed-child"
        })
    );
    for targets in [
        "a: {kind: server, endpoint: 'https://a.example', credential: auth}\n  b: {kind: server, endpoint: 'https://b.example', credential: auth}",
        "a: {kind: server, endpoint: 'https://a.example', credential: auth-a}\n  b: {kind: server, endpoint: 'https://a.example', credential: auth-b}",
        "a: {kind: server, endpoint: 'https://a.example', credential: prod-a}\n  b: {kind: server, endpoint: 'https://b.example', credential: prod_a}",
    ] {
        assert!(
            parse_operator_config(&format!(
                "target_config_version: 1\ntargets:\n  {targets}\n"
            ))
            .is_err()
        );
    }
}

#[test]
fn saved_credentials_cannot_follow_an_edited_endpoint() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    std::fs::write(&path,"target_config_version: 1\ntargets:\n  a: {kind: server, endpoint: 'https://a.example/api', credential: isolated-test-owner}\n").unwrap();
    let input = InvocationConfig::at(Some(home.path().into()), None);
    write_server_credential(
        &input,
        "isolated-test-owner",
        "https://a.example/api",
        Some("secret"),
    )
    .unwrap();
    let auth = ServerAuth::Token {
        owner: "isolated-test-owner".into(),
    };
    assert_eq!(
        resolve_server_token(&input, &auth, "https://a.example/api/graphs/g")
            .unwrap()
            .as_deref(),
        Some("secret")
    );
    std::fs::write(&path,"target_config_version: 1\ntargets:\n  a: {kind: server, endpoint: 'https://b.example/api', credential: isolated-test-owner}\n").unwrap();
    let fresh = InvocationConfig::at(Some(home.path().into()), None);
    assert!(
        resolve_server_token(&fresh, &auth, "https://b.example/api")
            .unwrap_err()
            .to_string()
            .contains("credential_scope_mismatch")
    );
    assert_eq!(
        crate::operator::read_credential_at(
            &home.path().join("credentials"),
            "isolated-test-owner"
        )
        .unwrap()
        .as_deref(),
        Some("secret")
    );
}

#[test]
fn edits_preserve_other_text_and_refuse_default_removal_in_command() {
    let before = "# operator comment\ntarget_config_version: 1\noperator:\n  actor: alice # keep\ntargets: {}\ndefaults:\n  output: json\n";
    let target = entry(r#"{"kind":"direct","scope":"graph","uri":"/tmp/g"}"#);
    let after = add_entry_text(before, "local", &target).unwrap();
    let expected = BTreeMap::from([("local".into(), target)]);
    verify_edit(before, &after, &expected).unwrap();
    assert!(after.contains("  actor: alice # keep\n"));
    let removed = remove_entry_text(&after, "local").unwrap();
    verify_edit(&after, &removed, &BTreeMap::new()).unwrap();
    assert_eq!(before, removed);
    assert!(
        add_entry_text(
            "target_config_version: 1\ntargets: {a: {kind: server}}\n",
            "local",
            expected.get("local").unwrap()
        )
        .is_err()
    );
}

#[test]
fn explicit_import_preserves_bindings_defaults_and_aliases_or_refuses_ambiguity() {
    let before = "servers:\n  prod: {url: 'https://prod.example'}\nprofiles:\n  analyst: {server: prod, default_graph: knowledge}\nclusters:\n  local: {root: /srv/graphs}\ndefaults:\n  server: prod\n  output: json\naliases:\n  people: {server: prod, graph: knowledge, query: people}\n";
    let imported = import::convert(before, Path::new("/tmp")).unwrap();
    let config = parse_operator_config(&imported.text).unwrap();
    assert_eq!(config.default_target(), Some("prod"));
    assert_eq!(config.aliases["people"].target, "prod");
    assert_eq!(config.targets.len(), 3);
    assert_eq!(
        imported.credentials,
        vec![("prod".into(), "https://prod.example".into())]
    );
    assert!(
        matches!(config.binding("analyst").unwrap(),TargetBinding::Server{default_graph:Some(graph),..} if graph=="knowledge")
    );
    for bad in [
        "servers: {x: {url: 'https://a.example'}}\nclusters: {x: {root: /tmp/x}}",
        "defaults: {store: ./g}",
        "profiles: {x: {server: missing}}",
    ] {
        assert!(
            import::convert(bad, Path::new("/tmp")).is_err(),
            "accepted {bad}"
        );
    }
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

#[cfg(unix)]
#[test]
fn unix_credential_matching_keeps_socket_separate_from_request_path() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(home.path().join("config.yaml"), "target_config_version: 1\ntargets:\n  unix: {kind: server, endpoint: 'unix:///tmp/target-credential.sock', credential: unix-test-owner}\n").unwrap();
    let input = InvocationConfig::at(Some(home.path().into()), None);
    write_server_credential(
        &input,
        "unix-test-owner",
        "unix:///tmp/target-credential.sock",
        Some("socket-token"),
    )
    .unwrap();
    let auth = ServerAuth::Token {
        owner: "unix-test-owner".into(),
    };
    assert_eq!(
        resolve_server_token_for(
            &input,
            &auth,
            "unix:///tmp/target-credential.sock",
            "unix:///tmp/target-credential.sock/graphs/g"
        )
        .unwrap()
        .as_deref(),
        Some("socket-token")
    );
    assert!(
        resolve_server_token_for(
            &input,
            &auth,
            "unix:///tmp/other.sock",
            "unix:///tmp/other.sock/graphs/g"
        )
        .is_err()
    );
}

#[test]
fn captured_request_scope_refuses_nested_owner_and_keeps_its_snapshot() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    std::fs::write(&path, "target_config_version: 1\ntargets:\n  root: {kind: server, endpoint: 'https://example.com', credential: scope-test}\n  nested: {kind: server, endpoint: 'https://example.com/graphs/g/private', auth: anonymous, default_graph: 'bad/graph'}\n").unwrap();
    let input = InvocationConfig::at(Some(home.path().into()), None);
    let scope = capture_server_scope(
        &input,
        &ServerAuth::Token {
            owner: "scope-test".into(),
        },
        "https://example.com",
    )
    .unwrap();
    assert!(
        scope
            .validate(
                &RequestTarget::of("https://example.com", "https://example.com/graphs/g/read")
                    .unwrap()
            )
            .is_ok()
    );
    std::fs::write(&path, "target_config_version: 1\n").unwrap();
    for url in [
        "https://example.com/graphs/g/private/read",
        "https://elsewhere.example/graphs/g/read",
    ] {
        assert!(
            scope
                .validate(&RequestTarget::of("https://example.com", url).unwrap())
                .is_err()
        );
    }
}
