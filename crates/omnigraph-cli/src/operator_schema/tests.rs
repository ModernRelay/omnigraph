use super::*;

const COMPLETE: &str = r#"
version: 1
profiles:
  service:
    api: HTTPS://Platform.Example:443/Control/V1
    organization_id: org_issued-123
    auth: {provider: omnigraph}
    transport:
      tls: {ca: {mode: append, file: ca.pem}}
      proxy: {mode: url, url: HTTP://Proxy.Example:80}
  stored:
    api: https://platform.example
    organization_id: org_stored
    auth: {provider: token, source: {store: {}}}
  environment:
    api: https://platform.example
    organization_id: org_environment
    auth: {provider: token, source: {env: _OG_TOKEN}}
  mounted:
    api: https://platform.example
    organization_id: org_mounted
    auth: {provider: token, source: {file: secrets/token}}
  helper:
    api: https://platform.example
    organization_id: org_helper
    auth: {provider: exec, command: [/missing/helper, get, ""]}
targets:
  cluster:
    kind: managed
    profile: service
    cluster_id: 123-issued-id
    cluster_incarnation: 456-issued-incarnation
    endpoint: https://Private.Example/Data/V1
    default_graph: 123-exact-graph
  control:
    kind: managed
    profile: service
    cluster_id: other
    cluster_incarnation: other-generation
  server:
    kind: server
    endpoint: https://server.example/prefix
    auth: {provider: anonymous}
    transport: {proxy: {mode: environment}}
  local:
    kind: direct
    scope: graph
    uri: file:///missing/graph.omni
  root:
    kind: direct
    scope: root
    uri: file:///missing/root
    default_graph: stored-graph
aliases:
  graph-query:
    target: local
    query: registry/query:1
    params:
      nullable: null
      nested: {values: [true, 1, 1.5, null, "value"]}
  cluster-query:
    target: cluster
    graph: alias-graph
    query: read_query
  root-query:
    target: root
    graph: pinned-graph
    query: read_query
defaults: {profile: service, target: cluster, output: jsonl}
"#;

fn decode(text: &str) -> Result<OperatorConfig, ConfigError> {
    parse(text, Path::new("/operator"))
}

fn reject(text: &str) {
    assert!(decode(text).is_err(), "unexpectedly accepted:\n{text}");
}

fn server(endpoint: &str, profile: &str) -> String {
    format!(
        "version: 1\ntargets:\n  server:\n    kind: server\n    endpoint: {endpoint:?}\n    auth: {profile}\n"
    )
}

#[test]
fn managed_id_boundaries_match_execution_issue_1115() {
    for (field, original, max_len) in [
        ("organization_id", "org_issued-123", 128),
        ("cluster_id", "123-issued-id", 256),
        ("cluster_incarnation", "456-issued-incarnation", 256),
    ] {
        for (value, accepted) in [
            ("A".to_owned(), true),
            ("0_-aZ".to_owned(), true),
            ("_".to_owned(), true),
            ("-".to_owned(), true),
            ("a".repeat(max_len), true),
            ("a".repeat(max_len + 1), false),
            (String::new(), false),
            ("org:issued/123".to_owned(), false),
            ("a.b".to_owned(), false),
            ("a/b".to_owned(), false),
            ("a\\b".to_owned(), false),
            ("a b".to_owned(), false),
            ("a\n".to_owned(), false),
            ("é".to_owned(), false),
        ] {
            let text = COMPLETE.replace(
                &format!("{field}: {original}"),
                &format!("{field}: {}", serde_json::to_string(&value).unwrap()),
            );
            let result = decode(&text);
            assert_eq!(result.is_ok(), accepted, "{field}={value:?}: {result:?}");
            assert_eq!(
                crate::helpers::valid_managed_identifier(&value, max_len),
                accepted,
                "{field}={value:?}"
            );
            if let Err(error) = result {
                assert!(error.to_string().contains(field), "{error}");
            }
        }
    }
}

#[test]
fn graph_id_boundaries_and_catalogue_scope_match_execution_issue_1115() {
    for value in [
        "a".to_owned(),
        "1graph".to_owned(),
        "-graph".to_owned(),
        "-".to_owned(),
        "Graphs".to_owned(),
        "a".repeat(64),
        "a".repeat(65),
        String::new(),
        "exact/graph:id".to_owned(),
        "graph_bad".to_owned(),
        "../graph".to_owned(),
        "a.b".to_owned(),
        "a\\b".to_owned(),
        "a b".to_owned(),
        "a\n".to_owned(),
        "é".to_owned(),
        "policies".to_owned(),
        "healthz".to_owned(),
        "openapi".to_owned(),
        "graphs".to_owned(),
        "og_catalog".to_owned(),
    ] {
        let real_graph = omnigraph_server::GraphId::try_from(value.as_str()).is_ok();
        let served_graph = real_graph || value == "og_catalog";
        assert_eq!(
            crate::helpers::require_graph_segment(&value).is_ok(),
            served_graph,
            "{value:?}"
        );
        let encoded = serde_json::to_string(&value).unwrap();
        for (original, replacement, accepted) in [
            (
                "default_graph: 123-exact-graph",
                format!("default_graph: {encoded}"),
                served_graph,
            ),
            (
                "endpoint: https://server.example/prefix",
                format!("endpoint: https://server.example/prefix\n    default_graph: {encoded}"),
                served_graph,
            ),
            (
                "default_graph: stored-graph",
                format!("default_graph: {encoded}"),
                real_graph,
            ),
            (
                "graph: alias-graph",
                format!("graph: {encoded}"),
                served_graph,
            ),
            (
                "graph: pinned-graph",
                format!("graph: {encoded}"),
                real_graph,
            ),
        ] {
            let result = decode(&COMPLETE.replace(original, &replacement));
            assert_eq!(result.is_ok(), accepted, "{original}={value:?}: {result:?}");
            if let Err(error) = result {
                assert!(error.to_string().contains("graph"), "{error}");
            }
        }
        let server_alias = COMPLETE
            .replace("target: cluster\n    graph:", "target: server\n    graph:")
            .replace("graph: alias-graph", &format!("graph: {encoded}"));
        assert_eq!(decode(&server_alias).is_ok(), served_graph, "{value:?}");
    }
}

#[test]
fn complete_document_resolves_paths_and_normalizes_routes_without_io() {
    let config = decode(COMPLETE).unwrap();
    let profile = &config.profiles["service"];
    assert_eq!(profile.api, "https://platform.example/Control/V1/");
    assert_eq!(profile.organization_id, "org_issued-123");
    assert_eq!(config.profiles.len(), 5);
    assert_eq!(profile.auth, AuthProfile::Omnigraph);
    assert_eq!(
        config.profiles["stored"].auth,
        AuthProfile::Token {
            source: TokenSource::Store { store: Empty {} },
        }
    );
    assert_eq!(
        config.profiles["environment"].auth,
        AuthProfile::Token {
            source: TokenSource::Env {
                env: "_OG_TOKEN".into()
            },
        }
    );
    assert_eq!(
        config.profiles["mounted"].auth,
        AuthProfile::Token {
            source: TokenSource::File {
                file: PathBuf::from("/operator/secrets/token")
            },
        }
    );
    assert_eq!(
        config.profiles["helper"].auth,
        AuthProfile::Exec {
            command: vec!["/missing/helper".into(), "get".into(), "".into()],
            timeout: "30s".into(),
        }
    );
    let transport = profile.transport.as_ref().unwrap();
    assert_eq!(
        transport.tls.as_ref().unwrap().ca.as_ref().unwrap().file,
        Path::new("/operator/ca.pem")
    );
    assert_eq!(
        transport.proxy,
        Proxy::Url {
            url: "http://proxy.example/".into()
        }
    );
    assert_eq!(config.defaults.output, OutputFormat::Jsonl);
    assert_eq!(
        config.aliases["graph-query"].params["nullable"],
        serde_json::Value::Null
    );
    assert_eq!(
        config.aliases["cluster-query"].graph.as_deref(),
        Some("alias-graph")
    );
    assert!(matches!(
        &config.targets["control"],
        Target::Managed { endpoint: None, .. }
    ));
    assert!(matches!(
        &config.targets["server"],
        Target::Server {
            auth: AuthProfile::Anonymous,
            ..
        }
    ));
    let serialized = yaml_serde::to_string(&config).unwrap();
    assert_eq!(decode(&serialized).unwrap(), config);
}

#[test]
fn minimal_document_and_output_defaults() {
    let config = decode("version: 1").unwrap();
    assert!(config.targets.is_empty());
    assert!(config.profiles.is_empty());
    assert_eq!(config.defaults.output, OutputFormat::Table);
    assert!(config.defaults.target.is_none());
    assert!(config.defaults.profile.is_none());
    for output in ["table", "json", "jsonl", "csv", "kv", "toon"] {
        assert!(decode(&format!("version: 1\ndefaults: {{output: {output}}}")).is_ok());
    }
    for output in ["yaml", "JSON", "null", "''", "3", "[]"] {
        reject(&format!("version: 1\ndefaults: {{output: {output}}}"));
    }
}

#[test]
fn document_grammar_is_strict() {
    for text in [
        "",
        "null",
        "[]",
        "{}",
        "version: 0",
        "version: 2",
        "version: '1'",
        "version: 1.0",
        "version: true",
        "version: -1",
        "version: 4294967296",
        "version: 1\nversion: 1",
        "version: 1\n---\nversion: 1",
        "version: 1\ntarget_config_version: 1",
        "target_config_version: 1",
        "version: 1\nservices: {}",
        "version: 1\nprofiles: null",
        "version: 1\nconnections: {}",
        "version: 1\ndefaults: {connection: service}",
        "version: 1\ndefaults: {profile: null}",
        "version: 1\ntargets: null",
        "version: 1\naliases: null",
        "version: 1\ndefaults: null",
        "version: 1\ndefaults: {target: null}",
        "version: 1\ndefaults: {auth: me}",
        "version: 1\n1: {}",
        "version: 1\n<<: {}",
        "version: !!int 1",
        "!!map {version: 1}",
        "version: !custom 1",
        "version: !<tag:yaml.org,2002:int> 1",
        "version: ! 1",
        "%TAG !e! tag:yaml.org,2002:\n---\nversion: !e!int 1",
    ] {
        reject(text);
    }
    assert!(parse("version: 1", Path::new("relative")).is_err());
}

#[test]
fn duplicates_are_rejected_at_every_mapping_depth() {
    for (needle, replacement) in [
        (
            "organization_id: org_issued-123",
            "organization_id: org_issued-123\n    organization_id: org_issued-123",
        ),
        (
            "auth: {provider: omnigraph}",
            "auth: {provider: omnigraph}\n    auth: {provider: omnigraph}",
        ),
        (
            "auth: {provider: anonymous}",
            "auth: {provider: anonymous, provider: anonymous}",
        ),
        (
            "{provider: omnigraph}",
            "{provider: omnigraph, provider: omnigraph}",
        ),
        ("{store: {}}", "{store: {}, store: {}}"),
        (
            "{mode: append, file: ca.pem}",
            "{mode: append, mode: replace, file: ca.pem}",
        ),
        ("nullable: null", "nullable: null\n      nullable: 1"),
        (
            "{values: [true, 1, 1.5, null, \"value\"]}",
            "{key: 1, key: 2}",
        ),
    ] {
        reject(&COMPLETE.replacen(needle, replacement, 1));
    }
}

#[test]
fn names_and_references_are_validated_document_wide() {
    for bad in [
        "''",
        "1bad",
        "_bad",
        "bad/name",
        "bad:name",
        "éclair",
        "'bad name'",
    ] {
        reject(&COMPLETE.replacen("  service:", &format!("  {bad}:"), 1));
        reject(&COMPLETE.replacen("  local:", &format!("  {bad}:"), 1));
        reject(&COMPLETE.replacen("  graph-query:", &format!("  {bad}:"), 1));
    }
    for (needle, replacement) in [
        ("profile: service", "profile: missing"),
        (
            "endpoint: https://server.example/prefix",
            "endpoint: https://server.example/prefix\n    default_auth: missing",
        ),
        ("target: local", "target: missing"),
        (
            "profile: service, target: cluster",
            "profile: missing, target: cluster",
        ),
        (
            "target: cluster, output: jsonl",
            "target: missing, output: jsonl",
        ),
        ("organization_id: org_issued-123", "organization_id: ''"),
        ("cluster_id: other", "cluster_id: ''"),
        (
            "cluster_incarnation: other-generation",
            "cluster_incarnation: ''",
        ),
        ("default_graph: stored-graph", "default_graph: ''"),
    ] {
        reject(&COMPLETE.replacen(needle, replacement, 1));
    }
    assert!(decode(&COMPLETE.replace("service", "Service_1.x-y")).is_ok());
}

#[test]
fn required_fields_and_variant_boundaries_are_enforced() {
    for field in [
        "    api: HTTPS://Platform.Example:443/Control/V1\n",
        "    organization_id: org_issued-123\n",
        "    auth: {provider: omnigraph}\n",
        "    profile: service\n",
        "    cluster_id: other\n",
        "    cluster_incarnation: other-generation\n",
        "    endpoint: https://server.example/prefix\n",
        "    auth: {provider: anonymous}\n",
        "    scope: graph\n",
        "    uri: file:///missing/graph.omni\n",
    ] {
        reject(&COMPLETE.replacen(field, "", 1));
    }
    for extra in [
        "auth: {}",
        "default_auth: human",
        "scope: cluster",
        "uri: file:///tmp/root",
        "storage: {}",
    ] {
        reject(&COMPLETE.replacen(
            "    kind: managed",
            &format!("    kind: managed\n    {extra}"),
            1,
        ));
    }
    for extra in [
        "profile: service",
        "default_auth: public",
        "cluster_id: id",
        "scope: graph",
        "storage: {}",
    ] {
        reject(&COMPLETE.replacen(
            "    kind: server",
            &format!("    kind: server\n    {extra}"),
            1,
        ));
    }
    for extra in [
        "auth: {}",
        "transport: {}",
        "default_auth: human",
        "endpoint: https://a.example",
        "default_graph: x",
        "storage: {}",
    ] {
        reject(&COMPLETE.replacen(
            "    scope: graph",
            &format!("    scope: graph\n    {extra}"),
            1,
        ));
    }
    for kind in ["catalogue", "byoc", "account", "cluster", "Server"] {
        reject(&COMPLETE.replacen("kind: server", &format!("kind: {kind}"), 1));
    }
    reject(&COMPLETE.replace("scope: root", "scope: cluster"));
}

#[test]
fn auth_provider_and_source_variants_are_exact() {
    for auth in [
        "null",
        "{human: {provider: omnigraph}}",
        "{human: {provider: omnigraph}, machine: {provider: token, source: {store: {}}}}",
        "{provider: omnigraph, default_auth: human}",
    ] {
        reject(&COMPLETE.replacen("auth: {provider: omnigraph}", &format!("auth: {auth}"), 1));
    }
    reject(&COMPLETE.replacen(
        "auth: {provider: omnigraph}",
        "auth: {provider: omnigraph}\n    default_auth: human",
        1,
    ));
    reject(&COMPLETE.replacen("profile: service", "connection: service", 1));
    for profile in [
        "{}",
        "{public: {provider: anonymous}}",
        "{reader: {provider: token, source: {env: READER}}, writer: {provider: token, source: {env: WRITER}}}",
        "{provider: workload}",
        "{provider: omnigraph}",
        "{provider: anonymous, source: {env: X}}",
        "{provider: anonymous, command: [/bin/helper]}",
        "{provider: token}",
        "{provider: token, token: secret}",
        "{provider: token, source: {}}",
        "{provider: token, source: null}",
        "{provider: token, source: {store: null}}",
        "{provider: token, source: {store: {key: x}}}",
        "{provider: token, source: {store: {}, env: X}}",
        "{provider: token, source: {env: X, file: token}}",
        "{provider: token, source: {value: secret}}",
        "{provider: token, source: {keychain: {}}}",
        "{provider: token, source: {env: ''}}",
        "{provider: token, source: {env: 123}}",
        "{provider: token, source: {env: 9TOKEN}}",
        "{provider: token, source: {env: A-B}}",
        "{provider: token, source: {env: 'A B'}}",
        "{provider: token, source: {file: ''}}",
        "{provider: token, source: {file: null}}",
        "{provider: token, source: {file: []}}",
        "{provider: token, source: {env: X}, timeout: 30s}",
    ] {
        reject(&server("https://server.example", profile));
    }
    for profile in [
        "{provider: anonymous}",
        "{provider: token, source: {store: {}}}",
        "{provider: token, source: {env: A}}",
        "{provider: token, source: {env: _A9}}",
        "{provider: token, source: {file: relative}}",
    ] {
        assert!(
            decode(&server("https://server.example", profile)).is_ok(),
            "{profile}"
        );
    }
    reject(&COMPLETE.replace("auth: {provider: omnigraph}", "auth: {provider: anonymous}"));
    reject(&COMPLETE.replace(
        "auth: {provider: omnigraph}",
        "auth: {provider: omnigraph, issuer: https://example.com}",
    ));
    reject(
        "version: 1\nprofiles: {c: {api: 'https://example.com', organization_id: id, auth: {}}}",
    );
    reject("version: 1\ntargets: {s: {kind: server, endpoint: 'https://example.com', auth: {}}}");
}

#[test]
fn exec_argv_and_timeout_contract_is_bounded() {
    for profile in [
        "{provider: exec}",
        "{provider: exec, command: []}",
        "{provider: exec, command: /bin/helper}",
        "{provider: exec, command: [helper]}",
        "{provider: exec, command: ['./helper']}",
        "{provider: exec, command: ['']}",
        "{provider: exec, command: [/helper, null]}",
        "{provider: exec, command: [/helper, 12]}",
        "{provider: exec, command: [/helper], env: {TOKEN: secret}}",
    ] {
        reject(&server("https://example.com", profile));
    }
    for timeout in [
        "0s",
        "121s",
        "1m",
        "1.5s",
        "-1s",
        "+1s",
        "30",
        "null",
        "''",
        "999999999999999999s",
    ] {
        reject(&server(
            "https://example.com",
            &format!("{{provider: exec, command: [/helper], timeout: {timeout}}}"),
        ));
    }
    for timeout in ["1s", "30s", "120s"] {
        assert!(
            decode(&server(
                "https://example.com",
                &format!("{{provider: exec, command: [/missing/helper], timeout: {timeout}}}")
            ))
            .is_ok()
        );
    }
}

#[test]
fn aliases_bind_graphs_explicitly_and_accept_json_values() {
    for (needle, replacement) in [
        ("target: local", "target: local\n    graph: redundant"),
        ("    graph: alias-graph\n", ""),
        ("    graph: pinned-graph\n", ""),
        ("graph: alias-graph", "graph: ''"),
        ("query: read_query", "query: ''"),
        ("query: read_query", "query: read_query\n    auth: helper"),
        (
            "query: read_query",
            "query: read_query\n    command: [/helper]",
        ),
        ("nullable: null", "nullable: .nan"),
        ("nullable: null", "nullable: .inf"),
        ("nullable: null", "nullable: {1: value}"),
        ("nullable: null", "nullable: {<<: {x: 1}}"),
        ("nullable: null", "nullable: !!str value"),
        ("nullable: null", "nullable: !custom value"),
        ("nullable: null", "nullable: !!null null"),
        ("nullable: null", "nullable: !!seq [1, 2]"),
        ("nullable: null", "nullable: !!map {a: 1}"),
        ("nullable: null", "nullable: !<tag:yaml.org,2002:str> value"),
        (
            "params:\n      nullable: null\n      nested: {values: [true, 1, 1.5, null, \"value\"]}",
            "params: null",
        ),
    ] {
        reject(&COMPLETE.replacen(needle, replacement, 1));
    }
}

#[test]
fn exclamations_are_data_when_they_are_not_yaml_tags() {
    for value in [
        "'!literal'",
        "\"!literal\"",
        "hello!",
        "hello !world",
        "'!!str literal'",
        "\"\\x21escaped\"",
        "\"\\u0021escaped!\"",
        "'OmnigraphLiteralExclamation!'",
        "|\n        !!str literal\n        !more",
        ">\n        !literal\n        !!map",
    ] {
        let text = COMPLETE.replace("nullable: null", &format!("nullable: {value}"));
        assert!(decode(&text).is_ok(), "{value}: {:?}", decode(&text));
    }
    assert!(decode(&format!("{COMPLETE}\n# !!str harmless comment\n")).is_ok());
    assert!(
        decode(&COMPLETE.replace(
            "nullable: null",
            "nullable: \"\\x4fmnigraphLiteralExclamation!\""
        ))
        .is_ok()
    );
}

#[test]
fn remote_routes_refuse_ambiguous_or_credential_leaking_urls() {
    for endpoint in [
        "",
        "example.com",
        "/relative",
        "https:/example.com",
        "https://",
        "http://remote.example",
        "ftp://example.com",
        "unix:///tmp/socket",
        "https://user@example.com",
        "https://user:password@example.com",
        "https://@example.com",
        "https://example.com?",
        "https://example.com?q=1",
        "https://example.com#fragment",
        "https://example.com/a/../b",
        "https://example.com/a/./b",
        "https://example.com/%2e%2e/b",
        "https://example.com/a%2fb",
        "https://example.com/a%5Cb",
        "https://example.com/%252e%252e",
        "https://example.com/%00",
        "https://example.com/%FF",
        "https://example.com/%",
        "https://example.com/%xy",
        "https://example.com//prefix",
        "https://example.com/a\\b",
        "https://example.com/a b",
        " https://example.com",
        "https://%65xample.com",
        "https://example.com/%3fquery",
        "https://example.com/%23fragment",
    ] {
        reject(&server(endpoint, "{provider: anonymous}"));
    }
    for endpoint in [
        "https://EXAMPLE.com:443",
        "https://example.com/Case/Prefix",
        "https://example.com/caf%C3%A9",
        "https://[::1]:8443/api/",
    ] {
        assert!(
            decode(&server(endpoint, "{provider: anonymous}")).is_ok(),
            "{endpoint}"
        );
    }
}

#[test]
fn http_exception_is_only_for_anonymous_standalone_loopback() {
    for endpoint in [
        "http://localhost:8080",
        "http://127.0.0.1",
        "http://127.10.20.30",
        "http://[::1]/api",
    ] {
        assert!(decode(&server(endpoint, "{provider: anonymous}")).is_ok());
        reject(&server(endpoint, "{provider: token, source: {env: TOKEN}}"));
        reject(&COMPLETE.replace("HTTPS://Platform.Example:443/Control/V1", endpoint));
        reject(&COMPLETE.replace("https://Private.Example/Data/V1", endpoint));
    }
    for endpoint in [
        "http://localhost.example",
        "http://example.localhost",
        "http://192.168.1.1",
        "http://[::]",
    ] {
        reject(&server(endpoint, "{provider: anonymous}"));
    }
    let mixed = server("http://localhost", "{provider: anonymous}")
        + "      secret: {provider: token, source: {store: {}}}\n";
    reject(&mixed);
}

#[test]
fn direct_uris_are_local_absolute_file_uris() {
    for uri in [
        "./graph",
        "/graph",
        "file:relative",
        "file:/absolute",
        "file://remote/graph",
        "file://localhost/graph",
        "s3://bucket/graph",
        "az://container/graph",
        "https://example.com",
        "file:///tmp/graph?x",
        "file:///tmp/graph#x",
    ] {
        reject(&COMPLETE.replace("file:///missing/graph.omni", uri));
    }
    assert!(
        decode(&COMPLETE.replace(
            "file:///missing/graph.omni",
            "file:///tmp/space%20name.omni"
        ))
        .is_ok()
    );
}

#[test]
fn transport_variants_are_exact_and_do_not_probe_files_or_environment() {
    for transport in [
        "{insecure: true}",
        "{tls: {insecure: true}}",
        "{tls: {ca: {file: ca.pem}}}",
        "{tls: {ca: {mode: append}}}",
        "{tls: {ca: {mode: unknown, file: ca.pem}}}",
        "{tls: {ca: {mode: append, file: ''}}}",
        "{tls: {cert: cert.pem, key: key.pem}}",
        "{proxy: {}}",
        "{proxy: {mode: direct, url: 'http://proxy'}}",
        "{proxy: {mode: environment, url: 'http://proxy'}}",
        "{proxy: {mode: url}}",
        "{proxy: {mode: socks}}",
        "{proxy: {mode: url, url: 'socks5://proxy'}}",
        "{proxy: {mode: url, url: 'http://user:pass@proxy'}}",
        "{proxy: {mode: url, url: 'http://proxy/path'}}",
        "{proxy: {mode: url, url: 'https://proxy?query'}}",
        "{proxy: {mode: url, url: 'https://proxy#fragment'}}",
        "{proxy: null}",
        "{tls: null}",
        "{tls: {ca: null}}",
    ] {
        let text = server("https://example.com", "{provider: anonymous}")
            + &format!("    transport: {transport}\n");
        reject(&text);
    }
    for transport in [
        "{}",
        "{tls: {}}",
        "{proxy: {mode: direct}}",
        "{proxy: {mode: environment}}",
        "{proxy: {mode: url, url: 'https://proxy:8443/'}}",
        "{tls: {ca: {mode: replace, file: /missing/bundle.pem}}}",
    ] {
        let text = server("https://example.com", "{provider: anonymous}")
            + &format!("    transport: {transport}\n");
        assert!(decode(&text).is_ok(), "{transport}");
    }
}

#[test]
fn relative_paths_are_literal_and_bound_to_the_supplied_directory() {
    let config = decode(&server(
        "https://example.com",
        "{provider: token, source: {file: '~/literal/$TOKEN'}}",
    ))
    .unwrap();
    let Target::Server { auth, .. } = &config.targets["server"] else {
        panic!("server expected")
    };
    assert_eq!(
        *auth,
        AuthProfile::Token {
            source: TokenSource::File {
                file: PathBuf::from("/operator/~/literal/$TOKEN")
            }
        }
    );
}

#[test]
fn custom_destination_ca_refuses_explicit_https_proxy_document_wide() {
    for mode in ["append", "replace"] {
        for scheme in ["https", "HTTPS", "hTtPs"] {
            let transport = format!(
                "{{tls: {{ca: {{mode: {mode}, file: /missing/ca.pem}}}}, proxy: {{mode: url, url: '{scheme}://proxy.example:8443'}}}}"
            );
            let standalone = server("https://example.com", "{provider: anonymous}")
                + &format!("    transport: {transport}\n");
            assert!(
                decode(&standalone)
                    .unwrap_err()
                    .to_string()
                    .contains("custom destination tls.ca")
            );
            let unselected_target = COMPLETE.replace(
                "    transport: {proxy: {mode: environment}}",
                &format!("    transport: {transport}"),
            );
            assert!(
                decode(&unselected_target)
                    .unwrap_err()
                    .to_string()
                    .contains("targets.server.transport")
            );
            let managed = COMPLETE.replace(
                "    cluster_incarnation: other-generation",
                &format!("    cluster_incarnation: other-generation\n    transport: {transport}"),
            );
            assert!(
                decode(&managed)
                    .unwrap_err()
                    .to_string()
                    .contains("targets.control.transport")
            );
            let profile = format!(
                "version: 1\nprofiles:\n  unused:\n    api: https://example.com\n    organization_id: org\n    auth: {{provider: omnigraph}}\n    transport: {transport}\n"
            );
            assert!(
                decode(&profile)
                    .unwrap_err()
                    .to_string()
                    .contains("profiles.unused.transport")
            );
        }
        for proxy in [
            "{mode: url, url: 'http://proxy.example:8080'}",
            "{mode: url, url: 'HTTP://proxy.example:8080'}",
            "{mode: url, url: 'hTtP://proxy.example:8080'}",
            "{mode: environment}",
            "{mode: direct}",
        ] {
            let transport =
                format!("{{tls: {{ca: {{mode: {mode}, file: /missing/ca.pem}}}}, proxy: {proxy}}}");
            let text = server("https://example.com", "{provider: anonymous}")
                + &format!("    transport: {transport}\n");
            assert!(decode(&text).is_ok(), "{text}");
        }
    }
    for scheme in ["https", "HTTPS", "hTtPs"] {
        let text = server("https://example.com", "{provider: anonymous}")
            + &format!(
                "    transport: {{tls: {{}}, proxy: {{mode: url, url: '{scheme}://proxy.example'}}}}\n"
            );
        assert!(decode(&text).is_ok());
    }
}

#[test]
fn input_limit_is_inclusive_and_counts_utf8_bytes_before_parsing() {
    let mut text = "version: 1\n#".to_owned();
    text.push_str(&"x".repeat(MAX_CONFIG_BYTES - text.len()));
    assert_eq!(text.len(), MAX_CONFIG_BYTES);
    assert!(decode(&text).is_ok());
    text.push('x');
    assert!(decode(&text).unwrap_err().to_string().contains("1 MiB"));
    let text = "é".repeat(MAX_CONFIG_BYTES / 2 + 1);
    assert!(decode(&text).unwrap_err().to_string().contains("1 MiB"));
}

#[test]
fn public_unit_variants_round_trip_but_refuse_extra_fields() {
    for profile in [AuthProfile::Omnigraph, AuthProfile::Anonymous] {
        let json = serde_json::to_value(&profile).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 1);
        assert_eq!(
            serde_json::from_value::<AuthProfile>(json.clone()).unwrap(),
            profile
        );
        let yaml = yaml_serde::to_string(&profile).unwrap();
        assert_eq!(yaml_serde::from_str::<AuthProfile>(&yaml).unwrap(), profile);
        for extra in [serde_json::Value::Null, serde_json::json!({"env": "TOKEN"})] {
            let mut json = json.clone();
            json.as_object_mut().unwrap().insert("source".into(), extra);
            assert!(serde_json::from_value::<AuthProfile>(json.clone()).is_err());
            assert!(
                yaml_serde::from_str::<AuthProfile>(&yaml_serde::to_string(&json).unwrap())
                    .is_err()
            );
        }
    }
    for proxy in [Proxy::Direct, Proxy::Environment] {
        let json = serde_json::to_value(&proxy).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 1);
        assert_eq!(
            serde_json::from_value::<Proxy>(json.clone()).unwrap(),
            proxy
        );
        let yaml = yaml_serde::to_string(&proxy).unwrap();
        assert_eq!(yaml_serde::from_str::<Proxy>(&yaml).unwrap(), proxy);
        for extra in [serde_json::Value::Null, serde_json::json!("http://proxy")] {
            let mut json = json.clone();
            json.as_object_mut().unwrap().insert("url".into(), extra);
            assert!(serde_json::from_value::<Proxy>(json.clone()).is_err());
            assert!(yaml_serde::from_str::<Proxy>(&yaml_serde::to_string(&json).unwrap()).is_err());
        }
    }
}

#[test]
fn scanner_preserves_literal_bangs_without_expanding_yaml_simple_keys() {
    let key = "!".repeat(1000);
    let text = COMPLETE.replace("nullable: null", &format!("'{key}': '!!str !literal'"));
    let config = decode(&text).unwrap();
    assert_eq!(config.aliases["graph-query"].params[&key], "!!str !literal");
    for value in [
        "'!literal'",
        "\"\\x21literal!\"",
        "|\n        !!str value\n        !tag value",
        ">\n        !folded\n        !!map text",
        "'hello !<tag:yaml.org,2002:str>'",
        "plain ! text",
    ] {
        let config =
            decode(&COMPLETE.replace("nullable: null", &format!("nullable: {value}"))).unwrap();
        assert!(
            config.aliases["graph-query"].params["nullable"]
                .as_str()
                .unwrap()
                .contains('!')
        );
    }
    for value in [
        "!!str value",
        "!!int 1",
        "!!bool true",
        "!!float 1.5",
        "!!null null",
        "!!seq []",
        "!!map {}",
        "! value",
        "!<tag:yaml.org,2002:str> value",
    ] {
        reject(&COMPLETE.replace("nullable: null", &format!("nullable: {value}")));
    }
    reject("%TAG !e! tag:example.com,2026:\n---\nversion: 1\n");
}
