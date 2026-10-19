use super::*;
use crate::planes::Capability;
use crate::scope::{ResolvedScope, ScopeFlags, resolve_scope};

fn parse(text: &str) -> Result<OperatorConfig> {
    crate::operator::parse_operator_config(text)
}

fn code(error: &color_eyre::Report) -> Option<ErrorCode> {
    error
        .downcast_ref::<CliRefusal>()
        .map(|refusal| refusal.code)
}

const TARGETS: &str = "target_config_version: 1
defaults:
  target: staging
targets:
  local:
    kind: direct
    scope: graph
    uri: file:///data/graph.omni
  ops:
    kind: direct
    scope: root
    uri: /data/cluster
    default_graph: knowledge
  staging:
    kind: server
    endpoint: https://staging.example
    credential: staging-auth
    default_graph: knowledge
  public:
    kind: server
    endpoint: https://public.example/base
    auth: anonymous
";

#[test]
fn an_entry_is_a_complete_binding_only_when_its_fields_make_one() {
    let config = parse(TARGETS).unwrap();
    assert_eq!(
        config.target("local").unwrap().binding("local").unwrap(),
        TargetBinding::DirectGraph {
            uri: "file:///data/graph.omni".into()
        }
    );
    assert_eq!(
        config
            .target("staging")
            .unwrap()
            .binding("staging")
            .unwrap(),
        TargetBinding::Server {
            endpoint: "https://staging.example".into(),
            auth: ServerAuth::Token {
                owner: "staging-auth".into()
            },
            default_graph: Some("knowledge".into()),
        }
    );
    let refused = config.target("nope").unwrap_err();
    assert_eq!(code(&refused), Some(ErrorCode::TargetUnknown), "{refused}");

    for (entry, why) in [
        ("kind: direct\nuri: /data/g", "scope"),
        ("kind: direct\nscope: graph", "uri"),
        ("kind: direct\nscope: graph\nuri: relative/g", "absolute"),
        ("kind: direct\nscope: graph\nuri: http://x/g", "http://"),
        (
            "kind: direct\nscope: graph\nuri: /g\ndefault_graph: k",
            "default_graph",
        ),
        (
            "kind: direct\nscope: root\nuri: /c\ncredential: a",
            "credential",
        ),
        ("kind: server\ncredential: a", "endpoint"),
        ("kind: server\nendpoint: https://x", "credential"),
        (
            "kind: server\nendpoint: https://x\ncredential: a\nauth: anonymous",
            "not both",
        ),
        (
            "kind: server\nendpoint: https://x\ncredential: 'a b'",
            "credential owner",
        ),
        (
            "kind: server\nendpoint: ftp://x\nauth: anonymous",
            "server scheme",
        ),
        (
            "kind: server\nendpoint: https://x\nscope: graph\nauth: anonymous",
            "scope",
        ),
        (
            "kind: server\nendpoint: https://x\nauth: anonymous\ndefault_graph: 'a/b'",
            "graph",
        ),
        // An endpoint is a whole server URL that holds no secret.
        (
            "kind: server\nendpoint: 'https://user:secret-token@h.example/g'\nauth: anonymous",
            "no user or password",
        ),
        (
            "kind: server\nendpoint: 'https://h.example/g?access=secret-token'\nauth: anonymous",
            "no query",
        ),
        (
            "kind: server\nendpoint: 'https://'\nauth: anonymous",
            "invalid server address",
        ),
        // Cloud storage takes its endpoint and account from the environment,
        // which this format cannot pin, so a cloud target is incomplete.
        (
            "kind: direct\nscope: graph\nuri: s3://bucket/graph",
            "cloud target",
        ),
        (
            "kind: direct\nscope: root\nuri: az://container/root",
            "cloud target",
        ),
        (
            "kind: direct\nscope: graph\nuri: 'file:///data/g?secret-token'",
            "no query",
        ),
    ] {
        let entry: TargetEntry = yaml_serde::from_str(entry).unwrap();
        let refused = entry.binding("t").unwrap_err();
        assert_eq!(code(&refused), Some(ErrorCode::TargetInvalid), "{refused}");
        assert!(refused.to_string().contains(why), "{why}: {refused}");
        assert!(!refused.to_string().contains("secret-token"), "{refused}");
    }
    // A unix socket endpoint is a server address too.
    #[cfg(unix)]
    {
        let entry: TargetEntry =
            yaml_serde::from_str("kind: server\nendpoint: unix:///run/og.sock\nauth: anonymous")
                .unwrap();
        entry.binding("t").unwrap();
    }
    // An incomplete entry does not block the others, and an unknown field
    // refuses the file.
    let mixed = format!("{TARGETS}  broken:\n    kind: server\n");
    let config_with_broken = parse(&mixed).unwrap();
    config_with_broken
        .target("staging")
        .unwrap()
        .binding("staging")
        .unwrap();
    assert!(
        parse(&format!(
            "{TARGETS}  typo:\n    kind: server\n    endpoints: https://x\n"
        ))
        .is_err()
    );
}

#[test]
fn the_file_declares_its_format_one_default_and_one_credential_per_endpoint() {
    for (text, expect) in [
        (
            "targets:\n  a:\n    kind: server\n    endpoint: https://a\n    auth: anonymous\n",
            "add `target_config_version: 1`",
        ),
        ("target_config_version: 2\n", "not supported"),
        (
            "target_config_version: 1\ndefaults:\n  target: a\n  server: s\n",
            "keep one default",
        ),
        (
            "target_config_version: 1\ntargets:\n  'bad name':\n    kind: server\n    endpoint: https://a\n    auth: anonymous\n",
            "target name",
        ),
        // One owner, two endpoints.
        (
            "target_config_version: 1\ntargets:\n  a:\n    kind: server\n    endpoint: https://a\n    credential: o\n  b:\n    kind: server\n    endpoint: https://b\n    credential: o\n",
            "different endpoints",
        ),
        // One endpoint, two owners.
        (
            "target_config_version: 1\ntargets:\n  a:\n    kind: server\n    endpoint: https://a\n    credential: o\n  b:\n    kind: server\n    endpoint: https://a/\n    credential: p\n",
            "same endpoint",
        ),
        // Anonymous beside a keyed binding of the same endpoint.
        (
            "target_config_version: 1\nservers:\n  s:\n    url: https://a\ntargets:\n  b:\n    kind: server\n    endpoint: https://a\n    auth: anonymous\n",
            "same endpoint",
        ),
        // A server's name used as an owner at another endpoint.
        (
            "target_config_version: 1\nservers:\n  s:\n    url: https://a\ntargets:\n  b:\n    kind: server\n    endpoint: https://b\n    credential: s\n",
            "different endpoints",
        ),
        // Two owners spelling one token variable.
        (
            "target_config_version: 1\ntargets:\n  a:\n    kind: server\n    endpoint: https://a\n    credential: o-p\n  b:\n    kind: server\n    endpoint: https://b\n    credential: o_p\n",
            "token variable",
        ),
    ] {
        let refused = parse(text).unwrap_err();
        assert_eq!(
            code(&refused),
            Some(ErrorCode::TargetInvalid),
            "{text}: {refused}"
        );
        assert!(refused.to_string().contains(expect), "{expect}: {refused}");
    }
    // Two targets may share an owner at one endpoint, and a target may use a
    // server's credential at that server's url.
    parse(
        "target_config_version: 1\nservers:\n  s:\n    url: https://a\ntargets:\n  a:\n    kind: server\n    endpoint: https://a\n    credential: s\n    default_graph: one\n  b:\n    kind: server\n    endpoint: https://a\n    credential: s\n    default_graph: two\n",
    )
    .unwrap();
}

#[test]
fn a_target_token_goes_only_to_urls_under_its_endpoint() {
    let config = parse(&format!(
        "{TARGETS}servers:\n  deep:\n    url: https://staging.example/graphs/special\n"
    ))
    .unwrap();
    let owner = |url: &str| {
        let request = RequestTarget::of(url, url).unwrap();
        config.credential_owner_for(&request)
    };
    assert_eq!(
        owner("https://staging.example/graphs/knowledge"),
        Some(CredentialOwner::Keyed("staging-auth"))
    );
    // The deepest binding wins, whichever section declares it.
    assert_eq!(
        owner("https://staging.example/graphs/special/query"),
        Some(CredentialOwner::Keyed("deep"))
    );
    assert_eq!(
        owner("https://public.example/base/graphs/g"),
        Some(CredentialOwner::Anonymous { target: "public" })
    );
    assert_eq!(owner("https://public.example/elsewhere"), None);
    assert_eq!(owner("https://staging.example.evil/graphs/g"), None);

    // An entry that fails validation supplies no credential, though the file
    // still loads: neither one sorting first at the anonymous target's
    // endpoint nor one bound deeper below it sends `staging-auth` there.
    let config = parse(&format!(
        "{TARGETS}  aaa:\n    kind: server\n    scope: graph\n    endpoint: https://public.example/base\n    credential: staging-auth\n  \
         deeper:\n    kind: server\n    scope: graph\n    endpoint: https://public.example/base/graphs\n    credential: staging-auth\n"
    ))
    .unwrap();
    let owner = |url: &str| {
        let request = RequestTarget::of(url, url).unwrap();
        config.credential_owner_for(&request)
    };
    assert_eq!(
        owner("https://public.example/base/graphs/g"),
        Some(CredentialOwner::Anonymous { target: "public" })
    );
}

fn resolve(
    config: &OperatorConfig,
    capability: Capability,
    flags: ScopeFlags<'_>,
) -> Result<ResolvedScope> {
    resolve_scope(config, None, capability, flags)
}

fn flags<'a>() -> ScopeFlags<'a> {
    ScopeFlags {
        target: None,
        profile: None,
        store: None,
        server: None,
        cluster: None,
        graph: None,
        uri: None,
        reads_root: true,
    }
}

#[test]
fn a_target_resolves_to_the_address_its_binding_names() {
    let config = parse(TARGETS).unwrap();
    // The default target, with its default graph; `--graph` overrides it.
    assert_eq!(
        resolve(&config, Capability::Any, flags()).unwrap(),
        ResolvedScope {
            server: Some("https://staging.example".into()),
            graph: Some("knowledge".into()),
            target: Some("staging".into()),
            ..Default::default()
        }
    );
    assert_eq!(
        resolve(
            &config,
            Capability::Any,
            ScopeFlags {
                graph: Some("other"),
                ..flags()
            }
        )
        .unwrap()
        .graph
        .as_deref(),
        Some("other")
    );
    assert_eq!(
        resolve(
            &config,
            Capability::Any,
            ScopeFlags {
                target: Some("local"),
                ..flags()
            }
        )
        .unwrap(),
        ResolvedScope {
            uri: Some("file:///data/graph.omni".into()),
            target: Some("local".into()),
            ..Default::default()
        }
    );
    assert_eq!(
        resolve(
            &config,
            Capability::Direct,
            ScopeFlags {
                target: Some("ops"),
                ..flags()
            }
        )
        .unwrap(),
        ResolvedScope {
            cluster: Some("/data/cluster".into()),
            cluster_graph: Some("knowledge".into()),
            target: Some("ops".into()),
            ..Default::default()
        }
    );
    // A target is a complete address: no other address or profile beside it.
    for (extra, name) in [
        (
            ScopeFlags {
                server: Some("s"),
                ..flags()
            },
            "--server",
        ),
        (
            ScopeFlags {
                store: Some("/g"),
                ..flags()
            },
            "--store",
        ),
        (
            ScopeFlags {
                profile: Some("p"),
                ..flags()
            },
            "--profile",
        ),
        (
            ScopeFlags {
                uri: Some("/g".into()),
                ..flags()
            },
            "a positional URI",
        ),
    ] {
        let refused = resolve(
            &config,
            Capability::Any,
            ScopeFlags {
                target: Some("local"),
                ..extra
            },
        )
        .unwrap_err();
        assert!(refused.to_string().contains(name), "{name}: {refused}");
    }
    // A direct graph already is one graph; a server target is not storage.
    assert!(
        resolve(
            &config,
            Capability::Any,
            ScopeFlags {
                target: Some("local"),
                graph: Some("g"),
                ..flags()
            }
        )
        .is_err()
    );
    assert!(
        resolve(
            &config,
            Capability::Direct,
            ScopeFlags {
                target: Some("staging"),
                ..flags()
            }
        )
        .is_err()
    );
    let refused = resolve(
        &config,
        Capability::Any,
        ScopeFlags {
            target: Some("missing"),
            ..flags()
        },
    )
    .unwrap_err();
    assert_eq!(code(&refused), Some(ErrorCode::TargetUnknown));
    // An explicit address still wins over the default target, and an
    // ambient profile beside it refuses.
    assert_eq!(
        resolve(
            &config,
            Capability::Any,
            ScopeFlags {
                store: Some("/g"),
                ..flags()
            }
        )
        .unwrap()
        .uri
        .as_deref(),
        Some("/g")
    );
    let refused = resolve_scope(&config, Some("ambient"), Capability::Any, flags()).unwrap_err();
    assert!(
        refused.to_string().contains("OMNIGRAPH_PROFILE"),
        "{refused}"
    );
}

fn entry(json: &str) -> TargetEntry {
    serde_json::from_str(json).unwrap()
}

#[test]
fn an_edit_changes_only_its_entry_and_keeps_the_rest_of_the_text() {
    let before = "# operator config\noperator:\n  actor: act-me  # me\nservers:\n  dev:\n    url: http://h:1\n";
    let server =
        entry(r#"{"kind":"server","endpoint":"https://staging.example","credential":"staging"}"#);
    let after = add_entry_text(before, "staging", &server).unwrap();
    assert!(after.starts_with(before), "{after}");
    let expected: BTreeMap<String, TargetEntry> = [("staging".to_string(), server.clone())].into();
    verify_edit(before, &after, &expected).unwrap();

    let graph = entry(r#"{"kind":"direct","scope":"graph","uri":"/data/g"}"#);
    let second = add_entry_text(&after, "local", &graph).unwrap();
    let mut both = expected.clone();
    both.insert("local".into(), graph);
    let parsed = verify_edit(&after, &second, &both).unwrap();
    assert_eq!(parsed.targets.len(), 2);

    let removed = remove_entry_text(&second, "local").unwrap();
    verify_edit(&second, &removed, &expected).unwrap();
    assert_eq!(removed, after);
    let empty = remove_entry_text(&removed, "staging").unwrap();
    verify_edit(&removed, &empty, &BTreeMap::new()).unwrap();
    assert!(empty.contains("targets: {}"), "{empty}");
    // An empty block takes the next entry.
    let again = add_entry_text(&empty, "staging", &server).unwrap();
    verify_edit(&empty, &again, &expected).unwrap();

    // A key line with a trailing comment is still a block.
    let annotated = "target_config_version: 1\ntargets:  # shared\n";
    let edited = add_entry_text(annotated, "staging", &server).unwrap();
    assert!(edited.starts_with(annotated), "{edited}");
    verify_edit(annotated, &edited, &expected).unwrap();

    // A block whose comments sit beside the entries keeps them.
    let commented = "target_config_version: 1\ntargets:\n  # the shared server\n  a:\n    kind: server\n    endpoint: https://a\n    auth: anonymous\n\ndefaults:\n  output: json\n";
    let edited = add_entry_text(commented, "staging", &server).unwrap();
    assert!(edited.contains("  # the shared server\n  a:\n"), "{edited}");
    let mut with_a = expected.clone();
    with_a.insert(
        "a".into(),
        entry(r#"{"kind":"server","endpoint":"https://a","auth":"anonymous"}"#),
    );
    verify_edit(commented, &edited, &with_a).unwrap();
}

#[test]
fn an_edit_refuses_a_file_it_cannot_change_line_by_line() {
    let server = entry(r#"{"kind":"server","endpoint":"https://s","auth":"anonymous"}"#);
    for text in [
        "target_config_version: 1\ntargets: {a: {kind: server, endpoint: 'https://a', auth: anonymous}}\n",
        "target_config_version: 1\ntargets:\n\ta:\n",
    ] {
        let refused = add_entry_text(text, "s", &server)
            .and_then(|after| verify_edit(text, &after, &BTreeMap::new()).map(|_| after))
            .unwrap_err();
        assert_eq!(
            code(&refused),
            Some(ErrorCode::ConfigNotEditable),
            "{text}: {refused}"
        );
    }
    // A change elsewhere in the text is caught by the check.
    let before = "operator:\n  actor: a\n";
    let tampered = "operator:\n  actor: b\ntarget_config_version: 1\ntargets: {}\n";
    let refused = verify_edit(before, tampered, &BTreeMap::new()).unwrap_err();
    assert_eq!(code(&refused), Some(ErrorCode::ConfigNotEditable));
}

#[test]
fn a_binding_digest_names_where_a_target_points_and_nothing_else() {
    let config = parse(TARGETS).unwrap();
    let digest = |name: &str| config.target(name).unwrap().binding(name).unwrap().digest();
    assert!(digest("staging").starts_with("sha256:"));
    assert_ne!(digest("staging"), digest("public"));
    // The default graph is not part of where a target points.
    let other_graph = TARGETS.replace(
        "    default_graph: knowledge\n  public:",
        "    default_graph: other\n  public:",
    );
    let other = crate::operator::parse_operator_config(&other_graph).unwrap();
    assert_eq!(
        other
            .target("staging")
            .unwrap()
            .binding("staging")
            .unwrap()
            .digest(),
        digest("staging")
    );
    // A direct target's storage is digested as the engine opens it, so two
    // spellings of one store are one binding.
    let direct = |uri: &str| {
        entry(&format!(
            r#"{{"kind":"direct","scope":"graph","uri":"{uri}"}}"#
        ))
        .binding("d")
        .unwrap()
        .digest()
    };
    assert_eq!(direct("/data/g"), direct("file:///data/g"));
    assert_eq!(direct("/data/g"), direct("/data/g/"));
    assert_ne!(direct("/data/g"), direct("/data/h"));
    let listing = list_output(&config);
    assert_eq!(listing.default_target.as_deref(), Some("staging"));
    let local = listing.targets.iter().find(|t| t.name == "local").unwrap();
    assert_eq!(local.local_servers.as_deref(), Some(&[][..]));
    let staging = listing
        .targets
        .iter()
        .find(|t| t.name == "staging")
        .unwrap();
    assert_eq!(staging.auth, Some("token"));
    assert!(staging.local_servers.is_none());
}

#[test]
fn a_listing_holds_no_secret_and_no_alias() {
    // An incomplete entry is listed with its error, its address without the
    // secret a complete binding would refuse.
    let config = parse(&format!(
        "{TARGETS}  leaky:\n    kind: server\n    endpoint: 'https://user:secret-token@h.example/g?k=secret-token'\n    auth: anonymous\naliases:\n  who:\n    server: dev\n    query: q\nservers:\n  dev:\n    url: https://dev.example\n"
    ))
    .unwrap();
    let listing = serde_json::to_value(list_output(&config)).unwrap();
    let text = listing.to_string();
    assert!(!text.contains("secret-token"), "{text}");
    let leaky = listing["targets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|target| target["name"] == "leaky")
        .unwrap();
    assert!(leaky["error"].is_string(), "{leaky}");
    assert!(leaky["binding"].is_null(), "{leaky}");
    assert!(listing.get("aliases").is_none(), "{listing}");
}
