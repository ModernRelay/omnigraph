use super::*;
use crate::managed::auth::tests::MemoryStore;
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
use clap::Parser;
use omnigraph::db::ReadTarget;

const DATA_TOKEN: &str = "header.payload.signature";

fn context() -> Context {
    Context {
        version: 1,
        cluster: "cluster-a".into(),
        api: "https://control.example".into(),
    }
}

fn credential(context: &Context, endpoint: &str) -> Credential {
    Credential {
        version: 1,
        api: context.api.clone(),
        cluster_id: context.cluster.clone(),
        endpoint: endpoint.into(),
        token: DATA_TOKEN.into(),
        expires_at: (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&Rfc3339)
            .unwrap(),
        kid: "a".repeat(64),
        actor: "principal:alice".into(),
        grants: vec![Grant {
            graph_id: "knowledge".into(),
            actions: vec!["read".into(), "change".into(), "invoke_query".into()],
        }],
    }
}

fn save(store: &MemoryStore, context: &Context, credential: &Credential) {
    store
        .put(&key(context), &serde_json::to_string(credential).unwrap())
        .unwrap();
}

fn read_reply() -> Value {
    json!({"query_name":"q","target":{"branch":"main"},"row_count":1,"columns":["value"],"rows":[{"value":42}],"graph_commit_id":"head-a"})
}

fn change_reply() -> Value {
    json!({"branch":"main","query_name":"m","affected_nodes":1,"affected_edges":0,"actor_id":"principal:alice","commit":null})
}

#[test]
fn token_arguments_bound_authority_and_keep_direct_compatibility() {
    for (input, expected) in [
        ("60", 60),
        ("1m", 60),
        ("1h", 3600),
        ("24h", 86400),
        ("1d", 86400),
    ] {
        assert_eq!(parse_ttl(input).unwrap(), expected);
    }
    for bad in [
        "0",
        "59s",
        "25h",
        "999999999999999999999999h",
        "-1h",
        "1.5h",
        "",
    ] {
        assert!(parse_ttl(bad).is_err());
    }
    for bad in ["", "read,read", "read,admin", "schema_apply", "*", " read"] {
        assert!(requested_grant(Some("knowledge"), Some(bad)).is_err());
    }
    for bad in [
        "../graph",
        "graph_bad",
        "policies",
        "graphs",
        "κnowledge",
        "1graph",
        "-graph",
    ] {
        assert!(requested_grant(Some(bad), Some("read")).is_err());
    }
    assert!(Cli::try_parse_from(["omnigraph", "cluster", "token", "--clear"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "omnigraph",
            "cluster",
            "token",
            "--clear",
            "--actions",
            "read"
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from(["omnigraph", "cluster", "status", "--direct"])
            .unwrap()
            .direct
    );
    assert!(
        Cli::try_parse_from([
            "omnigraph",
            "query",
            "q",
            "--direct",
            "--server",
            "https://data.example"
        ])
        .unwrap()
        .direct
    );
}

#[tokio::test]
async fn minted_data_credential_is_separate_and_works_after_api_stops() {
    let data = IntentApiFixture::new(vec![
        IntentReply::json(200, read_reply()),
        IntentReply::json(200, change_reply()),
        IntentReply::json(200, read_reply()),
    ]);
    let mut context = context();
    let mut response_credential = credential(&context, &data.origin);
    response_credential.expires_at = (OffsetDateTime::now_utc() + time::Duration::seconds(3629))
        .format(&Rfc3339)
        .unwrap();
    let mut response = response_credential.metadata();
    response["token"] = json!(DATA_TOKEN);
    response["access_token"] = json!("must-not-be-output");
    let cp = IntentApiFixture::new(vec![IntentReply::json(
        200,
        json!({"data":response,"meta":{"cluster_id":context.cluster}}),
    )]);
    context.api = cp.origin.clone();
    let cp_store = MemoryStore::default();
    cp_store
        .put(&context.api, "unrelated-control-session")
        .unwrap();
    let data_store = MemoryStore::default();
    let api = Api::new(cp.origin.clone(), Some("control-session-secret".into())).unwrap();
    let output = mint(
        &data_store,
        &context,
        &api,
        response_credential.grants[0].clone(),
        3600,
    )
    .await
    .unwrap();
    let rendered = output.to_string();
    assert!(!rendered.contains(DATA_TOKEN));
    assert!(!rendered.contains("must-not-be-output"));
    assert_eq!(
        cp_store.get(&context.api).unwrap().as_deref(),
        Some("unrelated-control-session")
    );
    let requests = cp.requests();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v1/clusters/cluster-a/tokens");
    assert_eq!(
        requests[0].headers["authorization"],
        "Bearer control-session-secret"
    );
    assert_eq!(
        requests[0].body,
        json!({"grants":response_credential.grants,"ttl_seconds":3600})
    );
    cp.assert_complete();
    drop(cp);
    let client = load(
        &data_store,
        &context,
        "knowledge",
        &["read", "change", "invoke_query"],
    )
    .unwrap();
    let result = client
        .query(
            ReadTarget::Branch("main".into()),
            "query q() { return { 42 as value } }",
            Some("q"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows.get(), "[{\"value\":42}]");
    let changed = client
        .mutate("main", "mutation m() {}", Some("m"), None, Some("head-a"))
        .await
        .unwrap();
    assert_eq!(changed.actor_id.as_deref(), Some("principal:alice"));
    let _: omnigraph_api_types::ReadOutput = client
        .invoke_named("q", false, None, Some("main".into()), None, None)
        .await
        .unwrap();
    let requests = data.requests();
    assert_eq!(
        requests.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        [
            "/graphs/knowledge/query",
            "/graphs/knowledge/mutate/if-graph-commit",
            "/graphs/knowledge/queries/q"
        ]
    );
    for request in &requests {
        assert_eq!(
            request.headers["authorization"],
            format!("Bearer {DATA_TOKEN}")
        );
    }
    assert_eq!(requests[1].headers["omnigraph-if-graph-commit"], "head-a");
    data.assert_complete();
    let other = Context {
        cluster: "other-cluster".into(),
        ..context.clone()
    };
    data_store.put(&key(&other), "other-data-entry").unwrap();
    assert_eq!(
        clear(&data_store, &context).unwrap()["data"]["revocation_performed"],
        false
    );
    assert_eq!(
        load(&data_store, &context, "knowledge", &["read"])
            .err()
            .unwrap()
            .body["type"],
        "data_credential_required"
    );
    assert!(cp_store.get(&context.api).unwrap().is_some());
    assert_eq!(
        data_store.get(&key(&other)).unwrap().as_deref(),
        Some("other-data-entry")
    );
}

#[test]
fn cached_authority_refuses_wrong_bindings_expiry_and_extra_fields() {
    let context = context();
    let store = MemoryStore::default();
    let original = credential(&context, "https://data.example");
    let mut clock_ahead = credential(&context, "https://data.example");
    clock_ahead.expires_at = (OffsetDateTime::now_utc() + time::Duration::seconds(86429))
        .format(&Rfc3339)
        .unwrap();
    assert!(clock_ahead.validate(&context).is_ok());
    clock_ahead.expires_at = (OffsetDateTime::now_utc() + time::Duration::seconds(86460))
        .format(&Rfc3339)
        .unwrap();
    assert!(clock_ahead.validate(&context).is_err());
    save(&store, &context, &original);
    assert_eq!(
        load(&store, &context, "foreign", &["read"])
            .err()
            .unwrap()
            .body["type"],
        "data_scope_missing"
    );
    assert_eq!(
        load(&store, &context, "knowledge", &["export"])
            .err()
            .unwrap()
            .body["type"],
        "data_scope_missing"
    );
    let base = serde_json::to_value(&original).unwrap();
    for (field, value) in [
        ("version", json!(2)),
        ("api", json!("https://foreign.example")),
        ("cluster_id", json!("foreign")),
        ("endpoint", json!("https://data.example/path")),
        ("endpoint", json!("http://data.example")),
        ("endpoint", json!("https://user:secret@data.example")),
        ("token", json!("a.b")),
        ("token", json!("x".repeat(MAX_TOKEN + 1))),
        ("kid", json!("not-a-fingerprint")),
        (
            "expires_at",
            json!(
                (OffsetDateTime::now_utc() - time::Duration::seconds(1))
                    .format(&Rfc3339)
                    .unwrap()
            ),
        ),
        (
            "expires_at",
            json!(
                (OffsetDateTime::now_utc() + time::Duration::hours(25))
                    .format(&Rfc3339)
                    .unwrap()
            ),
        ),
        ("unknown", json!("authority")),
        (
            "grants",
            json!([{"graph_id":"knowledge","actions":["admin"]}]),
        ),
    ] {
        let mut corrupt = base.clone();
        corrupt[field] = value;
        store.put(&key(&context), &corrupt.to_string()).unwrap();
        assert!(
            load(&store, &context, "knowledge", &["read"]).is_err(),
            "accepted {field}"
        );
    }
    let duplicate = serde_json::to_string(&original)
        .unwrap()
        .replacen("{", "{\"version\":1,", 1);
    store.put(&key(&context), &duplicate).unwrap();
    assert!(load(&store, &context, "knowledge", &["read"]).is_err());
}

#[tokio::test]
async fn invalid_issuance_never_replaces_cached_authority() {
    for corruption in ["extra-action", "foreign-endpoint", "oversize-token"] {
        let mut context = context();
        let valid = credential(&context, "https://data.example");
        let mut response = valid.metadata();
        response["token"] = json!(DATA_TOKEN);
        match corruption {
            "extra-action" => response["grants"][0]["actions"] = json!(["read", "export"]),
            "foreign-endpoint" => {
                response["endpoint"] = json!("https://user:password@data.example/path")
            }
            _ => response["token"] = json!("x".repeat(MAX_TOKEN + 1)),
        }
        let cp = IntentApiFixture::new(vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster}}),
        )]);
        context.api = cp.origin.clone();
        let store = MemoryStore::default();
        store.put(&key(&context), "prior-authority").unwrap();
        let api = Api::new(context.api.clone(), Some("control-only".into())).unwrap();
        assert!(
            mint(
                &store,
                &context,
                &api,
                requested_grant(Some("knowledge"), Some("read")).unwrap(),
                3600
            )
            .await
            .is_err()
        );
        assert_eq!(
            store.get(&key(&context)).unwrap().as_deref(),
            Some("prior-authority")
        );
        cp.assert_complete();
    }
}

#[test]
fn managed_routing_preserves_selected_managed_authority_without_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let context = context();
    super::super::save_context(dir.path(), &context).unwrap();
    let store = MemoryStore::default();
    for args in [
        vec!["query", "q"],
        vec!["mutate", "m", "--graph", "knowledge", "--as", "fake"],
    ] {
        let cli = Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).unwrap();
        let failure = resolve(&cli, dir.path(), &store, || Ok(false))
            .err()
            .unwrap();
        assert_ne!(failure.body["type"], "data_credential_required");
    }
    let direct = Cli::try_parse_from([
        "omnigraph",
        "query",
        "q",
        "--direct",
        "--server",
        "https://legacy.example",
    ])
    .unwrap();
    assert!(
        resolve(&direct, dir.path(), &store, || panic!(
            "explicit selection read operator defaults"
        ))
        .unwrap()
        .is_none()
    );
    let child = dir.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let query = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
    assert!(
        resolve(&query, &child, &store, || panic!(
            "absent context read operator defaults"
        ))
        .unwrap()
        .is_none()
    );
    assert_eq!(
        resolve(&query, dir.path(), &store, || Ok(false))
            .err()
            .unwrap()
            .body["type"],
        "data_credential_required"
    );
    let cached = credential(&context, "https://data.example");
    save(&store, &context, &cached);
    assert!(
        resolve(&query, dir.path(), &store, || Ok(false))
            .unwrap()
            .is_some()
    );
    let mut narrowed = cached;
    narrowed.grants[0].actions = vec!["read".into()];
    save(&store, &context, &narrowed);
    assert_eq!(
        resolve(&query, dir.path(), &store, || Ok(false))
            .err()
            .unwrap()
            .body["type"],
        "data_scope_missing"
    );
}

struct NoCredentialAccess;

impl Store for NoCredentialAccess {
    fn get(&self, _: &str) -> Result<Option<String>> {
        panic!("routing preflight accessed credentials")
    }
    fn put(&self, _: &str, _: &str) -> Result<()> {
        panic!("routing preflight wrote credentials")
    }
    fn remove(&self, _: &str) -> Result<()> {
        panic!("routing preflight removed credentials")
    }
}

#[test]
fn managed_data_issue_633_explicit_and_unrelated_commands_skip_context() {
    let dir = tempfile::tempdir().unwrap();
    super::super::save_context(dir.path(), &context()).unwrap();
    // An unreadable-as-context object makes an accidental context read fail;
    // the ambient callback and store also fail if either is consulted.
    std::fs::remove_file(dir.path().join(".omnigraph/context")).unwrap();
    std::fs::create_dir(dir.path().join(".omnigraph/context")).unwrap();
    for args in [
        vec!["query", "q", "--server", "legacy"],
        vec!["read", "q", "--profile", "legacy"],
        vec!["mutate", "--store", "file:///scratch", "-e", "source"],
        vec!["change", "m", "--cluster", "local"],
        vec!["query", "q", "--direct"],
        vec!["init", "--schema", "schema.pg", "file:///scratch"],
        vec![
            "load",
            "--data",
            "data.jsonl",
            "--mode",
            "append",
            "--direct",
        ],
        vec![
            "load",
            "--data",
            "data.jsonl",
            "--mode",
            "append",
            "file:///scratch",
        ],
        vec![
            "load",
            "--data",
            "data.jsonl",
            "--mode",
            "append",
            "--store",
            "file:///scratch",
        ],
        vec![
            "load",
            "--data",
            "data.jsonl",
            "--mode",
            "append",
            "--profile",
            "legacy",
        ],
        vec![
            "load",
            "--data",
            "data.jsonl",
            "--mode",
            "append",
            "--server",
            "legacy",
        ],
        vec!["schema", "plan", "--schema", "schema.pg"],
        vec!["commit", "list"],
        vec!["graphs", "list"],
        vec!["alias", "people"],
        vec!["queries", "list"],
        vec!["queries", "validate"],
        vec!["lint", "--schema", "schema.pg", "--query", "q.gq"],
        vec!["snapshot"],
        vec!["branch", "list"],
        vec!["cluster", "status"],
    ] {
        let cli = Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).unwrap();
        assert!(
            resolve(&cli, dir.path(), &NoCredentialAccess, || {
                panic!("bypassed command read operator routing")
            })
            .unwrap()
            .is_none()
        );
    }
}

#[test]
fn managed_load_requires_exact_graph_change_and_explicit_fork_authority() {
    let dir = tempfile::tempdir().unwrap();
    let context = context();
    super::super::save_context(dir.path(), &context).unwrap();
    let store = MemoryStore::default();
    let args = [
        "omnigraph",
        "load",
        "--data",
        "batch.jsonl",
        "--mode",
        "append",
        "--graph",
        "knowledge",
    ];
    let cli = Cli::try_parse_from(args).unwrap();
    assert_eq!(
        resolve(&cli, dir.path(), &store, || Ok(false))
            .err()
            .unwrap()
            .body["type"],
        "data_credential_required"
    );
    assert_eq!(
        resolve(&cli, dir.path(), &NoCredentialAccess, || Ok(true))
            .err()
            .unwrap()
            .body["type"],
        "managed_target_ambiguous"
    );
    for (actions, graph, from, allowed) in [
        (vec!["read"], "knowledge", false, false),
        (vec!["change"], "other", false, false),
        (vec!["change"], "knowledge", false, true),
        (vec!["branch_create"], "knowledge", true, false),
        (vec!["change"], "knowledge", true, false),
        (vec!["change", "branch_create"], "knowledge", true, true),
    ] {
        let mut cached = credential(&context, "https://data.example");
        cached.grants[0].graph_id = graph.into();
        cached.grants[0].actions = actions.into_iter().map(str::to_string).collect();
        save(&store, &context, &cached);
        let extra = if from {
            vec!["--branch", "review", "--from", "main"]
        } else {
            vec![]
        };
        let cli = Cli::try_parse_from(args.into_iter().chain(extra)).unwrap();
        let result = resolve(&cli, dir.path(), &store, || Ok(false));
        if allowed {
            assert!(result.unwrap().is_some());
        } else {
            assert_eq!(result.err().unwrap().body["type"], "data_scope_missing");
        }
    }
    let cli = Cli::try_parse_from(args.into_iter().chain(["--as", "fake"])).unwrap();
    assert_eq!(
        resolve(&cli, dir.path(), &NoCredentialAccess, || Ok(false))
            .err()
            .unwrap()
            .body["type"],
        "managed_scope_conflict"
    );
    let child = dir.path().join("child");
    std::fs::create_dir(&child).unwrap();
    assert!(
        resolve(&cli, &child, &NoCredentialAccess, || panic!(
            "parent context was read"
        ))
        .unwrap()
        .is_none()
    );
    std::fs::write(dir.path().join(".omnigraph/context"), "invalid").unwrap();
    assert_eq!(
        resolve(&cli, dir.path(), &NoCredentialAccess, || panic!(
            "invalid context consulted defaults"
        ))
        .err()
        .unwrap()
        .body["type"],
        "context_invalid"
    );
}

#[test]
fn managed_data_issue_633_ambiguity_and_invalid_context_precede_credentials() {
    let dir = tempfile::tempdir().unwrap();
    super::super::save_context(dir.path(), &context()).unwrap();
    for verb in ["query", "read", "mutate", "change"] {
        let cli = Cli::try_parse_from(["omnigraph", verb, "q", "--graph", "knowledge"]).unwrap();
        assert_eq!(
            resolve(&cli, dir.path(), &NoCredentialAccess, || Ok(true))
                .err()
                .unwrap()
                .body["type"],
            "managed_target_ambiguous"
        );
        assert_eq!(
            resolve(&cli, dir.path(), &NoCredentialAccess, || {
                Err(Failure::refused("operator_config_invalid", "fixture"))
            })
            .err()
            .unwrap()
            .body["type"],
            "operator_config_invalid"
        );
    }
    std::fs::write(dir.path().join(".omnigraph/context"), "malformed").unwrap();
    let cli = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
    assert_eq!(
        resolve(&cli, dir.path(), &NoCredentialAccess, || {
            panic!("invalid context consulted another target")
        })
        .err()
        .unwrap()
        .body["type"],
        "context_invalid"
    );
}

#[tokio::test]
async fn managed_data_transport_refuses_redirect_and_bounds_body() {
    let target = IntentApiFixture::new(vec![]);
    let batch = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(batch.path(), "{}\n").unwrap();
    let mut chunked = format!("{:x}\r\n", 8 * 1024 * 1024 + 1).into_bytes();
    chunked.extend(vec![b' '; 8 * 1024 * 1024 + 1]);
    chunked.extend_from_slice(b"\r\n0\r\n\r\n");
    for (reply, expected) in [
        (
            IntentReply {
                status: 307,
                headers: vec![("Location".into(), target.origin.clone())],
                body: vec![],
            },
            "redirect",
        ),
        (
            IntentReply {
                status: 200,
                headers: vec![("Content-Length".into(), (8 * 1024 * 1024 + 1).to_string())],
                body: vec![],
            },
            "8 MiB",
        ),
        (
            IntentReply {
                status: 200,
                headers: vec![("Transfer-Encoding".into(), "chunked".into())],
                body: chunked,
            },
            "8 MiB",
        ),
    ] {
        for loading in [false, true] {
            let server = IntentApiFixture::new(vec![reply.clone()]);
            let client =
                GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
            let error = if loading {
                client
                    .load(
                        "main",
                        None,
                        batch.path().to_str().unwrap(),
                        crate::cli::CliLoadMode::Append,
                    )
                    .await
                    .unwrap_err()
            } else {
                client
                    .query(
                        ReadTarget::Branch("main".into()),
                        "query q() {}",
                        Some("q"),
                        None,
                    )
                    .await
                    .unwrap_err()
            };
            assert!(error.to_string().contains(expected), "{error}");
            server.assert_complete();
        }
    }
    target.assert_complete();
}

#[tokio::test]
async fn managed_data_errors_redact_reflected_credentials_including_preconditions() {
    let encoded = DATA_TOKEN.replace('h', "\\u0068");
    let batch = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(batch.path(), "{}\n").unwrap();
    for (status, body) in [
        (200, json!(DATA_TOKEN).to_string()),
        (401, format!("{{\"error\":\"rejected {encoded}\"}}")),
        (403, format!("rejected {DATA_TOKEN}")),
        (403, json!({DATA_TOKEN: "rejected"}).to_string()),
        (403, format!("{{\"{encoded}\":\"rejected\"}}")),
        (
            412,
            json!({"error":format!("rejected {DATA_TOKEN}"),"precondition_failure":{"expected":DATA_TOKEN,"actual":null}}).to_string(),
        ),
    ] {
      for loading in [false, true] {
        let server = IntentApiFixture::new(vec![IntentReply { status, headers: vec![], body: body.as_bytes().to_vec() }]);
        let client = GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
        let error = if loading {
            client.load("main", None, batch.path().to_str().unwrap(), crate::cli::CliLoadMode::Append).await.unwrap_err()
        } else { client
            .mutate("main", "mutation m() {}", Some("m"), None, Some("head-a"))
            .await
            .unwrap_err() };
        let rendered = if status == 412 {
            serde_json::to_string(
                &error.downcast_ref::<crate::helpers::PreconditionFailedCli>().unwrap().output,
            )
            .unwrap()
        } else {
            error.to_string()
        };
        assert!(!rendered.contains(DATA_TOKEN), "credential leaked: {status}");
        if status == 200 {
            assert_eq!(rendered, "invalid managed data response");
        } else {
            assert!(rendered.contains("[redacted]"), "{rendered}");
        }
        server.assert_complete();
      }
    }
}

#[tokio::test]
async fn managed_load_sends_exact_ndjson_and_preserves_the_server_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let context = context();
    super::super::save_context(dir.path(), &context).unwrap();
    let batch = dir.path().join("batch.jsonl");
    let ndjson = "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n{\"type\":\"Person\",\"data\":{\"name\":\"Grace\"}}\n";
    std::fs::write(&batch, ndjson).unwrap();
    let commit = json!({"graph_commit_id":"head-load","graph_branch":"review","graph_manifest_version":7,"parent_commit_id":"before","merged_parent_commit_id":null,"actor_id":"principal:alice","created_at":12345});
    let reply = json!({"branch":"review","base_branch":"main","branch_created":true,"mode":"append","nodes":[{"name":"Person","entities_loaded":2}],"edges":[],"total_entities":2,"actor_id":"principal:alice","commit":commit});
    // This is a real response past the ordinary managed 10-second deadline.
    // The request-construction owner separately pins load's 300-second ceiling.
    let server = IntentApiFixture::with_response_delay(
        vec![IntentReply::json(200, reply)],
        std::time::Duration::from_millis(10_250),
    );
    let store = MemoryStore::default();
    let mut cached = credential(&context, &server.origin);
    cached.grants[0].actions = vec!["change".into(), "branch_create".into()];
    save(&store, &context, &cached);
    let cli = Cli::try_parse_from([
        "omnigraph",
        "load",
        "--data",
        batch.to_str().unwrap(),
        "--graph",
        "knowledge",
        "--mode",
        "append",
        "--branch",
        "review",
        "--from",
        "main",
    ])
    .unwrap();
    let client = resolve(&cli, dir.path(), &store, || Ok(false))
        .unwrap()
        .unwrap();
    let result = client
        .load(
            "review",
            Some("main"),
            batch.to_str().unwrap(),
            crate::cli::CliLoadMode::Append,
        )
        .await
        .unwrap();
    let result = serde_json::to_value(result).unwrap();
    assert_eq!(result["commit"], commit);
    assert_eq!(result["commit"]["actor_id"], "principal:alice");
    assert_eq!(result["base_branch"], "main");
    assert_eq!(result["branch_created"], true);
    assert_eq!(result["total_entities"], 2);
    assert_eq!(result["branch"], "review");
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(
        requests[0].path,
        "/graphs/knowledge/load/ndjson?branch=review&mode=append&from=main"
    );
    assert_eq!(requests[0].headers["content-type"], "application/x-ndjson");
    assert_eq!(
        requests[0].headers["authorization"],
        format!("Bearer {DATA_TOKEN}")
    );
    assert_eq!(requests[0].raw_body, ndjson.as_bytes());
    assert!(!requests[0].headers.contains_key("x-actor-id"));
    server.assert_complete();
}

#[tokio::test]
async fn managed_load_refuses_local_overflow_before_io_and_never_replays_failed_receipts() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let empty_server = IntentApiFixture::new(vec![]);
    let client =
        GraphClient::managed(&empty_server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
    file.as_file().set_len(32 * 1024 * 1024 + 1).unwrap();
    assert!(
        client
            .load(
                "review",
                Some("main"),
                file.path().to_str().unwrap(),
                crate::cli::CliLoadMode::Append
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("32 MiB")
    );
    std::fs::write(file.path(), [255]).unwrap();
    assert!(
        client
            .load(
                "review",
                Some("main"),
                file.path().to_str().unwrap(),
                crate::cli::CliLoadMode::Append
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("UTF-8")
    );
    empty_server.assert_complete();
    std::fs::write(file.path(), "{}\n").unwrap();
    for reply in [
        IntentReply::json(500, json!({"error":"load failed"})),
        IntentReply::json(429, json!({"error":"busy"})),
        IntentReply::json(503, json!({"error":"recover first"})),
        IntentReply {
            status: 200,
            headers: vec![("Content-Length".into(), "1000".into())],
            body: b"{}".to_vec(),
        },
        IntentReply::json(200, json!({"not":"a receipt"})),
    ] {
        let server = IntentApiFixture::new(vec![reply]);
        let client = GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
        assert!(
            client
                .load(
                    "review",
                    Some("main"),
                    file.path().to_str().unwrap(),
                    crate::cli::CliLoadMode::Append
                )
                .await
                .is_err()
        );
        server.assert_complete();
    }
}
