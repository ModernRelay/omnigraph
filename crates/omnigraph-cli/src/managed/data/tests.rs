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
fn managed_routing_refuses_scope_overrides_and_other_data_verbs_before_keychain() {
    let dir = tempfile::tempdir().unwrap();
    let context = context();
    super::super::save_context(dir.path(), &context).unwrap();
    let store = MemoryStore::default();
    for args in [
        vec!["query", "q"],
        vec![
            "query",
            "q",
            "--graph",
            "knowledge",
            "--server",
            "https://foreign.example",
        ],
        vec!["mutate", "m", "--graph", "knowledge", "--as", "fake"],
        vec!["snapshot", "--store", "/tmp/never-open"],
        vec!["alias", "dangerous"],
        vec!["graphs", "list"],
    ] {
        let cli = Cli::try_parse_from(std::iter::once("omnigraph").chain(args)).unwrap();
        let failure = resolve(&cli, dir.path(), &store).err().unwrap();
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
    assert!(resolve(&direct, dir.path(), &store).unwrap().is_none());
    let child = dir.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let query = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
    assert!(resolve(&query, &child, &store).unwrap().is_none());
    assert_eq!(
        resolve(&query, dir.path(), &store).err().unwrap().body["type"],
        "data_credential_required"
    );
    let cached = credential(&context, "https://data.example");
    save(&store, &context, &cached);
    assert!(resolve(&query, dir.path(), &store).unwrap().is_some());
    let mut narrowed = cached;
    narrowed.grants[0].actions = vec!["read".into()];
    save(&store, &context, &narrowed);
    assert_eq!(
        resolve(&query, dir.path(), &store).err().unwrap().body["type"],
        "data_scope_missing"
    );
}

#[tokio::test]
async fn managed_data_transport_refuses_redirect_and_bounds_body() {
    let target = IntentApiFixture::new(vec![]);
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
        let server = IntentApiFixture::new(vec![reply]);
        let client = GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
        let error = client
            .query(
                ReadTarget::Branch("main".into()),
                "query q() {}",
                Some("q"),
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        server.assert_complete();
    }
    target.assert_complete();
}

#[tokio::test]
async fn managed_data_errors_redact_reflected_credentials_including_preconditions() {
    let encoded = DATA_TOKEN.replace('h', "\\u0068");
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
        let server = IntentApiFixture::new(vec![IntentReply {
            status,
            headers: vec![],
            body: body.into_bytes(),
        }]);
        let client = GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
        let error = client
            .mutate("main", "mutation m() {}", Some("m"), None, Some("head-a"))
            .await
            .unwrap_err();
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
