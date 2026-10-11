use super::*;
use crate::managed::auth::tests::MemoryStore;
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
use clap::Parser;
use omnigraph::db::ReadTarget;

const DATA_TOKEN: &str = "header.payload.signature";

#[tokio::test]
async fn ordinary_graph_command_acquires_missing_or_expired_identity_once_before_submission() {
    for expired in [false, true] {
        let mut context = context();
        let cp = IntentApiFixture::with_origin(|origin| {
            context.api = origin.to_owned();
            let credential = identity_credential(&context, "https://data.example");
            let mut response = credential.metadata();
            response["token"] = json!(credential.token);
            vec![IntentReply::json(
                200,
                json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}),
            )]
        });
        let store = MemoryStore::default();
        if expired {
            let mut saved = identity_credential(&context, "https://data.example");
            expire_identity(&mut saved);
            save(&store, &context, &saved);
        }
        let dir = tempfile::tempdir().unwrap();
        super::super::save_context(dir.path(), &context).unwrap();
        let cli =
            Cli::try_parse_from(["omnigraph", "mutate", "m", "--graph", "knowledge"]).unwrap();
        let client = resolve_with_acquisition(
            &cli,
            dir.path(),
            &store,
            || Ok(false),
            async |_| Ok(Some("alice".into())),
            async |context| Api::new(context.api.clone(), Some("provider-access".into())),
        )
        .await
        .unwrap();
        assert!(client.is_some());
        assert_eq!(cp.requests().len(), 1);
        assert_eq!(cp.requests()[0].path, "/v1/clusters/cluster-a/tokens");
        assert_eq!(
            cp.requests()[0].body,
            json!({"version":2,"ttl_seconds":3600})
        );
        assert!(
            resolve_with_acquisition(
                &cli,
                dir.path(),
                &store,
                || Ok(false),
                async |_| Ok(Some("alice".into())),
                async |_| { panic!("valid graph credential must not call the API") }
            )
            .await
            .unwrap()
            .is_some()
        );
        cp.assert_complete();
    }
}

#[tokio::test]
async fn acquisition_preserves_explicit_target_priority_and_refuses_retired_credentials() {
    let context = context();
    let dir = tempfile::tempdir().unwrap();
    super::super::save_context(dir.path(), &context).unwrap();
    let store = MemoryStore::default();
    let explicit = Cli::try_parse_from([
        "omnigraph",
        "query",
        "q",
        "--server",
        "https://explicit.example",
        "--graph",
        "knowledge",
    ])
    .unwrap();
    assert!(
        resolve_with_acquisition(
            &explicit,
            dir.path(),
            &store,
            || panic!("explicit target reads ambient state"),
            async |_| panic!("explicit target reads managed identity"),
            async |_| panic!("explicit target calls issuer")
        )
        .await
        .unwrap()
        .is_none()
    );
    let cli = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
    for corruption in [
        "retired-cache",
        "retired-token",
        "permissions",
        "malformed-token",
        "wrong-actor",
        "wrong-expiry",
    ] {
        let mut cached = credential(&context, "https://data.example");
        expire_identity(&mut cached);
        match corruption {
            "retired-cache" => cached.version = 1,
            "malformed-token" => cached.token = DATA_TOKEN.into(),
            "wrong-actor" => cached.actor = "principal:other".into(),
            "wrong-expiry" => {
                cached.expires_at = (OffsetDateTime::now_utc() - time::Duration::seconds(60))
                    .format(&Rfc3339)
                    .unwrap()
            }
            _ => {
                let parts: Vec<_> = cached.token.split('.').collect();
                let mut claims: Value =
                    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
                if corruption == "retired-token" {
                    claims["version"] = json!(1);
                }
                claims["grants"] = json!([]);
                cached.token = format!(
                    "{}.{}.{}",
                    parts[0],
                    URL_SAFE_NO_PAD.encode(claims.to_string()),
                    parts[2]
                );
            }
        }
        save(&store, &context, &cached);
        let before = store.get(&key(&context)).unwrap();
        let failure = resolve_with_acquisition(
            &cli,
            dir.path(),
            &store,
            || Ok(false),
            async |_| panic!("invalid cache consulted identity"),
            async |_| panic!("invalid cache called issuer"),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            failure.body["type"], "data_credential_invalid",
            "{corruption}"
        );
        assert_eq!(store.get(&key(&context)).unwrap(), before, "{corruption}");

        // A second process can replace the cache before this caller gets its
        // cache lock. Revalidate those bytes rather than treating invalidity
        // as permission to mint over them.
        let mut initially_expired = credential(&context, "https://data.example");
        expire_identity(&mut initially_expired);
        save(&store, &context, &initially_expired);
        let failure = resolve_with_acquisition(
            &cli,
            dir.path(),
            &store,
            || Ok(false),
            async |_| {
                save(&store, &context, &cached);
                Ok(Some("alice".into()))
            },
            async |_| panic!("invalid replacement cache called issuer"),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            failure.body["type"], "data_credential_invalid",
            "replacement: {corruption}"
        );
        assert_eq!(
            store.get(&key(&context)).unwrap(),
            before,
            "replacement: {corruption}"
        );
    }
}

#[tokio::test]
async fn acquisition_never_reuses_another_principal_or_accepts_a_wrong_issued_identity() {
    let mut context = context();
    let cp = IntentApiFixture::with_origin(|origin| {
        context.api = origin.into();
        let credential = identity_credential(&context, "https://data.example");
        let mut response = credential.metadata();
        response["token"] = json!(credential.token);
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}),
        )]
    });
    let store = MemoryStore::default();
    let old = identity_credential(&context, "https://data.example");
    save(&store, &context, &old);
    let before = store.get(&key(&context)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    super::super::save_context(dir.path(), &context).unwrap();
    let cli = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
    assert!(
        resolve_with_acquisition(
            &cli,
            dir.path(),
            &store,
            || Ok(false),
            async |_| Ok(Some("bob".into())),
            async |context| Api::new(context.api.clone(), Some("bob-provider-token".into()))
        )
        .await
        .is_err()
    );
    assert_eq!(store.get(&key(&context)).unwrap(), before);
    cp.assert_complete();
}

fn context() -> Context {
    Context {
        version: 1,
        cluster: "cluster-a".into(),
        api: "https://control.example".into(),
    }
}

fn credential(context: &Context, endpoint: &str) -> Credential {
    identity_credential(context, endpoint)
}

fn identity_credential(context: &Context, endpoint: &str) -> Credential {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let kid = "a".repeat(64);
    let header = json!({"typ":"JWT","alg":"ES256","kid":kid});
    let claims = json!({"version":2,"iss":context.api,"aud":format!("urn:omnigraph:data:{}", context.cluster),
        "sub":"alice","account_id":"account-a","cluster_id":context.cluster,"cluster_incarnation":"incarnation-a",
        "principal_kind":"human","assurance":"verified_human","iat":now,"exp":now+3600,"jti":"test-credential"});
    Credential {
        version: 2,
        api: context.api.clone(),
        cluster_id: context.cluster.clone(),
        endpoint: endpoint.into(),
        token: format!(
            "{}.{}.signature",
            URL_SAFE_NO_PAD.encode(header.to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        ),
        expires_at: OffsetDateTime::from_unix_timestamp(now + 3600)
            .unwrap()
            .format(&Rfc3339)
            .unwrap(),
        kid,
        actor: "principal:alice".into(),
        cluster_incarnation: Some("incarnation-a".into()),
    }
}

fn expire_identity(credential: &mut Credential) {
    let expiry = OffsetDateTime::now_utc().unix_timestamp() - 1;
    credential.expires_at = OffsetDateTime::from_unix_timestamp(expiry)
        .unwrap()
        .format(&Rfc3339)
        .unwrap();
    let parts: Vec<_> = credential.token.split('.').collect();
    let mut claims: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    claims["iat"] = json!(expiry - 3600);
    claims["exp"] = json!(expiry);
    credential.token = format!(
        "{}.{}.{}",
        parts[0],
        URL_SAFE_NO_PAD.encode(claims.to_string()),
        parts[2]
    );
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
fn token_arguments_bound_lifetime_and_refuse_removed_actions() {
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

    for bad in [
        "../graph",
        "graph_bad",
        "policies",
        "graphs",
        "κnowledge",
        "1graph",
        "-graph",
    ] {
        assert!(graph_id(bad).is_err());
    }
    assert!(Cli::try_parse_from(["omnigraph", "cluster", "token", "--managed", "--clear"]).is_ok());
    assert!(Cli::try_parse_from(["omnigraph", "cluster", "token", "--managed"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "omnigraph",
            "cluster",
            "token",
            "--managed",
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
    let data = IntentApiFixture::graph(vec![
        IntentReply::json(200, read_reply()),
        IntentReply::json(200, change_reply()),
        IntentReply::json(200, read_reply()),
    ]);
    let mut context = context();
    let cp = IntentApiFixture::with_origin(|origin| {
        context.api = origin.to_owned();
        let response_credential = credential(&context, &data.origin);
        let mut response = response_credential.metadata();
        response["token"] = json!(response_credential.token);
        response["access_token"] = json!("must-not-be-output");
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}),
        )]
    });
    let cp_store = MemoryStore::default();
    cp_store
        .put(&context.api, "unrelated-control-session")
        .unwrap();
    let data_store = MemoryStore::default();
    let api = Api::new(cp.origin.clone(), Some("control-session-secret".into())).unwrap();
    let output = mint(&data_store, &context, &api, 3600).await.unwrap();
    let token = load_credential(&data_store, &context).unwrap().token;
    let rendered = output.to_string();
    assert!(!rendered.contains(&token));
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
    assert_eq!(requests[0].body, json!({"version":2,"ttl_seconds":3600}));
    cp.assert_complete();
    drop(cp);
    let client = load(&data_store, &context, "knowledge").unwrap();
    let result = client
        .query(
            ReadTarget::Branch("main".into()),
            "query q() { return { 42 as value } }",
            Some("q"),
            None,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows.get(), "[{\"value\":42}]");
    let changed = client
        .mutate(
            "main",
            "mutation m() {}",
            Some("m"),
            None,
            Some("head-a"),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(changed.actor_id.as_deref(), Some("principal:alice"));
    let _: omnigraph_api_types::ReadOutput = client
        .invoke_named("q", false, None, Some("main".into()), None, None)
        .await
        .unwrap();
    let requests = data.workflow_requests();
    assert_eq!(
        requests.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        [
            "/graphs/knowledge/query",
            "/graphs/knowledge/mutate/if-graph-commit",
            "/graphs/knowledge/queries/q"
        ]
    );
    for request in &requests {
        assert_eq!(request.headers["authorization"], format!("Bearer {token}"));
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
        load(&data_store, &context, "knowledge").err().unwrap().body["type"],
        "data_credential_required"
    );
    assert!(cp_store.get(&context.api).unwrap().is_some());
    assert_eq!(
        data_store.get(&key(&other)).unwrap().as_deref(),
        Some("other-data-entry")
    );
}

#[tokio::test]
async fn identity_issuance_caches_no_permissions_and_discovers_without_control_calls() {
    let discovery = json!({"graphs":[{"graph_id":"hidden","display_name":"hidden"}]});
    let commit = json!({"graph_commit_id":"head-a","graph_branch":"main","graph_manifest_version":7,
        "parent_commit_id":null,"merged_parent_commit_id":null,"actor_id":"principal:alice","created_at":12345});
    let data = IntentApiFixture::graph(vec![
        IntentReply::json(200, discovery.clone()),
        IntentReply::json(
            403,
            json!({"error":"current policy denies change","code":"forbidden"}),
        ),
        IntentReply::json(200, json!({"commits":[commit.clone()]})),
        IntentReply::json(200, commit.clone()),
    ]);
    let mut context = context();
    let cp = IntentApiFixture::with_origin(|origin| {
        context.api = origin.to_owned();
        let credential = identity_credential(&context, &data.origin);
        let mut response = credential.metadata();
        response["token"] = json!(credential.token);
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}),
        )]
    });
    let store = MemoryStore::default();
    let api = Api::new(cp.origin.clone(), Some("control-session".into())).unwrap();
    let output = mint(&store, &context, &api, 3600).await.unwrap();
    assert_eq!(output["data"]["version"], 2);
    assert!(output["data"].get("grants").is_none());
    assert!(output["data"].get("token").is_none());
    assert_eq!(
        cp.requests()[0].body,
        json!({"version":2,"ttl_seconds":3600})
    );
    cp.assert_complete();
    drop(cp);
    let saved: Value = serde_json::from_str(&store.get(&key(&context)).unwrap().unwrap()).unwrap();
    assert!(saved.get("grants").is_none());
    assert!(
        load(&store, &context, "any-graph").is_ok(),
        "Cedar, not the local cache, decides permission"
    );
    let dir = tempfile::tempdir().unwrap();
    super::super::save_context(dir.path(), &context).unwrap();
    let cli = Cli::try_parse_from(["omnigraph", "graphs", "list", "--json"]).unwrap();
    let client = resolve(&cli, dir.path(), &store, || Ok(false))
        .unwrap()
        .unwrap();
    let result = client.discover_graphs().await.unwrap();
    assert_eq!(serde_json::to_value(result).unwrap(), discovery);
    assert_eq!(data.workflow_requests()[0].path, "/graphs/discovery");
    // Identity credentials deliberately have no local action ceiling. Load
    // and lineage commands reach the exact cached endpoint, where current
    // policy decides permission, even after the control API is gone.
    let batch = dir.path().join("batch.jsonl");
    std::fs::write(&batch, "{}\n").unwrap();
    for (command, path) in [
        (
            vec![
                "load",
                "--data",
                batch.to_str().unwrap(),
                "--mode",
                "append",
                "--from",
                "main",
                "--branch",
                "review",
            ],
            "/graphs/knowledge/load/ndjson?branch=review&mode=append&from=main",
        ),
        (
            vec!["commit", "list", "--branch", "main"],
            "/graphs/knowledge/commits?branch=main",
        ),
        (
            vec!["commit", "show", "head-a"],
            "/graphs/knowledge/commits/head-a",
        ),
    ] {
        let selected = Cli::try_parse_from(
            ["omnigraph", "--graph", "knowledge"]
                .into_iter()
                .chain(command.clone()),
        )
        .unwrap();
        let client = resolve(&selected, dir.path(), &store, || Ok(false))
            .unwrap()
            .unwrap();
        match command.as_slice() {
            ["load", ..] => {
                let error = client
                    .load(
                        "review",
                        Some("main"),
                        batch.to_str().unwrap(),
                        crate::cli::CliLoadMode::Append,
                        &[],
                    )
                    .await
                    .unwrap_err();
                let refusal = error
                    .downcast_ref::<crate::helpers::RemoteErrorCli>()
                    .expect("preserve the server policy refusal");
                assert_eq!(
                    serde_json::to_value(&refusal.output).unwrap()["code"],
                    "forbidden"
                );
                assert_eq!(refusal.output.error, "current policy denies change");
            }
            ["commit", "list", ..] => {
                let output = client.list_commits(Some("main")).await.unwrap();
                assert_eq!(serde_json::to_value(&output.commits[0]).unwrap(), commit);
            }
            _ => assert_eq!(
                serde_json::to_value(client.get_commit("head-a").await.unwrap()).unwrap(),
                commit
            ),
        }
        let requests = data.workflow_requests();
        let request = requests.last().unwrap();
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers["authorization"],
            format!("Bearer {}", saved["token"].as_str().unwrap())
        );
        assert!(!request.headers.contains_key("x-actor-id"));
    }
    assert_eq!(
        data.workflow_requests().len(),
        4,
        "one attempt per operation"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&store.get(&key(&context)).unwrap().unwrap()).unwrap(),
        saved
    );
    data.assert_complete();
    let mut retired = credential(&context, &data.origin);
    retired.version = 1;
    save(&store, &context, &retired);
    let failure = resolve(&cli, dir.path(), &store, || Ok(false))
        .err()
        .unwrap();
    assert_eq!(failure.body["type"], "data_credential_invalid");
}

#[tokio::test]
async fn identity_issuance_rejects_wrong_profile_and_authority_without_cache_replacement() {
    for field in [
        "version",
        "grants",
        "roles",
        "actor",
        "incarnation",
        "endpoint",
        "token",
    ] {
        let mut context = context();
        let cp = IntentApiFixture::with_origin(|origin| {
            context.api = origin.to_owned();
            let credential = identity_credential(&context, "https://data.example");
            let mut response = credential.metadata();
            response["token"] = json!(credential.token);
            let mut envelope = json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}});
            match field {
                "version" => envelope["data"]["version"] = json!(1),
                "grants" => envelope["data"]["grants"] = json!([]),
                "roles" => envelope["data"]["roles"] = json!(["admin"]),
                "actor" => envelope["data"]["actor"] = json!("principal:other"),
                "endpoint" => {
                    envelope["data"]["endpoint"] = json!("https://user:password@data.example/path")
                }
                "token" => envelope["data"]["token"] = json!("x".repeat(MAX_TOKEN + 1)),
                _ => envelope["meta"]["incarnation"] = json!("other"),
            }
            vec![IntentReply::json(200, envelope)]
        });
        let store = MemoryStore::default();
        store.put(&key(&context), "existing-credential").unwrap();
        let api = Api::new(cp.origin.clone(), Some("control-session".into())).unwrap();
        assert!(
            mint(&store, &context, &api, 3600).await.is_err(),
            "accepted {field}"
        );
        assert_eq!(
            store.get(&key(&context)).unwrap().as_deref(),
            Some("existing-credential")
        );
        cp.assert_complete();
    }
}

#[test]
fn cached_authority_refuses_wrong_bindings_expiry_and_extra_fields() {
    let context = context();
    let store = MemoryStore::default();
    let original = credential(&context, "https://data.example");
    save(&store, &context, &original);
    let base = serde_json::to_value(&original).unwrap();
    for (field, value) in [
        ("version", json!(1)),
        ("api", json!("https://foreign.example")),
        ("cluster_id", json!("foreign")),
        ("endpoint", json!("https://data.example/path")),
        ("endpoint", json!("http://data.example")),
        ("endpoint", json!("https://user:secret@data.example")),
        ("token", json!("a.b")),
        ("token", json!(DATA_TOKEN)),
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
            load(&store, &context, "knowledge").is_err(),
            "accepted {field}"
        );
    }
    let duplicate = serde_json::to_string(&original)
        .unwrap()
        .replacen("{", "{\"version\":1,", 1);
    store.put(&key(&context), &duplicate).unwrap();
    assert!(load(&store, &context, "knowledge").is_err());
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
    for malformed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        super::super::save_context(dir.path(), &context()).unwrap();
        // An unreadable-as-context object makes an accidental context read fail;
        // the ambient callback and store also fail if either is consulted.
        if malformed {
            std::fs::remove_file(dir.path().join(".omnigraph/context")).unwrap();
            std::fs::create_dir(dir.path().join(".omnigraph/context")).unwrap();
        }
        for args in [
            vec!["query", "q", "--server", "legacy"],
            vec!["query", "q", "--profile", "legacy"],
            vec!["mutate", "--store", "file:///scratch", "-e", "source"],
            vec!["mutate", "m", "--cluster", "local"],
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
            vec!["commit", "list", "file:///scratch"],
            vec!["commit", "list", "--direct"],
            vec!["commit", "list", "--server", "legacy"],
            vec!["commit", "list", "--profile", "legacy"],
            vec!["commit", "show", "commit-a", "--uri", "file:///scratch"],
            vec!["commit", "show", "commit-a", "--store", "file:///scratch"],
            vec!["commit", "changes", "commit-a"],
            vec!["graphs", "list", "--server", "legacy"],
            vec!["graphs", "list", "--server", "legacy", "--discovery"],
            vec!["graphs", "list", "--profile", "legacy"],
            vec!["graphs", "list", "--direct"],
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
}

#[tokio::test]
async fn managed_commit_reads_use_exact_cached_read_authority_without_api_or_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let context = context();
    super::super::save_context(dir.path(), &context).unwrap();
    let store = MemoryStore::default();
    let commit = json!({
        "graph_commit_id":"commit-a", "graph_branch":null,
        "graph_manifest_version":3, "parent_commit_id":"prior",
        "merged_parent_commit_id":"imported-head", "actor_id":"principal:alice",
        "created_at":123456,
    });
    let server = IntentApiFixture::graph(vec![
        IntentReply::json(200, json!({"commits":[commit.clone()]})),
        IntentReply::json(200, commit.clone()),
    ]);
    let cached = credential(&context, &server.origin);
    for command in [vec!["commit", "list"], vec!["commit", "show", "commit-a"]] {
        let cli = Cli::try_parse_from(
            ["omnigraph", "--graph", "knowledge"]
                .into_iter()
                .chain(command.clone()),
        )
        .unwrap();
        assert_eq!(
            resolve(&cli, dir.path(), &NoCredentialAccess, || Ok(true))
                .err()
                .unwrap()
                .body["type"],
            "managed_target_ambiguous"
        );
        assert_eq!(
            resolve(&cli, dir.path(), &store, || Ok(false))
                .err()
                .unwrap()
                .body["type"],
            "data_credential_required"
        );
        save(&store, &context, &cached);
        let client = resolve(&cli, dir.path(), &store, || Ok(false))
            .unwrap()
            .unwrap();
        if command[1] == "list" {
            let output = client.list_commits(Some("main")).await.unwrap();
            assert_eq!(serde_json::to_value(&output.commits[0]).unwrap(), commit);
        } else {
            let output = client.get_commit("commit-a").await.unwrap();
            assert_eq!(serde_json::to_value(output).unwrap(), commit);
        }
        clear(&store, &context).unwrap();
        let no_graph = Cli::try_parse_from(std::iter::once("omnigraph").chain(command)).unwrap();
        assert_eq!(
            resolve(&no_graph, dir.path(), &NoCredentialAccess, || Ok(false))
                .err()
                .unwrap()
                .body["type"],
            "graph_required"
        );
    }
    let requests = server.workflow_requests();
    assert_eq!(requests.len(), 2);
    for (request, path) in requests.iter().zip([
        "/graphs/knowledge/commits?branch=main",
        "/graphs/knowledge/commits/commit-a",
    ]) {
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, path);
        assert_eq!(
            request.headers["authorization"],
            format!("Bearer {}", cached.token)
        );
    }
    server.assert_complete();
}

#[test]
fn managed_load_uses_identity_and_keeps_target_preflight() {
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
    save(
        &store,
        &context,
        &credential(&context, "https://data.example"),
    );
    for extra in [vec![], vec!["--branch", "review", "--from", "main"]] {
        let cli = Cli::try_parse_from(args.into_iter().chain(extra)).unwrap();
        assert!(
            resolve(&cli, dir.path(), &store, || Ok(false))
                .unwrap()
                .is_some()
        );
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
    for verb in ["query", "mutate"] {
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
    let operations = ["query", "load", "commit-list", "commit-show"];
    let cases = [
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
    ];
    let server = IntentApiFixture::graph(
        cases
            .iter()
            .flat_map(|(reply, _)| std::iter::repeat_n(reply.clone(), operations.len()))
            .collect(),
    );
    let client = GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
    let mut completed = 0;
    for (_, expected) in &cases {
        for operation in operations {
            let error = match operation {
                "load" => client
                    .load(
                        "main",
                        None,
                        batch.path().to_str().unwrap(),
                        crate::cli::CliLoadMode::Append,
                        &[],
                    )
                    .await
                    .unwrap_err(),
                "commit-list" => client.list_commits(Some("main")).await.unwrap_err(),
                "commit-show" => client.get_commit("commit-a").await.unwrap_err(),
                _ => client
                    .query(
                        ReadTarget::Branch("main".into()),
                        "query q() {}",
                        Some("q"),
                        None,
                        &[],
                    )
                    .await
                    .unwrap_err(),
            };
            assert!(error.to_string().contains(expected), "{error}");
            completed += 1;
            assert_eq!(server.workflow_requests().len(), completed, "{operation}");
        }
    }
    server.assert_complete();
    target.assert_complete();
}

#[tokio::test]
async fn managed_data_errors_redact_reflected_credentials_including_preconditions() {
    let encoded = DATA_TOKEN.replace('h', "\\u0068");
    let batch = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(batch.path(), "{}\n").unwrap();
    let operations = ["mutate", "load", "commit-list", "commit-show"];
    let cases = [
        (200, json!(DATA_TOKEN).to_string()),
        (401, format!("{{\"error\":\"rejected {encoded}\"}}")),
        (403, format!("rejected {DATA_TOKEN}")),
        (403, json!({DATA_TOKEN: "rejected"}).to_string()),
        (403, format!("{{\"{encoded}\":\"rejected\"}}")),
        (
            412,
            json!({"error":format!("rejected {DATA_TOKEN}"),"precondition_failure":{"expected":DATA_TOKEN,"actual":null}}).to_string(),
        ),
    ];
    let server = IntentApiFixture::graph(
        cases
            .iter()
            .flat_map(|(status, body)| {
                std::iter::repeat_n(
                    IntentReply {
                        status: *status,
                        headers: vec![("Retry-After".into(), format!("retry-{DATA_TOKEN}"))],
                        body: body.as_bytes().to_vec(),
                    },
                    operations.len(),
                )
            })
            .collect(),
    );
    let client = GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
    let mut completed = 0;
    for (status, _) in cases {
        for operation in operations {
            let (error, evidence) = crate::command_outcome::observe(async {
                match operation {
                    "load" => client
                        .load(
                            "main",
                            None,
                            batch.path().to_str().unwrap(),
                            crate::cli::CliLoadMode::Append,
                            &[],
                        )
                        .await
                        .unwrap_err(),
                    "commit-list" => client.list_commits(Some("main")).await.unwrap_err(),
                    "commit-show" => client.get_commit("commit-a").await.unwrap_err(),
                    _ => client
                        .mutate(
                            "main",
                            "mutation m() {}",
                            Some("m"),
                            None,
                            Some("head-a"),
                            &[],
                        )
                        .await
                        .unwrap_err(),
                }
            })
            .await;
            assert!(
                error
                    .downcast_ref::<crate::helpers::PreconditionFailedCli>()
                    .is_none(),
                "{status} {operation}: a reflected or mismatched expected token never proves our conditional request was refused, even at HTTP 412"
            );
            let rendered =
                if let Some(remote) = error.downcast_ref::<crate::helpers::RemoteErrorCli>() {
                    serde_json::to_string(&remote.output).unwrap()
                } else {
                    error.to_string()
                };
            assert!(
                !rendered.contains(DATA_TOKEN),
                "credential leaked: {status}"
            );
            if status == 200 {
                assert_eq!(rendered, "invalid managed data response");
            } else {
                assert!(rendered.contains("[redacted]"), "{rendered}");
            }
            let failure =
                serde_json::to_string(&crate::command_outcome::Failure::classify(error, evidence))
                    .unwrap();
            assert!(
                !failure.contains(DATA_TOKEN),
                "outcome leaked credential: {failure}"
            );
            assert!(failure.contains("retry-[redacted]"), "{failure}");
            completed += 1;
            assert_eq!(
                server.workflow_requests().len(),
                completed,
                "{status} {operation}"
            );
        }
    }
    server.assert_complete();
}

#[tokio::test]
#[ignore = "nightly: a real 30.75 s reply proves the 300 s load timeout outlives the 30 s managed deadline"]
async fn managed_load_sends_exact_ndjson_and_preserves_the_server_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let context = context();
    super::super::save_context(dir.path(), &context).unwrap();
    let batch = dir.path().join("batch.jsonl");
    let ndjson = "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n{\"type\":\"Person\",\"data\":{\"name\":\"Grace\"}}\n";
    std::fs::write(&batch, ndjson).unwrap();
    let commit = json!({"graph_commit_id":"head-load","graph_branch":"review","graph_manifest_version":7,"parent_commit_id":"before","merged_parent_commit_id":null,"actor_id":"principal:alice","created_at":12345});
    let reply = json!({"branch":"review","base_branch":"main","branch_created":true,"mode":"append","nodes":[{"name":"Person","entities_loaded":2}],"edges":[],"total_entities":2,"embedding_generation":null,"actor_id":"principal:alice","commit":commit});
    // This is a real response past the ordinary managed 30-second deadline.
    // The request-construction owner separately pins load's 300-second ceiling.
    let server = IntentApiFixture::graph_with_response_delay(
        vec![IntentReply::json(200, reply)],
        std::time::Duration::from_millis(30_750),
    );
    let store = MemoryStore::default();
    let cached = credential(&context, &server.origin);
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
            &[],
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
    let requests = server.workflow_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(
        requests[0].path,
        "/graphs/knowledge/load/ndjson?branch=review&mode=append&from=main"
    );
    assert_eq!(requests[0].headers["content-type"], "application/x-ndjson");
    assert_eq!(
        requests[0].headers["authorization"],
        format!("Bearer {}", cached.token)
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
                crate::cli::CliLoadMode::Append,
                &[],
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
                crate::cli::CliLoadMode::Append,
                &[],
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
        let server = IntentApiFixture::graph(vec![reply]);
        let client = GraphClient::managed(&server.origin, "knowledge", DATA_TOKEN.into()).unwrap();
        assert!(
            client
                .load(
                    "review",
                    Some("main"),
                    file.path().to_str().unwrap(),
                    crate::cli::CliLoadMode::Append,
                    &[],
                )
                .await
                .is_err()
        );
        server.assert_complete();
    }
}
