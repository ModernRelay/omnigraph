use super::*;
use crate::managed::auth::tests::MemoryStore;
use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
use clap::Parser;
use omnigraph::db::ReadTarget;

const DATA_TOKEN: &str = "header.payload.signature";

struct ReadBudgetStore {
    raw: MemoryStore,
    reads: std::cell::Cell<usize>,
    budget: usize,
}

impl Store for ReadBudgetStore {
    fn persistent(&self) -> bool {
        false
    }
    fn get(&self, key: &str) -> Result<Option<String>> {
        let reads = self.reads.get() + 1;
        self.reads.set(reads);
        if reads > self.budget {
            return Err(Failure::refused(
                "keychain_unavailable",
                "test read failure",
            ));
        }
        self.raw.get(key)
    }
    fn put(&self, key: &str, value: &str) -> Result<()> {
        self.raw.put(key, value)
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.raw.remove(key)
    }
}

#[tokio::test]
async fn cached_resource_uses_one_credential_read_and_propagates_initial_store_failure() {
    let context = context();
    let raw = MemoryStore::default();
    save(
        &raw,
        &context,
        &identity_credential(&context, "https://data.example"),
    );
    let store = ReadBudgetStore {
        raw,
        reads: Default::default(),
        budget: 1,
    };
    acquire_resource(
        Some("knowledge"),
        &context,
        &store,
        async |_| Ok(Some("alice".into())),
        async |_| panic!("valid cache must not issue a credential"),
    )
    .await
    .unwrap();
    assert_eq!(store.reads.get(), 1);
    let failure = acquire_resource(
        Some("knowledge"),
        &context,
        &store,
        async |_| panic!("store failure must not acquire control authority"),
        async |_| panic!("store failure must not issue a credential"),
    )
    .await
    .err()
    .expect("store read must fail");
    assert_eq!(failure.body["type"], "keychain_unavailable");
    assert_eq!(store.reads.get(), 2);
}

#[test]
fn loaded_credential_is_revalidated_before_constructing_graph_or_registry_client() {
    let context = context();
    for graph in [Some("knowledge"), None] {
        let mut credential = identity_credential(&context, "https://data.example");
        credential.validate(&context).unwrap();
        expire_identity(&mut credential);
        let failure = resolve_loaded(graph, &context, credential)
            .err()
            .expect("expired credential must not construct a client");
        assert_eq!(failure.body["type"], "data_credential_expired");
    }
}

#[tokio::test]
async fn loaded_data_credential_expiry_is_rechecked_after_discovery() {
    for graph in [Some("knowledge"), None] {
        let expiry = OffsetDateTime::now_utc().unix_timestamp() + 2;
        let data = IntentApiFixture::graph_with_discovery_hook(
            vec![IntentReply::json(200, json!({"graphs": []}))],
            move || {
                while OffsetDateTime::now_utc().unix_timestamp() < expiry {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            },
        );
        let mut context = context();
        context.endpoint = Some(data.origin.clone());
        let mut credential = identity_credential(&context, &data.origin);
        set_identity_expiry(&mut credential, expiry);
        crate::operator_routing::test_transport_scope(&[&data.origin], async {
            let client = resolve_loaded(graph, &context, credential).unwrap();
            let result = if graph.is_some() {
                client
                    .invoke_named::<Value>("write", true, None, None, None, None)
                    .await
            } else {
                client.list_graphs().await.map(|_| Value::Null)
            };
            assert_eq!(
                data.requests().len(),
                1,
                "expired data credentials must not be dispatched"
            );
            assert!(!data.requests()[0].headers.contains_key("authorization"));
            assert_eq!(
                result
                    .unwrap_err()
                    .downcast_ref::<crate::credential_sources::CredentialError>(),
                Some(&crate::credential_sources::CredentialError::Expired)
            );
        })
        .await;
    }
}

#[tokio::test]
async fn managed_mcp_credential_expiry_is_rechecked_after_discovery() {
    let expiry = OffsetDateTime::now_utc().unix_timestamp() + 2;
    let data = IntentApiFixture::graph_with_discovery_hook(vec![], move || {
        while OffsetDateTime::now_utc().unix_timestamp() < expiry {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
    let mut context = context();
    context.endpoint = Some(data.origin.clone());
    let mut credential = identity_credential(&context, &data.origin);
    set_identity_expiry(&mut credential, expiry);
    crate::operator_routing::test_transport_scope(&[&data.origin], async {
        let GraphClient::Remote { http, token, .. } =
            resolve_loaded(None, &context, credential).unwrap()
        else {
            unreachable!()
        };
        let request = http
            .request(Method::POST, format!("{}/mcp", data.origin))
            .unwrap()
            .bearer_auth(token.unwrap())
            .body("{}");
        let error = http.send_mcp(request).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<crate::credential_sources::CredentialError>(),
            Some(&crate::credential_sources::CredentialError::Expired)
        );
        assert_eq!(data.requests().len(), 1);
        assert!(!data.requests()[0].headers.contains_key("authorization"));
        data.assert_complete();
    })
    .await;
}

#[test]
fn refused_credential_removal_preserves_a_concurrent_replacement() {
    let context = context();
    let store = MemoryStore::default();
    let credential = identity_credential(&context, "https://data.example");
    save(&store, &context, &credential);
    remove_refused(&store, &context, "another-refused-token").unwrap();
    assert!(store.get(&key(&context)).unwrap().is_some());
    remove_refused(&store, &context, &credential.token).unwrap();
    assert!(store.get(&key(&context)).unwrap().is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn valid_data_credential_remains_usable_when_control_expires_during_discovery() {
    let directory = tempfile::tempdir().unwrap();
    let expiry = std::sync::Arc::new(std::sync::Mutex::new(None::<OffsetDateTime>));
    let data = IntentApiFixture::graph_with_discovery_hook(
        vec![IntentReply::json(200, json!({"graphs": []}))],
        {
            let expiry = expiry.clone();
            move || {
                let expiry = expiry.lock().unwrap().unwrap();
                while OffsetDateTime::now_utc() < expiry {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        },
    );
    let helper = directory.path().join("helper.sh");
    let response = directory.path().join("response.json");
    let calls = directory.path().join("calls");
    std::fs::write(
        &helper,
        "IFS= read -r request\nprintf 'called\\n' >> \"$2\"\ncat \"$1\"\n",
    )
    .unwrap();
    let mut context = context();
    context.endpoint = Some(data.origin.clone());
    std::fs::write(
        directory.path().join("config.yaml"),
        json!({
            "version": 1, "profiles": {"work": {
                "api": context.api, "organization_id": context.organization_id,
                "auth": {"provider": "exec", "command": ["/bin/sh", helper, response, calls]}
            }}
        })
        .to_string(),
    )
    .unwrap();
    let input = crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
    let cli = crate::invocation::parse_from(["omnigraph", "cluster", "list", "--profile", "work"])
        .unwrap()
        .cli;
    let target = crate::scope::resolve(&cli, &input, directory.path(), false, &mut Vec::new())
        .unwrap()
        .unwrap();
    let authentication = crate::operator_routing::authentication(&cli, &input, Some(&target))
        .unwrap()
        .unwrap();
    crate::operator_routing::test_transport_scope(&[&data.origin], async {
        crate::operator_routing::AUTH
            .scope(Some(authentication.clone()), async {
                let expires = OffsetDateTime::now_utc() + time::Duration::seconds(2);
                *expiry.lock().unwrap() = Some(expires);
                std::fs::write(
                    &response,
                    json!({"version": 1,
                        "credential": {"type": "bearer", "token": "expiring-control-test-only"},
                        "expires_at": expires.format(&Rfc3339).unwrap()
                    })
                    .to_string(),
                )
                .unwrap();
                authentication.token().await.unwrap();
                let credential = identity_credential(&context, &data.origin);
                let expected = format!("Bearer {}", credential.token);
                let client = resolve_loaded(None, &context, credential).unwrap();
                client.list_graphs().await.unwrap();
                assert_eq!(data.requests().len(), 2);
                assert_eq!(data.requests()[1].headers["authorization"], expected);
                assert_eq!(
                    std::fs::read_to_string(&calls).unwrap(),
                    "called\n",
                    "the valid data request does not renew its expired control credential",
                );
                assert_eq!(
                    authentication
                        .token()
                        .await
                        .unwrap_err()
                        .downcast_ref::<crate::credential_sources::CredentialError>(),
                    Some(&crate::credential_sources::CredentialError::Expired)
                );
                assert_eq!(
                    std::fs::read_to_string(&calls).unwrap(),
                    "called\ncalled\n",
                    "explicit control acquisition retries the helper, which still returns an expired credential",
                );
                data.assert_complete();
            })
            .await;
    })
    .await;
}

#[tokio::test]
async fn issuance_reuses_rechecked_store_and_returns_persisted_credential_without_rereading() {
    let mut context = context();
    let cp = IntentApiFixture::with_origin(|origin| {
        context.api = origin.into();
        let credential = identity_credential(&context, "https://data.example");
        let mut response = credential.metadata();
        response["token"] = json!(credential.token);
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":context.incarnation}}),
        )]
    });
    crate::operator_routing::test_transport_scope(&[&cp.origin], async {
        let store = ReadBudgetStore {
            raw: MemoryStore::default(),
            reads: Default::default(),
            budget: 2,
        };
        let client = acquire_resource(
            None,
            &context,
            &store,
            async |_| Ok(Some("alice".into())),
            async |context| Api::new(context.api.clone(), Some("control-token".into())),
        )
        .await
        .unwrap();
        assert_eq!(store.reads.get(), 2);
        let saved: Credential =
            serde_json::from_str(&store.raw.get(&key(&context)).unwrap().unwrap()).unwrap();
        assert!(
            matches!(client, GraphClient::Remote { token: Some(token), .. } if token == saved.token)
        );
        cp.assert_complete();
    })
    .await;
}

#[derive(Default)]
struct BoundDataStore(MemoryStore);

impl Store for BoundDataStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.0.get(&crate::operator_routing::data_namespace(key))
    }
    fn put(&self, key: &str, value: &str) -> Result<()> {
        self.0
            .put(&crate::operator_routing::data_namespace(key), value)
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.0.remove(&crate::operator_routing::data_namespace(key))
    }
}

#[tokio::test]
async fn stored_token_cache_survives_control_outage_but_not_expiry_or_generation_change() {
    let data = IntentApiFixture::graph(vec![IntentReply::json(200, json!({"graphs":[]}))]);
    let mut context = context();
    context.endpoint = Some(data.origin.clone());
    let cp = IntentApiFixture::with_origin(|origin| {
        context.api = origin.into();
        let credential = identity_credential(&context, &data.origin);
        let mut response = credential.metadata();
        response["token"] = json!(credential.token);
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":context.incarnation}}),
        )]
    });
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("config.yaml"), format!(
        "version: 1\nprofiles:\n  work:\n    api: {}\n    organization_id: org_test\n    auth:\n      provider: token\n      source: {{store: {{}}}}\n", cp.origin
    )).unwrap();
    let cli =
        crate::invocation::parse_from(["omnigraph", "credentials", "status", "--profile", "work"])
            .unwrap()
            .cli;
    let binding = || {
        let input = crate::operator::InvocationConfig::at(Some(directory.path().to_owned()), None);
        let target =
            crate::scope::resolve(&cli, &input, directory.path(), true, &mut std::io::sink())
                .unwrap();
        crate::operator_routing::authentication(&cli, &input, target.as_ref())
            .unwrap()
            .unwrap()
    };
    let auth = binding();
    let persistent = BoundDataStore::default();
    let api_origin = cp.origin.clone();
    let data_origin = data.origin.clone();
    crate::operator_routing::test_transport_scope(&[&api_origin, &data_origin], async {
        crate::operator_routing::AUTH
            .scope(Some(auth.clone()), async {
                let store = ResourceStore::for_profile(
                    &auth.profile,
                    &auth.derived_credentials,
                    &persistent,
                    false,
                );
                let api = Api::new(context.api.clone(), Some("test-control-token".into())).unwrap();
                mint_for_principal(&store, &context, &api, 3600, Some("alice"))
                    .await
                    .unwrap();
                cp.assert_complete();
                drop(cp);
                let client = acquire_resource(
                    None,
                    &context,
                    &store,
                    async |_| {
                        panic!("validated stored-token cache must not contact control identity")
                    },
                    async |_| panic!("validated stored-token cache must not exchange again"),
                )
                .await
                .unwrap();
                assert!(client.list_graphs().await.unwrap().graphs.is_empty());
                let original = store.get(&key(&context)).unwrap().unwrap();
                let mut expired: Credential = serde_json::from_str(&original).unwrap();
                expire_identity(&mut expired);
                store
                    .put(&key(&context), &serde_json::to_string(&expired).unwrap())
                    .unwrap();
                let failure = acquire_resource(
                    None,
                    &context,
                    &store,
                    async |_| Err(Failure::refused("control_unavailable", "test outage")),
                    async |_| panic!("failed identity verification must not issue credentials"),
                )
                .await
                .err()
                .expect("expired credential must require control verification");
                assert_eq!(failure.body["type"], "control_unavailable");
                store.put(&key(&context), &original).unwrap();
                auth.invalidate().unwrap();
                let failure = acquire_resource(
                    None,
                    &context,
                    &store,
                    async |_| panic!("stale invocation must refuse locally"),
                    async |_| panic!("stale invocation must not issue credentials"),
                )
                .await
                .err()
                .expect("invalidated generation must refuse reuse");
                assert_eq!(failure.body["type"], "credential_generation_changed");
            })
            .await;
        let fresh = binding();
        crate::operator_routing::AUTH
            .scope(Some(fresh.clone()), async {
                let store = ResourceStore::for_profile(
                    &fresh.profile,
                    &fresh.derived_credentials,
                    &persistent,
                    false,
                );
                assert!(store.get(&key(&context)).unwrap().is_none());
                let failure = acquire_resource(
                    None,
                    &context,
                    &store,
                    async |_| Err(Failure::refused("control_unavailable", "test outage")),
                    async |_| panic!("new generation requires successful verification"),
                )
                .await
                .err()
                .expect("new generation must require control verification");
                assert_eq!(failure.body["type"], "control_unavailable");
            })
            .await;
    })
    .await;
    data.assert_complete();
}

#[tokio::test]
async fn token_clear_skips_unavailable_transport_and_removes_local_credential() {
    for proxy_conflict in [false, true] {
        let api = IntentApiFixture::new(vec![]);
        let directory = tempfile::tempdir().unwrap();
        crate::operator_routing::test_transport_scope(&[&api.origin], async {
            crate::operator_routing::test_broken_control_transport_scope(
                &api.origin,
                directory.path().join("missing.pem"),
                proxy_conflict,
                async {
                    let mut context = context();
                    context.api = api.origin.clone();
                    let store = MemoryStore::default();
                    save(
                        &store,
                        &context,
                        &credential(&context, "https://data.example"),
                    );
                    let cli = crate::invocation::parse_from([
                        "omnigraph",
                        "cluster",
                        "token",
                        "--clear",
                        "--target",
                        "cloud",
                    ])
                    .unwrap()
                    .cli;
                    let target = crate::targets::ResolvedTarget::new(
                        crate::targets::TargetBinding::Managed(crate::managed::Binding {
                            api: api.origin.clone(),
                            organization_id: Some(context.organization_id.clone()),
                            cluster: Some(crate::managed::ClusterBinding {
                                cluster_id: context.cluster.clone(),
                                cluster_incarnation: context.incarnation.clone(),
                                endpoint: context.endpoint.clone(),
                                default_graph: None,
                            }),
                            control_auth: crate::managed::ControlAuth::HumanDevice,
                        }),
                        crate::targets::TargetProvenance::Explicit {
                            name: "cloud".into(),
                        },
                    );
                    crate::operator_routing::freeze_transports(&cli, Some(&target)).unwrap();
                    let result = token_with_store(&cli, &context, None, true, &store)
                        .await
                        .unwrap();
                    assert_eq!(result["data"]["local_credential_removed"], true);
                    assert_eq!(result["data"]["revocation_performed"], false);
                    assert!(store.get(&key(&context)).unwrap().is_none());
                    // Invocation-only profiles cannot claim to have removed a
                    // credential from a previous process or persistent source.
                    let invocation = InvocationCredentials::default();
                    let unavailable = UnavailablePersistentStore::default();
                    let profile = crate::operator_schema::AuthProfile::OauthClientCredentials {
                        token_endpoint: "https://provider.example/token".into(),
                        client_id: "client_one".into(),
                        client_secret: crate::operator_schema::ClientSecretSource::Env {
                            env: "UNUSED_CLIENT_SECRET".into(),
                        },
                        scope: None,
                    };
                    let selected =
                        ResourceStore::for_profile(&profile, &invocation, &unavailable, true);
                    let cleared = token_with_store(&cli, &context, None, true, &selected)
                        .await
                        .unwrap();
                    assert_eq!(cleared["data"]["local_credential_removed"], false);
                    invocation
                        .put(&key(&context), "invocation-only-credential")
                        .unwrap();
                    let cleared = token_with_store(&cli, &context, None, true, &selected)
                        .await
                        .unwrap();
                    assert_eq!(cleared["data"]["local_credential_removed"], true);
                    assert!(invocation.get(&key(&context)).unwrap().is_none());
                    assert_eq!(
                        unavailable.0.get(),
                        0,
                        "OAuth clear never probes prior persistent custody"
                    );
                    assert!(api.requests().is_empty());
                },
            )
            .await;
        })
        .await;
        api.assert_complete();
    }
}

#[derive(Default)]
struct UnavailablePersistentStore(std::cell::Cell<usize>);

impl Store for UnavailablePersistentStore {
    fn get(&self, _: &str) -> Result<Option<String>> {
        self.0.set(self.0.get() + 1);
        Err(Failure::refused(
            "keychain_unavailable",
            "unavailable persistent test store",
        ))
    }
    fn put(&self, _: &str, _: &str) -> Result<()> {
        panic!("unavailable store must never receive credentials")
    }
    fn remove(&self, _: &str) -> Result<()> {
        panic!("resource acquisition must not remove persistent custody")
    }
}

fn external_profiles() -> Vec<crate::operator_schema::AuthProfile> {
    use crate::operator_schema::{AuthProfile, TokenSource};
    vec![
        AuthProfile::Token {
            source: TokenSource::Env {
                env: "EXPLICIT_TEST_TOKEN".into(),
            },
        },
        AuthProfile::Token {
            source: TokenSource::File {
                file: "/explicit/token".into(),
            },
        },
        AuthProfile::Exec {
            command: vec!["/explicit/helper".into()],
            timeout: "30s".into(),
        },
    ]
}

#[tokio::test]
async fn external_resource_and_export_acquire_only_invocation_credentials_without_keychain() {
    for profile in external_profiles() {
        let mut context = context();
        let cp = IntentApiFixture::with_origin(|origin| {
            context.api = origin.to_owned();
            let credential = identity_credential(&context, "https://data.example");
            let mut response = credential.metadata();
            response["token"] = json!(credential.token);
            vec![IntentReply::json(
                200,
                json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":context.incarnation}}),
            )]
        });
        crate::operator_routing::test_transport_scope(&[&cp.origin], async {
        let unavailable = UnavailablePersistentStore::default();
        let memory = InvocationCredentials::default();
        let store = ResourceStore::for_profile(&profile, &memory, &unavailable, false);
        assert!(!store.persistent());
        assert!(ResourceStore::for_profile(&profile, &memory, &unavailable, true).persistent());
        let exported = acquire_resource(
            None,
            &context,
            &store,
            async |_| Ok(Some("alice".into())),
            async |context| Api::new(context.api.clone(), Some("external-control-token".into())),
        )
        .await
        .unwrap();
        let GraphClient::Remote {
            token: Some(exported),
            ..
        } = exported
        else {
            panic!("managed export requires the shared remote client");
        };
        assert_ne!(exported, "external-control-token");
        let client = acquire_resource(
            Some("knowledge"),
            &context,
            &store,
            async |_| Ok(Some("alice".into())),
            async |_| panic!("same invocation should reuse its validated data credential"),
        )
        .await
        .unwrap();
        assert!(
            matches!(client, GraphClient::Remote { token: Some(token), .. } if token == exported)
        );
        assert_eq!(unavailable.0.get(), 0);
        assert_eq!(cp.requests().len(), 1);
        assert_eq!(cp.requests()[0].path, "/v1/clusters/cluster-a/tokens");
        assert!(
            InvocationCredentials::default()
                .get(&key(&context))
                .unwrap()
                .is_none()
        );
        assert!(!format!("{memory:?}").contains(&exported));
        cp.assert_complete();
    }).await;
    }
}

#[tokio::test]
async fn external_source_failure_never_falls_back_or_submits_an_exchange() {
    for profile in external_profiles() {
        let unavailable = UnavailablePersistentStore::default();
        let memory = InvocationCredentials::default();
        let store = ResourceStore::for_profile(&profile, &memory, &unavailable, false);
        let context = context();
        let failure = acquire_resource(
            Some("knowledge"),
            &context,
            &store,
            async |_| {
                Err(Failure::refused(
                    "credential_unavailable",
                    "selected external source failed",
                ))
            },
            async |_| {
                panic!("failed external source must not contact issuer or select another provider")
            },
        )
        .await
        .err()
        .unwrap();
        assert_eq!(failure.body["type"], "credential_unavailable");
        assert_eq!(unavailable.0.get(), 0);
        assert!(memory.get(&key(&context)).unwrap().is_none());
    }
}

#[tokio::test]
async fn persistent_profiles_do_not_fall_back_to_invocation_custody() {
    use crate::operator_schema::{AuthProfile, Empty, TokenSource};
    for profile in [
        AuthProfile::Omnigraph,
        AuthProfile::Token {
            source: TokenSource::Store { store: Empty {} },
        },
    ] {
        let unavailable = UnavailablePersistentStore::default();
        let memory = InvocationCredentials::default();
        let store = ResourceStore::for_profile(&profile, &memory, &unavailable, false);
        assert!(store.persistent());
        let context = context();
        let failure = acquire_resource(
            Some("knowledge"),
            &context,
            &store,
            async |_| {
                panic!("unavailable persistent custody must fail before identity verification")
            },
            async |_| panic!("unavailable persistent custody must fail before exchange"),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(failure.body["type"], "keychain_unavailable");
        assert_eq!(unavailable.0.get(), 1);
        assert!(memory.get(&key(&context)).unwrap().is_none());
    }
}

#[tokio::test]
async fn invocation_credentials_require_verified_identity_and_exact_resource_exchange() {
    for mismatch in [
        "principal",
        "tenant",
        "incarnation",
        "endpoint",
        "forbidden",
    ] {
        let mut context = context();
        let cp = IntentApiFixture::with_origin(|origin| {
            context.api = origin.to_owned();
            if mismatch == "forbidden" {
                return vec![IntentReply::json(403, json!({"type":"forbidden"}))];
            }
            let mut issued = context.clone();
            if mismatch == "tenant" {
                issued.organization_id = "org_other".into();
            }
            let credential = identity_credential(
                &issued,
                if mismatch == "endpoint" {
                    "https://wrong.example"
                } else {
                    "https://data.example"
                },
            );
            let mut response = credential.metadata();
            response["token"] = json!(credential.token);
            vec![IntentReply::json(
                200,
                json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":if mismatch == "incarnation" { "incarnation-other" } else { "incarnation-a" }}}),
            )]
        });
        crate::operator_routing::test_transport_scope(&[&cp.origin], async {
            let unavailable = UnavailablePersistentStore::default();
            let memory = InvocationCredentials::default();
            let profile = external_profiles().remove(0);
            let store = ResourceStore::for_profile(&profile, &memory, &unavailable, false);
            assert!(
                acquire_resource(
                    Some("knowledge"),
                    &context,
                    &store,
                    async |_| Ok(Some(
                        if mismatch == "principal" {
                            "bob"
                        } else {
                            "alice"
                        }
                        .into()
                    )),
                    async |context| Api::new(
                        context.api.clone(),
                        Some("external-control-token".into())
                    )
                )
                .await
                .is_err()
            );
            assert_eq!(unavailable.0.get(), 0);
            assert!(memory.get(&key(&context)).unwrap().is_none());
            assert_eq!(cp.requests().len(), 1);
            cp.assert_complete();
        })
        .await;
    }
    let profile = external_profiles().remove(0);
    let memory = InvocationCredentials::default();
    let unavailable = UnavailablePersistentStore::default();
    let store = ResourceStore::for_profile(&profile, &memory, &unavailable, false);
    assert!(
        acquire_resource(
            None,
            &context(),
            &store,
            async |_| Ok(None),
            async |_| panic!("unverified identity must not exchange credentials")
        )
        .await
        .is_err()
    );
    assert_eq!(unavailable.0.get(), 0);
}

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
        crate::operator_routing::test_transport_scope(&[&cp.origin], async {
            let store = MemoryStore::default();
            if expired {
                let mut saved = identity_credential(&context, "https://data.example");
                expire_identity(&mut saved);
                save(&store, &context, &saved);
            }

            let cli =
                Cli::try_parse_from(["omnigraph", "mutate", "m", "--graph", "knowledge"]).unwrap();
            let client = resolve_with_acquisition(
                &cli,
                &context,
                &store,
                async |_| Ok(Some("alice".into())),
                async |context| Api::new(context.api.clone(), Some("provider-access".into())),
            )
            .await
            .unwrap();
            let _ = client;
            assert_eq!(cp.requests().len(), 1);
            assert_eq!(cp.requests()[0].path, "/v1/clusters/cluster-a/tokens");
            assert_eq!(
                cp.requests()[0].body,
                json!({"version":2,"ttl_seconds":3600})
            );
            assert!(
                resolve_with_acquisition(
                    &cli,
                    &context,
                    &store,
                    async |_| Ok(Some("alice".into())),
                    async |_| { panic!("valid graph credential must not call the API") }
                )
                .await
                .is_ok()
            );
            cp.assert_complete();
        })
        .await;
    }
}

#[tokio::test]
async fn acquisition_refuses_retired_credentials_without_replacement() {
    let context = context();

    let store = MemoryStore::default();
    let cli = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
    for corruption in [
        "retired-cache",
        "retired-token",
        "permissions",
        "malformed-token",
        "wrong-actor",
        "wrong-expiry",
        "wrong-organization",
    ] {
        let mut cached = credential(&context, "https://data.example");
        expire_identity(&mut cached);
        match corruption {
            "retired-cache" => cached.version = 1,
            "malformed-token" => cached.token = DATA_TOKEN.into(),
            "wrong-actor" => cached.actor = "principal:other".into(),
            "wrong-organization" => {
                cached = credential(
                    &Context {
                        organization_id: "org_other".into(),
                        ..context.clone()
                    },
                    "https://data.example",
                );
                expire_identity(&mut cached);
            }
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
            &context,
            &store,
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
            &context,
            &store,
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
    crate::operator_routing::test_transport_scope(&[&cp.origin], async {
        let store = MemoryStore::default();
        let old = identity_credential(&context, "https://data.example");
        save(&store, &context, &old);
        let before = store.get(&key(&context)).unwrap();

        let cli = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
        assert!(
            resolve_with_acquisition(
                &cli,
                &context,
                &store,
                async |_| Ok(Some("bob".into())),
                async |context| Api::new(context.api.clone(), Some("bob-provider-token".into()))
            )
            .await
            .is_err()
        );
        assert_eq!(store.get(&key(&context)).unwrap(), before);
        cp.assert_complete();
    })
    .await;
}

fn context() -> Context {
    Context {
        incarnation: "incarnation-a".into(),
        endpoint: Some("https://data.example".into()),
        default_graph: None,
        control_auth: super::super::ControlAuth::HumanDevice,
        organization_id: "org_test".into(),
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
        "sub":"alice","account_id":context.organization_id,"cluster_id":context.cluster,"cluster_incarnation":"incarnation-a",
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
    set_identity_expiry(credential, OffsetDateTime::now_utc().unix_timestamp() - 1);
}

fn set_identity_expiry(credential: &mut Credential, expiry: i64) {
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
        "healthz",
        "openapi",
    ] {
        assert!(graph_id(bad).is_err());
    }
    for valid in ["1graph", "-graph", "og_catalog", &"a".repeat(64)] {
        assert!(graph_id(valid).is_ok(), "{valid}");
    }
    assert!(graph_id(&"a".repeat(65)).is_err());
    assert!(Cli::try_parse_from(["omnigraph", "cluster", "token", "--clear"]).is_ok());
    assert!(Cli::try_parse_from(["omnigraph", "cluster", "token"]).is_ok());
    assert!(Cli::try_parse_from(["omnigraph", "query", "q", "--direct"]).is_err());
}

#[tokio::test]
async fn minted_data_credential_is_separate_and_works_after_api_stops() {
    let data = IntentApiFixture::graph(vec![
        IntentReply::json(200, read_reply()),
        IntentReply::json(200, change_reply()),
        IntentReply::json(200, read_reply()),
    ]);
    crate::operator_routing::test_transport_scope(&[&data.origin], async {
    let mut context = context();
    let cp = IntentApiFixture::with_origin(|origin| {
        context.api = origin.to_owned();
        context.endpoint = Some(data.origin.clone());
        let response_credential = credential(&context, &data.origin);
        let mut response = response_credential.metadata();
        response["token"] = json!(response_credential.token);
        response["access_token"] = json!("must-not-be-output");
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}),
        )]
    });
let cp_origin = cp.origin.clone();
crate::operator_routing::test_transport_scope(&[&cp_origin], async {
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
    let client = resolve_resource(Some("knowledge"), &context, &data_store).unwrap();
    let result = client
        .query(
            Some(ReadTarget::Branch("main".into())),
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
            Some("main"),
            "mutation m() {}",
            Some("m"),
            None,
            Some("head-a"),
            &[],
            &mut Vec::new(),
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
        resolve_resource(Some("knowledge"), &context, &data_store).err().unwrap().body["type"],
        "data_credential_required"
    );
    assert!(cp_store.get(&context.api).unwrap().is_some());
    assert_eq!(
        data_store.get(&key(&other)).unwrap().as_deref(),
        Some("other-data-entry")
    );
}).await;
}).await;
}

#[tokio::test]
async fn identity_issuance_caches_no_permissions_and_discovers_without_control_calls() {
    let discovery = json!({"graphs":[{"graph_id":"hidden","display_name":"hidden"}]});
    let commit = json!({"graph_commit_id":"head-a","graph_branch":"main","graph_manifest_version":7,
        "parent_commit_id":null,"merged_parent_commit_id":null,"actor_id":"principal:alice","created_at":12345,"published":true,
        "kind":"published","root_commit_id":null});
    let data = IntentApiFixture::graph(vec![
        IntentReply::json(200, discovery.clone()),
        IntentReply::json(
            403,
            json!({"error":"current policy denies change","code":"forbidden"}),
        ),
        IntentReply::json(200, json!({"commits":[commit.clone()]})),
        IntentReply::json(200, commit.clone()),
    ]);
    crate::operator_routing::test_transport_scope(&[&data.origin], async {
    let mut context = context();
    let cp = IntentApiFixture::with_origin(|origin| {
        context.api = origin.to_owned();
        context.endpoint = Some(data.origin.clone());
        let credential = identity_credential(&context, &data.origin);
        let mut response = credential.metadata();
        response["token"] = json!(credential.token);
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}),
        )]
    });
let cp_origin = cp.origin.clone();
crate::operator_routing::test_transport_scope(&[&cp_origin], async {
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
        resolve_resource(Some("any-graph"), &context, &store).is_ok(),
        "Cedar, not the local cache, decides permission"
    );
    let dir = tempfile::tempdir().unwrap();

    let cli = Cli::try_parse_from(["omnigraph", "graphs", "list", "--json"]).unwrap();
    let client = resolve_context(&cli, &context, &store).unwrap();
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
        let client = resolve_context(&selected, &context, &store).unwrap();
        match command.as_slice() {
            ["load", ..] => {
                let error = client
                    .load(
                        "review",
                        Some("main"),
                        batch.to_str().unwrap(),
                        crate::cli::CliLoadMode::Append,
                        &[],
                        &mut Vec::new(),
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
    let failure = resolve_context(&cli, &context, &store).err().unwrap();
    assert_eq!(failure.body["type"], "data_credential_invalid");
}).await;
}).await;
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
        "different-endpoint",
        "token",
        "organization",
    ] {
        let mut context = context();
        let cp = IntentApiFixture::with_origin(|origin| {
            context.api = origin.to_owned();
            let credential_context = if field == "organization" {
                Context {
                    organization_id: "org_other".into(),
                    ..context.clone()
                }
            } else {
                context.clone()
            };
            let credential = identity_credential(&credential_context, "https://data.example");
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
                "different-endpoint" => {
                    envelope["data"]["endpoint"] = json!("https://rotated.example")
                }
                "token" => envelope["data"]["token"] = json!("x".repeat(MAX_TOKEN + 1)),
                "organization" => {}
                _ => envelope["meta"]["incarnation"] = json!("other"),
            }
            vec![IntentReply::json(200, envelope)]
        });
        crate::operator_routing::test_transport_scope(&[&cp.origin], async {
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
        })
        .await;
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
            resolve_resource(Some("knowledge"), &context, &store).is_err(),
            "accepted {field}"
        );
    }
    let duplicate = serde_json::to_string(&original)
        .unwrap()
        .replacen("{", "{\"version\":1,", 1);
    store.put(&key(&context), &duplicate).unwrap();
    assert!(resolve_resource(Some("knowledge"), &context, &store).is_err());
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

#[tokio::test]
async fn managed_commit_reads_use_exact_cached_read_authority_without_api_or_fallback() {
    let mut context = context();

    let store = MemoryStore::default();
    let commit = json!({
        "graph_commit_id":"commit-a", "graph_branch":null,
        "graph_manifest_version":3, "parent_commit_id":"prior",
        "merged_parent_commit_id":"imported-head", "actor_id":"principal:alice",
        "created_at":123456, "published":true,
        "kind":"published", "root_commit_id":null,
    });
    let server = IntentApiFixture::graph(vec![
        IntentReply::json(200, json!({"commits":[commit.clone()]})),
        IntentReply::json(200, commit.clone()),
    ]);
    crate::operator_routing::test_transport_scope(&[&server.origin], async {
        context.endpoint = Some(server.origin.clone());
        let cached = credential(&context, &server.origin);
        for command in [vec!["commit", "list"], vec!["commit", "show", "commit-a"]] {
            let cli = Cli::try_parse_from(
                ["omnigraph", "--graph", "knowledge"]
                    .into_iter()
                    .chain(command.clone()),
            )
            .unwrap();
            assert_eq!(
                resolve_context(&cli, &context, &store).err().unwrap().body["type"],
                "data_credential_required"
            );
            save(&store, &context, &cached);
            let client = resolve_context(&cli, &context, &store).unwrap();
            if command[1] == "list" {
                let output = client.list_commits(Some("main")).await.unwrap();
                assert_eq!(serde_json::to_value(&output.commits[0]).unwrap(), commit);
            } else {
                let output = client.get_commit("commit-a").await.unwrap();
                assert_eq!(serde_json::to_value(output).unwrap(), commit);
            }
            clear(&store, &context).unwrap();
            let no_graph =
                Cli::try_parse_from(std::iter::once("omnigraph").chain(command)).unwrap();
            assert_eq!(
                resolve_context(&no_graph, &context, &NoCredentialAccess)
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
    })
    .await;
}

#[test]
fn managed_load_uses_identity_and_keeps_target_preflight() {
    let context = context();

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
        resolve_context(&cli, &context, &store).err().unwrap().body["type"],
        "data_credential_required"
    );
    save(
        &store,
        &context,
        &credential(&context, "https://data.example"),
    );
    for extra in [vec![], vec!["--branch", "review", "--from", "main"]] {
        let cli = Cli::try_parse_from(args.into_iter().chain(extra)).unwrap();
        assert!(resolve_context(&cli, &context, &store).is_ok());
    }
    let cli = Cli::try_parse_from(args.into_iter().chain(["--as", "fake"])).unwrap();
    assert_eq!(
        resolve_context(&cli, &context, &NoCredentialAccess)
            .err()
            .unwrap()
            .body["type"],
        "managed_scope_conflict"
    );
}

#[tokio::test]
async fn managed_data_transport_refuses_redirect_and_bounds_body() {
    let target = IntentApiFixture::new(vec![]);
    crate::operator_routing::test_transport_scope(&[&target.origin], async {
        let batch = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(batch.path(), "{}\n").unwrap();
        let mut chunked = format!("{:x}\r\n", 8 * 1024 * 1024 + 1).into_bytes();
        chunked.extend(vec![b' '; 8 * 1024 * 1024 + 1]);
        chunked.extend_from_slice(b"\r\n0\r\n\r\n");
        let operations = [
            "query",
            "load",
            "commit-list",
            "commit-show",
            "access",
            "queries",
            "branch-list",
            "snapshot",
            "schema",
            "graph-list",
        ];
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
        crate::operator_routing::test_transport_scope(&[&server.origin], async {
            let client = GraphClient::managed(
                &server.origin,
                "knowledge",
                crate::credential_sources::test_credential(&server.origin, DATA_TOKEN),
            )
            .unwrap();
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
                                &mut Vec::new(),
                            )
                            .await
                            .unwrap_err(),
                        "commit-list" => client.list_commits(Some("main")).await.unwrap_err(),
                        "commit-show" => client.get_commit("commit-a").await.unwrap_err(),
                        "access" => client.access(None, None).await.unwrap_err(),
                        "queries" => client.list_queries().await.unwrap_err(),
                        "branch-list" => client.branch_list().await.unwrap_err(),
                        "snapshot" => client.snapshot("main").await.unwrap_err(),
                        "schema" => client.schema_source().await.unwrap_err(),
                        "graph-list" => client.list_graphs().await.unwrap_err(),
                        _ => client
                            .query(
                                Some(ReadTarget::Branch("main".into())),
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
        })
        .await;
    })
    .await;
}

#[tokio::test]
async fn managed_stream_and_bodyless_refusals_remain_bounded_and_redacted() {
    let refused = IntentReply::json(403, json!({"error": format!("refused {DATA_TOKEN}")}));
    let oversized = IntentReply {
        status: 403,
        headers: vec![("Content-Length".into(), (8 * 1024 * 1024 + 1).to_string())],
        body: vec![],
    };
    let export_size = 8 * 1024 * 1024 + 1;
    let server = IntentApiFixture::graph(vec![
        refused.clone(),
        refused.clone(),
        refused,
        oversized.clone(),
        oversized.clone(),
        oversized,
        IntentReply {
            status: 200,
            headers: vec![],
            body: vec![b' '; export_size],
        },
        IntentReply {
            status: 204,
            headers: vec![],
            body: vec![],
        },
    ]);
    crate::operator_routing::test_transport_scope(&[&server.origin], async {
        let client = GraphClient::managed(
            &server.origin,
            "knowledge",
            crate::credential_sources::test_credential(&server.origin, DATA_TOKEN),
        )
        .unwrap();
        for oversized in [false, true] {
            for operation in ["export", "baseline", "drop"] {
                let error = match operation {
                    "export" => client
                        .export("main", &[], &mut Vec::new())
                        .await
                        .unwrap_err(),
                    "baseline" => client
                        .change_baseline(
                            Some("main"),
                            &crate::client::ChangeFilterArgs {
                                kinds: &[],
                                types: &[],
                                ops: &[],
                            },
                            &mut Vec::new(),
                        )
                        .await
                        .unwrap_err(),
                    _ => client
                        .commit_drop("commit-a", &mut Vec::new())
                        .await
                        .unwrap_err(),
                };
                assert!(!format!("{error:?}").contains(DATA_TOKEN));
                if oversized {
                    assert!(error.to_string().contains("8 MiB"), "{error}");
                }
            }
        }
        // Successful exports retain streaming size freedom; 204 decodes no body.
        let mut output = Vec::new();
        client.export("main", &[], &mut output).await.unwrap();
        assert_eq!(output.len(), export_size);
        client
            .commit_drop("commit-a", &mut Vec::new())
            .await
            .unwrap();
    })
    .await;
    server.assert_complete();
}

#[tokio::test]
async fn managed_data_errors_redact_reflected_credentials_including_preconditions() {
    let encoded = DATA_TOKEN.replace('h', "\\u0068");
    let batch = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(batch.path(), "{}\n").unwrap();
    let operations = [
        "mutate",
        "load",
        "commit-list",
        "commit-show",
        "access",
        "queries",
        "branch-list",
        "snapshot",
        "schema",
        "graph-list",
    ];
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
    crate::operator_routing::test_transport_scope(&[&server.origin], async {
    let client = GraphClient::managed(&server.origin, "knowledge", crate::credential_sources::test_credential(&server.origin, DATA_TOKEN)).unwrap();
    let mut completed = 0;
    for (status, _) in cases {
        for operation in operations {
            let (error, evidence) = crate::command_outcome::observe(
                crate::command_outcome::CommandKind::Write,
                async {
                    match operation {
                        "load" => client
                            .load(
                                "main",
                                None,
                                batch.path().to_str().unwrap(),
                                crate::cli::CliLoadMode::Append,
                                &[],
                                &mut Vec::new(),
                            )
                            .await
                            .unwrap_err(),
                        "commit-list" => client.list_commits(Some("main")).await.unwrap_err(),
                        "commit-show" => client.get_commit("commit-a").await.unwrap_err(),
                        "access" => client.access(None, None).await.unwrap_err(),
                        "queries" => client.list_queries().await.unwrap_err(),
                        "branch-list" => client.branch_list().await.unwrap_err(),
                        "snapshot" => client.snapshot("main").await.unwrap_err(),
                        "schema" => client.schema_source().await.unwrap_err(),
                        "graph-list" => client.list_graphs().await.unwrap_err(),
                        _ => client
                            .mutate(
                                Some("main"),
                                "mutation m() {}",
                                Some("m"),
                                None,
                                Some("head-a"),
                                &[],
                                &mut Vec::new(),
                            )
                            .await
                            .unwrap_err(),
                    }
                },
            )
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
}).await;
}

#[tokio::test]
#[ignore = "nightly: a real 30.75 s reply proves the 300 s load timeout outlives the 30 s managed deadline"]
async fn managed_load_sends_exact_ndjson_and_preserves_the_server_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let mut context = context();

    let batch = dir.path().join("batch.jsonl");
    let ndjson = "{\"type\":\"Person\",\"data\":{\"name\":\"Ada\"}}\n{\"type\":\"Person\",\"data\":{\"name\":\"Grace\"}}\n";
    std::fs::write(&batch, ndjson).unwrap();
    let commit = json!({"graph_commit_id":"head-load","graph_branch":"review","graph_manifest_version":7,"parent_commit_id":"before","merged_parent_commit_id":null,"actor_id":"principal:alice","created_at":12345,"published":true,
        "kind":"published","root_commit_id":null});
    let reply = json!({"branch":"review","base_branch":"main","branch_created":true,"mode":"append","nodes":[{"name":"Person","entities_loaded":2}],"edges":[],"total_entities":2,"embedding_generation":null,"actor_id":"principal:alice","commit":commit});
    // This is a real response past the ordinary managed 30-second deadline.
    // The request-construction owner separately pins load's 300-second ceiling.
    let server = IntentApiFixture::graph_with_response_delay(
        vec![IntentReply::json(200, reply)],
        std::time::Duration::from_millis(30_750),
    );
    crate::operator_routing::test_transport_scope(&[&server.origin], async {
        let store = MemoryStore::default();
        context.endpoint = Some(server.origin.clone());
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
        let client = resolve_context(&cli, &context, &store).unwrap();
        let result = client
            .load(
                "review",
                Some("main"),
                batch.to_str().unwrap(),
                crate::cli::CliLoadMode::Append,
                &[],
                &mut Vec::new(),
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
    })
    .await;
}

#[tokio::test]
async fn managed_load_refuses_local_overflow_before_io_and_never_replays_failed_receipts() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let empty_server = IntentApiFixture::new(vec![]);
    crate::operator_routing::test_transport_scope(&[&empty_server.origin], async {
        let client = GraphClient::managed(
            &empty_server.origin,
            "knowledge",
            crate::credential_sources::test_credential(&empty_server.origin, DATA_TOKEN),
        )
        .unwrap();
        file.as_file().set_len(32 * 1024 * 1024 + 1).unwrap();
        assert!(
            client
                .load(
                    "review",
                    Some("main"),
                    file.path().to_str().unwrap(),
                    crate::cli::CliLoadMode::Append,
                    &[],
                    &mut Vec::new(),
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
                    &mut Vec::new(),
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
            crate::operator_routing::test_transport_scope(&[&server.origin], async {
                let client = GraphClient::managed(
                    &server.origin,
                    "knowledge",
                    crate::credential_sources::test_credential(&server.origin, DATA_TOKEN),
                )
                .unwrap();
                assert!(
                    client
                        .load(
                            "review",
                            Some("main"),
                            file.path().to_str().unwrap(),
                            crate::cli::CliLoadMode::Append,
                            &[],
                            &mut Vec::new(),
                        )
                        .await
                        .is_err()
                );
                server.assert_complete();
            })
            .await;
        }
    })
    .await;
}

#[tokio::test]
async fn target_preflight_precedes_credentials_and_missing_data_endpoint_never_comes_from_cache() {
    let mut context = context();
    let cli = Cli::try_parse_from(["omnigraph", "query", "q", "--graph", "knowledge"]).unwrap();
    context.endpoint = None;
    let error = resolve_with_acquisition(
        &cli,
        &context,
        &NoCredentialAccess,
        async |_| panic!("incomplete target consulted identity"),
        async |_| panic!("incomplete target contacted issuer"),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.body["type"], "data_endpoint_required");
    context.endpoint = Some("https://data.example".into());
    let store = MemoryStore::default();
    save(
        &store,
        &context,
        &credential(&context, "https://data.example"),
    );
    for args in [
        vec!["branch", "list"],
        vec!["schema", "show"],
        vec!["snapshot"],
        vec!["export"],
        vec!["changes", "poll"],
    ] {
        let cli = Cli::try_parse_from(
            std::iter::once("omnigraph")
                .chain(args)
                .chain(["--graph", "knowledge"]),
        )
        .unwrap();
        assert!(resolve_context(&cli, &context, &store).is_ok());
    }
    let graphless = Cli::try_parse_from(["omnigraph", "query", "q"]).unwrap();
    assert_eq!(
        resolve_context(&graphless, &context, &NoCredentialAccess)
            .err()
            .unwrap()
            .body["type"],
        "graph_required"
    );
    let store = MemoryStore::default();
    save(
        &store,
        &context,
        &credential(&context, "https://data.example"),
    );
    context.default_graph = Some("knowledge".into());
    assert!(resolve_context(&graphless, &context, &store).is_ok());
    context.endpoint = Some("https://other.example".into());
    assert_eq!(
        resolve_context(&cli, &context, &store).err().unwrap().body["type"],
        "data_endpoint_mismatch"
    );
}

#[tokio::test]
async fn token_bootstrap_reports_endpoint_without_configuring_a_data_destination() {
    let mut context = context();
    context.endpoint = None;
    let issuer = IntentApiFixture::with_origin(|origin| {
        context.api = origin.into();
        let credential = credential(&context, "https://issued.example");
        let mut response = credential.metadata();
        response["token"] = json!(credential.token);
        vec![IntentReply::json(
            200,
            json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}),
        )]
    });
    crate::operator_routing::test_transport_scope(&[&issuer.origin], async {
        let store = MemoryStore::default();
        let api = Api::new(issuer.origin.clone(), Some("control-token".into())).unwrap();
        let result = mint(&store, &context, &api, 3600).await.unwrap();
        assert_eq!(result["data"]["endpoint"], "https://issued.example");
        assert!(context.endpoint.is_none());
        assert_eq!(
            resolve_resource(Some("knowledge"), &context, &store)
                .err()
                .unwrap()
                .body["type"],
            "data_endpoint_required"
        );
        context.endpoint = Some("https://issued.example".into());
        assert!(resolve_resource(Some("knowledge"), &context, &store).is_ok());
        issuer.assert_complete();
    })
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn workload_near_expiry_renews_once_only_before_native_issuance() {
    for (status, problem, second_refusal, calls, renewable) in [
        (409, "credential_near_expiry", false, 2, true),
        (409, "credential_near_expiry", true, 2, true),
        (401, "credential_near_expiry", false, 1, true),
        (409, "other_refusal", false, 1, true),
        (409, "credential_near_expiry", false, 1, false),
    ] {
        let mut context = context();
        context.control_auth = super::super::ControlAuth::AutomationEnv;
        let cp = IntentApiFixture::with_origin(|origin| {
            context.api = origin.to_owned();
            let response_credential = credential(&context, "https://data.example");
            let mut response = response_credential.metadata();
            response["token"] = json!(response_credential.token);
            let session = || crate::managed::auth::tests::metadata(120, "alice");
            let refusal = || {
                IntentReply::json(
                    status,
                    json!({"type":problem,"detail":"issuance preflight refused"}),
                )
            };
            let mut replies = vec![session(), refusal()];
            if calls == 2 {
                let mut renewed = session();
                let mut body: Value = serde_json::from_slice(&renewed.body).unwrap();
                body["data"]["subject"] = json!("rotated-key-subject");
                body["data"]["provider_client_id"] = json!("rotated-client");
                body["meta"]["assurance"] = json!("verified_workload");
                renewed.body = serde_json::to_vec(&body).unwrap();
                replies.push(renewed);
                replies.push(if second_refusal {refusal()} else {IntentReply::json(200, json!({"data":response,"meta":{"cluster_id":context.cluster,"incarnation":"incarnation-a"}}))});
            }
            replies
        });
        let home = tempfile::tempdir().unwrap();
        let helper = home.path().join("helper.sh");
        let count = home.path().join("calls");
        std::fs::write(&helper, "IFS= read -r request\nprintf 'called\\n' >> \"$1\"\nprintf '%s\\n' '{\"version\":1,\"credential\":{\"type\":\"bearer\",\"token\":\"test-control-token\"}}'\n").unwrap();
        let profile = if renewable {
            json!({"provider":"exec","command":["/bin/sh", helper, count]})
        } else {
            std::fs::write(home.path().join("token"), "test-control-token").unwrap();
            json!({"provider":"token","source":{"file":"./token"}})
        };
        std::fs::write(home.path().join("config.yaml"), json!({"version":1,"profiles":{"work":{"api":cp.origin,"organization_id":"org_test","auth":profile}}}).to_string()).unwrap();
        let input = crate::operator::InvocationConfig::at(Some(home.path().to_owned()), None);
        let cli =
            crate::invocation::parse_from(["omnigraph", "cluster", "list", "--profile", "work"])
                .unwrap()
                .cli;
        let target = crate::scope::resolve(&cli, &input, home.path(), false, &mut Vec::new())
            .unwrap()
            .unwrap();
        let auth = crate::operator_routing::authentication(&cli, &input, Some(&target)).unwrap();
        crate::operator_routing::test_transport_scope(&[&cp.origin], async {
            crate::operator_routing::AUTH
                .scope(auth, async {
                    let api = context.client().await.unwrap();
                    let result = issue_for_principal(
                        &MemoryStore::default(),
                        &context,
                        &api,
                        3600,
                        Some("alice"),
                    )
                    .await;
                    assert_eq!(
                        result.is_ok(),
                        calls == 2 && !second_refusal,
                        "{status}/{problem}"
                    );
                    if renewable {
                        assert_eq!(
                            std::fs::read_to_string(&count).unwrap(),
                            "called\n".repeat(calls)
                        );
                    } else {
                        assert!(!count.exists());
                        let failure = result.err().unwrap();
                        assert_eq!(failure.body["type"], problem);
                        assert_eq!(failure.body["detail"], "issuance preflight refused");
                        assert_eq!(failure.response_status, Some(reqwest::StatusCode::CONFLICT));
                    }
                    let requests: Vec<_> = cp
                        .requests()
                        .into_iter()
                        .filter(|request| request.path.ends_with("/tokens"))
                        .collect();
                    assert_eq!(requests.len(), calls, "token acquisition is bounded");
                    assert!(
                        requests
                            .iter()
                            .all(|request| request.body == json!({"version":2,"ttl_seconds":3600}))
                    );
                })
                .await;
        })
        .await;
        cp.assert_complete();
    }
}
