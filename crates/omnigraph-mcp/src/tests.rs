use super::*;
use crate::settings::{SETUP_URL_BYTES, validate_allowed_host, validate_setup_url};
use serde_json::json;

#[test]
fn refusals_delegate_write_context_and_preserve_resource_error_data() {
    assert!(WRITE_DEADLINE < COMMON_CLIENT_TIMEOUT);
    let host = HeldHost::default();
    // The host owns the error registry. The fixture echoes protocol events
    // and write context so this owner checks the boundary, not a second copy
    // of the server's code/disposition rules.
    for error in [
        ProtocolError::Unauthenticated,
        ProtocolError::MissingContext,
        ProtocolError::Capacity,
        ProtocolError::Cancelled,
        ProtocolError::Deadline,
        ProtocolError::ResultTooLarge,
        ProtocolError::ReadOverBudget(ReadBudget::DEFAULT),
        ProtocolError::InvalidResult,
    ] {
        for write in [false, true] {
            let result = Refusal::Protocol(error).tool_result(&host, write);
            assert_eq!(result.is_error, Some(true));
            assert_eq!(
                result.structured_content,
                Some(json!({
                    "error": {"event": format!("{error:?}"), "write": write}
                }))
            );
        }
        let resource = Refusal::Protocol(error).resource_error(&host);
        assert_eq!(
            resource.data,
            Refusal::Protocol(error)
                .tool_result(&host, false)
                .structured_content
        );
        let code = if error == ProtocolError::Unauthenticated {
            ErrorData::invalid_request("", None).code
        } else {
            ErrorData::internal_error("", None).code
        };
        assert_eq!(resource.code, code);
    }
}

/// Every tool: a write tool is annotated as one; the arguments its schema
/// requires parse, and an argument it does not declare is refused.
#[test]
fn every_tool_parses_its_required_arguments_and_writes_are_annotated() {
    let mut writes = Vec::new();
    for tool in tools() {
        let spec = spec(&tool.name).unwrap();
        let write = spec.writes();
        let hints = tool.annotations.as_ref().unwrap();
        assert_eq!(hints.read_only_hint, Some(!write), "{}", tool.name);
        if write {
            writes.push(spec.name);
        }
        let schema = &tool.input_schema;
        let mut arguments = serde_json::Map::new();
        let required = schema.get("required").and_then(Value::as_array);
        for name in required.into_iter().flatten() {
            let name = name.as_str().unwrap();
            let property = &schema["properties"][name];
            let value = match property.get("enum") {
                Some(values) => values[0].clone(),
                None => json!("x"),
            };
            arguments.insert(name.to_owned(), value);
        }
        let request = |arguments| ToolRequest::new(spec.name, Value::Object(arguments), write);
        assert!(
            request(arguments.clone()).parse().is_ok(),
            "{}: {arguments:?}",
            tool.name
        );
        arguments.insert("undeclared".into(), json!(true));
        assert!(request(arguments).parse().is_err(), "{}", tool.name);
    }
    assert_eq!(
        writes,
        [
            "mutate",
            "load",
            "blobs_clear",
            "branches_create",
            "branches_delete",
            "branches_merge",
            "commits_publish",
            "commits_drop",
        ]
    );
    // `mutate` without `query` names a stored mutation; the host requires
    // one of the two.
    assert!(matches!(
        ToolRequest::new("mutate", json!({"graph": "g", "name": "add"}), true).parse(),
        Ok(ToolCall::Mutate(MutateArguments {
            query: None,
            name: Some(_),
            ..
        }))
    ));
}

#[test]
fn every_best_practice_resource_is_an_embedded_skill_reference() {
    for topic in BEST_PRACTICES {
        let body = topic
            .body()
            .unwrap_or_else(|| panic!("{} is not embedded", topic.key));
        assert!(body.starts_with('#'), "{}", topic.key);
        assert!(best_practices_index().contains(&topic.uri()));
    }
}

#[test]
fn allowed_hosts_are_bare_authorities() {
    for host in [
        "graphs.internal",
        "graphs.internal:8080",
        "10.0.0.7",
        "[::1]:8080",
    ] {
        assert!(validate_allowed_host(host).is_ok(), "{host}");
    }
    for host in [
        "",
        "https://graphs.internal",
        "graphs.internal/mcp",
        "user@graphs.internal",
        "graphs.internal?x",
        "a b",
    ] {
        assert!(validate_allowed_host(host).is_err(), "{host}");
    }
}

#[test]
fn complete_tool_result_including_legacy_text_has_one_byte_bound() {
    let small = json!({"rows":[{"name":"Alice"}]});
    let result = bounded_result(&small).unwrap();
    assert_eq!(result.is_error, Some(false));
    assert_eq!(result.structured_content, Some(small));
    let service = test_service(ReadBudget::new(RESULT_BYTES).unwrap());
    // The first source fits but its structured + legacy text envelope does
    // not. Both limits must return an error, not disguise a completed write
    // as an ordinary successful handler return.
    for value in [
        json!({"text":"x".repeat(RESULT_BYTES * 3 / 4)}),
        json!({"text":"x".repeat(RESULT_BYTES + 1)}),
    ] {
        assert_eq!(
            bounded_result(&value).unwrap_err(),
            ProtocolError::ResultTooLarge
        );
        assert!(matches!(
            HostError::from_output(None, &value),
            Err(ProtocolError::ResultTooLarge)
        ));
        let write = service.fit_result(CallToolResult::structured(value.clone()), true);
        assert_eq!(
            write.structured_content,
            Some(json!({"error": {"event": "ResultTooLarge", "write": true}}))
        );
        assert!(serde_json::to_vec(&write).unwrap().len() <= RESULT_BYTES);
        // A read is judged by its budget, here the whole cap.
        let read = service.fit_result(CallToolResult::structured(value.clone()), false);
        assert_eq!(
            read.structured_content,
            Some(json!({
                "error": {"event": format!("ReadOverBudget({:?})", service.read_budget), "write": false}
            }))
        );
    }
}

fn test_service(read_budget: ReadBudget) -> GraphTools<HeldHost> {
    let settings = McpSettings {
        read_budget,
        ..McpSettings::default()
    };
    GraphTools {
        host: HeldHost::default(),
        version: "test".into(),
        slots: Arc::new(Semaphore::new(1)),
        read_budget,
        instructions: Arc::from(instructions(&settings)),
    }
}

/// The budget holds 1 byte to the memory cap and nothing else, and the
/// default is the documented smallest client limit: a text copy of at most
/// 50,000 characters.
#[test]
fn a_read_budget_is_never_zero_or_above_the_cap() {
    assert_eq!(ReadBudget::new(0), None);
    assert_eq!(ReadBudget::new(RESULT_BYTES + 1), None);
    assert_eq!(ReadBudget::new(1).unwrap().bytes(), 1);
    assert_eq!(ReadBudget::new(RESULT_BYTES).unwrap().bytes(), RESULT_BYTES);
    assert_eq!(ReadBudget::default(), ReadBudget::DEFAULT);
    assert_eq!(ReadBudget::DEFAULT.to_string(), "96 KiB");
    assert!(ReadBudget::DEFAULT.bytes() / 2 <= 50_000);
    assert_eq!(McpSettings::default().read_budget, ReadBudget::DEFAULT);
}

/// The budget applies to successful reads only, tool results and graph
/// resources alike: a refusal larger than it, on a read or a write, and a
/// write receipt larger than it arrive unchanged, so the caller still learns
/// what the call did. A read over it is refused, naming it.
#[test]
fn the_read_budget_bounds_successful_reads_and_nothing_else() {
    let budget = ReadBudget::new(256).unwrap();
    let service = test_service(budget);
    let over = ProtocolError::ReadOverBudget(budget);
    let refusal = HostError::from_output(
        Some(StatusCode::PRECONDITION_FAILED),
        &json!({"code": "precondition_failed", "error": "x".repeat(256)}),
    )
    .unwrap()
    .into_tool_result();
    assert!(serde_json::to_vec(&refusal).unwrap().len() > 256);
    for write in [false, true] {
        assert_eq!(service.fit_result(refusal.clone(), write), refusal);
    }
    let receipt = CallToolResult::structured(json!({"receipt": "x".repeat(256)}));
    assert_eq!(service.fit_result(receipt.clone(), true), receipt);
    let read = service.fit_result(receipt, false);
    assert_eq!(
        read.structured_content,
        Some(json!({"error": {"event": format!("{over:?}"), "write": false}}))
    );
    let fits = CallToolResult::structured(json!({"rows": []}));
    assert_eq!(service.fit_result(fits.clone(), false), fits);
    let resource = ReadResourceResult::new(vec![ResourceContents::text(
        "x".repeat(256),
        "omnigraph://graphs/g/schema",
    )]);
    assert_eq!(service.fit(&resource, false, false), Err(over));
    // The same bytes within budget pass, and a larger budget admits them.
    let small = ReadResourceResult::new(vec![ResourceContents::text(
        "x",
        "omnigraph://graphs/g/schema",
    )]);
    assert_eq!(service.fit(&small, false, false), Ok(()));
    assert_eq!(
        test_service(ReadBudget::DEFAULT).fit(&resource, false, false),
        Ok(())
    );
}

#[test]
fn setup_urls_are_validated() {
    for (url, sent) in [
        (
            "https://platform.example.com/setup",
            "https://platform.example.com/setup",
        ),
        (
            "https://example.com/organizations/o/clusters/c/graphs/new",
            "https://example.com/organizations/o/clusters/c/graphs/new",
        ),
        // Clients receive the encoded URI the schema declares.
        (
            "https://example.com/créer",
            "https://example.com/cr%C3%A9er",
        ),
        ("HTTPS://Example.COM", "https://example.com/"),
    ] {
        assert_eq!(validate_setup_url(url, "--flag").unwrap(), sent, "{url}");
    }
    let long = format!("https://example.com/{}", "a".repeat(SETUP_URL_BYTES));
    for url in [
        "http://example.com/setup",
        "https://user:secret@example.com/setup",
        "https://example.com/setup?token=x",
        "https://example.com/setup#frag",
        "https:example.com/setup",
        "https://example.com/se\ttup",
        "https://example.com/se\ntup",
        "example.com/setup",
        "",
        long.as_str(),
    ] {
        assert!(validate_setup_url(url, "--flag").is_err(), "{url}");
    }
    assert_eq!(format_bytes(RESULT_BYTES), "1 MiB");
    assert_eq!(format_bytes(128 * 1024), "128 KiB");
    assert_eq!(format_bytes(1500), "1500 bytes");
    assert_eq!(format_bytes(1), "1 byte");
}

/// Each flag wins over its environment variable, which wins over the
/// default; allowed hosts come from both. An invalid value names its source.
/// Nothing in either sets the read budget.
#[test]
fn settings_resolve_flags_over_environment() {
    let env = |name: &str| match name {
        MCP_ALLOWED_HOSTS_ENV => Some("b.example, ,c.example:8080".to_owned()),
        MCP_SETUP_URL_ENV => Some(" https://env.example.com/setup ".to_owned()),
        "OMNIGRAPH_MCP_MAX_RESULT_BYTES" => Some("256".to_owned()),
        _ => None,
    };
    let from_env = McpSettings::resolve_with(vec!["a.example".into()], None, env).unwrap();
    assert_eq!(
        from_env.allowed_hosts,
        ["a.example", "b.example", "c.example:8080"]
    );
    assert_eq!(
        from_env.setup_url.as_deref(),
        Some("https://env.example.com/setup")
    );
    assert_eq!(from_env.read_budget, ReadBudget::DEFAULT);
    let from_flags = McpSettings::resolve_with(
        Vec::new(),
        Some(" https://flag.example.com/créer ".into()),
        env,
    )
    .unwrap();
    assert_eq!(
        from_flags.setup_url.as_deref(),
        Some("https://flag.example.com/cr%C3%A9er")
    );
    let defaults = McpSettings::resolve_with(Vec::new(), None, |_| None).unwrap();
    assert_eq!(defaults, McpSettings::default());
    let error = McpSettings::resolve_with(Vec::new(), None, |asked| {
        (asked == MCP_SETUP_URL_ENV).then(|| "http://example.com".to_owned())
    })
    .unwrap_err();
    assert!(error.to_string().starts_with(MCP_SETUP_URL_ENV), "{error}");
}

#[test]
fn instructions_name_the_read_budget_and_the_setup_step_only_when_configured() {
    let defaults = instructions(&McpSettings::default());
    assert!(defaults.contains("A read's result is limited to 96 KiB"));
    assert!(defaults.contains("narrow it with filters"));
    assert!(!defaults.contains("`setup`"));
    let settings = McpSettings {
        read_budget: ReadBudget::new(128 * 1024).unwrap(),
        setup_url: Some("https://example.com/setup".into()),
        ..McpSettings::default()
    };
    let configured = instructions(&settings);
    assert!(configured.contains("A read's result is limited to 128 KiB"));
    assert!(configured.contains("returns a `setup` link"));
    // The URL itself is never in the session instructions.
    assert!(!configured.contains("https://example.com/setup"));
}

/// The GQ guide links name this binary's release tag, never `main`, and
/// every page they link exists in the tree that tag is cut from.
#[test]
fn guide_links_pin_the_release_tag_and_name_pages_that_exist() {
    let tag = concat!("/blob/v", env!("CARGO_PKG_VERSION"), "/");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for page in GUIDE_PAGES {
        assert!(root.join(page).is_file(), "{page}");
    }
    for (name, page) in [("query", GUIDE_PAGES[0]), ("mutate", GUIDE_PAGES[1])] {
        let tool = tools().into_iter().find(|tool| tool.name == name).unwrap();
        let description = tool.description.unwrap();
        assert!(
            description.contains(&format!("{tag}{page}")),
            "{description}"
        );
    }
    for tool in tools() {
        let description = tool.description.unwrap_or_default();
        assert!(!description.contains("/blob/main/"), "{}", tool.name);
    }
}

#[derive(Clone, Default)]
struct HeldHost {
    releases: Arc<std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<()>>>>,
    started: Arc<std::sync::atomic::AtomicUsize>,
    finished: Arc<tokio::sync::Notify>,
}

impl Host for HeldHost {
    type Context = ();

    fn error(&self, error: ProtocolError, write: bool) -> HostError {
        HostError::from_output(
            None,
            &json!({"event": format!("{error:?}"), "write": write}),
        )
        .unwrap()
    }

    fn admit(&self, _: Option<&Parts>) -> Result<(), AdmissionError> {
        Ok(())
    }

    fn start_tool(
        &self,
        (): (),
        request: ToolRequest,
        permit: OwnedSemaphorePermit,
    ) -> Pending<ToolResult> {
        self.started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (release, held) = tokio::sync::oneshot::channel();
        self.releases.lock().unwrap().push(release);
        let finished = self.finished.clone();
        let (send, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            assert!(matches!(request.parse(), Ok(ToolCall::Health)));
            held.await.unwrap();
            let _ = send.send(Ok(bounded_result(&json!({"finished": true})).unwrap()));
            drop(permit);
            finished.notify_one();
        });
        Box::pin(async move { response.await.unwrap() })
    }

    fn start_resource(
        &self,
        (): (),
        _: ResourceRequest,
        _: OwnedSemaphorePermit,
    ) -> Pending<ResourceOutput> {
        panic!("this owner exercises tools")
    }
}

#[tokio::test]
async fn cancellation_and_deadline_keep_the_host_owned_permit_until_completion() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for cancelled in [true, false] {
            let host = HeldHost::default();
            let service = GraphTools {
                host: host.clone(),
                ..test_service(ReadBudget::DEFAULT)
            };
            let result = service
                .run_tool(
                    (),
                    ToolRequest::new("health", json!({}), false),
                    async move {
                        if !cancelled {
                            std::future::pending::<()>().await;
                        }
                    },
                    if cancelled {
                        READ_DEADLINE
                    } else {
                        Duration::ZERO
                    },
                )
                .await;
            assert!(matches!(
                (&result, cancelled),
                (Err(Refusal::Protocol(ProtocolError::Cancelled)), true)
                    | (Err(Refusal::Protocol(ProtocolError::Deadline)), false)
            ));
            assert_eq!(service.slots.available_permits(), 0);
            assert_eq!(host.started.load(std::sync::atomic::Ordering::SeqCst), 1);

            // Saturation refuses before a host can parse malformed arguments.
            let refused = service
                .run_tool(
                    (),
                    ToolRequest::new("health", json!({"unexpected": true}), false),
                    std::future::pending(),
                    READ_DEADLINE,
                )
                .await;
            assert!(matches!(
                refused,
                Err(Refusal::Protocol(ProtocolError::Capacity))
            ));
            assert_eq!(host.started.load(std::sync::atomic::Ordering::SeqCst), 1);
            host.releases
                .lock()
                .unwrap()
                .pop()
                .unwrap()
                .send(())
                .unwrap();
            host.finished.notified().await;
            assert_eq!(service.slots.available_permits(), 1);
        }
    })
    .await
    .expect("host-owned MCP call did not settle");
}

#[test]
fn host_error_projection_preserves_the_judged_body_and_optional_status() {
    let body = json!({
        "error": "uncertain execution", "code": "outcome_unknown",
        "disposition": {"effects": "unknown", "action": "reconcile"},
        "details": {"receipt": "host-owned"}
    });
    let host = HeldHost::default();
    for status in [
        None,
        Some(StatusCode::NOT_FOUND),
        Some(StatusCode::BAD_REQUEST),
        Some(StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        let projected = || HostError::from_output(status, &body).unwrap();
        for write in [false, true] {
            let output = Refusal::Api(projected()).tool_result(&host, write);
            let output = output.structured_content.unwrap();
            assert_eq!(output["error"], body);
            assert_eq!(
                output.get("status"),
                status.map(|s| json!(s.as_u16())).as_ref()
            );
            assert!(output.get("outcome_unknown").is_none());
        }
        let resource = Refusal::Api(projected()).resource_error(&host);
        assert_eq!(
            resource.data,
            Refusal::Api(projected())
                .tool_result(&host, false)
                .structured_content
        );
        let code = match status {
            Some(StatusCode::NOT_FOUND) => ErrorData::resource_not_found("", None).code,
            Some(status) if status.is_client_error() => ErrorData::invalid_request("", None).code,
            _ => ErrorData::internal_error("", None).code,
        };
        assert_eq!(resource.code, code);
    }
}
