use super::*;
use crate::settings::{validate_allowed_host, validate_result_bytes, validate_setup_url};
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
        ProtocolError::ResultTooLarge {
            limit: RESULT_BYTES,
        },
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
    let service = test_service(RESULT_BYTES);
    // The first source fits but its structured + legacy text envelope does
    // not. Both limits must return an error, not disguise a completed write
    // as an ordinary successful handler return.
    for value in [
        json!({"text":"x".repeat(RESULT_BYTES * 3 / 4)}),
        json!({"text":"x".repeat(RESULT_BYTES + 1)}),
    ] {
        assert_eq!(
            bounded_result(&value).unwrap_err(),
            ProtocolError::ResultTooLarge {
                limit: RESULT_BYTES
            }
        );
        assert!(matches!(
            HostError::from_output(None, &value),
            Err(ProtocolError::ResultTooLarge {
                limit: RESULT_BYTES
            })
        ));
        for write in [false, true] {
            let result = service.fit_result(CallToolResult::structured(value.clone()), write);
            assert_eq!(
                result.structured_content,
                Some(json!({
                    "error": {
                        "event": format!("ResultTooLarge {{ limit: {RESULT_BYTES} }}"),
                        "write": write
                    }
                }))
            );
            assert!(serde_json::to_vec(&result).unwrap().len() <= RESULT_BYTES);
        }
    }
}

fn test_service(result_bytes: usize) -> GraphTools<HeldHost> {
    let settings = McpSettings {
        result_bytes,
        ..McpSettings::default()
    };
    GraphTools {
        host: HeldHost::default(),
        version: "test".into(),
        slots: Arc::new(Semaphore::new(1)),
        result_bytes,
        instructions: Arc::from(instructions(&settings)),
    }
}

#[test]
fn a_configured_result_bound_lowers_the_limit_and_names_it() {
    let limit = 128 * 1024;
    let value = json!({"text": "x".repeat(limit)});
    assert!(bounded_result(&value).is_ok());
    assert_eq!(
        bounded_result_within(&value, limit).unwrap_err(),
        ProtocolError::ResultTooLarge { limit }
    );
    // A bound above the built-in one never raises it.
    let large = json!({"text": "x".repeat(RESULT_BYTES)});
    assert_eq!(
        bounded_result_within(&large, RESULT_BYTES * 2).unwrap_err(),
        ProtocolError::ResultTooLarge {
            limit: RESULT_BYTES
        }
    );
    let service = test_service(limit);
    let result = service.fit_result(CallToolResult::structured(value), false);
    assert_eq!(
        result.structured_content,
        Some(json!({
            "error": {"event": format!("ResultTooLarge {{ limit: {limit} }}"), "write": false}
        }))
    );
    // A refusal keeps the built-in bound, so it is never itself refused.
    assert!(serde_json::to_vec(&result).unwrap().len() <= RESULT_BYTES);
}

#[test]
fn result_bound_and_setup_url_settings_are_validated() {
    for bytes in [1, 128 * 1024, RESULT_BYTES] {
        assert!(validate_result_bytes(bytes).is_ok(), "{bytes}");
    }
    for bytes in [0, RESULT_BYTES + 1] {
        assert!(validate_result_bytes(bytes).is_err(), "{bytes}");
    }
    for url in [
        "https://platform.example.com/setup",
        "https://example.com/organizations/o/clusters/c/graphs/new",
    ] {
        assert!(validate_setup_url(url).is_ok(), "{url}");
    }
    for url in [
        "http://example.com/setup",
        "https://user:secret@example.com/setup",
        "https://example.com/setup?token=x",
        "https://example.com/setup#frag",
        "example.com/setup",
        "",
    ] {
        assert!(validate_setup_url(url).is_err(), "{url}");
    }
    assert_eq!(format_bytes(RESULT_BYTES), "1 MiB");
    assert_eq!(format_bytes(128 * 1024), "128 KiB");
    assert_eq!(format_bytes(1500), "1500 bytes");
    assert_eq!(format_bytes(1), "1 byte");
}

#[test]
fn instructions_name_the_configured_bound_and_the_setup_step_only_when_configured() {
    let defaults = instructions(&McpSettings::default());
    assert!(defaults.contains("A result is limited to 1 MiB"));
    assert!(!defaults.contains("`setup`"));
    let settings = McpSettings {
        result_bytes: 128 * 1024,
        setup_url: Some("https://example.com/setup".into()),
        ..McpSettings::default()
    };
    let configured = instructions(&settings);
    assert!(configured.contains("A result is limited to 128 KiB"));
    assert!(configured.contains("returns a `setup` link"));
    // The URL itself is never in the session instructions.
    assert!(!configured.contains("https://example.com/setup"));
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
                ..test_service(RESULT_BYTES)
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
