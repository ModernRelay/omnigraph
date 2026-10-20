use super::*;
use crate::settings::validate_allowed_host;
use serde_json::json;

#[tokio::test]
async fn a_write_whose_wait_ends_first_reports_an_unknown_outcome() {
    // The server's answer must arrive before a client on a common default
    // timeout gives up and never sees `outcome_unknown`.
    assert!(WRITE_DEADLINE < COMMON_CLIENT_TIMEOUT);
    for refusal in [Refusal::Cancelled, Refusal::Deadline] {
        let result = refusal.tool_result(true).await;
        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.structured_content.unwrap()["outcome_unknown"], true);
    }
    let read = Refusal::Deadline.tool_result(false).await;
    assert!(
        read.structured_content
            .unwrap()
            .get("outcome_unknown")
            .is_none()
    );
    // A handler failure the server cannot settle keeps that report, for a
    // write only; a refusal before effects is a known outcome.
    let uncertain = Refusal::Api(HostError::from_output(
        StatusCode::INTERNAL_SERVER_ERROR,
        &json!({"error":"lost","code":"internal_error"}),
        true,
    ))
    .tool_result(true)
    .await
    .structured_content
    .unwrap();
    assert_eq!(uncertain["status"], 500);
    assert_eq!(uncertain["outcome_unknown"], true);
    for (error, write) in [
        (
            HostError::from_output(
                StatusCode::BAD_REQUEST,
                &json!({"error":"refused","code":"bad_request"}),
                false,
            ),
            true,
        ),
        (
            HostError::from_output(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error":"lost","code":"internal_error"}),
                true,
            ),
            false,
        ),
    ] {
        let body = Refusal::Api(error)
            .tool_result(write)
            .await
            .structured_content
            .unwrap();
        assert!(body.get("outcome_unknown").is_none(), "{body}");
    }
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
    // The source JSON fits, but the SDK's structured + legacy text
    // compatibility representation exceeds the complete result bound.
    let duplicate = json!({"text":"x".repeat(RESULT_BYTES * 3 / 4)});
    let result = bounded_result(&duplicate).unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(
        result.structured_content.unwrap()["code"],
        "result_too_large"
    );
    let too_large = json!({"text":"x".repeat(RESULT_BYTES+1)});
    assert_eq!(bounded_result(&too_large).unwrap().is_error, Some(true));
}

#[derive(Clone, Default)]
struct HeldHost {
    releases: Arc<std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<()>>>>,
    started: Arc<std::sync::atomic::AtomicUsize>,
    finished: Arc<tokio::sync::Notify>,
}

impl Host for HeldHost {
    type Context = ();

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
                settings: McpSettings::default(),
                version: "test".into(),
                slots: Arc::new(Semaphore::new(1)),
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
                (Err(Refusal::Cancelled), true) | (Err(Refusal::Deadline), false)
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
            assert!(matches!(refused, Err(Refusal::Capacity)));
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

#[tokio::test]
async fn host_error_projection_is_bounded_and_keeps_completion_evidence_separate() {
    let error = HostError::from_output(
        StatusCode::BAD_REQUEST,
        &json!({"error":"uncertain execution","code":"bad_request"}),
        true,
    );
    let output = Refusal::Api(error)
        .tool_result(true)
        .await
        .structured_content
        .unwrap();
    assert_eq!(output["status"], 400);
    assert_eq!(output["error"]["code"], "bad_request");
    assert_eq!(output["outcome_unknown"], true);

    let error = HostError::from_output(
        StatusCode::INTERNAL_SERVER_ERROR,
        &json!({"error":"known refusal","code":"internal_error"}),
        false,
    );
    let output = Refusal::Api(error)
        .tool_result(true)
        .await
        .structured_content
        .unwrap();
    assert!(output.get("outcome_unknown").is_none());

    let large = json!({"error":"x".repeat(RESULT_BYTES + 1)});
    let error = HostError::from_output(StatusCode::NOT_FOUND, &large, false);
    let output = Refusal::Api(error).resource_error().await;
    assert_eq!(output.code, ErrorData::resource_not_found("", None).code);
    assert_eq!(output.data.unwrap()["code"], "request_failed");
}
