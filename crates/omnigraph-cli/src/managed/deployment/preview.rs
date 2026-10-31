//! Observe one accepted preview without submitting a replacement.
use super::*;

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Preview {
    preview_id: String,
    revision: String,
    first_attempt_deadline: Option<String>,
}

impl Preview {
    fn read(body: &Value, context: &Context, revision: &str) -> Result<Self> {
        preview_exit(body, context, revision)?;
        if !body["data"]["failure_code"].is_null()
            && body["data"]["failure_code"].as_str().is_none_or(|code| {
                code.is_empty()
                    || code.len() > 128
                    || !code.bytes().all(|byte| byte.is_ascii_graphic())
            })
        {
            return Err(Failure::protocol());
        }
        let deadline = match &body["data"]["first_attempt_deadline"] {
            Value::Null if body["data"]["state"] != "capturing" => None,
            Value::String(value) => {
                provider_timestamp(value)?;
                Some(value.clone())
            }
            _ => return Err(Failure::protocol()),
        };
        Ok(Self {
            preview_id: body["data"]["preview_id"]
                .as_str()
                .ok_or_else(Failure::protocol)?
                .to_owned(),
            revision: revision.to_owned(),
            first_attempt_deadline: deadline,
        })
    }

    fn verify(&self, body: &Value, context: &Context) -> Result<()> {
        if Self::read(body, context, &self.revision)? != *self {
            return Err(Failure::refused(
                "context_mismatch",
                "preview observation changed the original ID, revision or deadline",
            ));
        }
        Ok(())
    }

    fn retain(&self, mut error: Failure, key: &str) -> Failure {
        error.body["accepted_preview"] = serde_json::to_value(self).expect("preview identity");
        error.body["idempotency_key"] = json!(key);
        error
    }

    fn diagnostic(
        &self,
        stderr: &mut dyn Write,
        key: &str,
        message: std::fmt::Arguments<'_>,
    ) -> Result<()> {
        write_diagnostic(stderr, message).map_err(|error| self.retain(error, key))
    }

    fn timed_out(
        &self,
        mut body: Value,
        key: &str,
        stderr: &mut dyn Write,
    ) -> Result<(Value, i32)> {
        self.diagnostic(stderr, key, format_args!(
            "local wait deadline reached; preview {} remains accepted; repeat `cluster plan --rev {} --idempotency-key KEY --target NAME` with the original key retained in the response; no work was cancelled or resubmitted\n",
            self.preview_id, self.revision
        ))?;
        body["idempotency_key"] = json!(key);
        Ok((body, 5))
    }
}

pub(super) async fn finish(
    api: &Api,
    context: &Context,
    submission: (Value, String),
    revision: &str,
    run: &ClusterRunArgs,
    deadline: Instant,
    stderr: &mut dyn Write,
) -> Result<(Value, i32)> {
    let (mut body, key) = submission;
    let key = key.as_str();
    let preview = Preview::read(&body, context, revision)
        .map_err(|error| retain_submission(error, key, &json!({"revision":revision})))?;
    if run.no_wait {
        let exit = preview_exit(&body, context, revision)?;
        if exit == 5 || (exit == 0 && !body["data"]["native_plan"].is_object()) {
            body["idempotency_key"] = json!(key);
        }
        return Ok((body, if exit == 5 { 0 } else { exit }));
    }
    let path = format!(
        "/v1/clusters/{}/plans/{}",
        context.cluster, preview.preview_id
    );
    let mut observed = false;
    let mut attempted_read = false;
    loop {
        preview
            .verify(&body, context)
            .map_err(|error| preview.retain(error, key))?;
        let exit =
            preview_exit(&body, context, revision).map_err(|error| preview.retain(error, key))?;
        if exit == 1 {
            let state = body["data"]["state"]
                .as_str()
                .ok_or_else(Failure::protocol)?;
            let code = body["data"]["failure_code"]
                .as_str()
                .unwrap_or("preview_failed");
            preview.diagnostic(
                stderr,
                key,
                format_args!(
                    "preview {}: {} ({}); a fresh plan requires a new idempotency key\n",
                    preview.preview_id, state, code
                ),
            )?;
            return Ok((body, exit));
        }
        if exit == 0 && body["data"]["native_plan"].is_object() {
            return Ok((body, 0));
        }
        if exit == 0 && observed {
            return Err(preview.retain(Failure::protocol(), key));
        }
        // A ready idempotent POST contains metadata only. Fetch its authorized
        // plan immediately; pending previews use the normal bounded poll rate.
        let next = if exit == 0 && !attempted_read {
            Instant::now()
        } else {
            Instant::now() + POLL_INTERVAL
        };
        if next >= deadline {
            tokio::time::sleep_until(deadline).await;
            return preview.timed_out(body, key, stderr);
        }
        preview.diagnostic(
            stderr,
            key,
            format_args!(
                "preview {} at revision {}: {}\n",
                preview.preview_id, preview.revision, body["data"]["state"]
            ),
        )?;
        tokio::time::sleep_until(next).await;
        attempted_read = true;
        match tokio::time::timeout_at(deadline, api.raw(Method::GET, &path, None, None)).await {
            Ok(Ok(response)) if response.status.is_success() => {
                body = response.body;
                observed = true;
            }
            Ok(Ok(response))
                if response.status.is_server_error()
                    || response.status == StatusCode::TOO_MANY_REQUESTS =>
            {
                preview.diagnostic(stderr, key, format_args!(
                    "preview observation unavailable; retain the original ID and continue bounded read-only lookup\n"
                ))?;
            }
            Ok(Ok(response)) => {
                return Err(preview.retain(
                    Failure {
                        body: response.body,
                        response_status: Some(response.status),
                        exit: if response.status.is_client_error() {
                            2
                        } else {
                            1
                        },
                    },
                    key,
                ));
            }
            Ok(Err(error)) if error.body["type"] == "transport_failed" => {
                preview.diagnostic(
                    stderr,
                    key,
                    format_args!(
                        "preview observation transport unavailable; retain the original ID\n"
                    ),
                )?;
            }
            Ok(Err(error)) => return Err(preview.retain(error, key)),
            Err(_) => return preview.timed_out(body, key, stderr),
        }
    }
}
