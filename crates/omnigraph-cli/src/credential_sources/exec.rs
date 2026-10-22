use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::{
    AcquiredCredential, BearerSecret, CredentialError, CredentialRequest, MAX_CREDENTIAL_BYTES,
    Purpose, Result,
};

#[derive(Clone, Debug)]
pub(crate) struct ExecSource {
    argv: Vec<String>,
    timeout: Duration,
}

impl ExecSource {
    pub(crate) fn new(argv: Vec<String>, timeout: &str) -> Result<Self> {
        if argv.is_empty()
            || !Path::new(&argv[0]).is_absolute()
            || argv.iter().any(|arg| arg.contains('\0'))
            || argv
                .iter()
                .try_fold(0usize, |size, arg| size.checked_add(arg.len() + 1))
                .is_none_or(|size| size > MAX_CREDENTIAL_BYTES)
        {
            return Err(CredentialError::InvalidCommand);
        }
        let seconds = timeout
            .strip_suffix('s')
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|seconds| (1..=120).contains(seconds))
            .ok_or(CredentialError::InvalidTimeout)?;
        Ok(Self {
            argv,
            timeout: Duration::from_secs(seconds),
        })
    }
}

#[derive(Serialize)]
struct HelperRequest<'a> {
    version: u8,
    purpose: Purpose,
    endpoint: &'a str,
    interactive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    audience: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_incarnation: Option<&'a str>,
}

fn encode_request(request: &CredentialRequest) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&HelperRequest {
        version: 1,
        purpose: request.purpose(),
        endpoint: request.endpoint(),
        interactive: false,
        audience: request.audience(),
        organization_id: request.owner().organization_id(),
        cluster_id: request.resource().map(|value| value.cluster_id.as_str()),
        cluster_incarnation: request
            .resource()
            .map(|value| value.cluster_incarnation.as_str()),
    })
    .map_err(|_| CredentialError::InvalidBinding)?;
    if bytes.len() >= MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::InvalidBinding);
    }
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HelperResponse {
    version: u8,
    credential: HelperCredential,
    #[serde(default, deserialize_with = "expiry_field")]
    expires_at: Option<String>,
}

fn expiry_field<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum HelperCredential {
    Bearer { token: String },
}

fn decode_response(
    request: &CredentialRequest,
    bytes: &[u8],
    now: OffsetDateTime,
) -> Result<AcquiredCredential> {
    if bytes.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::HelperOutputLimit);
    }
    let response: HelperResponse =
        serde_json::from_slice(bytes).map_err(|_| CredentialError::HelperResponse)?;
    if response.version != 1 {
        return Err(CredentialError::HelperResponse);
    }
    let expires_at = response
        .expires_at
        .as_deref()
        .map(|value| {
            OffsetDateTime::parse(value, &Rfc3339).map_err(|_| CredentialError::HelperResponse)
        })
        .transpose()?;
    if expires_at.is_some_and(|expiry| expiry <= now) {
        return Err(CredentialError::Expired);
    }
    let HelperCredential::Bearer { token } = response.credential;
    Ok(AcquiredCredential {
        request: request.clone(),
        secret: BearerSecret::new(token)?,
        expires_at,
    })
}

#[cfg(unix)]
struct ProcessGroup {
    id: Option<i32>,
}

#[cfg(unix)]
impl ProcessGroup {
    fn terminate(&mut self) {
        if let Some(id) = self.id.take() {
            unsafe {
                libc::kill(-id, libc::SIGKILL);
            }
        }
    }
}

#[cfg(unix)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(unix)]
async fn read_output(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    retain: bool,
) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut total = 0usize;
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|_| CredentialError::HelperIo)?;
        if count == 0 {
            return Ok(output);
        }
        total += count;
        if total > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::HelperOutputLimit);
        }
        if retain {
            output.extend_from_slice(&buffer[..count]);
        }
    }
}

#[cfg(unix)]
pub(crate) async fn acquire_exec(
    request: &CredentialRequest,
    source: &ExecSource,
) -> Result<AcquiredCredential> {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;

    let input = encode_request(request)?;
    let mut command = tokio::process::Command::new(&source.argv[0]);
    command
        .args(&source.argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0);
    let mut child = command.spawn().map_err(|_| CredentialError::HelperSpawn)?;
    let mut group = ProcessGroup {
        id: child.id().and_then(|id| i32::try_from(id).ok()),
    };
    let mut stdin = child.stdin.take().ok_or(CredentialError::HelperIo)?;
    let stdout = child.stdout.take().ok_or(CredentialError::HelperIo)?;
    let stderr = child.stderr.take().ok_or(CredentialError::HelperIo)?;
    let exchange = async {
        let write = async {
            stdin
                .write_all(&input)
                .await
                .map_err(|_| CredentialError::HelperIo)?;
            stdin
                .shutdown()
                .await
                .map_err(|_| CredentialError::HelperIo)?;
            drop(stdin);
            Ok(())
        };
        let (_, output, _) =
            tokio::try_join!(write, read_output(stdout, true), read_output(stderr, false))?;
        let status = child.wait().await.map_err(|_| CredentialError::HelperIo)?;
        group.id = None;
        if !status.success() {
            return Err(CredentialError::HelperFailed);
        }
        Ok(output)
    };
    let result = tokio::time::timeout(source.timeout, exchange)
        .await
        .unwrap_or(Err(CredentialError::HelperTimeout));
    match result {
        Ok(output) => decode_response(request, &output, OffsetDateTime::now_utc()),
        Err(error) => {
            group.terminate();
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
            Err(error)
        }
    }
}

#[cfg(not(unix))]
pub(crate) async fn acquire_exec(
    _: &CredentialRequest,
    _: &ExecSource,
) -> Result<AcquiredCredential> {
    Err(CredentialError::HelperUnsupported)
}

#[cfg(test)]
mod tests;
