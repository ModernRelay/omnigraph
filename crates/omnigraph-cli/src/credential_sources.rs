use std::ffi::OsString;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use time::OffsetDateTime;

mod binding;
mod exec;
mod oauth;
mod store;

pub(crate) use binding::{CredentialRequest, OwnerBinding, OwnerKind, Purpose, ResourceIdentity};
pub(crate) use exec::{ExecSource, acquire_exec};
pub(crate) use oauth::{acquire_client_credentials, validate_client_credentials};
pub(crate) use store::{OsSecretStore, SecretStore, acquire_stored};

#[cfg(test)]
mod tests;

pub(crate) const MAX_CREDENTIAL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CredentialError {
    InvalidToken,
    TooLarge,
    InvalidEnvironmentName,
    MissingEnvironment,
    InvalidEnvironmentEncoding,
    FileUnavailable,
    NotRegularFile,
    InvalidBinding,
    BindingMismatch,
    #[cfg(any(
        test,
        not(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "windows",
            target_os = "linux",
            target_os = "freebsd",
            target_os = "openbsd"
        ))
    ))]
    StoreUnavailable,
    StoreFailure(crate::secure_store_error::StoreFailure),
    MissingStoredCredential,
    InvalidCommand,
    InvalidTimeout,
    #[cfg(not(unix))]
    HelperUnsupported,
    HelperSpawn,
    HelperIo,
    HelperFailed,
    HelperTimeout,
    HelperOutputLimit,
    HelperResponse,
    InvalidClientCredentials,
    ClientCredentialsUnavailable,
    ClientCredentialsRefused,
    ClientCredentialsResponse,
    Expired,
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidToken => "credential is not a nonempty HTTP bearer token",
            Self::TooLarge => "credential input exceeds 64 KiB",
            Self::InvalidEnvironmentName => "credential environment variable name is invalid",
            Self::MissingEnvironment => "selected credential environment variable is missing",
            Self::InvalidEnvironmentEncoding => {
                "selected credential environment variable is not UTF-8"
            }
            Self::FileUnavailable => "selected credential file could not be read",
            Self::NotRegularFile => "selected credential file is not a regular file",
            Self::InvalidBinding => {
                "credential request requires an explicit valid owner and profile binding"
            }
            Self::BindingMismatch => "credential does not belong to this request binding",
            #[cfg(any(
                test,
                not(any(
                    target_os = "macos",
                    target_os = "ios",
                    target_os = "windows",
                    target_os = "linux",
                    target_os = "freebsd",
                    target_os = "openbsd"
                ))
            ))]
            Self::StoreUnavailable => {
                "secure credential store is unavailable; no plaintext fallback is used"
            }
            Self::StoreFailure(failure) => return fmt::Display::fmt(failure, f),
            Self::MissingStoredCredential => "selected owner/profile has no stored credential",
            Self::InvalidCommand => {
                "credential helper requires an absolute executable path and valid argv"
            }
            Self::InvalidTimeout => {
                "credential helper timeout must be an integer from 1s through 120s"
            }
            #[cfg(not(unix))]
            Self::HelperUnsupported => {
                "credential helpers require supported process-group termination"
            }
            Self::HelperSpawn => "credential helper could not be started",
            Self::HelperIo => "credential helper communication failed",
            Self::HelperFailed => "credential helper exited unsuccessfully",
            Self::HelperTimeout => "credential helper exceeded its timeout",
            Self::HelperOutputLimit => "credential helper output exceeded 64 KiB",
            Self::HelperResponse => "credential helper returned an invalid version-1 response",
            Self::InvalidClientCredentials => "client-credentials configuration is invalid",
            Self::ClientCredentialsUnavailable => {
                "client-credentials token request failed; no retry was attempted"
            }
            Self::ClientCredentialsRefused => "the token endpoint refused the client credentials",
            Self::ClientCredentialsResponse => {
                "the token endpoint returned an invalid credential response"
            }
            Self::Expired => "credential has expired",
        };
        f.write_str(message)
    }
}

impl std::error::Error for CredentialError {}

pub(crate) type Result<T> = std::result::Result<T, CredentialError>;

#[derive(Clone)]
pub(crate) struct BearerSecret(String);

impl BearerSecret {
    pub(crate) fn new(value: String) -> Result<Self> {
        if value.len() > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::TooLarge);
        }
        let body = value.trim_end_matches('=');
        if body.is_empty()
            || !body.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
            })
        {
            return Err(CredentialError::InvalidToken);
        }
        Ok(Self(value))
    }

    pub(crate) fn from_input(mut value: Vec<u8>) -> Result<Self> {
        if value.len() > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::TooLarge);
        }
        if value.last() == Some(&b'\n') {
            value.pop();
            if value.last() == Some(&b'\r') {
                value.pop();
            }
        }
        Self::new(String::from_utf8(value).map_err(|_| CredentialError::InvalidToken)?)
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BearerSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerSecret([REDACTED])")
    }
}

impl fmt::Display for BearerSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AcquiredCredential {
    request: CredentialRequest,
    secret: BearerSecret,
    expires_at: Option<OffsetDateTime>,
}

impl AcquiredCredential {
    pub(crate) fn bearer_for(
        &self,
        request: &CredentialRequest,
        now: OffsetDateTime,
    ) -> Result<&BearerSecret> {
        if &self.request != request {
            return Err(CredentialError::BindingMismatch);
        }
        if self.expires_at.is_some_and(|expiry| expiry <= now) {
            return Err(CredentialError::Expired);
        }
        Ok(&self.secret)
    }

    #[cfg(test)]
    pub(crate) fn expires_at(&self) -> Option<OffsetDateTime> {
        self.expires_at
    }

    pub(crate) fn bound_expiry(mut self, verified: OffsetDateTime) -> Self {
        self.expires_at = Some(
            self.expires_at
                .map_or(verified, |expiry| expiry.min(verified)),
        );
        self
    }

    pub(crate) fn same_credential(&self, other: &Self) -> bool {
        self.request == other.request && self.secret.expose() == other.secret.expose()
    }

    pub(crate) fn request(&self) -> &CredentialRequest {
        &self.request
    }

    fn new(request: &CredentialRequest, secret: BearerSecret) -> Self {
        Self {
            request: request.clone(),
            secret,
            expires_at: None,
        }
    }

    /// Retain the independently verified lifetime and binding of an issued
    /// managed data credential through its final HTTP dispatch.
    pub(crate) fn issued(
        request: CredentialRequest,
        token: String,
        expires_at: OffsetDateTime,
    ) -> Result<Self> {
        Ok(Self {
            request,
            secret: BearerSecret::new(token)?,
            expires_at: Some(expires_at),
        })
    }
}

#[cfg(test)]
pub(crate) fn test_credential(endpoint: &str, token: &str) -> AcquiredCredential {
    let owner = OwnerBinding::new(
        OwnerKind::Server,
        "test-owner".into(),
        "test-auth".into(),
        endpoint.into(),
        None,
    )
    .unwrap();
    let request =
        CredentialRequest::new(owner, Purpose::Data, endpoint.into(), None, None).unwrap();
    AcquiredCredential::new(&request, BearerSecret::new(token.into()).unwrap())
}

pub(crate) fn acquire_environment(
    request: &CredentialRequest,
    name: &str,
) -> Result<AcquiredCredential> {
    acquire_environment_with(request, name, |name| std::env::var_os(name))
}

pub(crate) fn acquire_environment_with(
    request: &CredentialRequest,
    name: &str,
    read: impl FnOnce(&str) -> Option<OsString>,
) -> Result<AcquiredCredential> {
    Ok(AcquiredCredential::new(
        request,
        BearerSecret::new(read_environment(name, read)?)?,
    ))
}

fn read_environment(name: &str, read: impl FnOnce(&str) -> Option<OsString>) -> Result<String> {
    let mut bytes = name.bytes();
    if !bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        || !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(CredentialError::InvalidEnvironmentName);
    }
    let value = read(name).ok_or(CredentialError::MissingEnvironment)?;
    let value = value
        .into_string()
        .map_err(|_| CredentialError::InvalidEnvironmentEncoding)?;
    if value.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::TooLarge);
    }
    Ok(value)
}

pub(crate) fn acquire_file(request: &CredentialRequest, path: &Path) -> Result<AcquiredCredential> {
    Ok(AcquiredCredential::new(
        request,
        BearerSecret::from_input(read_file(path)?)?,
    ))
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    if !path.is_absolute() {
        return Err(CredentialError::FileUnavailable);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    let file = options
        .open(path)
        .map_err(|_| CredentialError::FileUnavailable)?;
    let metadata = file
        .metadata()
        .map_err(|_| CredentialError::FileUnavailable)?;
    if !metadata.is_file() {
        return Err(CredentialError::NotRegularFile);
    }
    if metadata.len() > MAX_CREDENTIAL_BYTES as u64 {
        return Err(CredentialError::TooLarge);
    }
    let mut bytes = Vec::new();
    file.take(MAX_CREDENTIAL_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CredentialError::FileUnavailable)?;
    if bytes.len() > MAX_CREDENTIAL_BYTES {
        return Err(CredentialError::TooLarge);
    }
    Ok(bytes)
}
