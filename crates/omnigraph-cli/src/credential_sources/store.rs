use sha2::{Digest, Sha256};

#[cfg(target_os = "macos")]
use crate::macos_secure_store::Entry as StoreEntry;
#[cfg(any(
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
use keyring::Entry as StoreEntry;

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
use crate::secure_store_error::{Operation, StoreFailure};

use super::{
    AcquiredCredential, BearerSecret, CredentialError, CredentialRequest, OwnerBinding, Result,
};

pub(crate) trait SecretStore {
    fn get(&self, owner: &OwnerBinding) -> Result<Option<BearerSecret>>;
    fn put(&self, owner: &OwnerBinding, secret: &BearerSecret) -> Result<()>;
    fn remove(&self, owner: &OwnerBinding) -> Result<()>;
}

pub(crate) struct OsSecretStore;

fn account(owner: &OwnerBinding) -> Result<String> {
    let encoded = serde_json::to_vec(owner).map_err(|_| CredentialError::InvalidBinding)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub(crate) fn acquire_stored(
    request: &CredentialRequest,
    store: &dyn SecretStore,
) -> Result<AcquiredCredential> {
    if request.endpoint() != request.owner().route() || request.resource().is_some() {
        return Err(CredentialError::BindingMismatch);
    }
    let secret = store
        .get(request.owner())?
        .ok_or(CredentialError::MissingStoredCredential)?;
    Ok(AcquiredCredential::new(request, secret))
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
impl OsSecretStore {
    fn entry(owner: &OwnerBinding) -> Result<StoreEntry> {
        StoreEntry::new("omnigraph.operator.static-token.v1", &account(owner)?).map_err(|error| {
            CredentialError::StoreFailure(StoreFailure::from_keyring(Operation::Open, &error))
        })
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
))]
impl SecretStore for OsSecretStore {
    fn get(&self, owner: &OwnerBinding) -> Result<Option<BearerSecret>> {
        match Self::entry(owner)?.get_password() {
            Ok(value) => BearerSecret::new(value).map(Some),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(CredentialError::StoreFailure(StoreFailure::from_keyring(
                Operation::Read,
                &error,
            ))),
        }
    }

    fn put(&self, owner: &OwnerBinding, secret: &BearerSecret) -> Result<()> {
        Self::entry(owner)?
            .set_password(secret.expose())
            .map_err(|error| {
                CredentialError::StoreFailure(StoreFailure::from_keyring(Operation::Write, &error))
            })
    }

    fn remove(&self, owner: &OwnerBinding) -> Result<()> {
        match Self::entry(owner)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(CredentialError::StoreFailure(StoreFailure::from_keyring(
                Operation::Delete,
                &error,
            ))),
        }
    }
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    target_os = "linux",
    target_os = "freebsd",
    target_os = "openbsd"
)))]
impl SecretStore for OsSecretStore {
    fn get(&self, _: &OwnerBinding) -> Result<Option<BearerSecret>> {
        Err(CredentialError::StoreUnavailable)
    }
    fn put(&self, _: &OwnerBinding, _: &BearerSecret) -> Result<()> {
        Err(CredentialError::StoreUnavailable)
    }
    fn remove(&self, _: &OwnerBinding) -> Result<()> {
        Err(CredentialError::StoreUnavailable)
    }
}

#[cfg(test)]
pub(super) fn test_account(owner: &OwnerBinding) -> String {
    account(owner).unwrap()
}
