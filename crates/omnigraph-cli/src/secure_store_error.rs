use std::fmt;

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Operation {
    Open,
    Read,
    Write,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StoreFailure {
    operation: Operation,
    category: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    os_status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<&'static str>,
    guidance: &'static str,
}

impl StoreFailure {
    #[cfg(any(target_os = "macos", test))]
    fn from_os_status(operation: Operation, code: i32) -> Self {
        let (category, symbol, guidance) = match code {
            -128 => (
                "cancelled",
                "errSecUserCanceled",
                "Keychain access was cancelled; retry only when ready to approve the selected operation.",
            ),
            -25293 => (
                "authorization_failed",
                "errSecAuthFailed",
                "Keychain authorization failed. Check the user keychain's lock state and this executable's signing identity. This status alone does not establish a wrong password, a locked keychain, or a missing credential.",
            ),
            -25308 => (
                "interaction_not_allowed",
                "errSecInteractionNotAllowed",
                "Keychain interaction is not allowed in this execution context. Retry from an interactive logged-in macOS session, or select an explicitly configured external credential source for unattended use.",
            ),
            -25315 => (
                "interaction_required",
                "errSecInteractionRequired",
                "Keychain interaction is required. Retry from an interactive logged-in macOS session, or select an explicitly configured external credential source for unattended use.",
            ),
            -67050 => (
                "code_requirement_failed",
                "errSecCSReqFailed",
                "The executable did not satisfy a code-signing requirement. Verify the installed binary's signature and use a consistently Developer ID-signed release; this diagnostic does not repair upgrade trust.",
            ),
            -34018 => (
                "missing_entitlement",
                "errSecMissingEntitlement",
                "A required entitlement is missing. Report the OSStatus and installed build to the release maintainer for signing/entitlement review.",
            ),
            -25299 => (
                "duplicate_item",
                "errSecDuplicateItem",
                "An item already exists. A concurrent creator may have won after a missing-item lookup; no retry was attempted. Inspect the selected binding without deleting the credential automatically.",
            ),
            -25291 => (
                "storage_unavailable",
                "errSecNotAvailable",
                "The user keychain is unavailable. Check its availability in the current login session before retrying.",
            ),
            -25292 => (
                "read_only",
                "errSecReadOnly",
                "The selected keychain is read-only. Check its availability and permissions before retrying the write.",
            ),
            -25294 => (
                "keychain_missing",
                "errSecNoSuchKeychain",
                "The default user keychain could not be found. Check the user keychain configuration; this does not establish that a credential is absent.",
            ),
            -25295 => (
                "keychain_invalid",
                "errSecInvalidKeychain",
                "The selected keychain is invalid. Inspect its configuration in Keychain Access; do not reset or delete it as an automatic recovery step.",
            ),
            -25300 => (
                "entry_missing",
                "errSecItemNotFound",
                "The selected credential entry was not found.",
            ),
            -26275 => (
                "decode_failed",
                "errSecDecode",
                "Keychain data could not be decoded. Report the OSStatus without exporting the stored value.",
            ),
            _ => {
                return Self {
                    operation,
                    category: "platform_failure",
                    os_status: Some(code),
                    symbol: None,
                    guidance: "Report the numeric OSStatus and operation to the maintainer; the cause is not classified.",
                };
            }
        };
        Self {
            operation,
            category,
            os_status: Some(code),
            symbol: Some(symbol),
            guidance,
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
    pub(crate) fn from_keyring(operation: Operation, error: &keyring::Error) -> Self {
        #[cfg(target_os = "macos")]
        if let keyring::Error::PlatformFailure(source) | keyring::Error::NoStorageAccess(source) =
            error
            && let Some(source) = source.downcast_ref::<security_framework::base::Error>()
        {
            return Self::from_os_status(operation, source.code());
        }
        let (category, guidance) = match error {
            keyring::Error::NoEntry => (
                "entry_missing",
                "The selected credential entry was not found.",
            ),
            keyring::Error::NoStorageAccess(_) => (
                "storage_unavailable",
                "The secure store could not be accessed. Check its availability and access permissions in the current login session.",
            ),
            keyring::Error::BadEncoding(_) => (
                "invalid_encoding",
                "The stored credential is not valid UTF-8. Restore the selected credential through its supported installation or login flow; do not print the stored bytes.",
            ),
            keyring::Error::TooLong(_, _) | keyring::Error::Invalid(_, _) => (
                "invalid_attributes",
                "Secure-store attributes were rejected. Report the operation to the maintainer without including credential values.",
            ),
            keyring::Error::Ambiguous(_) => (
                "ambiguous_entry",
                "More than one credential matched. Inspect the selected binding without exporting secrets or deleting entries automatically.",
            ),
            _ => (
                "platform_failure",
                "The secure-store backend failed. Report the operation and installed build; backend error text is withheld because it may contain secrets.",
            ),
        };
        Self {
            operation,
            category,
            os_status: None,
            symbol: None,
            guidance,
        }
    }
}

impl fmt::Display for StoreFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let operation = match self.operation {
            Operation::Open => "open",
            Operation::Read => "read",
            Operation::Write => "write",
            Operation::Delete => "delete",
        };
        write!(
            f,
            "secure-store {operation} failed ({category}",
            category = self.category
        )?;
        if let Some(code) = self.os_status {
            write!(f, "; OSStatus {code}")?;
        }
        if let Some(symbol) = self.symbol {
            write!(f, "; {symbol}")?;
        }
        write!(
            f,
            "). {} No plaintext credential fallback is used.",
            self.guidance
        )
    }
}

impl std::error::Error for StoreFailure {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_status_classification_preserves_code_without_guessing_root_cause() {
        for (code, category) in [
            (-128, "cancelled"),
            (-25293, "authorization_failed"),
            (-25308, "interaction_not_allowed"),
            (-25315, "interaction_required"),
            (-67050, "code_requirement_failed"),
            (-34018, "missing_entitlement"),
            (-25299, "duplicate_item"),
            (-25291, "storage_unavailable"),
            (-25292, "read_only"),
            (-25294, "keychain_missing"),
            (-25295, "keychain_invalid"),
            (-25300, "entry_missing"),
            (-26275, "decode_failed"),
            (-99999, "platform_failure"),
        ] {
            let failure = StoreFailure::from_os_status(Operation::Read, code);
            assert_eq!(failure.category, category);
            assert_eq!(failure.os_status, Some(code));
            assert!(failure.to_string().contains(&format!("OSStatus {code}")));
            assert!(
                failure
                    .to_string()
                    .contains("No plaintext credential fallback")
            );
            assert!(std::error::Error::source(&failure).is_none());
        }
        assert!(
            StoreFailure::from_os_status(Operation::Read, -25293)
                .to_string()
                .contains("does not establish")
        );
        assert!(
            StoreFailure::from_os_status(Operation::Write, -25299)
                .to_string()
                .contains("no retry was attempted")
        );
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "windows",
        target_os = "linux",
        target_os = "freebsd",
        target_os = "openbsd"
    ))]
    #[test]
    fn raw_errors_are_never_formatted_or_retained() {
        struct Unprintable;
        impl fmt::Debug for Unprintable {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("raw debug must not run")
            }
        }
        impl fmt::Display for Unprintable {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("raw display must not run")
            }
        }
        impl std::error::Error for Unprintable {}
        for error in [
            keyring::Error::PlatformFailure(Box::new(Unprintable)),
            keyring::Error::NoStorageAccess(Box::new(Unprintable)),
            keyring::Error::BadEncoding(b"synthetic-secret".to_vec()),
            keyring::Error::Invalid("synthetic-secret".into(), "synthetic-secret".into()),
            keyring::Error::TooLong("synthetic-secret".into(), 123),
            keyring::Error::Ambiguous(Vec::new()),
            keyring::Error::NoEntry,
        ] {
            for operation in [
                Operation::Open,
                Operation::Read,
                Operation::Write,
                Operation::Delete,
            ] {
                let failure = StoreFailure::from_keyring(operation, &error);
                let output = format!(
                    "{failure} {failure:?} {}",
                    serde_json::to_string(&failure).unwrap()
                );
                assert!(!output.contains("synthetic-secret"));
                assert_eq!(failure.os_status, None);
                assert!(std::error::Error::source(&failure).is_none());
                let credential_error =
                    crate::credential_sources::CredentialError::StoreFailure(failure);
                assert_eq!(credential_error.to_string(), failure.to_string());
                assert!(!format!("{credential_error:?}").contains("synthetic-secret"));
            }
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn native_error_downcast_extracts_only_numeric_os_status() {
        for code in [-25293, -25308, -67050, -25299, -25291, -99999] {
            for error in [
                keyring::Error::PlatformFailure(Box::new(
                    security_framework::base::Error::from_code(code),
                )),
                keyring::Error::NoStorageAccess(Box::new(
                    security_framework::base::Error::from_code(code),
                )),
            ] {
                assert_eq!(
                    StoreFailure::from_keyring(Operation::Write, &error),
                    StoreFailure::from_os_status(Operation::Write, code)
                );
            }
        }
    }
}
