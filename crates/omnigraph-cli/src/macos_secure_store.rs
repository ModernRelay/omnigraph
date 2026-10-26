use core_foundation::base::TCFType;
use security_framework::base::Error;
use security_framework::os::macos::keychain::{SecKeychain, SecPreferencesDomain};
use security_framework::os::macos::keychain_item::SecKeychainItem;
use security_framework::os::macos::passwords::SecKeychainItemPassword;

pub(crate) struct Entry {
    service: String,
    account: String,
}

impl Entry {
    pub(crate) fn new(service: &str, account: &str) -> keyring::Result<Self> {
        if service.is_empty() {
            return Err(keyring::Error::Invalid(
                "service".into(),
                "cannot be empty".into(),
            ));
        }
        if account.is_empty() {
            return Err(keyring::Error::Invalid(
                "user".into(),
                "cannot be empty".into(),
            ));
        }
        Ok(Self {
            service: service.into(),
            account: account.into(),
        })
    }

    pub(crate) fn get_password(&self) -> keyring::Result<String> {
        read(&Native::open()?, &self.service, &self.account)
    }

    pub(crate) fn set_password(&self, password: &str) -> keyring::Result<()> {
        write(
            &Native::open()?,
            &self.service,
            &self.account,
            password.as_bytes(),
        )
    }

    pub(crate) fn delete_credential(&self) -> keyring::Result<()> {
        remove(&Native::open()?, &self.service, &self.account)
    }
}

trait Backend {
    type Item;
    type Secret: AsRef<[u8]>;

    fn find(&self, service: &str, account: &str) -> keyring::Result<(Self::Secret, Self::Item)>;
    fn update(&self, item: Self::Item, password: &[u8]) -> keyring::Result<()>;
    fn add(&self, service: &str, account: &str, password: &[u8]) -> keyring::Result<()>;
    fn delete(&self, item: Self::Item) -> keyring::Result<()>;
}

fn read(backend: &impl Backend, service: &str, account: &str) -> keyring::Result<String> {
    let (secret, _) = backend.find(service, account)?;
    keyring::error::decode_password(secret.as_ref().to_vec())
}

fn write(
    backend: &impl Backend,
    service: &str,
    account: &str,
    password: &[u8],
) -> keyring::Result<()> {
    match backend.find(service, account) {
        Ok((_, item)) => backend.update(item, password),
        Err(keyring::Error::NoEntry) => backend.add(service, account, password),
        Err(error) => Err(error),
    }
}

fn remove(backend: &impl Backend, service: &str, account: &str) -> keyring::Result<()> {
    let (_, item) = backend.find(service, account)?;
    backend.delete(item)
}

struct Native(SecKeychain);

impl Native {
    fn open() -> keyring::Result<Self> {
        SecKeychain::default_for_domain(SecPreferencesDomain::User)
            .map(Self)
            .map_err(keyring::macos::decode_error)
    }
}

impl Backend for Native {
    type Item = SecKeychainItem;
    type Secret = SecKeychainItemPassword;

    fn find(&self, service: &str, account: &str) -> keyring::Result<(Self::Secret, Self::Item)> {
        self.0
            .find_generic_password(service, account)
            .map_err(keyring::macos::decode_error)
    }

    fn update(&self, mut item: Self::Item, password: &[u8]) -> keyring::Result<()> {
        item.set_password(password)
            .map_err(keyring::macos::decode_error)
    }

    fn add(&self, service: &str, account: &str, password: &[u8]) -> keyring::Result<()> {
        self.0
            .add_generic_password(service, account, password)
            .map_err(keyring::macos::decode_error)
    }

    fn delete(&self, item: Self::Item) -> keyring::Result<()> {
        let status = unsafe {
            security_framework_sys::keychain_item::SecKeychainItemDelete(item.as_concrete_TypeRef())
        };
        deletion_status(status)
    }
}

fn deletion_status(status: i32) -> keyring::Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(keyring::macos::decode_error(Error::from_code(status)))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::secure_store_error::{Operation, StoreFailure};

    struct Fake {
        calls: RefCell<Vec<&'static str>>,
        find_status: Option<i32>,
        mutation_status: Option<i32>,
    }

    impl Fake {
        fn new(find_status: Option<i32>, mutation_status: Option<i32>) -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
                find_status,
                mutation_status,
            }
        }

        fn mutate(&self, operation: &'static str) -> keyring::Result<()> {
            self.calls.borrow_mut().push(operation);
            match self.mutation_status {
                Some(code) => Err(keyring::macos::decode_error(Error::from_code(code))),
                None => Ok(()),
            }
        }
    }

    impl Backend for Fake {
        type Item = u8;
        type Secret = Vec<u8>;

        fn find(
            &self,
            service: &str,
            account: &str,
        ) -> keyring::Result<(Self::Secret, Self::Item)> {
            assert_eq!(service, "synthetic-service");
            assert_eq!(account, "synthetic-account");
            self.calls.borrow_mut().push("find");
            match self.find_status {
                Some(code) => Err(keyring::macos::decode_error(Error::from_code(code))),
                None => Ok((b"synthetic-secret".to_vec(), 42)),
            }
        }

        fn update(&self, item: Self::Item, password: &[u8]) -> keyring::Result<()> {
            assert_eq!(item, 42);
            assert_eq!(password, b"replacement");
            self.mutate("update")
        }

        fn add(&self, service: &str, account: &str, password: &[u8]) -> keyring::Result<()> {
            assert_eq!(service, "synthetic-service");
            assert_eq!(account, "synthetic-account");
            assert_eq!(password, b"replacement");
            self.mutate("add")
        }

        fn delete(&self, item: Self::Item) -> keyring::Result<()> {
            assert_eq!(item, 42);
            self.mutate("delete")
        }
    }

    fn assert_status(error: keyring::Error, code: i32) {
        let diagnostic = StoreFailure::from_keyring(Operation::Write, &error);
        assert_eq!(serde_json::to_value(diagnostic).unwrap()["os_status"], code);
        assert!(!diagnostic.to_string().contains("synthetic-secret"));
    }

    #[test]
    fn denied_lookup_never_adds_updates_or_retries() {
        for code in [-25293, -25308, -128, -25291, -67050] {
            let fake = Fake::new(Some(code), None);
            assert_status(
                write(
                    &fake,
                    "synthetic-service",
                    "synthetic-account",
                    b"replacement",
                )
                .unwrap_err(),
                code,
            );
            assert_eq!(*fake.calls.borrow(), ["find"]);
        }
    }

    #[test]
    fn missing_lookup_adds_once_and_existing_lookup_updates_once() {
        for (status, expected) in [(Some(-25300), "add"), (None, "update")] {
            let fake = Fake::new(status, None);
            write(
                &fake,
                "synthetic-service",
                "synthetic-account",
                b"replacement",
            )
            .unwrap();
            assert_eq!(*fake.calls.borrow(), ["find", expected]);
        }
    }

    #[test]
    fn failed_add_or_update_preserves_error_without_retry_or_alternate_write() {
        for (lookup, expected) in [(Some(-25300), "add"), (None, "update")] {
            for code in [-25299, -25293, -25308] {
                let fake = Fake::new(lookup, Some(code));
                assert_status(
                    write(
                        &fake,
                        "synthetic-service",
                        "synthetic-account",
                        b"replacement",
                    )
                    .unwrap_err(),
                    code,
                );
                assert_eq!(*fake.calls.borrow(), ["find", expected]);
            }
        }
    }

    #[test]
    fn failed_deletion_is_an_error_and_never_retried() {
        let fake = Fake::new(None, Some(-25293));
        assert_status(
            remove(&fake, "synthetic-service", "synthetic-account").unwrap_err(),
            -25293,
        );
        assert_eq!(*fake.calls.borrow(), ["find", "delete"]);
        let fake = Fake::new(None, None);
        remove(&fake, "synthetic-service", "synthetic-account").unwrap();
        assert_eq!(*fake.calls.borrow(), ["find", "delete"]);
    }

    #[test]
    fn native_deletion_status_is_not_discarded() {
        assert!(deletion_status(0).is_ok());
        for code in [-25293, -25308, -128, -25291, -67050, -99999] {
            assert_status(deletion_status(code).unwrap_err(), code);
        }
        assert!(matches!(
            deletion_status(-25300),
            Err(keyring::Error::NoEntry)
        ));
    }

    #[test]
    fn missing_or_denied_lookup_never_deletes() {
        let fake = Fake::new(Some(-25300), None);
        assert!(matches!(
            remove(&fake, "synthetic-service", "synthetic-account"),
            Err(keyring::Error::NoEntry)
        ));
        assert_eq!(*fake.calls.borrow(), ["find"]);
        let fake = Fake::new(Some(-25308), None);
        assert_status(
            remove(&fake, "synthetic-service", "synthetic-account").unwrap_err(),
            -25308,
        );
        assert_eq!(*fake.calls.borrow(), ["find"]);
    }

    #[test]
    fn read_returns_the_selected_value_once_and_entry_construction_has_no_io() {
        let fake = Fake::new(None, None);
        assert_eq!(
            read(&fake, "synthetic-service", "synthetic-account").unwrap(),
            "synthetic-secret"
        );
        assert_eq!(*fake.calls.borrow(), ["find"]);
        let entry = Entry::new("synthetic-service", "synthetic-account").unwrap();
        assert_eq!(entry.service, "synthetic-service");
        assert_eq!(entry.account, "synthetic-account");
        assert!(Entry::new("", "account").is_err());
        assert!(Entry::new("service", "").is_err());
    }
}
