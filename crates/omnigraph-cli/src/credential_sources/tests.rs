use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use super::*;

fn owner(profile: &str) -> OwnerBinding {
    OwnerBinding::new(
        OwnerKind::Server,
        "owner-binding-1".into(),
        profile.into(),
        "https://example.invalid/service/".into(),
        None,
    )
    .unwrap()
}

fn request() -> CredentialRequest {
    let owner = owner("profile-binding-1");
    CredentialRequest::new(
        owner.clone(),
        Purpose::Data,
        owner.route().into(),
        None,
        None,
    )
    .unwrap()
}

#[test]
fn bearer_grammar_and_redaction() {
    for valid in ["opaque", "a.b_c-d~e+f/g", "a=", "a==="] {
        let secret = BearerSecret::new(valid.into()).unwrap();
        assert_eq!(secret.expose(), valid);
        assert_eq!(format!("{secret}"), "[REDACTED]");
        assert_eq!(format!("{secret:?}"), "BearerSecret([REDACTED])");
    }
    for invalid in [
        "",
        "=",
        "==",
        "a=b",
        "a b",
        " a",
        "a ",
        "a\t",
        "a\r\n",
        "a\0",
        "é",
        "a:b",
        "a,b",
        "a;b",
        "a\"",
        "{\"token\":\"a\"}",
    ] {
        assert_eq!(
            BearerSecret::new(invalid.into()).unwrap_err(),
            CredentialError::InvalidToken
        );
    }
    assert!(BearerSecret::new("a".repeat(MAX_CREDENTIAL_BYTES)).is_ok());
    assert_eq!(
        BearerSecret::new("a".repeat(MAX_CREDENTIAL_BYTES + 1)).unwrap_err(),
        CredentialError::TooLarge
    );
}

#[test]
fn input_strips_only_one_terminal_line_ending_and_bounds_raw_input() {
    for valid in [b"opaque".as_slice(), b"opaque\n", b"opaque\r\n"] {
        assert_eq!(
            BearerSecret::from_input(valid.into()).unwrap().expose(),
            "opaque"
        );
    }
    for invalid in [
        b"opaque\r".as_slice(),
        b"opaque\n\n",
        b" opaque\n",
        b"opaque \n",
        b"\n",
        b"\xff",
    ] {
        assert_eq!(
            BearerSecret::from_input(invalid.into()).unwrap_err(),
            CredentialError::InvalidToken
        );
    }
    let mut too_large = vec![b'a'; MAX_CREDENTIAL_BYTES];
    too_large.push(b'\n');
    assert_eq!(
        BearerSecret::from_input(too_large).unwrap_err(),
        CredentialError::TooLarge
    );
}

#[test]
fn environment_is_explicit_read_once_and_never_trimmed() {
    let request = request();
    let calls = Cell::new(0);
    let credential = acquire_environment_with(&request, "TEST_TOKEN", |name| {
        calls.set(calls.get() + 1);
        assert_eq!(name, "TEST_TOKEN");
        Some("fake-only-token".into())
    })
    .unwrap();
    for _ in 0..2 {
        assert_eq!(
            credential
                .bearer_for(&request, OffsetDateTime::now_utc())
                .unwrap()
                .expose(),
            "fake-only-token"
        );
    }
    assert_eq!(calls.get(), 1);
    for invalid in ["", "1A", "a-b", "a=b", "a\0b"] {
        assert_eq!(
            acquire_environment_with(&request, invalid, |_| panic!("must not read")).unwrap_err(),
            CredentialError::InvalidEnvironmentName
        );
    }
    assert_eq!(
        acquire_environment_with(&request, "A", |_| None).unwrap_err(),
        CredentialError::MissingEnvironment
    );
    for value in ["", "token\n", "token\r\n", " token"] {
        assert_eq!(
            acquire_environment_with(&request, "A", |_| Some(value.into())).unwrap_err(),
            CredentialError::InvalidToken
        );
    }
}

#[cfg(unix)]
#[test]
fn environment_rejects_non_utf8_without_printing_value() {
    use std::os::unix::ffi::OsStringExt;
    let error = acquire_environment_with(&request(), "A", |_| Some(OsString::from_vec(vec![0xff])))
        .unwrap_err();
    assert_eq!(error, CredentialError::InvalidEnvironmentEncoding);
}

#[test]
fn file_reads_snapshot_and_rejects_nonregular_missing_relative_and_oversize() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    let request = request();
    std::fs::write(&path, b"old-token\r\n").unwrap();
    let captured = acquire_file(&request, &path).unwrap();
    std::fs::write(&path, b"new-token\n").unwrap();
    assert_eq!(
        captured
            .bearer_for(&request, OffsetDateTime::now_utc())
            .unwrap()
            .expose(),
        "old-token"
    );
    assert_eq!(
        acquire_file(&request, &path).unwrap().secret.expose(),
        "new-token"
    );
    assert_eq!(
        acquire_file(&request, dir.path()).unwrap_err(),
        CredentialError::NotRegularFile
    );
    assert_eq!(
        acquire_file(&request, &dir.path().join("missing")).unwrap_err(),
        CredentialError::FileUnavailable
    );
    assert_eq!(
        acquire_file(&request, Path::new("token")).unwrap_err(),
        CredentialError::FileUnavailable
    );
    std::fs::write(&path, vec![b'a'; MAX_CREDENTIAL_BYTES + 1]).unwrap();
    assert_eq!(
        acquire_file(&request, &path).unwrap_err(),
        CredentialError::TooLarge
    );
    std::fs::write(&path, b"token\n\n").unwrap();
    assert_eq!(
        acquire_file(&request, &path).unwrap_err(),
        CredentialError::InvalidToken
    );
}

#[cfg(unix)]
#[test]
fn projected_file_atomic_symlink_rotation_is_observed_only_on_next_acquisition() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let current = dir.path().join("current");
    let replacement = dir.path().join("replacement");
    std::fs::write(dir.path().join("v1"), "first").unwrap();
    std::fs::write(dir.path().join("v2"), "second").unwrap();
    symlink("v1", &current).unwrap();
    let request = request();
    let first = acquire_file(&request, &current).unwrap();
    symlink("v2", &replacement).unwrap();
    std::fs::rename(&replacement, &current).unwrap();
    let second = acquire_file(&request, &current).unwrap();
    assert_eq!(first.secret.expose(), "first");
    assert_eq!(second.secret.expose(), "second");
    assert_eq!(first.request(), second.request());
}

#[cfg(unix)]
#[test]
fn fifo_is_rejected_without_waiting_for_a_writer() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fifo");
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    assert_eq!(
        acquire_file(&request(), &path).unwrap_err(),
        CredentialError::NotRegularFile
    );
}

#[derive(Default)]
struct MemoryStore {
    values: RefCell<BTreeMap<String, String>>,
    reads: Cell<usize>,
}

impl SecretStore for MemoryStore {
    fn get(&self, owner: &OwnerBinding) -> Result<Option<BearerSecret>> {
        self.reads.set(self.reads.get() + 1);
        self.values
            .borrow()
            .get(&store::test_account(owner))
            .cloned()
            .map(BearerSecret::new)
            .transpose()
    }
    fn put(&self, owner: &OwnerBinding, secret: &BearerSecret) -> Result<()> {
        self.values
            .borrow_mut()
            .insert(store::test_account(owner), secret.expose().into());
        Ok(())
    }
    fn remove(&self, owner: &OwnerBinding) -> Result<()> {
        self.values.borrow_mut().remove(&store::test_account(owner));
        Ok(())
    }
}

#[test]
fn stored_credentials_are_profile_bound_and_read_once() {
    let store = MemoryStore::default();
    let request = request();
    assert_eq!(
        acquire_stored(&request, &store).unwrap_err(),
        CredentialError::MissingStoredCredential
    );
    store
        .put(request.owner(), &BearerSecret::new("first".into()).unwrap())
        .unwrap();
    let captured = acquire_stored(&request, &store).unwrap();
    let reads = store.reads.get();
    store
        .put(
            request.owner(),
            &BearerSecret::new("second".into()).unwrap(),
        )
        .unwrap();
    assert_eq!(
        captured
            .bearer_for(&request, OffsetDateTime::now_utc())
            .unwrap()
            .expose(),
        "first"
    );
    assert_eq!(store.reads.get(), reads);
    assert!(store.get(&owner("other-profile")).unwrap().is_none());
    store.remove(request.owner()).unwrap();
    store.remove(request.owner()).unwrap();
    assert!(store.get(request.owner()).unwrap().is_none());
}

#[test]
fn store_keys_bind_every_owner_constraint_without_ambiguous_concatenation() {
    let make = |kind, id: &str, profile: &str, route: &str, org: Option<&str>| {
        OwnerBinding::new(
            kind,
            id.into(),
            profile.into(),
            route.into(),
            org.map(str::to_owned),
        )
        .unwrap()
    };
    let owners = [
        make(
            OwnerKind::Server,
            "a:b",
            "c",
            "https://example.invalid/one/",
            None,
        ),
        make(
            OwnerKind::Server,
            "a",
            "b:c",
            "https://example.invalid/one/",
            None,
        ),
        make(
            OwnerKind::Server,
            "a:b",
            "c",
            "https://example.invalid/two/",
            None,
        ),
        make(
            OwnerKind::Server,
            "a:b",
            "c",
            "https://other.invalid/one/",
            None,
        ),
        make(
            OwnerKind::Profile,
            "a:b",
            "c",
            "https://example.invalid/one/",
            Some("org-1"),
        ),
        make(
            OwnerKind::Profile,
            "a:b",
            "c",
            "https://example.invalid/one/",
            Some("org-2"),
        ),
    ];
    let keys: std::collections::BTreeSet<_> = owners.iter().map(store::test_account).collect();
    assert_eq!(keys.len(), owners.len());
    assert!(keys.iter().all(|key| key.len() == 64));
}

#[test]
fn unavailable_store_fails_without_fallback() {
    struct Unavailable;
    impl SecretStore for Unavailable {
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
    assert_eq!(
        acquire_stored(&request(), &Unavailable).unwrap_err(),
        CredentialError::StoreUnavailable
    );
}

#[test]
fn result_rejects_a_different_request_and_expiry_at_the_boundary() {
    let request = request();
    let other = CredentialRequest::new(
        owner("other"),
        Purpose::Data,
        request.endpoint().into(),
        None,
        None,
    )
    .unwrap();
    let now = OffsetDateTime::UNIX_EPOCH;
    let mut captured =
        AcquiredCredential::new(&request, BearerSecret::new("test-only".into()).unwrap());
    assert_eq!(
        captured.bearer_for(&other, now).unwrap_err(),
        CredentialError::BindingMismatch
    );
    assert_eq!(captured.expires_at(), None);
    captured.expires_at = Some(now);
    assert_eq!(
        captured.bearer_for(&request, now).unwrap_err(),
        CredentialError::Expired
    );
    assert!(
        captured
            .bearer_for(&request, now - time::Duration::seconds(1))
            .is_ok()
    );
    assert!(!format!("{captured:?}").contains("test-only"));
    assert_eq!(
        captured
            .clone()
            .bound_expiry(now + time::Duration::seconds(60))
            .expires_at(),
        Some(now),
        "verified session metadata cannot extend provider expiry"
    );
    assert_eq!(
        captured
            .bound_expiry(now - time::Duration::seconds(60))
            .expires_at(),
        Some(now - time::Duration::seconds(60)),
        "the shorter verified expiry wins"
    );
}

#[test]
fn binding_refuses_incomplete_owners_and_wrong_planes() {
    assert!(
        OwnerBinding::new(
            OwnerKind::Profile,
            "o".into(),
            "p".into(),
            "https://example.invalid/".into(),
            None
        )
        .is_err()
    );
    for route in [
        "http://localhost/",
        "https://user:password@example.invalid/",
        "https://example.invalid/?q=1",
        "https://example.invalid/#fragment",
        "not-a-url",
    ] {
        assert!(
            OwnerBinding::new(
                OwnerKind::Server,
                "o".into(),
                "p".into(),
                route.into(),
                None
            )
            .is_err()
        );
    }
    assert!(
        CredentialRequest::new(
            owner("p"),
            Purpose::Control,
            owner("p").route().into(),
            None,
            None
        )
        .is_err()
    );
    assert!(
        CredentialRequest::new(
            owner("p"),
            Purpose::Data,
            "https://example.invalid/sibling/".into(),
            None,
            None
        )
        .is_err()
    );
}

#[test]
fn profile_store_never_substitutes_a_control_token_for_data() {
    let owner = OwnerBinding::new(
        OwnerKind::Profile,
        "o".into(),
        "p".into(),
        "https://control.invalid/api/".into(),
        Some("org".into()),
    )
    .unwrap();
    assert_eq!(serde_json::to_value(&owner).unwrap()["kind"], "profile");
    let data = CredentialRequest::new(
        owner.clone(),
        Purpose::Data,
        "https://data.invalid/".into(),
        Some("aud".into()),
        Some(ResourceIdentity {
            cluster_id: "cluster".into(),
            cluster_incarnation: "incarnation".into(),
        }),
    )
    .unwrap();
    let store = MemoryStore::default();
    store
        .put(&owner, &BearerSecret::new("control-only".into()).unwrap())
        .unwrap();
    assert_eq!(
        acquire_stored(&data, &store).unwrap_err(),
        CredentialError::BindingMismatch
    );
    assert_eq!(store.reads.get(), 0);
}
