//! What one commit's conditional manifest request established.
//!
//! Lance commits a version by writing its manifest put-if-absent through the
//! dataset's commit handler, once per attempt, and returns a failed commit's
//! error whether or not that request was ever sent: a transaction file put
//! that fails before it and a manifest put that times out after the store
//! applied it come back as the same opaque storage error. The handler
//! OmniGraph installs on every dataset (`lance_clone`) sits on that request.
//! It re-issues a request whose answer was ambiguous, unchanged, until the
//! store answers it definitively, and it records what it established in a
//! [`ManifestRequestProbe`] the committer may pass for one commit. The
//! committer then knows from the request itself, never from error text,
//! whether the version it attempted can still land.
//!
//! The re-issue is the resolution the precondition makes safe: two requests
//! for one key with the same manifest apply at most once, and once the key is
//! held, by this commit or another, no earlier request for it can land. Lance's
//! own read-back (`verify_commit_outcome`) then decides whose manifest holds a
//! conflicting key by comparing the complete transaction it records.

use std::sync::{Arc, Mutex};

use lance_core::Error;
use omnigraph_storage::conditional::{self, RequestClient};

/// The re-issue budget, pauses and failure classes are the storage crate's
/// one policy for conditional requests, shared with control-object writes.
pub use omnigraph_storage::conditional::{
    CONDITIONAL_REQUEST_REISSUES, FailedRequest, reissue_pause,
};

/// What a commit's manifest requests established by the time the commit
/// returned. Ordered by what a caller may conclude: everything above
/// [`Self::Refused`] leaves open that the attempted version is this commit's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ManifestRequest {
    /// The commit sent no manifest request: it failed before its conditional
    /// create, at a transaction file, a read or its own validation.
    NotSent,
    /// The store refused the request before it could apply (a missing
    /// container, an invalid path, a refusal of the owning storage scope),
    /// and no earlier request for the key was ambiguous.
    Refused,
    /// The version is held. Lance reads it back: the commit succeeds when the
    /// holder is this commit and fails with a conflict when it is another.
    Held,
    /// The store wrote the manifest.
    Landed,
    /// A request may have applied, or may still, and no later request for
    /// the key got an answer that settles it. The version may hold this
    /// commit, now or later. A re-issue that meets a held version ends here
    /// too: on S3 the conflict can answer the earlier copy, still in flight,
    /// so Lance's read-back decides and a missing version proves nothing.
    Unresolved,
}

impl ManifestRequest {
    /// Whether the attempted version can hold this commit, now or later.
    pub fn may_have_landed(self) -> bool {
        self > Self::Refused
    }
}

/// One commit's record of its manifest requests. Cloned into the commit
/// handler the commit runs with; read by the committer after Lance returns.
#[derive(Clone, Debug, Default)]
pub struct ManifestRequestProbe {
    state: Arc<Mutex<Option<ManifestRequest>>>,
}

impl ManifestRequestProbe {
    pub fn new() -> Self {
        Self::default()
    }

    /// What every request the handler sent for this commit established.
    pub fn outcome(&self) -> ManifestRequest {
        self.state
            .lock()
            .unwrap()
            .unwrap_or(ManifestRequest::NotSent)
    }

    /// Account one manifest request before it is sent. A request whose
    /// future is dropped before it answers is unresolved: it may still be
    /// applied by the store.
    pub fn attempt(&self) -> ManifestRequestAttempt {
        ManifestRequestAttempt {
            probe: self.clone(),
            finished: false,
        }
    }

    fn record(&self, outcome: ManifestRequest) {
        let mut state = self.state.lock().unwrap();
        // A commit can send requests for more than one key (Lance retries a
        // detached version under a fresh random id). The worst outcome stands:
        // an unresolved request for one key is not settled by another key.
        *state = Some(state.map_or(outcome, |previous| previous.max(outcome)));
    }
}

/// One accounted manifest request; see [`ManifestRequestProbe::attempt`].
#[derive(Debug)]
pub struct ManifestRequestAttempt {
    probe: ManifestRequestProbe,
    finished: bool,
}

impl ManifestRequestAttempt {
    pub fn finish(mut self, outcome: ManifestRequest) {
        self.finished = true;
        self.probe.record(outcome);
    }
}

impl Drop for ManifestRequestAttempt {
    fn drop(&mut self) {
        if !self.finished {
            self.probe.record(ManifestRequest::Unresolved);
        }
    }
}

/// A log-safe name for a failed request's error: its object-store variant,
/// never its message, which can carry object paths.
pub fn request_error_class(error: &Error) -> &'static str {
    use object_store::Error as Store;

    match error {
        Error::NotFound { .. } => "not_found",
        Error::IO { source, .. } => match source.downcast_ref::<Store>() {
            Some(Store::Generic { .. }) => "generic",
            Some(Store::NotFound { .. }) => "not_found",
            Some(Store::AlreadyExists { .. }) => "already_exists",
            Some(Store::Precondition { .. }) => "precondition",
            Some(Store::NotModified { .. }) => "not_modified",
            Some(Store::PermissionDenied { .. }) => "permission_denied",
            Some(Store::Unauthenticated { .. }) => "unauthenticated",
            Some(Store::NotSupported { .. } | Store::NotImplemented { .. }) => "not_supported",
            Some(_) => "object_store",
            None => "io",
        },
        _ => "lance",
    }
}

/// Classify a manifest put's failure by the typed error the store returned,
/// with the storage crate's classification on a repeating client.
///
/// Lance's own "not found" is a refusal. The handler cannot tell Lance's
/// provider from a storage scope's single-attempt client, so it takes the
/// client to repeat requests: an authorization failure and a join error can
/// then follow an earlier internal attempt that the server applied. A local
/// filesystem failure is ambiguous too, by the shared rule: Lance's Windows
/// rename handler can create the manifest before deleting its staging file
/// fails. An error that is not the store's is ambiguous.
pub fn classify_failed_request(error: &Error) -> FailedRequest {
    match error {
        Error::NotFound { .. } => FailedRequest::Refused,
        Error::IO { source, .. } => match source.downcast_ref::<object_store::Error>() {
            Some(store) => conditional::classify_failed_request(store, RequestClient::Repeating),
            None => FailedRequest::Ambiguous,
        },
        _ => FailedRequest::Ambiguous,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_error(error: object_store::Error) -> Error {
        error.into()
    }

    fn generic(store: &'static str) -> Error {
        store_error(object_store::Error::Generic {
            store,
            source: std::io::Error::other("failed").into(),
        })
    }

    #[test]
    fn a_dropped_request_is_unresolved_and_the_worst_outcome_stands_issue_1067() {
        let probe = ManifestRequestProbe::new();
        assert_eq!(probe.outcome(), ManifestRequest::NotSent);
        assert!(!probe.outcome().may_have_landed());

        probe.attempt().finish(ManifestRequest::Refused);
        assert_eq!(probe.outcome(), ManifestRequest::Refused);
        assert!(!probe.outcome().may_have_landed());

        drop(probe.attempt());
        assert_eq!(probe.outcome(), ManifestRequest::Unresolved);
        probe.attempt().finish(ManifestRequest::Landed);
        assert_eq!(
            probe.outcome(),
            ManifestRequest::Unresolved,
            "a later request for another key does not settle an unresolved one"
        );
        assert!(probe.outcome().may_have_landed());

        let held = ManifestRequestProbe::new();
        held.attempt().finish(ManifestRequest::Held);
        assert!(held.outcome().may_have_landed());
    }

    #[test]
    fn only_a_definite_refusal_rules_out_a_late_landing_issue_1067() {
        for error in [
            generic("InMemory"),
            generic(omnigraph_storage::OWNED_STORE_NAME),
            Error::not_found("missing"),
            store_error(object_store::Error::NotImplemented {
                operation: "put".into(),
                implementer: "test".into(),
            }),
        ] {
            assert_eq!(
                classify_failed_request(&error),
                FailedRequest::Refused,
                "{error}"
            );
        }
        for error in [
            generic("LocalFileSystem"),
            generic("S3"),
            generic("MicrosoftAzure"),
            generic("fault injector"),
            store_error(object_store::Error::PermissionDenied {
                path: "p".into(),
                source: std::io::Error::other("403").into(),
            }),
            Error::io("not a store error"),
        ] {
            assert_eq!(
                classify_failed_request(&error),
                FailedRequest::Ambiguous,
                "{error}"
            );
        }
        // What a log records is the error's class, never its message.
        let secret = "s3://bucket/root/__manifest/_versions/7.manifest";
        for (error, class) in [
            (generic("S3"), "generic"),
            (Error::not_found(secret), "not_found"),
            (
                store_error(object_store::Error::PermissionDenied {
                    path: secret.into(),
                    source: std::io::Error::other("403").into(),
                }),
                "permission_denied",
            ),
            (Error::io(secret), "io"),
        ] {
            assert_eq!(request_error_class(&error), class, "{error}");
            assert!(!request_error_class(&error).contains("bucket"));
        }
    }
}
