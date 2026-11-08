//! One policy for a conditional request whose answer can be lost.
//!
//! A conditional put (create-if-absent, if-match) applies at most once. Two
//! copies of one request, for one key with one precondition, apply at most
//! once between them, and once the key fails the precondition no copy can
//! apply. A request whose answer was lost can therefore be sent again
//! unchanged. This module owns how many times, how far apart, which failures
//! are lost answers, and which answers end the request. Each caller owns its
//! read-back, since only it knows what the key holds when its own request
//! applied. The control-object writes in this crate and the manifest commit
//! handler in `omnigraph-core` (`lance_clone`) both resolve through it.

use std::time::Duration;

use crate::OWNED_STORE_NAME;

/// How many times one conditional request is re-issued after a lost answer,
/// and the pause before each re-issue: 0.2 s doubling to 1.6 s, 3 s in all,
/// beside each request's own client timeout. A store that answers nothing
/// for that long is failing, not throttling one request.
pub const CONDITIONAL_REQUEST_REISSUES: u32 = 4;
const FIRST_REISSUE_PAUSE: Duration = Duration::from_millis(200);

/// The pause before re-issue `reissue`, counted from 1.
pub fn reissue_pause(reissue: u32) -> Duration {
    FIRST_REISSUE_PAUSE * 2u32.saturating_pow(reissue.saturating_sub(1))
}

/// What the client that carries a conditional request can do with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestClient {
    /// Sends each request once: a storage scope's single-attempt client
    /// (`max_retries` 0) or a store with no network client. Its error
    /// answers the only copy it sent.
    SingleAttempt,
    /// May send one request more than once on its own: `object_store`'s
    /// retrying client, which an unscoped S3 or Azure adapter and Lance's
    /// default provider use (up to `max_retries` copies; on an S3 if-match,
    /// HTTP 409 too). Its error can follow an earlier copy that applied.
    Repeating,
}

/// How a copy of a conditional request failed, other than by its
/// precondition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailedRequest {
    /// Refused before it could apply, or it never left the process: no copy
    /// sent by this call can land because of it.
    Refused,
    /// The copy may have applied, or may still.
    Ambiguous,
}

/// Classify a failed copy by the typed error the store returned.
///
/// A missing key or container, an invalid path, an unsupported operation, an
/// in-memory store's error (it applies a put before it answers) and a
/// refusal by the owning storage scope (which sends nothing) are refusals.
/// An authorization failure is a refusal only on a single-attempt client: a
/// repeating client can return one after an earlier attempt the store
/// applied. Everything else is ambiguous, including a generic failure (a
/// 5xx, a throttle, a timeout, a lost connection) and anything this function
/// does not recognise.
pub fn classify_failed_request(
    error: &object_store::Error,
    client: RequestClient,
) -> FailedRequest {
    use object_store::Error as Store;

    match error {
        Store::Generic { store, .. } if *store == "InMemory" || *store == OWNED_STORE_NAME => {
            FailedRequest::Refused
        }
        Store::NotFound { .. }
        | Store::InvalidPath { .. }
        | Store::NotSupported { .. }
        | Store::NotImplemented { .. }
        | Store::UnknownConfigurationKey { .. } => FailedRequest::Refused,
        Store::PermissionDenied { .. } | Store::Unauthenticated { .. }
            if client == RequestClient::SingleAttempt =>
        {
            FailedRequest::Refused
        }
        _ => FailedRequest::Ambiguous,
    }
}

/// The answer to one copy of a conditional request.
#[derive(Debug)]
pub enum Answer<T, E> {
    /// The store applied this copy.
    Applied(T),
    /// This copy failed its precondition.
    Conflict(E),
    /// This copy was refused before it could apply.
    Refused(E),
    /// This copy may have applied, or may still.
    Lost(E),
}

/// How a conditional request ended.
#[derive(Debug)]
pub enum Resolution<T, E> {
    /// A copy applied, after `reissues` re-issues.
    Applied { value: T, reissues: u32 },
    /// The only copy sent failed its precondition: another request holds
    /// the key.
    Conflict(E),
    /// The only copy sent was refused before it could apply.
    Refused(E),
    /// A copy may have applied and no answer settled it. `error` is the last
    /// answer: a conflict (`conflict`), which an earlier copy may explain,
    /// or the last lost or refused re-issue. Only a read of the key tells.
    Unsettled {
        error: E,
        conflict: bool,
        reissues: u32,
    },
}

/// What a caller does after one answer.
#[derive(Debug)]
pub enum Step<T, E> {
    /// Send the identical request again after `pause`. `error` is the answer
    /// being re-issued, for the caller's log.
    Reissue {
        error: E,
        reissue: u32,
        pause: Duration,
    },
    /// The request has ended.
    Done(Resolution<T, E>),
}

/// The re-issue policy for one conditional request. The caller sends a
/// copy, passes its answer to [`Self::answer`], and sends again while the
/// step says so:
///
/// - an applied copy ends the request;
/// - the first copy's conflict ends it as a conflict when nothing else can
///   explain it (`first_conflict_settles`: a single-attempt client), and as
///   unsettled otherwise;
/// - a later copy's conflict ends it unsettled: the earlier copy may be what
///   the key holds, or, on S3, a write still in flight that the store
///   answered 409 over;
/// - the first copy's refusal ends it as refused;
/// - a lost answer, and any refused re-issue, is re-issued until the budget
///   is spent, and then ends unsettled.
#[derive(Debug)]
pub struct ConditionalRequest {
    first_conflict_settles: bool,
    reissues: u32,
}

impl ConditionalRequest {
    pub fn new(first_conflict_settles: bool) -> Self {
        Self {
            first_conflict_settles,
            reissues: 0,
        }
    }

    /// How many re-issues have been sent.
    pub fn reissues(&self) -> u32 {
        self.reissues
    }

    pub fn answer<T, E>(&mut self, answer: Answer<T, E>) -> Step<T, E> {
        let first = self.reissues == 0;
        let error = match answer {
            Answer::Applied(value) => {
                return Step::Done(Resolution::Applied {
                    value,
                    reissues: self.reissues,
                });
            }
            Answer::Conflict(error) if first && self.first_conflict_settles => {
                return Step::Done(Resolution::Conflict(error));
            }
            Answer::Conflict(error) => {
                return Step::Done(Resolution::Unsettled {
                    error,
                    conflict: true,
                    reissues: self.reissues,
                });
            }
            Answer::Refused(error) if first => return Step::Done(Resolution::Refused(error)),
            Answer::Refused(error) | Answer::Lost(error) => error,
        };
        if self.reissues == CONDITIONAL_REQUEST_REISSUES {
            return Step::Done(Resolution::Unsettled {
                error,
                conflict: false,
                reissues: self.reissues,
            });
        }
        self.reissues += 1;
        Step::Reissue {
            error,
            reissue: self.reissues,
            pause: reissue_pause(self.reissues),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic(store: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store,
            source: std::io::Error::other("failed").into(),
        }
    }

    fn denied() -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: "p".into(),
            source: std::io::Error::other("403").into(),
        }
    }

    #[test]
    fn reissue_pauses_double_and_stay_bounded() {
        let pauses: Vec<_> = (1..=CONDITIONAL_REQUEST_REISSUES)
            .map(reissue_pause)
            .collect();
        assert_eq!(
            pauses,
            [200, 400, 800, 1600].map(Duration::from_millis).to_vec()
        );
        assert!(pauses.iter().sum::<Duration>() <= Duration::from_secs(3));
    }

    /// An authorization failure is a refusal only where no earlier copy was
    /// sent; on a repeating client it can follow one the store applied.
    #[test]
    fn only_a_single_attempt_reads_an_auth_failure_as_a_refusal_issue_1067() {
        use RequestClient::*;
        for client in [SingleAttempt, Repeating] {
            for error in [
                generic("InMemory"),
                generic(OWNED_STORE_NAME),
                object_store::Error::NotFound {
                    path: "p".into(),
                    source: std::io::Error::other("404").into(),
                },
                object_store::Error::NotImplemented {
                    operation: "put".into(),
                    implementer: "test".into(),
                },
            ] {
                assert_eq!(
                    classify_failed_request(&error, client),
                    FailedRequest::Refused,
                    "{client:?}: {error}"
                );
            }
            for error in [generic("S3"), generic("LocalFileSystem")] {
                assert_eq!(
                    classify_failed_request(&error, client),
                    FailedRequest::Ambiguous,
                    "{client:?}: {error}"
                );
            }
        }
        assert_eq!(
            classify_failed_request(&denied(), SingleAttempt),
            FailedRequest::Refused
        );
        assert_eq!(
            classify_failed_request(&denied(), Repeating),
            FailedRequest::Ambiguous
        );
    }

    /// Drive one request over scripted answers; returns its resolution and
    /// how many copies were sent.
    fn drive(
        first_conflict_settles: bool,
        answers: &[Answer<(), &'static str>],
    ) -> (Resolution<(), &'static str>, usize) {
        let mut request = ConditionalRequest::new(first_conflict_settles);
        for (sent, answer) in answers.iter().enumerate() {
            let answer = match answer {
                Answer::Applied(()) => Answer::Applied(()),
                Answer::Conflict(error) => Answer::Conflict(*error),
                Answer::Refused(error) => Answer::Refused(*error),
                Answer::Lost(error) => Answer::Lost(*error),
            };
            match request.answer(answer) {
                Step::Done(resolution) => return (resolution, sent + 1),
                Step::Reissue { reissue, pause, .. } => {
                    assert_eq!(reissue as usize, sent + 1);
                    assert_eq!(pause, reissue_pause(reissue));
                }
            }
        }
        panic!("the script ended before the request: {answers:?}");
    }

    #[test]
    fn a_request_ends_only_on_an_answer_that_settles_it_issue_1067() {
        use Answer::*;
        assert!(matches!(
            drive(true, &[Applied(())]),
            (Resolution::Applied { reissues: 0, .. }, 1)
        ));
        assert!(matches!(
            drive(true, &[Lost("a"), Refused("b"), Applied(())]),
            (Resolution::Applied { reissues: 2, .. }, 3)
        ));
        assert!(matches!(
            drive(true, &[Conflict("held")]),
            (Resolution::Conflict("held"), 1)
        ));
        assert!(
            matches!(
                drive(false, &[Conflict("held")]),
                (
                    Resolution::Unsettled {
                        conflict: true,
                        reissues: 0,
                        ..
                    },
                    1
                )
            ),
            "a repeating client's conflict may answer its own earlier copy"
        );
        assert!(
            matches!(
                drive(true, &[Lost("a"), Conflict("409")]),
                (
                    Resolution::Unsettled {
                        conflict: true,
                        reissues: 1,
                        ..
                    },
                    2
                )
            ),
            "a re-issue's conflict may answer the earlier copy"
        );
        assert!(matches!(
            drive(true, &[Refused("denied")]),
            (Resolution::Refused("denied"), 1)
        ));
        let lost = [const { Lost("timeout") }; 1 + CONDITIONAL_REQUEST_REISSUES as usize];
        assert!(matches!(
            drive(true, &lost),
            (
                Resolution::Unsettled {
                    error: "timeout",
                    conflict: false,
                    reissues: CONDITIONAL_REQUEST_REISSUES,
                },
                5
            )
        ));
    }
}
