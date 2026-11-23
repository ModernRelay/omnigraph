//! What one bootstrap attempt actually sent to storage.
//!
//! Every control request of an attempt goes through [`RecordingAdapter`]. It
//! forwards reads, counts each conditional write when it is sent and when it is
//! answered, and refuses every unconditional mutation before sending it: first
//! bootstrap issues none. The attempt's [`BootstrapEffects`] is derived from
//! those counts alone, so no step reports its own effects.

use std::sync::{Arc, Mutex};

use omnigraph_storage::{
    ListDirBounds, ReadThroughMount, Result as StorageResult, StorageAdapter, StorageError,
    StorageFailure, StorageFailureKind, StorageIoScope,
};
use serde::{Deserialize, Serialize};

/// What the writes an attempt sent may have changed, ordered by how little is
/// known: `none` < `confirmed` < `unconfirmed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapEffects {
    /// No write was sent, or every write sent was answered as not applied.
    None,
    /// Every write sent has a known outcome and at least one applied.
    Confirmed,
    /// A write sent has an unknown outcome: it may have applied, or may still.
    Unconfirmed,
}

#[derive(Debug, Default)]
struct Counts {
    sent: u64,
    answered: u64,
    applied: u64,
    unresolved: u64,
}

/// The conditional writes one attempt sent and how each was answered.
#[derive(Debug, Default)]
pub(super) struct EffectLog(Mutex<Counts>);

impl EffectLog {
    fn sent(&self) {
        self.0.lock().unwrap().sent += 1;
    }

    /// The storage contract: an applied answer applied; a precondition failure
    /// and every error other than `Unresolved` applied nothing; `Unresolved`
    /// may have applied.
    fn answered(&self, applied: Result<bool, &StorageError>) {
        let mut counts = self.0.lock().unwrap();
        counts.answered += 1;
        match applied {
            Ok(true) => counts.applied += 1,
            Ok(false) => {}
            Err(error) if error.is_unresolved_conditional() => counts.unresolved += 1,
            Err(_) => {}
        }
    }

    /// A request sent and never answered (its future was dropped) counts as
    /// unknown, as an `Unresolved` answer does.
    pub(super) fn effects(&self) -> BootstrapEffects {
        let counts = self.0.lock().unwrap();
        if counts.unresolved > 0 || counts.sent > counts.answered {
            BootstrapEffects::Unconfirmed
        } else if counts.applied > 0 {
            BootstrapEffects::Confirmed
        } else {
            BootstrapEffects::None
        }
    }
}

#[derive(Debug)]
pub(super) struct RecordingAdapter {
    inner: Arc<dyn StorageAdapter>,
    log: Arc<EffectLog>,
}

impl RecordingAdapter {
    pub(super) fn new(inner: Arc<dyn StorageAdapter>, log: Arc<EffectLog>) -> Self {
        Self { inner, log }
    }

    /// Refused before anything is sent, so it records nothing.
    fn refuse<T>(operation: &str) -> StorageResult<T> {
        Err(StorageError::Backend(StorageFailure::new(
            StorageFailureKind::Configuration,
            format!(
                "first bootstrap sends only conditional writes; refused an unconditional {operation}"
            ),
        )))
    }
}

#[async_trait::async_trait]
impl StorageAdapter for RecordingAdapter {
    fn io_scope(&self) -> Option<StorageIoScope> {
        self.inner.io_scope()
    }

    fn read_through_mount(&self) -> Option<ReadThroughMount> {
        self.inner.read_through_mount()
    }

    async fn read_text(&self, uri: &str) -> StorageResult<String> {
        self.inner.read_text(uri).await
    }

    async fn read_text_if_exists(&self, uri: &str) -> StorageResult<Option<String>> {
        self.inner.read_text_if_exists(uri).await
    }

    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> StorageResult<Option<String>> {
        self.inner.read_text_if_exists_bounded(uri, max_bytes).await
    }

    async fn read_bytes_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> StorageResult<Option<Vec<u8>>> {
        self.inner
            .read_bytes_if_exists_bounded(uri, max_bytes)
            .await
    }

    async fn write_text(&self, _uri: &str, _contents: &str) -> StorageResult<()> {
        Self::refuse("text put")
    }

    async fn write_bytes(&self, _uri: &str, _contents: &[u8]) -> StorageResult<()> {
        Self::refuse("bytes put")
    }

    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> StorageResult<bool> {
        self.log.sent();
        let answer = self.inner.write_text_if_absent(uri, contents).await;
        self.log.answered(answer.as_ref().map(|created| *created));
        answer
    }

    async fn exists(&self, uri: &str) -> StorageResult<bool> {
        self.inner.exists(uri).await
    }

    async fn rename_text(&self, _from_uri: &str, _to_uri: &str) -> StorageResult<()> {
        Self::refuse("rename")
    }

    async fn delete(&self, _uri: &str) -> StorageResult<()> {
        Self::refuse("delete")
    }

    async fn list_dir(&self, dir_uri: &str) -> StorageResult<Vec<String>> {
        self.inner.list_dir(dir_uri).await
    }

    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: ListDirBounds,
    ) -> StorageResult<Vec<String>> {
        self.inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await
    }

    async fn read_text_versioned(&self, uri: &str) -> StorageResult<(String, String)> {
        self.inner.read_text_versioned(uri).await
    }

    async fn read_text_versioned_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> StorageResult<Option<(String, String)>> {
        self.inner
            .read_text_versioned_if_exists_bounded(uri, max_bytes)
            .await
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> StorageResult<Option<String>> {
        self.log.sent();
        let answer = self
            .inner
            .write_text_if_match(uri, contents, expected_version)
            .await;
        self.log
            .answered(answer.as_ref().map(|version| version.is_some()));
        answer
    }

    async fn delete_prefix(&self, _prefix_uri: &str) -> StorageResult<()> {
        Self::refuse("prefix delete")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unresolved() -> StorageError {
        StorageError::Unresolved(StorageFailure::new(StorageFailureKind::Transient, "lost"))
    }

    #[test]
    fn effects_follow_the_answers_of_what_was_sent() {
        let log = EffectLog::default();
        assert_eq!(log.effects(), BootstrapEffects::None);
        log.sent();
        log.answered(Ok(false));
        assert_eq!(log.effects(), BootstrapEffects::None);
        log.sent();
        log.answered(Err(&StorageError::Backend(StorageFailure::new(
            StorageFailureKind::Configuration,
            "refused",
        ))));
        assert_eq!(log.effects(), BootstrapEffects::None);
        log.sent();
        log.answered(Ok(true));
        assert_eq!(log.effects(), BootstrapEffects::Confirmed);
        log.sent();
        assert_eq!(log.effects(), BootstrapEffects::Unconfirmed, "in flight");
        log.answered(Err(&unresolved()));
        assert_eq!(log.effects(), BootstrapEffects::Unconfirmed);
    }
}
