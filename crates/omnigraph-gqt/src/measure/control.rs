//! The control realm of `--measure`: the engine's `StorageAdapter` calls,
//! logged as the object-store requests the DST in-memory adapter makes for
//! them (the mapping is in the crate README's measure section).

use std::sync::Arc;

use async_trait::async_trait;
use omnigraph::error::{OmniError, Result as OmniResult};
use omnigraph::storage::{ListDirBounds, StorageAdapter};

use super::{MEASURE, Measure, Object, Started, failed, verb_of, LIST_PAGE};

/// The measured adapter around `base`, for the worker to hand the engine,
/// calling `progress` per request; `base` itself when the process is not
/// measuring.
pub(crate) fn wrap_adapter(
    base: Arc<dyn StorageAdapter>,
    progress: fn(),
) -> Arc<dyn StorageAdapter> {
    match MEASURE.get() {
        Some(measure) => Arc::new(MeasuredAdapter {
            inner: base,
            measure: Arc::clone(measure),
            progress,
        }),
        None => base,
    }
}

/// The mapping is kept in step with `ObjectStorageAdapter` by hand and
/// pinned, as this wrapper's output, by
/// `adapter_calls_are_logged_as_the_in_memory_adapters_requests`.
#[derive(Debug)]
struct MeasuredAdapter {
    inner: Arc<dyn StorageAdapter>,
    measure: Arc<Measure>,
    progress: fn(),
}

/// The verb a served request is logged under when the adapter's result,
/// not an error, says whether the store refused it.
fn verb_if(verb: &'static str, served: bool) -> &'static str {
    if served { verb } else { failed(verb) }
}

impl MeasuredAdapter {
    async fn begin(&self, bytes: u64) -> Started {
        (self.progress)();
        Started::enter(&self.measure, bytes).await
    }

    fn note(&self, started: &Started, verb: &'static str, uri: &str, bytes: u64) {
        self.measure
            .note_object(started, verb, Object::control(uri), bytes, None);
    }

    /// A read's bytes, known once it returned, charged and logged.
    async fn note_read(
        &self,
        mut started: Started,
        verb: &'static str,
        uri: &str,
        bytes: u64,
        range: Option<String>,
    ) {
        started.charge(&self.measure.model, bytes).await;
        self.measure
            .note_object(&started, verb, Object::control(uri), bytes, range);
    }

    /// A bounded read: one `get` of `0-(max+1)`, `get_failed` on a miss; an
    /// object over the bound was read up to the bound and refused after.
    async fn note_bounded_read<T>(
        &self,
        uri: &str,
        max_bytes: u64,
        result: OmniResult<Option<T>>,
        started: Started,
        len: impl FnOnce(&T) -> usize,
    ) -> OmniResult<Option<T>> {
        let (verb, bytes) = match &result {
            Ok(Some(value)) => ("get", len(value) as u64),
            Err(OmniError::ResourceLimitExceeded { actual, .. }) => ("get", *actual),
            Ok(None) | Err(_) => ("get_failed", 0),
        };
        let range = Some(format!("0-{}", max_bytes.saturating_add(1)));
        self.note_read(started, verb, uri, bytes, range).await;
        result
    }

    /// A listing: one `list` per page of the entries it returned, at least
    /// one, `list_failed` when the store refused it (a bound the adapter
    /// enforced after a served listing is not a refusal).
    async fn note_listing(&self, started: Started, uri: &str, result: &OmniResult<Vec<String>>) {
        let served = match result {
            Ok(_) | Err(OmniError::ResourceLimitExceeded { .. }) => true,
            Err(_) => false,
        };
        self.note(&started, verb_if("list", served), uri, 0);
        let entries = result.as_ref().map_or(0, Vec::len);
        for _ in 1..entries.div_ceil(LIST_PAGE) {
            let started = self.begin(0).await;
            self.note(&started, "list", uri, 0);
        }
    }
}

#[async_trait]
impl StorageAdapter for MeasuredAdapter {
    async fn read_text(&self, uri: &str) -> OmniResult<String> {
        let started = self.begin(0).await;
        let result = self.inner.read_text(uri).await;
        let bytes = result.as_ref().map_or(0, |text| text.len() as u64);
        self.note_read(started, verb_of("get", &result), uri, bytes, None)
            .await;
        result
    }

    async fn read_text_if_exists(&self, uri: &str) -> OmniResult<Option<String>> {
        let started = self.begin(0).await;
        let result = self.inner.read_text_if_exists(uri).await;
        let (verb, bytes) = match &result {
            Ok(Some(text)) => ("get", text.len() as u64),
            Ok(None) | Err(_) => ("get_failed", 0),
        };
        self.note_read(started, verb, uri, bytes, None).await;
        result
    }

    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> OmniResult<Option<String>> {
        let started = self.begin(0).await;
        let result = self.inner.read_text_if_exists_bounded(uri, max_bytes).await;
        self.note_bounded_read(uri, max_bytes, result, started, String::len)
            .await
    }

    async fn read_bytes_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> OmniResult<Option<Vec<u8>>> {
        let started = self.begin(0).await;
        let result = self
            .inner
            .read_bytes_if_exists_bounded(uri, max_bytes)
            .await;
        self.note_bounded_read(uri, max_bytes, result, started, Vec::len)
            .await
    }

    async fn write_text(&self, uri: &str, contents: &str) -> OmniResult<()> {
        let bytes = contents.len() as u64;
        let started = self.begin(bytes).await;
        let result = self.inner.write_text(uri, contents).await;
        self.note(&started, verb_of("put", &result), uri, bytes);
        result
    }

    async fn write_bytes(&self, uri: &str, contents: &[u8]) -> OmniResult<()> {
        let bytes = contents.len() as u64;
        let started = self.begin(bytes).await;
        let result = self.inner.write_bytes(uri, contents).await;
        self.note(&started, verb_of("put", &result), uri, bytes);
        result
    }

    /// One conditional `put`; the store refusing it (the object exists) is
    /// `put_failed`.
    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> OmniResult<bool> {
        let bytes = contents.len() as u64;
        let started = self.begin(bytes).await;
        let result = self.inner.write_text_if_absent(uri, contents).await;
        let verb = verb_if("put", matches!(result, Ok(true)));
        self.note(&started, verb, uri, bytes);
        result
    }

    async fn exists(&self, uri: &str) -> OmniResult<bool> {
        let started = self.begin(0).await;
        let result = self.inner.exists(uri).await;
        let verb = verb_if("head", matches!(result, Ok(true)));
        self.note(&started, verb, uri, 0);
        if matches!(result, Ok(false)) {
            let started = self.begin(0).await;
            self.note(&started, "list", uri, 0);
        }
        result
    }

    /// The in-memory store has no rename: a `copy` then the `delete` of the
    /// source. A refusal is logged as the copy's.
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> OmniResult<()> {
        let started = self.begin(0).await;
        let result = self.inner.rename_text(from_uri, to_uri).await;
        self.note(&started, verb_of("copy", &result), from_uri, 0);
        if result.is_ok() {
            let started = self.begin(0).await;
            self.note(&started, "delete", from_uri, 0);
        }
        result
    }

    async fn delete(&self, uri: &str) -> OmniResult<()> {
        let started = self.begin(0).await;
        let result = self.inner.delete(uri).await;
        self.note(&started, verb_of("delete", &result), uri, 0);
        result
    }

    async fn list_dir(&self, dir_uri: &str) -> OmniResult<Vec<String>> {
        let started = self.begin(0).await;
        let result = self.inner.list_dir(dir_uri).await;
        self.note_listing(started, dir_uri, &result).await;
        result
    }

    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: ListDirBounds,
    ) -> OmniResult<Vec<String>> {
        let started = self.begin(0).await;
        let result = self
            .inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await;
        self.note_listing(started, dir_uri, &result).await;
        result
    }

    async fn read_text_versioned(&self, uri: &str) -> OmniResult<(String, String)> {
        let started = self.begin(0).await;
        let result = self.inner.read_text_versioned(uri).await;
        let bytes = result.as_ref().map_or(0, |(text, _)| text.len() as u64);
        self.note_read(started, verb_of("get", &result), uri, bytes, None)
            .await;
        result
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> OmniResult<Option<String>> {
        let bytes = contents.len() as u64;
        let started = self.begin(bytes).await;
        let result = self
            .inner
            .write_text_if_match(uri, contents, expected_version)
            .await;
        let verb = verb_if("put", matches!(result, Ok(Some(_))));
        self.note(&started, verb, uri, bytes);
        result
    }

    /// Logged as its listing alone: the deletes that follow are as many as
    /// the listing found, unknown here (README, the control realm's limits).
    async fn delete_prefix(&self, prefix_uri: &str) -> OmniResult<()> {
        let started = self.begin(0).await;
        let result = self.inner.delete_prefix(prefix_uri).await;
        self.note(&started, verb_of("list", &result), prefix_uri, 0);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::measuring;
    use super::super::Model;

    /// The mapping, pinned against the in-memory adapter the DST worker
    /// wraps: each call, the requests it is logged as, their bytes and range,
    /// one tick apart under the unit model since the adapter awaits each. The
    /// ledger is report output a case cannot assert.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn adapter_calls_are_logged_as_the_in_memory_adapters_requests() {
        use omnigraph::storage::{ListDirBounds, ObjectStorageAdapter, StorageAdapter};
        let measure = measuring(Model::named("unit").unwrap());
        let adapter = super::MeasuredAdapter {
            inner: std::sync::Arc::new(ObjectStorageAdapter::in_memory()),
            measure: std::sync::Arc::clone(&measure),
            progress: || {},
        };
        adapter
            .write_text("r/_schema.pg", "node Person {}")
            .await
            .unwrap();
        assert!(adapter.exists("r/_schema.pg").await.unwrap());
        assert!(!adapter.exists("r/_schema.ir.json").await.unwrap());
        assert_eq!(
            adapter.read_text("r/_schema.pg").await.unwrap(),
            "node Person {}"
        );
        assert!(adapter.read_text("r/_schema.ir.json").await.is_err());
        assert!(
            adapter
                .read_text_if_exists("r/_schema.ir.json")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            adapter
                .read_text_if_exists_bounded("r/_schema.pg", 64)
                .await
                .unwrap()
                .as_deref(),
            Some("node Person {}")
        );
        assert!(
            adapter
                .read_text_if_exists_bounded("r/_schema.pg", 4)
                .await
                .is_err()
        );
        assert!(
            adapter
                .write_text_if_absent("r/__init_claim.json", "{}")
                .await
                .unwrap()
        );
        assert!(
            !adapter
                .write_text_if_absent("r/__init_claim.json", "{}")
                .await
                .unwrap()
        );
        let (_, version) = adapter
            .read_text_versioned("r/__init_claim.json")
            .await
            .unwrap();
        assert!(
            adapter
                .write_text_if_match("r/__init_claim.json", "{ }", &version)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            adapter
                .write_text_if_match("r/__init_claim.json", "{  }", &version)
                .await
                .unwrap()
                .is_none()
        );
        adapter
            .rename_text("r/_schema.pg", "r/_schema.pg.staging")
            .await
            .unwrap();
        adapter.delete("r/__init_claim.json").await.unwrap();
        assert_eq!(adapter.list_dir("r").await.unwrap().len(), 1);
        adapter
            .write_bytes("r/__graph_index/csr-current.bin", &[0; 3])
            .await
            .unwrap();
        assert_eq!(
            adapter
                .read_bytes_if_exists_bounded("r/__graph_index/csr-current.bin", 8)
                .await
                .unwrap()
                .unwrap()
                .len(),
            3
        );
        let bounds = ListDirBounds {
            max_matching_entries: 8,
            max_irrelevant_entries: 8,
            max_uri_bytes: 1 << 16,
        };
        assert_eq!(
            adapter
                .list_dir_bounded("r", ".staging", bounds)
                .await
                .unwrap(),
            ["r/_schema.pg.staging"]
        );
        adapter.delete_prefix("r/__graph_index").await.unwrap();
        let ledger = measure.ledger.lock().unwrap();
        let rows: Vec<(&str, &str, &str, Option<&str>, u64)> = ledger
            .iter()
            .map(|r| {
                (
                    r.verb,
                    r.class.as_str(),
                    r.path.as_str(),
                    r.range.as_deref(),
                    r.bytes,
                )
            })
            .collect();
        assert_eq!(
            rows,
            [
                ("put", "control_schema", "r/_schema.pg", None, 14),
                ("head", "control_schema", "r/_schema.pg", None, 0),
                (
                    "head_failed",
                    "control_schema",
                    "r/_schema.ir.json",
                    None,
                    0
                ),
                ("list", "control_schema", "r/_schema.ir.json", None, 0),
                ("get", "control_schema", "r/_schema.pg", None, 14),
                ("get_failed", "control_schema", "r/_schema.ir.json", None, 0),
                ("get_failed", "control_schema", "r/_schema.ir.json", None, 0),
                ("get", "control_schema", "r/_schema.pg", Some("0-65"), 14),
                ("get", "control_schema", "r/_schema.pg", Some("0-5"), 5),
                ("put", "control_claim", "r/__init_claim.json", None, 2),
                (
                    "put_failed",
                    "control_claim",
                    "r/__init_claim.json",
                    None,
                    2
                ),
                ("get", "control_claim", "r/__init_claim.json", None, 2),
                ("put", "control_claim", "r/__init_claim.json", None, 3),
                (
                    "put_failed",
                    "control_claim",
                    "r/__init_claim.json",
                    None,
                    4
                ),
                ("copy", "control_schema", "r/_schema.pg", None, 0),
                ("delete", "control_schema", "r/_schema.pg", None, 0),
                ("delete", "control_claim", "r/__init_claim.json", None, 0),
                ("list", "control_other", "r", None, 0),
                (
                    "put",
                    "control_graph_index",
                    "r/__graph_index/csr-current.bin",
                    None,
                    3
                ),
                (
                    "get",
                    "control_graph_index",
                    "r/__graph_index/csr-current.bin",
                    Some("0-9"),
                    3
                ),
                ("list", "control_other", "r", None, 0),
                ("list", "control_graph_index", "r/__graph_index", None, 0),
            ]
        );
        let ticks: Vec<u64> = ledger.iter().map(|r| r.tick).collect();
        assert_eq!(ticks, (0..ledger.len() as u64).collect::<Vec<_>>());
        assert!(ledger.iter().all(|r| r.dataset == "control"));
    }
}
