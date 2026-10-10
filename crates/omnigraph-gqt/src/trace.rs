use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanValues};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

pub(crate) const WORKER_PATH: &str = "OMNIGRAPH_GQT_WORKER_TRACE";
pub(crate) const FORMAT: &str = "omnigraph-gqt-diagnostic-trace";
pub(crate) const VERSION: u64 = 1;
#[cfg(test)]
const SCHEMA: &str = include_str!("../trace_schema.json");
const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECORDS: u64 = 100_000;
const TERMINATOR_BYTES: usize = 256;
const MAX_FIELD_BYTES: usize = 64 * 1024;
static WRITER: OnceLock<Arc<Mutex<Writer>>> = OnceLock::new();

/// The step a row belongs to: the operation the runner began last, read
/// from the value `begin_operation` receives and written as four columns.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct Step {
    #[serde(rename(serialize = "step", deserialize = "ordinal"))]
    pub ordinal: u64,
    #[serde(rename(serialize = "step_line", deserialize = "source_line"))]
    pub source_line: Option<u64>,
    pub loop_binding: Option<Value>,
    pub generation: u64,
}

impl Step {
    /// The step as the terminal report nests it under every evidence record.
    pub(crate) fn report_value(&self) -> Value {
        serde_json::json!({
            "ordinal": self.ordinal, "source_line": self.source_line,
            "loop_binding": self.loop_binding, "generation": self.generation,
        })
    }
}

/// The header row the supervisor writes before the worker starts.
#[derive(Serialize)]
pub(crate) struct Start<'a> {
    pub format: &'static str,
    pub version: u64,
    pub invocation_id: &'a str,
    pub case_path: &'a Path,
    pub case_digest: &'a str,
    pub source_digest: &'a str,
    pub executable_digest: &'a str,
    pub environment: Value,
    pub seed: u64,
    pub replay: usize,
}

#[cfg(test)]
impl<'a> Start<'a> {
    pub(crate) fn test(case_path: &'a Path) -> Self {
        Self {
            format: FORMAT,
            version: VERSION,
            invocation_id: "test",
            case_path,
            case_digest: "",
            source_digest: "",
            executable_digest: "",
            environment: Value::Null,
            seed: 0,
            replay: 0,
        }
    }
}

/// Where a `tracing` span or event was emitted, as the engine declared it.
#[derive(Serialize)]
pub(crate) struct Site<'a> {
    pub target: &'a str,
    pub name: &'a str,
    pub level: &'a str,
    pub file: Option<&'a str>,
    pub line: Option<u32>,
    pub thread: Option<&'a str>,
    pub session: Option<&'static str>,
    pub fields: Map<String, Value>,
}

/// One row of the trace: `kind` is the tag, every other column is the
/// variant's own; `Line` adds `idx` and the step columns.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Row<'a> {
    Start(&'a Start<'a>),
    Operation,
    Observation {
        text: &'a str,
    },
    Evidence {
        record: &'a str,
        value: &'a Value,
        session: Option<&'static str>,
    },
    SeamCrossing {
        seam: &'static str,
        decision: &'static str,
        effect: Option<&'static str>,
        session: Option<&'static str>,
    },
    StoreRequest {
        verb: &'static str,
        class: &'a str,
        dataset: &'a str,
        path: &'a str,
        range: Option<&'a str>,
        bytes: u64,
        session: &'static str,
    },
    Span {
        id: u64,
        parent: Option<u64>,
        #[serde(flatten)]
        site: Site<'a>,
    },
    SpanRecord {
        id: u64,
        fields: Map<String, Value>,
    },
    SpanClose {
        id: u64,
    },
    Event {
        parent: Option<u64>,
        #[serde(flatten)]
        site: Site<'a>,
    },
    Finish {
        code: &'a str,
        phase: &'a str,
    },
    Truncated {
        reason: &'static str,
        max_bytes: usize,
        max_records: u64,
    },
}

#[derive(Serialize)]
struct Line<'a, T: ?Sized> {
    idx: u64,
    #[serde(flatten)]
    record: &'a T,
    #[serde(flatten)]
    step: Option<&'a Step>,
}

struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other("trace record exceeds byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn line_bytes<T: Serialize + ?Sized>(
    idx: u64,
    record: &T,
    step: Option<&Step>,
    limit: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut output = BoundedBytes {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    let encoded = serde_json::to_writer(&mut output, &Line { idx, record, step });
    if output.exceeded {
        return Ok(None);
    }
    encoded?;
    match output.write_all(b"\n") {
        Ok(()) => Ok(Some(output.bytes)),
        Err(_) if output.exceeded => Ok(None),
        Err(error) => Err(error),
    }
}

struct Writer {
    file: File,
    idx: u64,
    bytes: usize,
    limit: usize,
    closed: bool,
    error: Option<String>,
}

impl Writer {
    fn write(&mut self, record: &Row<'_>, step: Option<&Step>) {
        if self.closed {
            return;
        }
        let result = self.write_record(record, step);
        if let Err(error) = result {
            self.error = Some(error.to_string());
            self.closed = true;
        }
    }

    fn write_record(&mut self, record: &Row<'_>, step: Option<&Step>) -> std::io::Result<()> {
        let remaining = self.limit.saturating_sub(self.bytes);
        let row = if self.idx < MAX_RECORDS - 1 {
            line_bytes(
                self.idx,
                record,
                step,
                remaining.saturating_sub(TERMINATOR_BYTES),
            )?
        } else {
            None
        };
        let bytes = match row {
            Some(bytes) => bytes,
            None => {
                self.closed = true;
                line_bytes(
                    self.idx,
                    &Row::Truncated {
                        reason: "trace limit",
                        max_bytes: self.limit,
                        max_records: MAX_RECORDS,
                    },
                    None,
                    remaining,
                )?
                .ok_or_else(|| std::io::Error::other("trace has no room for terminator"))?
            }
        };
        self.file.write_all(&bytes)?;
        self.bytes += bytes.len();
        self.idx += 1;
        Ok(())
    }

    fn finish(&mut self, code: &str, phase: &str, step: Option<&Step>) -> Result<(), String> {
        self.write(&Row::Finish { code, phase }, step);
        self.closed = true;
        if let Err(error) = self.file.sync_all() {
            self.error.get_or_insert_with(|| error.to_string());
        }
        match &self.error {
            Some(error) => Err(format!("report_failed: write trace: {error}")),
            None => Ok(()),
        }
    }
}

pub(crate) fn create(root: &Path, start: &Start<'_>) -> Result<PathBuf, String> {
    let header = line_bytes(0, &Row::Start(start), None, MAX_BYTES - TERMINATOR_BYTES)
        .map_err(|e| format!("report_failed: encode trace header: {e}"))?
        .ok_or("report_failed: trace header exceeds byte limit")?;
    let root = root.join("trace");
    std::fs::create_dir_all(&root)
        .map_err(|e| format!("report_failed: create trace directory: {e}"))?;
    let mut file = tempfile::Builder::new()
        .prefix("attempt-")
        .suffix(".jsonl")
        .tempfile_in(&root)
        .map_err(|e| format!("report_failed: create trace: {e}"))?;
    file.write_all(&header)
        .and_then(|()| file.as_file().sync_all())
        .map_err(|e| format!("report_failed: flush trace header: {e}"))?;
    let (_, path) = file
        .keep()
        .map_err(|e| format!("report_failed: retain trace: {e}"))?;
    let path = path
        .canonicalize()
        .map_err(|e| format!("report_failed: resolve trace: {e}"))?;
    println!("GQT trace: {}", path.display());
    Ok(path)
}

pub(crate) struct Recording(Arc<Mutex<Writer>>);

impl Recording {
    pub(crate) fn start(path: &Path) -> Result<Self, String> {
        let file = File::options()
            .append(true)
            .open(path)
            .map_err(|e| format!("report_failed: open trace: {e}"))?;
        let bytes = usize::try_from(
            file.metadata()
                .map_err(|e| format!("report_failed: trace metadata: {e}"))?
                .len(),
        )
        .map_err(|e| format!("report_failed: trace size: {e}"))?;
        let writer = Arc::new(Mutex::new(Writer {
            file,
            idx: 1,
            bytes,
            limit: MAX_BYTES,
            closed: false,
            error: None,
        }));
        WRITER
            .set(writer.clone())
            .map_err(|_| "report_failed: trace already installed")?;
        let layer = TraceLayer(writer.clone()).with_filter(tracing_subscriber::filter::filter_fn(
            |metadata| metadata.target().starts_with("omnigraph"),
        ));
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
            .map_err(|e| format!("report_failed: install trace subscriber: {e}"))?;
        Ok(Self(writer))
    }

    pub(crate) fn finish(self, code: &str, phase: &str) -> Result<(), String> {
        self.0
            .lock()
            .map_err(|_| "report_failed: trace writer poisoned")?
            .finish(code, phase, current_step().as_ref())
    }
}

/// Whether this process records a trace, the switch for the seam observers.
pub(crate) fn active() -> bool {
    WRITER.get().is_some()
}

/// The operation the worker began last, the step every later row belongs
/// to; steps run one after another, a concurrent block being one step.
static STEP: Mutex<Option<Step>> = Mutex::new(None);

pub(crate) fn current_step() -> Option<Step> {
    STEP.lock().ok().and_then(|step| step.clone())
}

/// The one stream every worker fact flows through: the trace file when one
/// is recording, and the report's fold always.
pub(crate) fn emit(row: &Row<'_>) {
    if let Some(writer) = WRITER.get()
        && let Ok(mut writer) = writer.lock()
    {
        writer.write(row, current_step().as_ref());
    }
    crate::dst_runner::fold(row);
}

pub(crate) fn operation(value: &Value) {
    if let Ok(mut step) = STEP.lock() {
        *step = Step::deserialize(value).ok();
    }
    emit(&Row::Operation);
}

pub(crate) fn store_request(
    verb: &'static str,
    class: &str,
    dataset: &str,
    path: &str,
    range: Option<&str>,
    bytes: u64,
    session: &'static str,
) {
    emit(&Row::StoreRequest {
        verb,
        class,
        dataset,
        path,
        range,
        bytes,
        session,
    });
}

pub(crate) fn crossing(seam: &'static str, decision: &'static str, effect: Option<&'static str>) {
    emit(&Row::SeamCrossing {
        seam,
        decision,
        effect,
        session: current_session(),
    });
}

/// The concurrent-block session whose task is being polled, if any.
fn current_session() -> Option<&'static str> {
    crate::measure::SESSION
        .try_with(|session| session.label.slot)
        .ok()
}

#[derive(Default)]
struct Fields(Map<String, Value>);

#[derive(Default)]
struct BoundedText {
    text: String,
    truncated: bool,
}

impl std::fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        if self.truncated {
            return Err(std::fmt::Error);
        }
        let mut end = value
            .len()
            .min(MAX_FIELD_BYTES.saturating_sub(self.text.len()));
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        self.text.push_str(&value[..end]);
        if end < value.len() {
            self.truncated = true;
            Err(std::fmt::Error)
        } else {
            Ok(())
        }
    }
}

impl BoundedText {
    fn value(self) -> Value {
        if self.truncated {
            serde_json::json!({ "value": self.text, "truncated": true })
        } else {
            Value::String(self.text)
        }
    }
}

fn debug_value(value: &dyn std::fmt::Debug) -> Value {
    let mut text = BoundedText::default();
    if write!(&mut text, "{value:?}").is_err() {
        text.truncated = true;
    }
    text.value()
}

fn string_value(value: &str) -> Value {
    let mut text = BoundedText::default();
    if text.write_str(value).is_err() {
        text.truncated = true;
    }
    text.value()
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), debug_value(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), string_value(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), Value::from(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), Value::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), Value::from(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(
            field.name().into(),
            if value.is_finite() {
                Value::from(value)
            } else {
                Value::from(value.to_string())
            },
        );
    }
}

fn site<'a>(metadata: &Metadata<'a>, thread: &'a std::thread::Thread, fields: Fields) -> Site<'a> {
    Site {
        target: metadata.target(),
        name: metadata.name(),
        level: metadata.level().as_str(),
        file: metadata.file(),
        line: metadata.line(),
        thread: thread.name(),
        session: current_session(),
        fields: fields.0,
    }
}

struct TraceLayer(Arc<Mutex<Writer>>);

impl TraceLayer {
    fn active(&self) -> bool {
        self.0.lock().is_ok_and(|writer| !writer.closed)
    }

    fn emit(&self, record: &Row<'_>) {
        if let Ok(mut writer) = self.0.lock() {
            writer.write(record, current_step().as_ref());
        }
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for TraceLayer {
    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        if !self.active() {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let thread = std::thread::current();
        self.emit(&Row::Event {
            parent: context.event_span(event).map(|span| span.id().into_u64()),
            site: site(event.metadata(), &thread, fields),
        });
    }

    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        if !self.active() {
            return;
        }
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        let thread = std::thread::current();
        self.emit(&Row::Span {
            id: id.into_u64(),
            parent: context
                .span(id)
                .and_then(|span| span.parent().map(|p| p.id().into_u64())),
            site: site(attributes.metadata(), &thread, fields),
        });
    }

    fn on_record(&self, id: &Id, values: &SpanValues<'_>, _context: Context<'_, S>) {
        if !self.active() {
            return;
        }
        let mut fields = Fields::default();
        values.record(&mut fields);
        self.emit(&Row::SpanRecord {
            id: id.into_u64(),
            fields: fields.0,
        });
    }

    fn on_close(&self, id: Id, _context: Context<'_, S>) {
        if !self.active() {
            return;
        }
        self.emit(&Row::SpanClose { id: id.into_u64() });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::SerializeSeq;
    use serde_json::json;
    use std::cell::Cell;
    use std::io::{Read, Seek};

    fn writer(file: File, idx: u64, limit: usize) -> Writer {
        Writer {
            file,
            idx,
            bytes: 0,
            limit,
            closed: false,
            error: None,
        }
    }

    fn records(writer: &mut Writer) -> Vec<Value> {
        writer.file.rewind().unwrap();
        let mut text = String::new();
        writer.file.read_to_string(&mut text).unwrap();
        assert!(text.ends_with('\n'));
        text.lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn step() -> Value {
        json!({"ordinal": 5, "source_line": 33, "loop_binding": null, "generation": 0})
    }

    fn examples<'a>(start: &'a Start<'a>) -> Vec<Row<'a>> {
        let site = || Site {
            target: "omnigraph::traverse",
            name: "event crates/omnigraph/src/engine/mod.rs:509",
            level: "DEBUG",
            file: Some("crates/omnigraph/src/engine/mod.rs"),
            line: Some(509),
            thread: Some("dst"),
            session: None,
            fields: Map::new(),
        };
        vec![
            Row::Start(start),
            Row::Operation,
            Row::Observation {
                text: "actual control: Ok(())",
            },
            Row::Evidence {
                record: "assertion",
                value: &Value::Null,
                session: None,
            },
            Row::SeamCrossing {
                seam: "branch_merge.post_authority_capture",
                decision: "fire",
                effect: Some("fail"),
                session: None,
            },
            Row::StoreRequest {
                verb: "get",
                class: "manifest_meta",
                dataset: "__manifest",
                path: "__manifest/_versions/1.manifest",
                range: Some("0-4096"),
                bytes: 4096,
                session: "step",
            },
            Row::Span {
                id: 1,
                parent: None,
                site: site(),
            },
            Row::SpanRecord {
                id: 1,
                fields: Map::new(),
            },
            Row::SpanClose { id: 1 },
            Row::Event {
                parent: Some(1),
                site: site(),
            },
            Row::Finish {
                code: "passed",
                phase: "teardown",
            },
            Row::Truncated {
                reason: "trace limit",
                max_bytes: MAX_BYTES,
                max_records: MAX_RECORDS,
            },
        ]
    }

    #[test]
    fn every_kind_matches_the_schema() {
        let schema: Value = serde_json::from_str(SCHEMA).unwrap();
        let branches = schema["oneOf"].as_array().unwrap();
        let branch = |kind: &str| {
            branches
                .iter()
                .find(|branch| branch["properties"]["kind"]["const"] == kind)
                .unwrap_or_else(|| panic!("schema has no branch for kind {kind}"))
        };
        let case_path = Path::new("cases/example.gqt");
        let start = Start::test(case_path);
        let step = Step::deserialize(&step()).unwrap();
        let mut seen = Vec::new();
        for (index, record) in examples(&start).iter().enumerate() {
            let with_step = !matches!(record, Row::Start(_) | Row::Truncated { .. });
            let bytes = line_bytes(index as u64, record, with_step.then_some(&step), MAX_BYTES)
                .unwrap()
                .unwrap();
            let row: Value = serde_json::from_slice(&bytes).unwrap();
            let kind = row["kind"].as_str().unwrap().to_string();
            let branch = branch(&kind);
            assert_eq!(branch["additionalProperties"], false, "{kind}");
            let keys: Vec<&str> = row
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            for required in branch["required"].as_array().unwrap() {
                let required = required.as_str().unwrap();
                assert!(keys.contains(&required), "{kind} lacks required {required}");
            }
            for key in &keys {
                assert!(
                    branch["properties"].get(key).is_some(),
                    "{kind} column {key} is not in the schema"
                );
            }
            assert_eq!(row["step"].as_u64().is_some(), with_step, "{kind}");
            seen.push(kind);
        }
        let declared: Vec<&str> = branches
            .iter()
            .map(|branch| branch["properties"]["kind"]["const"].as_str().unwrap())
            .collect();
        assert_eq!(
            seen, declared,
            "the schema lists kinds the recorder never writes"
        );
    }

    #[test]
    fn operation_stamps_the_step_on_later_rows() {
        let mut writer = writer(tempfile::tempfile().unwrap(), 0, MAX_BYTES);
        writer.write(
            &Row::Observation {
                text: "lifetime: initialized generation 0",
            },
            None,
        );
        let five = Step::deserialize(&step()).unwrap();
        writer.write(&Row::Operation, Some(&five));
        writer.write(
            &Row::Observation {
                text: "actual control: Ok(())",
            },
            Some(&five),
        );
        let six = Step::deserialize(&json!({"ordinal": 6})).unwrap();
        writer.write(&Row::Operation, Some(&six));
        writer.write(
            &Row::Observation {
                text: "actual control: Ok(())",
            },
            Some(&six),
        );
        writer.finish("passed", "teardown", Some(&six)).unwrap();
        let records = records(&mut writer);
        assert!(records[0].get("step").is_none());
        assert_eq!(records[1]["kind"], "operation");
        assert_eq!(records[1]["step"], 5);
        assert_eq!(records[1]["step_line"], 33);
        assert_eq!(records[2]["step"], 5);
        assert_eq!(records[3]["step"], 6);
        assert_eq!(records[3]["step_line"], Value::Null);
        assert_eq!(records[4]["step"], 6);
        assert_eq!(records[5]["kind"], "finish");
        assert_eq!(records[5]["step"], 6);
    }

    #[test]
    fn limit_closes_recording_and_keeps_complete_json_lines() {
        let mut writer = writer(tempfile::tempfile().unwrap(), 0, 1024);
        writer.write(&Row::Observation { text: "small" }, None);
        writer.write(
            &Row::Observation {
                text: &"x".repeat(2048),
            },
            None,
        );
        let bytes = writer.bytes;
        writer.write(
            &Row::Observation {
                text: "after limit",
            },
            None,
        );
        writer.finish("passed", "teardown", None).unwrap();
        assert_eq!(writer.bytes, bytes);
        assert!(writer.bytes <= writer.limit);
        let records = records(&mut writer);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["kind"], "truncated");
    }

    #[test]
    fn write_failure_survives_until_finish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readonly");
        std::fs::write(&path, "").unwrap();
        let mut writer = writer(File::open(path).unwrap(), 0, MAX_BYTES);
        writer.write(&Row::Operation, None);
        assert!(
            writer
                .finish("passed", "teardown", None)
                .unwrap_err()
                .starts_with("report_failed: write trace:")
        );
    }

    #[test]
    fn serialization_stops_before_visiting_an_oversized_sequence() {
        struct Sequence<'a>(&'a Cell<usize>);
        impl Serialize for Sequence<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut sequence = serializer.serialize_seq(Some(1_000_000))?;
                for _ in 0..1_000_000 {
                    self.0.set(self.0.get() + 1);
                    sequence.serialize_element(&0_u64)?;
                }
                sequence.end()
            }
        }
        #[derive(Serialize)]
        struct Row<'a> {
            kind: &'static str,
            items: Sequence<'a>,
        }
        let visited = Cell::new(0);
        let row = Row {
            kind: "evidence",
            items: Sequence(&visited),
        };
        assert!(line_bytes(0, &row, None, 128).unwrap().is_none());
        assert!(visited.get() < 64, "visited {} elements", visited.get());
    }

    #[test]
    fn oversized_header_creates_no_trace() {
        let root = tempfile::tempdir().unwrap();
        let huge = "x".repeat(MAX_BYTES);
        let error = create(root.path(), &Start::test(Path::new(&huge))).unwrap_err();
        assert_eq!(error, "report_failed: trace header exceeds byte limit");
        assert!(!root.path().join("trace").exists());
    }

    #[test]
    fn record_limit_reserves_the_last_record_for_truncation() {
        let mut writer = writer(tempfile::tempfile().unwrap(), MAX_RECORDS - 2, MAX_BYTES);
        let five = Step::deserialize(&step()).unwrap();
        writer.write(&Row::Operation, Some(&five));
        writer.write(&Row::Observation { text: "at limit" }, Some(&five));
        writer.write(
            &Row::Observation {
                text: "after limit",
            },
            Some(&five),
        );
        writer.finish("passed", "teardown", Some(&five)).unwrap();
        assert_eq!(writer.idx, MAX_RECORDS);
        let records = records(&mut writer);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["idx"], MAX_RECORDS - 1);
        assert_eq!(records[1]["kind"], "truncated");
        assert!(records[1].get("step").is_none());
        assert!(serde_json::to_vec(&records[1]).unwrap().len() < TERMINATOR_BYTES);
    }

    #[test]
    fn terminator_fits_whatever_the_step_carries() {
        let mut writer = writer(tempfile::tempfile().unwrap(), 1, 1024);
        let binding = json!(["v".repeat(2000), 7]);
        let wide = Step::deserialize(&json!({"ordinal": 5, "loop_binding": binding})).unwrap();
        writer.write(&Row::Observation { text: "x" }, Some(&wide));
        writer.finish("passed", "teardown", Some(&wide)).unwrap();
        assert!(writer.bytes <= writer.limit);
        let records = records(&mut writer);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["kind"], "truncated");
        assert!(records[0].get("loop_binding").is_none());
    }

    #[test]
    fn field_formatting_is_bounded_and_marks_incomplete_values() {
        struct Repeated<'a>(&'a Cell<usize>);
        impl std::fmt::Debug for Repeated<'_> {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for _ in 0..1_000_000 {
                    self.0.set(self.0.get() + 1);
                    formatter.write_str("0123456789abcdef")?;
                }
                Ok(())
            }
        }
        let visited = Cell::new(0);
        let field = debug_value(&Repeated(&visited));
        assert_eq!(field["truncated"], true);
        assert_eq!(field["value"].as_str().unwrap().len(), MAX_FIELD_BYTES);
        assert!(visited.get() <= MAX_FIELD_BYTES / 16 + 1);
        let field = string_value(&"€".repeat(MAX_FIELD_BYTES));
        assert_eq!(field["truncated"], true);
        assert!(field["value"].as_str().unwrap().len() <= MAX_FIELD_BYTES);
        assert_eq!(string_value("small"), json!("small"));
    }

    #[test]
    fn closed_layer_does_not_format_event_fields() {
        struct RefuseFormat;
        impl std::fmt::Debug for RefuseFormat {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("closed recording formatted an event");
            }
        }
        let mut closed = writer(tempfile::tempfile().unwrap(), 0, MAX_BYTES);
        closed.closed = true;
        let subscriber =
            tracing_subscriber::registry().with(TraceLayer(Arc::new(Mutex::new(closed))));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "omnigraph::trace_test", value = ?RefuseFormat);
        });
    }
}
