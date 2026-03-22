#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use criterion::Criterion;
use nanograph::query_input::find_named_query;
use nanograph::store::database::{Database, PreparedReadQuery};
use tokio::runtime::{Builder, Runtime};

const LOOKUP_SCALES_DEFAULT: [(&str, usize); 2] = [("small", 10_000), ("medium", 100_000)];
const LOOKUP_SCALES_SMOKE: [(&str, usize); 1] = [("small", 10_000)];

static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

pub fn criterion_config() -> Criterion {
    if smoke_mode() {
        Criterion::default()
            .sample_size(10)
            .warm_up_time(Duration::from_millis(250))
            .measurement_time(Duration::from_secs(1))
    } else {
        Criterion::default()
            .sample_size(10)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(4))
    }
}

pub fn smoke_mode() -> bool {
    matches!(
        std::env::var("NANOGRAPH_BENCH_SMOKE"),
        Ok(value) if value == "1" || value.eq_ignore_ascii_case("true")
    )
}

pub fn lookup_scales() -> &'static [(&'static str, usize)] {
    if smoke_mode() {
        &LOOKUP_SCALES_SMOKE
    } else {
        &LOOKUP_SCALES_DEFAULT
    }
}

pub fn traversal_scale() -> (&'static str, usize, usize) {
    if smoke_mode() {
        ("small", 2_000, 10_000)
    } else {
        ("medium", 10_000, 50_000)
    }
}

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

pub fn example_path(example: &str, file: &str) -> PathBuf {
    repo_root().join("examples").join(example).join(file)
}

pub fn read_example_file(example: &str, file: &str) -> String {
    fs::read_to_string(example_path(example, file)).expect("read example file")
}

pub fn bench_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(if smoke_mode() { 2 } else { 4 })
        .enable_all()
        .build()
        .expect("bench runtime")
}

pub struct MockEmbeddingsGuard {
    previous_mock: Option<String>,
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl Drop for MockEmbeddingsGuard {
    fn drop(&mut self) {
        match &self.previous_mock {
            Some(value) => unsafe { std::env::set_var("NANOGRAPH_EMBEDDINGS_MOCK", value) },
            None => unsafe { std::env::remove_var("NANOGRAPH_EMBEDDINGS_MOCK") },
        }
    }
}

pub fn enable_mock_embeddings() -> MockEmbeddingsGuard {
    let guard = ENV_MUTEX
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("embedding env mutex");
    let previous_mock = std::env::var("NANOGRAPH_EMBEDDINGS_MOCK").ok();
    unsafe {
        std::env::set_var("NANOGRAPH_EMBEDDINGS_MOCK", "1");
    }
    MockEmbeddingsGuard {
        previous_mock,
        _guard: guard,
    }
}

pub fn load_example_db(runtime: &Runtime, example: &str) -> (Database, String) {
    let schema = read_example_file(example, &format!("{example}.pg"));
    let query_source = read_example_file(example, &format!("{example}.gq"));
    let data_path = example_path(example, &format!("{example}.jsonl"));
    let db = runtime
        .block_on(Database::open_in_memory(&schema))
        .expect("open example db");
    let _mock = enable_mock_embeddings();
    runtime
        .block_on(db.load_file(&data_path))
        .expect("load example data");
    (db, query_source)
}

pub fn prepare_named_query(
    db: &Database,
    query_source: &str,
    query_name: &str,
) -> PreparedReadQuery {
    let query = find_named_query(query_source, query_name).expect("named query");
    db.prepare_read_query(&query).expect("prepare read query")
}

pub fn keyed_people_schema(indexed_email: bool) -> &'static str {
    if indexed_email {
        r#"
node Person {
    slug: String @key
    name: String
    email: String @index
    team: String
    joinedAt: Date
}
"#
    } else {
        r#"
node Person {
    slug: String @key
    name: String
    email: String
    team: String
    joinedAt: Date
}
"#
    }
}

pub fn build_people_rows(start: usize, count: usize) -> String {
    let mut data = String::with_capacity(count.saturating_mul(160));
    for i in start..start + count {
        data.push_str(&format!(
            r#"{{"type":"Person","data":{{"slug":"user_{:06}","name":"User {:06}","email":"user_{:06}@example.com","team":"team_{:02}","joinedAt":"2024-01-{:02}"}}}}"#,
            i,
            i,
            i,
            i % 16,
            (i % 28) + 1
        ));
        data.push('\n');
    }
    data
}

pub fn build_people_merge_payload(
    update_count: usize,
    insert_start: usize,
    insert_count: usize,
) -> String {
    let mut data = String::with_capacity((update_count + insert_count).saturating_mul(180));
    for i in 0..update_count {
        data.push_str(&format!(
            r#"{{"type":"Person","data":{{"slug":"user_{:06}","name":"Updated User {:06}","email":"user_{:06}@example.com","team":"team_{:02}","joinedAt":"2024-02-{:02}"}}}}"#,
            i,
            i,
            i,
            (i + 3) % 16,
            (i % 28) + 1
        ));
        data.push('\n');
    }
    data.push_str(&build_people_rows(insert_start, insert_count));
    data
}

pub fn social_graph_schema() -> &'static str {
    r#"
node Person {
    slug: String @key
    name: String
    team: String
}

edge Knows: Person -> Person
"#
}

pub fn build_social_graph_data(nodes: usize, out_degree: usize) -> String {
    let mut data = String::with_capacity(nodes.saturating_mul(64) + nodes * out_degree * 56);
    for i in 0..nodes {
        data.push_str(&format!(
            r#"{{"type":"Person","data":{{"slug":"user_{:06}","name":"User {:06}","team":"team_{:02}"}}}}"#,
            i,
            i,
            i % 16
        ));
        data.push('\n');
    }
    for i in 0..nodes {
        for offset in 1..=out_degree {
            let dst = (i + offset) % nodes;
            data.push_str(&format!(
                r#"{{"edge":"knows","from":"user_{:06}","to":"user_{:06}"}}"#,
                i, dst
            ));
            data.push('\n');
        }
    }
    data
}
