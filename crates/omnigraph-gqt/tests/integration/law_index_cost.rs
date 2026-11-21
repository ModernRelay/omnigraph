//! Law 8: an index path is cheaper than the scan it replaces, counted in
//! object-store requests under the GQT runner's DST `--measure` ledger.
//!
//! Every read below runs under the in-memory object store with `--measure`,
//! one measured query per case so no earlier step warms a cache, and the
//! law counts the requests the step made to the one table it reads through
//! an index path. Each read's own `--- expect plan` claims that path (an
//! `access index_probe <column> <kind>` or `access id_lookup` line), so a
//! read the planner answers another way fails rather than passing unmeasured.
//!
//! - (i) With every declared index built (`# traversal: auto`), the step's
//!   requests to the table are fewer than the same step's under layout (a),
//!   which builds no index and reads the table by a scan.
//! - (ii) At [`N`] rows (eight fragments of 512) and at `8 * N` (64
//!   fragments) with the same matching rows, the index path's requests stay
//!   within [`FACTOR`] of each other, while the scan's grow at least
//!   [`SCAN_GROWTH`] times, which shows `N` is large enough for the scan to
//!   pay for the table's size.
//! - (iii) A traversal's destination read by `id_lookup` with no index on
//!   the identity column scans the table once per read, so its reads must
//!   not multiply with the frontier: at [`FRONTIER`] and four times as many
//!   frontier rows its requests stay within [`FACTOR`].
//!
//! The shapes are each index kind the planner probes: a key equality
//! (`__id` BTREE), a BTREE equality on an `I64`, a BITMAP equality on an
//! enum, an NGRAM `contains` over mixed-case text (its lowercased trigrams
//! and recheck), a LABEL_LIST membership, and a traversal's destination by
//! `id_lookup` over the `__id` BTREE. A predicate over a function of a
//! column (`contains(lower($x.name), …)`) takes the scan, so it has no
//! index path to hold. The generated part draws a shape, the number of
//! matching rows and where they sit in the table.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use omnigraph_compiler::catalog::schema_ir::{SchemaIdentityDomain, initialize_schema_ir};
use omnigraph_compiler::catalog::schema_shape::compile_schema_shape;
use omnigraph_compiler::schema::parser::parse_schema;
use proptest::test_runner::TestCaseError;

use super::law_engine_reference::law_runner;

/// Rows of the smaller table, in eight fragments; the larger holds `8 * N`
/// in 64.
const N: u64 = 4_096;
/// Rows per fragment.
const FRAGMENT_ROWS: u64 = 512;
/// How far an index path's requests may move from `N` to `8 * N`, and
/// a no-index lookup's from one frontier to four times it.
const FACTOR: u64 = 2;
/// How much a scan's requests must grow from `N` to `8 * N` for `N` to
/// be large enough to tell a flat index path from a scan.
const SCAN_GROWTH: u64 = 4;
/// The smaller frontier of (iii); the larger is four times it.
const FRONTIER: u64 = 1_024;

/// The per-pull-request generated count; the nightly test runs `NIGHTLY`.
const CASES: u32 = 4;
const NIGHTLY: u32 = 60;

/// One shape's read: what it declares, how its matching rows are written,
/// the query and the plan line its index path claims.
#[derive(Clone, Copy, Debug)]
enum Shape {
    Key,
    Btree,
    Bitmap,
    Ngram,
    LabelList,
    IdLookup,
}

impl Shape {
    const ALL: [Shape; 6] = [
        Shape::Key,
        Shape::Btree,
        Shape::Bitmap,
        Shape::Ngram,
        Shape::LabelList,
        Shape::IdLookup,
    ];

    /// The table the index path reads.
    fn table(self) -> &'static str {
        match self {
            Shape::IdLookup => "Holding",
            _ => "Item",
        }
    }

    fn schema(self) -> &'static str {
        match self {
            Shape::Key | Shape::Btree => {
                "node Item {\n    slug: String @key\n    n: I64 @index\n}\n"
            }
            Shape::Bitmap => {
                "node Item {\n    slug: String @key\n    status: enum(open, closed, blocked)? @index\n}\n"
            }
            Shape::Ngram => "node Item {\n    slug: String @key\n    text: String? @index\n}\n",
            Shape::LabelList => {
                "node Item {\n    slug: String @key\n    tags: [String]? @index\n}\n"
            }
            Shape::IdLookup => {
                "node Filing {\n    accession: String @key\n}\nnode Holding {\n    row_key: String @key\n    note: String\n}\nedge InFiling: Holding -> Filing\n"
            }
        }
    }

    /// The matching value and the value every other row holds.
    fn values(self) -> (&'static str, &'static str) {
        match self {
            Shape::Key | Shape::Btree => ("7", "-1"),
            Shape::Bitmap => ("blocked", "open"),
            Shape::Ngram => ("\"zq7 hello\"", "\"plain text\""),
            Shape::LabelList => ("[rust, db]", "[misc]"),
            Shape::IdLookup => unreachable!("the traversal writes its frontier by its edges"),
        }
    }

    fn query(self, first: u64) -> String {
        let read = |filter: &str, item: &str| {
            format!(
                "query q() {{\n    match {{ $x: Item }} where {filter}\n    return {{ {item} }}\n}}\n"
            )
        };
        match self {
            Shape::Key => read(&format!("$x.slug = \"{}\"", key("i", first)), "count($x) as n"),
            Shape::Btree => read("$x.n = 7", "count($x) as n"),
            Shape::Bitmap => read("$x.status = \"blocked\"", "count($x) as n"),
            Shape::Ngram => read("contains($x.text, \"zq7\")", "count($x) as n"),
            Shape::LabelList => read("\"rust\" in $x.tags", "count($x) as n"),
            Shape::IdLookup => "query q() {\n    match { $f: Filing, $h inFiling $f }\n    where $f.accession = \"f00000\" and $h.note <> \"\"\n    return { count($h) as n }\n}\n".to_string(),
        }
    }

    fn plan(self) -> &'static str {
        match self {
            Shape::Key => "scan Item as $x: access index_probe __id",
            Shape::Btree => "scan Item as $x: access index_probe n btree",
            Shape::Bitmap => "scan Item as $x: access index_probe status bitmap",
            Shape::Ngram => "scan Item as $x: access index_probe text ngram",
            Shape::LabelList => "scan Item as $x: access index_probe tags label_list",
            Shape::IdLookup => "scan Holding as $h: access id_lookup",
        }
    }
}

/// The key of row `ordinal` under the recipe's `key` column.
fn key(prefix: &str, ordinal: u64) -> String {
    format!("{prefix}{ordinal:05}")
}

/// One read to measure: `rows` in the read's table, `matching` rows from
/// `first` holding the matching value (the traversal's frontier for
/// `IdLookup`), with or without every index built.
#[derive(Clone, Copy, Debug)]
struct Read {
    shape: Shape,
    rows: u64,
    first: u64,
    matching: u64,
    indexed: bool,
    /// The query's pool, when the plan needs one to read by `id_lookup`.
    pool: Option<u64>,
    /// Whether every row but the matching ones holds one value, so a
    /// fragment's value range can rule it out (a numeric column only).
    clustered: bool,
}

impl Read {
    fn case(&self) -> String {
        let commits = self.rows.div_ceil(FRAGMENT_ROWS);
        let tables = match self.shape {
            Shape::IdLookup => format!(
                "  - kind: node\n    name: Filing\n    rows: 1\n    commits: 1\n    columns:\n      accession: {{kind: key, prefix: f, width: 5}}\n  - kind: node\n    name: Holding\n    rows: {rows}\n    commits: {commits}\n    columns:\n      row_key: {{kind: key, prefix: h, width: 5}}\n      note: {{kind: repeat, text: x, count: 400}}\n  - kind: edge\n    name: InFiling\n    rows: {m}\n    commits: 1\n    from: {{kind: key, prefix: h, width: 5}}\n    to: {{kind: literal, value: f00000}}\n    columns: {{}}\n",
                rows = self.rows,
                m = self.matching,
            ),
            shape => {
                let (hit, miss) = shape.values();
                let column = match shape {
                    Shape::Key | Shape::Btree => "n",
                    Shape::Bitmap => "status",
                    Shape::Ngram => "text",
                    _ => "tags",
                };
                format!(
                    "  - kind: node\n    name: Item\n    rows: {rows}\n    commits: {commits}\n    columns:\n      slug: {{kind: key, prefix: i, width: 5}}\n      {column}: {{kind: ranges, ranges: [{ranges}], fallback: {miss}}}\n",
                    rows = self.rows,
                    ranges = self.ranges(hit, miss),
                )
            }
        };
        let expected = match self.shape {
            Shape::Key => 1,
            _ => self.matching,
        };
        let setting = self
            .pool
            .map(|bytes| format!("set query_memory_bytes = {bytes};\n"))
            .unwrap_or_default();
        let plan = if self.indexed || matches!(self.shape, Shape::IdLookup) {
            self.shape.plan()
        } else {
            match self.shape {
                Shape::IdLookup => unreachable!(),
                _ => "scan Item as $x: access sequential",
            }
        };
        format!(
            "{traversal}# issue: none\n# notes: law 8 read\n\n--- runner\ntimeout_ms: 600000\nenvironments:\n  - target: omnigraph-engine-dst\n    storage: in-memory-object-store\n    seeds: [0]\n\n--- schema\n{schema}\n--- seed generate: v1 seed: 8\ntables:\n{tables}\n--- query\n{setting}{query}\n--- expect unordered\n{{\"n\":{expected}}}\n--- expect shape\nn: I64\n--- expect plan\n{plan}\n",
            traversal = if self.indexed {
                "# traversal: auto\n"
            } else {
                ""
            },
            schema = self.shape.schema(),
            query = self.shape.query(self.first),
        )
    }

    /// The matching rows' value from `first`, every other row's `miss`;
    /// unless `clustered`, each fragment's first two rows hold values on
    /// either side of the matching one, so no fragment's value range rules
    /// it out and a scan must read every fragment.
    fn ranges(&self, hit: &str, miss: &str) -> String {
        let value = |ordinal: u64| {
            let within = ordinal % FRAGMENT_ROWS;
            if (self.first..self.first + self.matching).contains(&ordinal) {
                hit
            } else if !self.clustered && within == 0 {
                "-1000"
            } else if !self.clustered && within == 1 {
                "1000"
            } else {
                miss
            }
        };
        let mut runs: Vec<(u64, &str)> = Vec::new();
        for ordinal in 0..self.rows {
            let v = value(ordinal);
            match runs.last_mut() {
                Some((end, last)) if *last == v => *end = ordinal + 1,
                _ => runs.push((ordinal + 1, v)),
            }
        }
        runs.iter()
            .map(|(end, v)| format!("{{end: {end}, value: {v}}}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The step's requests to the read's table.
    fn requests(&self) -> Result<u64, String> {
        Ok(self.cost()?.0)
    }

    /// The step's requests to the read's table and the bytes they read.
    fn cost(&self) -> Result<(u64, u64), String> {
        let dataset = dataset(self.shape.schema(), self.shape.table());
        let steps = measure(&self.case())?;
        Ok(steps
            .first()
            .and_then(|step| step.get(&dataset).copied())
            .unwrap_or((0, 0)))
    }
}

/// The directory under the graph root of `table`'s dataset: its stable
/// identities, which a new graph allocates in one canonical sequence.
fn dataset(schema: &str, table: &str) -> String {
    let parsed = parse_schema(schema).expect("the law's schema parses");
    let shape = compile_schema_shape(&parsed).expect("the law's schema compiles");
    let domain = SchemaIdentityDomain::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let ir = initialize_schema_ir(domain, &shape).unwrap().schema_ir;
    let node = ir
        .nodes
        .iter()
        .find(|node| node.name == table)
        .expect("the measured table is a node type");
    format!(
        "{:016x}-{:016x}",
        node.type_id.get(),
        node.table_incarnation_id.get()
    )
}

/// One measured step's requests per dataset directory and the bytes they read.
type StepCost = BTreeMap<String, (u64, u64)>;

/// Run `case` under `--measure`; for each measured step, its requests per
/// dataset directory and the bytes they read.
fn measure(case: &str) -> Result<Vec<StepCost>, String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let path = dir.path().join("law_index_cost.gqt");
    std::fs::write(&path, case).map_err(|e| e.to_string())?;
    let output = Command::new(env!("CARGO_BIN_EXE_omnigraph-gqt"))
        .arg(&path)
        .arg("--measure")
        .arg("--artifacts")
        .arg(dir.path())
        .output()
        .map_err(|e| e.to_string())?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        let tail: Vec<&str> = stdout.lines().filter(|l| !l.starts_with('|')).collect();
        return Err(format!(
            "the measured read failed:\n{}\n{}\n{case}",
            tail.join("\n"),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let report = stdout
        .lines()
        .find_map(|line| line.strip_prefix("GQT report: "))
        .ok_or("no report path")?;
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(report).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    let groups = summary["attempts"][0]["outcome"]["Ok"]["measurements"]
        .as_array()
        .ok_or("the first attempt is not measured")?;
    let mut steps = Vec::new();
    for group in groups.iter().filter(|g| g["slot"] == "step") {
        let mut by_dataset = BTreeMap::new();
        for request in group["value"]["log"].as_array().into_iter().flatten() {
            let dataset = request["dataset"].as_str().unwrap_or_default().to_string();
            let entry = by_dataset.entry(dataset).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += request["bytes"].as_u64().unwrap_or(0);
        }
        steps.push(by_dataset);
    }
    Ok(steps)
}

/// The query pool under which the traversal's destination is read by
/// `id_lookup` rather than a hash join at both table sizes.
const LOOKUP_POOL: u64 = 4 * 1024 * 1024;

/// Parts (i) and (ii) for one shape, `matching` rows from `first`.
fn holds(shape: Shape, first: u64, matching: u64) -> Result<String, String> {
    let pool = matches!(shape, Shape::IdLookup).then_some(LOOKUP_POOL);
    let at = |rows: u64, indexed: bool| {
        Read {
            shape,
            rows,
            first: first.min(rows - matching),
            matching,
            indexed,
            pool,
            clustered: !matches!(shape, Shape::Btree),
        }
        .requests()
    };
    let (index_small, scan_small) = (at(N, true)?, at(N, false)?);
    let (index_large, scan_large) = (at(8 * N, true)?, at(8 * N, false)?);
    let numbers = format!(
        "{shape:?} matching {matching} from {first}: index {index_small} -> {index_large}, \
         without the index {scan_small} -> {scan_large}"
    );
    if index_small >= scan_small || index_large >= scan_large {
        return Err(format!("(i) the index path is not cheaper: {numbers}"));
    }
    if index_large > FACTOR * index_small.max(1) {
        return Err(format!(
            "(ii) the index path grows with the table: {numbers}"
        ));
    }
    if !matches!(shape, Shape::IdLookup) && scan_large < SCAN_GROWTH * scan_small {
        return Err(format!(
            "(ii) the scan does not grow {SCAN_GROWTH} times, so N is too small: {numbers}"
        ));
    }
    Ok(numbers)
}

/// Every index path a corpus step claims (`access index_probe <column>[
/// <kind>]`, `access id_lookup`, `ran id_lookup`) is one of the law's
/// shapes. The corpus's own steps are not measured: each seed loads in one
/// commit, one fragment, which no scan pays more for than an index probe.
#[test]
fn every_index_path_the_corpus_claims_is_a_shape_of_the_law() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases");
    let mut files = vec![root];
    let mut claims = BTreeMap::new();
    while let Some(path) = files.pop() {
        if path.is_dir() {
            files.extend(std::fs::read_dir(&path).unwrap().map(|e| e.unwrap().path()));
            continue;
        }
        if path.extension().is_none_or(|e| e != "gqt") {
            continue;
        }
        for line in std::fs::read_to_string(&path).unwrap().lines() {
            let shape = if line.contains("access id_lookup") || line.contains("ran id_lookup") {
                Some(Shape::IdLookup)
            } else if let Some((_, probe)) = line.split_once("access index_probe ") {
                let words: Vec<&str> = probe.split_whitespace().collect();
                Some(match (words.first().copied(), words.get(1).copied()) {
                    (Some("__id"), _) => Shape::Key,
                    (_, None | Some("btree")) => Shape::Btree,
                    (_, Some("bitmap")) => Shape::Bitmap,
                    (_, Some("ngram")) => Shape::Ngram,
                    (_, Some("label_list")) => Shape::LabelList,
                    (_, Some(kind)) => panic!(
                        "{}: an index kind the law has no shape for: {kind}",
                        path.display()
                    ),
                })
            } else {
                None
            };
            if let Some(shape) = shape {
                *claims.entry(format!("{shape:?}")).or_insert(0) += 1;
            }
        }
    }
    let every: Vec<String> = Shape::ALL.iter().map(|s| format!("{s:?}")).collect();
    assert_eq!(
        claims.keys().cloned().collect::<Vec<_>>().len(),
        every.len(),
        "{claims:?}"
    );
}

/// Every index kind once, one matching row at the table's start.
#[cfg(tokio_unstable)]
#[test]
fn every_index_kind_reads_fewer_requests_than_its_scan() {
    let failures: Vec<String> = Shape::ALL
        .into_iter()
        .filter_map(|shape| match holds(shape, 0, 1) {
            Ok(numbers) => {
                eprintln!("{numbers}");
                None
            }
            Err(failure) => Some(failure),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn run_generated(cases: u32) {
    let mut runner = law_runner(cases, "tests/regressions/law_index_cost.txt");
    let strategy = (0..Shape::ALL.len(), 1u64..=16, 0u64..N);
    runner
        .run(&strategy, |(shape, matching, first)| {
            holds(Shape::ALL[shape], first, matching)
                .map(|_| ())
                .map_err(TestCaseError::fail)
        })
        .unwrap();
}

#[cfg(tokio_unstable)]
#[test]
fn generated_index_paths_are_cheaper_than_their_scans() {
    run_generated(CASES);
}

#[cfg(tokio_unstable)]
#[test]
#[ignore = "nightly: law 8 over generated index-path reads at its nightly count"]
fn generated_index_paths_are_cheaper_than_their_scans_at_nightly_breadth() {
    run_generated(NIGHTLY);
}

/// (iii): a destination with no `__id` index, read by `id_lookup` under a
/// pool too small for the hash join's build, at 65,536 rows in sixteen
/// fragments: its requests do not multiply with the frontier.
#[cfg(tokio_unstable)]
#[test]
fn an_id_lookup_without_an_index_does_not_multiply_its_reads_with_the_frontier_issue_1432() {
    let at = |frontier: u64| {
        Read {
            shape: Shape::IdLookup,
            rows: 16 * 4_096,
            first: 0,
            matching: frontier,
            indexed: false,
            pool: Some(64 * 1024 * 1024),
            clustered: true,
        }
        .requests()
        .unwrap_or_else(|failure| panic!("{failure}"))
    };
    let (small, large) = (at(FRONTIER), at(4 * FRONTIER));
    eprintln!(
        "id_lookup without an index: {small} at {FRONTIER} ids, {large} at {} ids",
        4 * FRONTIER
    );
    assert!(
        large <= FACTOR * small,
        "the lookup's requests grow with the frontier: {small} at {FRONTIER} ids, \
         {large} at {} ids",
        4 * FRONTIER
    );
}

/// (i) over clustered data: every row but the matching one holds one value,
/// so a scan skips each fragment whose value range rules the equality out.
/// Measured 2026-11-21 (seed 0, one matching row): at 4,096 rows the BTREE
/// probe takes 7 requests and 22,163 bytes against the scan's 5 and 12,306;
/// at 32,768 rows the probe takes 12 and 39,601, the scan still 5 and
/// 12,306. The index path is chosen by rule, not by what each path reads.
/// Preferring the pruned scan is a choice by the data's value ranges, which
/// the planner does not read: stage S5 prices both paths from statistics an
/// explain records (each fragment's value range, the index's depth), never
/// a rule that reads them silently.
#[cfg(tokio_unstable)]
#[test]
#[ignore = "hunt: a BTREE equality on clustered I64 data costs more requests than the scan whose fragment value ranges prune it"]
fn a_btree_equality_on_clustered_data_costs_more_than_the_pruned_scan() {
    let at = |rows: u64, indexed: bool| {
        Read {
            shape: Shape::Btree,
            rows,
            first: 0,
            matching: 1,
            indexed,
            pool: None,
            clustered: true,
        }
        .cost()
        .unwrap_or_else(|failure| panic!("{failure}"))
    };
    let mut costs = Vec::new();
    for rows in [N, 8 * N] {
        let (index, scan) = (at(rows, true), at(rows, false));
        eprintln!(
            "{rows} rows: index {} requests, {} bytes; scan {} requests, {} bytes",
            index.0, index.1, scan.0, scan.1
        );
        costs.push((rows, index, scan));
    }
    for (rows, index, scan) in costs {
        assert!(
            index.0 < scan.0,
            "{rows} rows: index {index:?}, scan {scan:?}"
        );
    }
}
