//! Law 7: the reference interpreter holds the laws the GQ 3 RFC states of
//! its own core (section 2's operator table and the envelope; section 10's
//! reference-interpreter bullet), over random terms.
//!
//! - **Totality.** Every generated read that type-checks evaluates to rows
//!   or to a run-time or input refusal; it never panics and never answers
//!   that it does not evaluate the form.
//! - **Bounds.** Every evaluated term is audited against its operator's row:
//!   `Scan` returns `|T|` rows, `Filter` a subsequence of its input,
//!   `Project` and `Order` as many rows as their input, `Group` at most its
//!   input's (exactly one with no keys) with distinct key tuples,
//!   `Correlate` the sum of its inner evaluations, one per outer row,
//!   `Union` both arms in order, `Expand` at most `|R| · |T′|` (with an edge
//!   variable, `|R| · deg`), `Score` at most its input, `ApproxTopK` at most
//!   `k` targets and `Fuse` one row per distinct target, at most the arms'
//!   rows.
//! - **Total order.** `Order` returns a permutation of its input sorted by
//!   its keys and then the tie rule, two rows that compare equal are equal
//!   in every column, and sorting the result again from another input order
//!   gives the same rows; `Cut` keeps the first `k` rows of its ordered
//!   input, or of its input in the tie rule's order when it is unordered.
//! - **Typing.** Every value of a read's result holds its column's declared
//!   type and nullability; a `union` is accepted exactly when its arms have
//!   the same column names and base types agree per name, and its column
//!   types are the arms' join, a column nullable when either arm's is.
//! - **Fixpoint agreement.** A recursion's least fixpoint by the
//!   interpreter's naive iteration equals a semi-naive iteration (each round
//!   reads only the rows the last added) and a per-node closure; it is a
//!   fixpoint (one more round adds nothing) within the domain bound.
//!
//! Desugaring equality and the inner-cut rule are the compiler's lowering
//! and the planner's, which the GQT corpus and the placement law own.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use omnigraph_compiler::arbitrary::{
    Choices, GenGraph, GenRead, GenSchema, PropTy, ReadOptions, Sc, SchemaOptions, choices,
};
use omnigraph_compiler::catalog::build_catalog;
use omnigraph_compiler::ir::core::CoreDecl;
use omnigraph_compiler::query::check::check_source;
use omnigraph_compiler::schema::parser::parse_schema;
use proptest::test_runner::{
    Config, FileFailurePersistence, RngAlgorithm, TestCaseError, TestRng, TestRunner,
};

use super::*;

/// The per-pull-request case counts; the nightly tests run [`NIGHTLY`], the
/// count the RFC's evidence gives for random terms.
const READS: u32 = 2_000;
const UNIONS: u32 = 1_000;
const RECURSIONS: u32 = 1_000;
const NIGHTLY: u32 = 50_000;

/// A law's runner: `cases` cases (`OMNIGRAPH_LAW_CASES` overrides it for a
/// local hunt) from the deterministic seed, or from `OMNIGRAPH_LAW_SEED`
/// when set; a failing case is persisted to `regressions` and replayed
/// first by every later run.
fn law_runner(cases: u32, regressions: &'static str) -> TestRunner {
    let cases = std::env::var("OMNIGRAPH_LAW_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(cases);
    let config = Config {
        cases,
        max_shrink_iters: 4_000,
        max_shrink_time: 120_000,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(regressions))),
        ..Config::default()
    };
    let rng = match std::env::var("OMNIGRAPH_LAW_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&seed.to_le_bytes());
            TestRng::from_seed(RngAlgorithm::ChaCha, &bytes)
        }
        None => TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    };
    TestRunner::new_with_rng(config, rng)
}

// ------------------------------------------------------------------ audit

/// What the audit saw: the relations each open term's children returned,
/// the violations, and how many terms of each operator it checked.
#[derive(Debug, Default)]
pub(super) struct Audit {
    frames: Vec<Vec<Rel>>,
    violations: Vec<String>,
    checked: BTreeMap<&'static str, usize>,
}

impl Audit {
    fn new() -> Self {
        Self {
            frames: vec![Vec::new()],
            ..Self::default()
        }
    }
}

/// Evaluate `t`, then check its result against its operator's laws.
pub(super) fn audited(it: &mut Interpreter, t: &Term, env: &Row) -> R<Rel> {
    it.audit.as_mut().unwrap().frames.push(Vec::new());
    let out = it.eval(t, env);
    let children = it.audit.as_mut().unwrap().frames.pop().unwrap();
    if let Ok(rel) = &out {
        let violations = check(it, t, env, &children, rel);
        let audit = it.audit.as_mut().unwrap();
        let operator = operator(t);
        *audit.checked.entry(operator).or_default() += 1;
        audit
            .violations
            .extend(violations.into_iter().map(|v| format!("{operator}: {v}")));
        if let Some(parent) = audit.frames.last_mut() {
            parent.push(rel.clone());
        }
    }
    out
}

fn operator(t: &Term) -> &'static str {
    match t {
        Term::Unit => "Unit",
        Term::Env(_) => "Env",
        Term::Scan { .. } => "Scan",
        Term::Expand { .. } => "Expand",
        Term::Filter { .. } => "Filter",
        Term::Project { .. } => "Project",
        Term::Group { .. } => "Group",
        Term::Correlate { .. } => "Correlate",
        Term::Union { .. } => "Union",
        Term::Order { .. } => "Order",
        Term::Cut { .. } => "Cut",
        Term::Score { .. } => "Score",
        Term::ApproxTopK { .. } => "ApproxTopK",
        Term::Fuse { .. } => "Fuse",
        Term::Fix { .. } => "Fix",
        Term::RecRef { .. } => "RecRef",
        Term::Call { .. } => "Call",
    }
}

/// `out` is `input` with some rows left out, in input order.
fn subsequence(out: &[Row], input: &[Row]) -> bool {
    let mut at = 0;
    out.iter().all(|row| {
        while at < input.len() && !same_row(&input[at], row) {
            at += 1;
        }
        let found = at < input.len();
        at += 1;
        found
    })
}

/// `a` and `b` hold the same rows, each as often.
fn same_bag(a: &[Row], b: &[Row]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut used = vec![false; b.len()];
    a.iter().all(
        |row| match (0..b.len()).find(|&i| !used[i] && same_row(row, &b[i])) {
            Some(i) => {
                used[i] = true;
                true
            }
            None => false,
        },
    )
}

fn same_rows(a: &[Row], b: &[Row]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(p, q)| same_row(p, q))
}

fn distinct_values(values: &[Value]) -> bool {
    values
        .iter()
        .enumerate()
        .all(|(i, v)| values[i + 1..].iter().all(|w| !identical(v, w)))
}

/// `Order`'s comparator over two rows of `keys`, as `order_rows` sorts.
fn order_cmp(it: &Interpreter, keys: &[IROrdering], env: &Row, a: &Row, b: &Row) -> Ordering {
    for key in keys {
        let (Ok(x), Ok(y)) = (it.expr(&key.expr, a, env), it.expr(&key.expr, b, env)) else {
            return Ordering::Equal;
        };
        let o = cmp_key(&x, &y, key.descending, key.places_nulls_first());
        if o != Ordering::Equal {
            return o;
        }
    }
    tie(a, b)
}

/// Every reordering of `rows` the total-order law sorts again.
fn reorderings(rows: &[Row]) -> Vec<Vec<Row>> {
    let mut reversed = rows.to_vec();
    reversed.reverse();
    let mut rotated = rows.to_vec();
    if !rotated.is_empty() {
        rotated.rotate_left(1);
    }
    let mut interleaved: Vec<Row> = rows.iter().skip(1).step_by(2).cloned().collect();
    interleaved.extend(rows.iter().step_by(2).cloned());
    vec![reversed, rotated, interleaved]
}

/// The total-order law over `sorted`, which `cmp` claims to have sorted.
fn total_order(sorted: &[Row], cmp: &dyn Fn(&Row, &Row) -> Ordering) -> Vec<String> {
    let mut out = Vec::new();
    for pair in sorted.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        let o = cmp(a, b);
        if o == Ordering::Greater {
            out.push(format!("rows out of order: {a:?} before {b:?}"));
        }
        if cmp(b, a) != o.reverse() {
            out.push(format!("the comparison is not antisymmetric: {a:?}, {b:?}"));
        }
        if o == Ordering::Equal && !same_row(a, b) {
            out.push(format!(
                "two rows tie under the keys and the tie rule but differ: {a:?}, {b:?}"
            ));
        }
    }
    out
}

fn check(it: &Interpreter, t: &Term, env: &Row, children: &[Rel], out: &Rel) -> Vec<String> {
    let mut v = Vec::new();
    let n = out.rows.len();
    let child = |i: usize| children.get(i);
    match t {
        Term::Unit | Term::Env(_) => {
            if n != 1 {
                v.push(format!("{n} rows, not one"));
            }
        }
        Term::Scan { ty, .. } => {
            let (edge, type_name) = match ty {
                Ty::Node { type_name, .. } => (false, type_name),
                Ty::Edge { type_name, .. } => (true, type_name),
                Ty::Value(_) => return v,
            };
            let size: usize = type_name
                .split('|')
                .map(|t| {
                    let types = if edge {
                        &it.graph.edges
                    } else {
                        &it.graph.nodes
                    };
                    types.get(t).map_or(0, Vec::len)
                })
                .sum();
            if n != size {
                v.push(format!("{n} rows of a type holding {size}"));
            }
        }
        Term::Expand {
            edges, edge_var, ..
        } => {
            let Some(input) = child(0) else {
                return v;
            };
            let r = input.rows.len();
            let bound = match edge_var {
                Some(_) => {
                    // `deg`: the most selector edges at one node.
                    let mut degree: HashMap<&str, usize> = HashMap::new();
                    for m in edges.members() {
                        for e in it.graph.edges.get(&m.edge_type).into_iter().flatten() {
                            let (a, b) = e.ends.as_ref().unwrap();
                            *degree.entry(a).or_default() += 1;
                            if a != b {
                                *degree.entry(b).or_default() += 1;
                            }
                        }
                    }
                    r * degree.values().copied().max().unwrap_or(0)
                }
                None => r * it.graph.nodes.values().map(Vec::len).sum::<usize>(),
            };
            if n > bound {
                v.push(format!(
                    "{n} rows over {r} input rows, past the bound {bound}"
                ));
            }
        }
        Term::Filter { .. } => {
            if let [input] = children {
                if !subsequence(&out.rows, &input.rows) {
                    v.push("the rows are not a subsequence of the input".into());
                }
                if out.ordered != input.ordered {
                    v.push("the order flag moved".into());
                }
            }
        }
        Term::Project { .. } => {
            if let Some(input) = child(0) {
                if n != input.rows.len() {
                    v.push(format!("{n} rows from {}", input.rows.len()));
                }
                if out.ordered != input.ordered {
                    v.push("the order flag moved".into());
                }
            }
        }
        Term::Group { keys, .. } => {
            if let Some(input) = child(0) {
                if keys.is_empty() && n != 1 {
                    v.push(format!("{n} rows with no keys"));
                }
                if !keys.is_empty() && n > input.rows.len() {
                    v.push(format!("{n} groups from {} rows", input.rows.len()));
                }
                let tuples: Vec<Value> = out
                    .rows
                    .iter()
                    .map(|row| {
                        Value::List(
                            keys.iter()
                                .map(|(k, _)| get(row, k).cloned().unwrap_or(Value::Null))
                                .collect(),
                        )
                    })
                    .collect();
                if !distinct_values(&tuples) {
                    v.push("two groups share a key tuple".into());
                }
            }
        }
        Term::Correlate { .. } => {
            if let Some((outer, inner)) = children.split_first() {
                if inner.len() != outer.rows.len() {
                    v.push(format!(
                        "{} inner evaluations for {} outer rows",
                        inner.len(),
                        outer.rows.len()
                    ));
                }
                let sum: usize = inner.iter().map(|r| r.rows.len()).sum();
                if n != sum {
                    v.push(format!("{n} rows, not the inner sum {sum}"));
                }
            }
        }
        Term::Union { .. } => {
            if let [l, r] = children {
                if n != l.rows.len() + r.rows.len() {
                    v.push(format!(
                        "{n} rows from {} and {}",
                        l.rows.len(),
                        r.rows.len()
                    ));
                } else if !same_rows(&out.rows[..l.rows.len()], &l.rows) {
                    v.push("the left arm's rows are not first, in order".into());
                }
            }
        }
        Term::Order { keys, .. } => {
            let Some(input) = child(0) else {
                return v;
            };
            if !same_bag(&out.rows, &input.rows) {
                v.push("the rows are not a permutation of the input".into());
            }
            v.extend(total_order(&out.rows, &|a, b| {
                order_cmp(it, keys, env, a, b)
            }));
            for rows in reorderings(&out.rows) {
                match it.order_rows(rows, keys, env) {
                    Ok(again) if same_rows(&again, &out.rows) => {}
                    Ok(again) => v.push(format!(
                        "another input order sorts differently: {:?} against {:?}",
                        again, out.rows
                    )),
                    Err(e) => v.push(format!("sorting again refused: {e}")),
                }
            }
        }
        Term::Cut { k, .. } => {
            let Some(input) = child(0) else {
                return v;
            };
            let mut expected = input.rows.clone();
            if !input.ordered {
                expected.sort_by(tie);
                v.extend(total_order(&expected, &tie));
            }
            if let Ok(k) = it.count(k) {
                expected.truncate(k);
            }
            if !same_rows(&out.rows, &expected) {
                v.push("not the first k rows of its input's order".into());
            }
            if !out.ordered {
                v.push("a cut's output is unordered".into());
            }
        }
        Term::Score { .. } => {
            if let Some(input) = child(0)
                && n > input.rows.len()
            {
                v.push(format!("{n} rows from {}", input.rows.len()));
            }
        }
        Term::ApproxTopK { column, k, .. } => {
            let mut targets: Vec<Value> = Vec::new();
            for row in &out.rows {
                let target = get(row, column).cloned().unwrap_or(Value::Null);
                if !targets.iter().any(|t| identical(t, &target)) {
                    targets.push(target);
                }
            }
            if let Ok(k) = it.count(k)
                && targets.len() > k
            {
                v.push(format!("{} targets past the window {k}", targets.len()));
            }
            // Every row of a selected target survives.
            if let Some(input) = child(0) {
                let kept = input
                    .rows
                    .iter()
                    .filter(|row| {
                        get(row, column).is_some_and(|t| targets.iter().any(|s| identical(s, t)))
                    })
                    .count();
                if kept != n {
                    v.push(format!("{n} rows of {kept} the selected targets hold"));
                }
            }
        }
        Term::Fuse { target, .. } => {
            let targets: Vec<Value> = out
                .rows
                .iter()
                .map(|row| get(row, target).cloned().unwrap_or(Value::Null))
                .collect();
            if !distinct_values(&targets) {
                v.push("two rows share a target".into());
            }
            let arms: usize = children.iter().map(|r| r.rows.len()).sum();
            if n > arms {
                v.push(format!("{n} targets from {arms} arm rows"));
            }
        }
        Term::Fix { .. } => {
            for (i, row) in out.rows.iter().enumerate() {
                if out.rows[i + 1..].iter().any(|r| same_row(r, row)) {
                    v.push(format!("a row twice in a fixpoint over sets: {row:?}"));
                }
            }
        }
        Term::RecRef { .. } | Term::Call { .. } => {}
    }
    v
}

/// Whether `value` holds `ty`: its base type, and null only where `ty` is
/// nullable.
fn holds(value: &Value, ty: &Ty) -> bool {
    if matches!(value, Value::Null) {
        return ty.nullable();
    }
    match ty {
        Ty::Node { type_name, .. } | Ty::Edge { type_name, .. } => match value {
            Value::Ref {
                edge, type_name: t, ..
            } => *edge == matches!(ty, Ty::Edge { .. }) && type_name.split('|').any(|n| n == t),
            _ => false,
        },
        Ty::Value(p) => {
            let element = |v: &Value| match p.scalar {
                ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64 => {
                    matches!(v, Value::Int(_))
                }
                ScalarType::F32 | ScalarType::F64 => matches!(v, Value::Float(_)),
                ScalarType::Bool => matches!(v, Value::Bool(_)),
                ScalarType::String | ScalarType::Date | ScalarType::DateTime => match v {
                    Value::Str(s) => p
                        .enum_values
                        .as_ref()
                        .is_none_or(|members| members.contains(s)),
                    _ => false,
                },
                ScalarType::Vector(dim) => match v {
                    Value::List(items) => {
                        items.len() == dim as usize
                            && items.iter().all(|i| matches!(i, Value::Float(_)))
                    }
                    _ => false,
                },
                ScalarType::Blob => true,
            };
            match (p.list, value) {
                (true, Value::List(items)) => items.iter().all(element),
                (true, _) => false,
                (false, v) => element(v),
            }
        }
    }
}

// ------------------------------------------------------------------ reads

/// A generated sample: schema text, export and read.
#[derive(Debug)]
struct Sample {
    schema: String,
    export: String,
    source: String,
    params: Vec<(String, Literal)>,
}

impl Sample {
    fn describe(&self) -> String {
        format!(
            "--- schema\n{}\n--- seed\n{}\n--- query\n{}\n--- params\n{:?}",
            self.schema, self.export, self.source, self.params
        )
    }
}

/// The one read of `source`, or the checker's refusal.
fn checked(catalog: &Catalog, source: &str) -> Result<CoreRead, String> {
    let mut decls = check_source(catalog, source).map_err(|d| d.to_string())?;
    let (_, outcome) = decls.pop().ok_or("no declaration")?;
    match outcome
        .map_err(|d| format!("{}: {}", d.code.as_str(), d.message))?
        .core
    {
        CoreDecl::Read(read) => Ok(*read),
        _ => Err("not a read".into()),
    }
}

/// Run `read` under the audit; the rows and the audit, or why the law
/// fails.
fn run_audited(
    catalog: &Catalog,
    graph: &Graph,
    read: &CoreRead,
    params: &[(String, Literal)],
) -> Result<(R<Vec<Row>>, Audit), String> {
    let params: HashMap<String, Literal> = params.iter().cloned().collect();
    let mut it = Interpreter::new(catalog, graph, &params);
    it.audit = Some(Audit::new());
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| it.run(read)))
        .map_err(|_| "totality: the interpreter panicked".to_string())?;
    let mut audit = it.audit.take().unwrap();
    let rows = match outcome {
        Ok(_) => Ok(audit.frames[0].pop().map(|r| r.rows).unwrap_or_default()),
        Err(Refusal::Unsupported(what)) => {
            return Err(format!(
                "totality: a checked read the interpreter does not evaluate: {what}"
            ));
        }
        Err(e) => Err(e),
    };
    Ok((rows, audit))
}

/// The read laws over one sample: totality, every operator's laws, and the
/// result's typing.
fn read_laws(sample: &Sample) -> Result<Audit, String> {
    let catalog = build_catalog(&parse_schema(&sample.schema).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let graph = Graph::from_export(&catalog, &sample.export).map_err(|e| e.to_string())?;
    let read = checked(&catalog, &sample.source)?;
    let (rows, audit) = run_audited(&catalog, &graph, &read, &sample.params)?;
    let mut violations = audit.violations.clone();
    if let Ok(rows) = rows {
        for row in &rows {
            for col in &read.result.cols {
                let value = get(row, &col.name).cloned().unwrap_or(Value::Null);
                if !holds(&value, &col.ty) {
                    violations.push(format!(
                        "typing: column `{}` declared {} holds {value:?}",
                        col.name,
                        col.ty.display()
                    ));
                }
            }
        }
    }
    if violations.is_empty() {
        Ok(audit)
    } else {
        violations.dedup();
        Err(violations.join("\n"))
    }
}

fn generated_reads(cases: u32) {
    let mut runner = law_runner(cases, "tests/regressions/law_reference_reads.txt");
    let seen: std::cell::RefCell<BTreeMap<&'static str, usize>> = Default::default();
    let refused = std::cell::Cell::new(0usize);
    let result = runner.run(&(choices(64), choices(1024), choices(512)), |(s, g, q)| {
        let schema = GenSchema::generate(&mut Choices::new(&s), SchemaOptions::default());
        let graph = GenGraph::generate(&mut Choices::new(&g), &schema, 5, 8);
        let read = GenRead::generate(&mut Choices::new(&q), &schema, ReadOptions::default());
        let sample = Sample {
            schema: schema.text(),
            export: graph.jsonl(&schema),
            source: read.source,
            params: read.params,
        };
        let audit = read_laws(&sample)
            .map_err(|e| TestCaseError::fail(format!("{e}\n{}", sample.describe())))?;
        if audit.checked.is_empty() {
            refused.set(refused.get() + 1);
        }
        let mut seen = seen.borrow_mut();
        for (op, count) in audit.checked {
            *seen.entry(op).or_default() += count;
        }
        Ok(())
    });
    eprintln!("operators audited: {:?}", seen.borrow());
    if let Err(e) = result {
        panic!("{e}");
    }
    // The generator reaches every operator a read lowers to.
    let seen = seen.into_inner();
    let missing: Vec<&str> = [
        "Scan",
        "Expand",
        "Filter",
        "Project",
        "Group",
        "Correlate",
        "Union",
        "Order",
        "Cut",
        "Score",
        "ApproxTopK",
        "Fuse",
    ]
    .into_iter()
    .filter(|op| !seen.contains_key(op))
    .collect();
    assert!(missing.is_empty(), "no generated read reaches {missing:?}");
}

#[test]
fn generated_reads_hold_the_operator_laws() {
    generated_reads(READS);
}

#[test]
#[ignore = "nightly: law 7's operator laws over generated reads at the RFC's count"]
fn generated_reads_hold_the_operator_laws_at_nightly_breadth() {
    generated_reads(NIGHTLY);
}

// ------------------------------------------------------------------ union

/// One column of a union arm: its text and its type as the RFC states it.
fn arm_column(c: &mut Choices, schema: &GenSchema, node: usize, var: &str) -> (String, Ty) {
    let props: Vec<_> = schema.nodes[node]
        .props
        .iter()
        .filter(|p| !matches!(p.ty, PropTy::Vector(_)))
        .collect();
    match c.below(4) {
        0 => (
            var.to_string(),
            Ty::Node {
                type_name: schema.nodes[node].name.clone(),
                nullable: false,
            },
        ),
        1 if !props.is_empty() => {
            let p = props[c.below(props.len())];
            let (scalar, list, enum_values) = match &p.ty {
                PropTy::Scalar(sc) => (scalar_of(*sc), false, None),
                PropTy::Enum => (
                    ScalarType::String,
                    false,
                    // The catalog holds an enum's members sorted.
                    Some({
                        let mut members: Vec<String> = omnigraph_compiler::arbitrary::ENUM_MEMBERS
                            .iter()
                            .map(|m| m.to_string())
                            .collect();
                        members.sort();
                        members
                    }),
                ),
                PropTy::ListStr => (ScalarType::String, true, None),
                PropTy::FullText => (ScalarType::String, false, None),
                PropTy::Vector(_) => unreachable!("filtered"),
            };
            (
                format!("{var}.{}", p.name),
                Ty::Value(PropType {
                    scalar,
                    nullable: p.nullable,
                    list,
                    enum_values,
                }),
            )
        }
        _ => {
            let (text, scalar) = *c.pick(&[
                ("7", ScalarType::I64),
                ("1.5", ScalarType::F64),
                ("\"s\"", ScalarType::String),
                ("true", ScalarType::Bool),
            ]);
            (text.to_string(), Ty::scalar(scalar, false))
        }
    }
}

fn scalar_of(sc: Sc) -> ScalarType {
    match sc {
        Sc::I32 => ScalarType::I32,
        Sc::I64 => ScalarType::I64,
        Sc::U32 => ScalarType::U32,
        Sc::F32 => ScalarType::F32,
        Sc::F64 => ScalarType::F64,
        Sc::Bool => ScalarType::Bool,
        Sc::Str => ScalarType::String,
        Sc::Date => ScalarType::Date,
        Sc::DateTime => ScalarType::DateTime,
    }
}

/// A union of two arms whose columns are drawn by name; the RFC's verdict:
/// the joined columns, or why it is refused.
fn union_sample(c: &mut Choices, schema: &GenSchema) -> (String, Result<Vec<Col>, String>) {
    let names = ["c0", "c1", "c2"];
    let mut first: Vec<&str> = names.iter().copied().filter(|_| c.flag(60)).collect();
    if first.is_empty() {
        first.push("c0");
    }
    let mut second = first.clone();
    if second.len() > 1 && c.flag(15) {
        let drop = c.below(second.len());
        second.remove(drop);
    }
    if c.flag(15) {
        let add = names[c.below(names.len())];
        if !second.contains(&add) {
            second.push(add);
        }
    }
    if c.flag(30) {
        second.reverse();
    }
    let mut arms = Vec::new();
    // Each column's source in the first arm, which the second mostly
    // repeats over the same type, so most unions are accepted.
    let mut sources: Vec<(String, (String, Ty))> = Vec::new();
    let first_node = c.below(schema.nodes.len());
    for (i, cols) in [&first, &second].into_iter().enumerate() {
        let node = if i == 0 || c.flag(70) {
            first_node
        } else {
            c.below(schema.nodes.len())
        };
        let var = format!("$x{i}");
        let mut items = Vec::new();
        let mut types = Vec::new();
        for name in cols {
            let repeated = sources
                .iter()
                .find(|(n, _)| n == name)
                .filter(|_| node == first_node && c.flag(70))
                .map(|(_, (text, ty))| (text.replace("$x0", &var), ty.clone()));
            let (text, ty) = match repeated {
                Some(column) => column,
                None => arm_column(c, schema, node, &var),
            };
            if i == 0 {
                sources.push((name.to_string(), (text.clone(), ty.clone())));
            }
            items.push(format!("{text} as {name}"));
            types.push((name.to_string(), ty));
        }
        arms.push((
            format!(
                "{{ match {{ {var}: {} }} return {{ {} }} }}",
                schema.nodes[node].name,
                items.join(", ")
            ),
            types,
        ));
    }
    let returned: Vec<String> = first.iter().map(|n| format!("${n}")).collect();
    let source = format!(
        "query q() {{ match {{ from union({}, {}) }} return {{ {} }} }}\n",
        arms[0].0,
        arms[1].0,
        returned.join(", ")
    );
    let (left, right) = (&arms[0].1, &arms[1].1);
    let same_names =
        left.len() == right.len() && left.iter().all(|(n, _)| right.iter().any(|(m, _)| m == n));
    let verdict = if !same_names {
        Err("the arms' column names differ".to_string())
    } else {
        left.iter()
            .map(|(name, a)| {
                let (_, b) = right.iter().find(|(m, _)| m == name).unwrap();
                if a.same_base(b) {
                    // Nullable when either arm's is; an enum's members the
                    // union of both arms', none when either is plain.
                    let mut ty = a.with_nullable(a.nullable() || b.nullable());
                    if let (Ty::Value(p), Ty::Value(q)) = (&mut ty, b) {
                        p.enum_values = match (&p.enum_values, &q.enum_values) {
                            (Some(x), Some(y)) => {
                                let mut members: Vec<String> = x.iter().chain(y).cloned().collect();
                                members.sort();
                                members.dedup();
                                Some(members)
                            }
                            _ => None,
                        };
                    }
                    Ok(Col {
                        name: name.clone(),
                        ty,
                    })
                } else {
                    Err(format!(
                        "column `{name}` is {} and {}",
                        a.display(),
                        b.display()
                    ))
                }
            })
            .collect()
    };
    (source, verdict)
}

fn generated_unions(cases: u32) {
    let mut runner = law_runner(cases, "tests/regressions/law_reference_unions.txt");
    let accepted = std::cell::Cell::new(0usize);
    let result = runner.run(&(choices(64), choices(1024), choices(64)), |(s, g, q)| {
        let schema = GenSchema::generate(&mut Choices::new(&s), SchemaOptions::default());
        let graph = GenGraph::generate(&mut Choices::new(&g), &schema, 5, 8);
        let (source, verdict) = union_sample(&mut Choices::new(&q), &schema);
        let sample = Sample {
            schema: schema.text(),
            export: graph.jsonl(&schema),
            source,
            params: Vec::new(),
        };
        let fail = |e: String| TestCaseError::fail(format!("{e}\n{}", sample.describe()));
        let catalog = build_catalog(&parse_schema(&sample.schema).unwrap()).unwrap();
        match (checked(&catalog, &sample.source), verdict) {
            (Ok(read), Ok(cols)) => {
                find_union(&read.term).ok_or_else(|| fail("no Union in the term".into()))?;
                let mut got = read.result.cols.clone();
                got.sort_by(|a, b| a.name.cmp(&b.name));
                let mut want = cols;
                want.sort_by(|a, b| a.name.cmp(&b.name));
                if got != want {
                    return Err(fail(format!(
                        "the union's columns are {got:?}, the RFC's join is {want:?}"
                    )));
                }
                accepted.set(accepted.get() + 1);
                read_laws(&sample).map_err(fail)?;
            }
            (Err(refusal), Err(_)) if refusal.starts_with("T73") => {}
            (Err(refusal), Ok(cols)) => {
                return Err(fail(format!(
                    "refused ({refusal}) where the RFC joins the arms to {cols:?}"
                )));
            }
            (Err(refusal), Err(why)) => {
                return Err(fail(format!(
                    "refused with {refusal} where the RFC's refusal is T73 ({why})"
                )));
            }
            (Ok(_), Err(why)) => {
                return Err(fail(format!("accepted where the RFC refuses: {why}")));
            }
        }
        Ok(())
    });
    eprintln!("unions accepted: {} of {cases}", accepted.get());
    if let Err(e) = result {
        panic!("{e}");
    }
    assert!(accepted.get() > 0, "no generated union was accepted");
}

fn find_union(t: &Term) -> Option<&Term> {
    if matches!(t, Term::Union { .. }) {
        return Some(t);
    }
    t.children().into_iter().find_map(find_union)
}

#[test]
fn generated_unions_type_as_the_rfc_joins_their_arms() {
    generated_unions(UNIONS);
}

#[test]
#[ignore = "nightly: law 7's union typing over generated arms at the RFC's count"]
fn generated_unions_type_as_the_rfc_joins_their_arms_at_nightly_breadth() {
    generated_unions(NIGHTLY);
}

// ------------------------------------------------------------- recursion

const REC_SCHEMA: &str =
    "node A {\n    k: String @key\n    p0: I64?\n}\nedge E0: A -> A\nedge E1: A -> A\n";
const REC_KEYS: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];

/// A graph over [`REC_SCHEMA`]: up to eight nodes, up to twelve edges of
/// each type, self-loops and cycles among them.
fn rec_graph(c: &mut Choices) -> String {
    let n = 1 + c.below(REC_KEYS.len());
    let mut s = String::new();
    for key in &REC_KEYS[..n] {
        let p0 = match c.below(5) {
            0 => "null".to_string(),
            v => (v as i64 - 2).to_string(),
        };
        s.push_str(&format!(
            "{{\"type\":\"A\",\"data\":{{\"k\":\"{key}\",\"p0\":{p0}}}}}\n"
        ));
    }
    for edge in ["E0", "E1"] {
        for _ in 0..c.below(13) {
            let (a, b) = (REC_KEYS[c.below(n)], REC_KEYS[c.below(n)]);
            s.push_str(&format!(
                "{{\"edge\":\"{edge}\",\"from\":\"{a}\",\"to\":\"{b}\",\"data\":{{}}}}\n"
            ));
        }
    }
    s
}

/// The base and step reads of a recursion over [`REC_SCHEMA`]: the base
/// returns the start nodes as `q`, the step one traversal from `$q` to
/// `$r`, returned as `q`.
fn rec_reads(c: &mut Choices) -> (String, String) {
    let filter = |c: &mut Choices, var: &str| match c.below(4) {
        0 => String::new(),
        1 => format!(" where {var}.p0 > {}", c.below(3) as i64 - 1),
        2 => format!(" where {var}.p0 is null"),
        _ => format!(" where {var}.k = \"{}\"", REC_KEYS[c.below(4)]),
    };
    let base = format!(
        "query base() {{ match {{ $q: A }}{} return {{ $q }} }}\n",
        filter(c, "$q")
    );
    let traversal = *c.pick(&[
        "$q e0 $r",
        "$r e0 $q",
        "$q e1 $r",
        "$q (e0 | e1) $r",
        "$q e0{1,2} $r",
        "$q e1{2,3} $r",
    ]);
    let step = format!(
        "query step() {{ match {{ $q: A, $r: A, {traversal} }}{} return {{ $r as q }} }}\n",
        filter(c, "$r")
    );
    (base, step)
}

/// Replace every scan of `var` in `term` with the recursive relation `name`.
fn scan_to_rec_ref(term: &mut Term, var: &str, name: &str) {
    if matches!(term, Term::Scan { var: v, .. } if v == var) {
        *term = Term::RecRef {
            name: name.into(),
            ty: Default::default(),
        };
        return;
    }
    match term {
        Term::Expand { input, .. }
        | Term::Filter { input, .. }
        | Term::Project { input, .. }
        | Term::Group { input, .. }
        | Term::Order { input, .. }
        | Term::Cut { input, .. }
        | Term::Score { input, .. }
        | Term::ApproxTopK { input, .. } => scan_to_rec_ref(Arc::make_mut(input), var, name),
        Term::Correlate { outer, inner, .. } => {
            scan_to_rec_ref(Arc::make_mut(outer), var, name);
            scan_to_rec_ref(Arc::make_mut(inner), var, name);
        }
        Term::Union { left, right } => {
            scan_to_rec_ref(Arc::make_mut(left), var, name);
            scan_to_rec_ref(Arc::make_mut(right), var, name);
        }
        _ => {}
    }
}

fn rec_refs(t: &Term) -> usize {
    usize::from(matches!(t, Term::RecRef { .. }))
        + t.children().into_iter().map(rec_refs).sum::<usize>()
}

/// The node ids of a relation's column `q`, sorted.
fn ids(rows: &[Row]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .filter_map(|r| match get(r, "q") {
            Some(Value::Ref { id, .. }) => Some(id.clone()),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// The fixpoint laws for one graph and one recursion.
fn fixpoint_laws(export: &str, base_src: &str, step_src: &str) -> Result<(), String> {
    let catalog = build_catalog(&parse_schema(REC_SCHEMA).unwrap()).unwrap();
    let graph = Graph::from_export(&catalog, export).map_err(|e| e.to_string())?;
    let base = checked(&catalog, base_src)?;
    let mut step = checked(&catalog, step_src)?.term;
    scan_to_rec_ref(&mut step, "q", "reachable");
    if rec_refs(&step) != 1 {
        return Err(format!(
            "the step reads the recursion {} times",
            rec_refs(&step)
        ));
    }
    let mut read = base.clone();
    read.term = Term::Fix {
        name: "reachable".into(),
        body: Arc::new(Term::Union {
            left: Arc::new(base.term.clone()),
            right: Arc::new(step.clone()),
        }),
        rounds: None,
        lattice: Vec::new(),
    };
    // The interpreter's naive iteration, under the operator audit.
    let (naive, audit) = run_audited(&catalog, &graph, &read, &[])?;
    if !audit.violations.is_empty() {
        return Err(audit.violations.join("\n"));
    }
    let naive = naive.map_err(|e| e.to_string())?;

    let mut it = Interpreter::new(&catalog, &graph, &HashMap::new());
    let eval_step = |it: &mut Interpreter, rec: Vec<Row>| -> Result<Vec<Row>, String> {
        it.rec.insert("reachable".into(), rec);
        let rows = it.term(&step, &Vec::new()).map_err(|e| e.to_string())?.rows;
        it.rec.remove("reachable");
        Ok(rows)
    };
    // Semi-naive: each round reads only the rows the last round added.
    let start = it
        .term(&base.term, &Vec::new())
        .map_err(|e| e.to_string())?
        .rows;
    let mut all: Vec<Row> = Vec::new();
    for row in start {
        if !all.iter().any(|r| same_row(r, &row)) {
            all.push(row);
        }
    }
    let mut delta = all.clone();
    let domain = graph.nodes.get("A").map_or(0, Vec::len);
    let mut rounds = 0;
    while !delta.is_empty() {
        rounds += 1;
        if rounds > domain + 1 {
            return Err(format!(
                "semi-naive iteration ran {rounds} rounds over {domain} nodes"
            ));
        }
        let mut added = Vec::new();
        for row in eval_step(&mut it, delta)? {
            if !all.iter().chain(&added).any(|r| same_row(r, &row)) {
                added.push(row);
            }
        }
        all.extend(added.iter().cloned());
        delta = added;
    }
    // A per-node closure: each node's successors by the step over that node
    // alone, followed from the base.
    let mut closure: Vec<String> = ids(&it
        .term(&base.term, &Vec::new())
        .map_err(|e| e.to_string())?
        .rows);
    closure.dedup();
    let mut frontier = closure.clone();
    while let Some(id) = frontier.pop() {
        let one = vec![vec![(
            "q".to_string(),
            Value::Ref {
                edge: false,
                type_name: "A".into(),
                id,
            },
        )]];
        for next in ids(&eval_step(&mut it, one)?) {
            if !closure.contains(&next) {
                closure.push(next.clone());
                frontier.push(next);
            }
        }
    }
    closure.sort();
    let (naive_ids, semi_ids) = (ids(&naive), ids(&all));
    if naive_ids != semi_ids || naive_ids != closure {
        return Err(format!(
            "the fixpoints disagree: naive {naive_ids:?}, semi-naive {semi_ids:?}, closure {closure:?}"
        ));
    }
    if naive.len() > domain {
        return Err(format!("{} rows over a domain of {domain}", naive.len()));
    }
    // One more round adds nothing.
    for row in eval_step(&mut it, naive.clone())? {
        if !naive.iter().any(|r| same_row(r, &row)) {
            return Err(format!("one more round adds {row:?}"));
        }
    }
    Ok(())
}

fn generated_recursions(cases: u32) {
    let mut runner = law_runner(cases, "tests/regressions/law_reference_recursions.txt");
    let result = runner.run(&(choices(128), choices(16)), |(g, q)| {
        let export = rec_graph(&mut Choices::new(&g));
        let (base, step) = rec_reads(&mut Choices::new(&q));
        fixpoint_laws(&export, &base, &step).map_err(|e| {
            TestCaseError::fail(format!(
                "{e}\n--- schema\n{REC_SCHEMA}\n--- seed\n{export}\n--- base\n{base}--- step\n{step}"
            ))
        })
    });
    if let Err(e) = result {
        panic!("{e}");
    }
}

#[test]
fn generated_recursions_agree_on_their_least_fixpoint() {
    generated_recursions(RECURSIONS);
}

#[test]
#[ignore = "nightly: law 7's fixpoint agreement over generated recursions at the RFC's count"]
fn generated_recursions_agree_on_their_least_fixpoint_at_nightly_breadth() {
    generated_recursions(NIGHTLY);
}

/// The tie rule compares a nullable reference column in its place among
/// the references, so two rows that differ tie under no rule: a null and a
/// reference swapped between two such columns used to tie, and a cut kept
/// whichever came first.
#[test]
fn the_tie_rule_compares_a_null_reference_in_its_column() {
    let x = Value::Ref {
        edge: false,
        type_name: "A".into(),
        id: "x".into(),
    };
    let a: Row = vec![("q".into(), x.clone()), ("r".into(), Value::Null)];
    let b: Row = vec![("q".into(), Value::Null), ("r".into(), x)];
    assert_eq!(tie(&b, &a), Ordering::Less, "null sorts first in `q`");
    assert_eq!(tie(&a, &b), Ordering::Greater);
    assert!(total_order(&[b.clone(), a.clone()], &tie).is_empty());
    let mut rows = vec![a.clone(), b.clone()];
    rows.sort_by(tie);
    assert!(same_rows(&rows, &[b, a]));
}
