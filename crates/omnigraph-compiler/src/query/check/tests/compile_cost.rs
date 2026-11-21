//! Law 3: compile cost grows as the plan budget's accounting says (the GQ 3
//! RFC, section 2, `Call` and the plan budget).
//!
//! A generated read (`crate::arbitrary`) is grown by one construct family,
//! one unit at a time: a run of `optional`s, a chain of `let`s, a nested
//! guard (`if` and `coalesce`), a nested chain of the registry's scalar
//! rows (`trim`, `upper`, `length`, `cast`, `abs`, `sqrt`, `round` with
//! `digits:`), a `case` growing by one `when` (lowered to nested `if`), a
//! run of blocks bound by `let`, and a chain of declared reads each calling
//! the next once. Two deterministic counts
//! are measured per size: the expressions the checker types for the read,
//! each called read typed once, and the term nodes the read's plan budget
//! is charged with, its expanded tree ([`crate::budget`]). The law: from the second unit on, each unit adds exactly
//! the same amount to both counts, so neither grows faster than the source;
//! an exponential construct (an `optional` that copies its outer relation,
//! a call typed once per call site) fails at the third size.

use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

use crate::arbitrary::{Choices, GenRead, GenSchema, ReadOptions, SchemaOptions, choices};
use crate::catalog::build_catalog;
use crate::schema::parser::parse_schema;

/// The sizes measured: increments from the second unit on are compared.
const SIZES: std::ops::RangeInclusive<usize> = 1..=5;
const CASES: u32 = 200;
const NIGHTLY: u32 = 5_000;

/// A construct family: the elements, `let`s and declarations `n` units add
/// to a read whose first binding is `$v` of type `ty`, given an edge from
/// `ty` (its traversal name and its destination type) when the schema has
/// one.
#[derive(Debug, Clone, Copy)]
enum Family {
    Optionals,
    Lets,
    Guards,
    Rows,
    Cases,
    Blocks,
    Calls,
}

const FAMILIES: [Family; 7] = [
    Family::Optionals,
    Family::Lets,
    Family::Guards,
    Family::Rows,
    Family::Cases,
    Family::Blocks,
    Family::Calls,
];

/// The first node binding the read writes: `($n<k>, type)`.
fn first_binding(source: &str) -> Option<(String, String)> {
    let at = source.find("match { $n")?;
    let rest = &source[at + "match { ".len()..];
    let (var, rest) = rest.split_once(": ")?;
    let ty: String = rest
        .chars()
        .take_while(char::is_ascii_alphanumeric)
        .collect();
    (!var.contains(' ') && !ty.is_empty()).then(|| (var.to_string(), ty))
}

/// The read grown by `n` units of `family`: its elements inserted after the
/// first binding, and any declarations it calls appended.
fn grown(
    source: &str,
    v: &str,
    ty: &str,
    edge: Option<(&str, &str)>,
    family: Family,
    n: usize,
) -> Option<String> {
    let mut elements = Vec::new();
    let mut decls = Vec::new();
    match family {
        Family::Optionals => {
            let (e, dst) = edge?;
            for i in 1..=n {
                elements.push(format!("optional {{ $zo{i}: {dst}, {v} {e} $zo{i} }}"));
            }
        }
        Family::Lets => {
            elements.push(format!("let $zl1 = concat({v}.k, \"a\")"));
            for i in 2..=n {
                elements.push(format!("let $zl{i} = concat($zl{}, \"a\")", i - 1));
            }
        }
        Family::Guards => {
            let mut e = format!("{v}.k");
            for _ in 1..=n {
                e = format!("coalesce(if(({v}.k = \"a\"), \"b\", {e}), \"c\")");
            }
            elements.push(format!("let $zg = {e}"));
        }
        Family::Rows => {
            let mut e = format!("{v}.k");
            for _ in 1..=n {
                e = format!(
                    "cast(round(sqrt(abs(cast(length(upper(trim({e}))), F64))), digits: 2), String)"
                );
            }
            elements.push(format!("let $zr = {e}"));
        }
        Family::Cases => {
            let whens: String = (1..=n)
                .map(|i| format!(" when ({v}.k = \"{i}\") then \"w{i}\""))
                .collect();
            elements.push(format!("let $zc = case{whens} else {v}.k end"));
        }
        Family::Blocks => {
            let (e, dst) = edge?;
            for i in 1..=n {
                elements.push(format!(
                    "let $zb{i} = count {{ $zx{i}: {dst}, {v} {e} $zx{i} }}"
                ));
            }
        }
        Family::Calls => {
            for i in 1..n {
                decls.push(format!(
                    "query zr{i}() {{ match {{ from zr{}() }} return {{ $zp }} }}",
                    i + 1
                ));
            }
            decls.push(format!(
                "query zr{n}() {{ match {{ $zp: {ty} }} where $zp.k = \"a\" return {{ $zp }} }}"
            ));
            elements.push("from zr1()".to_string());
        }
    }
    let anchor = format!("match {{ {v}: {ty}");
    let at = source.find(&anchor)? + anchor.len();
    let mut out = String::new();
    out.push_str(&source[..at]);
    for e in &elements {
        out.push_str(", ");
        out.push_str(e);
    }
    out.push_str(&source[at..]);
    for d in decls {
        out.push('\n');
        out.push_str(&d);
    }
    Some(out)
}

/// `(the expressions typing the read types, its callees included once
/// each, the term nodes its plan budget is charged with, the expanded
/// tree)`; every declaration must be accepted. Each declaration of a
/// source is typed as its own root, so a sum over a chain of `n` reads
/// would count the chain's tail `n` times.
fn measure(catalog: &crate::catalog::Catalog, source: &str) -> Result<(usize, usize), String> {
    let work = super::super::typing_work(catalog, source);
    let mut exprs = 0;
    let mut charged = 0;
    for (name, w) in &work {
        if !w.accepted {
            return Err(format!("`{name}` is refused"));
        }
        if name == "q" {
            exprs = w.typed_exprs;
            let Some(crate::ir::core::CoreDecl::Read(read)) = &w.core else {
                return Err("`q` is not a read".to_string());
            };
            let shape = crate::shape::read::ReadShape::of(read, catalog)
                .map_err(|e| format!("`q` does not shape: {e}"))?;
            charged = shape.work.spent.term_nodes as usize;
        }
    }
    Ok((exprs, charged))
}

fn law(cases: u32) {
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases,
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let compared = std::cell::Cell::new(0usize);
    let result = runner.run(&(choices(64), choices(512), 0..FAMILIES.len()), |(s, q, f)| {
        let schema = GenSchema::generate(&mut Choices::new(&s), SchemaOptions::default());
        let catalog = build_catalog(&parse_schema(&schema.text()).unwrap()).unwrap();
        let read = GenRead::generate(
            &mut Choices::new(&q),
            &schema,
            ReadOptions {
                search: false,
                ..ReadOptions::default()
            },
        );
        let Some((v, ty)) = first_binding(&read.source) else {
            return Ok(());
        };
        let t = schema.nodes.iter().position(|n| n.name == ty).unwrap();
        let edge = schema
            .edges
            .iter()
            .find(|e| e.from == t)
            .map(|e| (e.surface(), schema.nodes[e.to].name.clone()));
        let edge = edge.as_ref().map(|(e, d)| (e.as_str(), d.as_str()));
        let family = FAMILIES[f];
        let mut counts = Vec::new();
        for n in SIZES {
            let Some(source) = grown(&read.source, &v, &ty, edge, family, n) else {
                return Ok(());
            };
            // A read the base refuses in a way growth does not change (a
            // product the new binding makes) is not this law's sample.
            match measure(&catalog, &source) {
                Ok(count) => counts.push((count, source)),
                Err(_) if n == 1 => return Ok(()),
                Err(e) => {
                    return Err(proptest::test_runner::TestCaseError::fail(format!(
                        "{family:?} at {n} units: {e}\n{source}"
                    )));
                }
            }
        }
        compared.set(compared.get() + 1);
        let increments: Vec<(isize, isize)> = counts
            .windows(2)
            .map(|w| {
                (
                    w[1].0.0 as isize - w[0].0.0 as isize,
                    w[1].0.1 as isize - w[0].0.1 as isize,
                )
            })
            .collect();
        if increments[1..].iter().any(|d| *d != increments[1]) {
            return Err(proptest::test_runner::TestCaseError::fail(format!(
                "{family:?}: (typed expressions, charged term nodes) per size {:?}, increments {increments:?}\n--- schema\n{}\n--- largest read\n{}",
                counts.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
                schema.text(),
                counts.last().unwrap().1
            )));
        }
        Ok(())
    });
    if let Err(e) = result {
        panic!("{e}");
    }
    assert!(
        compared.get() * 2 >= cases as usize,
        "only {} of {cases} generated reads were grown and measured",
        compared.get()
    );
}

#[test]
fn compile_cost_grows_linearly_in_every_construct_family() {
    law(CASES);
}

#[test]
#[ignore = "nightly: 5,000 generated reads; the gate runs 200"]
fn compile_cost_grows_linearly_in_every_construct_family_at_nightly_breadth() {
    law(NIGHTLY);
}
