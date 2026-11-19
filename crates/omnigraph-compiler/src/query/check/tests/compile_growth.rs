//! Compile work grows linearly in the query text (the GQ 3 RFC, section 2,
//! `Call` and the plan budget; invariant 11).
//!
//! Each family below grows one construct by one unit at a time, at several
//! sizes, and measures the work compiling it takes, counted, never timed:
//!
//! - the core term's nodes as stored (a node two parents share counts once);
//! - the expression nodes of the stored term and of the read's shape, which
//!   is what the planner lowers, except the padding row of each `optional`
//!   (the RFC's `Project(Filter(Env, ¬ exists p), nulls)`), which names one
//!   column per column the pattern adds and so grows with the width of the
//!   relation, not with copies; it is counted apart;
//! - the compile steps: the expressions the type checker typed plus the term
//!   nodes the shaper visited.
//!
//! The law: from the second unit on, every unit adds the same amount to each
//! count, so none grows faster than the source does. A construct that copies
//! its pattern, substitutes a value into every use or types a subterm once
//! per enclosing guard fails it at the third size.
//!
//! The families are the compile-cost families the planner audit measured,
//! each the smallest text that grows its construct: a chain of `let`s that
//! each read the one before twice (F1a, F1b), nested `optional`s (F2), a run
//! of `optional`s (F3), nested blocks (F4), nested guarded blocks that can
//! fail (F5), nested inline reads holding an `optional` (F6), nested `if`s
//! (F7), nested `union`s (F9), nested parentheses (F10), an `or` chain of
//! blocks that can fail (F11) and a run of `where` conjuncts reading `let`s
//! (F12). A chain of declared reads each calling the next twice (F8) is
//! exponential by the RFC's own rule, the expanded tree of every `Call`
//! charged to the plan budget, and is refused rather than measured.

use crate::catalog::Catalog;
use crate::ir::IRExpr;
use crate::ir::core::{CoreDecl, Term};
use crate::shape::read::{PatternStep, ReadShape};

use super::catalog;

/// The sizes measured; increments from the second unit on are compared.
const SIZES: [usize; 5] = [1, 2, 3, 4, 5];

#[derive(Debug, Clone, Copy)]
pub(super) enum Family {
    /// `let $a{i} = $a{i-1} + $a{i-1}`, the last returned.
    LetDoubling,
    /// `let $s{i} = concat($s{i-1}, $s{i-1})`, the last read by `where`
    /// and `order`.
    LetFiltered,
    /// `optional { …, optional { … } }`, each binding the next person.
    NestedOptionals,
    /// `optional { … }, optional { … }, …` over one binding.
    OptionalRun,
    /// `count { …, where count { … } > 0 } > 0`, nested.
    NestedBlocks,
    /// `$x.age = 0 or count { …, where $y.age = 0 or count { … 10 / … } > 0 } > 0`.
    NestedGuardedBlocks,
    /// `from { match { …, optional { … }, from { … } } return { … } }`.
    NestedInlineOptionals,
    /// `if($p.age = i, i, if(…))`.
    NestedIfs,
    /// `from union({ … union(…) … }, { … })`.
    NestedUnions,
    /// `((((… $p.age …) + 1) + 1) + 1)`.
    NestedParentheses,
    /// `$p.age = 0 or count { … 10 / $x.age … } > 0 or …`.
    OrChainOfFailingBlocks,
    /// `let $v{i} = $p.age + i` and `where $v{i} > i` for each.
    FilteredLetRun,
}

const FAMILIES: [Family; 12] = [
    Family::LetDoubling,
    Family::LetFiltered,
    Family::NestedOptionals,
    Family::OptionalRun,
    Family::NestedBlocks,
    Family::NestedGuardedBlocks,
    Family::NestedInlineOptionals,
    Family::NestedIfs,
    Family::NestedUnions,
    Family::NestedParentheses,
    Family::OrChainOfFailingBlocks,
    Family::FilteredLetRun,
];

/// The read `q` grown by `n` units of `family`.
pub(super) fn source(family: Family, n: usize) -> String {
    match family {
        Family::LetDoubling => {
            let mut elements = vec!["$p: Person".to_string(), "let $a0 = $p.age".to_string()];
            for i in 1..=n {
                elements.push(format!("let $a{i} = $a{} + $a{}", i - 1, i - 1));
            }
            format!(
                "query q() {{ match {{ {} }} return {{ $p.handle, $a{n} }} }}",
                elements.join(", ")
            )
        }
        Family::LetFiltered => {
            let mut elements = vec!["$p: Person".to_string(), "let $s0 = $p.name".to_string()];
            for i in 1..=n {
                elements.push(format!("let $s{i} = concat($s{}, $s{})", i - 1, i - 1));
            }
            format!(
                "query q() {{ match {{ {} }} where $s{n} != \"x\" return {{ $p.handle }} order {{ $s{n} }} }}",
                elements.join(", ")
            )
        }
        Family::NestedOptionals => {
            let mut inner = String::new();
            for i in (1..=n).rev() {
                let nested = if inner.is_empty() {
                    String::new()
                } else {
                    format!(", {inner}")
                };
                inner = format!(
                    "optional {{ $p{i}: Person, $p{} knows $p{i}{nested} }}",
                    i - 1
                );
            }
            let returned: Vec<String> = (0..=n).map(|i| format!("$p{i}.handle")).collect();
            format!(
                "query q() {{ match {{ $p0: Person, {inner} }} return {{ {} }} }}",
                returned.join(", ")
            )
        }
        Family::OptionalRun => {
            let optionals: Vec<String> = (1..=n)
                .map(|i| format!("optional {{ $q{i}: Person, $p knows $q{i} }}"))
                .collect();
            let returned: Vec<String> = (1..=n).map(|i| format!("$q{i}.handle")).collect();
            format!(
                "query q() {{ match {{ $p: Person, {} }} return {{ $p.handle, {} }} }}",
                optionals.join(", "),
                returned.join(", ")
            )
        }
        Family::NestedBlocks => {
            let mut block = format!("count {{ $b{n}: Person, $b{} knows $b{n} }}", n - 1);
            for i in (1..n).rev() {
                block = format!(
                    "count {{ $b{i}: Person, $b{} knows $b{i} where {block} > 0 }}",
                    i - 1
                );
            }
            format!(
                "query q() {{ match {{ $b0: Person }} where {block} > 0 return {{ $b0.handle }} }}"
            )
        }
        Family::NestedGuardedBlocks => {
            let mut block = format!(
                "count {{ $g{n}: Person, $g{} knows $g{n} where 10 / $g{n}.age > 1 }}",
                n - 1
            );
            for i in (1..n).rev() {
                block = format!(
                    "count {{ $g{i}: Person, $g{} knows $g{i} where $g{i}.age = 0 or {block} > 0 }}",
                    i - 1
                );
            }
            format!(
                "query q() {{ match {{ $g0: Person }} where $g0.age = 0 or {block} > 0 return {{ $g0.handle }} }}"
            )
        }
        Family::NestedInlineOptionals => {
            let mut read = format!(
                "{{ match {{ $r{n}: Person, $r{} knows $r{n}, optional {{ $o{n}: Person, $r{n} knows $o{n} }} }} return {{ $r{n}, $o{n} }} }}",
                n - 1
            );
            for i in (1..n).rev() {
                read = format!(
                    "{{ match {{ $r{i}: Person, $r{} knows $r{i}, optional {{ $o{i}: Person, $r{i} knows $o{i} }}, from {read} }} return {{ $r{i}, $o{i}, $r{} }} }}",
                    i - 1,
                    i + 1
                );
            }
            format!(
                "query q() {{ match {{ $r0: Person, from {read} }} return {{ $r0.handle, $r1.handle }} }}"
            )
        }
        Family::NestedIfs => {
            let mut e = "$p.age".to_string();
            for i in 1..=n {
                e = format!("if($p.age = {i}, {i}, {e})");
            }
            format!(
                "query q() {{ match {{ $p: Person, let $v = {e} }} return {{ $p.handle, $v }} }}"
            )
        }
        Family::NestedUnions => {
            let mut rel = "{ match { $u: Person } where $u.age = 0 return { $u } }".to_string();
            for i in 1..=n {
                rel = format!(
                    "{{ match {{ from union({rel}, {{ match {{ $u: Person }} where $u.age = {i} return {{ $u }} }}) }} return {{ $u }} }}"
                );
            }
            format!("query q() {{ match {{ from {rel} }} return {{ $u.handle }} }}")
        }
        Family::NestedParentheses => {
            let mut e = "$p.age".to_string();
            for _ in 1..=n {
                e = format!("({e} + 1)");
            }
            format!("query q() {{ match {{ $p: Person }} where {e} > 0 return {{ $p.handle }} }}")
        }
        Family::OrChainOfFailingBlocks => {
            let mut chain = "$p.age = 0".to_string();
            for i in 1..=n {
                chain.push_str(&format!(
                    " or count {{ $x{i}: Person, $p knows $x{i} where 10 / $x{i}.age > {i} }} > 0"
                ));
            }
            format!("query q() {{ match {{ $p: Person }} where {chain} return {{ $p.handle }} }}")
        }
        Family::FilteredLetRun => {
            let lets: Vec<String> = (1..=n)
                .map(|i| format!("let $v{i} = $p.age + {i}"))
                .collect();
            let conjuncts: Vec<String> = (1..=n).map(|i| format!("$v{i} > {i}")).collect();
            format!(
                "query q() {{ match {{ $p: Person, {} }} where {} return {{ $p.handle }} }}",
                lets.join(", "),
                conjuncts.join(" and ")
            )
        }
    }
}

/// The term's nodes as stored: a node reached twice counts once.
fn stored_nodes<'a>(
    term: &'a Term,
    seen: &mut std::collections::HashSet<*const Term>,
) -> Vec<&'a Term> {
    let mut out = Vec::new();
    let mut pending = vec![term];
    while let Some(t) = pending.pop() {
        if !seen.insert(t as *const Term) {
            continue;
        }
        out.push(t);
        pending.extend(t.children());
    }
    out
}

fn expr_nodes(e: &IRExpr) -> usize {
    let mut n = 0;
    e.visit_nodes(|_| n += 1);
    n
}

/// Whether `t` is the padding arm of an `optional`'s `Union`, whose left
/// arm is `left`.
fn is_padding(t: &Term, left: &Term) -> bool {
    let Term::Project {
        input, keep: false, ..
    } = t
    else {
        return false;
    };
    let Term::Filter { input, .. } = input.as_ref() else {
        return false;
    };
    let Term::Correlate { inner, .. } = input.as_ref() else {
        return false;
    };
    matches!(inner.as_ref(), Term::Group { input, .. } if std::ptr::eq(input.as_ref(), left))
}

/// The padding rows' columns: one per column each `optional` pads.
fn padding(nodes: &[&Term]) -> usize {
    nodes
        .iter()
        .filter_map(|t| match t {
            Term::Union { left, right } => match right.as_ref() {
                Term::Project { items, .. } if is_padding(right, left) => Some(items.len()),
                _ => None,
            },
            _ => None,
        })
        .sum()
}

/// The expressions a stored term node holds.
fn term_exprs(t: &Term) -> usize {
    match t {
        Term::Filter { pred, .. } => expr_nodes(pred),
        Term::Project { items, .. } => items.iter().map(|(_, e)| expr_nodes(e)).sum(),
        Term::Group { keys, aggs, .. } => {
            keys.iter().map(|(_, e)| expr_nodes(e)).sum::<usize>()
                + aggs
                    .iter()
                    .filter_map(|(_, a)| a.arg.as_ref())
                    .map(expr_nodes)
                    .sum::<usize>()
        }
        Term::Order { keys, .. } => keys.iter().map(|k| expr_nodes(&k.expr)).sum(),
        Term::Call { bindings, .. } => bindings.iter().map(|(_, e)| expr_nodes(e)).sum(),
        _ => 0,
    }
}

/// The expression nodes of a shape's steps and of what it returns.
fn shape_exprs(shape: &ReadShape) -> usize {
    fn steps(list: &[PatternStep]) -> usize {
        list.iter()
            .map(|step| match step {
                PatternStep::NodeScan { .. } | PatternStep::Expand { .. } => 0,
                PatternStep::Filter { pred, .. } => expr_nodes(pred),
                PatternStep::Rank { key, .. } => expr_nodes(key),
                PatternStep::Score { query, .. } => expr_nodes(query),
                PatternStep::Let { value, .. } => expr_nodes(value),
                PatternStep::AntiJoin {
                    inner, predicate, ..
                } => {
                    steps(inner)
                        + predicate.left.arg().map_or(0, expr_nodes)
                        + expr_nodes(&predicate.right)
                }
                PatternStep::Optional { inner, .. } => steps(inner),
                PatternStep::Correlate { inner, read, .. } => {
                    steps(inner) + read.exprs().map(expr_nodes).sum::<usize>()
                }
                PatternStep::Union { arms, .. } => arms.iter().map(|a| steps(a)).sum(),
                PatternStep::Fuse {
                    arms, k, weights, ..
                } => {
                    arms.iter().map(|a| steps(&a.steps)).sum::<usize>()
                        + k.iter().chain(weights).map(expr_nodes).sum::<usize>()
                }
            })
            .sum()
    }
    steps(&shape.pipeline)
        + shape
            .return_exprs
            .iter()
            .map(|p| expr_nodes(&p.expr))
            .sum::<usize>()
        + shape
            .order_by
            .iter()
            .map(|o| expr_nodes(&o.expr))
            .sum::<usize>()
}

/// The work of compiling `q` in `source`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Work {
    pub term_nodes: usize,
    pub expr_nodes: usize,
    pub steps: usize,
    /// The work the plan budget is charged with when `q` is typed and
    /// shaped ([`crate::budget`]).
    pub charged: crate::budget::WorkSpent,
    /// The `optional` padding rows' columns, apart from `expr_nodes`.
    pub padding: usize,
}

pub(super) fn measure(catalog: &Catalog, source: &str) -> Result<Work, String> {
    let work = super::super::typing_work(catalog, source);
    let (_, w) = work
        .into_iter()
        .find(|(name, _)| name == "q")
        .ok_or("no `q`")?;
    let Some(CoreDecl::Read(read)) = w.core else {
        let refusal = crate::query::check::type_source(catalog, source)
            .ok()
            .and_then(|r| r.into_iter().find_map(|(_, r)| r.err()))
            .map_or_else(|| "refused".to_string(), |d| d.to_string());
        return Err(refusal);
    };
    let mut seen = std::collections::HashSet::new();
    let nodes = stored_nodes(&read.term, &mut seen);
    let (shape, visited) = ReadShape::of_measured(&read, catalog);
    let shape = shape.map_err(|e| e.to_string())?;
    // Every node of the expanded tree was charged as it was built.
    let tree = read.term.size();
    if (shape.work.spent.term_nodes as usize) < tree {
        return Err(format!(
            "{tree} term nodes, {} charged",
            shape.work.spent.term_nodes
        ));
    }
    let padding = padding(&nodes);
    Ok(Work {
        term_nodes: nodes.len(),
        expr_nodes: nodes.iter().map(|t| term_exprs(t)).sum::<usize>() + shape_exprs(&shape)
            - padding,
        steps: w.typed_exprs + visited,
        charged: shape.work.spent,
        padding,
    })
}

/// Each family's work per size, or the refusal of the size that failed.
pub(super) fn grow(family: Family) -> Result<Vec<Work>, String> {
    let catalog = catalog();
    SIZES
        .iter()
        .map(|&n| {
            let src = source(family, n);
            measure(&catalog, &src).map_err(|e| format!("{family:?} at {n} units: {e}\n{src}"))
        })
        .collect()
}

/// Whether every count adds the same amount per unit from the second unit
/// on.
fn linear(works: &[Work]) -> bool {
    let counts = |f: fn(&Work) -> usize| -> Vec<isize> {
        works
            .windows(2)
            .map(|w| f(&w[1]) as isize - f(&w[0]) as isize)
            .collect()
    };
    [
        counts(|w| w.term_nodes),
        counts(|w| w.expr_nodes),
        counts(|w| w.steps),
        // The expanded tree, charged as it is built.
        counts(|w| w.charged.term_nodes as usize),
    ]
    .iter()
    .all(|d| d[1..].iter().all(|x| *x == d[1]))
}

/// Whether the padding columns grow by the same amount per unit from the
/// second unit on, or, for nested `optional`s, whose relation widens by one
/// column per level, by an amount that itself grows by the same amount.
fn padding_bounded(family: Family, works: &[Work]) -> bool {
    let steps: Vec<isize> = works
        .windows(2)
        .map(|w| w[1].padding as isize - w[0].padding as isize)
        .collect();
    let steps = match family {
        Family::NestedOptionals => steps.windows(2).map(|w| w[1] - w[0]).collect(),
        _ => steps,
    };
    steps[1..].iter().all(|step| *step == steps[1])
}

#[test]
fn compile_work_grows_linearly_in_every_family() {
    let mut failures = Vec::new();
    for family in FAMILIES {
        match grow(family) {
            Ok(works) if linear(&works) && padding_bounded(family, &works) => {}
            Ok(works) => failures.push(format!("{family:?}: {works:?}")),
            Err(e) => failures.push(e),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Sizes the type checker refused, or took exponential time over, before
/// the core term shared an `optional`'s pattern and a `let` became a
/// column: each compiles now, within the default plan budget.
#[test]
fn reads_past_the_old_limits_compile() {
    let catalog = catalog();
    for (family, n) in [
        // Refused at 8 levels (`T70`, 4,336 nodes).
        (Family::NestedOptionals, 40),
        // 2^40 expression nodes when each read of a `let` copied its value.
        (Family::LetDoubling, 40),
        (Family::LetFiltered, 40),
        // Shaped 2^n times.
        (Family::NestedInlineOptionals, 30),
        // Each guard copied every disjunct before it.
        (Family::OrChainOfFailingBlocks, 200),
    ] {
        let src = source(family, n);
        let work = measure(&catalog, &src).unwrap_or_else(|e| panic!("{family:?} at {n}: {e}"));
        assert!(
            work.term_nodes + work.expr_nodes < 50 * src.len(),
            "{family:?} at {n}: {work:?} for {} bytes",
            src.len()
        );
    }
}

/// The table the change's report quotes: every family's work at each size.
#[test]
#[ignore = "instrument: prints each family's compile work per size"]
fn compile_work_table() {
    for family in FAMILIES {
        let catalog = catalog();
        for n in [1, 2, 4, 8, 12, 16] {
            let src = source(family, n);
            let started = std::time::Instant::now();
            let work = measure(&catalog, &src);
            println!(
                "{family:?}\t{n}\t{} bytes\t{work:?}\t{:?}",
                src.len(),
                started.elapsed()
            );
            if work.is_err() || started.elapsed().as_secs() > 20 {
                break;
            }
        }
    }
}

/// The budgets a refusal's cost is measured at: the work at refusal must
/// stay within twice each, so it grows with the budget, not the source.
const REFUSAL_BUDGETS: [usize; 3] = [256, 1024, 4096];

/// `q` declaring `budget`, with `elements` in its `match` and returning
/// `returned`, then `decls`.
fn declared(budget: usize, elements: &str, returned: &str, decls: &[String]) -> String {
    let mut source = format!(
        "query q() @budget(plan = {budget}) {{ match {{ {elements} }} return {{ {returned} }} }}"
    );
    for d in decls {
        source.push('\n');
        source.push_str(d);
    }
    source
}

/// A chain of `n` conjuncts bound by a `let`: about four typing units and
/// four expression nodes each.
fn conjunct_chain(n: usize) -> String {
    let chain: Vec<String> = (0..n).map(|i| format!("$p.age > {i}")).collect();
    format!("$p: Person, let $b = {}", chain.join(" and "))
}

/// A chain of `n` triples: no expression, one term node each.
fn triple_chain(n: usize) -> String {
    let triples: Vec<String> = (1..=n)
        .map(|i| format!("$p{} knows $p{i}", i - 1))
        .collect();
    format!("$p0: Person, {}", triples.join(", "))
}

/// Reads that each call the next twice, `depth` deep: an expanded tree of
/// 2^depth bodies, each body typed once.
fn doubling_calls(depth: usize) -> Vec<String> {
    let mut reads = Vec::new();
    for i in 1..depth {
        reads.push(format!(
            "query r{i}() {{ match {{ from union(r{}(), r{}()) }} return {{ $p }} }}",
            i + 1,
            i + 1
        ));
    }
    reads.push(format!(
        "query r{depth}() {{ match {{ $p: Person }} return {{ $p }} }}"
    ));
    reads
}

/// A read built to pass its plan budget is refused with `T70` by the charge
/// that passes it, with at most twice the budget's work done, whatever the
/// size of the source, in every kind of work: typing (a long `and` chain,
/// read and typed node by node), expression nodes (a shorter chain typed
/// within the budget, whose stored nodes pass it), term nodes (reads that
/// each call the next twice, sixteen times the budget expanded, each shared
/// body charged at a later call site by a walk that stops at the budget
/// left), the read
/// of a long pattern (a chain of triples) and shaping (the shaper on a read
/// typed under a larger budget). The planner's share is the planner's test
/// (`query_plan::a_plan_past_its_budget_stops_within_twice_the_budget`).
/// The refusal names the construct whose work grew and how much it charged,
/// and advises no larger budget.
#[test]
fn a_compile_past_its_budget_stops_within_twice_the_budget_in_every_kind() {
    use crate::budget::WorkKind;
    let catalog = catalog();
    let refused = |source: &str, budget: usize, kind: Option<WorkKind>| {
        let work = super::super::typing_work(&catalog, source);
        let (_, w) = work.into_iter().find(|(n, _)| n == "q").expect("q");
        assert!(!w.accepted, "accepted:\n{source}");
        let overrun = w.overrun.expect("refused by the budget");
        let message = w.refusals[0].message.clone();
        assert_eq!(w.refusals[0].code.as_str(), "T70", "{message}");
        assert!(message.contains("grew the most, charging"), "{message}");
        assert!(!message.contains("@budget"), "{message}");
        if let Some(kind) = kind {
            assert_eq!(overrun.kind, kind, "{budget}: {message}");
        }
        let total = overrun.spent.total() as usize;
        assert!(
            (budget + 1..=2 * budget).contains(&total),
            "{budget}: {total}: {message}"
        );
        message
    };
    for budget in REFUSAL_BUDGETS {
        refused(
            &declared(budget, &conjunct_chain(budget), "$p.handle, $b", &[]),
            budget,
            Some(WorkKind::Typing),
        );
        refused(
            &declared(
                budget,
                &conjunct_chain(budget * 3 / 32),
                "$p.handle, $b",
                &[],
            ),
            budget,
            Some(WorkKind::ExprNodes),
        );
        // The shared bodies' nodes are charged together with the
        // expressions they hold; term nodes are most of the work.
        let calls = declared(
            budget,
            "from union(r1(), r1())",
            "$p",
            &doubling_calls(budget.ilog2() as usize + 4),
        );
        let message = refused(&calls, budget, None);
        assert!(message.contains("q → r1 → r2"), "{message}");
        let work = super::super::typing_work(&catalog, &calls);
        let spent = work[0].1.overrun.as_ref().expect("refused").spent;
        assert!(
            WorkKind::ALL
                .iter()
                .all(|k| spent.of(*k) <= spent.term_nodes),
            "{budget}: {spent:?}"
        );
        refused(
            &declared(budget, &triple_chain(2 * budget), "$p0.handle", &[]),
            budget,
            None,
        );
        // Shaping: the triple chain typed under a larger budget, shaped
        // under one that leaves `budget` units for the shaper.
        let source = declared(65_536, &triple_chain(budget), "$p0.handle", &[]);
        let work = super::super::typing_work(&catalog, &source);
        let Some(CoreDecl::Read(mut read)) = work.into_iter().next().unwrap().1.core else {
            panic!("the chain types under the larger budget");
        };
        let typed = read.work.0.spent.total() as usize;
        read.work.0.limit = (typed + budget) as u64;
        let (shaped, visited) = ReadShape::of_measured(&read, &catalog);
        let Err(crate::shape::ShapeError::OverBudget { overrun, .. }) = shaped else {
            panic!("shaped past its budget");
        };
        assert_eq!(overrun.kind, WorkKind::Shaping);
        let shaping = overrun.spent.shaping as usize;
        assert_eq!(shaping, visited, "one unit per node the shaper visited");
        assert_eq!(
            shaping,
            budget + 1,
            "the shaper stops at its first node past"
        );
    }
}
