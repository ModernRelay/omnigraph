//! The components of one scope's pattern and the product rule
//! (docs/rfcs/2026-11-07-gq-3.md, the derived forms and "Added: two elements
//! of `match`"): two patterns that share no variable multiply their rows by a
//! product, which a scope admits only when every component but one is
//! key-pinned, a single node binding whose every `@key` property, or whose
//! `@id`, its `where` equates with a literal or a parameter; otherwise `T81`.
//!
//! A component is a set of elements that produce rows, connected through
//! the variables they bind or read: a binding, a triple, an `optional` and a
//! `from`, each with the columns it binds and the columns of the scope its
//! inner term reads. A `let` value produces no row; reading its name reads
//! the variables its value reads, so it connects nothing of its own. The
//! `where` and every block in it filter the product rather than produce it,
//! so they connect nothing either; its top-level conjuncts only pin. A
//! `from` that reads nothing of the scope is the product written out, the
//! form the fix recommends, and stays admitted.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::catalog::Catalog;
use crate::ir::core::{ANON_PREFIX, FRESH_PREFIX, Ty};
use crate::query::codes::T81;
use crate::query::diagnostic::{Applicability, QueryDiagnostic, Suggestion, TextEdit};
use crate::query::syntax::print;
use crate::query::syntax::{Element, Expr, LetValue, Pattern};
use crate::query::value::CompOp;

use super::{Scope, conjuncts, pattern_vars};

/// One row-producing element of a scope.
struct Producer {
    /// Its index among the pattern's elements.
    element: usize,
    /// The columns it binds and the columns of the scope it reads.
    vars: BTreeSet<String>,
    /// The columns it binds.
    bound: Vec<String>,
    /// A node binding `$x: T` that introduces `x`.
    binding: Option<String>,
    /// A `from` whose relation reads no column of the scope.
    uncorrelated_from: bool,
    /// A refused element, or one of the error type: it passes the product
    /// rule, so the component holding it is admitted.
    error: bool,
}

/// What a scope's elements produce, gathered while they are typed.
#[derive(Default)]
pub(super) struct Parts {
    producers: Vec<Producer>,
    /// Each `let` value's name and the variables its value reads.
    lets: HashMap<String, BTreeSet<String>>,
    /// Each binding that restates the type of a name already in scope: its
    /// element and the name.
    restated: Vec<(usize, String)>,
}

impl Parts {
    /// Record element `index`, given the columns it bound and the columns
    /// it read while it was typed.
    pub(super) fn element(
        &mut self,
        index: usize,
        element: &Element,
        bound: Vec<String>,
        reads: Vec<String>,
    ) {
        let read = self.resolve(reads);
        match element {
            Element::Let {
                name,
                value: LetValue::Expr(_),
            } if bound.iter().any(|b| b == name) => {
                self.lets.insert(name.clone(), read);
            }
            Element::Let { .. } => {}
            // A binding of a name already in scope produces no row: it
            // restates a local's type, or re-mentions an outer name in an
            // `optional` or in a block that negates a text search.
            Element::Binding { variable, .. } if !bound.contains(variable) => {
                self.restated.push((index, variable.clone()));
            }
            Element::Binding { variable, .. } => {
                let binding = Some(variable.clone());
                self.push(index, read, bound, binding, false);
            }
            Element::Traversal(t) => {
                let mut vars = read;
                for end in [Some(&t.src), Some(&t.dst), t.edge_binding.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    if end != "_" {
                        vars.insert(end.clone());
                    }
                }
                self.push(index, vars, bound, None, false);
            }
            Element::Optional(_) => self.push(index, read, bound, None, false),
            Element::From { .. } => {
                let uncorrelated = read.is_empty();
                self.push(index, read, bound, None, uncorrelated);
            }
        }
    }

    fn push(
        &mut self,
        element: usize,
        mut vars: BTreeSet<String>,
        bound: Vec<String>,
        binding: Option<String>,
        uncorrelated_from: bool,
    ) {
        vars.extend(bound.iter().cloned());
        self.producers.push(Producer {
            element,
            vars,
            bound,
            binding,
            uncorrelated_from,
            error: false,
        });
    }

    /// Record element `index`, refused or of the error type, with the names
    /// it binds or names: it joins the components sharing one, and passes.
    pub(super) fn refused(&mut self, index: usize, names: Vec<String>) {
        let vars: BTreeSet<String> = names.into_iter().filter(|n| n != "_").collect();
        self.producers.push(Producer {
            element: index,
            vars,
            bound: Vec::new(),
            binding: None,
            uncorrelated_from: false,
            error: true,
        });
    }

    /// The variables `reads` stands for: a `let` value's name stands for
    /// the variables its value reads.
    fn resolve(&self, reads: Vec<String>) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for r in reads {
            match self.lets.get(&r) {
                Some(vars) => out.extend(vars.iter().cloned()),
                None => {
                    out.insert(r);
                }
            }
        }
        out
    }

    /// The producers grouped into components, each in written order, the
    /// components ordered by their first element.
    fn components(&self) -> Vec<Vec<usize>> {
        let mut parent: Vec<usize> = (0..self.producers.len()).collect();
        fn root(parent: &mut [usize], mut i: usize) -> usize {
            while parent[i] != i {
                parent[i] = parent[parent[i]];
                i = parent[i];
            }
            i
        }
        let mut owner: HashMap<&str, usize> = HashMap::new();
        for (i, p) in self.producers.iter().enumerate() {
            for v in &p.vars {
                match owner.get(v.as_str()) {
                    Some(&j) => {
                        let (a, b) = (root(&mut parent, i), root(&mut parent, j));
                        parent[a.max(b)] = a.min(b);
                    }
                    None => {
                        owner.insert(v, i);
                    }
                }
            }
        }
        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut index: HashMap<usize, usize> = HashMap::new();
        for i in 0..self.producers.len() {
            let r = root(&mut parent, i);
            match index.get(&r) {
                Some(&g) => groups[g].push(i),
                None => {
                    index.insert(r, groups.len());
                    groups.push(vec![i]);
                }
            }
        }
        groups
    }

    /// `T81` when two components of the scope are not pinned, at the
    /// second one.
    pub(super) fn refusal(
        &self,
        catalog: &Catalog,
        sc: &Scope,
        pattern: &Pattern,
    ) -> Option<QueryDiagnostic> {
        let pins = key_pins(pattern, sc);
        let unpinned: Vec<Vec<usize>> = self
            .components()
            .into_iter()
            .filter(|c| !c.iter().all(|&p| self.producers[p].uncorrelated_from))
            .filter(|c| !self.pinned(c, catalog, sc, &pins))
            .collect();
        let [first, second, ..] = unpinned.as_slice() else {
            return None;
        };
        Some(self.diagnose(first, second, catalog, sc, pattern))
    }

    /// A component is key-pinned when it is one node binding whose every
    /// `@key` property, or whose `@id`, a top-level conjunct of the scope's
    /// `where` equates with a literal or a parameter. A component of two or
    /// more elements is never pinned, nor is a triple, an `optional` or a
    /// correlated `from`, whatever it reads.
    fn pinned(
        &self,
        component: &[usize],
        catalog: &Catalog,
        sc: &Scope,
        pins: &HashMap<String, HashSet<String>>,
    ) -> bool {
        if component.iter().any(|&p| self.producers[p].error) {
            return true;
        }
        let [only] = component else {
            return false;
        };
        let Some(var) = &self.producers[*only].binding else {
            return false;
        };
        // A refused conjunct naming it passes the key-pin check.
        if sc.loose_pins.contains(var) {
            return true;
        }
        let Some(Ty::Node { type_name, .. }) = sc.local(var).map(|c| &c.ty) else {
            return false;
        };
        let Some(equated) = pins.get(var) else {
            return false;
        };
        if equated.contains("@id") {
            return true;
        }
        let Some(key) = catalog
            .query_node_type(type_name)
            .and_then(|n| n.key.as_ref())
        else {
            return false;
        };
        !key.is_empty() && key.iter().all(|k| equated.contains(k))
    }

    fn diagnose(
        &self,
        first: &[usize],
        second: &[usize],
        catalog: &Catalog,
        sc: &Scope,
        pattern: &Pattern,
    ) -> QueryDiagnostic {
        let text = |component: &[usize]| {
            component
                .iter()
                .map(|&p| print::element(&pattern.elements[self.producers[p].element]))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let (one, other) = (text(first), text(second));
        let mut d = QueryDiagnostic::typecheck(
            T81,
            format!(
                "`{other}` shares no variable with `{one}`, so the pattern multiplies their rows by a product; a product is written only when every component but one is key-pinned, one node binding whose every `@key` property, or whose `@id`, `where` equates with a literal or a parameter"
            ),
        );
        let spans = &pattern.spans.elements;
        let at = self.producers[second[0]].element;
        if let Some(span) = spans.get(at) {
            d.position = Some(span.position);
            d.end = Some(span.end_position);
        }
        let triples = self.triples(first, second, catalog, sc);
        let named = self.named_relation(second, sc, pattern);
        let named_text = named.as_ref().map(|(text, _)| text.as_str());
        let fix = match (triples.as_slice(), named_text) {
            ([one], Some(named)) => format!(
                "join them through the edge, `{one}`, or write the intended product as a named relation, `{named}`"
            ),
            ([one], None) => format!("join them through the edge, `{one}`"),
            ([], Some(named)) => format!(
                "write the intended product as a named relation, `{named}`, or join the components through a triple"
            ),
            ([], None) => {
                "join the components through a triple, or write the intended product as a named relation, `let $r = { match { … } return { … } }, from $r`".to_string()
            }
            (many, named) => {
                let list = many
                    .iter()
                    .map(|t| format!("`{t}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                match named {
                    Some(named) => format!(
                        "join them through a triple, one of {list}, or write the intended product as a named relation, `{named}`"
                    ),
                    None => format!("join them through a triple, one of {list}"),
                }
            }
        };
        d = d.with_fix(fix);
        let last = spans.last().map(|s| s.end);
        match (triples.as_slice(), last, named) {
            ([one], Some(end), _) => d.with_suggestion(Suggestion {
                applicability: Applicability::MachineApplicable,
                edits: vec![TextEdit {
                    start: end,
                    end,
                    replacement: format!(", {one}"),
                }],
            }),
            (_, _, Some((_, edits))) => d.with_suggestion(Suggestion {
                applicability: Applicability::NeedsReview,
                edits,
            }),
            _ => d,
        }
    }

    /// Every triple an edge type of the schema writes from a node variable of
    /// one component to one of the other, source first.
    fn triples(
        &self,
        first: &[usize],
        second: &[usize],
        catalog: &Catalog,
        sc: &Scope,
    ) -> Vec<String> {
        let nodes = |component: &[usize]| -> Vec<(String, String)> {
            let mut out: Vec<(String, String)> = Vec::new();
            for &p in component {
                for v in &self.producers[p].vars {
                    if v.starts_with(ANON_PREFIX) || v.starts_with(FRESH_PREFIX) {
                        continue;
                    }
                    let col = sc.local(v).or_else(|| sc.outer_col(v));
                    if let Some(Ty::Node { type_name, .. }) = col.map(|c| &c.ty)
                        && !out.iter().any(|(n, _)| n == v)
                    {
                        out.push((v.clone(), type_name.clone()));
                    }
                }
            }
            out
        };
        let (a, b) = (nodes(first), nodes(second));
        let is = |node: &str, ty: &str| {
            node == ty
                || catalog
                    .query_node_type(node)
                    .is_some_and(|n| n.implements.iter().any(|i| i == ty))
        };
        let mut edges: Vec<_> = catalog.edge_types.values().collect();
        edges.sort_by(|x, y| x.name.cmp(&y.name));
        let mut out = Vec::new();
        for e in edges {
            let word = edge_word(&e.name);
            for (x, xt) in &a {
                for (y, yt) in &b {
                    for (src, st, dst, dt) in [(x, xt, y, yt), (y, yt, x, xt)] {
                        if is(st, &e.from_type) && is(dt, &e.to_type) {
                            let t = format!("${src} {word} ${dst}");
                            if !out.contains(&t) {
                                out.push(t);
                            }
                        }
                    }
                }
            }
        }
        out
    }

    /// The second component written as a named relation drawn with `from`:
    /// its text and the edits that put it in place of the component's
    /// elements.
    fn named_relation(
        &self,
        component: &[usize],
        sc: &Scope,
        pattern: &Pattern,
    ) -> Option<(String, Vec<TextEdit>)> {
        let mut returned: Vec<String> = Vec::new();
        for &p in component {
            for b in &self.producers[p].bound {
                if !(b.starts_with(ANON_PREFIX) || b.starts_with(FRESH_PREFIX))
                    && !returned.contains(b)
                {
                    returned.push(b.clone());
                }
            }
        }
        // The component's elements, with every binding that restates the
        // type of a name it binds.
        let mut elements: Vec<usize> = component
            .iter()
            .map(|&p| self.producers[p].element)
            .chain(
                self.restated
                    .iter()
                    .filter(|(_, v)| returned.contains(v))
                    .map(|(i, _)| *i),
            )
            .collect();
        elements.sort_unstable();
        let first = returned.first()?;
        let mut taken = Vec::new();
        pattern_vars(pattern, &mut taken);
        let base = plural(first);
        let mut name = base.clone();
        let mut n = 2;
        while taken.contains(&name) || sc.in_scope(&name) || sc.future.contains(&name) {
            name = format!("{base}{n}");
            n += 1;
        }
        let inner = elements
            .iter()
            .map(|&i| print::element(&pattern.elements[i]))
            .collect::<Vec<_>>()
            .join(", ");
        let items = returned
            .iter()
            .map(|r| format!("${r}"))
            .collect::<Vec<_>>()
            .join(", ");
        let text =
            format!("let ${name} = {{ match {{ {inner} }} return {{ {items} }} }}, from ${name}");
        let spans = &pattern.spans.elements;
        let mut edits = Vec::new();
        for (k, &i) in elements.iter().enumerate() {
            let span = spans.get(i)?;
            if k == 0 {
                edits.push(TextEdit {
                    start: span.start,
                    end: span.end,
                    replacement: text.clone(),
                });
            } else {
                let prev = spans.get(i - 1)?;
                edits.push(TextEdit {
                    start: prev.end,
                    end: span.end,
                    replacement: String::new(),
                });
            }
        }
        Some((text, edits))
    }
}

/// A relation's name from the variable it returns first: `$ps` for `$p`,
/// `$companies` for `$company`.
fn plural(name: &str) -> String {
    let consonant_y = name.len() > 1
        && name.ends_with('y')
        && !name[..name.len() - 1].ends_with(['a', 'e', 'i', 'o', 'u']);
    if consonant_y {
        format!("{}ies", &name[..name.len() - 1])
    } else if name.len() > 1
        && (name.ends_with(['s', 'x', 'z']) || name.ends_with("ch") || name.ends_with("sh"))
    {
        format!("{name}es")
    } else {
        format!("{name}s")
    }
}

/// An edge type's name as a triple writes it: its first letter lower case.
fn edge_word(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) => c.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// The properties, `@id` included, each variable's top-level `where`
/// conjuncts equate with a literal or a parameter.
fn key_pins(pattern: &Pattern, sc: &Scope) -> HashMap<String, HashSet<String>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    let Some(predicate) = &pattern.predicate else {
        return out;
    };
    let constant = |e: &Expr| match e {
        Expr::Literal(_) => true,
        Expr::Neg(inner) => matches!(inner.as_ref(), Expr::Literal(_)),
        Expr::Variable(v) => sc.params.contains_key(v),
        _ => false,
    };
    for c in conjuncts(predicate) {
        let Expr::Compare {
            op: CompOp::Eq,
            left,
            right,
            ..
        } = c
        else {
            continue;
        };
        for (prop, value) in [(left, right), (right, left)] {
            if let Expr::Prop { variable, property } = prop.as_ref()
                && constant(value)
            {
                out.entry(variable.clone())
                    .or_default()
                    .insert(property.clone());
            }
        }
    }
    out
}
