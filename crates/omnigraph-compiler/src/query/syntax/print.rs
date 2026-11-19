//! A printer for the parse tree.
//!
//! It writes the text the grammar reads back to an equal tree, inserting a
//! parenthesis only where precedence requires one: every list separated by
//! commas, each pattern's `where` after its elements.

use std::fmt::Write as _;

use super::*;
use crate::query::file::{MutationWriteMode, UnattachedBase};
use crate::query::syntax::EdgeSelector;
use crate::query::value::{AggFunc, CompOp, Literal};

pub fn print_file(file: &SourceFile) -> String {
    let mut out = String::from("");
    for setting in &file.settings {
        let _ = writeln!(out, "{setting:?}");
    }
    match &file.body {
        Body::Decls(decls) => {
            for d in decls {
                out.push_str(&print_decl(d));
                out.push('\n');
            }
        }
        Body::Explain { before, decl } => {
            for d in before {
                out.push_str(&print_decl(d));
                out.push('\n');
            }
            out.push_str("explain ");
            out.push_str(&print_decl(decl));
        }
        Body::Statement(_) => out.push_str("/* statement */"),
    }
    out
}

pub fn print_decl(d: &Decl) -> String {
    let mut out = header(d);
    out.push_str(" { ");
    match &d.body {
        DeclBody::Read(body) => out.push_str(&read_body(body)),
        DeclBody::Write(body) => {
            let groups: Vec<String> = body
                .groups
                .iter()
                .map(|group| {
                    let mut parts = Vec::new();
                    if !group.pattern.elements.is_empty() {
                        parts.push(match_pattern(&group.pattern));
                    }
                    parts.extend(group.statements.iter().map(statement));
                    parts.join("  ")
                })
                .collect();
            out.push_str(&groups.join("  "));
        }
    }
    out.push_str(" }");
    out
}

/// A parameter declaration: `$name: Type?`, then `= default` when it has
/// one.
pub(crate) fn param(p: &Param) -> String {
    let mut out = format!(
        "${}: {}{}",
        p.name,
        p.type_name,
        if p.nullable { "?" } else { "" }
    );
    if let Some(default) = &p.default {
        let _ = write!(
            out,
            " = {}{}",
            if default.negative { "-" } else { "" },
            literal(&default.value)
        );
    }
    out
}

/// A declaration's text before its body's brace: the name, the parameters,
/// the annotations and the write mode.
pub fn header(d: &Decl) -> String {
    let params: Vec<String> = d.params.iter().map(param).collect();
    let mut out = format!("query {}({})", d.name, params.join(", "));
    for a in &d.annotations {
        let _ = write!(out, " @{}", a.name);
        if let Some(args) = &a.args {
            out.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                match arg {
                    AnnotationArg::Positional(v) => out.push_str(&annotation_value(v)),
                    AnnotationArg::Named(k, v) => {
                        let _ = write!(out, "{k} = {}", annotation_value(v));
                    }
                }
            }
            out.push(')');
        }
    }
    match &d.write_mode {
        MutationWriteMode::Publish => {}
        MutationWriteMode::AsCommit => out.push_str(" as commit"),
        MutationWriteMode::Unattached { base } => {
            out.push_str(" as unattached branch");
            match base {
                None => {}
                Some(UnattachedBase::Literal(s)) => {
                    let _ = write!(out, " on {}", string(s));
                }
                Some(UnattachedBase::Param(p)) => {
                    let _ = write!(out, " on ${p}");
                }
            }
        }
    }
    out
}

fn annotation_value(v: &AnnotationValue) -> String {
    match v {
        AnnotationValue::Literal(l) => literal(l),
        AnnotationValue::Variable(v) => format!("${v}"),
        AnnotationValue::MetaField(m) => format!("@{m}"),
        AnnotationValue::Ident(i) => i.clone(),
    }
}

pub(crate) fn statement(s: &Stmt) -> String {
    let assignments = |a: &[Assignment]| {
        a.iter()
            .map(|a| format!("{}: {}", a.property, expr(&a.value)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match s {
        Stmt::Insert {
            mode,
            type_name,
            assignments: a,
        } => format!("{} {type_name} {{ {} }}", mode.keyword(), assignments(a)),
        Stmt::Update {
            variable,
            assignments: a,
        } => format!("update ${variable} set {{ {} }}", assignments(a)),
        Stmt::Delete { variable } => format!("delete ${variable}"),
    }
}

pub fn read_body(body: &ReadBody) -> String {
    let mut out = match_pattern(&body.pattern);
    out.push_str(" return ");
    if body.distinct {
        out.push_str("distinct ");
    }
    let items: Vec<String> = body
        .projections
        .iter()
        .map(|p| match &p.alias {
            Some(a) => format!("{} as {a}", expr(&p.expr)),
            None => expr(&p.expr),
        })
        .collect();
    let _ = write!(out, "{{ {} }}", items.join(", "));
    if !body.order.is_empty() {
        let keys: Vec<String> = body
            .order
            .iter()
            .map(|o| {
                let key = match o.descending {
                    None => expr(&o.expr),
                    Some(true) => format!("{} desc", expr(&o.expr)),
                    Some(false) => format!("{} asc", expr(&o.expr)),
                };
                match o.nulls_first {
                    None => key,
                    Some(true) => format!("{key} nulls first"),
                    Some(false) => format!("{key} nulls last"),
                }
            })
            .collect();
        let _ = write!(out, " order {{ {} }}", keys.join(", "));
    }
    match &body.limit {
        None => {}
        Some(Limit::Literal(n)) => {
            let _ = write!(out, " limit {n}");
        }
        Some(Limit::Param(p)) => {
            let _ = write!(out, " limit ${p}");
        }
    }
    out
}

/// `match { … }` and its `where`.
pub fn match_pattern(p: &Pattern) -> String {
    let mut out = format!("match {{ {} }}", elements(p));
    if let Some(w) = &p.predicate {
        let _ = write!(out, " where {}", expr(w));
    }
    out
}

/// A block's or an `optional`'s braces: its elements and its own `where`.
pub(crate) fn braced(p: &Pattern) -> String {
    match &p.predicate {
        Some(w) => format!("{{ {} where {} }}", elements(p), expr(w)),
        None => format!("{{ {} }}", elements(p)),
    }
}

pub(crate) fn elements(p: &Pattern) -> String {
    crate::stack::deep(|| {
        p.elements
            .iter()
            .map(element)
            .collect::<Vec<_>>()
            .join(", ")
    })
}

pub(crate) fn inline(body: &ReadBody) -> String {
    format!("{{ {} }}", read_body(body))
}

pub fn element(c: &Element) -> String {
    crate::stack::deep(|| match c {
        Element::Let { name, value } => match value {
            LetValue::Relation(body) => format!("let ${name} = {}", inline(body)),
            LetValue::Expr(e) => format!("let ${name} = {}", expr(e)),
        },
        Element::From { rel, score } => match score {
            Some(s) => format!("from {} as ${s}", relation(rel)),
            None => format!("from {}", relation(rel)),
        },
        Element::Optional(p) => format!("optional {}", braced(p)),
        Element::Binding {
            variable,
            type_name,
        } => format!("${variable}: {type_name}"),
        Element::Traversal(t) => {
            let mut out = format!("${} ", t.src);
            if let Some(b) = &t.edge_binding {
                let _ = write!(out, "${b}:");
            }
            let sel = match &t.selector {
                EdgeSelector::Wildcard => "*".to_string(),
                EdgeSelector::Named(n) => n.clone(),
                EdgeSelector::Alternation(ns) if t.undirected => ns.join(" | "),
                EdgeSelector::Alternation(ns) => format!("({})", ns.join(" | ")),
            };
            if t.undirected {
                let _ = write!(out, "<{sel}>");
            } else {
                out.push_str(&sel);
            }
            if (t.min_hops, t.max_hops) != (1, Some(1)) {
                match t.max_hops {
                    Some(max) => {
                        let _ = write!(out, "{{{},{max}}}", t.min_hops);
                    }
                    None => {
                        let _ = write!(out, "{{{},}}", t.min_hops);
                    }
                }
            }
            let _ = write!(out, " ${}", t.dst);
            out
        }
    })
}

pub(crate) fn relation(rel: &RelExpr) -> String {
    match rel {
        RelExpr::Inline(body) => inline(body),
        RelExpr::Call(c) => call(c),
        RelExpr::Variable(v) => format!("${v}"),
    }
}

pub(crate) fn block(b: &Block) -> String {
    let head = match (b.func, &b.arg, &b.fraction) {
        (BlockFunc::Exists, _, _) => "exists".to_string(),
        (BlockFunc::Agg(f), None, _) => agg(f).to_string(),
        (BlockFunc::Agg(f), Some(a), None) => format!("{}({})", agg(f), expr(a)),
        (BlockFunc::Agg(f), Some(a), Some(p)) => {
            format!("{}({}, {})", agg(f), expr(a), expr(p))
        }
    };
    format!("{head} {}", braced(&b.pattern))
}

pub(crate) fn agg(f: AggFunc) -> &'static str {
    match f {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::Collect => "collect",
        AggFunc::Stddev => "stddev",
        AggFunc::Percentile => "percentile",
    }
}

pub(crate) fn comp(op: CompOp) -> &'static str {
    match op {
        CompOp::Eq => "=",
        CompOp::Ne => "!=",
        CompOp::Gt => ">",
        CompOp::Lt => "<",
        CompOp::Ge => ">=",
        CompOp::Le => "<=",
        CompOp::Contains | CompOp::StringContains => "contains",
        CompOp::StartsWith => "starts_with",
    }
}

pub(crate) fn call(c: &Call) -> String {
    crate::stack::deep(|| {
        let mut parts: Vec<String> = c
            .args
            .iter()
            .map(|a| match a {
                Arg::Expr(e) => expr(e),
                Arg::Inline(body) => inline(body),
            })
            .collect();
        parts.extend(c.options.iter().map(|(k, v)| format!("{k}: {}", expr(v))));
        format!("{}({})", c.name, parts.join(", "))
    })
}

/// Precedence, lowest first, following the grammar's ladder.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Prec {
    Or,
    And,
    Not,
    Comparison,
    Sum,
    Product,
    Unary,
    Atom,
}

pub(crate) fn prec(e: &Expr) -> Prec {
    crate::stack::deep(|| match e {
        Expr::Or(..) => Prec::Or,
        Expr::And(..) => Prec::And,
        Expr::Not(_) => Prec::Not,
        Expr::Compare { .. } | Expr::In { .. } | Expr::IsNull { .. } | Expr::InfixString { .. } => {
            Prec::Comparison
        }
        Expr::Arith {
            op: ArithOp::Add | ArithOp::Sub,
            ..
        } => Prec::Sum,
        Expr::Arith { .. } => Prec::Product,
        Expr::Neg(_) => Prec::Unary,
        _ => Prec::Atom,
    })
}

/// `e` printed where the grammar requires at least `min`.
fn at(e: &Expr, min: Prec) -> String {
    crate::stack::deep(|| {
        if prec(e) < min {
            format!("({})", expr(e))
        } else {
            expr(e)
        }
    })
}

pub fn expr(e: &Expr) -> String {
    crate::stack::deep(|| match e {
        Expr::Literal(l) => literal(l),
        Expr::Variable(v) => format!("${v}"),
        Expr::List { elements, .. } => format!(
            "[{}]",
            elements.iter().map(expr).collect::<Vec<_>>().join(", ")
        ),
        Expr::Prop { variable, property } => format!("${variable}.{property}"),
        Expr::Meta(m) => format!("@{m}"),
        Expr::Ident(i) => i.clone(),
        Expr::Call(c) => call(c),
        Expr::AggDistinct {
            func,
            arg,
            fraction: None,
        } => format!("{}(distinct {})", agg(*func), expr(arg)),
        Expr::AggDistinct {
            func,
            arg,
            fraction: Some(p),
        } => format!("{}(distinct {}, {})", agg(*func), expr(arg), expr(p)),
        Expr::Block(b) => block(b),
        Expr::Neg(inner) => format!("-{}", at(inner, Prec::Unary)),
        Expr::Arith { op, left, right } => {
            let own = prec(e);
            let right_min = if own == Prec::Sum {
                Prec::Product
            } else {
                Prec::Unary
            };
            format!("{} {} {}", at(left, own), op.symbol(), at(right, right_min))
        }
        Expr::Compare {
            op, left, right, ..
        } => format!(
            "{} {} {}",
            at(left, Prec::Sum),
            comp(*op),
            at(right, Prec::Sum)
        ),
        Expr::In { needle, list, .. } => {
            format!("{} in {}", at(needle, Prec::Sum), at(list, Prec::Sum))
        }
        Expr::IsNull {
            expr: inner,
            negated,
        } => format!(
            "{} is {}null",
            at(inner, Prec::Sum),
            if *negated { "not " } else { "" }
        ),
        Expr::Not(inner) => format!("not {}", at(inner, Prec::Not)),
        Expr::And(l, r) => format!("{} and {}", at(l, Prec::And), at(r, Prec::Not)),
        Expr::Or(l, r) => format!("{} or {}", at(l, Prec::Or), at(r, Prec::And)),
        Expr::InfixString {
            op, left, right, ..
        } => format!(
            "{} {} {}",
            at(left, Prec::Sum),
            op.name(),
            at(right, Prec::Sum)
        ),
    })
}

pub fn literal(l: &Literal) -> String {
    match l {
        Literal::Null => "null".to_string(),
        Literal::String(s) => string(s),
        Literal::Integer(i) => i.to_string(),
        // A literal too large for `F64` reads as infinity; this spelling
        // reads back to it.
        Literal::Float(f) if f.is_infinite() => {
            if f.is_sign_negative() {
                "-1.0e999".to_string()
            } else {
                "1.0e999".to_string()
            }
        }
        Literal::Float(f) => {
            let mut s = format!("{f:?}");
            if let Some(e) = s.find('e') {
                if !s[..e].contains('.') {
                    s.insert_str(e, ".0");
                }
            } else if !s.contains('.') {
                s.push_str(".0");
            }
            s
        }
        Literal::Bool(b) => b.to_string(),
        Literal::Date(d) => format!("date({})", string(d)),
        Literal::DateTime(d) => format!("datetime({})", string(d)),
        Literal::List(items) => format!(
            "[{}]",
            items.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
    }
}

pub(crate) fn string(s: &str) -> String {
    let mut out = String::from("\"");
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}
