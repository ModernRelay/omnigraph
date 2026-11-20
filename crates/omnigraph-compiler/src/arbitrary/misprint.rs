//! Misprints: a well-formed GQ 3 text written the way an agent misremembers
//! it, at one place a parse of the text picks, so that the refusal law
//! (`omnigraph-gqt`'s `law_refusals.rs`) can hold the printed fix to the
//! text it was written from: `parse(apply_fixes(misprint(t))) == parse(t)`.
//!
//! Each misprint is a spelling the grammar refuses with a machine-applicable
//! fix. Driven by the parse tree, so a misprint lands only where the
//! construct it misspells stands; a text with no such place has no misprint
//! of that kind.

use pest::Parser as _;
use pest::iterators::Pair;

use crate::query::syntax::{QueryParser, Rule};

/// One misspelling the grammar refuses with a machine-applicable fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Misprint {
    /// `a == b` for the comparison `a = b`.
    DoubleEquals,
    /// `a && b` for the conjunction `a and b`.
    DoubleAmpersand,
    /// `x not in l` for `not x in l`.
    NotIn,
    /// `$a -e-> $b` for the triple `$a e $b`.
    Arrow,
    /// Two elements of a `match` separated by whitespace alone.
    MissingComma,
    /// A `where` predicate written as the last element of its `match`.
    FilterInMatch,
    /// `mutation` for the declaration keyword `query`.
    MutationKeyword,
}

impl Misprint {
    pub const ALL: [Misprint; 7] = [
        Misprint::DoubleEquals,
        Misprint::DoubleAmpersand,
        Misprint::NotIn,
        Misprint::Arrow,
        Misprint::MissingComma,
        Misprint::FilterInMatch,
        Misprint::MutationKeyword,
    ];

    /// The code the misprinted text is refused with.
    pub fn code(self) -> &'static str {
        match self {
            Misprint::DoubleEquals | Misprint::DoubleAmpersand | Misprint::NotIn => "Q014",
            Misprint::Arrow => "Q007",
            Misprint::MissingComma => "Q016",
            Misprint::FilterInMatch => "Q017",
            Misprint::MutationKeyword => "Q009",
        }
    }
}

/// The places in `text` where `form` can be written: each a byte range and
/// its misprinted replacement. Empty when `text` does not parse.
pub fn sites(text: &str, form: Misprint) -> Vec<(usize, usize, String)> {
    let Ok(pairs) = QueryParser::parse(Rule::query_file, text) else {
        return Vec::new();
    };
    let span = |p: &Pair<Rule>| (p.as_span().start(), p.as_span().end());
    let mut out = Vec::new();
    for p in pairs.flatten() {
        match (form, p.as_rule()) {
            (Misprint::DoubleEquals, Rule::comp_op) if p.as_str() == "=" => {
                let (s, e) = span(&p);
                out.push((s, e, "==".to_string()));
            }
            (Misprint::DoubleAmpersand, Rule::and_expr) => {
                for and in p.into_inner().filter(|c| c.as_rule() == Rule::kw_and) {
                    let (s, e) = span(&and);
                    out.push((s, e, "&&".to_string()));
                }
            }
            (Misprint::NotIn, Rule::not_expr) => {
                let parts: Vec<_> = p.clone().into_inner().collect();
                let [not, inner] = parts.as_slice() else {
                    continue;
                };
                if not.as_rule() != Rule::kw_not {
                    continue;
                }
                // `not x in l`, or `not (x in l)`: parentheses leave no node.
                let Some(comparison) = membership(inner) else {
                    continue;
                };
                let parts: Vec<_> = comparison.into_inner().collect();
                if let [x, kw, list] = parts.as_slice()
                    && kw.as_rule() == Rule::kw_in
                    && list.as_rule() == Rule::sum
                {
                    let (s, e) = span(&p);
                    out.push((s, e, format!("{} not in {}", x.as_str(), list.as_str())));
                }
            }
            (Misprint::Arrow, Rule::traversal) => {
                let parts: Vec<_> = p.clone().into_inner().collect();
                if let [a, sel, b] = parts.as_slice()
                    && sel.as_rule() == Rule::edge_sel
                    && sel
                        .clone()
                        .into_inner()
                        .next()
                        .is_some_and(|e| e.as_rule() == Rule::edge_ident)
                {
                    let (s, e) = span(&p);
                    out.push((
                        s,
                        e,
                        format!("{} -{}-> {}", a.as_str(), sel.as_str(), b.as_str()),
                    ));
                }
            }
            (Misprint::MissingComma, Rule::elements) => {
                let elements: Vec<_> = p
                    .clone()
                    .into_inner()
                    .filter(|c| c.as_rule() == Rule::element)
                    .collect();
                for pair in elements.windows(2) {
                    let (gap_start, gap_end) = (pair[0].as_span().end(), pair[1].as_span().start());
                    if text[gap_start..gap_end].trim() == "," {
                        out.push((gap_start, gap_end, " ".to_string()));
                    }
                }
            }
            (Misprint::FilterInMatch, Rule::read_body) => {
                let parts: Vec<_> = p.clone().into_inner().collect();
                let (Some(matched), Some(filter)) = (
                    parts.iter().find(|c| c.as_rule() == Rule::match_clause),
                    parts.iter().find(|c| c.as_rule() == Rule::where_clause),
                ) else {
                    continue;
                };
                let Some(elements) = matched
                    .clone()
                    .into_inner()
                    .find(|c| c.as_rule() == Rule::elements)
                else {
                    continue;
                };
                let Some(predicate) = filter
                    .clone()
                    .into_inner()
                    .find(|c| c.as_rule() == Rule::expr)
                else {
                    continue;
                };
                // The predicate moves to the end of the elements, and the
                // `where` and the text between it and its match go. A span
                // may hold the whitespace after its last token, so each is
                // trimmed.
                let s = elements.as_span().start();
                let e = filter.as_span().start() + filter.as_str().trim_end().len();
                let elements_text = elements.as_str().trim_end().trim_end_matches(',');
                out.push((
                    s,
                    e,
                    format!("{elements_text}, {} }}", predicate.as_str().trim()),
                ));
            }
            (Misprint::MutationKeyword, Rule::query_decl) => {
                if let Some(kw) = p.into_inner().find(|c| c.as_rule() == Rule::kw_query) {
                    let (s, e) = span(&kw);
                    out.push((s, e, "mutation".to_string()));
                }
            }
            _ => {}
        }
    }
    out
}

/// `text` with `form` written at its `pick`-th place, if it has one.
pub fn misprint(text: &str, form: Misprint, pick: usize) -> Option<String> {
    let sites = sites(text, form);
    if sites.is_empty() {
        return None;
    }
    let (start, end, replacement) = &sites[pick % sites.len()];
    // A span may hold the whitespace after its last token; it stays.
    let end = start + text[*start..*end].trim_end().len();
    let mut out = text.to_string();
    out.replace_range(*start..end, replacement.trim_end());
    Some(out)
}

/// The comparison `x in l` that `p` consists of, through single-child
/// nodes and parentheses.
fn membership<'i>(p: &Pair<'i, Rule>) -> Option<Pair<'i, Rule>> {
    let mut p = p.clone();
    loop {
        if p.as_rule() == Rule::comparison
            && p.clone().into_inner().any(|c| c.as_rule() == Rule::kw_in)
        {
            return Some(p);
        }
        let mut inner = p.clone().into_inner();
        let only = inner.next()?;
        if inner.next().is_some() {
            return None;
        }
        p = only;
    }
}
