//! The foreign-habit table of the query front end
//! (docs/rfcs/2026-11-07-gq-3.md, section 5).
//!
//! Some habits an agent brings from other languages are lexical: a grammar
//! rule cannot hold them without admitting them (a single-quoted string,
//! `<>`, `==`, `&&`, `!`, an uppercase keyword, `?x`, an uppercase variable,
//! a `--` or `#` comment, a `;` after a declaration, a `::` segment). When the
//! structural parse refuses a source, [`with_habits`] scans it for these,
//! applies their edits, and reparses. If the result parses, the refusal is the
//! first habit, with every habit's edit as the suggestion. If it is refused
//! with a suggestion of its own, the two edit sets are composed into one
//! suggestion over the original source, so a text that mixes habits with a
//! structural respelling (`ORDER BY $p.age DESC`, `WHERE $p.city = 'x'`) gets
//! one fix.

use pest::Parser;

use super::parse::{Refusal, apply_edits, refusal, refusal_note};
use super::{QueryParser, Rule};
use crate::query::codes::{Q012, Q014, Q015};
use crate::query::diagnostic::{Applicability, QueryCode, TextEdit};

/// Words an uppercase spelling of which is lowercased: GQ's own keywords and
/// the foreign words the grammar's recognizers hold.
const KEYWORDS: &[&str] = &[
    "match", "return", "order", "by", "limit", "as", "asc", "desc", "distinct", "is", "not",
    "null", "and", "or", "in", "let", "from", "optional", "exists", "delete", "insert", "update",
    "set", "where", "filter", "with", "call", "detach", "case", "when", "then", "else", "end",
    "coalesce", "count", "sum", "avg", "min", "max", "starts", "ends", "between", "like", "create",
    "merge", "having", "group", "offset", "skip", "true", "false", "union", "unwind", "for",
    "query", "contains", "if", "concat",
];

/// Registry names, so `!name(…)` is `not name(…)` and not a Datalog atom.
const FUNCTIONS: &[&str] = &[
    "contains",
    "starts_with",
    "coalesce",
    "if",
    "concat",
    "search",
    "fuzzy",
    "match_text",
    "bm25",
    "nearest",
    "rrf",
    "now",
    "date",
    "datetime",
    "terms",
    "match_terms",
    "distance",
    "union",
    "is_null",
    "is_not_null",
];

#[derive(Debug, Clone)]
struct Habit {
    span: (usize, usize),
    edit: Option<TextEdit>,
    message: String,
    note: String,
    code: QueryCode,
    applicability: Applicability,
}

fn habit(span: (usize, usize), replacement: &str, message: impl Into<String>) -> Habit {
    Habit {
        span,
        edit: Some(TextEdit {
            start: span.0,
            end: span.1,
            replacement: replacement.to_string(),
        }),
        message: message.into(),
        note: replacement.to_string(),
        code: Q014,
        applicability: Applicability::MachineApplicable,
    }
}

fn ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Every lexical habit in `input`, in source order. Strings and comments are
/// skipped.
fn scan(input: &str) -> Vec<Habit> {
    let b = input.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let mut depth: i32 = 0;
    while i < b.len() {
        let c = b[i];
        let prev = if i > 0 { b[i - 1] } else { b'\n' };
        match c {
            b'"' => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
                continue;
            }
            b'{' => depth += 1,
            b'}' => depth -= 1,
            b'\'' => {
                if let Some(len) = input[i + 1..].find(['\'', '\n']) {
                    if b[i + 1 + len] == b'\'' {
                        let body = &input[i + 1..i + 1 + len];
                        let replacement = format!("\"{}\"", body.replace('"', "\\\""));
                        out.push(habit(
                            (i, i + len + 2),
                            &replacement,
                            "a string is double-quoted in GQ",
                        ));
                        i += len + 2;
                        continue;
                    }
                }
            }
            b'<' if b.get(i + 1) == Some(&b'>') => {
                out.push(habit(
                    (i, i + 2),
                    "!=",
                    "`<>` is SQL's; GQ's inequality is `!=`",
                ));
                i += 2;
                continue;
            }
            b'=' if b.get(i + 1) == Some(&b'=') => {
                out.push(habit((i, i + 2), "=", "`==` is not GQ; equality is `=`"));
                i += 2;
                continue;
            }
            b'&' if b.get(i + 1) == Some(&b'&') => {
                let pre = if prev.is_ascii_whitespace() { "" } else { " " };
                let post = if b.get(i + 2).is_some_and(u8::is_ascii_whitespace) {
                    ""
                } else {
                    " "
                };
                out.push(habit(
                    (i, i + 2),
                    &format!("{pre}and{post}"),
                    "`&&` is not GQ; conjunction is `and`",
                ));
                i += 2;
                continue;
            }
            b'!' if b.get(i + 1) != Some(&b'=') => {
                if let Some(h) = datalog_negation(input, i) {
                    i = h.span.1;
                    out.push(h);
                    continue;
                }
                let post = if b.get(i + 1).is_some_and(u8::is_ascii_whitespace) {
                    ""
                } else {
                    " "
                };
                out.push(habit(
                    (i, i + 1),
                    &format!("not{post}"),
                    "`!` is not GQ; negation is `not`",
                ));
            }
            b'-' if b.get(i + 1) == Some(&b'-')
                && prev.is_ascii_whitespace()
                && b.get(i + 2).is_some_and(|c| *c == b' ') =>
            {
                out.push(habit(
                    (i, i + 2),
                    "//",
                    "a comment is `//` or `/* … */` in GQ",
                ));
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'#' => {
                out.push(habit(
                    (i, i + 1),
                    "//",
                    "a comment is `//` or `/* … */` in GQ",
                ));
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'?' if b.get(i + 1).is_some_and(u8::is_ascii_lowercase)
                && !(ident_char(prev) || prev == b')' || prev == b']') =>
            {
                out.push(habit(
                    (i, i + 1),
                    "$",
                    "a variable is written `$name`; `?name` is SPARQL's",
                ));
            }
            b'$' if input[..i].trim_end().ends_with(':')
                && input[..i].trim_end()[..input[..i].trim_end().len() - 1]
                    .trim_end()
                    .rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
                    .next()
                    .is_some_and(|w| w.starts_with('$') && w.len() > 1)
                && depth > 0 =>
            {
                out.push(Habit {
                    span: (i, i + 1),
                    edit: None,
                    message: "the type-variable binding `$x: $t` is the polymorphic types stage (Poly); a binding names one type now".into(),
                    note: "$x: <Type>".into(),
                    code: Q015,
                    applicability: Applicability::NeedsReview,
                });
            }
            b'$' if b.get(i + 1).is_some_and(u8::is_ascii_uppercase) => {
                let lower = (b[i + 1] as char).to_ascii_lowercase().to_string();
                let mut h = habit(
                    (i + 1, i + 2),
                    &lower,
                    "a variable name starts with a lowercase letter",
                );
                h.span = (i, i + 2);
                out.push(h);
            }
            b'$' if b.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                out.push(Habit {
                    span: (i, i + 2),
                    edit: None,
                    message: "`$1` is not a variable: a variable starts with a letter, and a parameter is declared by name, `$name: Type`".into(),
                    note: "$name".into(),
                    code: Q014,
                    applicability: Applicability::NeedsReview,
                });
            }
            b'`' => {
                out.push(Habit {
                    span: (i, i + 1),
                    edit: None,
                    message: "a backticked name is not GQ; a property name is a plain identifier"
                        .into(),
                    note: "$p.full_name".into(),
                    code: Q014,
                    applicability: Applicability::NeedsReview,
                });
            }
            b';' if depth == 0 && input[..i].trim_end().ends_with('}') => {
                let start = input[..i].trim_end().len();
                out.push(habit(
                    (start, i + 1),
                    "",
                    "a declaration ends at its `}`; no `;` follows it",
                ));
            }
            c if (c.is_ascii_alphabetic() || c == b'_') && !ident_char(prev) => {
                let end = i + input[i..]
                    .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                    .unwrap_or(input.len() - i);
                let word = &input[i..end];
                let after_colons = input[end..].starts_with("::");
                if let Some(mut h) = later_word(input, i, end, prev, depth) {
                    let close = h
                        .note
                        .strip_prefix("close:")
                        .and_then(|n| n.parse::<usize>().ok());
                    if let Some(close) = close {
                        h.note.clear();
                        out.push(h.clone());
                        out.push(Habit {
                            span: (close, close + 1),
                            edit: Some(TextEdit {
                                start: close,
                                end: close + 1,
                                replacement: String::new(),
                            }),
                            ..h
                        });
                    } else {
                        out.push(h);
                    }
                    i = end;
                    continue;
                }
                if after_colons && !matches!(prev, b'.' | b'$' | b'@') {
                    out.push(Habit {
                        span: (i, end + 2),
                        edit: Some(TextEdit {
                            start: i,
                            end: end + 2,
                            replacement: String::new(),
                        }),
                        message: format!(
                            "`{word}::` is a schema namespace, which the schema namespaces RFC adds after this release"
                        ),
                        note: String::new(),
                        code: Q015,
                        applicability: Applicability::NeedsReview,
                    });
                } else if word.len() >= 2
                    && word.bytes().all(|c| c.is_ascii_uppercase() || c == b'_')
                    && !matches!(prev, b'.' | b'$' | b'@' | b':')
                    && KEYWORDS.contains(&word.to_ascii_lowercase().as_str())
                {
                    out.push(habit(
                        (i, end),
                        &word.to_ascii_lowercase(),
                        format!("GQ keywords are lowercase: `{}`", word.to_ascii_lowercase()),
                    ));
                }
                i = end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    out
}

/// The span of the braced block that starts after `from` (whitespace
/// first), through its matching `}`.
fn braced_block(input: &str, from: usize) -> Option<usize> {
    let rest = &input[from..];
    let open = from + (rest.len() - rest.trim_start().len());
    if !input[open..].starts_with('{') {
        return None;
    }
    let mut depth = 0;
    for (k, c) in input[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + k + 1);
                }
            }
            _ => {}
        }
    }
    None
}

/// A form a later stage adds, refused naming the stage (section 5).
fn later_word(input: &str, start: usize, end: usize, prev: u8, depth: i32) -> Option<Habit> {
    if matches!(prev, b'.' | b'$' | b'@') {
        return None;
    }
    let word = &input[start..end];
    let note = |message: &str, edit: Option<TextEdit>| Habit {
        span: (start, end),
        edit,
        message: message.to_string(),
        note: String::new(),
        code: Q015,
        applicability: Applicability::NeedsReview,
    };
    let delete_block = |close: usize| {
        let (s, e) = (start, close);
        let e = e + (input[e..].len() - input[e..].trim_start().len());
        Some(TextEdit {
            start: s,
            end: e,
            replacement: String::new(),
        })
    };
    let after = input[end..].trim_start();
    match word {
        "returning" => {
            let close = braced_block(input, end)?;
            Some(note(
                "`returning { … }` is the writes RFC's (stage W1); until it lands a write reports no rows",
                delete_block(close),
            ))
        }
        "assert" if after.starts_with('{') => {
            let close = braced_block(input, end)?;
            Some(note(
                "`assert { read }` is the writes RFC's sink (stage W1)",
                delete_block(close),
            ))
        }
        "batch" if depth == 0 && after.starts_with('{') => {
            let close = braced_block(input, end)?;
            let open = end + (input[end..].len() - after.len());
            let mut h = note(
                "`batch` was withdrawn: several declarations in one request already share its snapshot (a request shape, stage N); write the declarations one after another",
                Some(TextEdit {
                    start,
                    end: open + 1,
                    replacement: String::new(),
                }),
            );
            h.span = (start, close);
            h.note = format!("close:{}", close - 1);
            Some(h)
        }
        "is" if after.starts_with(|c: char| c.is_ascii_uppercase()) => Some(note(
            "`$x is T` is the polymorphic types stage (Poly); a binding `$x: T` selects one type now",
            None,
        )),
        "Commit" | "Branch" if depth == 0 && input[..start].trim_end().ends_with(':') => {
            Some(note(
                "the `Branch` and `Commit` parameter kinds are a later change after the break; a commit id is a `String` parameter now",
                Some(TextEdit {
                    start,
                    end,
                    replacement: "String".into(),
                }),
            ))
        }
        // SQL's `union` between two reads: GQ draws the union of two
        // relations with the `union` relation function.
        "union"
            if depth == 1
                && input[..start].trim_end().ends_with('}')
                && after.starts_with("match") =>
        {
            Some(Habit {
                code: Q014,
                note: "match { from union({ match { … } return { $x } }, { match { … } return { $x } }) } return { $x }".into(),
                ..note(
                    "`union` between two reads is SQL's; GQ draws the union of two reads with `from union({ … }, { … })`",
                    None,
                )
            })
        }
        "gq" if after.starts_with(|c: char| c.is_ascii_digit()) =>
        {
            let semi = end + input[end..].find(';')? + 1;
            let semi = semi + (input[semi..].len() - input[semi..].trim_start().len());
            Some(Habit {
                span: (start, semi),
                code: Q012,
                ..note(
                    "a source has no edition line; delete it",
                    Some(TextEdit {
                        start,
                        end: semi,
                        replacement: String::new(),
                    }),
                )
            })
        }
        _ => None,
    }
}

/// `!knows($p, $q)`, Datalog's negated atom over an edge: `not { $p knows $q }`.
fn datalog_negation(input: &str, at: usize) -> Option<Habit> {
    let rest = &input[at + 1..];
    let name_len = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))?;
    let name = &rest[..name_len];
    if name.is_empty() || FUNCTIONS.contains(&name) || !rest[name_len..].starts_with('(') {
        return None;
    }
    let close = rest.find(')')?;
    let args: Vec<&str> = rest[name_len + 1..close]
        .split(',')
        .map(str::trim)
        .collect();
    if args.len() != 2 || !args.iter().all(|a| a.starts_with('$') && a.len() > 1) {
        return None;
    }
    let end = at + 1 + close + 1;
    Some(habit(
        (at, end),
        &format!("not {{ {} {name} {} }}", args[0], args[1]),
        "`!edge(a, b)` is Datalog's negated atom; GQ writes the triple in `not { … }`",
    ))
}

/// Text with provenance: each byte's offset in the original, or `None` for
/// inserted text.
struct Tracked {
    text: Vec<u8>,
    prov: Vec<Option<usize>>,
}

impl Tracked {
    fn new(s: &str) -> Self {
        Self {
            text: s.as_bytes().to_vec(),
            prov: (0..s.len()).map(Some).collect(),
        }
    }

    fn apply(&mut self, edits: &[TextEdit]) {
        let mut sorted: Vec<&TextEdit> = edits.iter().collect();
        sorted.sort_by_key(|e| std::cmp::Reverse((e.start, e.end)));
        for e in sorted {
            let bytes = e.replacement.as_bytes();
            self.text.splice(e.start..e.end, bytes.iter().copied());
            self.prov
                .splice(e.start..e.end, std::iter::repeat_n(None, bytes.len()));
        }
    }

    /// A position in this text as a position in the original: a start maps
    /// to the first original byte at or after it, an end to one past the last
    /// original byte before it.
    fn map_start(&self, pos: usize, original_len: usize) -> usize {
        self.prov[pos.min(self.prov.len())..]
            .iter()
            .find_map(|p| *p)
            .unwrap_or(original_len)
    }

    fn map_end(&self, pos: usize) -> usize {
        self.prov[..pos.min(self.prov.len())]
            .iter()
            .rev()
            .find_map(|p| p.map(|o| o + 1))
            .unwrap_or(0)
    }

    /// The edits that turn the original into this text: each run of bytes
    /// whose provenance is not the next original byte is one edit.
    fn edits(&self, original: &str) -> Vec<TextEdit> {
        let mut out = Vec::new();
        let mut o = 0;
        let mut cur: Option<(usize, Vec<u8>)> = None;
        let flush = |cur: &mut Option<(usize, Vec<u8>)>, end: usize, out: &mut Vec<TextEdit>| {
            if let Some((s, rep)) = cur.take() {
                out.push(TextEdit {
                    start: s,
                    end,
                    replacement: String::from_utf8_lossy(&rep).into_owned(),
                });
            }
        };
        for (k, byte) in self.text.iter().enumerate() {
            match self.prov[k] {
                Some(j) if j == o && cur.is_none() => o += 1,
                Some(j) if j >= o => {
                    let start = cur.as_ref().map_or(o, |c| c.0);
                    let rep = cur.take().map(|c| c.1).unwrap_or_default();
                    cur = Some((start, rep));
                    flush(&mut cur, j, &mut out);
                    o = j + 1;
                }
                _ => cur.get_or_insert_with(|| (o, Vec::new())).1.push(*byte),
            }
        }
        if cur.is_some() || o < original.len() {
            let start = cur.as_ref().map_or(o, |c| c.0);
            let rep = cur.take().map(|c| c.1).unwrap_or_default();
            cur = Some((start, rep));
            flush(&mut cur, original.len(), &mut out);
        }
        out.retain(|e| !(e.start == e.end && e.replacement.is_empty()));
        out
    }
}

/// The innermost clause, item, key, statement or assignment of `text` that
/// holds byte `pos`.
fn construct_at(text: &str, pos: usize) -> Option<(usize, usize)> {
    let pairs = QueryParser::parse(Rule::query_file, text).ok()?;
    let mut best: Option<(usize, usize)> = None;
    for p in pairs.flatten() {
        if !matches!(
            p.as_rule(),
            Rule::clause
                | Rule::projection
                | Rule::ordering
                | Rule::mutation_stmt
                | Rule::assignment
                | Rule::alias
                | Rule::param
                | Rule::annotation
                | Rule::limit_clause
                | Rule::return_clause
                | Rule::match_clause
        ) {
            continue;
        }
        let (s, e) = (p.as_span().start(), p.as_span().end());
        if s <= pos && pos < e.max(s + 1) && best.is_none_or(|(bs, be)| e - s < be - bs) {
            best = Some((s, e));
        }
    }
    best
}

/// The refusal of a source whose structural refusal is `first`, with the
/// lexical habits it holds folded in.
pub(crate) fn with_habits(
    input: &str,
    first: Refusal,
    reparse: fn(&str) -> Option<Refusal>,
) -> Refusal {
    let habits = scan(input);
    let Some(lead) = habits.first().cloned() else {
        return first;
    };
    let e1: Vec<TextEdit> = habits.iter().filter_map(|h| h.edit.clone()).collect();
    let applicability = if habits
        .iter()
        .any(|h| h.applicability == Applicability::NeedsReview)
    {
        Applicability::NeedsReview
    } else {
        Applicability::MachineApplicable
    };
    if e1.is_empty() {
        return refusal_note(input, lead.code, lead.message, lead.span, lead.note);
    }
    let mut t1_track = Tracked::new(input);
    t1_track.apply(&e1);
    let t1 = apply_edits(input, &e1);
    // The lead habit's position in the edited text.
    let delta: isize = e1
        .iter()
        .filter(|e| e.end <= lead.span.0)
        .map(|e| e.replacement.len() as isize - (e.end - e.start) as isize)
        .sum();
    let lead_t1 = (lead.span.0 as isize + delta).max(0) as usize;
    let lead_span = construct_at(&t1, lead_t1)
        .map(|(s, e)| (t1_track.map_start(s, input.len()), t1_track.map_end(e)))
        .map(|(s, e)| (s.min(lead.span.0), e.max(lead.span.1)))
        .unwrap_or(lead.span);
    let r2 = match reparse(&t1) {
        Some(r2) if !r2.edits().is_empty() && r2.span.is_some() => r2,
        _ => return refusal(input, lead.code, lead.message, lead_span, e1, applicability),
    };
    let (rs, re) = r2.span.unwrap();
    let r2_span = (t1_track.map_start(rs, input.len()), t1_track.map_end(re));
    let app2 = r2
        .diagnostic
        .suggestion
        .as_ref()
        .map_or(Applicability::NeedsReview, |s| s.applicability);
    let mut t2_track = t1_track;
    t2_track.apply(r2.edits());
    let composed = t2_track.edits(input);
    let app = if applicability == Applicability::NeedsReview || app2 == Applicability::NeedsReview {
        Applicability::NeedsReview
    } else {
        Applicability::MachineApplicable
    };
    let overlaps = lead_span.0 < r2_span.1 && r2_span.0 < lead_span.1;
    if overlaps {
        let span = (r2_span.0.min(lead_span.0), r2_span.1.max(lead_span.1));
        // The structural refusal names the construct; the habit is inside it.
        refusal(
            input,
            r2.diagnostic.code,
            r2.diagnostic.message,
            span,
            composed,
            app,
        )
    } else if r2_span.0 < lead_span.0 {
        refusal(
            input,
            r2.diagnostic.code,
            r2.diagnostic.message,
            r2_span,
            composed,
            app,
        )
    } else {
        refusal(input, lead.code, lead.message, lead_span, composed, app)
    }
}
