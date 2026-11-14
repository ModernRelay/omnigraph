//! Parse recovery (docs/rfcs/2026-11-07-gq-3.md, section 5): after a parse
//! refusal the parser resumes at the next element, item, statement or
//! declaration boundary, so the independent parse refusals of one source are
//! all reported.
//!
//! The grammar has no recovery of its own: a source it refuses has no tree.
//! The boundaries are found here by a lexical scan that needs no tree, only
//! balanced brackets, strings and comments: the elements of a `match`, a
//! block or an `optional`, the items of a `return` or an `order`, the
//! assignments of a statement, the arguments of a call, the parameters of a
//! declaration, the predicate of a `where`, the statements of a write body,
//! and the declarations of the source. The innermost such unit holding a
//! refusal's position is its region. The unit is then neutralized, its bytes
//! replaced, keeping every byte offset, by spaces (with the comma that
//! separates it from a sibling) or by a placeholder the grammar accepts in
//! that position when it has no sibling, and the source is parsed again: the
//! next refusal is the next independent one. A placeholder is never typed,
//! since a source with a parse refusal stops at parsing.

/// The kind of boundary a unit stands between.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnitKind {
    /// A declaration or a statement of the source, or the text before the
    /// first one.
    Declaration,
    /// An element of `match { }`, a block or an `optional`.
    Element,
    /// A return item or an order key.
    Item,
    /// An assignment of `insert` or `update`.
    Assignment,
    /// An argument of a call.
    Argument,
    /// A parameter of a declaration.
    Parameter,
    /// A `where` and its predicate.
    Where,
    /// A statement of a write body.
    Statement,
}

/// One unit: its byte range, its kind, and the comma-separated list it
/// belongs to, when it does.
#[derive(Debug, Clone)]
pub(crate) struct Unit {
    pub start: usize,
    pub end: usize,
    pub kind: UnitKind,
    /// The index of its list among [`Scan::lists`].
    list: Option<usize>,
    /// A declaration unit of whitespace alone, which is no unit.
    blank: bool,
}

/// The units of one comma-separated list, and the comma after each.
#[derive(Debug, Default, Clone)]
struct List {
    /// Each member's unit index and the byte of the comma after it.
    members: Vec<(usize, Option<usize>)>,
}

#[derive(Debug, Default)]
struct Scan {
    units: Vec<Unit>,
    lists: Vec<List>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Frame {
    /// The source itself.
    Top,
    /// A declaration's body or an inline read's.
    Body,
    /// A list of elements, then an optional `where`.
    Elements,
    /// A comma-separated list of units of one kind.
    List(UnitKind),
    /// Brackets holding no unit of their own.
    Paren,
}

struct Open {
    frame: Frame,
    /// Where the current member of a list began.
    member: usize,
    list: Option<usize>,
    /// A `where` or a statement of a body, or the predicate after an
    /// elements list's `where`: its kind and start.
    clause: Option<(UnitKind, usize)>,
    /// The start of the top-level unit being scanned.
    top: Option<usize>,
}

impl Open {
    fn new(frame: Frame, member: usize) -> Self {
        Self {
            frame,
            member,
            list: None,
            clause: None,
            top: None,
        }
    }
}

/// A word token: its text and start.
type Word<'a> = (&'a str, usize);

const AGGREGATES: &[&str] = &["count", "sum", "avg", "min", "max"];

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn keyword(word: &str, wanted: &[&str]) -> bool {
    wanted.iter().any(|w| word.eq_ignore_ascii_case(w))
}

impl Scan {
    fn of(text: &str) -> Self {
        let b = text.as_bytes();
        let mut scan = Scan::default();
        let mut stack: Vec<Open> = vec![Open::new(Frame::Top, 0)];
        // The last two significant tokens before the current one: a word,
        // or a punctuation byte as a one-byte word.
        let mut prev: Option<Word> = None;
        let mut prev2: Option<Word> = None;
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            let start = i;
            // Whitespace, comments and strings.
            if c.is_ascii_whitespace() {
                i += 1;
                continue;
            }
            if c == b'/' && b.get(i + 1) == Some(&b'/') {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            if c == b'/' && b.get(i + 1) == Some(&b'*') {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(b.len());
                continue;
            }
            if c == b'"' || c == b'\'' {
                i += 1;
                while i < b.len() && b[i] != c {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i = (i + 1).min(b.len());
                prev2 = prev;
                prev = Some(("\"", start));
                continue;
            }
            if is_word_byte(c) || c == b'$' || c == b'@' {
                i += 1;
                while i < b.len() && is_word_byte(b[i]) {
                    i += 1;
                }
                let word = &text[start..i];
                scan.word(&mut stack, word, start, prev);
                prev2 = prev;
                prev = Some((word, start));
                continue;
            }
            i += 1;
            match c {
                b'{' => {
                    let frame = if stack.len() == 1 {
                        Frame::Body
                    } else {
                        brace_frame(prev, prev2)
                    };
                    scan.open(&mut stack, frame, i);
                }
                b'(' => {
                    let frame = match prev {
                        Some((w, _))
                            if stack.len() == 1
                                && prev2.is_some_and(|(p, _)| {
                                    keyword(p, &["query", "mutation", "view"])
                                })
                                && !w.starts_with('$') =>
                        {
                            Frame::List(UnitKind::Parameter)
                        }
                        Some((w, _))
                            if (w.starts_with('@')
                                || w.as_bytes()[0].is_ascii_lowercase()
                                || w.as_bytes()[0] == b'_')
                                && !keyword(w, &["and", "or", "not", "in", "is"]) =>
                        {
                            Frame::List(UnitKind::Argument)
                        }
                        _ => Frame::Paren,
                    };
                    scan.open(&mut stack, frame, i);
                }
                b'[' => scan.open(&mut stack, Frame::Paren, i),
                b'}' | b')' | b']' => {
                    if stack.len() > 1 {
                        scan.close(&mut stack, start);
                    }
                }
                b',' => scan.comma(&mut stack, start),
                _ => {}
            }
            prev2 = prev;
            prev = Some((&text[start..i], start));
        }
        while stack.len() > 1 {
            scan.close(&mut stack, b.len());
        }
        if let Some(top) = stack[0].top
            && !text[top..].trim().is_empty()
        {
            scan.push(top, b.len(), UnitKind::Declaration, None);
        }
        // A declaration unit of whitespace alone holds no refusal.
        for u in &mut scan.units {
            u.blank = u.kind == UnitKind::Declaration && text[u.start..u.end].trim().is_empty();
        }
        scan
    }

    fn push(&mut self, start: usize, end: usize, kind: UnitKind, list: Option<usize>) -> usize {
        self.units.push(Unit {
            start,
            end,
            kind,
            list,
            blank: false,
        });
        self.units.len() - 1
    }

    fn open(&mut self, stack: &mut Vec<Open>, frame: Frame, after: usize) {
        let mut open = Open::new(frame, after);
        if matches!(frame, Frame::List(_) | Frame::Elements) {
            self.lists.push(List::default());
            open.list = Some(self.lists.len() - 1);
        }
        stack.push(open);
    }

    /// The member of a list ending at `end`, and the comma after it.
    fn member(&mut self, open: &mut Open, end: usize, comma: Option<usize>) {
        let kind = match open.frame {
            Frame::List(kind) => kind,
            Frame::Elements => UnitKind::Element,
            _ => return,
        };
        let list = open.list.expect("a list frame holds its list");
        let unit = self.push(open.member, end, kind, Some(list));
        self.lists[list].members.push((unit, comma));
        open.member = comma.map_or(end, |c| c + 1);
    }

    fn close_clause(&mut self, open: &mut Open, end: usize) {
        if let Some((kind, start)) = open.clause.take() {
            self.push(start, end, kind, None);
        }
    }

    fn close(&mut self, stack: &mut Vec<Open>, at: usize) {
        let mut open = stack.pop().expect("a frame to close");
        match open.frame {
            Frame::Elements if open.clause.is_some() => self.close_clause(&mut open, at),
            Frame::List(_) | Frame::Elements => self.member(&mut open, at, None),
            Frame::Body => self.close_clause(&mut open, at),
            Frame::Top | Frame::Paren => {}
        }
    }

    fn comma(&mut self, stack: &mut [Open], at: usize) {
        let open = stack.last_mut().expect("the top frame");
        if matches!(open.frame, Frame::List(_))
            || (open.frame == Frame::Elements && open.clause.is_none())
        {
            self.member(open, at, Some(at));
        }
    }

    fn word(&mut self, stack: &mut [Open], word: &str, start: usize, prev: Option<Word>) {
        // A property name after `.` is never a keyword.
        if prev.is_some_and(|(p, _)| p == ".") {
            return;
        }
        let open = stack.last_mut().expect("the top frame");
        match open.frame {
            Frame::Top => {
                let starts = keyword(word, &["query", "mutation", "view", "explain"])
                    && !prev.is_some_and(|(p, _)| keyword(p, &["explain"]));
                let top = open.top.unwrap_or(0);
                if starts && top < start {
                    self.push(top, start, UnitKind::Declaration, None);
                }
                if starts || open.top.is_none() {
                    open.top = Some(if starts { start } else { top });
                }
            }
            Frame::Body => {
                if keyword(word, &["match", "where", "return", "order", "limit"])
                    || keyword(word, &["insert", "update", "delete"])
                {
                    self.close_clause(open, start);
                    if keyword(word, &["where"]) {
                        open.clause = Some((UnitKind::Where, start));
                    } else if keyword(word, &["insert", "update", "delete"]) {
                        open.clause = Some((UnitKind::Statement, start));
                    }
                }
            }
            Frame::Elements => {
                if keyword(word, &["where"]) && open.clause.is_none() {
                    self.member(open, start, None);
                    open.clause = Some((UnitKind::Where, start));
                }
            }
            Frame::List(_) | Frame::Paren => {}
        }
    }

    /// The innermost unit holding byte `at`; failing that, the innermost
    /// ending at it (a refusal where a list's last member stops short).
    fn innermost(&self, at: usize) -> Option<usize> {
        let smallest = |holds: &dyn Fn(&Unit) -> bool| {
            self.units
                .iter()
                .enumerate()
                .filter(|(_, u)| !u.blank && holds(u))
                .min_by_key(|(_, u)| u.end - u.start)
                .map(|(i, _)| i)
        };
        smallest(&|u| u.start <= at && at < u.end)
            .or_else(|| smallest(&|u| u.start < u.end && at == u.end))
    }

    /// The smallest unit strictly holding `inner`.
    fn parent(&self, inner: (usize, usize)) -> Option<usize> {
        self.units
            .iter()
            .enumerate()
            .filter(|(_, u)| {
                !u.blank && u.start <= inner.0 && inner.1 <= u.end && (u.start, u.end) != inner
            })
            .min_by_key(|(_, u)| u.end - u.start)
            .map(|(i, _)| i)
    }
}

/// The frame a `{` opens, read from the two tokens before it.
fn brace_frame(prev: Option<Word>, prev2: Option<Word>) -> Frame {
    let Some((p, _)) = prev else {
        return Frame::Body;
    };
    if keyword(p, &["match", "optional", "exists", "not"]) || keyword(p, AGGREGATES) {
        return Frame::Elements;
    }
    if keyword(p, &["return", "distinct", "order"]) {
        return Frame::List(UnitKind::Item);
    }
    if keyword(p, &["set"]) {
        return Frame::List(UnitKind::Assignment);
    }
    if p.as_bytes()[0].is_ascii_uppercase() {
        return Frame::List(if prev2.is_some_and(|(w, _)| keyword(w, &["insert"])) {
            UnitKind::Assignment
        } else {
            UnitKind::Item
        });
    }
    // An aggregate block with its argument: `sum($x.p) { … }`.
    if p == ")" {
        return Frame::Elements;
    }
    Frame::Body
}

/// What [`neutralize`] did: the region it replaced.
pub(crate) struct Neutralized {
    pub text: String,
    pub region: (usize, usize),
}

/// The region of a refusal at byte `at`: the innermost unit holding it, the
/// whole text when none does.
pub(crate) fn region_at(text: &str, at: usize) -> (usize, usize) {
    let scan = Scan::of(text);
    scan.innermost(at)
        .map(|u| (scan.units[u].start, scan.units[u].end))
        .unwrap_or((0, text.len()))
}

/// `text` with the innermost unit holding byte `at` neutralized; `None` when
/// no unit holds it.
pub(crate) fn neutralize(text: &str, at: usize) -> Option<Neutralized> {
    let scan = Scan::of(text);
    let unit = scan.innermost(at)?;
    neutralize_unit(text, &scan, unit)
}

/// `text` with the smallest unit strictly holding `region` neutralized: a
/// neutralization that left its own region refused escalates to the unit
/// around it.
pub(crate) fn neutralize_parent(text: &str, region: (usize, usize)) -> Option<Neutralized> {
    let scan = Scan::of(text);
    let unit = scan.parent(region)?;
    neutralize_unit(text, &scan, unit)
}

fn blank(text: &mut [u8], start: usize, end: usize) {
    for b in &mut text[start..end] {
        if *b != b'\n' {
            *b = b' ';
        }
    }
}

fn neutralize_unit(text: &str, scan: &Scan, unit: usize) -> Option<Neutralized> {
    let u = &scan.units[unit];
    let mut bytes = text.as_bytes().to_vec();
    let empty = |u: &Unit| text[u.start..u.end].trim().is_empty();
    // A member with a sibling: blank it with the comma that separates them.
    if let Some(list) = u.list {
        let members = &scan.lists[list].members;
        let others = members
            .iter()
            .filter(|(m, _)| *m != unit && !empty(&scan.units[*m]))
            .count();
        if others > 0 {
            let at = members.iter().position(|(m, _)| *m == unit)?;
            let (_, comma) = members[at];
            let (start, end) = match comma {
                Some(c) => (u.start, c + 1),
                None => match at.checked_sub(1).and_then(|p| members[p].1) {
                    Some(c) => (c, u.end),
                    None => (u.start, u.end),
                },
            };
            blank(&mut bytes, start, end);
            return finish(bytes, (start, end));
        }
    }
    let placeholder: &[&str] = match u.kind {
        UnitKind::Declaration | UnitKind::Parameter | UnitKind::Argument | UnitKind::Where => &[""],
        UnitKind::Element => &["let $_=1", "$_:T"],
        UnitKind::Item => &["1"],
        UnitKind::Assignment => &["a:1"],
        UnitKind::Statement => &["delete $_"],
    };
    let len = u.end - u.start;
    let fit = placeholder.iter().find(|p| p.len() <= len)?;
    blank(&mut bytes, u.start, u.end);
    // A placeholder starts after the unit's leading whitespace, where the
    // unit's own text started, so a word before it stays separate.
    let lead = text[u.start..u.end].len() - text[u.start..u.end].trim_start().len();
    let at = if lead + fit.len() <= len && lead > 0 {
        u.start + lead
    } else {
        u.start
    };
    let at = if at > 0 && is_word_byte(bytes[at - 1]) && !fit.is_empty() {
        // Keep the placeholder apart from a word before it.
        if fit.len() < len {
            at + 1
        } else {
            return None;
        }
    } else {
        at
    };
    bytes[at..at + fit.len()].copy_from_slice(fit.as_bytes());
    finish(bytes, (u.start, u.end))
}

fn finish(bytes: Vec<u8>, region: (usize, usize)) -> Option<Neutralized> {
    // Only ASCII bytes were written over whole units, and a unit starts and
    // ends at a token boundary, so the text stays UTF-8.
    String::from_utf8(bytes)
        .ok()
        .map(|text| Neutralized { text, region })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units(text: &str) -> Vec<(UnitKind, &str)> {
        let scan = Scan::of(text);
        let mut out: Vec<(usize, UnitKind, &str)> = scan
            .units
            .iter()
            .map(|u| (u.start, u.kind, text[u.start..u.end].trim()))
            .collect();
        out.sort_by_key(|(s, k, _)| (*s, *k == UnitKind::Declaration));
        out.into_iter().map(|(_, k, t)| (k, t)).collect()
    }

    #[test]
    fn a_read_splits_into_its_units() {
        let text = "query q($a: String, $b: I64) { match { $p: Person, $p knows $q } where $p.age > 3 return { $p.name, count($q) } order { $p.name } }";
        let got = units(text);
        let kinds: Vec<(UnitKind, &str)> = got
            .into_iter()
            .filter(|(k, _)| *k != UnitKind::Declaration)
            .collect();
        assert_eq!(
            kinds,
            vec![
                (UnitKind::Parameter, "$a: String"),
                (UnitKind::Parameter, "$b: I64"),
                (UnitKind::Element, "$p: Person"),
                (UnitKind::Element, "$p knows $q"),
                (UnitKind::Where, "where $p.age > 3"),
                (UnitKind::Item, "$p.name"),
                (UnitKind::Item, "count($q)"),
                (UnitKind::Argument, "$q"),
                (UnitKind::Item, "$p.name"),
            ]
        );
    }

    #[test]
    fn a_write_splits_into_its_statements() {
        let text = "query w($n: String) { insert Person { handle: $n, name: $n } update $p set { age: 3 } }";
        let got: Vec<(UnitKind, &str)> = units(text)
            .into_iter()
            .filter(|(k, _)| matches!(k, UnitKind::Statement | UnitKind::Assignment))
            .collect();
        assert_eq!(
            got,
            vec![
                (
                    UnitKind::Statement,
                    "insert Person { handle: $n, name: $n }"
                ),
                (UnitKind::Assignment, "handle: $n"),
                (UnitKind::Assignment, "name: $n"),
                (UnitKind::Statement, "update $p set { age: 3 }"),
                (UnitKind::Assignment, "age: 3"),
            ]
        );
    }

    #[test]
    fn neutralizing_keeps_every_offset() {
        let text = "query q() { match { $p: Person, $p -knows-> $q } return { $p } }";
        let at = text.find("-knows").unwrap();
        let n = neutralize(text, at).unwrap();
        assert_eq!(n.text.len(), text.len());
        assert_eq!(
            n.text,
            "query q() { match { $p: Person                 } return { $p } }"
        );
    }

    #[test]
    fn a_lone_unit_takes_a_placeholder() {
        let text = "query q() { match { $p -knows-> $q } return { $p } }";
        let at = text.find("-knows").unwrap();
        let n = neutralize(text, at).unwrap();
        assert_eq!(
            n.text,
            "query q() { match { let $_=1       } return { $p } }"
        );
    }
}
