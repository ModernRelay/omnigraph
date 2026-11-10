//! The nesting bound a query source meets before the grammar runs (`Q006`).
//!
//! The parser, the typechecker and every later stage recurse once per level
//! of an expression tree, so an unbounded source can exhaust the stack of the
//! thread compiling it, which aborts the process instead of refusing the
//! request. A chain `a or b or c` is a left-deep tree as deep as it is long,
//! so its operators are levels too. This module bounds the depth with one
//! linear scan over the bytes, before any recursion: it skips string
//! literals and comments as the grammar does, opens a level at `(`, `{` and
//! `[`, charges a level for each `not` in a run of adjacent `not`s, and
//! charges each `and`/`or` to the total. The estimate never undercounts the
//! tree a source can build; it overcounts operators of sibling clauses that
//! share a level, which only matters for a source near the bound.

use super::codes::Q006;
use super::diagnostic::{Position, QueryDiagnostic};

/// The deepest nesting of brackets and adjacent `not`s a query source may
/// hold. Each such level costs several frames in the grammar alone.
pub const MAX_QUERY_NESTING: usize = 64;

/// The deepest expression a query source may build, counting every
/// `and`/`or` of a chain as one level on top of the nesting. A long `or`
/// chain over one property is written `$p.name in [...]`, which is one level
/// whatever its length.
pub const MAX_QUERY_EXPRESSION_DEPTH: usize = 256;

#[derive(Default)]
struct Level {
    /// The `not` run directly before this level's opening bracket.
    lead: usize,
    /// The longest run of adjacent `not` keywords at this level.
    nots: usize,
    /// The `not` run in progress, ended by any other token.
    run: usize,
    /// `and`/`or` operators at this level.
    chain: usize,
    /// The deepest completed child, in nesting and in total.
    child_nesting: usize,
    child_total: usize,
}

/// Where a source first passed a bound, and which one.
#[derive(Debug, PartialEq, Eq)]
enum Exceeded {
    Nesting(usize),
    Total(usize),
}

/// Refuse `input` when the expression tree it can build passes
/// [`MAX_QUERY_NESTING`] or [`MAX_QUERY_EXPRESSION_DEPTH`], before the
/// grammar recurses over it.
pub(crate) fn check_query_nesting(input: &str) -> Result<(), QueryDiagnostic> {
    let (byte, message) = match measure(input, MAX_QUERY_NESTING, MAX_QUERY_EXPRESSION_DEPTH) {
        Ok(_) => return Ok(()),
        Err(Exceeded::Nesting(byte)) => (
            byte,
            format!(
                "the query nests deeper than {MAX_QUERY_NESTING} levels: every bracket and every `not` in a row is one level"
            ),
        ),
        Err(Exceeded::Total(byte)) => (
            byte,
            format!(
                "the query builds an expression deeper than {MAX_QUERY_EXPRESSION_DEPTH} levels: every bracket, every `not` in a row and every `and`/`or` is one level"
            ),
        ),
    };
    Err(
        QueryDiagnostic::parse(Q006, message, Some(Position::at(input, byte))).with_fix(
            "write a long `or` chain over one property as `$p.name in [...]`, and split deep nesting into simpler predicates",
        ),
    )
}

/// The estimated `(nesting, total)` depth of `input`, or where it first
/// passed a bound.
fn measure(
    input: &str,
    nesting_limit: usize,
    total_limit: usize,
) -> Result<(usize, usize), Exceeded> {
    let bytes = input.as_bytes();
    let mut stack = vec![Level::default()];
    // Sums over the open levels: brackets, `lead` and `nots` in `nesting`,
    // and `chain` on top of them in `total`.
    let mut nesting = 0usize;
    let mut total = 0usize;
    let mut deepest = (0usize, 0usize);
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let level = stack.last_mut().expect("the root level stays");
        match bytes[i] {
            b' ' | b'\t' | b'\r' | b'\n' => {
                i += 1;
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                i = find(bytes, i + 2, b"\n").map_or(bytes.len(), |end| end + 1);
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = find(bytes, i + 2, b"*/").map_or(bytes.len(), |end| end + 2);
                continue;
            }
            b'"' => {
                level.run = 0;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'(' | b'{' | b'[' => {
                let lead = std::mem::take(&mut level.run);
                stack.push(Level {
                    lead,
                    ..Level::default()
                });
                nesting += lead + 1;
                total += lead + 1;
                i += 1;
            }
            b')' | b'}' | b']' => {
                level.run = 0;
                if stack.len() > 1 {
                    let child = stack.pop().expect("a bracket level is open");
                    let own = child.lead + 1 + child.nots;
                    nesting -= own;
                    total -= own + child.chain;
                    let parent = stack.last_mut().expect("the root level stays");
                    parent.child_nesting = parent.child_nesting.max(own + child.child_nesting);
                    parent.child_total = parent
                        .child_total
                        .max(own + child.chain + child.child_total);
                }
                i += 1;
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                // `$not`, `@and` and `$p.or` are names, not operators.
                let operator = start == 0 || !matches!(bytes[start - 1], b'$' | b'@' | b'.');
                match &bytes[start..i] {
                    b"not" if operator => {
                        level.run += 1;
                        if level.run > level.nots {
                            level.nots = level.run;
                            nesting += 1;
                            total += 1;
                        }
                    }
                    b"and" | b"or" if operator => {
                        level.run = 0;
                        level.chain += 1;
                        total += 1;
                    }
                    _ => level.run = 0,
                }
            }
            _ => {
                level.run = 0;
                i += 1;
            }
        }
        let top = stack.last().expect("the root level stays");
        let now = (nesting + top.child_nesting, total + top.child_total);
        if now.0 > nesting_limit {
            return Err(Exceeded::Nesting(start));
        }
        if now.1 > total_limit {
            return Err(Exceeded::Total(start));
        }
        deepest = (deepest.0.max(now.0), deepest.1.max(now.1));
    }
    Ok(deepest)
}

fn find(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    bytes
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| from + offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth(source: &str) -> (usize, usize) {
        measure(source, usize::MAX, usize::MAX).expect("no limit")
    }

    fn filter(expr: &str) -> String {
        format!("query q() {{ match {{ $p: Person\n {expr} }} return {{ $p.name }} }}")
    }

    type Shape = (&'static str, fn(usize) -> String);

    fn nested(n: usize) -> String {
        format!("{}$p.age > 1{}", "(".repeat(n), ")".repeat(n))
    }

    #[test]
    fn every_recursive_shape_counts_one_level_per_repetition() {
        let (base_nesting, base_total) = depth(&filter("$p.age > 1"));
        let nesting_shapes: [Shape; 5] = [
            ("brackets", nested),
            ("not run", |n| format!("{}$p.age > 1", "not ".repeat(n))),
            ("not before brackets", |n| {
                format!("{}$p.age > 1{}", "not (".repeat(n), ")".repeat(n))
            }),
            ("negation blocks", |n| {
                format!("{}$p.age > 1{}", "not { ".repeat(n), " }".repeat(n))
            }),
            ("list literals", |n| {
                format!("$p.age in {}1{}", "[".repeat(n), "]".repeat(n))
            }),
        ];
        for (shape, build) in nesting_shapes {
            let (nesting, total) = depth(&filter(&build(10)));
            assert!(nesting >= base_nesting + 10, "{shape}: {nesting}");
            assert!(total >= base_total + 10, "{shape}: {total}");
        }
        let chain_shapes: [Shape; 3] = [
            ("or chain", |n| vec!["$p.age > 1"; n + 1].join(" or ")),
            ("and chain", |n| vec!["$p.age > 1"; n + 1].join(" and ")),
            ("chain after a deep operand", |n| {
                format!("{} or {}", nested(n), vec!["$p.age > 1"; n].join(" or "))
            }),
        ];
        for (shape, build) in chain_shapes {
            let (_, total) = depth(&filter(&build(10)));
            assert!(total >= base_total + 10, "{shape}: {total}");
        }
    }

    #[test]
    fn names_strings_comments_and_separated_nots_are_not_levels() {
        let base = depth(&filter("$p.age > 1"));
        let quiet = filter(
            "$p.age > 1 // ((((( not not or and\n /* [[[[ or or */ $p.name = \"(((( \\\" not or\" $not = $p.or",
        );
        assert_eq!(depth(&quiet), base);
        let siblings = filter(&vec!["not $p.age > 1"; 100].join("\n"));
        assert_eq!(depth(&siblings).0, base.0 + 1);
    }

    #[test]
    fn a_source_past_either_bound_is_refused_with_q006() {
        let accepted = [
            filter(&nested(MAX_QUERY_NESTING - 8)),
            filter(&vec!["$p.age > 1"; 200].join(" or ")),
        ];
        for source in &accepted {
            assert!(check_query_nesting(source).is_ok());
        }
        let refused = [
            filter(&nested(100_000)),
            filter(&format!("{}$p.age > 1", "not ".repeat(100_000))),
            filter(&vec!["$p.age > 1"; 100_000].join(" or ")),
            format!("{}{}", "(".repeat(100_000), "query q() {}"),
        ];
        for source in &refused {
            let diagnostic = check_query_nesting(source).unwrap_err();
            assert_eq!(diagnostic.code, Q006);
            assert!(diagnostic.position.is_some());
        }
        assert!(matches!(
            measure(
                &filter(&nested(MAX_QUERY_NESTING)),
                MAX_QUERY_NESTING,
                usize::MAX
            ),
            Err(Exceeded::Nesting(_))
        ));
        assert!(matches!(
            measure(
                &filter(&vec!["$p.age > 1"; 300].join(" or ")),
                usize::MAX,
                MAX_QUERY_EXPRESSION_DEPTH
            ),
            Err(Exceeded::Total(_))
        ));
    }
}
