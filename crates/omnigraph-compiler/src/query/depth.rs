//! The nesting bound a query source meets before the grammar runs (`Q006`).
//!
//! The parser, the typechecker and every later stage recurse once per level
//! of an expression tree, so an unbounded source can exhaust the stack of the
//! thread compiling it, which aborts the process instead of refusing the
//! request. A chain `a or b or c` is a left-deep tree as deep as it is long,
//! so its operators are levels too. This module bounds the depth with one
//! linear scan over the bytes, before any recursion: it skips string
//! literals and comments as the grammar does, opens a level at `(`, `{` and
//! `[`, charges a level for each prefix operator in a run of adjacent ones
//! (`not` in any case, `!`, a unary `-`) and for each nested `case`, charges
//! each `and`/`or` (in any case, and `&&`) of an expression to the total,
//! and charges the longest run of binary operators (arithmetic, `||`,
//! comparisons, `in`) between two `and`/`or`s. Every spelling the habit
//! rewrite turns into an operator is counted as that operator, so the
//! rewrite's reparse meets the same bound.
//!
//! The bound is on depth, not breadth. An expression ends where a list
//! separator (`,`, `;`, `:`) or a second operand follows it (a clause word,
//! a variable or a literal right after an operand: no expression puts two
//! operands side by side), and the next one starts from zero; a level
//! charges only its deepest expression, so sibling clauses, arguments and
//! write groups never add up. The estimate never undercounts the tree a
//! source can build.

use super::codes::Q006;
use super::diagnostic::{Position, QueryDiagnostic};

/// The deepest nesting of brackets, `case`s and adjacent prefix operators a
/// query source may hold. Each such level costs several frames in the grammar alone.
pub const MAX_QUERY_NESTING: usize = 64;

/// The deepest expression a query source may build, counting every
/// `and`/`or` of a chain and every operator of an arithmetic or comparison
/// chain as one level on top of the nesting. A long `or`
/// chain over one property is written `$p.name in [...]`, which is one level
/// whatever its length.
pub const MAX_QUERY_EXPRESSION_DEPTH: usize = 256;

/// The stack of a thread that compiles, plans or runs a query. The bounds
/// above keep the recursion finite; this keeps a source at them within the
/// thread, in a debug build too, where each level's frames are largest. The
/// server's and the CLI's runtimes give every worker and blocking thread
/// this stack: Tokio's default (2 MiB) is too small for an `or` chain of
/// sixty terms.
pub const QUERY_THREAD_STACK_BYTES: usize = 16 * 1024 * 1024;

/// Run `f` on a thread with [`QUERY_THREAD_STACK_BYTES`] of stack and return
/// its result, a panic in it resumed on the caller. A binary's `main` runs
/// its runtime's `block_on` through this: the future `block_on` drives runs
/// on the calling thread, whose stack the runtime's `thread_stack_size` does
/// not set, and a CLI compiles its query there.
pub fn on_query_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let spawned = std::thread::Builder::new()
        .name("omnigraph-main".into())
        .stack_size(QUERY_THREAD_STACK_BYTES)
        .spawn(f);
    let handle =
        spawned.unwrap_or_else(|error| panic!("the query thread could not be spawned: {error}"));
    handle
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

#[derive(Default)]
struct Level {
    /// The prefix-operator run directly before this level's opening bracket.
    lead: usize,
    /// The longest run of adjacent prefix operators (`not`, `!`, a unary
    /// `-`) at this level.
    nots: usize,
    /// The `case` expressions at this level, each nesting its branches.
    cases: usize,
    /// The prefix-operator run in progress, ended by any other token.
    run: usize,
    /// `and`/`or` operators of the expression in progress at this level,
    /// plus the longest run of binary operators between two of them.
    chain: usize,
    /// The deepest `chain` of an expression this level has completed.
    chain_max: usize,
    /// The binary-operator run in progress, ended by `and`/`or` or `,`.
    ops: usize,
    /// The longest binary-operator run at this level.
    ops_max: usize,
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
                "the query nests deeper than {MAX_QUERY_NESTING} levels: every bracket, every `case` and every `not`, `!` or unary `-` in a row is one level"
            ),
        ),
        Err(Exceeded::Total(byte)) => (
            byte,
            format!(
                "the query builds an expression deeper than {MAX_QUERY_EXPRESSION_DEPTH} levels: every bracket, every `not` in a row, every `and`/`or` and every operator of an arithmetic or comparison chain is one level"
            ),
        ),
    };
    Err(
        QueryDiagnostic::parse(Q006, message, Some(Position::at(input, byte))).with_fix(
            "write a long `or` chain over one property as `$p.name in [...]`, and split deep nesting or a long arithmetic chain into simpler predicates",
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
    // Sums over the open levels: brackets, `lead`, `nots` and `cases` in `nesting`,
    // and `chain` on top of them in `total`.
    let mut nesting = 0usize;
    let mut total = 0usize;
    let mut deepest = (0usize, 0usize);
    let mut i = 0;
    // Whether the last token ended an operand, which makes a `-` binary.
    let mut after_operand = false;
    while i < bytes.len() {
        let start = i;
        let level = stack.last_mut().expect("the root level stays");
        // A prefix operator lengthens the run; it is a level once the run
        // passes the longest at this level.
        let prefix = |level: &mut Level, nesting: &mut usize, total: &mut usize| {
            level.run += 1;
            if level.run > level.nots {
                level.nots = level.run;
                *nesting += 1;
                *total += 1;
            }
        };
        // A binary operator lengthens the operator run; it is a level once
        // the run passes the longest at this level.
        let binary = |level: &mut Level, total: &mut usize| {
            level.run = 0;
            level.ops += 1;
            if level.ops > level.ops_max {
                level.ops_max = level.ops;
                level.chain += 1;
                *total += 1;
            }
        };
        // `and`/`or` ends the operator run and is a level of its own.
        let logical = |level: &mut Level, total: &mut usize| {
            level.run = 0;
            level.ops = 0;
            level.chain += 1;
            *total += 1;
        };
        // An expression ended: the next one at this level starts from zero,
        // and the level keeps only the deeper of the two.
        let end_expression = |level: &mut Level, total: &mut usize| {
            level.run = 0;
            *total -= level.chain;
            level.chain_max = level.chain_max.max(level.chain);
            level.chain = 0;
            level.ops = 0;
            level.ops_max = 0;
        };
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
                if after_operand {
                    end_expression(level, &mut total);
                }
                level.run = 0;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
                after_operand = true;
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
                after_operand = false;
            }
            b')' | b'}' | b']' => {
                level.run = 0;
                if stack.len() > 1 {
                    let child = stack.pop().expect("a bracket level is open");
                    let own = child.lead + 1 + child.nots + child.cases;
                    nesting -= own;
                    total -= own + child.chain;
                    let parent = stack.last_mut().expect("the root level stays");
                    parent.child_nesting = parent.child_nesting.max(own + child.child_nesting);
                    parent.child_total = parent
                        .child_total
                        .max(own + child.chain.max(child.chain_max) + child.child_total);
                }
                i += 1;
                after_operand = true;
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'0'..=b'9' => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                // `$not`, `@and` and `$p.or` are names, not operators.
                let operator = start == 0 || !matches!(bytes[start - 1], b'$' | b'@' | b'.');
                let word = &bytes[start..i];
                let keyword = |words: &[&[u8]]| words.iter().any(|w| word.eq_ignore_ascii_case(w));
                // A word right after an operand that is not an operator
                // starts the next expression.
                if operator
                    && after_operand
                    && !keyword(&[
                        b"and", b"or", b"not", b"in", b"is", b"when", b"then", b"else",
                    ])
                {
                    end_expression(level, &mut total);
                }
                let was_operand = after_operand;
                after_operand = true;
                if operator && word.eq_ignore_ascii_case(b"in") && was_operand {
                    binary(level, &mut total);
                    after_operand = false;
                } else if operator && word.eq_ignore_ascii_case(b"not") {
                    prefix(level, &mut nesting, &mut total);
                    after_operand = false;
                } else if operator
                    && (word.eq_ignore_ascii_case(b"and") || word.eq_ignore_ascii_case(b"or"))
                {
                    logical(level, &mut total);
                    after_operand = false;
                } else if operator && word.eq_ignore_ascii_case(b"case") {
                    // A `case` nests its branches without a bracket; it is
                    // charged as a level that stays open.
                    level.run = 0;
                    level.cases += 1;
                    nesting += 1;
                    total += 1;
                    after_operand = false;
                } else if operator
                    && [&b"when"[..], b"then", b"else"]
                        .iter()
                        .any(|w| word.eq_ignore_ascii_case(w))
                {
                    level.run = 0;
                    level.ops = 0;
                    after_operand = false;
                } else {
                    level.run = 0;
                }
            }
            b'&' if bytes.get(i + 1) == Some(&b'&') => {
                logical(level, &mut total);
                i += 2;
                after_operand = false;
            }
            b'!' if bytes.get(i + 1) != Some(&b'=') => {
                prefix(level, &mut nesting, &mut total);
                i += 1;
                after_operand = false;
            }
            b'-' if !after_operand => {
                prefix(level, &mut nesting, &mut total);
                i += 1;
            }
            b'-' | b'+' | b'*' | b'/' | b'%' | b'|' | b'<' | b'>' | b'=' | b'!' | b'~' => {
                // One operator, however many characters it is spelled with
                // (`<=`, `!=`, `=~`, `||`, an arrow's `->`).
                i += 1;
                while i < bytes.len() && matches!(bytes[i], b'|' | b'<' | b'>' | b'=' | b'~') {
                    i += 1;
                }
                binary(level, &mut total);
                after_operand = false;
            }
            b',' | b';' | b':' => {
                end_expression(level, &mut total);
                i += 1;
                after_operand = false;
            }
            b'$' if after_operand => {
                end_expression(level, &mut total);
                i += 1;
                after_operand = false;
            }
            _ => {
                level.run = 0;
                i += 1;
                after_operand = false;
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

    /// `expr` as a `where`, in parentheses so that it, not the `match`
    /// braces, is the deepest point of the base source.
    fn filter(expr: &str) -> String {
        format!("query q() {{ match {{ $p: Person }} where ({expr}) return {{ $p.name }} }}")
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
                format!(
                    "{}$p.age > 1{}",
                    "not exists { $p: Person where ".repeat(n),
                    " }".repeat(n)
                )
            }),
            ("list literals", |n| {
                format!("$p.age > {}1{}", "[".repeat(n), "]".repeat(n))
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
        assert_eq!(
            depth(&quiet),
            depth(&filter("$p.age > 1 $p.name = \"x\" $not = $p.or"))
        );
        let siblings = filter(&vec!["not $p.age > 1"; 100].join("\n"));
        assert_eq!(depth(&siblings).0, base.0 + 1);
    }

    /// Breadth is not depth: sibling expressions (write groups, the
    /// arguments of a call, the assignments of a `set`) each start from
    /// zero, so a source as wide as the size budget allows measures as deep
    /// as one of its siblings.
    #[test]
    fn siblings_are_measured_by_the_deepest_not_their_sum() {
        let group = |i: usize| {
            format!(
                "match {{ $p: Person }} where $p.handle = \"h{i}\" and $p.age > 1 or $p.age < 0 update $p set {{ age: $p.age + 1, city: \"c\" }}"
            )
        };
        let write = |n: usize| {
            format!(
                "query w() {{ {} }}",
                (0..n).map(group).collect::<Vec<_>>().join("\n")
            )
        };
        assert_eq!(depth(&write(1)), depth(&write(1_000)));
        assert!(check_query_nesting(&write(1_000)).is_ok());
        let arguments = |n: usize| {
            filter(&format!(
                "coalesce({}) > 0",
                vec!["$p.age > 1 or $p.age < 0 and $p.age = 3"; n].join(", ")
            ))
        };
        assert_eq!(depth(&arguments(1)), depth(&arguments(500)));
        // One expression still counts every operator of its chain.
        assert!(depth(&filter(&vec!["$p.age > 1"; 20].join(" or "))).1 > depth(&arguments(500)).1);
    }

    /// A source at the bounds compiles on the stack [`on_query_stack`]
    /// gives, from a caller whose own stack is far smaller, as a binary's
    /// main thread can be.
    #[test]
    fn a_source_at_the_bounds_compiles_on_the_query_stack() {
        let source = filter(&format!(
            "{} or {}",
            nested(MAX_QUERY_NESTING - 8),
            vec!["$p.age > 1"; 180].join(" or ")
        ));
        assert!(check_query_nesting(&source).is_ok());
        let parsed = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || {
                on_query_stack(move || {
                    crate::query::syntax::parse::parse_source_all(&source)
                        .1
                        .is_empty()
                })
            })
            .unwrap()
            .join()
            .unwrap();
        assert!(parsed);
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
