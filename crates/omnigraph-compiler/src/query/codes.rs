//! The catalog of query compile diagnostic codes (RFC 0047). A code is
//! stable once published: its meaning is frozen, its message text may
//! improve. `Q…` codes are the parser's, `T…` codes the typechecker's, `P…`
//! codes the planner's, `E…` codes a row refused while the query ran; the
//! numbering has gaps where earlier stages were withdrawn, and a withdrawn
//! number is never reused.

use super::diagnostic::{QueryCode, QueryCodeSpec};

macro_rules! codes {
    ($($name:ident = $short:literal;)*) => {
        $(pub const $name: QueryCode = QueryCode(&QueryCodeSpec { code: stringify!($name), short: $short });)*
        /// Every code in the catalog, for uniqueness and documentation checks.
        pub const ALL: &[QueryCode] = &[$($name),*];
    };
}

codes! {
    // Parser.
    Q001 = "the source does not match the grammar at this position";
    Q002 = "a query declaration is missing its parameter list";
    Q003 = "a settings statement is refused";
    Q004 = "a branch, commit, show or explain statement is misplaced or malformed";
    Q005 = "a declaration body is refused";
    Q006 = "the source nests deeper than the compiler accepts";
    // docs/rfcs/2026-11-07-gq-3.md section 5: each refusal names the
    // construct and carries a byte-range suggestion wherever one edit set
    // makes the text legal.
    Q007 = "a traversal is written with an arrow; the fix is the triple";
    Q008 = "an update or delete statement carries a where; the fix is the group's match and where";
    Q009 = "`mutation` is not a declaration keyword; the fix is `query`";
    Q010 = "a binding carries a property map or braces; the fix is the binding and a where conjunct";
    Q011 = "a read body carries `as commit` or `as unattached branch`";
    Q012 = "a respelling the language refuses; the fix prints the legal form";
    Q013 = "a write's elements are written without match, or after a statement; the fix is the group's match";
    Q014 = "a spelling from another language; the fix prints the GQ form";
    Q015 = "a form a later stage adds; the message names the stage";
    Q016 = "two items of a list are separated by whitespace alone; the fix is the comma";
    Q017 = "a filter, a where or a comparison inside the match braces; the fix moves it to the where after them";
    Q018 = "a bare name where a value is read; a bare name is a return alias in order only";
    Q019 = "a call named by an element keyword, or `from` over a parenthesized relation";
    Q020 = "a read body's parts are missing, misplaced or unbraced";
    Q021 = "an annotation's named argument is written `name: value`; the fix is `name = value`";

    // Typechecker.
    T1 = "unknown node type";
    T2 = "type has no such property";
    T3 = "a literal or binding-match value is invalid for its property, or names an undeclared parameter";
    T4 = "unknown edge type";
    T5 = "variable is not an endpoint of the traversed edge";
    T6 = "a read variable, type or property cannot be resolved";
    T7 = "assignment or comparison operands are invalid";
    T8 = "an aggregate argument is invalid for the aggregate function";
    T9 = "grouping is invalid or a negation block is uncorrelated";
    T10 = "insert has no assignment";
    T11 = "a mutation property or meta-field is unavailable in this context";
    T12 = "insert omits a required property";
    T13 = "duplicate assignment";
    T14 = "mutation variable is not a declared parameter";
    T15 = "traversal hop bounds or nearest operands are invalid";
    T16 = "update is not supported for edge types";
    T17 = "nearest ordering requires a limit";
    T19 = "search or fuzzy operands are invalid";
    T20 = "match_text or bm25 operands are invalid";
    T21 = "rrf operands are invalid or its ordering lacks a limit";
    T22 = "undirected traversal needs a same-endpoint-type edge";
    T23 = "a node or edge binding is used in an incompatible role";
    T24 = "a Blob property is not a read value";
    T25 = "two projections produce the same result column";
    T27 = "a full-text call needs a full-text index on its property";
    T32 = "a retrieval cannot sit under an aggregate";
    T33 = "a projected rank must repeat the executed retrieval";
    T35 = "a search predicate cannot be projected";
    T36 = "an alias cannot be projected";
    // T38 is withdrawn: a lexical predicate is a Boolean operand like any
    // other (docs/rfcs/2026-11-07-gq-3.md, section 5).
    T39 = "an exists or aggregate block must reference an outer-bound variable";
    T40 = "an aggregate block argument or comparison value is invalid";
    T41 = "a filter or Boolean expression has invalid operands";
    T42 = "an order key must appear in return or name an alias";
    T43 = "a Boolean expression in return needs an alias";
    T44 = "a call is not allowed in a mutation condition, assignment or binding match";
    T45 = "a property value cannot appear where a constant is required";
    T46 = "a Boolean literal where a bare property name was meant";
    T47 = "parameter name is reserved";
    T48 = "unknown parameter type";
    T49 = "variable name is reserved for the compiler";
    T50 = "variable is rebound to another type";
    T51 = "unknown type";
    T52 = "nested list literals are not supported";
    T53 = "list literal elements must share one scalar type";
    T54 = "a relevance ranking does not accept the asc modifier";
    T55 = "an unattached base parameter is undeclared or not a String";
    T56 = "distinct does not apply to an aggregated return";
    T57 = "distinct applies only to count";
    T58 = "a Boolean return item compares aggregates with constants only";
    T59 = "an rrf() ranks two bindings one traversal connects";
    // docs/rfcs/2026-11-07-gq-3.md section 5. T60 to T64, T66 and T67 are
    // unallocated.
    T65 = "a ranking is written as a call in order or return; it is a relation function";
    T68 = "a name drawn into scope, or bound inside an inline read or a block, is already in scope";
    T69 = "a relation where a scalar is read, or a scalar where a relation is read";
    T70 = "a call cycle is not monotone, has no base arm, passes a computed argument, or exceeds the plan budget";
    T71 = "an option is unknown, out of range or written positionally";
    T72 = "an arithmetic operand is not numeric or the operation is unsupported";
    T73 = "the arms of a union disagree on a column";
    T74 = "a name is refused as a relation";
    T75 = "an rrf arm is invalid";
    T76 = "a write mixes delete with insert or update";
    T77 = "a statement's variable is not a bound node or edge reference";
    T78 = "an endpoint assignment is neither a key of the endpoint type nor a bound reference of it";
    T79 = "a ranked column is not a node column of its population";
    T80 = "an annotation the language does not read";
    T81 = "a pattern whose components share no variable and are not pinned to one row by a `@key` equality";
    T82 = "a triple written against its edge's declared direction; the fix swaps its ends, source first";
    T83 = "two declarations of one name in a source unit";
    T84 = "a name an inner scope, a block or an inline read, uses in a triple without binding it";
    T85 = "a ranked `from` without `as`, or an `as` on a `from` that ranks nothing";
    T86 = "a term this release's planner does not lower; the message names the stage that adds it";

    // Planner. P001 and P005 are withdrawn: a ranking roots its traversal at
    // the binding it ranks, and `rrf()` arms one traversal connects are T59.
    P002 = "an edge alternation or wildcard traversal needs a finite traversal work limit";
    P003 = "the traversal work limit is out of range or recorded twice";
    P004 = "an edge alternation or wildcard traversal cannot run in CSR traversal mode";
    // P006 is the planner's assertion, not a refusal: the type checker
    // refuses every term the planner does not lower (`T86`), so a term that
    // arrives unlowered is a defect.
    P006 = "a type-checked term arrived at the planner unlowered: a defect, never a refusal";

    // Execution (docs/rfcs/2026-11-07-gq-3.md section 10): an arithmetic
    // term refused a row; the failure keeps its place in the plan.
    E1 = "integer arithmetic overflowed its type";
    E2 = "a divisor is zero";
    E3 = "a float result is not finite";
    // A write statement's rows give one entity's property two values
    // (docs/rfcs/2026-11-07-gq-3.md section 2, the sinks).
    E4 = "one statement's rows give one entity's property two values";
}

#[cfg(test)]
mod tests {
    use super::ALL;
    use std::collections::BTreeSet;

    #[test]
    fn codes_are_unique_and_well_formed() {
        let mut seen = BTreeSet::new();
        for code in ALL {
            assert!(seen.insert(code.as_str()), "duplicate code {code}");
            let (prefix, number) = code.as_str().split_at(1);
            assert!(
                matches!(prefix, "Q" | "T" | "P" | "E"),
                "{code}: unknown namespace"
            );
            assert!(
                number.parse::<u32>().is_ok(),
                "{code}: the number must parse"
            );
            assert!(!code.short().is_empty(), "{code}: empty short");
        }
    }
}
