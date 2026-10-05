//! The catalog of query compile diagnostic codes (RFC 0047). A code is
//! stable once published: its meaning is frozen, its message text may
//! improve. `Q…` codes are the parser's, `T…` codes the typechecker's, `P…`
//! codes the planner's; the
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
    Q004 = "a branch, show or explain statement is misplaced or malformed";
    Q005 = "a declaration body is refused";

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
    T18 = "alias ordering cannot be combined with nearest";
    T19 = "search or fuzzy operands are invalid";
    T20 = "match_text or bm25 operands are invalid";
    T21 = "rrf operands are invalid or its ordering lacks a limit";
    T22 = "undirected traversal needs a same-endpoint-type edge";
    T23 = "a node or edge binding is used in an incompatible role";
    T24 = "a Blob property is not a read value";
    T25 = "two projections produce the same result column";
    T32 = "a retrieval cannot sit under an aggregate";
    T33 = "a projected rank must repeat the executed retrieval";
    T35 = "a search predicate cannot be projected";
    T36 = "an alias cannot be projected";
    T37 = "rrf cannot be projected";
    T38 = "a search predicate must stand alone or be compared with true";
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

    // Planner.
    P001 = "a ranking orders a binding a traversal reaches";
    P002 = "an edge alternation or wildcard traversal needs a finite traversal work limit";
    P003 = "the traversal work limit is out of range or recorded twice";
    P004 = "an edge alternation or wildcard traversal cannot run in CSR traversal mode";
    P005 = "an rrf() ranks two bindings one traversal connects";
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
                matches!(prefix, "Q" | "T" | "P"),
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
