//! The planner's door for GQ edition 3 (`gq 3;`, docs/rfcs/2026-11-07-gq-3.md).
//!
//! The compiler types an edition-3 declaration into the core IR
//! ([`omnigraph_compiler::ir::core`]). This release lowers none of it:
//! [`plan_core`] refuses every declaration with `P006` before any plan
//! source is consulted, so the refusal needs no snapshot and reads no storage.
//! The refusal names the first core node, in pre-order, that today's plan
//! has no node for, or the declaration itself when every node has one; the
//! release that lowers the core IR replaces this function.

use omnigraph_compiler::ir::core::{CoreDecl, CoreNode};
use omnigraph_compiler::query::codes::P006;

use crate::error::PlanError;

/// Whether today's plan has a node of this kind's shape: a scan, a
/// traversal, a filter, a projection, a group, a correlated block, an order,
/// a cut, a ranked scan, a fusion and the keyed write sinks. `Union`, the
/// recursion nodes and a captured `Call` have none.
pub fn has_plan_node(node: CoreNode) -> bool {
    match node {
        CoreNode::Unit
        | CoreNode::Env
        | CoreNode::Scan
        | CoreNode::Expand
        | CoreNode::Filter
        | CoreNode::Project
        | CoreNode::Group
        | CoreNode::Correlate
        | CoreNode::Order
        | CoreNode::Cut
        | CoreNode::Score
        | CoreNode::ApproxTopK
        | CoreNode::Fuse
        | CoreNode::Insert
        | CoreNode::Update
        | CoreNode::Delete => true,
        CoreNode::Union | CoreNode::Fix | CoreNode::RecRef | CoreNode::Call => false,
    }
}

/// Plan an edition-3 declaration. No core node is lowered in this release,
/// so the answer is always the `P006` refusal.
pub fn plan_core(decl: &CoreDecl) -> PlanError {
    let name = decl.name();
    let message = match decl.nodes().into_iter().find(|n| !has_plan_node(*n)) {
        // A self-calling read: the fixpoint evaluator is stage Rec's.
        Some(node @ (CoreNode::Fix | CoreNode::RecRef)) => format!(
            "`{name}` is an edition-3 declaration, which this release type-checks but does not run: the planner does not lower its `{}` node, which the recursion stage (Rec) evaluates",
            node.name()
        ),
        Some(node) => format!(
            "`{name}` is an edition-3 declaration, which this release type-checks but does not run: the planner does not lower its `{}` node",
            node.name()
        ),
        None => format!(
            "`{name}` is an edition-3 declaration, which this release type-checks but does not run: the planner does not lower the edition-3 core IR yet"
        ),
    };
    PlanError::refused(P006, message, None)
}

#[cfg(test)]
mod tests {
    use omnigraph_compiler::query::ast::FileBody;
    use omnigraph_compiler::query::parser::parse_query;
    use omnigraph_compiler::query::typecheck::{CheckedQuery, typecheck_query_decl};
    use omnigraph_compiler::{build_catalog, schema::parser::parse_schema};

    use super::*;

    const SCHEMA: &str =
        "node Person { handle: String @key  name: String }\nedge Knows: Person -> Person\n";

    fn refusal(source: &str) -> String {
        let catalog = build_catalog(&parse_schema(SCHEMA).unwrap()).unwrap();
        let file = parse_query(source).unwrap();
        let FileBody::Queries(decls) = file.body else {
            panic!("a declaration file")
        };
        let CheckedQuery::Core(core) = typecheck_query_decl(&catalog, &decls[0]).unwrap() else {
            panic!("an edition-3 declaration")
        };
        let PlanError::Unsupported(diagnostic) = plan_core(&core) else {
            panic!("a refusal")
        };
        assert_eq!(diagnostic.code, P006);
        diagnostic.message
    }

    #[test]
    fn every_core_declaration_is_refused_naming_its_first_unplanned_node() {
        let message = refusal(
            "gq 3;\nquery q() { match { from union({ match { $p: Person } return { $p } }, { match { $p: Person } return { $p } }) } return { $p.name } }",
        );
        assert!(message.contains("`Union` node"), "{message}");
        let message = refusal(
            "gq 3;\nquery q($h: String) { match { $p: Person { handle: $h }  $p knows $q } return { $q.name } }",
        );
        assert!(message.contains("`q`"), "{message}");
        assert!(message.contains("core IR yet"), "{message}");
        let message = refusal("gq 3;\nquery w() { insert Person { handle: \"a\", name: \"b\" } }");
        assert!(message.contains("`w`"), "{message}");
        let message = refusal(
            "gq 3;\nquery reach($h: String) { match { from union({ match { $p: Person { handle: $h }  $p knows $q } return { $q } }, { match { from reach($h)  $q knows $r } return { $r as q } }) } return distinct { $q } }",
        );
        assert!(message.contains("`Fix` node"), "{message}");
        assert!(message.contains("(Rec)"), "{message}");
    }
}
