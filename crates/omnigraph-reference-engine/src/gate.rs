use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::ir::{IRExpr, IROp, QueryIR};
use omnigraph_compiler::query::ast::{BinaryOp, CompOp, Literal, Param};
use omnigraph_core::error::OmniError;

/// The tail of every refusal of a query v1 cannot run, the construct named in
/// front. Its only caller is GQT's `--- expect same as v1` comparison.
const REFERENCE_ADVICE: &str = " are not supported by the frozen reference engine; use a query \
                                shape the reference supports, or drop `--- expect same as v1` \
                                from this step";

/// What the v1 door refuses in a compiled read: each variant names the
/// construct in front of `REFERENCE_ADVICE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V1Refusal {
    CompoundPredicate,
    ReturnOrOrderComparison,
    FilterShape,
    OrderKey,
    SubqueryPredicate,
    StringNearest,
    EdgeSelection,
    EdgeTypeMetadata,
}

impl V1Refusal {
    fn construct(self) -> &'static str {
        match self {
            Self::CompoundPredicate => "compound predicates (and, or, not, is null)",
            Self::ReturnOrOrderComparison => "comparisons in return or order",
            Self::FilterShape => "filters other than one comparison or search call",
            Self::OrderKey => {
                "order keys other than a property, a system field, an alias or the leading \
                 search key"
            }
            Self::SubqueryPredicate => "correlated subquery predicates other than not { … }",
            Self::StringNearest => "text queries in nearest",
            Self::EdgeSelection => "edge alternatives and wildcards",
            Self::EdgeTypeMetadata => "edge type metadata access",
        }
    }

    pub(crate) fn error(self) -> OmniError {
        CompilerError::Plan(format!("{}{REFERENCE_ADVICE}", self.construct())).into()
    }
}

/// The first shape of `ir` engine v1 does not evaluate: a filter beyond one
/// comparison over property, literal and parameter operands or the search call,
/// a Boolean node in `return` or `order`, or an order key of another shape.
pub(crate) fn v1_refusal(ir: &QueryIR) -> Option<V1Refusal> {
    if ir.has_edge_selections() {
        return Some(V1Refusal::EdgeSelection);
    }
    if ir
        .return_exprs
        .iter()
        .any(|projection| has_edge_type(&projection.expr))
        || ir.order_by.iter().any(|key| has_edge_type(&key.expr))
        || pipeline_has_edge_type(&ir.pipeline)
    {
        return Some(V1Refusal::EdgeTypeMetadata);
    }
    if let Some(refusal) = pipeline_refusal(&ir.pipeline) {
        return Some(refusal);
    }
    if ir
        .return_exprs
        .iter()
        .any(|projection| has_boolean_node(&projection.expr))
    {
        return Some(V1Refusal::ReturnOrOrderComparison);
    }
    for (index, key) in ir.order_by.iter().enumerate() {
        if has_boolean_node(&key.expr) {
            return Some(V1Refusal::ReturnOrOrderComparison);
        }
        let accepted = match &key.expr {
            IRExpr::PropAccess { .. } | IRExpr::AliasRef(_) => true,
            IRExpr::Nearest { .. } | IRExpr::Bm25 { .. } | IRExpr::Rrf { .. } => index == 0,
            _ => false,
        };
        if !accepted {
            return Some(V1Refusal::OrderKey);
        }
    }
    let searched = ir.order_by.iter().map(|key| &key.expr);
    let returned = ir.return_exprs.iter().map(|projection| &projection.expr);
    if searched
        .chain(returned)
        .any(|expr| has_text_nearest(expr, &ir.params))
    {
        return Some(V1Refusal::StringNearest);
    }
    None
}

fn pipeline_refusal(pipeline: &[IROp]) -> Option<V1Refusal> {
    pipeline.iter().find_map(|op| match op {
        IROp::NodeScan { filters, .. }
        | IROp::Expand {
            dst_filters: filters,
            ..
        } => filters.iter().find_map(filter_refusal),
        IROp::Filter(filter) => filter_refusal(filter),
        IROp::AntiJoin { predicate, .. } if !predicate.is_not_exists() => {
            Some(V1Refusal::SubqueryPredicate)
        }
        IROp::AntiJoin { inner, .. } => pipeline_refusal(inner),
    })
}

fn pipeline_has_edge_type(pipeline: &[IROp]) -> bool {
    pipeline.iter().any(|op| match op {
        IROp::NodeScan { filters, .. }
        | IROp::Expand {
            dst_filters: filters,
            ..
        } => filters.iter().any(has_edge_type),
        IROp::Filter(expr) => has_edge_type(expr),
        IROp::AntiJoin {
            inner, predicate, ..
        } => {
            pipeline_has_edge_type(inner)
                || predicate.arg.as_ref().is_some_and(has_edge_type)
                || has_edge_type(&predicate.right)
        }
    })
}

fn has_edge_type(expr: &IRExpr) -> bool {
    match expr {
        IRExpr::PropAccess { property, .. } => {
            property == omnigraph_compiler::traversal::EDGE_TYPE_COLUMN
        }
        IRExpr::Binary { left, right, .. } => has_edge_type(left) || has_edge_type(right),
        IRExpr::Not(arg)
        | IRExpr::IsNull { expr: arg, .. }
        | IRExpr::Aggregate { arg, .. }
        | IRExpr::Nearest { query: arg, .. } => has_edge_type(arg),
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => has_edge_type(field) || has_edge_type(query),
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            has_edge_type(field)
                || has_edge_type(query)
                || max_edits.as_deref().is_some_and(has_edge_type)
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            has_edge_type(primary)
                || has_edge_type(secondary)
                || k.as_deref().is_some_and(has_edge_type)
        }
        IRExpr::Variable(_) | IRExpr::Param(_) | IRExpr::Literal(_) | IRExpr::AliasRef(_) => false,
    }
}

fn filter_refusal(filter: &IRExpr) -> Option<V1Refusal> {
    let v1_operand = |expr: &IRExpr| {
        matches!(
            expr,
            IRExpr::PropAccess { .. } | IRExpr::Literal(_) | IRExpr::Param(_)
        )
    };
    match filter {
        IRExpr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            ..
        }
        | IRExpr::Not(_)
        | IRExpr::IsNull { .. } => Some(V1Refusal::CompoundPredicate),
        IRExpr::Binary {
            left,
            op: BinaryOp::Compare(op),
            right,
        } => {
            let search_call = matches!(
                **left,
                IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. }
            ) && *op == CompOp::Eq
                && **right == IRExpr::Literal(Literal::Bool(true));
            (!search_call && !(v1_operand(left) && v1_operand(right)))
                .then_some(V1Refusal::FilterShape)
        }
        _ => Some(V1Refusal::FilterShape),
    }
}

fn has_boolean_node(expr: &IRExpr) -> bool {
    match expr {
        IRExpr::Binary { .. } | IRExpr::Not(_) | IRExpr::IsNull { .. } => true,
        IRExpr::Aggregate { arg, .. } | IRExpr::Nearest { query: arg, .. } => has_boolean_node(arg),
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => has_boolean_node(field) || has_boolean_node(query),
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            has_boolean_node(field)
                || has_boolean_node(query)
                || max_edits.as_deref().is_some_and(has_boolean_node)
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            has_boolean_node(primary)
                || has_boolean_node(secondary)
                || k.as_deref().is_some_and(has_boolean_node)
        }
        IRExpr::PropAccess { .. }
        | IRExpr::Variable(_)
        | IRExpr::Param(_)
        | IRExpr::Literal(_)
        | IRExpr::AliasRef(_) => false,
    }
}

/// A `nearest` whose query is text, a literal or a `String` parameter: v1
/// holds no embedding client.
fn has_text_nearest(expr: &IRExpr, params: &[Param]) -> bool {
    match expr {
        IRExpr::Nearest { query, .. } => match query.as_ref() {
            IRExpr::Literal(Literal::String(_)) => true,
            IRExpr::Param(name) => params
                .iter()
                .any(|param| param.name == *name && param.type_name == "String"),
            _ => false,
        },
        IRExpr::Rrf {
            primary, secondary, ..
        } => has_text_nearest(primary, params) || has_text_nearest(secondary, params),
        IRExpr::Aggregate { arg, .. } => has_text_nearest(arg, params),
        _ => false,
    }
}
