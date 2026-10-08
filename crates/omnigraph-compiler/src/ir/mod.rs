pub mod coerce;
mod erase;
pub mod fold;
pub(crate) mod lower;
pub mod untyped;
pub(crate) mod validate;

use std::collections::HashMap;

use crate::error::{CompilerError, Result};
use crate::query::ast::{AggFunc, BinaryOp, CompOp, Literal, NOW_PARAM_NAME, Param, Precedence};
use crate::query::typecheck::MutationTarget;
use crate::traversal::EdgeSelection;
use crate::types::{AggSignature, ExprType, PropType};

/// A declaration and the type resolved for it by the compiler.
#[derive(Debug, Clone)]
pub struct IRParam {
    pub declaration: Param,
    pub ty: ExprType,
}

#[derive(Debug, Clone)]
pub struct QueryIR {
    pub name: String,
    pub params: Vec<IRParam>,
    pub pipeline: Vec<IROp>,
    pub return_exprs: Vec<IRProjection>,
    pub order_by: Vec<IROrdering>,
    pub limit: Option<u64>,
}

impl QueryIR {
    pub fn has_edge_selections(&self) -> bool {
        self.any_edge_selection(|edges| edges.named().is_none())
    }

    pub fn has_wildcard_traversal(&self) -> bool {
        self.any_edge_selection(EdgeSelection::is_wildcard)
    }

    fn any_edge_selection(&self, predicate: impl Fn(&EdgeSelection) -> bool) -> bool {
        let mut pending: Vec<_> = self.pipeline.iter().collect();
        while let Some(op) = pending.pop() {
            match op {
                IROp::Expand {
                    edges,
                    src_var: _,
                    dst_var: _,
                    src_type: _,
                    dst_type: _,
                    min_hops: _,
                    max_hops: _,
                    dst_filters: _,
                    edge_binding: _,
                } => {
                    if predicate(edges) {
                        return true;
                    }
                }
                IROp::AntiJoin {
                    inner,
                    outer_var: _,
                    predicate: _,
                } => pending.extend(inner),
                IROp::NodeScan {
                    variable: _,
                    type_name: _,
                    filters: _,
                }
                | IROp::Filter(_) => {}
            }
        }
        false
    }
}

#[derive(Debug, Clone)]
pub struct MutationIR {
    pub name: String,
    pub params: Vec<IRParam>,
    pub ops: Vec<MutationOpIR>,
}

#[derive(Debug, Clone)]
pub enum MutationOpIR {
    Insert {
        target: MutationTarget,
        assignments: Vec<IRAssignment>,
    },
    /// `predicate` is Boolean over the target's properties, each spelled
    /// `IRExpr::PropAccess { variable: <type name>, property: <physical column> }`,
    /// and the declared parameters.
    Update {
        target: MutationTarget,
        assignments: Vec<IRAssignment>,
        predicate: IRExpr,
    },
    Delete {
        target: MutationTarget,
        predicate: IRExpr,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IRAssignment {
    pub property: String,
    pub value: IRExpr,
}

/// Resolved runtime parameters: param name → literal value.
pub type ParamMap = HashMap<String, Literal>;

#[derive(Debug, Clone)]
pub enum IROp {
    NodeScan {
        variable: String,
        type_name: String,
        /// Boolean expressions over `variable`, one per inline binding match.
        filters: Vec<IRExpr>,
    },
    Expand {
        src_var: String,
        dst_var: String,
        edges: EdgeSelection,
        src_type: String,
        dst_type: String,
        min_hops: u32,
        max_hops: Option<u32>,
        /// Filters from a deferred destination binding, pushed into the
        /// Expand so the executor can apply them during hydration (Lance
        /// SQL pushdown) rather than as a separate post-expand pass.
        dst_filters: Vec<IRExpr>,
        /// Variable bound to the matched edge row (`$p $w:knows $f`), if any.
        /// Changes the op's contract: one output row per matching edge ROW
        /// (not per distinct endpoint pair), edge property columns carried
        /// under this prefix. Always single-hop (typecheck T23).
        edge_binding: Option<String>,
    },
    /// A Boolean expression the type checker proved, as written.
    Filter(IRExpr),
    /// A correlated subquery, decorrelated: `inner` runs once over the outer
    /// rows and `predicate` decides per outer row on the aggregate of its
    /// matches. `not { … }` is the anti-join case, `count = 0`.
    AntiJoin {
        /// The outer variable whose id is used for the join key
        outer_var: String,
        /// The inner pipeline that produces rows to anti-join against
        inner: Vec<IROp>,
        predicate: SubqueryPredicate,
    },
}

/// The restricted aggregate side of a correlated block predicate. Conversions
/// wrap the sole aggregate signature instead of adding a second type owner.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BlockAggregateExpr {
    CountRows {
        ty: ExprType,
    },
    Aggregate {
        func: AggFunc,
        arg: Box<IRExpr>,
        signature: AggSignature,
    },
    Cast {
        expr: Box<BlockAggregateExpr>,
        ty: ExprType,
    },
}

impl BlockAggregateExpr {
    pub fn count_rows() -> Self {
        Self::CountRows {
            ty: scalar_type(crate::types::ScalarType::I64, false),
        }
    }

    pub fn ty(&self) -> &ExprType {
        match self {
            Self::CountRows { ty } | Self::Cast { ty, .. } => ty,
            Self::Aggregate { signature, .. } => &signature.result,
        }
    }

    /// The CountRows or Aggregate owner below all explicit conversions.
    pub fn leaf(&self) -> &Self {
        let mut current = self;
        while let Self::Cast { expr, .. } = current {
            current = expr;
        }
        current
    }

    pub fn arg(&self) -> Option<&IRExpr> {
        let mut current = self;
        loop {
            match current {
                Self::CountRows { .. } => return None,
                Self::Aggregate { arg, .. } => return Some(arg),
                Self::Cast { expr, .. } => current = expr,
            }
        }
    }

    pub fn arg_mut(&mut self) -> Option<&mut IRExpr> {
        let mut current = self;
        loop {
            match current {
                Self::CountRows { .. } => return None,
                Self::Aggregate { arg, .. } => return Some(arg),
                Self::Cast { expr, .. } => current = expr,
            }
        }
    }

    pub fn check_types(&self) -> Result<()> {
        let mut current = self;
        loop {
            match current {
                Self::CountRows { ty } => {
                    if ty != &scalar_type(crate::types::ScalarType::I64, false) {
                        return Err(CompilerError::Plan(
                            "row count must declare non-null I64".into(),
                        ));
                    }
                    return Ok(());
                }
                Self::Aggregate {
                    func,
                    arg,
                    signature,
                } => {
                    if matches!(signature.arg, ExprType::Node { .. }) {
                        return Err(CompilerError::Plan(
                            "counting a node binding must use CountRows".into(),
                        ));
                    }
                    check_aggregate_types(*func, arg, signature)?;
                    return arg.check_types();
                }
                Self::Cast { expr, ty } => {
                    if !coerce::cast_allowed(expr.ty(), ty) {
                        return Err(CompilerError::Plan(
                            "stored block Cast is not a permitted widening conversion".into(),
                        ));
                    }
                    current = expr;
                }
            }
        }
    }
}

impl std::fmt::Display for BlockAggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut current = self;
        loop {
            match current {
                Self::CountRows { .. } => return f.write_str("count"),
                Self::Aggregate { func, arg, .. } => return write!(f, "{func}({arg})"),
                Self::Cast { expr, .. } => current = expr,
            }
        }
    }
}

/// The HAVING predicate of one correlated block, with both conversions stored.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubqueryPredicate {
    pub left: BlockAggregateExpr,
    pub op: CompOp,
    pub right: IRExpr,
}

impl SubqueryPredicate {
    /// `count = 0`: what `not { … }` lowers to.
    pub fn not_exists() -> Self {
        Self {
            left: BlockAggregateExpr::count_rows(),
            op: CompOp::Eq,
            right: IRExpr::Literal(
                Literal::Integer(0),
                scalar_type(crate::types::ScalarType::I64, false),
            ),
        }
    }

    pub fn is_row_count(&self) -> bool {
        matches!(self.left.leaf(), BlockAggregateExpr::CountRows { .. })
    }

    pub fn check_types(&self) -> Result<()> {
        self.left.check_types()?;
        self.right.check_types()?;
        let mut bound = &self.right;
        while let IRExpr::Cast { expr, .. } = bound {
            bound = expr;
        }
        if !matches!(bound, IRExpr::Literal(_, _) | IRExpr::Param(_, _)) {
            return Err(CompilerError::Plan(
                "block comparison bound must be a literal or parameter with explicit casts".into(),
            ));
        }
        let (left, right) =
            coerce::comparison_types(self.op, self.left.ty(), None, self.right.ty(), None)?;
        if &left != self.left.ty() || &right != self.right.ty() {
            return Err(CompilerError::Plan(
                "block comparison operands need explicit casts to one stored domain".into(),
            ));
        }
        if matches!(
            self.op,
            CompOp::Contains | CompOp::StringContains | CompOp::StartsWith
        ) {
            return Err(CompilerError::Plan(
                "block comparison requires equality or ordering".into(),
            ));
        }
        if self.left.ty().is_list() || self.right.ty().is_list() {
            return Err(CompilerError::Plan(
                "block comparison operands must be scalar".into(),
            ));
        }
        Ok(())
    }

    /// Only an uncast row count against a canonical literal may bypass the
    /// stored comparison and ask solely whether a match exists.
    pub fn existence(&self) -> Option<bool> {
        let BlockAggregateExpr::CountRows { ty } = &self.left else {
            return None;
        };
        let IRExpr::Literal(Literal::Integer(bound), bound_ty) = &self.right else {
            return None;
        };
        let canonical = scalar_type(crate::types::ScalarType::I64, false);
        if ty != &canonical || bound_ty != &canonical {
            return None;
        }
        match (self.op, *bound) {
            (CompOp::Eq | CompOp::Le, 0) | (CompOp::Lt, 1) => Some(false),
            (CompOp::Ne | CompOp::Gt, 0) | (CompOp::Ge, 1) => Some(true),
            _ => None,
        }
    }

    pub fn is_existence_test(&self) -> bool {
        self.existence().is_some()
    }
    pub fn is_not_exists(&self) -> bool {
        self.existence() == Some(false)
    }
}

impl std::fmt::Display for SubqueryPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl IRExpr {
    /// The compiler-owned result type of every expression node.
    pub fn ty(&self) -> &ExprType {
        match self {
            Self::PropAccess { ty, .. }
            | Self::Nearest { ty, .. }
            | Self::Search { ty, .. }
            | Self::Fuzzy { ty, .. }
            | Self::MatchText { ty, .. }
            | Self::Bm25 { ty, .. }
            | Self::Rrf { ty, .. }
            | Self::Binary { ty, .. }
            | Self::IsNull { ty, .. }
            | Self::Cast { ty, .. }
            | Self::Variable(_, ty)
            | Self::Param(_, ty)
            | Self::Literal(_, ty)
            | Self::AliasRef(_, ty)
            | Self::Not(_, ty) => ty,
            Self::Aggregate { signature, .. } => &signature.result,
        }
    }

    /// Boolean builders preserve stored child nullability without choosing an
    /// operand conversion. Compiler lowering owns coercion before using them.
    pub fn binary(left: IRExpr, op: BinaryOp, right: IRExpr) -> Self {
        let ty = boolean_type(left.ty().nullable() || right.ty().nullable());
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
            ty,
        }
    }

    pub fn logical_not(expr: IRExpr) -> Self {
        let ty = boolean_type(expr.ty().nullable());
        Self::Not(Box::new(expr), ty)
    }

    pub fn null_test(expr: IRExpr, negated: bool) -> Self {
        Self::IsNull {
            expr: Box::new(expr),
            negated,
            ty: boolean_type(false),
        }
    }

    /// Visit each typed leaf without recursing on a user-sized expression tree.
    pub fn visit_leaves(&self, mut visit: impl FnMut(&Self)) {
        let mut pending = vec![self];
        while let Some(expr) = pending.pop() {
            match expr {
                Self::PropAccess { .. }
                | Self::Variable(_, _)
                | Self::Param(_, _)
                | Self::Literal(_, _)
                | Self::AliasRef(_, _) => visit(expr),
                Self::Nearest { query, .. } => pending.push(query),
                Self::Search { field, query, .. }
                | Self::MatchText { field, query, .. }
                | Self::Bm25 { field, query, .. } => {
                    pending.extend([field.as_ref(), query.as_ref()])
                }
                Self::Fuzzy {
                    field,
                    query,
                    max_edits,
                    ..
                } => {
                    pending.extend([field.as_ref(), query.as_ref()]);
                    pending.extend(max_edits.as_deref());
                }
                Self::Rrf {
                    primary,
                    secondary,
                    k,
                    ..
                } => {
                    pending.extend([primary.as_ref(), secondary.as_ref()]);
                    pending.extend(k.as_deref());
                }
                Self::Aggregate { arg, .. }
                | Self::Not(arg, _)
                | Self::IsNull { expr: arg, .. }
                | Self::Cast { expr: arg, .. } => pending.push(arg),
                Self::Binary { left, right, .. } => pending.extend([left.as_ref(), right.as_ref()]),
            }
        }
    }

    /// The compiler-owned type of a leaf, retained through planner rewrites.
    pub fn leaf_type(&self) -> Option<&ExprType> {
        match self {
            Self::PropAccess { ty, .. }
            | Self::Variable(_, ty)
            | Self::Param(_, ty)
            | Self::Literal(_, ty)
            | Self::AliasRef(_, ty) => Some(ty),
            Self::Nearest { .. }
            | Self::Search { .. }
            | Self::Fuzzy { .. }
            | Self::MatchText { .. }
            | Self::Bm25 { .. }
            | Self::Rrf { .. }
            | Self::Aggregate { .. }
            | Self::Binary { .. }
            | Self::Not(_, _)
            | Self::IsNull { .. }
            | Self::Cast { .. } => None,
        }
    }

    /// Validate each stored local type rule without consulting the catalog or
    /// bound parameter values. Operator return owners validate alias identity.
    pub fn check_types(&self) -> Result<()> {
        use crate::types::ScalarType;
        let mut pending = vec![self];
        while let Some(expr) = pending.pop() {
            let bad = |detail: &str| CompilerError::Plan(detail.to_string());
            let expect = |ty: &ExprType, expected: ExprType| {
                if ty == &expected {
                    Ok(())
                } else {
                    Err(bad("stored expression result type violates its local rule"))
                }
            };
            match expr {
                Self::Aggregate {
                    func,
                    arg,
                    signature,
                } => {
                    check_aggregate_types(*func, arg, signature)?;
                    pending.push(arg);
                }
                Self::Nearest { query, ty, .. } => {
                    expect(ty, scalar_type(ScalarType::F32, false))?;
                    let accepted = matches!(
                        query.ty(),
                        ExprType::Value {
                            scalar: ScalarType::String | ScalarType::Vector(_),
                            list: false,
                            ..
                        }
                    ) || matches!(query.as_ref(), Self::Literal(Literal::List(_), ExprType::Value { scalar, list: true, .. }) if scalar.is_numeric());
                    if !accepted {
                        return Err(bad(
                            "nearest query must be String, Vector, or a numeric literal list",
                        ));
                    }
                    pending.push(query);
                }
                Self::Search { field, query, ty }
                | Self::MatchText { field, query, ty }
                | Self::Bm25 { field, query, ty } => {
                    let result = if matches!(expr, Self::Bm25 { .. }) {
                        ScalarType::F32
                    } else {
                        ScalarType::Bool
                    };
                    expect(ty, scalar_type(result, false))?;
                    if !scalar_is(field.ty(), ScalarType::String)
                        || !scalar_is(query.ty(), ScalarType::String)
                    {
                        return Err(bad("search operands must be scalar String"));
                    }
                    pending.extend([field.as_ref(), query.as_ref()]);
                }
                Self::Fuzzy {
                    field,
                    query,
                    max_edits,
                    ty,
                } => {
                    expect(ty, boolean_type(false))?;
                    if !scalar_is(field.ty(), ScalarType::String)
                        || !scalar_is(query.ty(), ScalarType::String)
                        || max_edits
                            .as_ref()
                            .is_some_and(|value| !integer_type(value.ty()))
                    {
                        return Err(bad("fuzzy operands violate their stored types"));
                    }
                    pending.extend([field.as_ref(), query.as_ref()]);
                    pending.extend(max_edits.as_deref());
                }
                Self::Rrf {
                    primary,
                    secondary,
                    k,
                    ty,
                } => {
                    expect(ty, scalar_type(ScalarType::F64, false))?;
                    for arm in [primary.as_ref(), secondary.as_ref()] {
                        if !matches!(arm, Self::Nearest { .. } | Self::Bm25 { .. })
                            || !scalar_is(arm.ty(), ScalarType::F32)
                        {
                            return Err(bad("rrf arms must be stored nearest or bm25 scores"));
                        }
                    }
                    if k.as_ref().is_some_and(|value| !integer_type(value.ty())) {
                        return Err(bad("rrf k must be an integer scalar"));
                    }
                    pending.extend([primary.as_ref(), secondary.as_ref()]);
                    pending.extend(k.as_deref());
                }
                Self::Binary {
                    left,
                    op,
                    right,
                    ty,
                } => {
                    expect(
                        ty,
                        boolean_type(left.ty().nullable() || right.ty().nullable()),
                    )?;
                    match op {
                        BinaryOp::And | BinaryOp::Or => {
                            if !scalar_is(left.ty(), ScalarType::Bool)
                                || !scalar_is(right.ty(), ScalarType::Bool)
                            {
                                return Err(bad("logical operands must be scalar Bool"));
                            }
                        }
                        BinaryOp::Compare(op) => {
                            let (expected_left, expected_right) =
                                coerce::comparison_types(*op, left.ty(), None, right.ty(), None)?;
                            if &expected_left != left.ty() || &expected_right != right.ty() {
                                return Err(bad(
                                    "comparison operands need explicit casts to one stored domain",
                                ));
                            }
                            if *op == CompOp::Contains
                                && (!left.ty().is_list() || right.ty().is_list())
                            {
                                return Err(bad("contains needs a list and a scalar operand"));
                            }
                            if *op != CompOp::Contains
                                && (left.ty().is_list() || right.ty().is_list())
                            {
                                return Err(bad("list comparisons require membership"));
                            }
                        }
                    }
                    pending.extend([left.as_ref(), right.as_ref()]);
                }
                Self::Not(inner, ty) => {
                    expect(ty, boolean_type(inner.ty().nullable()))?;
                    if !scalar_is(inner.ty(), ScalarType::Bool) {
                        return Err(bad("not operand must be scalar Bool"));
                    }
                    pending.push(inner);
                }
                Self::IsNull {
                    expr: inner, ty, ..
                } => {
                    expect(ty, boolean_type(false))?;
                    if matches!(inner.ty(), ExprType::Node { .. }) {
                        return Err(bad("is null needs a value operand"));
                    }
                    pending.push(inner);
                }
                Self::Cast { expr: inner, ty } => {
                    if !coerce::cast_expr_allowed(inner, ty) {
                        return Err(bad("stored Cast is not a permitted numeric conversion"));
                    }
                    pending.push(inner);
                }
                Self::PropAccess { ty, .. } | Self::Param(_, ty) | Self::Literal(_, ty) => {
                    if !matches!(ty, ExprType::Value { .. }) {
                        return Err(bad("value leaf carries an internal or node type"));
                    }
                    validate_leaf_type(ty)?;
                }
                Self::Variable(_, ty) => {
                    if !matches!(ty, ExprType::Node { .. }) {
                        return Err(bad("node variable carries a value type"));
                    }
                    validate_leaf_type(ty)?;
                }
                Self::AliasRef(_, ty) => {
                    if matches!(ty, ExprType::ExactInteger { .. }) {
                        return Err(bad("alias cannot declare an exact carrier"));
                    }
                    validate_leaf_type(ty)?;
                }
            }
        }
        Ok(())
    }

    /// `left <op> right` as one comparison node.
    pub fn comparison(left: IRExpr, op: CompOp, right: IRExpr) -> Self {
        IRExpr::binary(left, BinaryOp::Compare(op), right)
    }

    /// The operands and operator of a comparison-rooted expression; `None`
    /// for every other node.
    pub fn comparison_parts(&self) -> Option<(&IRExpr, CompOp, &IRExpr)> {
        match self {
            IRExpr::Binary {
                left,
                op: BinaryOp::Compare(op),
                right,
                ty: _,
            } => Some((left, *op, right)),
            _ => None,
        }
    }

    /// The top-level `and` chain split into its conjuncts, in written order;
    /// an `or`, a `not` or a comparison is one conjunct.
    pub fn into_conjuncts(self) -> Vec<IRExpr> {
        match self {
            IRExpr::Binary {
                left,
                op: BinaryOp::And,
                right,
                ty: _,
            } => {
                let mut conjuncts = left.into_conjuncts();
                conjuncts.extend(right.into_conjuncts());
                conjuncts
            }
            other => vec![other],
        }
    }

    /// The conjunction of `conjuncts`, left-nested; `None` when empty.
    pub fn and_all(conjuncts: impl IntoIterator<Item = IRExpr>) -> Option<IRExpr> {
        conjuncts
            .into_iter()
            .reduce(|left, right| IRExpr::binary(left, BinaryOp::And, right))
    }

    fn precedence(&self) -> Precedence {
        match self {
            IRExpr::Binary {
                op: BinaryOp::Or, ..
            } => Precedence::Or,
            IRExpr::Binary {
                op: BinaryOp::And, ..
            } => Precedence::And,
            IRExpr::Not(_, _) => Precedence::Not,
            IRExpr::Cast { expr, .. } => expr.precedence(),
            IRExpr::Binary {
                op: BinaryOp::Compare(_),
                ..
            }
            | IRExpr::IsNull { .. } => Precedence::Comparison,
            _ => Precedence::Atom,
        }
    }

    /// Print `self` as an operand of a node with `parent` precedence, in
    /// parentheses when it binds looser, when both are comparisons or null tests,
    /// or as the right operand of an `and`/`or` it repeats (left-nested prints bare).
    fn fmt_operand(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        parent: Precedence,
        right_operand: bool,
    ) -> std::fmt::Result {
        let own = self.precedence();
        let parenthesized =
            own < parent || (own == parent && (parent == Precedence::Comparison || right_operand));
        if parenthesized {
            write!(f, "({self})")
        } else {
            write!(f, "{self}")
        }
    }
}

/// The expression as GQ text, the spelling the parser accepts; the `now()`
/// parameter prints as the call it came from. Parentheses are the minimal
/// ones (`fmt_operand`), never the user's.
impl std::fmt::Display for IRExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IRExpr::Cast { expr, .. } => expr.fmt(f),
            IRExpr::Binary {
                left,
                op,
                right,
                ty: _,
            } => {
                let precedence = self.precedence();
                left.fmt_operand(f, precedence, false)?;
                write!(f, " {op} ")?;
                right.fmt_operand(f, precedence, true)
            }
            IRExpr::Not(operand, _) => {
                f.write_str("not ")?;
                operand.fmt_operand(f, Precedence::Not, false)
            }
            IRExpr::IsNull {
                expr,
                negated,
                ty: _,
            } => {
                expr.fmt_operand(f, Precedence::Comparison, false)?;
                f.write_str(if *negated { " is not null" } else { " is null" })
            }
            IRExpr::PropAccess {
                variable,
                property,
                ty: _,
            } => write!(f, "${variable}.{property}"),
            IRExpr::Nearest {
                variable,
                property,
                query,
                ty: _,
            } => write!(f, "nearest(${variable}.{property}, {query})"),
            IRExpr::Search {
                field,
                query,
                ty: _,
            } => write!(f, "search({field}, {query})"),
            IRExpr::Fuzzy {
                field,
                query,
                max_edits: None,
                ty: _,
            } => write!(f, "fuzzy({field}, {query})"),
            IRExpr::Fuzzy {
                field,
                query,
                max_edits: Some(max_edits),
                ty: _,
            } => write!(f, "fuzzy({field}, {query}, {max_edits})"),
            IRExpr::MatchText {
                field,
                query,
                ty: _,
            } => write!(f, "match_text({field}, {query})"),
            IRExpr::Bm25 {
                field,
                query,
                ty: _,
            } => write!(f, "bm25({field}, {query})"),
            IRExpr::Rrf {
                primary,
                secondary,
                k: None,
                ty: _,
            } => write!(f, "rrf({primary}, {secondary})"),
            IRExpr::Rrf {
                primary,
                secondary,
                k: Some(k),
                ty: _,
            } => write!(f, "rrf({primary}, {secondary}, {k})"),
            IRExpr::Variable(name, _) => write!(f, "${name}"),
            IRExpr::Param(name, _) if name == NOW_PARAM_NAME => f.write_str("now()"),
            IRExpr::Param(name, _) => write!(f, "${name}"),
            IRExpr::Literal(literal, _) => write!(f, "{literal}"),
            IRExpr::Aggregate { func, arg, .. } => write!(f, "{func}({arg})"),
            IRExpr::AliasRef(alias, _) => f.write_str(alias),
        }
    }
}

#[cfg(test)]
mod render_tests {
    use super::*;

    fn prop(variable: &str, property: &str) -> IRExpr {
        IRExpr::PropAccess {
            variable: variable.to_string(),
            property: property.to_string(),
            ty: match property {
                "age" | "a" | "b" | "c" => {
                    ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::I64, false))
                }
                "since" => ExprType::from_prop(&PropType::scalar(
                    crate::types::ScalarType::DateTime,
                    false,
                )),
                "tags" => {
                    ExprType::from_prop(&PropType::list_of(crate::types::ScalarType::String, false))
                }
                _ => {
                    ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::String, false))
                }
            },
        }
    }

    fn int(value: i64) -> IRExpr {
        IRExpr::Literal(
            Literal::Integer(value),
            crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                crate::types::ScalarType::I64,
                false,
            )),
        )
    }

    fn and(left: IRExpr, right: IRExpr) -> IRExpr {
        IRExpr::binary(left, BinaryOp::And, right)
    }

    fn or(left: IRExpr, right: IRExpr) -> IRExpr {
        IRExpr::binary(left, BinaryOp::Or, right)
    }

    #[test]
    fn aggregate_signature_must_match_its_stored_argument_leaf() {
        let arg_type = ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::I32, true));
        let mut expression = IRExpr::Aggregate {
            func: AggFunc::Sum,
            arg: Box::new(IRExpr::Param("amount".into(), arg_type.clone())),
            signature: AggSignature {
                arg: arg_type,
                result: ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::F64, true)),
            },
        };
        assert!(expression.check_types().is_ok());
        let IRExpr::Aggregate { arg, .. } = &mut expression else {
            unreachable!()
        };
        **arg = IRExpr::Param(
            "amount".into(),
            ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::I64, true)),
        );
        assert!(
            expression
                .check_types()
                .unwrap_err()
                .to_string()
                .contains("argument leaf")
        );

        let expression = IRExpr::Aggregate {
            func: AggFunc::Count,
            arg: Box::new(IRExpr::Variable(
                "p".into(),
                ExprType::Node {
                    type_name: "Other".into(),
                },
            )),
            signature: AggSignature {
                arg: ExprType::Node {
                    type_name: "Person".into(),
                },
                result: ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::I64, true)),
            },
        };
        assert!(
            expression
                .check_types()
                .unwrap_err()
                .to_string()
                .contains("argument leaf")
        );
    }

    #[test]
    fn invalid_cast_display_preserves_child_precedence_and_leaf_walks() {
        let disjunction = or(
            IRExpr::comparison(prop("p", "a"), CompOp::Eq, int(1)),
            IRExpr::comparison(prop("p", "b"), CompOp::Eq, int(2)),
        );
        let wrapped = IRExpr::Cast {
            ty: disjunction.ty().clone(),
            expr: Box::new(disjunction.clone()),
        };
        let plain = and(
            disjunction,
            IRExpr::comparison(prop("p", "c"), CompOp::Eq, int(3)),
        );
        let casted = and(
            wrapped,
            IRExpr::comparison(prop("p", "c"), CompOp::Eq, int(3)),
        );
        assert_eq!(casted.to_string(), plain.to_string());
        assert_eq!(casted.to_string(), "($p.a = 1 or $p.b = 2) and $p.c = 3");
        let mut leaves = Vec::new();
        casted.visit_leaves(|leaf| leaves.push(leaf.ty().clone()));
        assert_eq!(leaves.len(), 6);
        assert!(leaves.iter().all(|ty| *ty == int(1).ty().clone()));
    }

    #[test]
    fn universal_type_validates_interiors_and_internal_carrier_boundaries() {
        let mut expression = IRExpr::null_test(
            IRExpr::comparison(prop("p", "a"), CompOp::Eq, int(1)),
            false,
        );
        expression.check_types().unwrap();
        let IRExpr::IsNull { ty, .. } = &mut expression else {
            unreachable!()
        };
        *ty = scalar_type(crate::types::ScalarType::Bool, true);
        assert!(expression.check_types().is_err());
        let exact = ExprType::ExactInteger {
            list: false,
            nullable: false,
        };
        for leaf in [
            IRExpr::Literal(Literal::Integer(1), exact.clone()),
            IRExpr::Param("x".into(), exact.clone()),
            IRExpr::AliasRef("x".into(), exact),
        ] {
            assert!(leaf.check_types().is_err());
        }
    }

    #[test]
    fn filters_and_expressions_print_as_gq() {
        let filter = IRExpr::comparison(prop("p", "age"), CompOp::Gt, int(30));
        assert_eq!(filter.to_string(), "$p.age > 30");
        let filter = IRExpr::comparison(
            prop("d", "slug"),
            CompOp::StartsWith,
            IRExpr::Param(
                "prefix".to_string(),
                ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::String, false)),
            ),
        );
        assert_eq!(filter.to_string(), "$d.slug starts_with $prefix");
        let filter = IRExpr::comparison(
            prop("d", "tags"),
            CompOp::Contains,
            IRExpr::Literal(
                Literal::List(vec![
                    Literal::String("a\"b".to_string()),
                    Literal::Bool(true),
                    Literal::Float(1.5),
                ]),
                crate::types::ExprType::from_prop(&crate::types::PropType::list_of(
                    crate::types::ScalarType::String,
                    false,
                )),
            ),
        );
        assert_eq!(
            filter.to_string(),
            "$d.tags contains [\"a\\\"b\", true, 1.5]"
        );
        let filter = IRExpr::comparison(
            prop("e", "since"),
            CompOp::Le,
            IRExpr::Param(
                NOW_PARAM_NAME.to_string(),
                ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::DateTime, false)),
            ),
        );
        assert_eq!(filter.to_string(), "$e.since <= now()");
        let nearest = IRExpr::Nearest {
            variable: "d".to_string(),
            property: "embedding".to_string(),
            query: Box::new(IRExpr::Param(
                "q".to_string(),
                ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::String, false)),
            )),
            ty: crate::types::ExprType::Value {
                scalar: crate::types::ScalarType::F32,
                list: false,
                nullable: false,
            },
        };
        assert_eq!(nearest.to_string(), "nearest($d.embedding, $q)");
        let rrf = IRExpr::Rrf {
            primary: Box::new(nearest),
            secondary: Box::new(IRExpr::Bm25 {
                field: Box::new(prop("d", "text")),
                query: Box::new(IRExpr::Literal(
                    Literal::String("needle".to_string()),
                    crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                        crate::types::ScalarType::String,
                        false,
                    )),
                )),
                ty: crate::types::ExprType::Value {
                    scalar: crate::types::ScalarType::F32,
                    list: false,
                    nullable: false,
                },
            }),
            k: Some(Box::new(IRExpr::Literal(
                Literal::Integer(60),
                crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                    crate::types::ScalarType::I64,
                    false,
                )),
            ))),
            ty: crate::types::ExprType::Value {
                scalar: crate::types::ScalarType::F64,
                list: false,
                nullable: false,
            },
        };
        assert_eq!(
            rrf.to_string(),
            "rrf(nearest($d.embedding, $q), bm25($d.text, \"needle\"), 60)"
        );
        let count = IRExpr::Aggregate {
            func: AggFunc::Count,
            arg: Box::new(IRExpr::Variable(
                "f".to_string(),
                ExprType::Node {
                    type_name: "Friend".into(),
                },
            )),
            signature: AggSignature {
                arg: ExprType::Node {
                    type_name: "Friend".into(),
                },
                result: ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::I64, true)),
            },
        };
        assert_eq!(count.to_string(), "count($f)");
        for (expression, expected) in [
            (
                IRExpr::Search {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param(
                        "q".into(),
                        ExprType::from_prop(&PropType::scalar(
                            crate::types::ScalarType::String,
                            false,
                        )),
                    )),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::Bool,
                        list: false,
                        nullable: false,
                    },
                },
                "search($d.text, $q)",
            ),
            (
                IRExpr::MatchText {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param(
                        "q".into(),
                        ExprType::from_prop(&PropType::scalar(
                            crate::types::ScalarType::String,
                            false,
                        )),
                    )),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::Bool,
                        list: false,
                        nullable: false,
                    },
                },
                "match_text($d.text, $q)",
            ),
            (
                IRExpr::Fuzzy {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param(
                        "q".into(),
                        ExprType::from_prop(&PropType::scalar(
                            crate::types::ScalarType::String,
                            false,
                        )),
                    )),
                    max_edits: None,
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::Bool,
                        list: false,
                        nullable: false,
                    },
                },
                "fuzzy($d.text, $q)",
            ),
            (
                IRExpr::Fuzzy {
                    field: Box::new(prop("d", "text")),
                    query: Box::new(IRExpr::Param(
                        "q".into(),
                        ExprType::from_prop(&PropType::scalar(
                            crate::types::ScalarType::String,
                            false,
                        )),
                    )),
                    max_edits: Some(Box::new(IRExpr::Literal(
                        Literal::Integer(2),
                        crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                            crate::types::ScalarType::I64,
                            false,
                        )),
                    ))),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::Bool,
                        list: false,
                        nullable: false,
                    },
                },
                "fuzzy($d.text, $q, 2)",
            ),
            (
                IRExpr::AliasRef(
                    "score".into(),
                    ExprType::from_prop(&PropType::scalar(crate::types::ScalarType::F32, false)),
                ),
                "score",
            ),
            (
                IRExpr::Literal(
                    Literal::Null,
                    crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                        crate::types::ScalarType::String,
                        true,
                    )),
                ),
                "null",
            ),
            (
                IRExpr::Literal(
                    Literal::DateTime("2026-01-31T12:00:00Z".into()),
                    crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                        crate::types::ScalarType::DateTime,
                        false,
                    )),
                ),
                "datetime(\"2026-01-31T12:00:00Z\")",
            ),
        ] {
            assert_eq!(expression.to_string(), expected);
        }

        assert_eq!(
            IRExpr::Literal(
                Literal::Date("2026-01-31".to_string()),
                crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                    crate::types::ScalarType::Date,
                    false
                ))
            )
            .to_string(),
            "date(\"2026-01-31\")"
        );
    }

    #[test]
    fn boolean_trees_print_with_minimal_parentheses() {
        let a = IRExpr::comparison(prop("p", "a"), CompOp::Eq, int(1));
        let b = IRExpr::comparison(prop("p", "b"), CompOp::Eq, int(2));
        let c = IRExpr::comparison(prop("p", "c"), CompOp::Eq, int(3));
        assert_eq!(
            or(a.clone(), and(b.clone(), c.clone())).to_string(),
            "$p.a = 1 or $p.b = 2 and $p.c = 3"
        );
        assert_eq!(
            and(or(a.clone(), b.clone()), c.clone()).to_string(),
            "($p.a = 1 or $p.b = 2) and $p.c = 3"
        );
        assert_eq!(
            IRExpr::logical_not(and(a.clone(), b.clone())).to_string(),
            "not ($p.a = 1 and $p.b = 2)"
        );
        assert_eq!(
            and(IRExpr::logical_not(a.clone()), b.clone()).to_string(),
            "not $p.a = 1 and $p.b = 2"
        );
        assert_eq!(
            and(and(a.clone(), b.clone()), c.clone()).to_string(),
            "$p.a = 1 and $p.b = 2 and $p.c = 3"
        );
        assert_eq!(
            and(a.clone(), and(b.clone(), c.clone())).to_string(),
            "$p.a = 1 and ($p.b = 2 and $p.c = 3)"
        );
        assert_eq!(
            or(a.clone(), or(b.clone(), c.clone())).to_string(),
            "$p.a = 1 or ($p.b = 2 or $p.c = 3)"
        );
        let age = IRExpr::comparison(prop("p", "age"), CompOp::Gt, int(30));
        assert_eq!(
            IRExpr::comparison(
                age.clone(),
                CompOp::Eq,
                IRExpr::Literal(
                    Literal::Bool(true),
                    crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                        crate::types::ScalarType::Bool,
                        false
                    ))
                )
            )
            .to_string(),
            "($p.age > 30) = true"
        );
        assert_eq!(
            IRExpr::null_test(age.clone(), false).to_string(),
            "($p.age > 30) is null"
        );
        assert_eq!(
            IRExpr::null_test(prop("p", "age"), true).to_string(),
            "$p.age is not null"
        );
        assert_eq!(
            and(and(a.clone(), b.clone()), c.clone()).into_conjuncts(),
            vec![a.clone(), b.clone(), c.clone()]
        );
        assert_eq!(
            IRExpr::and_all([a.clone(), b.clone(), c.clone()]),
            Some(and(and(a, b), c))
        );
    }

    #[test]
    fn floats_print_in_fixed_notation_with_a_decimal_point() {
        for (value, text) in [
            (1e-7, "0.0000001"),
            (1e21, "1000000000000000000000.0"),
            (2.0, "2.0"),
            (-0.25, "-0.25"),
            (-3e-9, "-0.000000003"),
        ] {
            assert_eq!(Literal::Float(value).to_string(), text);
        }
    }

    #[test]
    fn string_escapes_are_the_ones_the_parser_decodes() {
        let text = "a\nb\r\tc\\\"d";
        let printed = Literal::String(text.to_string()).to_string();
        assert_eq!(printed, "\"a\\nb\\r\\tc\\\\\\\"d\"");
        assert_eq!(crate::error::decode_string_literal(&printed).unwrap(), text);
    }
}

fn scalar_type(scalar: crate::types::ScalarType, nullable: bool) -> ExprType {
    ExprType::Value {
        scalar,
        list: false,
        nullable,
    }
}

fn boolean_type(nullable: bool) -> ExprType {
    scalar_type(crate::types::ScalarType::Bool, nullable)
}

fn scalar_is(ty: &ExprType, expected: crate::types::ScalarType) -> bool {
    matches!(ty, ExprType::Value { scalar, list: false, .. } if *scalar == expected)
}

fn integer_type(ty: &ExprType) -> bool {
    matches!(
        ty,
        ExprType::Value {
            scalar: crate::types::ScalarType::I32
                | crate::types::ScalarType::I64
                | crate::types::ScalarType::U32
                | crate::types::ScalarType::U64,
            list: false,
            ..
        }
    )
}

fn check_aggregate_types(func: AggFunc, arg: &IRExpr, signature: &AggSignature) -> Result<()> {
    let expected = func
        .result_type(&signature.arg)
        .map(|scalar| ExprType::from_prop(&PropType::scalar(scalar, true)))
        .ok_or_else(|| {
            CompilerError::Plan(format!(
                "invalid {func} aggregate argument {}",
                signature.arg.spelling()
            ))
        })?;
    if signature.result != expected {
        return Err(CompilerError::Plan(format!(
            "{func}({}) has invalid result type {}",
            signature.arg.spelling(),
            signature.result.spelling()
        )));
    }
    if arg.ty() != &signature.arg {
        return Err(CompilerError::Plan(format!(
            "{func} argument leaf has {}, signature declares {}",
            arg.ty().spelling(),
            signature.arg.spelling()
        )));
    }
    Ok(())
}

fn validate_leaf_type(ty: &ExprType) -> Result<()> {
    match ty {
        ExprType::Value {
            scalar: crate::types::ScalarType::Vector(dim),
            list,
            nullable: _,
        } if *list || *dim == 0 || *dim > i32::MAX as u32 => Err(CompilerError::Plan(
            "invalid vector type on expression leaf".into(),
        )),
        ExprType::Node { type_name } if type_name.is_empty() => Err(CompilerError::Plan(
            "node leaf has an empty type name".into(),
        )),
        _ => Ok(()),
    }
}

/// The bound expression: one type for filters, projections, order keys,
/// assignments and mutation predicates. Equal when the trees are equal node
/// by node (`Literal::Float` by bits), the equality the planner's filter
/// deduplication and the `Predicate` hash rely on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IRExpr {
    PropAccess {
        variable: String,
        property: String,
        ty: ExprType,
    },
    Nearest {
        variable: String,
        property: String,
        query: Box<IRExpr>,
        ty: ExprType,
    },
    Search {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
        ty: ExprType,
    },
    Fuzzy {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
        max_edits: Option<Box<IRExpr>>,
        ty: ExprType,
    },
    MatchText {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
        ty: ExprType,
    },
    Bm25 {
        field: Box<IRExpr>,
        query: Box<IRExpr>,
        ty: ExprType,
    },
    Rrf {
        primary: Box<IRExpr>,
        secondary: Box<IRExpr>,
        k: Option<Box<IRExpr>>,
        ty: ExprType,
    },
    Variable(String, ExprType),
    Param(String, ExprType),
    Literal(Literal, ExprType),
    Aggregate {
        func: AggFunc,
        arg: Box<IRExpr>,
        signature: AggSignature,
    },
    AliasRef(String, ExprType),
    Binary {
        left: Box<IRExpr>,
        op: BinaryOp,
        right: Box<IRExpr>,
        ty: ExprType,
    },
    Not(Box<IRExpr>, ExprType),
    Cast {
        expr: Box<IRExpr>,
        ty: ExprType,
    },
    IsNull {
        expr: Box<IRExpr>,
        negated: bool,
        ty: ExprType,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IRProjection {
    pub expr: IRExpr,
    pub alias: Option<String>,
    /// The executed column name, before lowering rewrites meta fields.
    pub column: String,
    pub ty: ExprType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IROrdering {
    pub expr: IRExpr,
    pub descending: bool,
}

#[cfg(test)]
mod block_type_tests {
    use super::*;
    use crate::types::ScalarType;

    fn value(scalar: ScalarType, nullable: bool) -> ExprType {
        ExprType::from_prop(&PropType::scalar(scalar, nullable))
    }
    fn aggregate(func: AggFunc, input: ScalarType, result: ScalarType) -> BlockAggregateExpr {
        let arg = value(input, true);
        BlockAggregateExpr::Aggregate {
            func,
            arg: Box::new(IRExpr::PropAccess {
                variable: "child".into(),
                property: "amount".into(),
                ty: arg.clone(),
            }),
            signature: AggSignature {
                arg,
                result: value(result, true),
            },
        }
    }

    #[test]
    fn block_coercion_uses_the_declared_aggregate_result_owner() {
        let predicate = coerce::block(
            aggregate(AggFunc::Max, ScalarType::I64, ScalarType::I64),
            CompOp::Eq,
            IRExpr::Literal(Literal::Float(1.0), value(ScalarType::F64, false)),
        )
        .unwrap();
        assert!(matches!(
            predicate.left,
            BlockAggregateExpr::Aggregate { .. }
        ));
        assert!(matches!(predicate.right, IRExpr::Cast { .. }));
        assert_eq!(predicate.left.ty(), &value(ScalarType::I64, true));
        assert_eq!(predicate.right.ty(), &value(ScalarType::I64, false));
        assert_eq!(predicate.to_string(), "max($child.amount) = 1.0");

        let predicate = coerce::block(
            aggregate(AggFunc::Max, ScalarType::F32, ScalarType::F32),
            CompOp::Eq,
            IRExpr::Param("bound".into(), value(ScalarType::F64, false)),
        )
        .unwrap();
        assert!(matches!(predicate.left, BlockAggregateExpr::Cast { .. }));
        assert_eq!(predicate.left.ty(), &value(ScalarType::F64, true));
        let BlockAggregateExpr::Aggregate { signature, .. } = predicate.left.leaf() else {
            panic!("expected owner")
        };
        assert_eq!(signature.result, value(ScalarType::F32, true));

        let predicate = coerce::block(
            aggregate(AggFunc::Max, ScalarType::U64, ScalarType::U64),
            CompOp::Gt,
            IRExpr::Param("bound".into(), value(ScalarType::I64, true)),
        )
        .unwrap();
        assert_eq!(
            predicate.left.ty(),
            &ExprType::ExactInteger {
                list: false,
                nullable: true
            }
        );
        assert_eq!(predicate.right.ty(), predicate.left.ty());
    }

    #[test]
    fn counts_keep_their_distinct_nullability_and_conservative_existence_path() {
        let row = SubqueryPredicate::not_exists();
        row.check_types().unwrap();
        assert_eq!(row.left.ty(), &value(ScalarType::I64, false));
        assert_eq!(row.existence(), Some(false));
        let column = coerce::block(
            aggregate(AggFunc::Count, ScalarType::I64, ScalarType::I64),
            CompOp::Eq,
            IRExpr::Literal(Literal::Integer(0), value(ScalarType::I64, false)),
        )
        .unwrap();
        assert_eq!(column.left.ty(), &value(ScalarType::I64, true));
        assert!(!column.is_row_count());
        assert!(!column.is_existence_test());
        let converted = coerce::block(
            BlockAggregateExpr::count_rows(),
            CompOp::Eq,
            IRExpr::Param("bound".into(), value(ScalarType::U64, false)),
        )
        .unwrap();
        assert!(converted.is_row_count());
        assert!(!converted.is_existence_test());
        assert_eq!(
            converted.left.ty(),
            &ExprType::ExactInteger {
                list: false,
                nullable: false
            }
        );

        let mut nullable_bound = row.clone();
        nullable_bound.right = IRExpr::Literal(Literal::Integer(0), value(ScalarType::I64, true));
        nullable_bound.check_types().unwrap();
        assert!(!nullable_bound.is_existence_test());
        let converted_zero = SubqueryPredicate {
            left: BlockAggregateExpr::Cast {
                expr: Box::new(BlockAggregateExpr::count_rows()),
                ty: value(ScalarType::F64, false),
            },
            op: CompOp::Eq,
            right: IRExpr::Cast {
                expr: Box::new(IRExpr::Literal(
                    Literal::Integer(0),
                    value(ScalarType::I64, false),
                )),
                ty: value(ScalarType::F64, false),
            },
        };
        converted_zero.check_types().unwrap();
        assert!(!converted_zero.is_existence_test());
    }

    #[test]
    fn block_validation_refuses_forged_signatures_casts_and_nonconstant_bounds() {
        for op in [CompOp::Contains, CompOp::StringContains, CompOp::StartsWith] {
            let predicate = SubqueryPredicate {
                left: aggregate(AggFunc::Max, ScalarType::String, ScalarType::String),
                op,
                right: IRExpr::Literal(
                    Literal::String("x".into()),
                    value(ScalarType::String, false),
                ),
            };
            assert!(predicate.check_types().is_err());
        }
        let base = coerce::block(
            aggregate(AggFunc::Max, ScalarType::I64, ScalarType::I64),
            CompOp::Gt,
            IRExpr::Param("bound".into(), value(ScalarType::I64, false)),
        )
        .unwrap();
        for scalar in [ScalarType::I32, ScalarType::F64] {
            let mut forged = base.clone();
            let BlockAggregateExpr::Aggregate { signature, .. } = &mut forged.left else {
                unreachable!()
            };
            signature.arg = value(scalar, true);
            assert!(forged.check_types().is_err());
        }
        let mut forged = base.clone();
        let BlockAggregateExpr::Aggregate { signature, .. } = &mut forged.left else {
            unreachable!()
        };
        signature.result = value(ScalarType::I64, false);
        assert!(forged.check_types().is_err());
        let mut forged = base.clone();
        forged.right = IRExpr::PropAccess {
            variable: "outer".into(),
            property: "amount".into(),
            ty: value(ScalarType::I64, false),
        };
        assert!(forged.check_types().is_err());
        let mut forged = base.clone();
        forged.right = IRExpr::Cast {
            expr: Box::new(forged.right),
            ty: value(ScalarType::F64, false),
        };
        assert!(forged.check_types().is_err());
        for ty in [value(ScalarType::I64, true), value(ScalarType::U64, false)] {
            let mut forged = SubqueryPredicate::not_exists();
            forged.left = BlockAggregateExpr::CountRows { ty };
            assert!(forged.check_types().is_err());
        }
        let forged = BlockAggregateExpr::Cast {
            expr: Box::new(BlockAggregateExpr::count_rows()),
            ty: value(ScalarType::I32, false),
        };
        assert!(forged.check_types().is_err());
        let node = ExprType::Node {
            type_name: "Person".into(),
        };
        let forged = BlockAggregateExpr::Aggregate {
            func: AggFunc::Count,
            arg: Box::new(IRExpr::Variable("p".into(), node.clone())),
            signature: AggSignature {
                arg: node,
                result: value(ScalarType::I64, true),
            },
        };
        assert!(forged.check_types().is_err());
    }
}
