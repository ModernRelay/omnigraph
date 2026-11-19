//! The bound expression every clause reads ([`IRExpr`]): filters,
//! projections, order keys, assignments and mutation predicates, with the
//! correlated block predicate's aggregate side.

use crate::error::{CompilerError, Result};
use crate::query::syntax::ArithOp;
use crate::query::value::{AggFunc, BinaryOp, CompOp, Literal, NOW_PARAM_NAME, Precedence};
use crate::types::{AggSignature, ExprType, ScalarType};

use super::coerce;

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
                    // A block that holds values to finish them (`stddev`,
                    // `percentile`) is read as a value through its plan's
                    // aggregate, which holds `percentile`'s fraction; no
                    // semi-join folds one.
                    if matches!(func, AggFunc::Stddev | AggFunc::Percentile) {
                        return Err(CompilerError::Plan(format!(
                            "a {func} block is read as a value, never folded by a semi-join"
                        )));
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

/// Whether `expr` holds an aggregate, a ranking or a result alias, which no
/// scalar of the outer row does.
fn contains_aggregate_or_search(expr: &IRExpr) -> bool {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            IRExpr::Aggregate { .. } | IRExpr::Nearest { .. } | IRExpr::AliasRef(_, _) => {
                return true;
            }
            IRExpr::Not(arg, _)
            | IRExpr::IsNull { expr: arg, .. }
            | IRExpr::Neg { expr: arg, .. }
            | IRExpr::Cast { expr: arg, .. } => pending.push(arg),
            IRExpr::Binary { left, right, .. } => pending.extend([left.as_ref(), right.as_ref()]),
            IRExpr::Call { args, .. } => pending.extend(args),
            IRExpr::PropAccess { .. }
            | IRExpr::Variable(_, _)
            | IRExpr::Param(_, _)
            | IRExpr::Literal(_, _) => {}
        }
    }
    false
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
        // The bound is any scalar of the enclosing row: a constant, or an
        // expression over the outer row's columns evaluated per outer row.
        self.right.check_types()?;
        if contains_aggregate_or_search(&self.right) {
            return Err(CompilerError::Plan(
                "block comparison bound must be a scalar of the outer row".into(),
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

/// The largest `rrf` weight: with `k >= 1` and at most sixteen arms the
/// fused sum stays finite.
pub const RRF_MAX_WEIGHT: f64 = 1_000_000.0;

/// The stored `weights` of an `rrf` over `arms` arms: `None` when every
/// weight is `1.0` (the unwritten default, so an unweighted fusion keeps one
/// spelling), otherwise the `[F64]` literal of the weights in arm order.
pub fn rrf_weights_expr(weights: &[f64]) -> Option<IRExpr> {
    if weights
        .iter()
        .all(|weight| weight.to_bits() == 1.0_f64.to_bits())
    {
        return None;
    }
    Some(IRExpr::Literal(
        Literal::List(
            weights
                .iter()
                .map(|weight| Literal::Float(*weight))
                .collect(),
        ),
        ExprType::Value {
            scalar: ScalarType::F64,
            list: true,
            nullable: false,
        },
    ))
}

/// The weights a stored `rrf` `weights` literal carries, when it is the shape
/// [`rrf_weights_expr`] builds for `arms` arms with every weight in
/// `(0, RRF_MAX_WEIGHT]`; `None` for anything else.
pub fn rrf_weight_values(weights: &IRExpr, arms: usize) -> Option<Vec<f64>> {
    let IRExpr::Literal(
        Literal::List(items),
        ExprType::Value {
            scalar: ScalarType::F64,
            list: true,
            nullable: false,
        },
    ) = weights
    else {
        return None;
    };
    if items.len() != arms {
        return None;
    }
    items
        .iter()
        .map(|item| match item {
            Literal::Float(weight) if *weight > 0.0 && *weight <= RRF_MAX_WEIGHT => Some(*weight),
            _ => None,
        })
        .collect()
}

impl IRExpr {
    /// The searched field of a full-text predicate, `search`, `fuzzy`,
    /// `match_text` or `match_terms`: a Boolean that is true where the row's
    /// field matches the call's query. `None` for any other node.
    pub fn lexical_predicate_field(&self) -> Option<&IRExpr> {
        match self {
            Self::Call {
                func: ScalarFunc::MatchTerms { .. },
                args,
                ..
            } => args.first(),
            _ => None,
        }
    }

    /// The compiler-owned result type of every expression node.
    pub fn ty(&self) -> &ExprType {
        match self {
            Self::PropAccess { ty, .. }
            | Self::Nearest { ty, .. }
            | Self::Binary { ty, .. }
            | Self::IsNull { ty, .. }
            | Self::Cast { ty, .. }
            | Self::Variable(_, ty)
            | Self::Param(_, ty)
            | Self::Literal(_, ty)
            | Self::AliasRef(_, ty)
            | Self::Not(_, ty)
            | Self::Neg { ty, .. }
            | Self::Call { ty, .. } => ty,
            Self::Aggregate { signature, .. } => &signature.result,
        }
    }

    /// Boolean builders preserve stored child nullability without choosing an
    /// operand conversion. Compiler lowering owns coercion before using them.
    /// Arithmetic takes its left operand's type, nullable when either is.
    pub fn binary(left: IRExpr, op: BinaryOp, right: IRExpr) -> Self {
        let nullable = left.ty().nullable() || right.ty().nullable();
        let ty = match (op, left.ty()) {
            (BinaryOp::Arith(_), ExprType::Value { scalar, .. }) => scalar_type(*scalar, nullable),
            _ => boolean_type(nullable),
        };
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

    /// The node with each direct child replaced by `f` of it, every other
    /// field kept: the one walk a rewrite of some nodes falls back on, so no
    /// variant is left unwalked.
    pub fn try_map_children<E>(
        &self,
        mut f: impl FnMut(&Self) -> std::result::Result<Self, E>,
    ) -> std::result::Result<Self, E> {
        crate::stack::deep(|| {
            let mut b = |x: &Self| f(x).map(Box::new);
            Ok(match self {
                Self::PropAccess { .. }
                | Self::Variable(_, _)
                | Self::Param(_, _)
                | Self::Literal(_, _)
                | Self::AliasRef(_, _) => self.clone(),
                Self::Nearest {
                    variable,
                    property,
                    query,
                    ty,
                } => Self::Nearest {
                    variable: variable.clone(),
                    property: property.clone(),
                    query: b(query)?,
                    ty: ty.clone(),
                },
                Self::Aggregate {
                    func,
                    distinct,
                    arg,
                    fraction,
                    signature,
                } => Self::Aggregate {
                    func: *func,
                    distinct: *distinct,
                    arg: b(arg)?,
                    fraction: match fraction {
                        Some(p) => Some(b(p)?),
                        None => None,
                    },
                    signature: signature.clone(),
                },
                Self::Binary {
                    left,
                    op,
                    right,
                    ty,
                } => Self::Binary {
                    left: b(left)?,
                    op: *op,
                    right: b(right)?,
                    ty: ty.clone(),
                },
                Self::Not(inner, ty) => Self::Not(b(inner)?, ty.clone()),
                Self::Cast { expr, ty } => Self::Cast {
                    expr: b(expr)?,
                    ty: ty.clone(),
                },
                Self::IsNull { expr, negated, ty } => Self::IsNull {
                    expr: b(expr)?,
                    negated: *negated,
                    ty: ty.clone(),
                },
                Self::Neg { expr, ty } => Self::Neg {
                    expr: b(expr)?,
                    ty: ty.clone(),
                },
                Self::Call { func, args, ty } => Self::Call {
                    func: *func,
                    args: args
                        .iter()
                        .map(&mut f)
                        .collect::<std::result::Result<_, E>>()?,
                    ty: ty.clone(),
                },
            })
        })
    }

    /// The direct children, in field order.
    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::PropAccess { .. }
            | Self::Variable(_, _)
            | Self::Param(_, _)
            | Self::Literal(_, _)
            | Self::AliasRef(_, _) => Vec::new(),
            Self::Nearest { query, .. } => vec![query],
            Self::Aggregate { arg, fraction, .. } => std::iter::once(arg)
                .chain(fraction.as_ref())
                .map(|b| &**b)
                .collect(),
            Self::Not(arg, _)
            | Self::IsNull { expr: arg, .. }
            | Self::Neg { expr: arg, .. }
            | Self::Cast { expr: arg, .. } => vec![arg],
            Self::Binary { left, right, .. } => vec![left, right],
            Self::Call { args, .. } => args.iter().collect(),
        }
    }

    /// The node with every child replaced by one placeholder: two nodes
    /// with equal heads differ at most in their children.
    pub fn head(&self) -> Self {
        let placeholder = Self::Literal(Literal::Null, boolean_type(true));
        self.try_map_children(|_| Ok::<_, std::convert::Infallible>(placeholder.clone()))
            .unwrap_or_else(|never| match never {})
    }

    /// Visit each typed leaf without recursing on a user-sized expression tree.
    pub fn visit_leaves(&self, mut visit: impl FnMut(&Self)) {
        self.visit_nodes(|expr| {
            if matches!(
                expr,
                Self::PropAccess { .. }
                    | Self::Variable(_, _)
                    | Self::Param(_, _)
                    | Self::Literal(_, _)
                    | Self::AliasRef(_, _)
            ) {
                visit(expr)
            }
        })
    }

    /// Visit every node, each before its children, without recursing on a
    /// user-sized expression tree.
    /// The expression's nodes: the plan budget's measure of it.
    pub fn nodes(&self) -> usize {
        let mut n = 0;
        self.visit_nodes(|_| n += 1);
        n
    }

    pub fn visit_nodes(&self, mut visit: impl FnMut(&Self)) {
        let mut pending = vec![self];
        while let Some(expr) = pending.pop() {
            visit(expr);
            match expr {
                Self::PropAccess { .. }
                | Self::Variable(_, _)
                | Self::Param(_, _)
                | Self::Literal(_, _)
                | Self::AliasRef(_, _) => {}
                Self::Nearest { query, .. } => pending.push(query),
                Self::Aggregate { arg, fraction, .. } => {
                    pending.push(arg);
                    pending.extend(fraction.as_deref());
                }
                Self::Not(arg, _)
                | Self::IsNull { expr: arg, .. }
                | Self::Neg { expr: arg, .. }
                | Self::Cast { expr: arg, .. } => pending.push(arg),
                Self::Binary { left, right, .. } => pending.extend([left.as_ref(), right.as_ref()]),
                Self::Call { args, .. } => pending.extend(args),
            }
        }
    }

    /// Whether evaluating the expression can refuse a row (arithmetic: an
    /// integer overflow, a zero divisor, a non-finite float). The planner
    /// keeps such a term behind its guards (A3).
    pub fn can_fail(&self) -> bool {
        let mut pending = vec![self];
        while let Some(expr) = pending.pop() {
            match expr {
                // A negated literal fails only where its negation leaves its
                // type (`-` of the smallest integer); its value is known.
                Self::Neg { expr: inner, ty } => match inner.as_ref() {
                    Self::Literal(Literal::Float(_), _) => {}
                    Self::Literal(Literal::Integer(n), _) if negation_fits(*n, ty) => {}
                    _ => return true,
                },
                Self::Binary {
                    op: BinaryOp::Arith(_),
                    ..
                } => return true,
                Self::Call { func, args, ty }
                    if func.row_can_fail(
                        &args.iter().map(|a| a.ty().clone()).collect::<Vec<_>>(),
                        ty,
                    ) =>
                {
                    return true;
                }
                Self::PropAccess { .. }
                | Self::Variable(_, _)
                | Self::Param(_, _)
                | Self::Literal(_, _)
                | Self::AliasRef(_, _) => {}
                Self::Nearest { query, .. } => pending.push(query),
                Self::Aggregate { arg, fraction, .. } => {
                    pending.push(arg);
                    pending.extend(fraction.as_deref());
                }
                Self::Not(arg, _)
                | Self::IsNull { expr: arg, .. }
                | Self::Cast { expr: arg, .. } => pending.push(arg),
                Self::Binary { left, right, .. } => pending.extend([left.as_ref(), right.as_ref()]),
                Self::Call { args, .. } => pending.extend(args),
            }
        }
        false
    }

    /// The columns of the current row the expression reads, in first-read
    /// order: a reference or value column, or the column a property reads.
    pub fn columns(&self, out: &mut Vec<String>) {
        let mut add = |name: &String| {
            if !out.contains(name) {
                out.push(name.clone());
            }
        };
        let mut pending = vec![self];
        while let Some(expr) = pending.pop() {
            match expr {
                Self::Variable(name, _) | Self::AliasRef(name, _) => add(name),
                Self::PropAccess { variable, .. } => add(variable),
                Self::Nearest {
                    variable, query, ..
                } => {
                    add(variable);
                    pending.push(query);
                }
                Self::Param(_, _) | Self::Literal(_, _) => {}
                Self::Aggregate { arg, fraction, .. } => {
                    pending.extend(fraction.as_deref());
                    pending.push(arg);
                }
                Self::Not(arg, _)
                | Self::IsNull { expr: arg, .. }
                | Self::Neg { expr: arg, .. }
                | Self::Cast { expr: arg, .. } => pending.push(arg),
                Self::Binary { left, right, .. } => pending.extend([right.as_ref(), left.as_ref()]),
                Self::Call { args, .. } => pending.extend(args.iter().rev()),
            }
        }
    }

    /// Whether the expression reads no column of the row.
    pub fn columns_empty(&self) -> bool {
        let mut cols = Vec::new();
        self.columns(&mut cols);
        cols.is_empty()
    }

    /// Whether the expression is a bare read of the current row: a column or
    /// the property of one.
    pub fn is_column_read(&self) -> bool {
        matches!(
            self,
            Self::Variable(_, _) | Self::AliasRef(_, _) | Self::PropAccess { .. }
        )
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
            | Self::Aggregate { .. }
            | Self::Binary { .. }
            | Self::Not(_, _)
            | Self::IsNull { .. }
            | Self::Neg { .. }
            | Self::Call { .. }
            | Self::Cast { .. } => None,
        }
    }

    /// Validate each stored local type rule without consulting the catalog or
    /// bound parameter values. Operator return owners validate alias identity.
    pub fn check_types(&self) -> Result<()> {
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
                    distinct: _,
                    arg,
                    fraction,
                    signature,
                } => {
                    check_aggregate_types(*func, arg, signature)?;
                    check_aggregate_fraction(*func, fraction.as_deref())?;
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
                Self::Binary {
                    left,
                    op: BinaryOp::Arith(op),
                    right,
                    ty,
                } => {
                    let Some(scalar) = numeric_scalar(left.ty()) else {
                        return Err(bad("arithmetic operands must be numeric scalars"));
                    };
                    if !same_value_type(left.ty(), right.ty()) {
                        return Err(bad("arithmetic operands need explicit casts to one type"));
                    }
                    expect(
                        ty,
                        scalar_type(scalar, left.ty().nullable() || right.ty().nullable()),
                    )?;
                    let _ = op;
                    pending.extend([left.as_ref(), right.as_ref()]);
                }
                Self::Neg { expr: inner, ty } => {
                    let Some(scalar) = numeric_scalar(inner.ty()) else {
                        return Err(bad("negation needs a numeric scalar"));
                    };
                    if matches!(scalar, ScalarType::U32 | ScalarType::U64) {
                        return Err(bad("negation needs a signed integer or a float"));
                    }
                    expect(ty, inner.ty().clone())?;
                    pending.push(inner);
                }
                Self::Call { func, args, ty } => {
                    check_call(*func, args, ty)?;
                    pending.extend(args);
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
                        BinaryOp::Arith(_) => unreachable!("matched above"),
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
                    if !matches!(ty, ExprType::Node { .. } | ExprType::Edge { .. }) {
                        return Err(bad("a reference variable carries a value type"));
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
    pub fn into_conjuncts(mut self) -> Vec<IRExpr> {
        crate::stack::deep(|| match &mut self {
            IRExpr::Binary {
                left,
                op: BinaryOp::And,
                right,
                ty: _,
            } => {
                let mut conjuncts = Self::take(left).into_conjuncts();
                conjuncts.extend(Self::take(right).into_conjuncts());
                conjuncts
            }
            _ => vec![self],
        })
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
            IRExpr::Binary {
                op: BinaryOp::Arith(ArithOp::Add | ArithOp::Sub),
                ..
            } => Precedence::Sum,
            IRExpr::Binary {
                op: BinaryOp::Arith(ArithOp::Mul | ArithOp::Div | ArithOp::Rem),
                ..
            } => Precedence::Product,
            IRExpr::Neg { .. } => Precedence::Negation,
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
        crate::stack::deep(|| {
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
                IRExpr::Variable(name, _) => write!(f, "${name}"),
                IRExpr::Param(name, _) if name == NOW_PARAM_NAME => f.write_str("now()"),
                IRExpr::Param(name, _) => write!(f, "${name}"),
                IRExpr::Literal(literal, _) => write!(f, "{literal}"),
                IRExpr::Aggregate {
                    func,
                    distinct: true,
                    arg,
                    ..
                } => write!(f, "{func}(distinct {arg})"),
                IRExpr::Aggregate {
                    func,
                    distinct: false,
                    arg,
                    fraction: Some(p),
                    ..
                } => write!(f, "{func}({arg}, {p})"),
                IRExpr::Aggregate {
                    func,
                    distinct: false,
                    arg,
                    fraction: None,
                    ..
                } => write!(f, "{func}({arg})"),
                IRExpr::AliasRef(alias, _) => f.write_str(alias),
                IRExpr::Neg { expr, .. } => {
                    f.write_str("-")?;
                    // `--` would read as a comment, and `-` before a negative
                    // literal as one; both keep their parentheses.
                    let negative_literal = matches!(
                        expr.as_ref(),
                        IRExpr::Literal(Literal::Integer(n), _) if *n < 0
                    ) || matches!(
                        expr.as_ref(),
                        IRExpr::Literal(Literal::Float(x), _) if x.is_sign_negative()
                    );
                    if negative_literal || matches!(expr.as_ref(), IRExpr::Neg { .. }) {
                        write!(f, "({expr})")
                    } else {
                        expr.fmt_operand(f, Precedence::Negation, false)
                    }
                }
                IRExpr::Call {
                    func: ScalarFunc::MatchTerms { mode },
                    args,
                    ..
                } => {
                    let mode = match mode {
                        TermsMode::All => "all",
                        TermsMode::Any => "any",
                    };
                    match args.as_slice() {
                        [field, text, max_edits] => write!(
                            f,
                            "match_terms({field}, terms({text}, mode: {mode}, max_edits: {max_edits}))"
                        ),
                        _ => write!(f, "match_terms(…)"),
                    }
                }
                // A list prints as the list literal it was written as.
                IRExpr::Call {
                    func: ScalarFunc::List,
                    args,
                    ..
                } => {
                    f.write_str("[")?;
                    for (i, arg) in args.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{arg}")?;
                    }
                    f.write_str("]")
                }
                // `cast` names its target type, `round` its digits option.
                IRExpr::Call {
                    func: ScalarFunc::Cast,
                    args,
                    ty,
                } if args.len() == 1 => match ty {
                    ExprType::Value { scalar, .. } => {
                        write!(f, "cast({}, {scalar})", args[0])
                    }
                    _ => write!(f, "cast({}, …)", args[0]),
                },
                IRExpr::Call {
                    func: ScalarFunc::Round,
                    args,
                    ..
                } if args.len() == 2 => write!(f, "round({}, digits: {})", args[0], args[1]),
                IRExpr::Call { func, args, .. } => {
                    write!(f, "{}(", func.name())?;
                    for (i, arg) in args.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{arg}")?;
                    }
                    f.write_str(")")
                }
            }
        })
    }
}

#[cfg(test)]
mod render_tests {
    use super::*;
    use crate::types::PropType;

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
            Literal::Integer(i128::from(value)),
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
            fraction: None,
            func: AggFunc::Sum,
            distinct: false,
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
            fraction: None,
            func: AggFunc::Count,
            distinct: false,
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
        // Unit weights keep the unweighted spelling; others are stored as an
        // `[F64]` literal of one weight per arm, and a malformed list is no
        // list of weights.
        assert_eq!(rrf_weights_expr(&[1.0, 1.0, 1.0]), None);
        let weights = rrf_weights_expr(&[1.0, 5.0, 2.0]).unwrap();
        assert_eq!(weights.to_string(), "[1.0, 5.0, 2.0]");
        assert_eq!(rrf_weight_values(&weights, 3), Some(vec![1.0, 5.0, 2.0]));
        assert_eq!(rrf_weight_values(&weights, 2), None);
        for bad in [vec![1.0, 0.0], vec![1.0, RRF_MAX_WEIGHT * 2.0]] {
            let weights = rrf_weights_expr(&bad).unwrap();
            assert_eq!(rrf_weight_values(&weights, 2), None, "{bad:?}");
        }
        let count = IRExpr::Aggregate {
            fraction: None,
            func: AggFunc::Count,
            distinct: false,
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
    fn arithmetic_negation_and_registry_calls_print_type_and_can_fail() {
        use crate::query::syntax::ArithOp;
        let arith = |left, op, right| IRExpr::binary(left, BinaryOp::Arith(op), right);
        let sum = arith(prop("p", "a"), ArithOp::Add, int(1));
        assert_eq!(sum.ty(), int(1).ty());
        sum.check_types().unwrap();
        assert!(sum.can_fail());
        // `*` binds tighter than `+`; a right operand of equal precedence keeps
        // its parentheses, so `a - (b - c)` is not `a - b - c`.
        let nested = arith(
            arith(prop("p", "a"), ArithOp::Mul, prop("p", "b")),
            ArithOp::Sub,
            arith(prop("p", "b"), ArithOp::Sub, prop("p", "c")),
        );
        assert_eq!(nested.to_string(), "$p.a * $p.b - ($p.b - $p.c)");
        let product = arith(sum.clone(), ArithOp::Mul, int(2));
        assert_eq!(product.to_string(), "($p.a + 1) * 2");
        let compared = IRExpr::comparison(sum.clone(), CompOp::Gt, int(3));
        assert_eq!(compared.to_string(), "$p.a + 1 > 3");
        assert!(compared.can_fail());

        let neg = IRExpr::Neg {
            ty: sum.ty().clone(),
            expr: Box::new(sum.clone()),
        };
        neg.check_types().unwrap();
        assert_eq!(neg.to_string(), "-($p.a + 1)");
        let double = IRExpr::Neg {
            ty: neg.ty().clone(),
            expr: Box::new(neg.clone()),
        };
        assert_eq!(double.to_string(), "-(-($p.a + 1))");

        let flag = IRExpr::comparison(prop("p", "a"), CompOp::Gt, int(0));
        let call = IRExpr::Call {
            func: ScalarFunc::If,
            args: vec![flag.clone(), prop("p", "a"), int(0)],
            ty: int(0).ty().clone(),
        };
        call.check_types().unwrap();
        assert!(!call.can_fail());
        assert_eq!(call.to_string(), "if($p.a > 0, $p.a, 0)");

        // Stored types are checked locally: operands of two types, a negated
        // unsigned value and a call whose result type disagrees are refused.
        let wide = IRExpr::Literal(
            Literal::Integer(1),
            scalar_type(crate::types::ScalarType::I32, false),
        );
        assert!(
            arith(prop("p", "a"), ArithOp::Add, wide)
                .check_types()
                .is_err()
        );
        let unsigned = IRExpr::Literal(
            Literal::Integer(1),
            scalar_type(crate::types::ScalarType::U64, false),
        );
        let negated = IRExpr::Neg {
            ty: unsigned.ty().clone(),
            expr: Box::new(unsigned),
        };
        assert!(negated.check_types().is_err());
        let text = prop("p", "name");
        let forged = IRExpr::Call {
            func: ScalarFunc::Coalesce,
            args: vec![text.clone()],
            ty: int(0).ty().clone(),
        };
        assert!(forged.check_types().is_err());
        let joined = IRExpr::Call {
            func: ScalarFunc::Concat,
            args: vec![text.clone(), text],
            ty: scalar_type(crate::types::ScalarType::String, false),
        };
        joined.check_types().unwrap();
        assert_eq!(joined.to_string(), "concat($p.name, $p.name)");
    }

    /// The stored widening conversions arithmetic and the conditional rows
    /// carry are exactly the ones the checker emits: a narrower integer to
    /// the wider of its signedness, `U32` to `I64`, an integer or `F32` to
    /// `F64`, each keeping its operand's nullability. A narrowing conversion
    /// of a value, or one that changes nullability, is refused.
    #[test]
    fn arithmetic_widening_casts_are_the_permitted_conversions() {
        use crate::query::syntax::ArithOp;
        use crate::types::ScalarType::{self, F32, F64, I32, I64, U32, U64};
        let ty = |scalar, nullable| scalar_type(scalar, nullable);
        let value = |scalar: ScalarType, nullable| IRExpr::PropAccess {
            variable: "p".into(),
            property: "v".into(),
            ty: ty(scalar, nullable),
        };
        let cast = |from, to, nullable| IRExpr::Cast {
            expr: Box::new(value(from, nullable)),
            ty: ty(to, nullable),
        };
        let widenings = [
            (I32, I64),
            (U32, U64),
            (U32, I64),
            (I32, F64),
            (I64, F64),
            (U32, F64),
            (U64, F64),
            (F32, F64),
        ];
        for (from, to) in widenings {
            for nullable in [false, true] {
                cast(from, to, nullable).check_types().unwrap();
                let sum = IRExpr::Binary {
                    left: Box::new(cast(from, to, nullable)),
                    op: BinaryOp::Arith(ArithOp::Add),
                    right: Box::new(value(to, false)),
                    ty: ty(to, nullable),
                };
                sum.check_types().unwrap();
                let first = IRExpr::Call {
                    func: ScalarFunc::Coalesce,
                    args: vec![cast(from, to, nullable), value(to, false)],
                    ty: ty(to, false),
                };
                first.check_types().unwrap();
            }
            let dropped = IRExpr::Cast {
                expr: Box::new(value(from, true)),
                ty: ty(to, false),
            };
            assert!(dropped.check_types().is_err(), "{from:?} -> {to:?}");
        }
        for (from, to) in [
            (I64, I32),
            (U64, U32),
            (I32, U32),
            (I64, U64),
            (U64, I64),
            (I32, F32),
            (F64, F32),
            (F64, I64),
            (F32, I32),
        ] {
            assert!(
                cast(from, to, true).check_types().is_err(),
                "{from:?} -> {to:?} narrows"
            );
        }
        // A conditional row whose argument is not widened to the result type.
        let unwidened = IRExpr::Call {
            func: ScalarFunc::Coalesce,
            args: vec![value(I32, true), value(I64, false)],
            ty: ty(I64, false),
        };
        assert!(unwidened.check_types().is_err());
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

/// `percentile` holds its fraction, a numeric literal or a numeric
/// parameter; no other aggregate holds one.
fn check_aggregate_fraction(func: AggFunc, fraction: Option<&IRExpr>) -> Result<()> {
    let numeric = |ty: &ExprType| matches!(ty, ExprType::Value { scalar, list: false, .. } if scalar.is_numeric());
    let valid = match (func.takes_fraction(), fraction) {
        (false, None) => true,
        (false, Some(_)) | (true, None) => false,
        // A literal's value is checked where it is written (and, bound to a
        // called read's parameter, where the engine reads it, as a
        // parameter's is).
        (true, Some(IRExpr::Literal(Literal::Float(_) | Literal::Integer(_), ty)))
        | (true, Some(IRExpr::Param(_, ty))) => numeric(ty),
        (true, Some(_)) => false,
    };
    if valid {
        Ok(())
    } else {
        Err(CompilerError::Plan(format!(
            "{func} holds an invalid fraction"
        )))
    }
}

fn check_aggregate_types(func: AggFunc, arg: &IRExpr, signature: &AggSignature) -> Result<()> {
    let expected = func.result_type(&signature.arg).ok_or_else(|| {
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
        ExprType::Node { type_name } | ExprType::Edge { type_name } if type_name.is_empty() => Err(
            CompilerError::Plan("reference leaf has an empty type name".into()),
        ),
        _ => Ok(()),
    }
}

/// The scalar of a numeric, non-list value type.
fn numeric_scalar(ty: &ExprType) -> Option<crate::types::ScalarType> {
    match ty {
        ExprType::Value {
            scalar,
            list: false,
            ..
        } if scalar.is_numeric() => Some(*scalar),
        _ => None,
    }
}

/// Two value types equal but for nullability.
fn same_value_type(left: &ExprType, right: &ExprType) -> bool {
    match (left, right) {
        (
            ExprType::Value {
                scalar: a,
                list: la,
                ..
            },
            ExprType::Value {
                scalar: b,
                list: lb,
                ..
            },
        ) => a == b && la == lb,
        _ => false,
    }
}

/// Whether `t` is a scalar number.
fn numeric_value(t: &ExprType) -> bool {
    matches!(t, ExprType::Value { scalar, list: false, .. } if scalar.is_numeric())
}

/// A registry call's stored types: the arity and argument types its row
/// declares, and the result type it derives from them.
fn check_call(func: ScalarFunc, args: &[IRExpr], ty: &ExprType) -> Result<()> {
    use crate::types::ScalarType;
    let bad = |detail: &str| Err(CompilerError::Plan(format!("{}: {detail}", func.name())));
    let string = |e: &IRExpr| scalar_is(e.ty(), ScalarType::String);
    match func {
        ScalarFunc::If => {
            let [cond, then, otherwise] = args else {
                return bad("takes a condition and two values");
            };
            if !scalar_is(cond.ty(), ScalarType::Bool) {
                return bad("the condition is a scalar Bool");
            }
            if !same_value_type(then.ty(), ty) || !same_value_type(otherwise.ty(), ty) {
                return bad("both values have the result type");
            }
            if ty.nullable() != (then.ty().nullable() || otherwise.ty().nullable()) {
                return bad("the result is nullable when either value is");
            }
        }
        ScalarFunc::Coalesce => {
            if args.is_empty() || !args.iter().all(|a| same_value_type(a.ty(), ty)) {
                return bad("every argument has the result type");
            }
            if ty.nullable() != args.iter().all(|a| a.ty().nullable()) {
                return bad("the result is nullable only when every argument is");
            }
        }
        ScalarFunc::Concat => {
            if args.is_empty() || !args.iter().all(string) {
                return bad("every argument is a String");
            }
            if ty != &scalar_type(ScalarType::String, args.iter().any(|a| a.ty().nullable())) {
                return bad("the result is a String, nullable when any argument is");
            }
        }
        ScalarFunc::List => {
            let ExprType::Value {
                scalar,
                list: true,
                nullable: false,
            } = ty
            else {
                return bad("the result is a non-null list");
            };
            if args.is_empty()
                || !args
                    .iter()
                    .all(|a| scalar_is(a.ty(), *scalar) && !a.ty().nullable())
            {
                return bad("every element is a non-null value of the list's element type");
            }
        }
        ScalarFunc::Lower
        | ScalarFunc::Upper
        | ScalarFunc::Trim
        | ScalarFunc::LTrim
        | ScalarFunc::RTrim => {
            let [x] = args else {
                return bad("takes one String");
            };
            if !string(x) || ty != x.ty() {
                return bad("takes a String and returns its type");
            }
        }
        ScalarFunc::Length => {
            let [x] = args else {
                return bad("takes one String");
            };
            if !string(x) || ty != &scalar_type(ScalarType::I64, x.ty().nullable()) {
                return bad("counts a String's characters as an I64");
            }
        }
        ScalarFunc::Left | ScalarFunc::Right | ScalarFunc::Substring => {
            let arity = if func == ScalarFunc::Substring { 3 } else { 2 };
            let int64 = |e: &IRExpr| scalar_is(e.ty(), ScalarType::I64);
            if args.len() != arity || !string(&args[0]) || !args[1..].iter().all(int64) {
                return bad("takes a String and I64 positions");
            }
            let nullable = args.iter().any(|a| a.ty().nullable());
            if ty != &scalar_type(ScalarType::String, nullable) {
                return bad("returns a String, nullable when any argument is");
            }
        }
        ScalarFunc::Abs | ScalarFunc::Floor | ScalarFunc::Ceil => {
            let [x] = args else {
                return bad("takes one number");
            };
            if !numeric_value(x.ty()) || ty != x.ty() {
                return bad("takes a number and returns its type");
            }
        }
        ScalarFunc::Round => {
            let [x, digits] = args else {
                return bad("takes a number and its digits");
            };
            if !numeric_value(x.ty())
                || !scalar_is(digits.ty(), ScalarType::I64)
                || !same_value_type(x.ty(), ty)
                || ty.nullable() != (x.ty().nullable() || digits.ty().nullable())
            {
                return bad("takes a number and I64 digits and returns the number's type");
            }
        }
        ScalarFunc::Sqrt
        | ScalarFunc::Exp
        | ScalarFunc::Ln
        | ScalarFunc::Log10
        | ScalarFunc::Power => {
            let arity = if func == ScalarFunc::Power { 2 } else { 1 };
            let f64s = |e: &IRExpr| scalar_is(e.ty(), ScalarType::F64);
            if args.len() != arity || !args.iter().all(f64s) {
                return bad("takes F64 arguments");
            }
            let nullable = args.iter().any(|a| a.ty().nullable());
            if ty != &scalar_type(ScalarType::F64, nullable) {
                return bad("returns an F64, nullable when any argument is");
            }
        }
        ScalarFunc::Size => {
            let [x] = args else {
                return bad("takes one list");
            };
            if !matches!(x.ty(), ExprType::Value { list: true, .. })
                || ty != &scalar_type(ScalarType::I64, x.ty().nullable())
            {
                return bad("counts a list's elements as an I64");
            }
        }
        ScalarFunc::Cast => {
            let [x] = args else {
                return bad("takes one value");
            };
            let (
                ExprType::Value {
                    scalar: from,
                    list: false,
                    nullable,
                },
                ExprType::Value {
                    scalar: to,
                    list: false,
                    nullable: result_nullable,
                },
            ) = (x.ty(), ty)
            else {
                return bad("converts a scalar to a scalar");
            };
            if !cast_converts(*from, *to) || nullable != result_nullable {
                return bad("converts between the scalar types it admits");
            }
        }
        ScalarFunc::MatchTerms { .. } => {
            let [field, text, max_edits] = args else {
                return bad("takes a field, a text and an edit budget");
            };
            if !string(field) || !string(text) || !integer_type(max_edits.ty()) {
                return bad("the field and text are Strings and the budget an integer");
            }
            if ty != &boolean_type(false) {
                return bad("the result is a non-null Bool");
            }
        }
    }
    Ok(())
}

/// The bound expression: one type for filters, projections, order keys,
/// assignments and mutation predicates. Equal when the trees are equal node
/// by node (`Literal::Float` by bits), the equality the planner's filter
/// deduplication and the `Predicate` hash rely on.
///
/// `Clone`, `PartialEq`, `Hash` and `Debug` are written out rather than
/// derived only so that each enters a level through [`crate::stack::deep`]:
/// they mean what the derived ones would.
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
    Variable(String, ExprType),
    Param(String, ExprType),
    Literal(Literal, ExprType),
    /// An aggregate over a group's rows. `fraction` is `percentile`'s `p`
    /// (a numeric literal in [0, 1] or a declared numeric parameter, which
    /// changes the result, so it is the aggregate's own child); `None` for
    /// every other aggregate.
    Aggregate {
        func: AggFunc,
        distinct: bool,
        arg: Box<IRExpr>,
        fraction: Option<Box<IRExpr>>,
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
    /// Unary `-` over a signed integer or a float, of the operand's type.
    Neg {
        expr: Box<IRExpr>,
        ty: ExprType,
    },
    /// A scalar registry row over typed arguments, in the row's order.
    Call {
        func: ScalarFunc,
        args: Vec<IRExpr>,
        ty: ExprType,
    },
}

impl Clone for IRExpr {
    fn clone(&self) -> Self {
        crate::stack::deep(|| match self {
            Self::PropAccess {
                variable,
                property,
                ty,
            } => Self::PropAccess {
                variable: variable.clone(),
                property: property.clone(),
                ty: ty.clone(),
            },
            Self::Nearest {
                variable,
                property,
                query,
                ty,
            } => Self::Nearest {
                variable: variable.clone(),
                property: property.clone(),
                query: query.clone(),
                ty: ty.clone(),
            },
            Self::Variable(name, ty) => Self::Variable(name.clone(), ty.clone()),
            Self::Param(name, ty) => Self::Param(name.clone(), ty.clone()),
            Self::Literal(value, ty) => Self::Literal(value.clone(), ty.clone()),
            Self::Aggregate {
                func,
                distinct,
                arg,
                fraction,
                signature,
            } => Self::Aggregate {
                func: *func,
                distinct: *distinct,
                arg: arg.clone(),
                fraction: fraction.clone(),
                signature: signature.clone(),
            },
            Self::AliasRef(name, ty) => Self::AliasRef(name.clone(), ty.clone()),
            Self::Binary {
                left,
                op,
                right,
                ty,
            } => Self::Binary {
                left: left.clone(),
                op: *op,
                right: right.clone(),
                ty: ty.clone(),
            },
            Self::Not(inner, ty) => Self::Not(inner.clone(), ty.clone()),
            Self::Cast { expr, ty } => Self::Cast {
                expr: expr.clone(),
                ty: ty.clone(),
            },
            Self::IsNull { expr, negated, ty } => Self::IsNull {
                expr: expr.clone(),
                negated: *negated,
                ty: ty.clone(),
            },
            Self::Neg { expr, ty } => Self::Neg {
                expr: expr.clone(),
                ty: ty.clone(),
            },
            Self::Call { func, args, ty } => Self::Call {
                func: *func,
                args: args.clone(),
                ty: ty.clone(),
            },
        })
    }
}

impl PartialEq for IRExpr {
    fn eq(&self, other: &Self) -> bool {
        crate::stack::deep(|| match (self, other) {
            (
                Self::PropAccess {
                    variable: v1,
                    property: p1,
                    ty: t1,
                },
                Self::PropAccess {
                    variable: v2,
                    property: p2,
                    ty: t2,
                },
            ) => v1 == v2 && p1 == p2 && t1 == t2,
            (
                Self::Nearest {
                    variable: v1,
                    property: p1,
                    query: q1,
                    ty: t1,
                },
                Self::Nearest {
                    variable: v2,
                    property: p2,
                    query: q2,
                    ty: t2,
                },
            ) => v1 == v2 && p1 == p2 && q1 == q2 && t1 == t2,
            (Self::Variable(n1, t1), Self::Variable(n2, t2))
            | (Self::Param(n1, t1), Self::Param(n2, t2))
            | (Self::AliasRef(n1, t1), Self::AliasRef(n2, t2)) => n1 == n2 && t1 == t2,
            (Self::Literal(l1, t1), Self::Literal(l2, t2)) => l1 == l2 && t1 == t2,
            (
                Self::Aggregate {
                    func: f1,
                    distinct: d1,
                    arg: a1,
                    fraction: p1,
                    signature: s1,
                },
                Self::Aggregate {
                    func: f2,
                    distinct: d2,
                    arg: a2,
                    fraction: p2,
                    signature: s2,
                },
            ) => f1 == f2 && d1 == d2 && a1 == a2 && p1 == p2 && s1 == s2,
            (
                Self::Binary {
                    left: l1,
                    op: o1,
                    right: r1,
                    ty: t1,
                },
                Self::Binary {
                    left: l2,
                    op: o2,
                    right: r2,
                    ty: t2,
                },
            ) => l1 == l2 && o1 == o2 && r1 == r2 && t1 == t2,
            (Self::Not(e1, t1), Self::Not(e2, t2)) => e1 == e2 && t1 == t2,
            (Self::Cast { expr: e1, ty: t1 }, Self::Cast { expr: e2, ty: t2 })
            | (Self::Neg { expr: e1, ty: t1 }, Self::Neg { expr: e2, ty: t2 }) => {
                e1 == e2 && t1 == t2
            }
            (
                Self::IsNull {
                    expr: e1,
                    negated: n1,
                    ty: t1,
                },
                Self::IsNull {
                    expr: e2,
                    negated: n2,
                    ty: t2,
                },
            ) => e1 == e2 && n1 == n2 && t1 == t2,
            (
                Self::Call {
                    func: f1,
                    args: a1,
                    ty: t1,
                },
                Self::Call {
                    func: f2,
                    args: a2,
                    ty: t2,
                },
            ) => f1 == f2 && a1 == a2 && t1 == t2,
            _ => false,
        })
    }
}

impl Eq for IRExpr {}

impl std::hash::Hash for IRExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        crate::stack::deep(|| {
            std::mem::discriminant(self).hash(state);
            match self {
                Self::PropAccess {
                    variable,
                    property,
                    ty,
                } => {
                    variable.hash(state);
                    property.hash(state);
                    ty.hash(state);
                }
                Self::Nearest {
                    variable,
                    property,
                    query,
                    ty,
                } => {
                    variable.hash(state);
                    property.hash(state);
                    query.hash(state);
                    ty.hash(state);
                }
                Self::Variable(name, ty) | Self::Param(name, ty) | Self::AliasRef(name, ty) => {
                    name.hash(state);
                    ty.hash(state);
                }
                Self::Literal(value, ty) => {
                    value.hash(state);
                    ty.hash(state);
                }
                Self::Aggregate {
                    func,
                    distinct,
                    arg,
                    fraction,
                    signature,
                } => {
                    func.hash(state);
                    distinct.hash(state);
                    arg.hash(state);
                    fraction.hash(state);
                    signature.hash(state);
                }
                Self::Binary {
                    left,
                    op,
                    right,
                    ty,
                } => {
                    left.hash(state);
                    op.hash(state);
                    right.hash(state);
                    ty.hash(state);
                }
                Self::Not(inner, ty) => {
                    inner.hash(state);
                    ty.hash(state);
                }
                Self::Cast { expr, ty } | Self::Neg { expr, ty } => {
                    expr.hash(state);
                    ty.hash(state);
                }
                Self::IsNull { expr, negated, ty } => {
                    expr.hash(state);
                    negated.hash(state);
                    ty.hash(state);
                }
                Self::Call { func, args, ty } => {
                    func.hash(state);
                    args.hash(state);
                    ty.hash(state);
                }
            }
        })
    }
}

impl std::fmt::Debug for IRExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::stack::deep(|| match self {
            Self::PropAccess {
                variable,
                property,
                ty,
            } => f
                .debug_struct("PropAccess")
                .field("variable", variable)
                .field("property", property)
                .field("ty", ty)
                .finish(),
            Self::Nearest {
                variable,
                property,
                query,
                ty,
            } => f
                .debug_struct("Nearest")
                .field("variable", variable)
                .field("property", property)
                .field("query", query)
                .field("ty", ty)
                .finish(),
            Self::Variable(name, ty) => f.debug_tuple("Variable").field(name).field(ty).finish(),
            Self::Param(name, ty) => f.debug_tuple("Param").field(name).field(ty).finish(),
            Self::Literal(value, ty) => f.debug_tuple("Literal").field(value).field(ty).finish(),
            Self::Aggregate {
                func,
                distinct,
                arg,
                fraction,
                signature,
            } => f
                .debug_struct("Aggregate")
                .field("func", func)
                .field("distinct", distinct)
                .field("arg", arg)
                .field("fraction", fraction)
                .field("signature", signature)
                .finish(),
            Self::AliasRef(name, ty) => f.debug_tuple("AliasRef").field(name).field(ty).finish(),
            Self::Binary {
                left,
                op,
                right,
                ty,
            } => f
                .debug_struct("Binary")
                .field("left", left)
                .field("op", op)
                .field("right", right)
                .field("ty", ty)
                .finish(),
            Self::Not(inner, ty) => f.debug_tuple("Not").field(inner).field(ty).finish(),
            Self::Cast { expr, ty } => f
                .debug_struct("Cast")
                .field("expr", expr)
                .field("ty", ty)
                .finish(),
            Self::IsNull { expr, negated, ty } => f
                .debug_struct("IsNull")
                .field("expr", expr)
                .field("negated", negated)
                .field("ty", ty)
                .finish(),
            Self::Neg { expr, ty } => f
                .debug_struct("Neg")
                .field("expr", expr)
                .field("ty", ty)
                .finish(),
            Self::Call { func, args, ty } => f
                .debug_struct("Call")
                .field("func", func)
                .field("args", args)
                .field("ty", ty)
                .finish(),
        })
    }
}

/// The scalar registry rows an expression calls (the GQ 3 RFC,
/// section 2). A comparison row (`contains`, `starts_with`), a search
/// predicate and `now()` keep their own nodes; these are the rows with no
/// other spelling.
/// Whether `-n` is a value of `ty`'s integer type: a negated literal that
/// cannot overflow.
pub fn negation_fits(n: i128, ty: &ExprType) -> bool {
    let Some(negated) = n.checked_neg() else {
        return false;
    };
    match ty {
        ExprType::Value { scalar, .. } => match scalar {
            ScalarType::I32 => i32::try_from(negated).is_ok(),
            ScalarType::I64 => i64::try_from(negated).is_ok(),
            ScalarType::F32 | ScalarType::F64 => true,
            _ => false,
        },
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarFunc {
    /// `if(c, a, b)`: `a` where `c` is true, else `b` (`b` for a null `c`).
    If,
    /// `coalesce(a, …)`: the first non-null argument, else null.
    Coalesce,
    /// `concat(a, …)`: the Strings joined; null when any argument is null.
    Concat,
    /// `match_terms(field, terms(text, mode:, max_edits:))`: arguments are the
    /// field, the text and the edit budget.
    MatchTerms {
        mode: TermsMode,
    },
    /// A list literal holding a parameter, `[$a, 2]`: its elements, each a
    /// literal or a declared non-null parameter of the list's element type,
    /// in order. The list is built when the parameters are bound.
    List,
    /// `lower(s)`, `upper(s)`: Unicode case mapping.
    Lower,
    Upper,
    /// `trim(s)`, `ltrim(s)`, `rtrim(s)`: Unicode whitespace removed from
    /// both ends, the start or the end.
    Trim,
    LTrim,
    RTrim,
    /// `length(s)`: the Unicode scalar values of a String, an `I64`.
    Length,
    /// `left(s, n)`, `right(s, n)`: the first or last `n` characters; a
    /// negative `n` is a typed error (`E5`).
    Left,
    Right,
    /// `substring(s, start, len)`: the characters from the 1-based `start`
    /// for `len`, clipped to the String; a negative `len` is `E5`.
    Substring,
    /// `abs(x)`, `floor(x)`, `ceil(x)`: the operand's type; `abs` of the
    /// smallest signed integer overflows (`E1`).
    Abs,
    Floor,
    Ceil,
    /// `round(x, digits: n)`: half away from zero at `n` decimal digits, the
    /// operand's type; its arguments are `x` and `n`. An integer that
    /// overflows is `E1`, a float result that is not finite `E3`.
    Round,
    /// `sqrt`, `exp`, `ln`, `log10` and `power` over `F64`: the root of a
    /// negative and the logarithm of a non-positive are `E5`, a result that
    /// is not finite `E3`.
    Sqrt,
    Exp,
    Ln,
    Log10,
    Power,
    /// `size(l)`: a list's elements, an `I64`.
    Size,
    /// `cast(x, T)`: `x` converted to the call's scalar type; a value that
    /// does not convert is `E6`.
    Cast,
}

/// The scalar type a `cast`'s type name names.
pub fn cast_target(name: &str) -> Option<ScalarType> {
    Some(match name {
        "String" => ScalarType::String,
        "Bool" => ScalarType::Bool,
        "I32" => ScalarType::I32,
        "I64" => ScalarType::I64,
        "U32" => ScalarType::U32,
        "U64" => ScalarType::U64,
        "F32" => ScalarType::F32,
        "F64" => ScalarType::F64,
        "Date" => ScalarType::Date,
        "DateTime" => ScalarType::DateTime,
        _ => return None,
    })
}

fn is_number(s: ScalarType) -> bool {
    s.is_numeric()
}

fn is_int(s: ScalarType) -> bool {
    matches!(
        s,
        ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64
    )
}

/// Whether `cast` converts a `from` value to `to`: between numbers, a
/// Bool and an integer, every scalar to and from String, and `Date` and
/// `DateTime` either way.
pub fn cast_converts(from: ScalarType, to: ScalarType) -> bool {
    use ScalarType::*;
    match (from, to) {
        (Vector(_) | Blob, _) | (_, Vector(_) | Blob) => false,
        (a, b) if a == b => true,
        (a, b) if is_number(a) && is_number(b) => true,
        (Bool, b) | (b, Bool) if is_int(b) => true,
        (String, _) | (_, String) => true,
        (Date, DateTime) | (DateTime, Date) => true,
        _ => false,
    }
}

/// Whether a `cast` from `from` to `to` can refuse a value (`E6`): every
/// conversion but a widening that holds every value, a conversion to
/// String, an integer to Bool, a Bool to an integer, and between `Date` and
/// `DateTime`. A String read as a Bool is refused unless it is `true` or
/// `false`.
pub fn cast_can_fail(from: ScalarType, to: ScalarType) -> bool {
    use ScalarType::*;
    if from == to || to == String || from == Bool {
        return false;
    }
    !matches!(
        (from, to),
        (Date, DateTime)
            | (DateTime, Date)
            | (I32 | U32 | I64 | U64, Bool)
            | (I32, I64 | F64)
            | (U32, I64 | U64 | F64)
            | (U64, F64)
            | (I64, F64)
            | (I32 | U32 | I64 | U64, F32)
            | (F32, F64)
    )
}

/// How a `terms(…)` value matches: every term, or any one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TermsMode {
    All,
    Any,
}

/// Every scalar row with no other node, in registry order, by its written
/// name; `match_terms` carries its mode and is built by the checker.
const NAMED_ROWS: &[ScalarFunc] = &[
    ScalarFunc::If,
    ScalarFunc::Coalesce,
    ScalarFunc::Concat,
    ScalarFunc::List,
    ScalarFunc::Lower,
    ScalarFunc::Upper,
    ScalarFunc::Trim,
    ScalarFunc::LTrim,
    ScalarFunc::RTrim,
    ScalarFunc::Length,
    ScalarFunc::Left,
    ScalarFunc::Right,
    ScalarFunc::Substring,
    ScalarFunc::Abs,
    ScalarFunc::Floor,
    ScalarFunc::Ceil,
    ScalarFunc::Round,
    ScalarFunc::Sqrt,
    ScalarFunc::Exp,
    ScalarFunc::Ln,
    ScalarFunc::Log10,
    ScalarFunc::Power,
    ScalarFunc::Size,
    ScalarFunc::Cast,
];

impl ScalarFunc {
    /// The registry row's name, as written.
    pub fn name(self) -> &'static str {
        match self {
            Self::If => "if",
            Self::Coalesce => "coalesce",
            Self::Concat => "concat",
            Self::MatchTerms { .. } => "match_terms",
            Self::List => "list",
            Self::Lower => "lower",
            Self::Upper => "upper",
            Self::Trim => "trim",
            Self::LTrim => "ltrim",
            Self::RTrim => "rtrim",
            Self::Length => "length",
            Self::Left => "left",
            Self::Right => "right",
            Self::Substring => "substring",
            Self::Abs => "abs",
            Self::Floor => "floor",
            Self::Ceil => "ceil",
            Self::Round => "round",
            Self::Sqrt => "sqrt",
            Self::Exp => "exp",
            Self::Ln => "ln",
            Self::Log10 => "log10",
            Self::Power => "power",
            Self::Size => "size",
            Self::Cast => "cast",
        }
    }

    /// The row a written name names, `match_terms` aside.
    pub fn from_name(name: &str) -> Option<Self> {
        NAMED_ROWS.iter().copied().find(|f| f.name() == name)
    }

    /// Whether the row itself can refuse a row of these argument types and
    /// this result type (the registry's `can_fail`), apart from what its
    /// arguments can: the planner keeps such a call behind its guards (A3).
    pub fn row_can_fail(self, args: &[ExprType], ty: &ExprType) -> bool {
        let scalar = |t: &ExprType| match t {
            ExprType::Value { scalar, .. } => Some(*scalar),
            _ => None,
        };
        match self {
            Self::If
            | Self::Coalesce
            | Self::Concat
            | Self::MatchTerms { .. }
            | Self::List
            | Self::Lower
            | Self::Upper
            | Self::Trim
            | Self::LTrim
            | Self::RTrim
            | Self::Length
            | Self::Floor
            | Self::Ceil
            | Self::Size => false,
            Self::Left
            | Self::Right
            | Self::Substring
            | Self::Round
            | Self::Sqrt
            | Self::Exp
            | Self::Ln
            | Self::Log10
            | Self::Power => true,
            Self::Abs => matches!(
                args.first().and_then(scalar),
                Some(ScalarType::I32 | ScalarType::I64)
            ),
            Self::Cast => match (args.first().and_then(scalar), scalar(ty)) {
                (Some(from), Some(to)) => cast_can_fail(from, to),
                _ => true,
            },
        }
    }
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
    /// The written null placement: `nulls first` (`Some(true)`) or `nulls
    /// last` (`Some(false)`). `None` keeps the fixed value order's
    /// placement: nulls first ascending, last descending.
    pub nulls_first: Option<bool>,
}

impl IROrdering {
    /// A key with the fixed value order's null placement.
    pub fn new(expr: IRExpr, descending: bool) -> Self {
        Self {
            expr,
            descending,
            nulls_first: None,
        }
    }

    /// Whether this key sorts its nulls before every non-null value.
    pub fn places_nulls_first(&self) -> bool {
        self.nulls_first.unwrap_or(!self.descending)
    }
}

#[cfg(test)]
mod block_type_tests {
    use super::*;
    use crate::types::PropType;
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
        // A float literal stays `F64`; the integer aggregate is cast.
        assert!(matches!(predicate.left, BlockAggregateExpr::Cast { .. }));
        assert!(matches!(predicate.right, IRExpr::Literal(..)));
        assert_eq!(predicate.left.ty(), &value(ScalarType::F64, true));
        assert_eq!(predicate.right.ty(), &value(ScalarType::F64, false));
        let BlockAggregateExpr::Aggregate { signature, .. } = predicate.left.leaf() else {
            panic!("expected owner")
        };
        assert_eq!(signature.result, value(ScalarType::I64, true));

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
    fn block_validation_refuses_forged_signatures_casts_and_aggregate_bounds() {
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
        // A bound read from the outer row is a scalar of the row; one
        // holding an aggregate is not.
        let mut outer = base.clone();
        outer.right = IRExpr::PropAccess {
            variable: "outer".into(),
            property: "amount".into(),
            ty: value(ScalarType::I64, false),
        };
        assert!(outer.check_types().is_ok());
        let mut forged = base.clone();
        forged.right = IRExpr::Aggregate {
            fraction: None,
            func: AggFunc::Sum,
            distinct: false,
            arg: Box::new(outer.right.clone()),
            signature: AggSignature {
                arg: value(ScalarType::I64, false),
                result: value(ScalarType::I64, true),
            },
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

/// A deep tree is taken apart child by child onto a heap stack, so dropping
/// it never recurses once per level: a dropped node with children moves
/// them out, leaving a leaf in each child's place, and drops them in turn.
impl Drop for IRExpr {
    fn drop(&mut self) {
        if self
            .children()
            .iter()
            .all(|child| child.children().is_empty())
        {
            return;
        }
        let mut pending = Vec::new();
        self.take_children(&mut pending);
        while let Some(mut child) = pending.pop() {
            child.take_children(&mut pending);
        }
    }
}

impl IRExpr {
    /// The levels of the expression's deepest path, counted without
    /// recursing.
    pub fn depth(&self) -> usize {
        let mut deepest = 0;
        let mut pending = vec![(self, 1)];
        while let Some((node, depth)) = pending.pop() {
            deepest = deepest.max(depth);
            pending.extend(node.children().into_iter().map(|child| (child, depth + 1)));
        }
        deepest
    }

    /// A leaf that owns nothing: what [`IRExpr::take`] leaves behind.
    pub fn placeholder() -> Self {
        Self::Literal(Literal::Null, boolean_type(true))
    }

    /// The expression in `slot`, a [`IRExpr::placeholder`] left there. A
    /// node is not moved out of field by field, since dropping one takes its
    /// children apart.
    pub fn take(slot: &mut IRExpr) -> IRExpr {
        std::mem::replace(slot, Self::placeholder())
    }

    /// Every child of the node moved into `out`, a placeholder in its place.
    fn take_children(&mut self, out: &mut Vec<IRExpr>) {
        match self {
            Self::PropAccess { .. }
            | Self::Variable(_, _)
            | Self::Param(_, _)
            | Self::Literal(_, _)
            | Self::AliasRef(_, _) => {}
            Self::Aggregate {
                arg: child,
                fraction,
                ..
            } => {
                out.push(Self::take(child));
                if let Some(p) = fraction {
                    out.push(Self::take(p));
                }
            }
            Self::Nearest { query: child, .. }
            | Self::Not(child, _)
            | Self::IsNull { expr: child, .. }
            | Self::Neg { expr: child, .. }
            | Self::Cast { expr: child, .. } => out.push(Self::take(child)),
            Self::Binary { left, right, .. } => {
                out.push(Self::take(left));
                out.push(Self::take(right));
            }
            Self::Call { args, .. } => out.append(args),
        }
    }
}
