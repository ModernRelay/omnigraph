use super::*;

use arrow_array::StructArray;
use arrow_ord::cmp;
use arrow_schema::Fields;
use datafusion::arrow::compute::kernels::boolean;
use omnigraph_compiler::catalog::NodeType;
use omnigraph_compiler::ir::expr::ScalarFunc;
use omnigraph_planner::PhysicalNode;

/// Node type per pipeline binding, for projecting a bare `$p` as one struct,
/// and the system column names the wide batch carries per binding. Owned:
/// the projection adapters of the lowered plan hold it for the query.
pub(super) struct ProjectionContext {
    catalog: Arc<Catalog>,
    bindings: HashMap<String, String>,
}

impl ProjectionContext {
    /// Every binding of `plan`: a scan or a metadata count binds its table's
    /// node type, an `Expand` binds its destination, inner trees included.
    pub(super) fn for_plan(catalog: &Arc<Catalog>, plan: &PhysicalPlan) -> Self {
        let mut bindings = HashMap::new();
        for (_, node) in plan.live() {
            match node {
                PhysicalNode::Scan { spec, .. } | PhysicalNode::MetadataCount { spec, .. } => {
                    let type_name = spec.table.type_key.strip_prefix("node:");
                    if let (Some(binding), Some(type_name)) = (&spec.binding, type_name) {
                        bindings.insert(binding.clone(), type_name.to_string());
                    }
                }
                PhysicalNode::Expand { dst, dst_type, .. } => {
                    bindings.insert(dst.clone(), dst_type.clone());
                }
                PhysicalNode::SortMergeJoin { .. }
                | PhysicalNode::HashJoin { .. }
                | PhysicalNode::HydrateByAddress { .. }
                | PhysicalNode::HydrateColumns { .. }
                | PhysicalNode::RowCompare { .. }
                | PhysicalNode::ClassifyThreeWay { .. }
                | PhysicalNode::Limit { .. }
                | PhysicalNode::TargetCut { .. }
                | PhysicalNode::Score { .. }
                | PhysicalNode::Let { .. }
                | PhysicalNode::Page { .. }
                | PhysicalNode::CrossJoin { .. }
                | PhysicalNode::ContainsJoin { .. }
                | PhysicalNode::Filter { .. }
                | PhysicalNode::AntiJoin { .. }
                | PhysicalNode::OptionalJoin { .. }
                | PhysicalNode::Union { .. }
                | PhysicalNode::OuterReference { .. }
                | PhysicalNode::RankFuse { .. }
                | PhysicalNode::Projection { .. }
                | PhysicalNode::Aggregate { .. }
                | PhysicalNode::Sort { .. }
                | PhysicalNode::CsvSource { .. }
                | PhysicalNode::DeltaSource { .. }
                | PhysicalNode::Validate { .. }
                | PhysicalNode::FragmentWrite { .. } => {}
            }
        }
        Self {
            catalog: Arc::clone(catalog),
            bindings,
        }
    }

    /// The node type bound to `variable`, when the catalog declares it.
    pub(super) fn node_type(&self, variable: &str) -> Option<&NodeType> {
        self.catalog.query_node_type(self.bindings.get(variable)?)
    }

    pub(super) fn declared_field(
        &self,
        name: &str,
        ty: &omnigraph_compiler::types::ExprType,
    ) -> Result<Field> {
        use omnigraph_compiler::types::ExprType;
        match ty {
            ExprType::Value { nullable, .. } => Ok(Field::new(
                name,
                ty.to_arrow()
                    .ok_or_else(|| OmniError::manifest_internal("value has no Arrow type"))?,
                *nullable,
            )),
            ExprType::ExactInteger { .. } => Err(OmniError::manifest_internal(
                "internal exact integer cannot be a public result field",
            )),
            ExprType::Edge { .. } => Err(OmniError::manifest_internal(
                "an edge reference cannot be a public result field",
            )),
            ExprType::Node { type_name } => {
                let node = self.catalog.query_node_type(type_name).ok_or_else(|| {
                    OmniError::manifest_internal(format!("node type {type_name} is absent"))
                })?;
                let fields: Vec<_> = node
                    .node_object_members()
                    .map(|(member, field)| {
                        Field::new(member, field.data_type().clone(), field.is_nullable())
                    })
                    .collect();
                Ok(Field::new(name, DataType::Struct(fields.into()), false))
            }
        }
    }

    #[cfg(test)]
    pub(super) fn bindings(&self) -> &HashMap<String, String> {
        &self.bindings
    }
}

#[cfg(test)]
pub(super) fn collect_node_bindings(pipeline: &[PatternStep], out: &mut HashMap<String, String>) {
    for op in pipeline {
        match op {
            PatternStep::NodeScan {
                variable,
                type_name,
                filters: _,
            } => {
                out.insert(variable.clone(), type_name.clone());
            }
            PatternStep::Expand {
                src_var: _,
                dst_var,
                edges: _,
                src_type: _,
                dst_type,
                min_hops: _,
                max_hops: _,
                dst_filters: _,
                edge_binding: _,
            } => {
                out.insert(dst_var.clone(), dst_type.clone());
            }
            PatternStep::Filter(_)
            | PatternStep::Rank { .. }
            | PatternStep::Score { .. }
            | PatternStep::Let { .. } => {}
            PatternStep::AntiJoin {
                outer_var: _,
                inner,
                predicate: _,
            } => collect_node_bindings(inner, out),
            PatternStep::Optional {
                outer_var: _,
                inner,
            } => collect_node_bindings(inner, out),
            PatternStep::Union { arms, bindings: _ } => {
                arms.iter().for_each(|arm| collect_node_bindings(arm, out))
            }
        }
    }
}

/// Evaluate a typed Boolean expression using the same kernels as projections.
pub(super) fn evaluate_filter(
    batch: &RecordBatch,
    filter: &IRExpr,
    params: &ParamMap,
) -> Result<BooleanArray> {
    boolean_mask(evaluate_expr(batch, filter, params)?, filter)
}

/// When at most this share of a batch's rows still needs a filter's value, the
/// filter runs over those rows alone; above it, over the whole batch, where
/// gathering the rows would cost more than it saves. DataFusion's `BinaryExpr`
/// uses the same share for its pre-selection.
const PRE_SELECTION_SHARE: f64 = 0.2;

/// The rows whose Kleene `and` (`decides` false) or `or` (`decides` true) the
/// left operand `left` has not decided: every row that is null or is not
/// `decides`. Never null itself.
pub(super) fn undecided(left: &BooleanArray, decides: bool) -> BooleanArray {
    let values = if decides {
        !left.values()
    } else {
        left.values().clone()
    };
    let undecided = match left.nulls() {
        Some(nulls) => &values | &!nulls.inner(),
        None => values,
    };
    BooleanArray::new(undecided, None)
}

/// The rows a filter mask keeps: true and not null. Never null itself.
pub(super) fn kept_rows(mask: &BooleanArray) -> BooleanArray {
    let kept = match mask.nulls() {
        Some(nulls) => mask.values() & nulls.inner(),
        None => mask.values().clone(),
    };
    BooleanArray::new(kept, None)
}

/// `filter` evaluated on the `selected` rows of `batch`: equal to
/// [`evaluate_filter`] on every selected row and null or that value on every
/// other row, so a caller combines it only with a mask that decides the
/// unselected rows. When no row is selected, no row is evaluated. When at
/// most [`PRE_SELECTION_SHARE`] of the rows are selected, only those rows are
/// evaluated, over just the columns `filter` reads, so an expensive operand
/// skips the rows a cheaper one already removed. Above that share every row
/// is evaluated, the unselected ones included.
///
/// So the result never depends on the unselected rows, but whether they are
/// evaluated does, through how many rows of the same batch are selected. That
/// is unobservable only while no operand can fail on a row's value (every
/// cast a filter records is a lossless widening, and writes refuse
/// non-finite floats), and so are the type and cast checks `evaluate_expr`
/// runs on the rows it evaluates. An operand that can fail must not be
/// evaluated on unselected rows at any share: the rule of
/// `docs/rfcs/2026-09-24-shared-expression-model.md` that keeps a fallible
/// conjunct behind the conjuncts guarding it. The change that adds such an
/// operand takes the gather path for it whatever the share.
pub(super) fn evaluate_filter_where(
    batch: &RecordBatch,
    filter: &IRExpr,
    params: &ParamMap,
    selected: &BooleanArray,
) -> Result<BooleanArray> {
    let rows = batch.num_rows();
    let kept = selected.true_count();
    if kept == 0 {
        return Ok(BooleanArray::new_null(rows));
    }
    if kept == rows || kept as f64 > rows as f64 * PRE_SELECTION_SHARE {
        return evaluate_filter(batch, filter, params);
    }
    let reads = columns_read(batch, filter).unwrap_or_else(|| batch.clone());
    let reads = arrow_select::filter::filter_record_batch(&reads, selected)
        .map_err(OmniError::arrow_internal)?;
    let values = evaluate_filter(&reads, filter, params)?;
    let scattered = datafusion::physical_expr_common::utils::scatter(selected, &values)
        .map_err(|err| OmniError::manifest_internal(err.to_string()))?;
    boolean_mask(scattered, filter)
}

/// The columns of `batch` that `filter` reads, by the names
/// [`evaluate_expr`] looks up; `None` when it reads a whole binding or a name
/// the batch lacks, so the caller keeps every column and evaluation reports
/// whatever it would report over the full batch.
fn columns_read(batch: &RecordBatch, filter: &IRExpr) -> Option<RecordBatch> {
    let schema = batch.schema();
    let mut indices = Vec::new();
    let mut complete = true;
    filter.visit_leaves(|leaf| {
        let name = match leaf {
            IRExpr::PropAccess {
                variable, property, ..
            } => property_column(variable, property),
            IRExpr::AliasRef(name, _) => name.clone(),
            IRExpr::Param(_, _) | IRExpr::Literal(_, _) => return,
            _ => {
                complete = false;
                return;
            }
        };
        match schema.index_of(&name) {
            Ok(index) => indices.push(index),
            Err(_) => complete = false,
        }
    });
    if !complete {
        return None;
    }
    indices.sort_unstable();
    indices.dedup();
    batch.project(&indices).ok()
}

fn evaluate_boolean(
    batch: &RecordBatch,
    filter: &IRExpr,
    params: &ParamMap,
) -> Result<BooleanArray> {
    match filter {
        IRExpr::Binary {
            left,
            op: BinaryOp::Compare(op),
            right,
            ty: _,
        } => evaluate_comparison(batch, left, *op, right, params),
        IRExpr::Binary {
            left,
            op: BinaryOp::And,
            right,
            ty: _,
        } => {
            let left = evaluate_filter(batch, left, params)?;
            let right = evaluate_filter_where(batch, right, params, &undecided(&left, false))?;
            boolean::and_kleene(&left, &right).map_err(OmniError::arrow_internal)
        }
        IRExpr::Binary {
            left,
            op: BinaryOp::Or,
            right,
            ty: _,
        } => {
            let left = evaluate_filter(batch, left, params)?;
            let right = evaluate_filter_where(batch, right, params, &undecided(&left, true))?;
            boolean::or_kleene(&left, &right).map_err(OmniError::arrow_internal)
        }
        IRExpr::Not(inner, _) => {
            let inner = evaluate_filter(batch, inner, params)?;
            boolean::not(&inner).map_err(OmniError::arrow_internal)
        }
        IRExpr::IsNull {
            expr,
            negated,
            ty: _,
        } => {
            let values = evaluate_expr(batch, expr, params)?;
            if *negated {
                boolean::is_not_null(&values)
            } else {
                boolean::is_null(&values)
            }
            .map_err(OmniError::arrow_internal)
        }
        IRExpr::PropAccess { .. }
        | IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Param(_, _)
        | IRExpr::Literal(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _)
        | IRExpr::Cast { .. }
        | IRExpr::Neg { .. }
        | IRExpr::Call { .. }
        | IRExpr::Binary {
            op: BinaryOp::Arith(_),
            ..
        } => Err(OmniError::manifest_internal("expected a Boolean operator")),
    }
}

fn boolean_mask(values: ArrayRef, expr: &IRExpr) -> Result<BooleanArray> {
    values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "filter `{expr}` is not Boolean: got {}",
                values.data_type()
            ))
        })
}

fn evaluate_comparison(
    batch: &RecordBatch,
    left: &IRExpr,
    op: CompOp,
    right: &IRExpr,
    params: &ParamMap,
) -> Result<BooleanArray> {
    let left = evaluate_expr(batch, left, params)?;
    let right = evaluate_expr(batch, right, params)?;
    compare_arrays(&left, op, &right)
}

/// Execute a comparison after both operands reached their recorded domain.
pub(super) fn compare_arrays(
    left: &ArrayRef,
    op: CompOp,
    right: &ArrayRef,
) -> Result<BooleanArray> {
    if op == CompOp::Contains {
        return evaluate_contains_filter(left, right);
    }
    if matches!(op, CompOp::StartsWith | CompOp::StringContains) {
        return evaluate_string_match_filter(op, left, right);
    }
    if left.data_type() != right.data_type() {
        return Err(OmniError::manifest_internal(format!(
            "comparison operands have different recorded domains: {} and {}",
            left.data_type(),
            right.data_type()
        )));
    }
    match op {
        CompOp::Eq => cmp::eq(left, right),
        CompOp::Ne => cmp::neq(left, right),
        CompOp::Gt => cmp::gt(left, right),
        CompOp::Lt => cmp::lt(left, right),
        CompOp::Ge => cmp::gt_eq(left, right),
        CompOp::Le => cmp::lt_eq(left, right),
        CompOp::Contains | CompOp::StartsWith | CompOp::StringContains => {
            unreachable!("handled above")
        }
    }
    .map_err(OmniError::arrow_internal)
}

/// Execute an expression in its recorded type and check the produced array.
pub(super) fn evaluate_expr(
    batch: &RecordBatch,
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<ArrayRef> {
    let values: ArrayRef = match expr {
        IRExpr::Binary {
            left,
            op: BinaryOp::Arith(op),
            right,
            ty: _,
        } => {
            let left = evaluate_expr(batch, left, params)?;
            let right = evaluate_expr(batch, right, params)?;
            super::arith::binary(*op, &left, &right)?
        }
        IRExpr::Neg { expr: child, .. } => {
            let values = evaluate_expr(batch, child, params)?;
            super::arith::negate(&values)?
        }
        // `if` evaluates its condition, then each branch only on the rows
        // that select it, and `coalesce` each argument only on the rows still
        // null: a branch not taken never fails (A3).
        IRExpr::Call {
            func: ScalarFunc::If,
            args,
            ty,
        } if args.len() == 3 => {
            let cond = evaluate_expr(batch, &args[0], params)?;
            let taken: BooleanArray = cond
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| OmniError::manifest_internal("`if` condition is not Boolean"))?
                .iter()
                .map(|value| Some(value == Some(true)))
                .collect();
            let other = boolean::not(&taken).map_err(OmniError::arrow_internal)?;
            let then = evaluate_selected(batch, &args[1], params, &taken)?;
            let otherwise = evaluate_selected(batch, &args[2], params, &other)?;
            super::arith::call(ScalarFunc::If, &[cond, then, otherwise], ty)?
        }
        IRExpr::Call {
            func: ScalarFunc::Coalesce,
            args,
            ty,
        } if !args.is_empty() => {
            let mut values = evaluate_expr(batch, &args[0], params)?;
            for next in &args[1..] {
                let missing = boolean::is_null(&values).map_err(OmniError::arrow_internal)?;
                let next = evaluate_selected(batch, next, params, &missing)?;
                values = super::arith::call(ScalarFunc::Coalesce, &[values, next], ty)?;
            }
            values
        }
        IRExpr::Call { func, args, ty } => {
            let args = args
                .iter()
                .map(|arg| evaluate_expr(batch, arg, params))
                .collect::<Result<Vec<_>>>()?;
            super::arith::call(*func, &args, ty)?
        }
        IRExpr::Binary { .. } | IRExpr::Not(_, _) | IRExpr::IsNull { .. } => {
            Arc::new(evaluate_boolean(batch, expr, params)?)
        }
        IRExpr::Cast { expr: child, ty } => {
            let values = evaluate_expr(batch, child, params)?;
            super::typed_value::cast_array(&values, child, ty)?
        }
        IRExpr::PropAccess {
            variable, property, ..
        } => {
            let col_name = property_column(variable, property);
            Arc::clone(batch.column_by_name(&col_name).ok_or_else(|| {
                OmniError::manifest(format!("column '{col_name}' not found in wide batch"))
            })?)
        }
        IRExpr::Literal(lit, ty) => typed_literal_to_array(lit, ty, batch.num_rows())?,
        IRExpr::Param(name, ty) => {
            let lit = params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{name}' not provided")))?;
            typed_literal_to_array(lit, ty, batch.num_rows())?
        }
        IRExpr::AliasRef(name, _) => alias_column(batch, name)?,
        IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Aggregate { .. } => {
            return Err(OmniError::manifest(format!(
                "unsupported expression in filter: {expr}"
            )));
        }
    };
    check_array_type(&values, expr.ty(), &expr.to_string())?;
    Ok(values)
}

/// `expr` evaluated on the rows `selected` keeps alone, null on the others;
/// a row not selected is never evaluated, so it never fails.
fn evaluate_selected(
    batch: &RecordBatch,
    expr: &IRExpr,
    params: &ParamMap,
    selected: &BooleanArray,
) -> Result<ArrayRef> {
    if selected.true_count() == batch.num_rows() {
        return evaluate_expr(batch, expr, params);
    }
    let rows = arrow_select::filter::filter_record_batch(batch, selected)
        .map_err(OmniError::arrow_internal)?;
    let values = evaluate_expr(&rows, expr, params)?;
    datafusion::physical_expr_common::utils::scatter(selected, &values)
        .map_err(|err| OmniError::manifest_internal(err.to_string()))
}

/// The wide-batch column that holds `variable.property`. Evaluation and
/// [`columns_read`] both name a property's column through it, so the columns
/// gathered for a selection are the ones evaluation then looks up.
fn property_column(variable: &str, property: &str) -> String {
    format!("{variable}.{property}")
}

/// A result column of the input: an aggregate leaf or a group key the
/// `Aggregate` node below projected under this name.
fn alias_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    batch.column_by_name(name).map(Arc::clone).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "the projection input carries no column '{name}' the alias names"
        ))
    })
}

/// Membership is null for a null list or needle; null items never match.
pub(super) fn evaluate_contains_filter(left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray> {
    let DataType::List(field) = left.data_type() else {
        return Err(OmniError::manifest_internal(
            "contains requires a recorded list domain",
        ));
    };
    if field.data_type() != right.data_type() || left.len() != right.len() {
        return Err(OmniError::manifest_internal(
            "contains operands have different recorded domains or lengths",
        ));
    }
    let list = left
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| OmniError::manifest_internal("contains requires an Arrow ListArray"))?;
    let mut values = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) || right.is_null(row) {
            values.push(None);
            continue;
        }
        let items = list.value(row);
        let mut found = false;
        for idx in 0..items.len() {
            if array_value_eq(items.as_ref(), idx, right.as_ref(), row)? {
                found = true;
                break;
            }
        }
        values.push(Some(found));
    }
    Ok(BooleanArray::from(values))
}

pub(super) fn evaluate_string_match_filter(
    op: CompOp,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<BooleanArray> {
    if left.data_type() != &DataType::Utf8 || right.data_type() != &DataType::Utf8 {
        return Err(OmniError::manifest_internal(
            "string comparison requires recorded String domains",
        ));
    }
    let (left, right): (&dyn Array, &dyn Array) = (left.as_ref(), right.as_ref());
    match op {
        CompOp::StartsWith => arrow_string::like::starts_with(&left, &right),
        CompOp::StringContains => arrow_string::like::contains(&left, &right),
        CompOp::Eq
        | CompOp::Ne
        | CompOp::Gt
        | CompOp::Lt
        | CompOp::Ge
        | CompOp::Le
        | CompOp::Contains => {
            return Err(OmniError::manifest_internal(
                "invalid string comparison operator",
            ));
        }
    }
    .map_err(OmniError::arrow_internal)
}

pub(super) fn array_value_eq(
    left: &dyn Array,
    left_index: usize,
    right: &dyn Array,
    right_index: usize,
) -> Result<bool> {
    if left.data_type() != right.data_type() {
        return Err(OmniError::manifest_internal(
            "membership element and needle domains differ",
        ));
    }
    if left.is_null(left_index) || right.is_null(right_index) {
        return Ok(false);
    }
    let equal = arrow_ord::cmp::eq(&left.slice(left_index, 1), &right.slice(right_index, 1))
        .map_err(OmniError::arrow_internal)?;
    Ok(equal.value(0))
}

/// Evaluate one return expression; lowering applies its alias.
/// The projection adapter validates the complete declared field, including
/// the captured members of a bare node.
pub(super) fn evaluate_projection(
    wide_batch: &RecordBatch,
    expr: &IRExpr,
    params: &ParamMap,
    ctx: &ProjectionContext,
) -> Result<(String, ArrayRef)> {
    match expr {
        IRExpr::PropAccess {
            variable,
            property,
            ty,
        } => {
            let col_name = property_column(variable, property);
            let col = wide_batch.column_by_name(&col_name).ok_or_else(|| {
                OmniError::manifest(format!("column '{}' not found in wide batch", col_name))
            })?;
            check_array_type(col, ty, &col_name)?;
            Ok((col_name, col.clone()))
        }
        IRExpr::Literal(lit, ty) => {
            let arr = typed_literal_to_array(lit, ty, wide_batch.num_rows())?;
            Ok(("literal".to_string(), arr))
        }
        IRExpr::Param(name, ty) => {
            let lit = params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?;
            let arr = typed_literal_to_array(lit, ty, wide_batch.num_rows())?;
            Ok((name.clone(), arr))
        }
        IRExpr::Variable(name, ty) => {
            let node_type = ctx.node_type(name).ok_or_else(|| {
                OmniError::manifest(format!("variable '{}' is not a node binding", name))
            })?;
            if ty
                != &(omnigraph_compiler::types::ExprType::Node {
                    type_name: node_type.name.clone(),
                })
            {
                return Err(OmniError::manifest_internal(
                    "node variable type disagrees with its binding",
                ));
            }
            let wide_schema = wide_batch.schema();
            let mut fields: Vec<Field> = Vec::new();
            let mut columns: Vec<ArrayRef> = Vec::new();
            for (member, field) in node_type.node_object_members() {
                let col_name = format!("{}.{}", name, field.name());
                let (idx, _wide_field) =
                    wide_schema.column_with_name(&col_name).ok_or_else(|| {
                        OmniError::manifest(format!(
                            "column '{}' not found in wide batch",
                            col_name
                        ))
                    })?;
                let col = wide_batch.column(idx).clone();
                if col.data_type() != field.data_type()
                    || !field.is_nullable() && col.null_count() != 0
                {
                    return Err(OmniError::manifest_internal(format!(
                        "node member {col_name} violates its declared field"
                    )));
                }
                fields.push(Field::new(
                    member,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
                columns.push(col);
            }
            let node = StructArray::try_new(Fields::from(fields), columns, None)
                .map_err(OmniError::arrow_internal)?;
            Ok((name.clone(), Arc::new(node) as ArrayRef))
        }
        IRExpr::Binary { .. }
        | IRExpr::Not(_, _)
        | IRExpr::IsNull { .. }
        | IRExpr::Cast { .. }
        | IRExpr::Neg { .. }
        | IRExpr::Call { .. } => Ok((expr.to_string(), evaluate_expr(wide_batch, expr, params)?)),
        IRExpr::AliasRef(name, _) => Ok((name.clone(), alias_column(wide_batch, name)?)),
        IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Aggregate { .. } => Err(OmniError::manifest(format!(
            "unsupported projection expression: {}",
            expr
        ))),
    }
}

/// What `count($var)` counts: the binding's identity column, one per row and
/// never null, under the bare `$var` projection's name. The scan behind the
/// binding is pruned to it (`projection_pushdown`, #704), so no node struct is built.
pub(super) fn identity_column(
    wide_batch: &RecordBatch,
    variable: &str,
    ctx: &ProjectionContext,
) -> Result<(String, ArrayRef)> {
    if ctx.node_type(variable).is_none() {
        return Err(OmniError::manifest(format!(
            "variable '{}' is not a node binding",
            variable
        )));
    }
    let col_name = format!("{}.{}", variable, ctx.catalog.system_columns.id);
    let col = wide_batch.column_by_name(&col_name).ok_or_else(|| {
        OmniError::manifest(format!("column '{}' not found in wide batch", col_name))
    })?;
    Ok((variable.to_string(), col.clone()))
}

#[cfg(test)]
mod pre_selection_tests {
    use super::*;
    use arrow_array::{Float32Array, Int64Array};
    use omnigraph_compiler::types::{ExprType, PropType};

    fn ty(scalar: ScalarType, nullable: bool) -> ExprType {
        ExprType::from_prop(&PropType::scalar(scalar, nullable))
    }

    fn prop(property: &str, scalar: ScalarType) -> IRExpr {
        IRExpr::PropAccess {
            variable: "a".to_string(),
            property: property.to_string(),
            ty: ty(scalar, true),
        }
    }

    fn compare(left: IRExpr, op: CompOp, right: i64) -> IRExpr {
        IRExpr::comparison(
            left,
            op,
            IRExpr::Literal(Literal::Integer(right.into()), ty(ScalarType::I64, false)),
        )
    }

    /// `a.x`, `a.y`, `a.z`: I64 values in 0..100, null where 7, 11 or 13
    /// divides the drawn value.
    fn batch(rows: usize, seed: u64) -> RecordBatch {
        let mut state = seed;
        let mut column = |nulls: u64| -> ArrayRef {
            Arc::new(Int64Array::from_iter((0..rows).map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let value = (state >> 33) % 100;
                (!value.is_multiple_of(nulls)).then_some(value as i64)
            })))
        };
        let (x, y, z) = (column(7), column(11), column(13));
        let fields: Vec<_> = ["a.x", "a.y", "a.z"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![x, y, z]).unwrap()
    }

    /// `left op right` with both operands evaluated over every row, the
    /// evaluator's behaviour before pre-selection.
    fn full(batch: &RecordBatch, expr: &IRExpr) -> BooleanArray {
        match expr {
            IRExpr::Binary {
                left,
                op: op @ (BinaryOp::And | BinaryOp::Or),
                right,
                ..
            } => {
                let (left, right) = (full(batch, left), full(batch, right));
                if *op == BinaryOp::And {
                    boolean::and_kleene(&left, &right).unwrap()
                } else {
                    boolean::or_kleene(&left, &right).unwrap()
                }
            }
            _ => evaluate_filter(batch, expr, &ParamMap::new()).unwrap(),
        }
    }

    /// When the left operand of `and` keeps 10 of 100 rows (at most
    /// `PRE_SELECTION_SHARE`), the right one runs only on those rows. The F32
    /// column holds an infinity (which no write stores) only on a removed
    /// row, so the cast to F64 that the comparison records fails there and
    /// only there. Before pre-selection both operands ran over every row and
    /// the filter failed. Above the share the right operand runs over every
    /// row again and this batch fails as before; see `evaluate_filter_where`.
    #[test]
    fn and_skips_the_rows_its_left_operand_removed_issue_1336() {
        let rows = 100;
        let keep = Int64Array::from_iter_values((0..rows).map(|row| row % 10));
        let measure = Float32Array::from_iter_values(
            (0..rows).map(|row| if row == 1 { f32::INFINITY } else { row as f32 }),
        );
        let schema = Schema::new(vec![
            Field::new("a.keep", DataType::Int64, true),
            Field::new("a.measure", DataType::Float32, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(keep) as ArrayRef, Arc::new(measure) as ArrayRef],
        )
        .unwrap();
        let widened = IRExpr::Cast {
            expr: Box::new(prop("measure", ScalarType::F32)),
            ty: ty(ScalarType::F64, true),
        };
        let threshold = IRExpr::Literal(Literal::Float(25.0), ty(ScalarType::F64, false));
        let expensive = IRExpr::comparison(widened, CompOp::Gt, threshold);
        assert!(
            evaluate_filter(&batch, &expensive, &ParamMap::new()).is_err(),
            "the infinite row fails the cast when it is evaluated"
        );
        let cheap = compare(prop("keep", ScalarType::I64), CompOp::Eq, 0);
        let filter = IRExpr::binary(cheap, BinaryOp::And, expensive);

        let mask = evaluate_filter(&batch, &filter, &ParamMap::new()).unwrap();
        let kept: Vec<usize> = (0..rows as usize).filter(|&row| mask.value(row)).collect();
        assert_eq!(kept, vec![30, 40, 50, 60, 70, 80, 90]);
        assert_eq!(mask.null_count(), 0);
    }

    /// Pre-selection changes no value: every `and`/`or` shape, every left
    /// selectivity (none, a few, the threshold, most, all) and nulls on both
    /// sides give exactly the Kleene result of evaluating both operands in
    /// full.
    #[test]
    fn pre_selection_keeps_kleene_values() {
        let x = || prop("x", ScalarType::I64);
        let y = || prop("y", ScalarType::I64);
        let z = || prop("z", ScalarType::I64);
        for seed in [1, 2, 3] {
            let batch = batch(1_000, seed);
            for cut in [0, 3, 20, 21, 60, 100] {
                let left = || compare(x(), CompOp::Lt, cut);
                let right = || compare(y(), CompOp::Ge, 50);
                let shapes = [
                    IRExpr::binary(left(), BinaryOp::And, right()),
                    IRExpr::binary(left(), BinaryOp::Or, right()),
                    IRExpr::binary(
                        IRExpr::logical_not(left()),
                        BinaryOp::Or,
                        IRExpr::binary(right(), BinaryOp::And, compare(z(), CompOp::Ne, 4)),
                    ),
                    IRExpr::binary(
                        IRExpr::binary(left(), BinaryOp::And, IRExpr::null_test(y(), true)),
                        BinaryOp::And,
                        IRExpr::binary(right(), BinaryOp::Or, compare(z(), CompOp::Lt, cut)),
                    ),
                ];
                for shape in shapes {
                    let got = evaluate_filter(&batch, &shape, &ParamMap::new()).unwrap();
                    assert_eq!(got, full(&batch, &shape), "{shape} at cut {cut}");
                }
            }
        }
    }

    /// A selection of no row evaluates nothing, so it reports no error even
    /// for a column the batch lacks; a selection of a few rows over a filter
    /// that reads no column keeps the selected row count.
    #[test]
    fn evaluate_filter_where_handles_empty_and_columnless_selections() {
        let batch = batch(50, 4);
        let missing = compare(prop("absent", ScalarType::I64), CompOp::Eq, 1);
        let none = BooleanArray::from(vec![false; 50]);
        let values = evaluate_filter_where(&batch, &missing, &ParamMap::new(), &none).unwrap();
        assert_eq!((values.len(), values.null_count()), (50, 50));

        let constant = IRExpr::comparison(
            IRExpr::Literal(Literal::Integer(1), ty(ScalarType::I64, false)),
            CompOp::Eq,
            IRExpr::Literal(Literal::Integer(1), ty(ScalarType::I64, false)),
        );
        let few = BooleanArray::from_iter((0..50).map(|row| Some(row % 10 == 0)));
        let values = evaluate_filter_where(&batch, &constant, &ParamMap::new(), &few).unwrap();
        for row in 0..50 {
            assert_eq!(values.is_valid(row), row % 10 == 0, "row {row}");
        }
        assert_eq!(values.true_count(), 5);
    }
}
