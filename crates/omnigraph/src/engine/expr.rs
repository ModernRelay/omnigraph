use super::*;

use arrow_array::StructArray;
use arrow_ord::cmp;
use arrow_schema::Fields;
use datafusion::arrow::compute::kernels::boolean;
use omnigraph_compiler::catalog::NodeType;
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
                | PhysicalNode::RowCompare { .. }
                | PhysicalNode::ClassifyThreeWay { .. }
                | PhysicalNode::Limit { .. }
                | PhysicalNode::Page { .. }
                | PhysicalNode::CrossJoin { .. }
                | PhysicalNode::ContainsJoin { .. }
                | PhysicalNode::Filter { .. }
                | PhysicalNode::AntiJoin { .. }
                | PhysicalNode::OuterReference { .. }
                | PhysicalNode::RankFuse { .. }
                | PhysicalNode::Projection { .. }
                | PhysicalNode::Aggregate { .. }
                | PhysicalNode::Sort { .. } => {}
            }
        }
        Self {
            catalog: Arc::clone(catalog),
            bindings,
        }
    }

    /// The node type bound to `variable`, when the catalog declares it.
    pub(super) fn node_type(&self, variable: &str) -> Option<&NodeType> {
        self.catalog.node_types.get(self.bindings.get(variable)?)
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
            ExprType::Node { type_name } => {
                let node = self.catalog.node_types.get(type_name).ok_or_else(|| {
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
pub(super) fn collect_node_bindings(pipeline: &[IROp], out: &mut HashMap<String, String>) {
    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters: _,
            } => {
                out.insert(variable.clone(), type_name.clone());
            }
            IROp::Expand {
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
            IROp::Filter(_) => {}
            IROp::AntiJoin {
                outer_var: _,
                inner,
                predicate: _,
            } => collect_node_bindings(inner, out),
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
            let right = evaluate_filter(batch, right, params)?;
            boolean::and_kleene(&left, &right).map_err(OmniError::arrow_internal)
        }
        IRExpr::Binary {
            left,
            op: BinaryOp::Or,
            right,
            ty: _,
        } => {
            let left = evaluate_filter(batch, left, params)?;
            let right = evaluate_filter(batch, right, params)?;
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
        | IRExpr::Cast { .. } => Err(OmniError::manifest_internal("expected a Boolean operator")),
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
            let col_name = format!("{variable}.{property}");
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
        IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _) => {
            return Err(OmniError::manifest(format!(
                "unsupported expression in filter: {expr}"
            )));
        }
    };
    check_array_type(&values, expr.ty(), &expr.to_string())?;
    Ok(values)
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

/// Evaluate a single projection expression against a wide batch; a Boolean
/// expression projects its `evaluate_filter` mask under its GQ text, and the
/// lowering names the column by the alias T43 requires.
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
            let col_name = format!("{}.{}", variable, property);
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
        IRExpr::Binary { .. } | IRExpr::Not(_, _) | IRExpr::IsNull { .. } | IRExpr::Cast { .. } => {
            Ok((expr.to_string(), evaluate_expr(wide_batch, expr, params)?))
        }
        IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _) => Err(OmniError::manifest(format!(
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
