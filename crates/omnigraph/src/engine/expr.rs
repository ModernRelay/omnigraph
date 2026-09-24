use super::*;

use arrow_array::StructArray;
use arrow_schema::Fields;
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
                edge_type: _,
                direction: _,
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

/// Evaluate a filter predicate against a batch, producing a boolean mask.
pub(super) fn evaluate_filter(
    batch: &RecordBatch,
    filter: &IRFilter,
    params: &ParamMap,
) -> Result<BooleanArray> {
    let left = evaluate_expr(batch, &filter.left, params)?;
    let right = evaluate_expr(batch, &filter.right, params)?;

    if filter.op == CompOp::Contains {
        return evaluate_contains_filter(&left, &right);
    }
    if matches!(filter.op, CompOp::StartsWith | CompOp::StringContains) {
        return evaluate_string_match_filter(filter.op, &left, &right);
    }
    let right = if left.data_type() != right.data_type() {
        arrow_cast::cast::cast(&right, left.data_type()).map_err(OmniError::arrow_internal)?
    } else {
        right
    };

    use arrow_ord::cmp;
    let result = match filter.op {
        CompOp::Eq => cmp::eq(&left, &right),
        CompOp::Ne => cmp::neq(&left, &right),
        CompOp::Gt => cmp::gt(&left, &right),
        CompOp::Lt => cmp::lt(&left, &right),
        CompOp::Ge => cmp::gt_eq(&left, &right),
        CompOp::Le => cmp::lt_eq(&left, &right),
        CompOp::Contains | CompOp::StartsWith | CompOp::StringContains => {
            unreachable!("handled above")
        }
    }
    .map_err(OmniError::arrow_internal)?;

    Ok(result)
}

/// Evaluate an IR expression against a wide batch, producing an array.
pub(super) fn evaluate_expr(
    batch: &RecordBatch,
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<ArrayRef> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let col_name = format!("{}.{}", variable, property);
            batch.column_by_name(&col_name).cloned().ok_or_else(|| {
                OmniError::manifest(format!("column '{}' not found in wide batch", col_name))
            })
        }
        IRExpr::Literal(lit) => literal_to_array(lit, batch.num_rows()),
        IRExpr::Param(name) => {
            let lit = params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?;
            literal_to_array(lit, batch.num_rows())
        }
        _ => Err(OmniError::manifest(format!(
            "unsupported expression in filter: {}",
            expr
        ))),
    }
}

/// Broadcast a literal in its natural Arrow type for residual and pushed filters.
pub(super) fn literal_to_array(lit: &Literal, num_rows: usize) -> Result<ArrayRef> {
    Ok(match lit {
        Literal::Null => arrow_array::new_null_array(&DataType::Utf8, num_rows),
        Literal::String(s) => Arc::new(StringArray::from(vec![s.as_str(); num_rows])) as ArrayRef,
        Literal::Integer(n) => Arc::new(Int64Array::from(vec![*n; num_rows])) as ArrayRef,
        Literal::Float(f) => Arc::new(Float64Array::from(vec![*f; num_rows])) as ArrayRef,
        Literal::Bool(b) => Arc::new(BooleanArray::from(vec![*b; num_rows])) as ArrayRef,
        Literal::Date(s) => {
            let days = crate::loader::parse_date32_literal(s)?;
            Arc::new(Date32Array::from(vec![days; num_rows])) as ArrayRef
        }
        Literal::DateTime(s) => {
            let ms = crate::loader::parse_date64_literal(s)?;
            Arc::new(Date64Array::from(vec![ms; num_rows])) as ArrayRef
        }
        Literal::List(items) => literal_list_to_array(items, num_rows)?,
    })
}

pub(super) fn evaluate_contains_filter(left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray> {
    let DataType::List(field) = left.data_type() else {
        return Err(OmniError::manifest(
            "contains requires a list property on the left".to_string(),
        ));
    };
    let right = if right.data_type() != field.data_type() {
        arrow_cast::cast::cast(right, field.data_type()).map_err(OmniError::arrow_internal)?
    } else {
        Arc::clone(right)
    };
    let list = left
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| OmniError::manifest("contains requires an Arrow ListArray"))?;

    let mut values = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) || right.is_null(row) {
            values.push(Some(false));
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

/// Evaluate exact, case-sensitive string predicates using Arrow's string kernels.
/// NULL on either side produces false, matching pushed filters' WHERE semantics.
pub(super) fn evaluate_string_match_filter(
    op: CompOp,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<BooleanArray> {
    let right = if right.data_type() != left.data_type() {
        arrow_cast::cast::cast(right, left.data_type()).map_err(OmniError::arrow_internal)?
    } else {
        Arc::clone(right)
    };
    let (left_dyn, right_dyn): (&dyn Array, &dyn Array) = (left.as_ref(), right.as_ref());
    let matches = match op {
        CompOp::StartsWith => arrow_string::like::starts_with(&left_dyn, &right_dyn),
        _ => arrow_string::like::contains(&left_dyn, &right_dyn),
    }
    .map_err(|e| OmniError::manifest(format!("{op} requires String operands: {e}")))?;
    if matches.nulls().is_some() {
        Ok(arrow_select::filter::prep_null_mask_filter(&matches))
    } else {
        Ok(matches)
    }
}

pub(super) fn array_value_eq(
    left: &dyn Array,
    left_index: usize,
    right: &dyn Array,
    right_index: usize,
) -> Result<bool> {
    if left.is_null(left_index) || right.is_null(right_index) {
        return Ok(false);
    }
    let left_value = array_value_to_string(left, left_index).map_err(OmniError::arrow_internal)?;
    let right_value =
        array_value_to_string(right, right_index).map_err(OmniError::arrow_internal)?;
    Ok(left_value == right_value)
}

pub(super) fn literal_list_to_array(items: &[Literal], num_rows: usize) -> Result<ArrayRef> {
    if items.is_empty() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for _ in 0..num_rows {
            builder.append(true);
        }
        return Ok(Arc::new(builder.finish()));
    }

    let scalar_type = list_scalar_type(items)?;
    match scalar_type {
        ScalarType::String => {
            let mut builder = ListBuilder::with_capacity(StringBuilder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Utf8, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::String(value) => builder.values().append_value(value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::Bool => {
            let mut builder = ListBuilder::with_capacity(BooleanBuilder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Boolean, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Bool(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::I32 => {
            let mut builder = ListBuilder::with_capacity(Int32Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Int32, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as i32),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::I64 | ScalarType::U32 | ScalarType::U64 => {
            let mut builder = ListBuilder::with_capacity(Int64Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Int64, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::F32 | ScalarType::F64 => {
            let mut builder = ListBuilder::with_capacity(Float64Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Float64, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f64),
                        Literal::Float(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::Date => {
            let mut builder = ListBuilder::with_capacity(Date32Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Date32, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Date(value) => builder
                            .values()
                            .append_value(crate::loader::parse_date32_literal(value)?),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::DateTime => {
            let mut builder = ListBuilder::with_capacity(Date64Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Date64, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::DateTime(value) => builder
                            .values()
                            .append_value(crate::loader::parse_date64_literal(value)?),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::Vector(_) | ScalarType::Blob => Err(OmniError::manifest(
            "unsupported list literal element type".to_string(),
        )),
    }
}

pub(super) fn list_scalar_type(items: &[Literal]) -> Result<ScalarType> {
    let first = items
        .first()
        .ok_or_else(|| OmniError::manifest("empty list literal"))?;
    let expected = literal_scalar_type(first)?;
    for item in items.iter().skip(1) {
        let item_type = literal_scalar_type(item)?;
        if item_type != expected {
            return Err(OmniError::manifest(
                "list literal elements must share a compatible scalar type".to_string(),
            ));
        }
    }
    Ok(expected)
}

pub(super) fn literal_scalar_type(lit: &Literal) -> Result<ScalarType> {
    match lit {
        Literal::Null => Ok(ScalarType::String),
        Literal::String(_) => Ok(ScalarType::String),
        Literal::Integer(_) => Ok(ScalarType::I64),
        Literal::Float(_) => Ok(ScalarType::F64),
        Literal::Bool(_) => Ok(ScalarType::Bool),
        Literal::Date(_) => Ok(ScalarType::Date),
        Literal::DateTime(_) => Ok(ScalarType::DateTime),
        Literal::List(_) => Err(OmniError::manifest(
            "nested list literals are not supported".to_string(),
        )),
    }
}

/// Evaluate a single projection expression against a wide batch.
pub(super) fn evaluate_projection(
    wide_batch: &RecordBatch,
    expr: &IRExpr,
    params: &ParamMap,
    ctx: &ProjectionContext,
) -> Result<(String, ArrayRef)> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let col_name = format!("{}.{}", variable, property);
            let col = wide_batch.column_by_name(&col_name).ok_or_else(|| {
                OmniError::manifest(format!("column '{}' not found in wide batch", col_name))
            })?;
            Ok((col_name, col.clone()))
        }
        IRExpr::Literal(lit) => {
            let arr = literal_to_array(lit, wide_batch.num_rows())?;
            Ok(("literal".to_string(), arr))
        }
        IRExpr::Param(name) => {
            let lit = params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?;
            let arr = literal_to_array(lit, wide_batch.num_rows())?;
            Ok((name.clone(), arr))
        }
        IRExpr::Variable(name) => {
            let node_type = ctx.node_type(name).ok_or_else(|| {
                OmniError::manifest(format!("variable '{}' is not a node binding", name))
            })?;
            let wide_schema = wide_batch.schema();
            let mut fields: Vec<Field> = Vec::new();
            let mut columns: Vec<ArrayRef> = Vec::new();
            for (member, field) in node_type.node_object_members() {
                let col_name = format!("{}.{}", name, field.name());
                let (idx, wide_field) =
                    wide_schema.column_with_name(&col_name).ok_or_else(|| {
                        OmniError::manifest(format!(
                            "column '{}' not found in wide batch",
                            col_name
                        ))
                    })?;
                let col = wide_batch.column(idx).clone();
                fields.push(Field::new(
                    member,
                    col.data_type().clone(),
                    wide_field.is_nullable(),
                ));
                columns.push(col);
            }
            let node = StructArray::try_new(Fields::from(fields), columns, None)
                .map_err(OmniError::arrow_internal)?;
            Ok((name.clone(), Arc::new(node) as ArrayRef))
        }
        _ => Err(OmniError::manifest(format!(
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
