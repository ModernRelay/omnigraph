use super::*;

pub(super) fn apply_filter(
    bindings: &mut HashMap<String, RecordBatch>,
    filter: &IRFilter,
    params: &ParamMap,
) -> Result<()> {
    // Find which binding this filter applies to
    let var_name = match &filter.left {
        IRExpr::PropAccess { variable, .. } => variable.clone(),
        _ => return Ok(()), // Can't determine variable
    };

    let batch = bindings.get(&var_name).ok_or_else(|| {
        OmniError::manifest(format!("filter references unbound variable '{}'", var_name))
    })?;

    let mask = evaluate_filter(batch, filter, params)?;
    let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    bindings.insert(var_name, filtered);
    Ok(())
}

/// Evaluate a filter predicate against a batch, producing a boolean mask.
fn evaluate_filter(
    batch: &RecordBatch,
    filter: &IRFilter,
    params: &ParamMap,
) -> Result<BooleanArray> {
    let left = evaluate_expr(batch, &filter.left, params)?;
    let right = evaluate_expr(batch, &filter.right, params)?;

    if filter.op == CompOp::Contains {
        return evaluate_contains_filter(&left, &right);
    }

    // Cast right to match left's type if needed (e.g. Int64 literal vs Int32 column)
    let right = if left.data_type() != right.data_type() {
        arrow_cast::cast::cast(&right, left.data_type())
            .map_err(|e| OmniError::Lance(e.to_string()))?
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
        CompOp::Contains => unreachable!("handled above"),
    }
    .map_err(|e| OmniError::Lance(e.to_string()))?;

    Ok(result)
}

/// Evaluate an IR expression against a batch, producing an array.
fn evaluate_expr(batch: &RecordBatch, expr: &IRExpr, params: &ParamMap) -> Result<ArrayRef> {
    match expr {
        IRExpr::PropAccess { property, .. } => {
            batch.column_by_name(property).cloned().ok_or_else(|| {
                OmniError::manifest(format!("column '{}' not found in batch", property))
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
            "unsupported expression in filter: {:?}",
            expr
        ))),
    }
}

/// Create a constant array from a literal value.
fn literal_to_array(lit: &Literal, num_rows: usize) -> Result<ArrayRef> {
    Ok(match lit {
        Literal::String(s) => Arc::new(StringArray::from(vec![s.as_str(); num_rows])) as ArrayRef,
        Literal::Integer(n) => {
            // Try to match the most common integer types
            Arc::new(Int64Array::from(vec![*n; num_rows])) as ArrayRef
        }
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

fn evaluate_contains_filter(left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray> {
    let DataType::List(field) = left.data_type() else {
        return Err(OmniError::manifest(
            "contains requires a list property on the left".to_string(),
        ));
    };
    let right = if right.data_type() != field.data_type() {
        arrow_cast::cast::cast(right, field.data_type())
            .map_err(|e| OmniError::Lance(e.to_string()))?
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

fn array_value_eq(
    left: &dyn Array,
    left_index: usize,
    right: &dyn Array,
    right_index: usize,
) -> Result<bool> {
    if left.is_null(left_index) || right.is_null(right_index) {
        return Ok(false);
    }
    let left_value =
        array_value_to_string(left, left_index).map_err(|e| OmniError::Lance(e.to_string()))?;
    let right_value =
        array_value_to_string(right, right_index).map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(left_value == right_value)
}

fn literal_list_to_array(items: &[Literal], num_rows: usize) -> Result<ArrayRef> {
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

fn list_scalar_type(items: &[Literal]) -> Result<ScalarType> {
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

fn literal_scalar_type(lit: &Literal) -> Result<ScalarType> {
    match lit {
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

/// Project return expressions into a result batch.
pub(super) fn project_return(
    bindings: &HashMap<String, RecordBatch>,
    projections: &[IRProjection],
    params: &ParamMap,
) -> Result<RecordBatch> {
    if projections.is_empty() {
        return Err(OmniError::manifest(
            "query has no return projections".to_string(),
        ));
    }

    let mut fields = Vec::with_capacity(projections.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projections.len());

    for proj in projections {
        let (name, col) = evaluate_projection(bindings, &proj.expr, params)?;
        let field_name = proj.alias.as_deref().unwrap_or(&name);
        fields.push(Field::new(
            field_name,
            col.data_type().clone(),
            col.null_count() > 0,
        ));
        columns.push(col);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

/// Evaluate a single projection expression.
fn evaluate_projection(
    bindings: &HashMap<String, RecordBatch>,
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<(String, ArrayRef)> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let batch = bindings.get(variable).ok_or_else(|| {
                OmniError::manifest(format!(
                    "projection references unbound variable '{}'",
                    variable
                ))
            })?;
            let col = batch.column_by_name(property).ok_or_else(|| {
                OmniError::manifest(format!(
                    "column '{}' not found in binding '{}'",
                    property, variable
                ))
            })?;
            Ok((format!("{}.{}", variable, property), col.clone()))
        }
        IRExpr::Literal(lit) => {
            // Get row count from first binding
            let num_rows = bindings.values().next().map(|b| b.num_rows()).unwrap_or(0);
            let arr = literal_to_array(lit, num_rows)?;
            Ok(("literal".to_string(), arr))
        }
        IRExpr::Param(name) => {
            let lit = params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?;
            let num_rows = bindings.values().next().map(|b| b.num_rows()).unwrap_or(0);
            let arr = literal_to_array(lit, num_rows)?;
            Ok((name.clone(), arr))
        }
        _ => Err(OmniError::manifest(format!(
            "unsupported projection expression: {:?}",
            expr
        ))),
    }
}

/// Apply ordering to a batch.
pub(super) fn apply_ordering(
    batch: RecordBatch,
    orderings: &[IROrdering],
    bindings: &HashMap<String, RecordBatch>,
    _params: &ParamMap,
) -> Result<RecordBatch> {
    use arrow_ord::sort::{SortColumn, lexsort_to_indices};

    let mut sort_columns = Vec::with_capacity(orderings.len());

    for ordering in orderings {
        let col = match &ordering.expr {
            IRExpr::PropAccess { variable, property } => {
                let binding = bindings.get(variable).ok_or_else(|| {
                    OmniError::manifest(format!(
                        "ordering references unbound variable '{}'",
                        variable
                    ))
                })?;
                binding
                    .column_by_name(property)
                    .ok_or_else(|| {
                        OmniError::manifest(format!("column '{}' not found for ordering", property))
                    })?
                    .clone()
            }
            IRExpr::AliasRef(alias) => {
                // Look up in the projected batch by column name
                batch
                    .column_by_name(alias)
                    .ok_or_else(|| {
                        OmniError::manifest(format!("alias '{}' not found for ordering", alias))
                    })?
                    .clone()
            }
            _ => {
                return Err(OmniError::manifest(
                    "unsupported ordering expression".to_string(),
                ));
            }
        };

        sort_columns.push(SortColumn {
            values: col,
            options: Some(arrow_schema::SortOptions {
                descending: ordering.descending,
                nulls_first: !ordering.descending,
            }),
        });
    }

    let indices =
        lexsort_to_indices(&sort_columns, None).map_err(|e| OmniError::Lance(e.to_string()))?;

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow_select::take::take(col.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    RecordBatch::try_new(batch.schema(), columns).map_err(|e| OmniError::Lance(e.to_string()))
}
