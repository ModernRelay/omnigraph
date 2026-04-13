use super::*;

pub(super) fn apply_filter(
    batch: &mut RecordBatch,
    filter: &IRFilter,
    params: &ParamMap,
) -> Result<()> {
    let mask = evaluate_filter(batch, filter, params)?;
    let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    *batch = filtered;
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

/// Evaluate an IR expression against a wide batch, producing an array.
fn evaluate_expr(batch: &RecordBatch, expr: &IRExpr, params: &ParamMap) -> Result<ArrayRef> {
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
            "unsupported expression in filter: {:?}",
            expr
        ))),
    }
}

/// Create a constant array from a literal value.
fn literal_to_array(lit: &Literal, num_rows: usize) -> Result<ArrayRef> {
    Ok(match lit {
        Literal::Null => arrow_array::new_null_array(&DataType::Utf8, num_rows),
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

/// Project return expressions into a result batch.
pub(super) fn project_return(
    wide_batch: &RecordBatch,
    projections: &[IRProjection],
    params: &ParamMap,
) -> Result<RecordBatch> {
    if projections.is_empty() {
        return Err(OmniError::manifest(
            "query has no return projections".to_string(),
        ));
    }

    // Route to aggregate path if any projection contains an aggregate
    let has_aggregates = projections
        .iter()
        .any(|p| matches!(&p.expr, IRExpr::Aggregate { .. }));
    if has_aggregates {
        return aggregate_return(wide_batch, projections, params);
    }

    let mut fields = Vec::with_capacity(projections.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projections.len());

    for proj in projections {
        let (name, col) = evaluate_projection(wide_batch, &proj.expr, params)?;
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

/// Evaluate a single projection expression against a wide batch.
fn evaluate_projection(
    wide_batch: &RecordBatch,
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<(String, ArrayRef)> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let col_name = format!("{}.{}", variable, property);
            let col = wide_batch.column_by_name(&col_name).ok_or_else(|| {
                OmniError::manifest(format!(
                    "column '{}' not found in wide batch",
                    col_name
                ))
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
            let col_name = format!("{}.id", name);
            let col = wide_batch.column_by_name(&col_name).ok_or_else(|| {
                OmniError::manifest(format!("column '{}' not found in wide batch", col_name))
            })?;
            Ok((name.clone(), col.clone()))
        }
        _ => Err(OmniError::manifest(format!(
            "unsupported projection expression: {:?}",
            expr
        ))),
    }
}

/// Apply ordering to a batch. `source` is used to resolve PropAccess columns
/// (typically the wide batch, or the result batch itself for aggregates).
pub(super) fn apply_ordering(
    batch: RecordBatch,
    orderings: &[IROrdering],
    source: &RecordBatch,
    _params: &ParamMap,
) -> Result<RecordBatch> {
    use arrow_ord::sort::{SortColumn, lexsort_to_indices};

    let mut sort_columns = Vec::with_capacity(orderings.len());

    for ordering in orderings {
        let col = match &ordering.expr {
            IRExpr::PropAccess { variable, property } => {
                let col_name = format!("{}.{}", variable, property);
                source
                    .column_by_name(&col_name)
                    .ok_or_else(|| {
                        OmniError::manifest(format!("column '{}' not found for ordering", col_name))
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

// ─── Aggregate execution ───────────────────────────────────────────────────

/// Project return expressions that contain aggregates.
fn aggregate_return(
    wide: &RecordBatch,
    projections: &[IRProjection],
    params: &ParamMap,
) -> Result<RecordBatch> {
    let num_rows = wide.num_rows();

    struct GroupKey {
        proj_idx: usize,
        name: String,
        column: ArrayRef,
    }
    struct AggProj {
        proj_idx: usize,
        name: String,
        func: AggFunc,
        arg_column: ArrayRef,
    }

    let mut group_keys: Vec<GroupKey> = Vec::new();
    let mut agg_projs: Vec<AggProj> = Vec::new();

    for (i, proj) in projections.iter().enumerate() {
        match &proj.expr {
            IRExpr::Aggregate { func, arg } => {
                let (name, col) = evaluate_projection(wide, arg, params)?;
                let alias = proj.alias.as_deref().unwrap_or(&name);
                agg_projs.push(AggProj {
                    proj_idx: i,
                    name: alias.to_string(),
                    func: *func,
                    arg_column: col,
                });
            }
            _ => {
                let (name, col) = evaluate_projection(wide, &proj.expr, params)?;
                let alias = proj.alias.as_deref().unwrap_or(&name);
                group_keys.push(GroupKey {
                    proj_idx: i,
                    name: alias.to_string(),
                    column: col,
                });
            }
        }
    }

    // Handle empty input: return a single row with count=0, others=null
    if num_rows == 0 && group_keys.is_empty() {
        return build_empty_aggregate_result(projections);
    }

    // Build group assignments
    let mut group_map: HashMap<String, usize> = HashMap::new();
    let mut group_indices: Vec<Vec<usize>> = Vec::new();
    let group_cols: Vec<&ArrayRef> = group_keys.iter().map(|gk| &gk.column).collect();

    if group_keys.is_empty() {
        group_indices.push((0..num_rows).collect());
    } else {
        for row in 0..num_rows {
            let key = build_group_key(&group_cols, row);
            let group_idx = match group_map.get(&key) {
                Some(&idx) => idx,
                None => {
                    let idx = group_indices.len();
                    group_map.insert(key, idx);
                    group_indices.push(Vec::new());
                    idx
                }
            };
            group_indices[group_idx].push(row);
        }
    }

    let num_groups = group_indices.len();
    let mut result_columns: Vec<(usize, String, ArrayRef)> =
        Vec::with_capacity(projections.len());

    for gk in &group_keys {
        let first_row_indices: Vec<u32> =
            group_indices.iter().map(|rows| rows[0] as u32).collect();
        let take_idx = UInt32Array::from(first_row_indices);
        let col = arrow_select::take::take(gk.column.as_ref(), &take_idx, None)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        result_columns.push((gk.proj_idx, gk.name.clone(), col));
    }

    for ap in &agg_projs {
        let col = compute_aggregate(ap.func, &ap.arg_column, &group_indices, num_groups)?;
        result_columns.push((ap.proj_idx, ap.name.clone(), col));
    }

    result_columns.sort_by_key(|(idx, _, _)| *idx);

    let fields: Vec<Field> = result_columns
        .iter()
        .map(|(_, name, col)| Field::new(name, col.data_type().clone(), true))
        .collect();
    let columns: Vec<ArrayRef> = result_columns.into_iter().map(|(_, _, col)| col).collect();

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

/// Build a string key for grouping using length-prefixed encoding.
fn build_group_key(group_columns: &[&ArrayRef], row: usize) -> String {
    let mut key = String::new();
    for col in group_columns {
        if col.is_null(row) {
            key.push('N');
        } else {
            let val = arrow_cast::display::array_value_to_string(col, row).unwrap_or_default();
            key.push('L');
            key.push_str(&val.len().to_string());
            key.push(':');
            key.push_str(&val);
        }
    }
    key
}

fn compute_aggregate(
    func: AggFunc,
    arg: &ArrayRef,
    group_indices: &[Vec<usize>],
    num_groups: usize,
) -> Result<ArrayRef> {
    match func {
        AggFunc::Count => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for group in group_indices {
                let count = group.iter().filter(|&&i| !arg.is_null(i)).count();
                builder.append_value(count as i64);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        AggFunc::Sum => compute_sum(arg, group_indices, num_groups),
        AggFunc::Avg => compute_avg(arg, group_indices, num_groups),
        AggFunc::Min => compute_min_max(arg, group_indices, num_groups, true),
        AggFunc::Max => compute_min_max(arg, group_indices, num_groups, false),
    }
}

fn compute_sum(arg: &ArrayRef, group_indices: &[Vec<usize>], num_groups: usize) -> Result<ArrayRef> {
    macro_rules! sum_numeric {
        ($arr_type:ty, $arg:expr, $dt:expr) => {{
            let arr = $arg.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                OmniError::manifest(format!("sum: expected {:?}, got {:?}", $dt, $arg.data_type()))
            })?;
            let mut builder = Float64Builder::with_capacity(num_groups);
            for group in group_indices {
                let mut sum = None;
                for &i in group {
                    if !arr.is_null(i) {
                        *sum.get_or_insert(0.0f64) += arr.value(i) as f64;
                    }
                }
                match sum {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }};
    }
    match arg.data_type() {
        dt @ DataType::Int32 => sum_numeric!(Int32Array, arg, dt),
        dt @ DataType::Int64 => sum_numeric!(Int64Array, arg, dt),
        dt @ DataType::UInt32 => sum_numeric!(UInt32Array, arg, dt),
        dt @ DataType::UInt64 => sum_numeric!(UInt64Array, arg, dt),
        dt @ DataType::Float32 => sum_numeric!(Float32Array, arg, dt),
        dt @ DataType::Float64 => sum_numeric!(Float64Array, arg, dt),
        dt => Err(OmniError::manifest(format!("sum: unsupported type {:?}", dt))),
    }
}

fn compute_avg(arg: &ArrayRef, group_indices: &[Vec<usize>], num_groups: usize) -> Result<ArrayRef> {
    macro_rules! avg_typed {
        ($arr_type:ty, $arg:expr) => {{
            let arr = $arg.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                OmniError::manifest(format!("avg: expected {:?}, got {:?}", stringify!($arr_type), $arg.data_type()))
            })?;
            let mut builder = Float64Builder::with_capacity(num_groups);
            for group in group_indices {
                let mut sum = 0.0f64;
                let mut count = 0usize;
                for &i in group {
                    if !arr.is_null(i) { sum += arr.value(i) as f64; count += 1; }
                }
                if count > 0 { builder.append_value(sum / count as f64); } else { builder.append_null(); }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }};
    }
    match arg.data_type() {
        DataType::Int32 => avg_typed!(Int32Array, arg),
        DataType::Int64 => avg_typed!(Int64Array, arg),
        DataType::UInt32 => avg_typed!(UInt32Array, arg),
        DataType::UInt64 => avg_typed!(UInt64Array, arg),
        DataType::Float32 => avg_typed!(Float32Array, arg),
        DataType::Float64 => avg_typed!(Float64Array, arg),
        dt => Err(OmniError::manifest(format!("avg: unsupported type {:?}", dt))),
    }
}

fn compute_min_max(arg: &ArrayRef, group_indices: &[Vec<usize>], num_groups: usize, is_min: bool) -> Result<ArrayRef> {
    macro_rules! minmax_typed {
        ($arr_type:ty, $builder_type:ty, $arg:expr, $is_min:expr) => {{
            let arr = $arg.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                OmniError::manifest(format!("min/max: expected {:?}, got {:?}", stringify!($arr_type), $arg.data_type()))
            })?;
            let mut builder = <$builder_type>::with_capacity(num_groups);
            for group in group_indices {
                let mut result = None;
                for &i in group {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        result = Some(match result {
                            None => v,
                            Some(cur) => if $is_min { if v < cur { v } else { cur } } else { if v > cur { v } else { cur } },
                        });
                    }
                }
                match result { Some(v) => builder.append_value(v), None => builder.append_null() }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }};
    }
    match arg.data_type() {
        DataType::Int32 => minmax_typed!(Int32Array, Int32Builder, arg, is_min),
        DataType::Int64 => minmax_typed!(Int64Array, Int64Builder, arg, is_min),
        DataType::UInt32 => minmax_typed!(UInt32Array, UInt32Builder, arg, is_min),
        DataType::UInt64 => minmax_typed!(UInt64Array, UInt64Builder, arg, is_min),
        DataType::Float32 => minmax_typed!(Float32Array, Float32Builder, arg, is_min),
        DataType::Float64 => minmax_typed!(Float64Array, Float64Builder, arg, is_min),
        DataType::Utf8 => {
            let arr = arg.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                OmniError::manifest(format!("min/max: expected Utf8, got {:?}", arg.data_type()))
            })?;
            let mut builder = StringBuilder::with_capacity(num_groups, num_groups * 16);
            for group in group_indices {
                let mut result: Option<&str> = None;
                for &i in group {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        result = Some(match result {
                            None => v,
                            Some(cur) => if is_min { if v < cur { v } else { cur } } else { if v > cur { v } else { cur } },
                        });
                    }
                }
                match result { Some(v) => builder.append_value(v), None => builder.append_null() }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        dt => Err(OmniError::manifest(format!("min/max: unsupported type {:?}", dt))),
    }
}

/// Build a single-row result for an aggregate query with zero input rows and no group keys.
fn build_empty_aggregate_result(projections: &[IRProjection]) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(projections.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projections.len());

    for proj in projections {
        let name = proj.alias.as_deref().unwrap_or("?");
        match &proj.expr {
            IRExpr::Aggregate { func, .. } => match func {
                AggFunc::Count => {
                    fields.push(Field::new(name, DataType::Int64, true));
                    columns.push(Arc::new(Int64Array::from(vec![0i64])) as ArrayRef);
                }
                _ => {
                    fields.push(Field::new(name, DataType::Float64, true));
                    columns.push(Arc::new(Float64Array::from(vec![None as Option<f64>])) as ArrayRef);
                }
            },
            _ => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![None as Option<&str>])) as ArrayRef);
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}
