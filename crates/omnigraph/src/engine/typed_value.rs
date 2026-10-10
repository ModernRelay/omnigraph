use super::*;

use std::collections::hash_map::Entry;

use arrow_array::FixedSizeListArray;
use datafusion::scalar::ScalarValue;
use omnigraph_compiler::ir::IRParam;
use omnigraph_compiler::types::ExprType;

/// Fill omitted nullable declarations and refuse omitted required ones.
/// Bound values, including the invocation's sampled clock, stay unchanged.
pub(crate) fn fill_declared_params(params: &mut ParamMap, declared: &[IRParam]) -> Result<()> {
    for param in declared {
        if let Entry::Vacant(entry) = params.entry(param.declaration.name.clone()) {
            if matches!(param.ty, ExprType::Value { nullable: true, .. }) {
                entry.insert(Literal::Null);
            } else {
                return Err(OmniError::manifest(format!(
                    "parameter '{}' not provided",
                    param.declaration.name
                )));
            }
        }
    }
    Ok(())
}

/// Validate every declaration at binding, including parameters the plan omits.
/// The caller owns defaults and the single sampling of `now()`.
pub(crate) fn validate_params(params: &ParamMap, declared: &[IRParam]) -> Result<()> {
    fn dates(literal: &Literal) -> Result<()> {
        match literal {
            Literal::Date(value) => {
                omnigraph_compiler::check_date_literal(value).map_err(OmniError::manifest)
            }
            Literal::DateTime(value) => {
                omnigraph_compiler::check_datetime_literal(value).map_err(OmniError::manifest)
            }
            Literal::List(items) => items.iter().try_for_each(dates),
            _ => Ok(()),
        }
    }
    for (name, value) in params {
        dates(value).map_err(|error| OmniError::manifest(format!("param '{name}': {error}")))?;
    }
    for param in declared {
        let Some(value) = params.get(&param.declaration.name) else {
            continue;
        };
        validate_parameter_literal(value, &param.ty, &param.declaration.type_name).map_err(
            |error| OmniError::manifest(format!("param '{}': {error}", param.declaration.name)),
        )?;
    }

    Ok(())
}

fn validate_parameter_literal(value: &Literal, ty: &ExprType, spelling: &str) -> Result<()> {
    if let ExprType::Value {
        scalar,
        list,
        nullable,
    } = ty
    {
        if matches!(scalar, ScalarType::Blob) && !list {
            return if matches!(value, Literal::String(_))
                || *nullable && matches!(value, Literal::Null)
            {
                Ok(())
            } else {
                Err(OmniError::manifest(format!(
                    "expected blob URI string, got {value:?}"
                )))
            };
        }
        if matches!(scalar, ScalarType::Vector(_))
            && let Literal::List(items) = value
            && items
                .iter()
                .any(|item| !matches!(item, Literal::Integer(_) | Literal::Float(_)))
        {
            return Err(OmniError::manifest("vector element is not numeric"));
        }
        let scalar_matches = |item: &Literal| match item {
            Literal::Date(_) => matches!(scalar, ScalarType::Date),
            Literal::DateTime(_) => matches!(scalar, ScalarType::DateTime),
            Literal::Null => *nullable,
            _ => false,
        };
        let date_shape = if matches!(scalar, ScalarType::Date | ScalarType::DateTime) {
            match value {
                Literal::List(items) if *list => items.iter().all(|item| {
                    scalar_matches(item)
                        && !(matches!(scalar, ScalarType::DateTime)
                            && matches!(item, Literal::Null))
                }),
                Literal::Null => *nullable,
                _ => !list && scalar_matches(value),
            }
        } else {
            !matches!(value, Literal::Null) || *nullable
        };
        if !date_shape {
            return Err(OmniError::manifest(format!(
                "expected {spelling}, got {value:?}"
            )));
        }
    }
    typed_literal_to_array(value, ty, 0).map(|_| ())
}

/// Build values in the type stored by the compiler, never from the payload's
/// numeric width or the first element of a list.
pub(super) fn typed_literal_to_array(
    literal: &Literal,
    ty: &ExprType,
    rows: usize,
) -> Result<ArrayRef> {
    let ExprType::Value {
        scalar,
        list,
        nullable,
    } = ty
    else {
        return Err(OmniError::manifest_internal("literal carries a node type"));
    };
    let data_type = ty
        .to_arrow()
        .ok_or_else(|| OmniError::manifest_internal("value has no Arrow type"))?;
    if matches!(literal, Literal::Null) {
        if !nullable {
            return Err(OmniError::manifest(format!(
                "expected {}, got null",
                ty.spelling()
            )));
        }
        return Ok(arrow_array::new_null_array(&data_type, rows));
    }
    if *list || matches!(scalar, ScalarType::Vector(_)) {
        let Literal::List(items) = literal else {
            return Err(OmniError::manifest(format!(
                "expected {}, got {literal:?}",
                ty.spelling()
            )));
        };
        let child_scalar = if matches!(scalar, ScalarType::Vector(_)) {
            ScalarType::F32
        } else {
            *scalar
        };
        let values = items
            .iter()
            .map(|item| typed_scalar(item, child_scalar))
            .collect::<Result<Vec<_>>>()?;
        let value = match scalar {
            ScalarType::Vector(dim) => {
                let size = i32::try_from(*dim)
                    .map_err(|_| OmniError::manifest("invalid vector dimension"))?;
                if size <= 0 || items.len() != *dim as usize {
                    return Err(OmniError::manifest(format!(
                        "expected Vector({dim}), got {} elements",
                        items.len()
                    )));
                }
                let child = ScalarValue::iter_to_array(values).map_err(OmniError::datafusion)?;
                ScalarValue::FixedSizeList(Arc::new(
                    FixedSizeListArray::try_new(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        size,
                        child,
                        None,
                    )
                    .map_err(OmniError::arrow_internal)?,
                ))
            }
            _ => ScalarValue::List(ScalarValue::new_list(
                &values,
                &child_scalar.to_arrow(),
                true,
            )),
        };
        return value.to_array_of_size(rows).map_err(OmniError::datafusion);
    }
    typed_scalar(literal, *scalar)?
        .to_array_of_size(rows)
        .map_err(OmniError::datafusion)
}

fn typed_scalar(literal: &Literal, scalar: ScalarType) -> Result<ScalarValue> {
    if matches!(literal, Literal::Null) {
        return ScalarValue::try_from(&scalar.to_arrow()).map_err(OmniError::datafusion);
    }
    let mismatch = || OmniError::manifest(format!("expected {scalar}, got {literal:?}"));
    let out_of_range = || OmniError::manifest(format!("value {literal:?} exceeds {scalar} range"));
    let numeric = || match literal {
        Literal::Integer(value) => Ok(*value as f64),
        Literal::Float(value) if value.is_finite() => Ok(*value),
        Literal::Float(_) => Err(out_of_range()),
        _ => Err(mismatch()),
    };
    let integer = |min: f64, upper: f64| {
        let value = numeric()?;
        if value.fract() != 0.0 || value < min || value >= upper {
            return Err(out_of_range());
        }
        Ok(value)
    };
    Ok(match scalar {
        ScalarType::String => match literal {
            Literal::String(value) => ScalarValue::Utf8(Some(value.clone())),
            _ => return Err(mismatch()),
        },
        ScalarType::Bool => match literal {
            Literal::Bool(value) => ScalarValue::Boolean(Some(*value)),
            _ => return Err(mismatch()),
        },
        ScalarType::I32 => ScalarValue::Int32(Some(match literal {
            Literal::Integer(value) => i32::try_from(*value).map_err(|_| out_of_range())?,
            _ => integer(-2_f64.powi(31), 2_f64.powi(31))? as i32,
        })),
        ScalarType::I64 => ScalarValue::Int64(Some(match literal {
            Literal::Integer(value) => *value,
            _ => integer(-2_f64.powi(63), 2_f64.powi(63))? as i64,
        })),
        ScalarType::U32 => ScalarValue::UInt32(Some(match literal {
            Literal::Integer(value) => u32::try_from(*value).map_err(|_| out_of_range())?,
            _ => integer(0.0, 2_f64.powi(32))? as u32,
        })),
        ScalarType::U64 => ScalarValue::UInt64(Some(match literal {
            Literal::Integer(value) => u64::try_from(*value).map_err(|_| out_of_range())?,
            _ => integer(0.0, 2_f64.powi(64))? as u64,
        })),
        ScalarType::F32 => {
            let value = numeric()? as f32;
            if !value.is_finite() {
                return Err(out_of_range());
            }
            ScalarValue::Float32(Some(value))
        }
        ScalarType::F64 => ScalarValue::Float64(Some(numeric()?)),
        ScalarType::Date => match literal {
            Literal::Date(value) => {
                ScalarValue::Date32(Some(crate::loader::parse_date32_literal(value)?))
            }
            _ => return Err(mismatch()),
        },
        ScalarType::DateTime => match literal {
            Literal::DateTime(value) => {
                omnigraph_compiler::check_datetime_literal(value).map_err(OmniError::manifest)?;
                ScalarValue::Date64(Some(crate::loader::parse_date64_literal(value)?))
            }
            _ => return Err(mismatch()),
        },
        ScalarType::Vector(_) | ScalarType::Blob => return Err(mismatch()),
    })
}

/// Execute only a compiler-recorded cast, refusing failed scalar or list elements.
pub(super) fn cast_array(array: &ArrayRef, source: &IRExpr, target: &ExprType) -> Result<ArrayRef> {
    if !omnigraph_compiler::ir::coerce::cast_expr_allowed(source, target) {
        return Err(OmniError::manifest_internal(format!(
            "invalid recorded cast from {} to {}",
            source.ty().spelling(),
            target.spelling()
        )));
    }
    cast_recorded_array(array, source.ty(), target)
}

/// A block aggregate/count is not a literal; its recorded casts only widen.
pub(super) fn cast_block_array(
    array: &ArrayRef,
    source: &ExprType,
    target: &ExprType,
) -> Result<ArrayRef> {
    if source == target || !omnigraph_compiler::ir::coerce::cast_allowed(source, target) {
        return Err(OmniError::manifest_internal("invalid recorded block cast"));
    }
    cast_recorded_array(array, source, target)
}

fn cast_recorded_array(array: &ArrayRef, source: &ExprType, target: &ExprType) -> Result<ArrayRef> {
    check_array_type(array, source, "cast input")?;
    let data_type = target
        .to_arrow()
        .ok_or_else(|| OmniError::manifest_internal("cast target has no Arrow type"))?;
    let casted = arrow_cast::cast::cast_with_options(
        array.as_ref(),
        &data_type,
        &arrow_cast::cast::CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .map_err(OmniError::arrow_internal)?;
    check_cast_values(array, &casted)?;
    check_array_type(&casted, target, "cast result")?;
    Ok(casted)
}

fn check_cast_values(before: &ArrayRef, after: &ArrayRef) -> Result<()> {
    if before.len() != after.len()
        || (0..before.len()).any(|row| before.is_null(row) != after.is_null(row))
    {
        return Err(OmniError::manifest_internal("cast changed value validity"));
    }
    if let Some(before) = before.as_any().downcast_ref::<ListArray>() {
        let after = after
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| OmniError::manifest_internal("list cast changed its shape"))?;
        check_cast_values(before.values(), after.values())?;
    }
    let finite = match after.data_type() {
        DataType::Float32 => after
            .as_any()
            .downcast_ref::<Float32Array>()
            .is_some_and(|array| array.iter().flatten().all(f32::is_finite)),
        DataType::Float64 => after
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .is_some_and(|array| array.iter().flatten().all(f64::is_finite)),
        _ => true,
    };
    if !finite {
        return Err(OmniError::manifest(
            "cast result exceeds the finite numeric range",
        ));
    }
    Ok(())
}

pub(super) fn check_array_type(array: &ArrayRef, ty: &ExprType, name: &str) -> Result<()> {
    let expected = ty
        .to_arrow()
        .ok_or_else(|| OmniError::manifest_internal("value has no Arrow type"))?;
    if array.data_type() != &expected || !ty.nullable() && array.null_count() != 0 {
        return Err(OmniError::manifest_internal(format!(
            "column '{name}' violates compiler type {}: Arrow {:?}, {} nulls",
            ty.spelling(),
            array.data_type(),
            array.null_count()
        )));
    }
    Ok(())
}

pub(super) fn check_output_schema(actual: &Schema, declared: &Schema, hidden: bool) -> Result<()> {
    let visible = actual
        .fields()
        .iter()
        .filter(|field| {
            !hidden
                || !(field.name().starts_with('~')
                    || field
                        .name()
                        .starts_with(omnigraph_planner::ROW_ADDRESS_PREFIX))
        })
        .count();
    if visible != declared.fields().len() {
        return Err(OmniError::manifest_internal(
            "return operator column count differs from declared schema",
        ));
    }
    for expected in declared.fields() {
        let actual = actual
            .field_with_name(expected.name())
            .map_err(OmniError::arrow_internal)?;
        if actual.data_type() != expected.data_type() {
            return Err(OmniError::manifest_internal(format!(
                "return column {} has {:?}, declared {:?}",
                expected.name(),
                actual.data_type(),
                expected.data_type()
            )));
        }
    }
    Ok(())
}

/// Check the schema of the dataset that was actually opened at the pinned
/// version, for every consumed physical column. A historical dataset may omit
/// unrelated newer declarations. Consumed fields must match type and nullability.
pub(super) fn check_stored_schema<'a>(
    dataset: &Dataset,
    declared: &Schema,
    table: &str,
    consumed: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let stored = Schema::from(dataset.schema());
    check_stored_fields(&stored, declared, table, consumed)
}

fn check_stored_fields<'a>(
    stored: &Schema,
    declared: &Schema,
    table: &str,
    consumed: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    for name in consumed {
        let expected = declared.field_with_name(name).map_err(|_| {
            OmniError::manifest_internal(format!("consumed {table}.{name} has no declared field"))
        })?;
        let actual = stored.field_with_name(expected.name()).map_err(|_| {
            OmniError::manifest_internal(format!(
                "stored {table} has no declared field '{}'",
                expected.name()
            ))
        })?;
        if actual.data_type() != expected.data_type()
            || actual.is_nullable() != expected.is_nullable()
        {
            return Err(OmniError::manifest_internal(format!(
                "stored {table}.{} has {:?}, nullable {}; declared {:?}, nullable {}",
                expected.name(),
                actual.data_type(),
                actual.is_nullable(),
                expected.data_type(),
                expected.is_nullable()
            )));
        }
    }
    Ok(())
}

/// A pushed filter consumes physical columns directly, so validate its stored
/// leaf declarations against the opened dataset before pushdown can hide them.
pub(super) fn check_scan_leaves(dataset: &Dataset, filters: &[IRExpr]) -> Result<()> {
    let stored = Schema::from(dataset.schema());
    for filter in filters {
        let mut result = Ok(());
        filter.visit_leaves(|leaf| {
            if result.is_err() {
                return;
            }
            let IRExpr::PropAccess { property, ty, .. } = leaf else {
                return;
            };
            result = (|| {
                let actual = stored
                    .field_with_name(property)
                    .map_err(OmniError::arrow_internal)?;
                let ExprType::Value { nullable, .. } = ty else {
                    return Err(OmniError::manifest_internal(
                        "pushed property carries a node type",
                    ));
                };
                if Some(actual.data_type()) != ty.to_arrow().as_ref()
                    || actual.is_nullable() != *nullable
                {
                    return Err(OmniError::manifest_internal(format!(
                        "pushed property {property} disagrees with opened dataset field"
                    )));
                }
                Ok(())
            })();
        });
        result?;
    }
    Ok(())
}

/// Replayed values must fit every stored leaf that consumes them, even when a
/// pushed predicate or an empty scan would otherwise avoid array construction.
pub(super) fn validate_plan_values(
    plan: &PhysicalPlan,
    params: &ParamMap,
    catalog: &Catalog,
) -> Result<()> {
    fn predicates<'a>(predicate: &'a omnigraph_planner::Predicate, out: &mut Vec<&'a IRExpr>) {
        use omnigraph_planner::Predicate;
        let mut predicates = vec![predicate];
        while let Some(predicate) = predicates.pop() {
            match predicate {
                Predicate::Gq {
                    reads: _,
                    text: _,
                    filter,
                } => out.push(&filter.0),
                Predicate::And { left, right } => {
                    predicates.extend([left.as_ref(), right.as_ref()])
                }
                Predicate::IdAfter { id: _ } | Predicate::VersionWindow { from: _, to: _ } => {}
            }
        }
    }
    /// Sibling subqueries may reuse a binding name for different types. Each
    /// inner pipeline inherits its outer owners but keeps local declarations
    /// separate; its bindings never escape into the enclosing pipeline.
    #[derive(Clone, Default)]
    struct Owners<'a> {
        bindings: HashMap<&'a str, &'a str>,
        edge_bindings: HashMap<&'a str, &'a [omnigraph_compiler::traversal::EdgeMember]>,
        aliases: HashMap<&'a str, &'a ExprType>,
        /// The score columns ranked scans and fusions append, by their
        /// `(binding, column)`: `F32` from Lance, `F64` for the fused sum.
        scores: HashMap<(&'a str, &'a str), DataType>,
    }
    let mut pipelines = vec![(plan.root(), Owners::default(), Vec::new())];
    while let Some((root, mut owners, mut pending)) = pipelines.pop() {
        let mut nodes = Vec::new();
        let mut inners = Vec::new();
        let mut work = vec![root];
        let mut seen = HashSet::new();
        while let Some(id) = work.pop() {
            if !seen.insert(id) {
                continue;
            }
            let node = plan.node(id).ok_or_else(|| {
                OmniError::manifest_internal("type validation reached a missing plan node")
            })?;
            nodes.push(node);
            if let PhysicalNode::AntiJoin {
                input,
                inner,
                predicate,
                ..
            } = node
            {
                predicate.check_types()?;
                work.push(*input);
                inners.push((*inner, predicate));
                pending.push(&predicate.right);
            } else {
                work.extend(node.inputs());
            }
            match node {
                PhysicalNode::Scan { spec, ranked, .. } => {
                    if let (Some(binding), Some(type_name)) =
                        (&spec.binding, spec.table.node_type_name())
                    {
                        owners.bindings.insert(binding.as_str(), type_name);
                    }
                    if let Some(ranked) = ranked {
                        let valid = matches!(&ranked.score,
                        IRExpr::PropAccess { variable, property, ty: ExprType::Value { scalar: ScalarType::F32, list: false, nullable: false } }
                        if Some(variable) == spec.binding.as_ref() && property == ranked.kind.score().0);
                        if !valid {
                            return Err(OmniError::manifest_internal(
                                "ranked score leaf disagrees with its scan producer",
                            ));
                        }
                        if let Some(binding) = &spec.binding {
                            owners.scores.insert(
                                (binding.as_str(), ranked.kind.score().0),
                                DataType::Float32,
                            );
                        }
                    }
                }
                PhysicalNode::RankFuse { arms, .. } => {
                    owners.scores.insert(
                        (
                            arms[0].binding.as_str(),
                            omnigraph_compiler::query::ast::RRF_COLUMN,
                        ),
                        DataType::Float64,
                    );
                }
                PhysicalNode::MetadataCount { spec, return_exprs } => {
                    if let (Some(binding), Some(type_name)) =
                        (&spec.binding, spec.table.node_type_name())
                    {
                        owners.bindings.insert(binding.as_str(), type_name);
                    }
                    for projection in return_exprs {
                        owners
                            .aliases
                            .insert(projection.column.as_str(), &projection.ty);
                    }
                }
                PhysicalNode::Expand {
                    src,
                    dst,
                    src_type,
                    dst_type,
                    edge_binding,
                    edges,
                    ..
                } => {
                    owners.bindings.insert(src.as_str(), src_type.as_str());
                    owners.bindings.insert(dst.as_str(), dst_type.as_str());
                    if let Some(binding) = edge_binding {
                        owners
                            .edge_bindings
                            .insert(binding.as_str(), edges.members());
                    }
                }
                PhysicalNode::Projection { return_exprs, .. }
                | PhysicalNode::Aggregate { return_exprs, .. } => {
                    for projection in return_exprs {
                        owners
                            .aliases
                            .insert(projection.column.as_str(), &projection.ty);
                    }
                }
                _ => {}
            }
        }
        for (inner, predicate) in inners {
            let mut expressions = Vec::new();
            expressions.extend(predicate.left.arg());
            pipelines.push((inner, owners.clone(), expressions));
        }
        let Owners {
            bindings,
            edge_bindings,
            aliases,
            scores,
        } = owners;
        let property_type = |variable: &str, property: &str| -> Result<(DataType, bool)> {
            if let Some(data_type) = scores.get(&(variable, property)) {
                return Ok((data_type.clone(), false));
            }
            if let Some(type_name) = bindings.get(variable) {
                let node = catalog.node_types.get(*type_name).ok_or_else(|| {
                    OmniError::manifest_internal(format!("unknown node type {type_name}"))
                })?;
                let field = node
                    .arrow_schema
                    .field_with_name(property)
                    .map_err(OmniError::arrow_internal)?;
                return Ok((field.data_type().clone(), field.is_nullable()));
            }
            if let Some(members) = edge_bindings.get(variable) {
                if property == omnigraph_compiler::traversal::EDGE_TYPE_COLUMN
                    || members.is_empty()
                        && [
                            catalog.system_columns.id,
                            catalog.system_columns.src,
                            catalog.system_columns.dst,
                        ]
                        .contains(&property)
                {
                    return Ok((DataType::Utf8, false));
                }
                let mut common: Option<(DataType, bool)> = None;
                for member in *members {
                    let edge = catalog.edge_types.get(&member.edge_type).ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "unknown edge type {}",
                            member.edge_type
                        ))
                    })?;
                    let field = edge
                        .arrow_schema
                        .field_with_name(property)
                        .map_err(OmniError::arrow_internal)?;
                    match &mut common {
                        Some((data_type, nullable)) => {
                            if data_type != field.data_type() {
                                return Err(OmniError::manifest_internal(
                                    "selected edge property types disagree",
                                ));
                            }
                            *nullable |= field.is_nullable();
                        }
                        None => common = Some((field.data_type().clone(), field.is_nullable())),
                    }
                }
                return common.ok_or_else(|| {
                    OmniError::manifest_internal("property has no selected edge owner")
                });
            }
            Err(OmniError::manifest_internal(format!(
                "property {variable}.{property} has no binding owner"
            )))
        };
        let mut pushed_filters = Vec::new();
        for node in nodes {
            match node {
                PhysicalNode::Scan { spec, ranked, .. } => {
                    if let Some(filter) = &spec.filter {
                        predicates(filter, &mut pushed_filters);
                        predicates(filter, &mut pending);
                    }
                    if let Some(ranked) = ranked {
                        pending.extend([&ranked.query, &ranked.score]);
                    }
                }
                PhysicalNode::MetadataCount { spec, return_exprs } => {
                    if let Some(filter) = &spec.filter {
                        predicates(filter, &mut pending);
                    }
                    pending.extend(return_exprs.iter().map(|projection| &projection.expr));
                }
                PhysicalNode::Projection { return_exprs, .. }
                | PhysicalNode::Aggregate { return_exprs, .. } => {
                    pending.extend(return_exprs.iter().map(|projection| &projection.expr))
                }
                PhysicalNode::CrossJoin { filters, .. } | PhysicalNode::Filter { filters, .. } => {
                    pending.extend(filters)
                }
                PhysicalNode::ContainsJoin {
                    conjunct, residual, ..
                } => {
                    pending.push(conjunct);
                    pending.extend(residual);
                }
                PhysicalNode::AntiJoin { .. } => {}
                PhysicalNode::RankFuse { k, .. } => pending.extend(k),
                PhysicalNode::Sort { order_by, .. } => {
                    pending.extend(order_by.iter().map(|ordering| &ordering.expr))
                }
                PhysicalNode::SortMergeJoin { .. }
                | PhysicalNode::HashJoin { .. }
                | PhysicalNode::HydrateByAddress { .. }
                | PhysicalNode::HydrateColumns { .. }
                | PhysicalNode::RowCompare { .. }
                | PhysicalNode::ClassifyThreeWay { .. }
                | PhysicalNode::Limit { .. }
                | PhysicalNode::Page { .. }
                | PhysicalNode::Expand { .. }
                | PhysicalNode::OuterReference { .. } => {}
            }
        }
        for expr in &pending {
            expr.check_types()?;
        }
        while let Some(expr) = pending.pop() {
            match expr {
                IRExpr::Literal(value, ty) => {
                    typed_literal_to_array(value, ty, 0)?;
                }
                IRExpr::Param(name, ty) => {
                    let value = params.get(name).ok_or_else(|| {
                        OmniError::manifest(format!("parameter '{name}' not provided"))
                    })?;
                    validate_parameter_literal(value, ty, ty.spelling().trim_end_matches('?'))
                        .map_err(|error| OmniError::manifest(format!("param '{name}': {error}")))?;
                }
                IRExpr::PropAccess {
                    variable,
                    property,
                    ty,
                } => {
                    let (data_type, nullable) = property_type(variable, property)?;
                    if ty.to_arrow().as_ref() != Some(&data_type)
                        || !matches!(ty, ExprType::Value { nullable: stored, .. } if *stored == nullable)
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "property {variable}.{property} leaf type {} disagrees with its catalog owner ({data_type}, nullable={nullable})",
                            ty.spelling()
                        )));
                    }
                }
                IRExpr::Variable(variable, ExprType::Node { type_name }) => {
                    if bindings.get(variable.as_str()).copied() != Some(type_name.as_str()) {
                        return Err(OmniError::manifest_internal(format!(
                            "node leaf {variable} type {type_name} disagrees with its binding owner"
                        )));
                    }
                }
                IRExpr::Variable(_, _) => {
                    return Err(OmniError::manifest_internal(
                        "node leaf carries a value type",
                    ));
                }
                IRExpr::AliasRef(alias, ty) => {
                    if aliases.get(alias.as_str()).copied() != Some(ty) {
                        return Err(OmniError::manifest_internal(format!(
                            "alias leaf {alias} type {} disagrees with its return owner",
                            ty.spelling()
                        )));
                    }
                }
                IRExpr::Nearest {
                    variable: _,
                    property: _,
                    query,
                    ty: _,
                } => pending.push(query),
                IRExpr::Search {
                    field,
                    query,
                    ty: _,
                }
                | IRExpr::MatchText {
                    field,
                    query,
                    ty: _,
                }
                | IRExpr::Bm25 {
                    field,
                    query,
                    ty: _,
                } => pending.extend([field.as_ref(), query.as_ref()]),
                IRExpr::Fuzzy {
                    field,
                    query,
                    max_edits,
                    ty: _,
                } => {
                    pending.extend([field.as_ref(), query.as_ref()]);
                    pending.extend(max_edits.as_deref());
                }
                IRExpr::Rrf {
                    primary,
                    secondary,
                    k,
                    ty: _,
                } => {
                    pending.extend([primary.as_ref(), secondary.as_ref()]);
                    pending.extend(k.as_deref());
                }
                IRExpr::Aggregate {
                    func: _,
                    arg,
                    signature: _,
                }
                | IRExpr::Not(arg, _)
                | IRExpr::Cast { expr: arg, ty: _ }
                | IRExpr::IsNull {
                    expr: arg,
                    negated: _,
                    ty: _,
                } => pending.push(arg),
                IRExpr::Binary {
                    left,
                    op: _,
                    right,
                    ty: _,
                } => pending.extend([left.as_ref(), right.as_ref()]),
            }
        }
        for filter in pushed_filters {
            if !is_search_filter(filter)
                && super::scan::ir_expr_to_df_expr(filter, params, None).is_none()
            {
                return Err(OmniError::manifest_internal(
                    "stored pushed filter cannot execute its recorded expression",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::query::ast::Param;
    use omnigraph_compiler::types::PropType;

    fn scalar(scalar: ScalarType, nullable: bool) -> ExprType {
        ExprType::from_prop(&PropType::scalar(scalar, nullable))
    }

    #[test]
    fn empty_arrays_and_nulls_keep_declared_widths() {
        for kind in [
            ScalarType::I32,
            ScalarType::U32,
            ScalarType::U64,
            ScalarType::F32,
            ScalarType::DateTime,
        ] {
            let ty = scalar(kind, true);
            for rows in [0, 3] {
                let array = typed_literal_to_array(&Literal::Null, &ty, rows).unwrap();
                assert_eq!(array.data_type(), &kind.to_arrow());
                assert_eq!(array.null_count(), rows);
            }
        }
        let ty = ExprType::from_prop(&PropType::list_of(ScalarType::F32, true));
        let values = Literal::List(vec![
            Literal::Integer(1),
            Literal::Null,
            Literal::Float(0.1),
        ]);
        let array = typed_literal_to_array(&values, &ty, 2).unwrap();
        assert_eq!(array.data_type(), &ty.to_arrow().unwrap());
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        let child = list.value(0);
        let child = child.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(child.value(0), 1.0);
        assert!(child.is_null(1));
        assert_eq!(child.value(2), 0.1_f32);
        assert!(typed_literal_to_array(&Literal::List(vec![Literal::Bool(true)]), &ty, 0).is_err());
    }

    #[test]
    fn direct_parameter_values_are_checked_even_without_a_consumer() {
        let invalid = [
            (ScalarType::I32, Literal::Integer(i64::from(i32::MAX) + 1)),
            (ScalarType::U32, Literal::Integer(-1)),
            (ScalarType::U64, Literal::Integer(-1)),
            (ScalarType::I64, Literal::Float(2_f64.powi(63))),
            (ScalarType::U64, Literal::Float(2_f64.powi(64))),
            (ScalarType::I32, Literal::Float(1.5)),
            (ScalarType::F32, Literal::Float(f64::MAX)),
            (ScalarType::F32, Literal::Float(f64::INFINITY)),
            (ScalarType::F64, Literal::Float(f64::NAN)),
            (ScalarType::F64, Literal::Float(f64::NEG_INFINITY)),
        ];
        for (kind, value) in invalid {
            for list in [false, true] {
                let prop = if list {
                    PropType::list_of(kind, false)
                } else {
                    PropType::scalar(kind, false)
                };
                let declaration = IRParam {
                    declaration: Param {
                        name: "unused".into(),
                        type_name: prop.display_name(),
                        nullable: false,
                    },
                    ty: ExprType::from_prop(&prop),
                };
                let value = if list {
                    Literal::List(vec![value.clone()])
                } else {
                    value.clone()
                };
                let params = ParamMap::from([("unused".into(), value)]);
                assert!(
                    validate_params(&params, &[declaration]).is_err(),
                    "{kind}, list={list}"
                );
            }
        }
        for (kind, value) in [
            (ScalarType::I64, Literal::Integer(i64::MAX)),
            (ScalarType::I64, Literal::Integer(i64::MIN)),
            (ScalarType::U32, Literal::Integer(i64::from(u32::MAX))),
            (ScalarType::F32, Literal::Float(f64::from(f32::MAX))),
        ] {
            assert!(typed_literal_to_array(&value, &scalar(kind, false), 0).is_ok());
        }
    }

    #[test]
    fn vector_parameter_children_are_numeric_even_without_a_consumer() {
        let value = Literal::List(vec![Literal::Float(1.0), Literal::Null]);
        for nullable in [false, true] {
            let declaration = IRParam {
                declaration: Param {
                    name: "unused".into(),
                    type_name: "Vector(2)".into(),
                    nullable,
                },
                ty: scalar(ScalarType::Vector(2), nullable),
            };
            let params = ParamMap::from([("unused".into(), value.clone())]);
            let fresh = validate_params(&params, std::slice::from_ref(&declaration)).unwrap_err();
            let replay =
                validate_parameter_literal(&value, &declaration.ty, "Vector(2)").unwrap_err();
            assert!(fresh.to_string().contains("vector element is not numeric"));
            assert!(replay.to_string().contains("vector element is not numeric"));
            let null = ParamMap::from([("unused".into(), Literal::Null)]);
            assert_eq!(validate_params(&null, &[declaration]).is_ok(), nullable);
        }
        let ty = ExprType::from_prop(&PropType::list_of(ScalarType::F32, false));
        assert!(validate_parameter_literal(&value, &ty, "[F32]").is_ok());
    }

    #[test]
    fn stored_datetime_literal_refuses_submillisecond_precision() {
        let value = Literal::DateTime("2025-01-01T00:00:00.0001Z".into());
        assert!(typed_literal_to_array(&value, &scalar(ScalarType::DateTime, false), 0).is_err());
    }

    #[test]
    fn unused_blob_declarations_validate_uri_payloads_without_read_arrays() {
        for nullable in [false, true] {
            let declaration = IRParam {
                declaration: Param {
                    name: "unused".into(),
                    type_name: "Blob".into(),
                    nullable: !nullable,
                },
                ty: scalar(ScalarType::Blob, nullable),
            };
            let uri = ParamMap::from([(
                "unused".into(),
                Literal::String("s3://bucket/object".into()),
            )]);
            assert!(validate_params(&uri, std::slice::from_ref(&declaration)).is_ok());
            let null = ParamMap::from([("unused".into(), Literal::Null)]);
            assert_eq!(
                validate_params(&null, std::slice::from_ref(&declaration)).is_ok(),
                nullable
            );
            let number = ParamMap::from([("unused".into(), Literal::Integer(1))]);
            assert!(validate_params(&number, &[declaration]).is_err());
        }
    }

    #[test]
    fn replay_parameter_temporal_shapes_match_fresh_binding() {
        let nulls = Literal::List(vec![Literal::Null]);
        for (kind, nullable, valid) in [
            (ScalarType::DateTime, false, false),
            (ScalarType::DateTime, true, false),
            (ScalarType::Date, false, false),
            (ScalarType::Date, true, true),
        ] {
            let ty = ExprType::from_prop(&PropType::list_of(kind, nullable));
            assert_eq!(
                validate_parameter_literal(&nulls, &ty, &ty.spelling()).is_ok(),
                valid
            );
            assert!(
                typed_literal_to_array(&nulls, &ty, 0).is_ok(),
                "literal-list null children have a separate language contract"
            );
        }
    }

    #[test]
    fn historical_schema_checks_only_consumed_fields_but_never_relaxes_them() {
        let declared = Schema::new(vec![
            Field::new("title", DataType::Utf8, false),
            Field::new("rating", DataType::Int32, true),
            Field::new("legacy", DataType::Int64, true),
        ]);
        let stored = Schema::new(vec![
            Field::new("title", DataType::Utf8, false),
            Field::new("legacy", DataType::Utf8, false),
        ]);
        assert!(check_stored_fields(&stored, &declared, "node:Doc", ["title"]).is_ok());
        for consumed in ["rating", "legacy", "unknown"] {
            assert!(check_stored_fields(&stored, &declared, "node:Doc", [consumed]).is_err());
        }
        let widened = Schema::new(vec![Field::new("title", DataType::Utf8, true)]);
        assert!(check_stored_fields(&widened, &declared, "node:Doc", ["title"]).is_err());
    }

    #[test]
    fn stored_schema_checks_nullability_while_derived_schema_checks_types() {
        let declared = Schema::new(vec![Field::new("amount", DataType::Int32, false)]);
        let widened = Schema::new(vec![Field::new("amount", DataType::Int32, true)]);
        let changed = Schema::new(vec![Field::new("amount", DataType::Int64, false)]);
        assert!(check_stored_fields(&declared, &declared, "node:Account", ["amount"]).is_ok());
        assert!(check_stored_fields(&widened, &declared, "node:Account", ["amount"]).is_err());
        assert!(check_stored_fields(&changed, &declared, "node:Account", ["amount"]).is_err());
        assert!(check_output_schema(&widened, &declared, false).is_ok());
        assert!(check_output_schema(&changed, &declared, false).is_err());
        let nulls: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![None]));
        assert!(check_array_type(&nulls, &scalar(ScalarType::I32, false), "amount").is_err());
    }
}
