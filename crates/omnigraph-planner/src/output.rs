//! Declared result columns and the named node objects they contain.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use omnigraph_compiler::ir::IRProjection;
use omnigraph_compiler::types::{ExprType, ScalarType};

use crate::{PhysicalNode, PhysicalPlan, PlanError, PlanSource};

/// A named node object's complete member type, captured from the compiler
/// catalog when a result node is planned. Saved plans retain the declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeObjectType {
    pub type_name: String,
    pub fields: Fields,
}

pub(crate) fn node_object_types(
    returns: &[IRProjection],
    source: &dyn PlanSource,
) -> Result<Vec<NodeObjectType>, PlanError> {
    let names: BTreeSet<&str> = returns
        .iter()
        .filter_map(|projection| match &projection.ty {
            ExprType::Node { type_name } => Some(type_name.as_str()),
            ExprType::Value { .. } | ExprType::ExactInteger { .. } => None,
        })
        .collect();
    names
        .into_iter()
        .map(|type_name| {
            Ok(NodeObjectType {
                type_name: type_name.to_string(),
                fields: source.node_type(type_name)?.object_fields,
            })
        })
        .collect()
}

/// The Arrow image of stored return types, using only declarations carried by
/// their result node. A replay never needs the current catalog to validate it.
pub(crate) fn return_schema(
    returns: &[IRProjection],
    node_objects: &[NodeObjectType],
) -> Result<SchemaRef, PlanError> {
    let mut objects = BTreeMap::new();
    for object in node_objects {
        if objects
            .insert(object.type_name.as_str(), &object.fields)
            .is_some()
        {
            return Err(PlanError::Internal(format!(
                "duplicate node object declaration `{}`",
                object.type_name
            )));
        }
    }
    let mut used = BTreeSet::new();
    let mut names = BTreeSet::new();
    let fields = returns.iter().map(|projection| {
        if projection.column.is_empty() || !names.insert(projection.column.as_str()) {
            return Err(PlanError::Internal("result column names must be nonempty and unique".into()));
        }
        if projection.expr.ty() != &projection.ty {
            return Err(PlanError::Internal(format!("result column `{}` differs from its expression type", projection.column)));
        }
        let (data_type, nullable) = match &projection.ty {
            ExprType::Value { scalar, nullable, .. } => {
                if matches!(scalar, ScalarType::Blob)
                    || matches!(scalar, ScalarType::Vector(n) if *n == 0 || *n > i32::MAX as u32)
                {
                    return Err(PlanError::Internal(format!("invalid result type {}", projection.ty.spelling())));
                }
                (projection.ty.to_arrow().ok_or_else(|| PlanError::Internal("value has no Arrow type".into()))?, *nullable)
            }
            ExprType::ExactInteger { .. } => {
                return Err(PlanError::Internal("exact_integer is not a public result type".into()));
            }
            ExprType::Node { type_name } => {
                let fields = objects.get(type_name.as_str()).ok_or_else(|| {
                    PlanError::Internal(format!("missing node object declaration `{type_name}`"))
                })?;
                used.insert(type_name.as_str());
                (DataType::Struct((*fields).clone()), false)
            }
        };
        Ok(Field::new(&projection.column, data_type, nullable))
    }).collect::<Result<Vec<_>, PlanError>>()?;
    if used.len() != objects.len() {
        return Err(PlanError::Internal(
            "unused node object declaration in result node".into(),
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn return_columns(returns: &[IRProjection]) -> Vec<String> {
    returns
        .iter()
        .map(|projection| format!("{}: {}", projection.column, projection.ty.spelling()))
        .collect()
}

/// Check result nodes and their Sort/Limit wrappers before planning returns
/// and again at execution, including plans restored from their saved mirror.
pub fn validate_output_schemas(plan: &PhysicalPlan) -> Result<(), PlanError> {
    for (id, node) in plan.live() {
        let expected = match node {
            PhysicalNode::Projection {
                return_exprs,
                node_objects,
                ..
            }
            | PhysicalNode::Aggregate {
                return_exprs,
                node_objects,
                ..
            } => return_schema(return_exprs, node_objects)?,
            PhysicalNode::MetadataCount { return_exprs, .. } => return_schema(return_exprs, &[])?,
            PhysicalNode::Sort { input, .. } | PhysicalNode::Limit { input, .. } => plan
                .properties(*input)
                .ok_or_else(|| {
                    PlanError::Internal(format!("result input {input} has no properties"))
                })?
                .schema
                .clone(),
            _ => continue,
        };
        let actual = &plan
            .properties(id)
            .ok_or_else(|| PlanError::Internal(format!("result node {id} has no properties")))?
            .schema;
        if actual.fields() != expected.fields() {
            return Err(PlanError::Internal(format!(
                "{} node {id} output schema differs from its declared columns",
                node.name()
            )));
        }
    }
    Ok(())
}
