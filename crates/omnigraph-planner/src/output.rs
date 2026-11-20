//! Declared result columns and the named node and edge objects they contain.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use omnigraph_compiler::ir::IRProjection;
use omnigraph_compiler::types::{ExprType, ScalarType};

use crate::{PhysicalNode, PhysicalPlan, PlanError, PlanSource};

/// A named result object's complete member type, captured from the compiler
/// catalog when a result node is planned: a node's, named by its type, or an
/// edge's, named `edge` and its selection's types (`edge Knows|Likes`).
/// Saved plans retain the declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeObjectType {
    pub type_name: String,
    pub fields: Fields,
}

/// The name of the object declaration a reference result column reads.
fn object_name(ty: &ExprType) -> Option<String> {
    match ty {
        ExprType::Node { type_name, .. } => Some(type_name.clone()),
        ExprType::Edge { type_name, .. } => Some(format!("edge {type_name}")),
        ExprType::Value { .. } | ExprType::ExactInteger { .. } => None,
    }
}

pub(crate) fn node_object_types(
    returns: &[IRProjection],
    source: &dyn PlanSource,
) -> Result<Vec<NodeObjectType>, PlanError> {
    let objects: BTreeMap<String, &ExprType> = returns
        .iter()
        .filter_map(|projection| Some((object_name(&projection.ty)?, &projection.ty)))
        .collect();
    objects
        .into_iter()
        .map(|(name, ty)| {
            let fields = match ty {
                ExprType::Edge { type_name, .. } => source.edge_object_fields(type_name)?,
                ExprType::Node { type_name, .. } => source.node_type(type_name)?.object_fields,
                ExprType::Value { .. } | ExprType::ExactInteger { .. } => {
                    return Err(PlanError::Internal(format!(
                        "result object `{name}` is not a reference"
                    )));
                }
            };
            Ok(NodeObjectType {
                type_name: name,
                fields,
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
                "duplicate result object declaration `{}`",
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
            // A reference an `optional` introduces is a null object on a
            // row the `optional` kept without a match.
            ExprType::Node { nullable, .. } | ExprType::Edge { nullable, .. } => {
                let name = object_name(&projection.ty).unwrap_or_default();
                let (name, fields) = objects.get_key_value(name.as_str()).ok_or_else(|| {
                    PlanError::Internal(format!("missing result object declaration `{name}`"))
                })?;
                used.insert(*name);
                (DataType::Struct((*fields).clone()), *nullable)
            }
        };
        Ok(Field::new(&projection.column, data_type, nullable))
    }).collect::<Result<Vec<_>, PlanError>>()?;
    if used.len() != objects.len() {
        return Err(PlanError::Internal(
            "unused result object declaration in result node".into(),
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
