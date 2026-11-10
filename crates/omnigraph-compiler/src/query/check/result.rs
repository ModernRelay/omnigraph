//! A checked read's result as Arrow fields: one per result column of the
//! core read, named as the executed result names it, typed from the stored
//! expression type; nothing is resolved again.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::catalog::Catalog;
use crate::error::{CompilerError, Result};
use crate::ir::core::CoreRead;
use crate::query::codes::T51;
use crate::types::ExprType;

/// The Arrow schema of `read`'s result.
pub fn result_schema(catalog: &Catalog, read: &CoreRead) -> Result<SchemaRef> {
    let fields = read
        .result
        .cols
        .iter()
        .map(|col| projection_field(catalog, &col.name, &col.ty.expr_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// One result field of type `ty`: a value's Arrow type, or a node's object
/// of its members.
pub fn projection_field(catalog: &Catalog, name: &str, ty: &ExprType) -> Result<Field> {
    let (data_type, nullable) = match ty {
        ExprType::Value { nullable, .. } => (
            ty.to_arrow()
                .ok_or_else(|| CompilerError::Plan("value has no Arrow type".into()))?,
            *nullable,
        ),
        ExprType::ExactInteger { .. } => {
            return Err(CompilerError::Plan(
                "internal exact integer cannot be a result column".into(),
            ));
        }
        ExprType::Edge { type_name } => {
            return Err(CompilerError::Plan(format!(
                "edge reference `{type_name}` cannot be a result column"
            )));
        }
        ExprType::Node { type_name } => {
            let node_type = catalog.query_node_type(type_name).ok_or_else(|| {
                CompilerError::typed(T51, format!("type `{}` not found in catalog", type_name))
            })?;
            let fields: Vec<Field> = node_type
                .node_object_members()
                .map(|(member, field)| {
                    Field::new(member, field.data_type().clone(), field.is_nullable())
                })
                .collect();
            (DataType::Struct(fields.into()), false)
        }
    };
    Ok(Field::new(name, data_type, nullable))
}
