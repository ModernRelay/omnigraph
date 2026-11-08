use std::collections::BTreeSet;

use arrow_schema::DataType;
use serde::Serialize;

use crate::catalog::Catalog;
use crate::error::{CompilerError, Result};

use super::ast::QueryDecl;
use super::typecheck::{
    BoundVariable, CheckedMutationOp, CheckedQuery, CheckedScope, MutationTarget, projection_field,
    typecheck_query_decl,
};

/// A compiled query's conservative graph access set and result shape.
///
/// `reads` is conservative: every graph type the supported execution path may
/// inspect is present. `writes` is the exact set of graph types the mutation
/// may change. Both sets are sorted and deduplicated for deterministic consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryOperationDescriptor {
    pub result: Vec<QueryResultFieldDescriptor>,
    pub reads: Vec<QueryGraphFact>,
    pub writes: Vec<QueryGraphFact>,
}

/// One field in a compiled query result.
///
/// Kinds are decomposed so clients never have to parse Arrow display strings
/// such as `Vector(3)` or `[Date]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryResultFieldDescriptor {
    pub name: String,
    pub kind: QueryValueKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_kind: Option<QueryValueKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_dim: Option<u32>,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryValueKind {
    String,
    Bool,
    Int,
    #[serde(rename = "bigint")]
    BigInt,
    Float,
    Date,
    #[serde(rename = "datetime")]
    DateTime,
    Blob,
    Vector,
    List,
    Object,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct QueryGraphFact {
    pub kind: QueryGraphFactKind,
    pub type_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryGraphFactKind {
    Node,
    Edge,
}

/// Compile one parsed query into the shared operation descriptor.
///
/// This is the single compiler owner used by lint today and by future server
/// and SDK projections. An invalid query returns an error rather than a
/// partial descriptor.
pub fn describe_query_operation(
    catalog: &Catalog,
    query: &QueryDecl,
) -> Result<QueryOperationDescriptor> {
    let checked = typecheck_query_decl(catalog, query)?;
    describe_checked_query_operation(catalog, &checked)
}

fn describe_checked_query_operation(
    catalog: &Catalog,
    checked: &CheckedQuery,
) -> Result<QueryOperationDescriptor> {
    let mut reads = BTreeSet::new();
    let mut writes = BTreeSet::new();

    let result = match checked {
        CheckedQuery::Read(query) => {
            collect_checked_reads(&query.scope, &mut reads);
            let ir = crate::lower_query(catalog, query)?;
            ir.return_exprs
                .iter()
                .zip(&query.returns)
                .map(|(projection, item)| {
                    let field = projection_field(catalog, &item.inferred_name, &projection.ty)?;
                    result_field_descriptor(&field)
                })
                .collect::<Result<Vec<_>>>()?
        }
        CheckedQuery::Mutation(query) => {
            for op in &query.ops {
                let fact = target_fact(op.target());
                reads.insert(fact.clone());
                writes.insert(fact);

                match op {
                    CheckedMutationOp::Insert {
                        target: MutationTarget::Edge { type_name },
                        ..
                    } => {
                        let edge = catalog.edge_types.get(type_name).ok_or_else(|| {
                            CompilerError::Plan(format!(
                                "typechecked edge type `{type_name}` is absent from the catalog"
                            ))
                        })?;
                        reads.insert(node_fact(&edge.from_type));
                        reads.insert(node_fact(&edge.to_type));
                    }
                    CheckedMutationOp::Delete {
                        target: MutationTarget::Node { type_name },
                        ..
                    } => {
                        for edge in catalog.edge_types.values().filter(|edge| {
                            edge.from_type == *type_name || edge.to_type == *type_name
                        }) {
                            let edge_fact = QueryGraphFact {
                                kind: QueryGraphFactKind::Edge,
                                type_name: edge.name.clone(),
                            };
                            reads.insert(edge_fact.clone());
                            writes.insert(edge_fact);
                        }
                    }
                    CheckedMutationOp::Insert { .. }
                    | CheckedMutationOp::Update { .. }
                    | CheckedMutationOp::Delete { .. } => {}
                }
            }
            Vec::new()
        }
    };

    Ok(QueryOperationDescriptor {
        result,
        reads: reads.into_iter().collect(),
        writes: writes.into_iter().collect(),
    })
}

fn collect_checked_reads(scope: &CheckedScope, reads: &mut BTreeSet<QueryGraphFact>) {
    let mut pending = vec![scope];
    while let Some(scope) = pending.pop() {
        for binding in scope.bindings.values() {
            if let BoundVariable::Node { type_name } = binding {
                reads.insert(node_fact(type_name));
            }
        }
        for traversal in scope.traversals() {
            reads.insert(node_fact(&traversal.src_type));
            reads.insert(node_fact(&traversal.dst_type));
            for member in traversal.edges.members() {
                reads.insert(QueryGraphFact {
                    kind: QueryGraphFactKind::Edge,
                    type_name: member.edge_type.clone(),
                });
            }
        }
        pending.extend(scope.blocks().map(|block| &block.inner));
    }
}

fn target_fact(target: &MutationTarget) -> QueryGraphFact {
    match target {
        MutationTarget::Node { type_name } => node_fact(type_name),
        MutationTarget::Edge { type_name } => QueryGraphFact {
            kind: QueryGraphFactKind::Edge,
            type_name: type_name.clone(),
        },
    }
}

fn node_fact(type_name: &str) -> QueryGraphFact {
    QueryGraphFact {
        kind: QueryGraphFactKind::Node,
        type_name: type_name.to_string(),
    }
}

fn result_field_descriptor(field: &arrow_schema::Field) -> Result<QueryResultFieldDescriptor> {
    let (kind, item_kind, vector_dim) = result_value_shape(field.data_type())?;
    Ok(QueryResultFieldDescriptor {
        name: field.name().clone(),
        kind,
        item_kind,
        vector_dim,
        nullable: field.is_nullable(),
    })
}

fn result_value_shape(
    data_type: &DataType,
) -> Result<(QueryValueKind, Option<QueryValueKind>, Option<u32>)> {
    match data_type {
        DataType::FixedSizeList(field, dim) if field.data_type() == &DataType::Float32 => {
            let dim = u32::try_from(*dim).map_err(|_| {
                CompilerError::Plan(format!(
                    "query result vector dimension `{dim}` cannot be represented"
                ))
            })?;
            Ok((QueryValueKind::Vector, None, Some(dim)))
        }
        DataType::List(field) => {
            let (item_kind, nested_item, vector_dim) = result_value_shape(field.data_type())?;
            if nested_item.is_some() || item_kind == QueryValueKind::List {
                return Err(CompilerError::Plan(
                    "nested query result lists are not representable".to_string(),
                ));
            }
            Ok((QueryValueKind::List, Some(item_kind), vector_dim))
        }
        DataType::Struct(_) => Ok((QueryValueKind::Object, None, None)),
        scalar => Ok((scalar_result_kind(scalar)?, None, None)),
    }
}

fn scalar_result_kind(data_type: &DataType) -> Result<QueryValueKind> {
    match data_type {
        DataType::Utf8 => Ok(QueryValueKind::String),
        DataType::Boolean => Ok(QueryValueKind::Bool),
        DataType::Int32 | DataType::UInt32 => Ok(QueryValueKind::Int),
        DataType::Int64 | DataType::UInt64 => Ok(QueryValueKind::BigInt),
        DataType::Float32 | DataType::Float64 => Ok(QueryValueKind::Float),
        DataType::Date32 => Ok(QueryValueKind::Date),
        DataType::Date64 => Ok(QueryValueKind::DateTime),
        DataType::LargeBinary => Ok(QueryValueKind::Blob),
        other => Err(CompilerError::Plan(format!(
            "query result data type `{other}` is not representable"
        ))),
    }
}
