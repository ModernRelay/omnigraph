use std::collections::BTreeSet;

use arrow_schema::DataType;
use serde::Serialize;

use crate::catalog::Catalog;
use crate::error::{CompilerError, Result};

use super::ast::{Clause, Mutation, QueryDecl};
use super::typecheck::{
    CheckedQuery, MutationTarget, infer_query_result_schema, typecheck_query_decl,
};

/// A compiled query's conservative graph access set and result shape.
///
/// `reads` is conservative: every table the supported execution path may
/// inspect is present. `writes` is the exact set of tables the mutation may
/// change. Both sets are sorted and deduplicated for deterministic consumers.
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
    describe_checked_query_operation(catalog, query, &checked)
}

fn describe_checked_query_operation(
    catalog: &Catalog,
    query: &QueryDecl,
    checked: &CheckedQuery,
) -> Result<QueryOperationDescriptor> {
    let mut reads = BTreeSet::new();
    let mut writes = BTreeSet::new();

    let result = match checked {
        CheckedQuery::Read(ctx) => {
            collect_clause_reads(catalog, &query.match_clause, &mut reads)?;
            infer_query_result_schema(catalog, query, ctx)?
                .fields()
                .iter()
                .map(result_field_descriptor)
                .collect::<Result<Vec<_>>>()?
        }
        CheckedQuery::Mutation(ctx) => {
            if ctx.targets.len() != query.mutations.len() {
                return Err(CompilerError::Type(
                    "typechecked mutation target count does not match the query".to_string(),
                ));
            }
            for (mutation, target) in query.mutations.iter().zip(&ctx.targets) {
                let fact = target_fact(target);
                reads.insert(fact.clone());
                writes.insert(fact);

                match (mutation, target) {
                    (Mutation::Insert(_), MutationTarget::Edge { type_name }) => {
                        let edge = catalog.edge_types.get(type_name).ok_or_else(|| {
                            CompilerError::Type(format!(
                                "typechecked edge type `{type_name}` is absent from the catalog"
                            ))
                        })?;
                        reads.insert(node_fact(&edge.from_type));
                        reads.insert(node_fact(&edge.to_type));
                    }
                    (Mutation::Delete(_), MutationTarget::Node { type_name }) => {
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
                    _ => {}
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

fn collect_clause_reads(
    catalog: &Catalog,
    clauses: &[Clause],
    reads: &mut BTreeSet<QueryGraphFact>,
) -> Result<()> {
    for clause in clauses {
        match clause {
            Clause::Binding(binding) => {
                reads.insert(node_fact(&binding.type_name));
            }
            Clause::Traversal(traversal) => {
                let edge = catalog
                    .lookup_edge_by_name(&traversal.edge_name)
                    .ok_or_else(|| {
                        CompilerError::Type(format!(
                            "typechecked edge type `{}` is absent from the catalog",
                            traversal.edge_name
                        ))
                    })?;
                reads.insert(QueryGraphFact {
                    kind: QueryGraphFactKind::Edge,
                    type_name: edge.name.clone(),
                });
                reads.insert(node_fact(&edge.from_type));
                reads.insert(node_fact(&edge.to_type));
            }
            Clause::Negation(inner) => collect_clause_reads(catalog, inner, reads)?,
            Clause::Filter(_) => {}
        }
    }
    Ok(())
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

fn result_field_descriptor(field: &arrow_schema::FieldRef) -> Result<QueryResultFieldDescriptor> {
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
                CompilerError::Type(format!(
                    "query result vector dimension `{dim}` cannot be represented"
                ))
            })?;
            Ok((QueryValueKind::Vector, None, Some(dim)))
        }
        DataType::List(field) => {
            let (item_kind, nested_item, vector_dim) = result_value_shape(field.data_type())?;
            if nested_item.is_some() || item_kind == QueryValueKind::List {
                return Err(CompilerError::Type(
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
        other => Err(CompilerError::Type(format!(
            "query result data type `{other}` is not representable"
        ))),
    }
}
