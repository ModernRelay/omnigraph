use std::collections::BTreeSet;

use arrow_schema::DataType;
use serde::Serialize;

use crate::catalog::Catalog;
use crate::error::{CompilerError, Result};

use super::check::{check_query_decl, result_schema};
use super::file::QueryDecl;
use crate::ir::core::{CoreDecl, MutationTarget, Sink, Term};

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
    let core = check_query_decl(catalog, query)?;
    describe_core_operation(catalog, &core)
}

fn describe_core_operation(catalog: &Catalog, core: &CoreDecl) -> Result<QueryOperationDescriptor> {
    let mut reads = BTreeSet::new();
    let mut writes = BTreeSet::new();
    let result = match core {
        CoreDecl::Read(read) => {
            collect_term_reads(&read.term, &mut reads);
            result_schema(catalog, read)?
                .fields()
                .iter()
                .map(|field| result_field_descriptor(field))
                .collect::<Result<Vec<_>>>()?
        }
        CoreDecl::Write(write) => {
            for group in &write.groups {
                collect_term_reads(&group.input, &mut reads);
                for sink in &group.sinks {
                    let target = match sink {
                        Sink::Insert { target, .. }
                        | Sink::Update { target, .. }
                        | Sink::Delete { target, .. } => target,
                    };
                    let fact = target_fact(target);
                    reads.insert(fact.clone());
                    writes.insert(fact);
                    match (sink, target) {
                        (Sink::Insert { .. }, MutationTarget::Edge { type_name }) => {
                            let edge = catalog.edge_types.get(type_name).ok_or_else(|| {
                                CompilerError::Plan(format!(
                                    "typechecked edge type `{type_name}` is absent from the catalog"
                                ))
                            })?;
                            reads.insert(node_fact(&edge.from_type));
                            reads.insert(node_fact(&edge.to_type));
                        }
                        (Sink::Delete { .. }, MutationTarget::Node { type_name }) => {
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

/// Every node type a term scans or expands to, and every edge type it
/// traverses.
fn collect_term_reads(term: &Term, reads: &mut BTreeSet<QueryGraphFact>) {
    match term {
        Term::Scan { ty, .. } => match ty {
            crate::ir::core::Ty::Node { type_name, .. } => {
                reads.insert(node_fact(type_name));
            }
            crate::ir::core::Ty::Edge { type_name, .. } => {
                for name in type_name.split('|') {
                    reads.insert(QueryGraphFact {
                        kind: QueryGraphFactKind::Edge,
                        type_name: name.to_string(),
                    });
                }
            }
            crate::ir::core::Ty::Value(_) => {}
        },
        Term::Expand {
            src_type,
            dst_type,
            edges,
            ..
        } => {
            reads.insert(node_fact(src_type));
            reads.insert(node_fact(dst_type));
            for member in edges.members() {
                reads.insert(QueryGraphFact {
                    kind: QueryGraphFactKind::Edge,
                    type_name: member.edge_type.clone(),
                });
            }
        }
        _ => {}
    }
    for child in term.children() {
        collect_term_reads(child, reads);
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
