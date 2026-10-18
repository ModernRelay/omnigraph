//! The load IR: one `load csv` statement resolved under the transaction's
//! catalog, the planner's input behind `Operation::Load` (RFC 2026-10-15,
//! bulk load as a query plan). Lowering reads the statement and the catalog
//! only, never a payload byte, so an explain and an acceptance cost the same.

use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::query::ast::{LoadStmt, LoadStmtMode, LoadTarget};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadIR {
    /// The statement as the grammar reads it back.
    pub statement: String,
    pub source: String,
    pub target: LoadTarget,
    pub mode: LoadStmtMode,
    pub delimiter: u8,
    pub header: bool,
    /// Property names by column position, `None` when the header names them.
    pub columns: Option<Vec<String>>,
    /// The columns the statement maps, counted without reading the source:
    /// the spelled list, else every declared property of the target plus an
    /// edge file's two endpoints. A target absent from the catalog counts no
    /// property.
    pub mapped_columns: usize,
    /// The constraints the load judges, in evaluation order, which is the
    /// refusal precedence.
    pub checks: Vec<LoadCheck>,
}

/// One constraint a load judges and the committed table it reads beside the
/// delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadCheck {
    pub check: ValidateCheck,
    /// The type key of the committed table the check reads; `None` when the
    /// delta alone decides it.
    pub consults: Option<String>,
}

/// An edge endpoint, named as the statement's `from`/`to` columns are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
    From,
    To,
}

impl Endpoint {
    pub fn as_str(self) -> &'static str {
        match self {
            Endpoint::From => "from",
            Endpoint::To => "to",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UniqueRole {
    Key,
    Unique,
}

impl UniqueRole {
    pub fn as_str(self) -> &'static str {
        match self {
            UniqueRole::Key => "key",
            UniqueRole::Unique => "unique",
        }
    }
}

/// One validation of a load plan. Identity is the check kind plus the
/// qualified table; a property constraint adds its column tuple and a
/// uniqueness its role, an endpoint constraint its endpoint and referenced
/// type, so every constraint of one table stays separately addressable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "check", rename_all = "snake_case")]
pub enum ValidateCheck {
    Value {
        columns: Vec<String>,
    },
    Unique {
        role: UniqueRole,
        columns: Vec<String>,
    },
    EdgeRi {
        endpoint: Endpoint,
        node_type: String,
    },
    Cardinality {
        endpoint: Endpoint,
        node_type: String,
        min: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<u32>,
    },
    /// A node overwrite's surviving incident edges in an untouched edge table.
    RetainedEdges {
        edge_table: String,
        endpoint: Endpoint,
    },
}

impl ValidateCheck {
    /// The check kind as the explain document spells it.
    pub fn name(&self) -> &'static str {
        match self {
            ValidateCheck::Value { .. } => "value",
            ValidateCheck::Unique { .. } => "unique",
            ValidateCheck::EdgeRi { .. } => "edge_ri",
            ValidateCheck::Cardinality { .. } => "cardinality",
            ValidateCheck::RetainedEdges { .. } => "retained_edges",
        }
    }
}

/// Lower a load statement under `catalog`. Infallible: a column the catalog
/// does not declare is refused where the source is parsed, with the
/// statement's own message, exactly as before the plan existed.
pub fn lower_load(catalog: &Catalog, stmt: &LoadStmt) -> LoadIR {
    LoadIR {
        statement: stmt.render(),
        source: stmt.source.clone(),
        target: stmt.target.clone(),
        mode: stmt.mode,
        delimiter: stmt.delimiter,
        header: stmt.header,
        columns: stmt.columns.clone(),
        mapped_columns: mapped_columns(catalog, stmt),
        checks: checks(catalog, &stmt.target, stmt.mode),
    }
}

fn mapped_columns(catalog: &Catalog, stmt: &LoadStmt) -> usize {
    if let Some(columns) = &stmt.columns {
        return columns.len();
    }
    match &stmt.target {
        LoadTarget::Node(name) => catalog
            .node_types
            .get(name)
            .map_or(0, |node_type| node_type.properties.len()),
        LoadTarget::Edge(name) => catalog
            .edge_types
            .get(name)
            .map_or(0, |edge_type| edge_type.properties.len())
            .saturating_add(2),
    }
}

/// Evaluator order: value, the target's `@key` then `@unique` groups, an
/// edge's two endpoint references and its `@card`, and for a node overwrite
/// the retained edges of every incident edge type in name order.
fn checks(catalog: &Catalog, target: &LoadTarget, mode: LoadStmtMode) -> Vec<LoadCheck> {
    let table = target.table_key();
    let overwrite = mode == LoadStmtMode::Overwrite;
    let mut checks = vec![LoadCheck {
        check: ValidateCheck::Value {
            columns: value_columns(catalog, target),
        },
        consults: None,
    }];
    match target {
        LoadTarget::Node(name) => {
            if let Some(node_type) = catalog.node_types.get(name) {
                let key = node_type.key.as_deref();
                let subsumed = |columns: &Vec<String>| key == Some(columns.as_slice());
                unique_checks(
                    &mut checks,
                    key,
                    node_type.unique_constraints.iter().filter(|c| !subsumed(c)),
                    &table,
                    overwrite,
                );
            }
            if !overwrite {
                return checks;
            }
            let mut edge_types: Vec<_> = catalog.edge_types.iter().collect();
            edge_types.sort_by(|a, b| a.0.cmp(b.0));
            for (edge_name, edge_type) in edge_types {
                let edge_table = format!("edge:{edge_name}");
                for (endpoint, node_type) in [
                    (Endpoint::From, &edge_type.from_type),
                    (Endpoint::To, &edge_type.to_type),
                ] {
                    if node_type != name {
                        continue;
                    }
                    checks.push(LoadCheck {
                        check: ValidateCheck::RetainedEdges {
                            edge_table: edge_table.clone(),
                            endpoint,
                        },
                        consults: Some(edge_table.clone()),
                    });
                }
            }
        }
        LoadTarget::Edge(name) => {
            let Some(edge_type) = catalog.edge_types.get(name) else {
                return checks;
            };
            let subsumed =
                |columns: &Vec<String>| same_column_set(edge_type.key.as_deref(), columns);
            unique_checks(
                &mut checks,
                edge_type.key.as_deref(),
                edge_type.unique_constraints.iter().filter(|c| !subsumed(c)),
                &table,
                overwrite,
            );
            for (endpoint, node_type) in [
                (Endpoint::From, &edge_type.from_type),
                (Endpoint::To, &edge_type.to_type),
            ] {
                checks.push(LoadCheck {
                    check: ValidateCheck::EdgeRi {
                        endpoint,
                        node_type: node_type.clone(),
                    },
                    consults: Some(format!("node:{node_type}")),
                });
            }
            if !edge_type.cardinality.is_default() {
                checks.push(LoadCheck {
                    check: ValidateCheck::Cardinality {
                        endpoint: Endpoint::From,
                        node_type: edge_type.from_type.clone(),
                        min: edge_type.cardinality.min,
                        max: edge_type.cardinality.max,
                    },
                    consults: Some(table.clone()),
                });
            }
        }
    }
    checks
}

/// A `@key` group is id-backed, so a committed holder of its value is the
/// same row: the delta alone decides it. A `@unique` group reads the
/// committed table unless the load replaces it.
fn unique_checks<'a>(
    checks: &mut Vec<LoadCheck>,
    key: Option<&[String]>,
    unique: impl Iterator<Item = &'a Vec<String>>,
    table: &str,
    overwrite: bool,
) {
    if let Some(key) = key {
        checks.push(LoadCheck {
            check: ValidateCheck::Unique {
                role: UniqueRole::Key,
                columns: key.to_vec(),
            },
            consults: None,
        });
    }
    for columns in unique {
        checks.push(LoadCheck {
            check: ValidateCheck::Unique {
                role: UniqueRole::Unique,
                columns: columns.clone(),
            },
            consults: (!overwrite).then(|| table.to_string()),
        });
    }
}

/// Uniqueness is order-free, and the catalog spells an edge key
/// endpoint-first but a `@unique` group lexically, so a key subsumes a group
/// when the column sets match.
fn same_column_set(key: Option<&[String]>, columns: &[String]) -> bool {
    let Some(key) = key else {
        return false;
    };
    let mut key = key.to_vec();
    key.sort();
    let mut columns = columns.to_vec();
    columns.sort();
    key == columns
}

/// The columns the value check judges: enum-typed, `@range` and `@check`
/// properties, sorted, each once.
fn value_columns(catalog: &Catalog, target: &LoadTarget) -> Vec<String> {
    let mut columns: Vec<String> = match target {
        LoadTarget::Node(name) => catalog.node_types.get(name).map_or_else(Vec::new, |node| {
            node.properties
                .iter()
                .filter(|(_, prop)| prop.enum_values.is_some())
                .map(|(name, _)| name.clone())
                .chain(node.range_constraints.iter().map(|c| c.property.clone()))
                .chain(node.check_constraints.iter().map(|c| c.property.clone()))
                .collect()
        }),
        LoadTarget::Edge(name) => catalog.edge_types.get(name).map_or_else(Vec::new, |edge| {
            edge.properties
                .iter()
                .filter(|(_, prop)| prop.enum_values.is_some())
                .map(|(name, _)| name.clone())
                .collect()
        }),
    };
    columns.sort();
    columns.dedup();
    columns
}
