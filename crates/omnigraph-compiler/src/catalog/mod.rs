pub mod schema_ir;
pub mod schema_plan;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{NanoError, Result};
use crate::schema::ast::{Cardinality, Constraint, ConstraintBound, SchemaDecl, SchemaFile};
use crate::types::{PropType, ScalarType};

#[derive(Debug, Clone)]
pub struct Catalog {
    pub node_types: HashMap<String, NodeType>,
    pub edge_types: HashMap<String, EdgeType>,
    /// Maps normalized lowercase edge name -> EdgeType key (e.g. "knows" -> "Knows")
    pub edge_name_index: HashMap<String, String>,
    /// Interface declarations (for Phase 2 polymorphic queries)
    pub interfaces: HashMap<String, InterfaceType>,
}

#[derive(Debug, Clone)]
pub struct InterfaceType {
    pub name: String,
    pub properties: HashMap<String, PropType>,
}

#[derive(Debug, Clone)]
pub struct NodeType {
    pub name: String,
    /// Interface names this type implements
    pub implements: Vec<String>,
    pub properties: HashMap<String, PropType>,
    /// Key property names (from `@key` or `@key(name)`). Usually 0 or 1 element.
    pub key: Option<Vec<String>>,
    /// Uniqueness constraints (each entry is a list of column names)
    pub unique_constraints: Vec<Vec<String>>,
    /// Index declarations (each entry is a list of column names)
    pub indices: Vec<Vec<String>>,
    /// Value range constraints
    pub range_constraints: Vec<RangeConstraint>,
    /// Regex check constraints
    pub check_constraints: Vec<CheckConstraint>,
    /// Maps @embed target property -> source text property
    pub embed_sources: HashMap<String, String>,
    pub blob_properties: HashSet<String>,
    pub arrow_schema: SchemaRef,
}

impl NodeType {
    /// Backward-compatible accessor: returns the first (and typically only) key property name.
    pub fn key_property(&self) -> Option<&str> {
        self.key
            .as_ref()
            .and_then(|v| v.first())
            .map(|s| s.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct RangeConstraint {
    pub property: String,
    pub min: Option<LiteralValue>,
    pub max: Option<LiteralValue>,
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
}

#[derive(Debug, Clone)]
pub struct CheckConstraint {
    pub property: String,
    pub pattern: String,
}

#[derive(Debug, Clone)]
pub struct EdgeType {
    pub name: String,
    pub from_type: String,
    pub to_type: String,
    pub cardinality: Cardinality,
    pub properties: HashMap<String, PropType>,
    /// Uniqueness constraints on edge columns (e.g. `@unique(src, dst)`)
    pub unique_constraints: Vec<Vec<String>>,
    /// Index declarations on edge properties
    pub indices: Vec<Vec<String>>,
    pub blob_properties: HashSet<String>,
    pub arrow_schema: SchemaRef,
}

impl Catalog {
    pub fn lookup_edge_by_name(&self, name: &str) -> Option<&EdgeType> {
        if let Some(et) = self.edge_types.get(name) {
            return Some(et);
        }
        if let Some(key) = self.edge_name_index.get(&normalize_edge_name(name)) {
            return self.edge_types.get(key);
        }
        None
    }
}

fn normalize_edge_name(name: &str) -> String {
    name.to_lowercase()
}

fn bound_to_literal(b: &ConstraintBound) -> LiteralValue {
    match b {
        ConstraintBound::Integer(n) => LiteralValue::Integer(*n),
        ConstraintBound::Float(f) => LiteralValue::Float(*f),
    }
}

pub fn build_catalog(schema: &SchemaFile) -> Result<Catalog> {
    let mut node_types = HashMap::new();
    let mut edge_types = HashMap::new();
    let mut edge_name_index = HashMap::new();
    let mut interfaces = HashMap::new();

    // Pass 0: collect interfaces
    for decl in &schema.declarations {
        if let SchemaDecl::Interface(iface) = decl {
            let mut properties = HashMap::new();
            for prop in &iface.properties {
                properties.insert(prop.name.clone(), prop.prop_type.clone());
            }
            interfaces.insert(
                iface.name.clone(),
                InterfaceType {
                    name: iface.name.clone(),
                    properties,
                },
            );
        }
    }

    // Pass 1: collect node types
    for decl in &schema.declarations {
        if let SchemaDecl::Node(node) = decl {
            if node_types.contains_key(&node.name) {
                return Err(NanoError::Catalog(format!(
                    "duplicate node type: {}",
                    node.name
                )));
            }

            let mut properties = HashMap::new();
            let mut embed_sources = HashMap::new();
            let mut blob_properties = HashSet::new();
            for prop in &node.properties {
                properties.insert(prop.name.clone(), prop.prop_type.clone());
                if matches!(prop.prop_type.scalar, ScalarType::Blob) {
                    blob_properties.insert(prop.name.clone());
                }
                // Extract @embed from property annotations (stays as annotation)
                if let Some(source_prop) = prop
                    .annotations
                    .iter()
                    .find(|ann| ann.name == "embed")
                    .and_then(|ann| ann.value.clone())
                {
                    embed_sources.insert(prop.name.clone(), source_prop);
                }
            }

            // Extract constraints from the typed Constraint enum
            let mut key: Option<Vec<String>> = None;
            let mut unique_constraints = Vec::new();
            let mut indices = Vec::new();
            let mut range_constraints = Vec::new();
            let mut check_constraints = Vec::new();

            for constraint in &node.constraints {
                match constraint {
                    Constraint::Key(cols) => {
                        key = Some(cols.clone());
                        // @key implies index on key columns
                        indices.push(cols.clone());
                    }
                    Constraint::Unique(cols) => {
                        unique_constraints.push(cols.clone());
                    }
                    Constraint::Index(cols) => {
                        indices.push(cols.clone());
                    }
                    Constraint::Range { property, min, max } => {
                        range_constraints.push(RangeConstraint {
                            property: property.clone(),
                            min: min.as_ref().map(bound_to_literal),
                            max: max.as_ref().map(bound_to_literal),
                        });
                    }
                    Constraint::Check { property, pattern } => {
                        check_constraints.push(CheckConstraint {
                            property: property.clone(),
                            pattern: pattern.clone(),
                        });
                    }
                }
            }

            // Build Arrow schema: id: Utf8 + all properties
            let mut fields = vec![Field::new("id", DataType::Utf8, false)];
            for prop in &node.properties {
                fields.push(Field::new(
                    &prop.name,
                    prop.prop_type.to_arrow(),
                    prop.prop_type.nullable,
                ));
            }
            let arrow_schema = Arc::new(Schema::new(fields));

            node_types.insert(
                node.name.clone(),
                NodeType {
                    name: node.name.clone(),
                    implements: node.implements.clone(),
                    properties,
                    key,
                    unique_constraints,
                    indices,
                    range_constraints,
                    check_constraints,
                    embed_sources,
                    blob_properties,
                    arrow_schema,
                },
            );
        }
    }

    // Pass 2: collect edge types, validate endpoints
    for decl in &schema.declarations {
        if let SchemaDecl::Edge(edge) = decl {
            if edge_types.contains_key(&edge.name) {
                return Err(NanoError::Catalog(format!(
                    "duplicate edge type: {}",
                    edge.name
                )));
            }
            if !node_types.contains_key(&edge.from_type) {
                return Err(NanoError::Catalog(format!(
                    "edge {} references unknown source type: {}",
                    edge.name, edge.from_type
                )));
            }
            if !node_types.contains_key(&edge.to_type) {
                return Err(NanoError::Catalog(format!(
                    "edge {} references unknown target type: {}",
                    edge.name, edge.to_type
                )));
            }

            let mut properties = HashMap::new();
            let mut blob_properties = HashSet::new();
            let mut fields = vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("src", DataType::Utf8, false),
                Field::new("dst", DataType::Utf8, false),
            ];
            for prop in &edge.properties {
                properties.insert(prop.name.clone(), prop.prop_type.clone());
                if matches!(prop.prop_type.scalar, ScalarType::Blob) {
                    blob_properties.insert(prop.name.clone());
                }
                fields.push(Field::new(
                    &prop.name,
                    prop.prop_type.to_arrow(),
                    prop.prop_type.nullable,
                ));
            }

            // Extract edge constraints
            let mut unique_constraints = Vec::new();
            let mut edge_indices = Vec::new();
            for constraint in &edge.constraints {
                match constraint {
                    Constraint::Unique(cols) => unique_constraints.push(cols.clone()),
                    Constraint::Index(cols) => edge_indices.push(cols.clone()),
                    _ => {} // Key/Range/Check validated at parse time to not appear on edges
                }
            }

            let normalized_name = normalize_edge_name(&edge.name);
            if let Some(existing) = edge_name_index.get(&normalized_name)
                && existing != &edge.name
            {
                return Err(NanoError::Catalog(format!(
                    "edge name collision after case folding: '{}' conflicts with '{}'",
                    edge.name, existing
                )));
            }
            edge_name_index.insert(normalized_name, edge.name.clone());

            edge_types.insert(
                edge.name.clone(),
                EdgeType {
                    name: edge.name.clone(),
                    from_type: edge.from_type.clone(),
                    to_type: edge.to_type.clone(),
                    cardinality: edge.cardinality.clone(),
                    properties,
                    unique_constraints,
                    indices: edge_indices,
                    blob_properties,
                    arrow_schema: Arc::new(Schema::new(fields)),
                },
            );
        }
    }

    Ok(Catalog {
        node_types,
        edge_types,
        edge_name_index,
        interfaces,
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
