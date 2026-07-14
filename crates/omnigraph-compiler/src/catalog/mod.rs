pub mod schema_ir;
pub mod schema_plan;
pub mod schema_shape;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{CompilerError, Result};
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
    /// Source-only callers are intentionally unbound. Runtime catalogs are a
    /// direct projection of one validated accepted SchemaIR and retain that
    /// authority so no consumer can reconstruct IDs from mutable names.
    pub identity: CatalogIdentity,
}

#[derive(Debug, Clone)]
pub enum CatalogIdentity {
    SourceUnbound,
    Bound(Arc<schema_ir::SchemaIR>),
}

#[derive(Debug, Clone)]
pub struct InterfaceType {
    pub name: String,
    pub properties: HashMap<String, PropType>,
}

/// The `@embed` binding for a vector property: its source text property and,
/// optionally, the embedding model recorded by `@embed("source", model="…")`.
/// The model is what the query-time same-space check validates against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedSource {
    pub source: String,
    pub model: Option<String>,
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
    /// Maps @embed target property -> its source text property + recorded model.
    pub embed_sources: HashMap<String, EmbedSource>,
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

    pub fn is_identity_bound(&self) -> bool {
        matches!(self.identity, CatalogIdentity::Bound(_))
    }

    pub fn bound_schema_ir(&self) -> Option<&schema_ir::SchemaIR> {
        match &self.identity {
            CatalogIdentity::SourceUnbound => None,
            CatalogIdentity::Bound(ir) => Some(ir),
        }
    }

    pub fn type_id(&self, name: &str) -> Option<schema_ir::StableTypeId> {
        let ir = self.bound_schema_ir()?;
        ir.interfaces
            .iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.type_id)
            .or_else(|| {
                ir.nodes
                    .iter()
                    .find(|entry| entry.name == name)
                    .map(|entry| entry.type_id)
            })
            .or_else(|| {
                ir.edges
                    .iter()
                    .find(|entry| entry.name == name)
                    .map(|entry| entry.type_id)
            })
    }

    pub fn table_incarnation_id(&self, name: &str) -> Option<schema_ir::TableIncarnationId> {
        let ir = self.bound_schema_ir()?;
        ir.nodes
            .iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.table_incarnation_id)
            .or_else(|| {
                ir.edges
                    .iter()
                    .find(|entry| entry.name == name)
                    .map(|entry| entry.table_incarnation_id)
            })
    }

    pub fn property_id(
        &self,
        owner_name: &str,
        property_name: &str,
    ) -> Option<schema_ir::StablePropertyId> {
        let ir = self.bound_schema_ir()?;
        ir.interfaces
            .iter()
            .map(|entry| (entry.name.as_str(), entry.properties.as_slice()))
            .chain(
                ir.nodes
                    .iter()
                    .map(|entry| (entry.name.as_str(), entry.properties.as_slice())),
            )
            .chain(
                ir.edges
                    .iter()
                    .map(|entry| (entry.name.as_str(), entry.properties.as_slice())),
            )
            .find(|(name, _)| *name == owner_name)
            .and_then(|(_, properties)| {
                properties
                    .iter()
                    .find(|property| property.name == property_name)
                    .map(|property| property.property_id)
            })
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
                return Err(CompilerError::Catalog(format!(
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
                // Extract @embed: the source text property (positional) and the
                // optional recorded model (the `model` kwarg).
                if let Some(ann) = prop.annotations.iter().find(|ann| ann.name == "embed") {
                    if let Some(source) = ann.value.clone() {
                        embed_sources.insert(
                            prop.name.clone(),
                            EmbedSource {
                                source,
                                model: ann.kwargs.get("model").cloned(),
                            },
                        );
                    }
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
                return Err(CompilerError::Catalog(format!(
                    "duplicate edge type: {}",
                    edge.name
                )));
            }
            if !node_types.contains_key(&edge.from_type) {
                return Err(CompilerError::Catalog(format!(
                    "edge {} references unknown source type: {}",
                    edge.name, edge.from_type
                )));
            }
            if !node_types.contains_key(&edge.to_type) {
                return Err(CompilerError::Catalog(format!(
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
                return Err(CompilerError::Catalog(format!(
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
        identity: CatalogIdentity::SourceUnbound,
    })
}

/// Build the runtime catalog directly from validated accepted identity
/// authority. This path never round-trips through the source AST and never
/// mints or derives an identity from a name.
pub fn build_catalog_from_ir(ir: &schema_ir::SchemaIR) -> Result<Catalog> {
    schema_ir::validate_schema_ir(ir)?;

    let interfaces = ir
        .interfaces
        .iter()
        .map(|interface| {
            (
                interface.name.clone(),
                InterfaceType {
                    name: interface.name.clone(),
                    properties: interface
                        .properties
                        .iter()
                        .map(|property| (property.name.clone(), property.prop_type.clone()))
                        .collect(),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    let mut node_types = HashMap::new();
    for node in &ir.nodes {
        let properties = node
            .properties
            .iter()
            .map(|property| (property.name.clone(), property.prop_type.clone()))
            .collect::<HashMap<_, _>>();
        let blob_properties = node
            .properties
            .iter()
            .filter(|property| matches!(property.prop_type.scalar, ScalarType::Blob))
            .map(|property| property.name.clone())
            .collect();
        let embed_sources = node
            .properties
            .iter()
            .filter_map(|property| {
                property.embed_source.as_ref().map(|embed| {
                    (
                        property.name.clone(),
                        EmbedSource {
                            source: embed.source.property_name.clone(),
                            model: embed.model.clone(),
                        },
                    )
                })
            })
            .collect();
        let mut key = None;
        let mut unique_constraints = Vec::new();
        let mut indices = Vec::new();
        let mut range_constraints = Vec::new();
        let mut check_constraints = Vec::new();
        for constraint in &node.constraints {
            match schema_ir::constraint_from_ir(constraint) {
                Constraint::Key(columns) => {
                    key = Some(columns.clone());
                    indices.push(columns);
                }
                Constraint::Unique(columns) => unique_constraints.push(columns),
                Constraint::Index(columns) => indices.push(columns),
                Constraint::Range { property, min, max } => {
                    range_constraints.push(RangeConstraint {
                        property,
                        min: min.as_ref().map(bound_to_literal),
                        max: max.as_ref().map(bound_to_literal),
                    });
                }
                Constraint::Check { property, pattern } => {
                    check_constraints.push(CheckConstraint { property, pattern });
                }
            }
        }
        let mut fields = vec![Field::new("id", DataType::Utf8, false)];
        fields.extend(node.properties.iter().map(|property| {
            Field::new(
                &property.name,
                property.prop_type.to_arrow(),
                property.prop_type.nullable,
            )
        }));
        node_types.insert(
            node.name.clone(),
            NodeType {
                name: node.name.clone(),
                implements: node
                    .implements
                    .iter()
                    .map(|reference| reference.type_name.clone())
                    .collect(),
                properties,
                key,
                unique_constraints,
                indices,
                range_constraints,
                check_constraints,
                embed_sources,
                blob_properties,
                arrow_schema: Arc::new(Schema::new(fields)),
            },
        );
    }

    let mut edge_types = HashMap::new();
    let mut edge_name_index = HashMap::new();
    for edge in &ir.edges {
        let properties = edge
            .properties
            .iter()
            .map(|property| (property.name.clone(), property.prop_type.clone()))
            .collect::<HashMap<_, _>>();
        let blob_properties = edge
            .properties
            .iter()
            .filter(|property| matches!(property.prop_type.scalar, ScalarType::Blob))
            .map(|property| property.name.clone())
            .collect();
        let mut unique_constraints = Vec::new();
        let mut indices = Vec::new();
        for constraint in &edge.constraints {
            match schema_ir::constraint_from_ir(constraint) {
                Constraint::Unique(columns) => unique_constraints.push(columns),
                Constraint::Index(columns) => indices.push(columns),
                _ => {}
            }
        }
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("src", DataType::Utf8, false),
            Field::new("dst", DataType::Utf8, false),
        ];
        fields.extend(edge.properties.iter().map(|property| {
            Field::new(
                &property.name,
                property.prop_type.to_arrow(),
                property.prop_type.nullable,
            )
        }));
        let normalized_name = normalize_edge_name(&edge.name);
        if let Some(existing) = edge_name_index.get(&normalized_name)
            && existing != &edge.name
        {
            return Err(CompilerError::Catalog(format!(
                "edge name collision after case folding: '{}' conflicts with '{}'",
                edge.name, existing
            )));
        }
        edge_name_index.insert(normalized_name, edge.name.clone());
        edge_types.insert(
            edge.name.clone(),
            EdgeType {
                name: edge.name.clone(),
                from_type: edge.from_type.type_name.clone(),
                to_type: edge.to_type.type_name.clone(),
                cardinality: edge.cardinality.clone(),
                properties,
                unique_constraints,
                indices,
                blob_properties,
                arrow_schema: Arc::new(Schema::new(fields)),
            },
        );
    }

    Ok(Catalog {
        node_types,
        edge_types,
        edge_name_index,
        interfaces,
        identity: CatalogIdentity::Bound(Arc::new(ir.clone())),
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
