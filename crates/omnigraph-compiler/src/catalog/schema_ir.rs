use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::catalog::{Catalog, build_catalog};
use crate::error::{NanoError, Result};
use crate::schema::ast::{Annotation, Cardinality, Constraint, PropDecl, SchemaDecl, SchemaFile};
use crate::types::PropType;

const SCHEMA_IR_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaIR {
    pub ir_version: u32,
    pub interfaces: Vec<InterfaceIR>,
    pub nodes: Vec<NodeIR>,
    pub edges: Vec<EdgeIR>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterfaceIR {
    pub name: String,
    pub type_id: u32,
    pub properties: Vec<PropertyIR>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeIR {
    pub name: String,
    pub type_id: u32,
    pub annotations: Vec<Annotation>,
    pub implements: Vec<String>,
    pub properties: Vec<PropertyIR>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeIR {
    pub name: String,
    pub type_id: u32,
    pub from_type: String,
    pub to_type: String,
    pub cardinality: Cardinality,
    pub annotations: Vec<Annotation>,
    pub properties: Vec<PropertyIR>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyIR {
    pub name: String,
    pub prop_id: u32,
    pub prop_type: PropType,
    pub annotations: Vec<Annotation>,
}

pub fn build_schema_ir(schema: &SchemaFile) -> Result<SchemaIR> {
    let mut seen_type_ids = HashMap::<u32, String>::new();
    let mut interfaces = Vec::new();
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for decl in &schema.declarations {
        match decl {
            SchemaDecl::Interface(interface) => {
                let type_id = stable_type_id("interface", &interface.name);
                check_type_id_collision(&mut seen_type_ids, type_id, &interface.name)?;
                interfaces.push(InterfaceIR {
                    name: interface.name.clone(),
                    type_id,
                    properties: canonical_properties(
                        "interface",
                        &interface.name,
                        &interface.properties,
                    )?,
                });
            }
            SchemaDecl::Node(node) => {
                let type_id = stable_type_id("node", &node.name);
                check_type_id_collision(&mut seen_type_ids, type_id, &node.name)?;
                nodes.push(NodeIR {
                    name: node.name.clone(),
                    type_id,
                    annotations: canonical_annotations(&node.annotations),
                    implements: canonical_strings(&node.implements),
                    properties: canonical_properties("node", &node.name, &node.properties)?,
                    constraints: canonical_constraints(&node.constraints),
                });
            }
            SchemaDecl::Edge(edge) => {
                let type_id = stable_type_id("edge", &edge.name);
                check_type_id_collision(&mut seen_type_ids, type_id, &edge.name)?;
                edges.push(EdgeIR {
                    name: edge.name.clone(),
                    type_id,
                    from_type: edge.from_type.clone(),
                    to_type: edge.to_type.clone(),
                    cardinality: edge.cardinality.clone(),
                    annotations: canonical_annotations(&edge.annotations),
                    properties: canonical_properties("edge", &edge.name, &edge.properties)?,
                    constraints: canonical_constraints(&edge.constraints),
                });
            }
        }
    }

    interfaces.sort_by(|a, b| a.name.cmp(&b.name));
    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    edges.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(SchemaIR {
        ir_version: SCHEMA_IR_VERSION,
        interfaces,
        nodes,
        edges,
    })
}

pub fn build_catalog_from_ir(ir: &SchemaIR) -> Result<Catalog> {
    if ir.ir_version != SCHEMA_IR_VERSION {
        return Err(NanoError::Catalog(format!(
            "unsupported schema ir_version {} (expected {})",
            ir.ir_version, SCHEMA_IR_VERSION
        )));
    }

    let schema = SchemaFile {
        declarations: ir
            .interfaces
            .iter()
            .map(|interface| {
                SchemaDecl::Interface(crate::schema::ast::InterfaceDecl {
                    name: interface.name.clone(),
                    properties: interface
                        .properties
                        .iter()
                        .map(property_decl_from_ir)
                        .collect(),
                })
            })
            .chain(ir.nodes.iter().map(|node| {
                SchemaDecl::Node(crate::schema::ast::NodeDecl {
                    name: node.name.clone(),
                    annotations: node.annotations.clone(),
                    implements: node.implements.clone(),
                    properties: node.properties.iter().map(property_decl_from_ir).collect(),
                    constraints: node.constraints.clone(),
                })
            }))
            .chain(ir.edges.iter().map(|edge| {
                SchemaDecl::Edge(crate::schema::ast::EdgeDecl {
                    name: edge.name.clone(),
                    from_type: edge.from_type.clone(),
                    to_type: edge.to_type.clone(),
                    cardinality: edge.cardinality.clone(),
                    annotations: edge.annotations.clone(),
                    properties: edge.properties.iter().map(property_decl_from_ir).collect(),
                    constraints: edge.constraints.clone(),
                })
            }))
            .collect(),
    };

    build_catalog(&schema)
}

pub fn schema_ir_json(ir: &SchemaIR) -> Result<String> {
    serde_json::to_string(ir)
        .map_err(|err| NanoError::Catalog(format!("serialize schema ir error: {}", err)))
}

pub fn schema_ir_pretty_json(ir: &SchemaIR) -> Result<String> {
    serde_json::to_string_pretty(ir)
        .map_err(|err| NanoError::Catalog(format!("serialize schema ir error: {}", err)))
}

pub fn schema_ir_hash(ir: &SchemaIR) -> Result<String> {
    let json = schema_ir_json(ir)?;
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn property_decl_from_ir(property: &PropertyIR) -> PropDecl {
    PropDecl {
        name: property.name.clone(),
        prop_type: property.prop_type.clone(),
        annotations: property.annotations.clone(),
    }
}

fn canonical_strings(values: &[String]) -> Vec<String> {
    let mut values = values.to_vec();
    values.sort();
    values.dedup();
    values
}

fn canonical_annotations(annotations: &[Annotation]) -> Vec<Annotation> {
    let mut annotations = annotations.to_vec();
    annotations.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value))
    });
    annotations
}

fn canonical_prop_type(prop_type: &PropType) -> PropType {
    let mut normalized = prop_type.clone();
    if let Some(values) = &mut normalized.enum_values {
        values.sort();
        values.dedup();
    }
    normalized
}

fn canonical_properties(
    kind: &str,
    owner_name: &str,
    properties: &[PropDecl],
) -> Result<Vec<PropertyIR>> {
    let mut seen_prop_ids = HashMap::<u32, String>::new();
    let owner_key = format!("{}:{}", kind, owner_name);
    let mut canonical = properties
        .iter()
        .map(|property| {
            let prop_id = stable_prop_id(&owner_key, &property.name);
            if let Some(previous) = seen_prop_ids.insert(prop_id, property.name.clone()) {
                return Err(NanoError::Catalog(format!(
                    "property id collision on {}: '{}' and '{}' both hash to {}",
                    owner_name, previous, property.name, prop_id
                )));
            }
            Ok(PropertyIR {
                name: property.name.clone(),
                prop_id,
                prop_type: canonical_prop_type(&property.prop_type),
                annotations: canonical_annotations(&property.annotations),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    canonical.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(canonical)
}

fn canonical_constraints(constraints: &[Constraint]) -> Vec<Constraint> {
    let mut constraints = constraints
        .iter()
        .cloned()
        .map(normalize_constraint)
        .collect::<Vec<_>>();
    constraints.sort_by_key(constraint_sort_key);
    constraints
}

fn normalize_constraint(constraint: Constraint) -> Constraint {
    match constraint {
        Constraint::Key(mut columns) => {
            columns.sort();
            Constraint::Key(columns)
        }
        Constraint::Unique(mut columns) => {
            columns.sort();
            Constraint::Unique(columns)
        }
        Constraint::Index(mut columns) => {
            columns.sort();
            Constraint::Index(columns)
        }
        other => other,
    }
}

fn constraint_sort_key(constraint: &Constraint) -> String {
    match constraint {
        Constraint::Key(columns) => format!("key:{}", columns.join(",")),
        Constraint::Unique(columns) => format!("unique:{}", columns.join(",")),
        Constraint::Index(columns) => format!("index:{}", columns.join(",")),
        Constraint::Range { property, min, max } => {
            format!("range:{}:{:?}:{:?}", property, min, max)
        }
        Constraint::Check { property, pattern } => format!("check:{}:{}", property, pattern),
    }
}

fn stable_type_id(kind: &str, name: &str) -> u32 {
    fnv1a_u32(&format!("{}:{}", kind, name))
}

fn stable_prop_id(owner: &str, name: &str) -> u32 {
    fnv1a_u32(&format!("{}:{}", owner, name))
}

fn fnv1a_u32(value: &str) -> u32 {
    let mut hash: u32 = 2_166_136_261;
    for byte in value.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    if hash == 0 { 1 } else { hash }
}

fn check_type_id_collision(
    seen_type_ids: &mut HashMap<u32, String>,
    type_id: u32,
    name: &str,
) -> Result<()> {
    if let Some(previous) = seen_type_ids.insert(type_id, name.to_string()) {
        return Err(NanoError::Catalog(format!(
            "type id collision: '{}' and '{}' both hash to {}",
            previous, name, type_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::schema::parser::parse_schema;

    #[test]
    fn schema_ir_hash_is_stable_across_source_ordering_noise() {
        let schema_a = parse_schema(
            r#"
node Person {
    age: I32?
    name: String @key
}

edge Knows: Person -> Person {
    since: Date?
}
"#,
        )
        .unwrap();
        let schema_b = parse_schema(
            r#"
edge Knows: Person -> Person {
    since: Date?
}

node Person {
    name: String @key
    age: I32?
}
"#,
        )
        .unwrap();

        let ir_a = build_schema_ir(&schema_a).unwrap();
        let ir_b = build_schema_ir(&schema_b).unwrap();
        assert_eq!(ir_a, ir_b);
        assert_eq!(
            schema_ir_hash(&ir_a).unwrap(),
            schema_ir_hash(&ir_b).unwrap()
        );
    }

    #[test]
    fn build_catalog_from_ir_round_trips_core_catalog_fields() {
        let schema = parse_schema(
            r#"
node Person @description("person") {
    name: String @key
    age: I32? @description("age")
}

edge Knows: Person -> Person @instruction("friendship") {
    since: Date?
}
"#,
        )
        .unwrap();
        let direct = build_catalog(&schema).unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let rebuilt = build_catalog_from_ir(&ir).unwrap();

        assert_eq!(direct.node_types.len(), rebuilt.node_types.len());
        assert_eq!(direct.edge_types.len(), rebuilt.edge_types.len());
        assert_eq!(
            direct.node_types["Person"].key_property(),
            rebuilt.node_types["Person"].key_property()
        );
        assert_eq!(
            direct.edge_types["Knows"].cardinality,
            rebuilt.edge_types["Knows"].cardinality
        );
    }
}
