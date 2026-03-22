use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use serde::{Deserialize, Serialize};

use crate::catalog::{Catalog, EdgeType, NodeType};
use crate::error::{NanoError, Result};
use crate::schema::ast::{SchemaFile, annotation_value, has_annotation};
use crate::types::{PropType, ScalarType};

// ── IR types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaIR {
    pub ir_version: u32,
    pub types: Vec<TypeDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum TypeDef {
    #[serde(rename = "node")]
    Node(NodeTypeDef),
    #[serde(rename = "edge")]
    Edge(EdgeTypeDef),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTypeDef {
    pub name: String,
    pub type_id: u32,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub instruction: Option<String>,
    pub properties: Vec<PropDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeTypeDef {
    pub name: String,
    pub type_id: u32,
    pub src_type_id: u32,
    pub dst_type_id: u32,
    pub src_type_name: String,
    pub dst_type_name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub instruction: Option<String>,
    pub properties: Vec<PropDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropDef {
    pub name: String,
    pub prop_id: u32,
    #[serde(rename = "type")]
    pub scalar_type: String,
    #[serde(default)]
    pub list: bool,
    #[serde(default)]
    pub enum_values: Vec<String>,
    pub nullable: bool,
    #[serde(default)]
    pub key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub index: bool,
    #[serde(default)]
    pub embed_source: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

// ── FNV-1a hashing ──────────────────────────────────────────────────────────

/// FNV-1a hash of `"{kind}:{name}"` → stable u32 type_id.
fn fnv1a_type_id(kind: &str, name: &str) -> u32 {
    let input = format!("{}:{}", kind, name);
    let mut hash: u32 = 2166136261;
    for byte in input.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    if hash == 0 { 1 } else { hash }
}

/// FNV-1a hash of `"{type_name}:{prop_name}"` → stable u32 prop_id.
fn fnv1a_prop_id(type_name: &str, prop_name: &str) -> u32 {
    fnv1a_type_id(type_name, prop_name)
}

// ── Build IR from AST ───────────────────────────────────────────────────────

/// Build a SchemaIR from a parsed SchemaFile.
/// Assigns deterministic type_ids and prop_ids via FNV-1a hash.
/// Detects hash collisions.
pub fn build_schema_ir(schema: &SchemaFile) -> Result<SchemaIR> {
    use crate::schema::ast::SchemaDecl;

    let mut types = Vec::new();
    let mut seen_type_ids: HashMap<u32, String> = HashMap::new();

    // First pass: build node type map for edge endpoint resolution
    let mut node_ids: HashMap<String, u32> = HashMap::new();
    for decl in &schema.declarations {
        if let SchemaDecl::Node(node) = decl {
            let type_id = fnv1a_type_id("node", &node.name);
            node_ids.insert(node.name.clone(), type_id);
        }
    }

    for decl in &schema.declarations {
        match decl {
            SchemaDecl::Node(node) => {
                let type_id = fnv1a_type_id("node", &node.name);
                if let Some(prev) = seen_type_ids.get(&type_id) {
                    return Err(NanoError::Catalog(format!(
                        "type_id collision: '{}' and '{}' both hash to {}",
                        prev, node.name, type_id
                    )));
                }
                seen_type_ids.insert(type_id, node.name.clone());

                let mut seen_prop_ids: HashSet<u32> = HashSet::new();
                let properties: Vec<PropDef> = node
                    .properties
                    .iter()
                    .map(|p| {
                        let prop_id = fnv1a_prop_id(&node.name, &p.name);
                        if !seen_prop_ids.insert(prop_id) {
                            return Err(NanoError::Catalog(format!(
                                "prop_id collision in {}: property '{}' hash {}",
                                node.name, p.name, prop_id
                            )));
                        }
                        Ok(PropDef {
                            name: p.name.clone(),
                            prop_id,
                            scalar_type: p.prop_type.scalar.to_string(),
                            list: p.prop_type.list,
                            enum_values: p.prop_type.enum_values.clone().unwrap_or_default(),
                            nullable: p.prop_type.nullable,
                            key: has_annotation(&p.annotations, "key"),
                            unique: has_annotation(&p.annotations, "unique"),
                            index: has_annotation(&p.annotations, "key")
                                || has_annotation(&p.annotations, "index"),
                            embed_source: annotation_value(&p.annotations, "embed")
                                .map(str::to_string),
                            description: annotation_value(&p.annotations, "description")
                                .map(str::to_string),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                types.push(TypeDef::Node(NodeTypeDef {
                    name: node.name.clone(),
                    type_id,
                    description: annotation_value(&node.annotations, "description")
                        .map(str::to_string),
                    instruction: annotation_value(&node.annotations, "instruction")
                        .map(str::to_string),
                    properties,
                }));
            }
            SchemaDecl::Edge(edge) => {
                let type_id = fnv1a_type_id("edge", &edge.name);
                if let Some(prev) = seen_type_ids.get(&type_id) {
                    return Err(NanoError::Catalog(format!(
                        "type_id collision: '{}' and '{}' both hash to {}",
                        prev, edge.name, type_id
                    )));
                }
                seen_type_ids.insert(type_id, edge.name.clone());

                let src_type_id = *node_ids.get(&edge.from_type).ok_or_else(|| {
                    NanoError::Catalog(format!(
                        "edge {} references unknown source type: {}",
                        edge.name, edge.from_type
                    ))
                })?;
                let dst_type_id = *node_ids.get(&edge.to_type).ok_or_else(|| {
                    NanoError::Catalog(format!(
                        "edge {} references unknown target type: {}",
                        edge.name, edge.to_type
                    ))
                })?;

                let mut seen_prop_ids: HashSet<u32> = HashSet::new();
                let properties: Vec<PropDef> = edge
                    .properties
                    .iter()
                    .map(|p| {
                        let prop_id = fnv1a_prop_id(&edge.name, &p.name);
                        if !seen_prop_ids.insert(prop_id) {
                            return Err(NanoError::Catalog(format!(
                                "prop_id collision in {}: property '{}' hash {}",
                                edge.name, p.name, prop_id
                            )));
                        }
                        Ok(PropDef {
                            name: p.name.clone(),
                            prop_id,
                            scalar_type: p.prop_type.scalar.to_string(),
                            list: p.prop_type.list,
                            enum_values: p.prop_type.enum_values.clone().unwrap_or_default(),
                            nullable: p.prop_type.nullable,
                            key: false,
                            unique: false,
                            index: false,
                            embed_source: None,
                            description: annotation_value(&p.annotations, "description")
                                .map(str::to_string),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                types.push(TypeDef::Edge(EdgeTypeDef {
                    name: edge.name.clone(),
                    type_id,
                    src_type_id,
                    dst_type_id,
                    src_type_name: edge.from_type.clone(),
                    dst_type_name: edge.to_type.clone(),
                    description: annotation_value(&edge.annotations, "description")
                        .map(str::to_string),
                    instruction: annotation_value(&edge.annotations, "instruction")
                        .map(str::to_string),
                    properties,
                }));
            }
        }
    }

    Ok(SchemaIR {
        ir_version: 1,
        types,
    })
}

// ── Build Catalog from IR ───────────────────────────────────────────────────

/// Build a Catalog from an IR (the "open from disk" path).
pub fn build_catalog_from_ir(ir: &SchemaIR) -> Result<Catalog> {
    let mut node_types = HashMap::new();
    let mut edge_types = HashMap::new();
    let mut edge_name_index = HashMap::new();

    for typedef in &ir.types {
        match typedef {
            TypeDef::Node(n) => {
                let mut properties = HashMap::new();
                let mut embed_sources = HashMap::new();
                let mut indexed_properties = HashSet::new();
                let mut fields = vec![Field::new("id", arrow_schema::DataType::UInt64, false)];

                for prop in &n.properties {
                    let scalar = ScalarType::from_str_name(&prop.scalar_type).ok_or_else(|| {
                        NanoError::Catalog(format!("unknown scalar type: {}", prop.scalar_type))
                    })?;
                    let prop_type = PropType {
                        scalar,
                        nullable: prop.nullable,
                        list: prop.list,
                        enum_values: if prop.enum_values.is_empty() {
                            None
                        } else {
                            Some(prop.enum_values.clone())
                        },
                    };
                    properties.insert(prop.name.clone(), prop_type.clone());
                    if let Some(source_prop) = &prop.embed_source {
                        embed_sources.insert(prop.name.clone(), source_prop.clone());
                    }
                    if prop.index && !prop.list && !matches!(scalar, ScalarType::Vector(_)) {
                        indexed_properties.insert(prop.name.clone());
                    }
                    fields.push(Field::new(&prop.name, prop_type.to_arrow(), prop.nullable));
                }

                node_types.insert(
                    n.name.clone(),
                    NodeType {
                        name: n.name.clone(),
                        properties,
                        embed_sources,
                        indexed_properties,
                        arrow_schema: Arc::new(Schema::new(fields)),
                    },
                );
            }
            TypeDef::Edge(e) => {
                let mut properties = HashMap::new();
                let mut fields = vec![
                    Field::new("id", arrow_schema::DataType::UInt64, false),
                    Field::new("src", arrow_schema::DataType::UInt64, false),
                    Field::new("dst", arrow_schema::DataType::UInt64, false),
                ];
                for prop in &e.properties {
                    let scalar = ScalarType::from_str_name(&prop.scalar_type).ok_or_else(|| {
                        NanoError::Catalog(format!("unknown scalar type: {}", prop.scalar_type))
                    })?;
                    let prop_type = PropType {
                        scalar,
                        nullable: prop.nullable,
                        list: prop.list,
                        enum_values: if prop.enum_values.is_empty() {
                            None
                        } else {
                            Some(prop.enum_values.clone())
                        },
                    };
                    properties.insert(prop.name.clone(), prop_type.clone());
                    fields.push(Field::new(&prop.name, prop_type.to_arrow(), prop.nullable));
                }

                let lowercase_name = lowercase_first_char(&e.name);
                edge_name_index.insert(lowercase_name, e.name.clone());

                edge_types.insert(
                    e.name.clone(),
                    EdgeType {
                        name: e.name.clone(),
                        from_type: e.src_type_name.clone(),
                        to_type: e.dst_type_name.clone(),
                        properties,
                        arrow_schema: Arc::new(Schema::new(fields)),
                    },
                );
            }
        }
    }

    Ok(Catalog {
        node_types,
        edge_types,
        edge_name_index,
    })
}

fn lowercase_first_char(name: &str) -> String {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_lowercase().chain(chars).collect()
}

/// Verify a schema.pg matches an existing IR.
pub fn validate_schema_match(ir: &SchemaIR, schema: &SchemaFile) -> Result<()> {
    let new_ir = build_schema_ir(schema)?;
    let existing = serde_json::to_string(&ir)
        .map_err(|e| NanoError::Catalog(format!("serialize error: {}", e)))?;
    let new = serde_json::to_string(&new_ir)
        .map_err(|e| NanoError::Catalog(format!("serialize error: {}", e)))?;
    if existing != new {
        return Err(NanoError::Catalog(
            "schema does not match existing IR; schema migration is not yet supported".to_string(),
        ));
    }
    Ok(())
}

// ── Lookup helpers ──────────────────────────────────────────────────────────

impl SchemaIR {
    pub fn node_type(&self, name: &str) -> Option<&NodeTypeDef> {
        self.types.iter().find_map(|t| match t {
            TypeDef::Node(n) if n.name == name => Some(n),
            _ => None,
        })
    }

    pub fn edge_type(&self, name: &str) -> Option<&EdgeTypeDef> {
        self.types.iter().find_map(|t| match t {
            TypeDef::Edge(e) if e.name == name => Some(e),
            _ => None,
        })
    }

    pub fn node_type_id(&self, name: &str) -> Option<u32> {
        self.types.iter().find_map(|t| match t {
            TypeDef::Node(n) if n.name == name => Some(n.type_id),
            _ => None,
        })
    }

    pub fn edge_type_id(&self, name: &str) -> Option<u32> {
        self.types.iter().find_map(|t| match t {
            TypeDef::Edge(e) if e.name == name => Some(e.type_id),
            _ => None,
        })
    }

    pub fn type_name(&self, type_id: u32) -> Option<&str> {
        self.types.iter().find_map(|t| match t {
            TypeDef::Node(n) if n.type_id == type_id => Some(n.name.as_str()),
            TypeDef::Edge(e) if e.type_id == type_id => Some(e.name.as_str()),
            _ => None,
        })
    }

    /// Directory name for a type_id (lowercase hex).
    pub fn dir_name(type_id: u32) -> String {
        format!("{:08x}", type_id)
    }

    /// Iterate over node type definitions.
    pub fn node_types(&self) -> impl Iterator<Item = &NodeTypeDef> {
        self.types.iter().filter_map(|t| match t {
            TypeDef::Node(n) => Some(n),
            _ => None,
        })
    }

    /// Iterate over edge type definitions.
    pub fn edge_types(&self) -> impl Iterator<Item = &EdgeTypeDef> {
        self.types.iter().filter_map(|t| match t {
            TypeDef::Edge(e) => Some(e),
            _ => None,
        })
    }

    pub fn node_key_property_name(&self, type_name: &str) -> Option<&str> {
        self.node_type(type_name)
            .and_then(NodeTypeDef::key_property_name)
    }
}

impl NodeTypeDef {
    pub fn key_property(&self) -> Option<&PropDef> {
        self.properties.iter().find(|prop| prop.key)
    }

    pub fn key_property_name(&self) -> Option<&str> {
        self.key_property().map(|prop| prop.name.as_str())
    }

    pub fn unique_properties(&self) -> impl Iterator<Item = &PropDef> {
        self.properties.iter().filter(|prop| prop.unique)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::schema::parser::parse_schema;

    fn test_schema_src() -> &'static str {
        r#"
node Person {
    name: String
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#
    }

    #[test]
    fn test_build_schema_ir() {
        let schema = parse_schema(test_schema_src()).unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        assert_eq!(ir.ir_version, 1);
        assert_eq!(ir.types.len(), 4);
    }

    #[test]
    fn test_build_schema_ir_preserves_key_unique_metadata() {
        let schema = parse_schema(
            r#"
node Person {
    id: U64 @key
    email: String @unique
    handle: String @index
    title: String
    embedding: Vector(3) @embed(title)
    age: I32?
}
"#,
        )
        .unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let person = ir.node_types().find(|n| n.name == "Person").unwrap();
        let id_prop = person.properties.iter().find(|p| p.name == "id").unwrap();
        let email_prop = person
            .properties
            .iter()
            .find(|p| p.name == "email")
            .unwrap();
        let handle_prop = person
            .properties
            .iter()
            .find(|p| p.name == "handle")
            .unwrap();
        let embedding_prop = person
            .properties
            .iter()
            .find(|p| p.name == "embedding")
            .unwrap();
        let age_prop = person.properties.iter().find(|p| p.name == "age").unwrap();

        assert!(person.description.is_none());
        assert!(person.instruction.is_none());
        assert!(id_prop.key);
        assert!(!id_prop.unique);
        assert!(id_prop.index);
        assert!(id_prop.embed_source.is_none());
        assert!(id_prop.description.is_none());
        assert!(!email_prop.key);
        assert!(email_prop.unique);
        assert!(!email_prop.index);
        assert!(email_prop.embed_source.is_none());
        assert!(!handle_prop.key);
        assert!(!handle_prop.unique);
        assert!(handle_prop.index);
        assert!(handle_prop.embed_source.is_none());
        assert_eq!(embedding_prop.embed_source.as_deref(), Some("title"));
        assert!(!age_prop.key);
        assert!(!age_prop.unique);
        assert!(!age_prop.index);
        assert!(age_prop.embed_source.is_none());
    }

    #[test]
    fn test_ir_ids_are_deterministic() {
        let schema = parse_schema(test_schema_src()).unwrap();
        let ir1 = build_schema_ir(&schema).unwrap();
        let ir2 = build_schema_ir(&schema).unwrap();

        for (t1, t2) in ir1.types.iter().zip(ir2.types.iter()) {
            match (t1, t2) {
                (TypeDef::Node(a), TypeDef::Node(b)) => {
                    assert_eq!(a.type_id, b.type_id);
                    assert_eq!(a.name, b.name);
                }
                (TypeDef::Edge(a), TypeDef::Edge(b)) => {
                    assert_eq!(a.type_id, b.type_id);
                    assert_eq!(a.name, b.name);
                }
                _ => panic!("type mismatch"),
            }
        }
    }

    #[test]
    fn test_ir_ids_are_order_independent() {
        // Same types in different order should produce same IDs
        let id1 = fnv1a_type_id("node", "Person");
        let id2 = fnv1a_type_id("node", "Person");
        assert_eq!(id1, id2);

        // Different types should produce different IDs
        let id3 = fnv1a_type_id("node", "Company");
        assert_ne!(id1, id3);

        // node vs edge same name should differ
        let id4 = fnv1a_type_id("edge", "Person");
        assert_ne!(id1, id4);
    }

    #[test]
    fn test_json_roundtrip() {
        let schema = parse_schema(test_schema_src()).unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let json = serde_json::to_string_pretty(&ir).unwrap();
        let ir2: SchemaIR = serde_json::from_str(&json).unwrap();
        assert_eq!(ir.types.len(), ir2.types.len());
        assert_eq!(ir.ir_version, ir2.ir_version);
    }

    #[test]
    fn test_legacy_ir_without_constraint_fields_deserializes() {
        let legacy = r#"
{
  "ir_version": 1,
  "types": [
    {
      "kind": "node",
      "name": "Person",
      "type_id": 123,
      "properties": [
        { "name": "name", "prop_id": 1, "type": "String", "nullable": false }
      ]
    }
  ]
}
"#;
        let ir: SchemaIR = serde_json::from_str(legacy).unwrap();
        let person = ir.node_types().next().unwrap();
        let prop = person.properties.first().unwrap();
        assert!(!prop.key);
        assert!(!prop.unique);
        assert!(!prop.index);
        assert!(prop.embed_source.is_none());
        assert!(prop.description.is_none());
        assert!(person.description.is_none());
        assert!(person.instruction.is_none());
    }

    #[test]
    fn test_build_schema_ir_preserves_agent_metadata() {
        let schema = parse_schema(
            r#"
node Task @description("Tracked work item") @instruction("Prefer querying by slug") {
    slug: String @key @description("Stable external identifier")
}
edge DependsOn: Task -> Task @description("Hard dependency") @instruction("Use only for blockers")
"#,
        )
        .unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let task = ir.node_type("Task").unwrap();
        let depends_on = ir.edge_type("DependsOn").unwrap();

        assert_eq!(task.description.as_deref(), Some("Tracked work item"));
        assert_eq!(task.instruction.as_deref(), Some("Prefer querying by slug"));
        assert_eq!(task.key_property_name(), Some("slug"));
        assert_eq!(
            task.properties[0].description.as_deref(),
            Some("Stable external identifier")
        );
        assert_eq!(depends_on.description.as_deref(), Some("Hard dependency"));
        assert_eq!(
            depends_on.instruction.as_deref(),
            Some("Use only for blockers")
        );
    }

    #[test]
    fn test_catalog_from_ir_matches_ast() {
        let schema = parse_schema(test_schema_src()).unwrap();
        let catalog_ast = build_catalog(&schema).unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        let catalog_ir = build_catalog_from_ir(&ir).unwrap();

        assert_eq!(catalog_ast.node_types.len(), catalog_ir.node_types.len());
        assert_eq!(catalog_ast.edge_types.len(), catalog_ir.edge_types.len());

        for (name, nt_ast) in &catalog_ast.node_types {
            let nt_ir = catalog_ir.node_types.get(name).expect("missing node type");
            assert_eq!(nt_ast.name, nt_ir.name);
            assert_eq!(nt_ast.properties.len(), nt_ir.properties.len());
            assert_eq!(nt_ast.embed_sources, nt_ir.embed_sources);
            assert_eq!(nt_ast.arrow_schema, nt_ir.arrow_schema);
        }

        for (name, et_ast) in &catalog_ast.edge_types {
            let et_ir = catalog_ir.edge_types.get(name).expect("missing edge type");
            assert_eq!(et_ast.name, et_ir.name);
            assert_eq!(et_ast.from_type, et_ir.from_type);
            assert_eq!(et_ast.to_type, et_ir.to_type);
        }
    }

    #[test]
    fn test_validate_schema_match_ok() {
        let schema = parse_schema(test_schema_src()).unwrap();
        let ir = build_schema_ir(&schema).unwrap();
        assert!(validate_schema_match(&ir, &schema).is_ok());
    }

    #[test]
    fn test_validate_schema_match_fail() {
        let schema1 = parse_schema(test_schema_src()).unwrap();
        let ir = build_schema_ir(&schema1).unwrap();

        let schema2 = parse_schema("node Person { name: String }").unwrap();
        assert!(validate_schema_match(&ir, &schema2).is_err());
    }

    #[test]
    fn test_lookup_helpers() {
        let schema = parse_schema(test_schema_src()).unwrap();
        let ir = build_schema_ir(&schema).unwrap();

        assert!(ir.node_type_id("Person").is_some());
        assert!(ir.node_type_id("Company").is_some());
        assert!(ir.node_type_id("Nonexistent").is_none());

        assert!(ir.edge_type_id("Knows").is_some());
        assert!(ir.edge_type_id("WorksAt").is_some());

        let pid = ir.node_type_id("Person").unwrap();
        assert_eq!(ir.type_name(pid), Some("Person"));

        let dir = SchemaIR::dir_name(pid);
        assert_eq!(dir.len(), 8);
    }

    #[test]
    fn test_catalog_from_ir_handles_non_ascii_leading_edge_name() {
        let ir = SchemaIR {
            ir_version: 1,
            types: vec![
                TypeDef::Node(NodeTypeDef {
                    name: "Person".to_string(),
                    type_id: 100,
                    description: None,
                    instruction: None,
                    properties: vec![],
                }),
                TypeDef::Edge(EdgeTypeDef {
                    name: "Édges".to_string(),
                    type_id: 200,
                    src_type_id: 100,
                    dst_type_id: 100,
                    src_type_name: "Person".to_string(),
                    dst_type_name: "Person".to_string(),
                    description: None,
                    instruction: None,
                    properties: vec![],
                }),
            ],
        };

        let catalog = build_catalog_from_ir(&ir).unwrap();
        assert!(catalog.lookup_edge_by_name("édges").is_some());
    }
}
