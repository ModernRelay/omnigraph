pub mod schema_ir;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{NanoError, Result};
use crate::schema::ast::{SchemaDecl, SchemaFile};
use crate::types::{PropType, ScalarType};

#[derive(Debug, Clone)]
pub struct Catalog {
    pub node_types: HashMap<String, NodeType>,
    pub edge_types: HashMap<String, EdgeType>,
    /// Maps lowercase edge name -> EdgeType key (e.g. "knows" -> "Knows")
    pub edge_name_index: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct NodeType {
    pub name: String,
    pub properties: HashMap<String, PropType>,
    /// Maps @embed target property -> source text property.
    pub embed_sources: HashMap<String, String>,
    pub indexed_properties: HashSet<String>,
    pub arrow_schema: SchemaRef,
}

#[derive(Debug, Clone)]
pub struct EdgeType {
    pub name: String,
    pub from_type: String,
    pub to_type: String,
    pub properties: HashMap<String, PropType>,
    pub arrow_schema: SchemaRef,
}

impl Catalog {
    pub fn lookup_edge_by_name(&self, name: &str) -> Option<&EdgeType> {
        // Try exact match first, then lowercase lookup
        if let Some(et) = self.edge_types.get(name) {
            return Some(et);
        }
        if let Some(key) = self.edge_name_index.get(name) {
            return self.edge_types.get(key);
        }
        None
    }
}

fn lowercase_first_char(name: &str) -> String {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_lowercase().chain(chars).collect()
}

pub fn build_catalog(schema: &SchemaFile) -> Result<Catalog> {
    let mut node_types = HashMap::new();
    let mut edge_types = HashMap::new();
    let mut edge_name_index = HashMap::new();

    // First pass: collect all node types
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
            let mut indexed_properties = HashSet::new();
            for prop in &node.properties {
                properties.insert(prop.name.clone(), prop.prop_type.clone());
                if let Some(source_prop) = prop
                    .annotations
                    .iter()
                    .find(|ann| ann.name == "embed")
                    .and_then(|ann| ann.value.clone())
                {
                    embed_sources.insert(prop.name.clone(), source_prop);
                }
                let scalar_index_eligible =
                    !prop.prop_type.list && !matches!(prop.prop_type.scalar, ScalarType::Vector(_));
                if scalar_index_eligible
                    && prop
                        .annotations
                        .iter()
                        .any(|a| a.name == "key" || a.name == "index")
                {
                    indexed_properties.insert(prop.name.clone());
                }
            }

            // Build Arrow schema: id: U64 + all properties
            let mut fields = vec![Field::new("id", DataType::UInt64, false)];
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
                    properties,
                    embed_sources,
                    indexed_properties,
                    arrow_schema,
                },
            );
        }
    }

    // Second pass: collect edge types, validate endpoints
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
            let mut fields = vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
            ];
            for prop in &edge.properties {
                properties.insert(prop.name.clone(), prop.prop_type.clone());
                fields.push(Field::new(
                    &prop.name,
                    prop.prop_type.to_arrow(),
                    prop.prop_type.nullable,
                ));
            }

            let lowercase_name = lowercase_first_char(&edge.name);
            edge_name_index.insert(lowercase_name, edge.name.clone());

            edge_types.insert(
                edge.name.clone(),
                EdgeType {
                    name: edge.name.clone(),
                    from_type: edge.from_type.clone(),
                    to_type: edge.to_type.clone(),
                    properties,
                    arrow_schema: Arc::new(Schema::new(fields)),
                },
            );
        }
    }

    Ok(Catalog {
        node_types,
        edge_types,
        edge_name_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::{EdgeDecl, NodeDecl};
    use crate::schema::parser::parse_schema;
    use crate::types::PropType;

    fn test_schema() -> &'static str {
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
edge WorksAt: Person -> Company {
    title: String?
}
"#
    }

    #[test]
    fn test_build_catalog() {
        let schema = parse_schema(test_schema()).unwrap();
        let catalog = build_catalog(&schema).unwrap();
        assert_eq!(catalog.node_types.len(), 2);
        assert_eq!(catalog.edge_types.len(), 2);
        assert!(catalog.node_types.contains_key("Person"));
        assert!(catalog.node_types.contains_key("Company"));
    }

    #[test]
    fn test_edge_lookup() {
        let schema = parse_schema(test_schema()).unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let edge = catalog.lookup_edge_by_name("knows").unwrap();
        assert_eq!(edge.from_type, "Person");
        assert_eq!(edge.to_type, "Person");
    }

    #[test]
    fn test_node_arrow_schema() {
        let schema = parse_schema(test_schema()).unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let person = &catalog.node_types["Person"];
        assert_eq!(person.arrow_schema.fields().len(), 3); // id, name, age
    }

    #[test]
    fn test_duplicate_node_error() {
        let input = r#"
node Person { name: String }
node Person { age: I32 }
"#;
        let schema = parse_schema(input).unwrap();
        assert!(build_catalog(&schema).is_err());
    }

    #[test]
    fn test_bad_edge_endpoint() {
        let input = r#"
node Person { name: String }
edge Knows: Person -> Alien
"#;
        let schema = parse_schema(input).unwrap();
        assert!(build_catalog(&schema).is_err());
    }

    #[test]
    fn test_edge_lookup_handles_non_ascii_leading_character() {
        let schema = SchemaFile {
            declarations: vec![
                SchemaDecl::Node(NodeDecl {
                    name: "Person".to_string(),
                    annotations: vec![],
                    parent: None,
                    properties: vec![crate::schema::ast::PropDecl {
                        name: "name".to_string(),
                        prop_type: PropType::scalar(ScalarType::String, false),
                        annotations: vec![],
                    }],
                }),
                SchemaDecl::Edge(EdgeDecl {
                    name: "Édges".to_string(),
                    from_type: "Person".to_string(),
                    to_type: "Person".to_string(),
                    annotations: vec![],
                    properties: vec![],
                }),
            ],
        };
        let catalog = build_catalog(&schema).unwrap();
        assert!(catalog.lookup_edge_by_name("édges").is_some());
    }
}
