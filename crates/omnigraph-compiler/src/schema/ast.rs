use crate::embedding_models::DEFAULT_EMBEDDING_MODEL;
use crate::types::PropType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaFile {
    #[serde(default)]
    pub config: SchemaConfig,
    pub declarations: Vec<SchemaDecl>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaConfig {
    pub embedding_model: String,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            embedding_model: DEFAULT_EMBEDDING_MODEL.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchemaDecl {
    Interface(InterfaceDecl),
    Node(NodeDecl),
    Edge(EdgeDecl),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterfaceDecl {
    pub name: String,
    pub properties: Vec<PropDecl>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeDecl {
    pub name: String,
    pub annotations: Vec<Annotation>,
    pub implements: Vec<String>,
    pub properties: Vec<PropDecl>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeDecl {
    pub name: String,
    pub from_type: String,
    pub to_type: String,
    pub cardinality: Cardinality,
    pub annotations: Vec<Annotation>,
    pub properties: Vec<PropDecl>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropDecl {
    pub name: String,
    pub prop_type: PropType,
    pub annotations: Vec<Annotation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Annotation {
    pub name: String,
    pub value: Option<String>,
}

/// A typed constraint declared in a node or edge body.
///
/// Property-level annotations (`@key`, `@unique`, `@index`) are desugared
/// into these during parsing, so both syntactic positions produce the same
/// representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Constraint {
    Key(Vec<String>),
    Unique(Vec<String>),
    Index(Vec<String>),
    Range {
        property: String,
        min: Option<ConstraintBound>,
        max: Option<ConstraintBound>,
    },
    Check {
        property: String,
        pattern: String,
    },
}

/// A numeric bound used in `@range` constraints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintBound {
    Integer(i64),
    Float(f64),
}

/// Edge cardinality: `@card(min..max)`. Default is `0..*`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cardinality {
    pub min: u32,
    pub max: Option<u32>,
}

impl Default for Cardinality {
    fn default() -> Self {
        Self { min: 0, max: None }
    }
}

impl Cardinality {
    pub fn is_default(&self) -> bool {
        self.min == 0 && self.max.is_none()
    }
}

pub fn has_annotation(annotations: &[Annotation], name: &str) -> bool {
    annotations.iter().any(|ann| ann.name == name)
}

pub fn annotation_value<'a>(annotations: &'a [Annotation], name: &str) -> Option<&'a str> {
    annotations
        .iter()
        .find(|ann| ann.name == name)
        .and_then(|ann| ann.value.as_deref())
}
