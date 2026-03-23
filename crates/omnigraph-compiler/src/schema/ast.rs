use crate::types::PropType;

#[derive(Debug, Clone)]
pub struct SchemaFile {
    pub declarations: Vec<SchemaDecl>,
}

#[derive(Debug, Clone)]
pub enum SchemaDecl {
    Node(NodeDecl),
    Edge(EdgeDecl),
}

#[derive(Debug, Clone)]
pub struct NodeDecl {
    pub name: String,
    pub annotations: Vec<Annotation>,
    pub parent: Option<String>,
    pub properties: Vec<PropDecl>,
}

#[derive(Debug, Clone)]
pub struct EdgeDecl {
    pub name: String,
    pub from_type: String,
    pub to_type: String,
    pub annotations: Vec<Annotation>,
    pub properties: Vec<PropDecl>,
}

#[derive(Debug, Clone)]
pub struct PropDecl {
    pub name: String,
    pub prop_type: PropType,
    pub annotations: Vec<Annotation>,
}

#[derive(Debug, Clone)]
pub struct Annotation {
    pub name: String,
    pub value: Option<String>,
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
