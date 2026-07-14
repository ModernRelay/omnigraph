//! Canonical, identity-free schema semantics.
//!
//! A `.pg` source file is not authoritative for stable identity.  This module
//! projects the parsed source into the deterministic semantic shape that is
//! compared with `_schema.pg` and fed to the identity resolver.  Migration
//! hints are retained for resolution but deliberately skipped by shape
//! serialization and hashing.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{CompilerError, Result};
use crate::schema::ast::{
    Annotation, Cardinality, Constraint, InterfacePropertyOrigin, PropDecl, SchemaDecl, SchemaFile,
    rename_from_annotation,
};
use crate::types::PropType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaShape {
    pub interfaces: Vec<InterfaceShape>,
    pub nodes: Vec<NodeShape>,
    pub edges: Vec<EdgeShape>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterfaceShape {
    pub name: String,
    pub properties: Vec<PropertyShape>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeShape {
    pub name: String,
    #[serde(skip)]
    pub rename_from: Option<String>,
    pub annotations: Vec<Annotation>,
    pub implements: Vec<String>,
    pub properties: Vec<PropertyShape>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeShape {
    pub name: String,
    #[serde(skip)]
    pub rename_from: Option<String>,
    pub from_type: String,
    pub to_type: String,
    pub cardinality: Cardinality,
    pub annotations: Vec<Annotation>,
    pub properties: Vec<PropertyShape>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyShape {
    pub name: String,
    #[serde(skip)]
    pub rename_from: Option<String>,
    pub prop_type: PropType,
    /// Opaque, non-relational annotations.  Known relationships such as
    /// `@embed`, `@key`, `@unique`, and `@index` have typed fields below.
    pub annotations: Vec<Annotation>,
    pub property_constraints: Vec<PropertyConstraintShape>,
    pub embed_source: Option<EmbedSourceShape>,
    /// True only when this effective property was written in the node body.
    /// Interface and edge properties are declarations and therefore true.
    pub declared_directly: bool,
    /// Every interface property contract satisfied by this node-effective
    /// property.  Empty for interface and edge declarations.
    pub satisfies_interface_properties: Vec<ShapePropertyRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShapePropertyRef {
    pub owner_name: String,
    pub property_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedSourceShape {
    pub property_name: String,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyConstraintShape {
    Key,
    Unique,
    Index,
}

pub fn compile_schema_shape(schema: &SchemaFile) -> Result<SchemaShape> {
    let interfaces_by_name = schema
        .declarations
        .iter()
        .filter_map(|decl| match decl {
            SchemaDecl::Interface(interface) => Some((interface.name.as_str(), interface)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    let mut interfaces = Vec::new();
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for declaration in &schema.declarations {
        match declaration {
            SchemaDecl::Interface(interface) => {
                interfaces.push(InterfaceShape {
                    name: interface.name.clone(),
                    properties: canonical_properties(
                        &interface.name,
                        &interface.properties,
                        |_| (true, Vec::new()),
                    )?,
                });
            }
            SchemaDecl::Node(node) => {
                let mut implements = node.implements.clone();
                implements.sort();
                implements.dedup();

                let mut properties = Vec::with_capacity(node.properties.len());
                for effective in &node.properties {
                    let origin = node.property_origins.get(&effective.name);
                    let declared_directly = origin.map_or(true, |origin| origin.declared_directly);
                    let mut contributions = origin
                        .map(|origin| origin.interface_properties.clone())
                        .unwrap_or_default();
                    contributions.sort();
                    contributions.dedup();

                    let semantic_source = if declared_directly || contributions.is_empty() {
                        effective
                    } else {
                        canonical_interface_property(
                            &node.name,
                            &effective.name,
                            &contributions,
                            &interfaces_by_name,
                        )?
                    };
                    let mut property = property_shape(semantic_source, declared_directly)?;
                    property.satisfies_interface_properties = contributions
                        .iter()
                        .map(|origin| ShapePropertyRef {
                            owner_name: origin.interface_name.clone(),
                            property_name: origin.property_name.clone(),
                        })
                        .collect();
                    properties.push(property);
                }
                properties.sort_by(|left, right| left.name.cmp(&right.name));

                let mut constraints = if node.property_origins.is_empty() {
                    // Hand-built ASTs predating provenance are treated as
                    // entirely direct declarations.
                    node.constraints.clone()
                } else {
                    node.source_constraints.clone()
                };
                // Rebuild injected property-level constraints from every
                // contributor.  This removes the parser's historical
                // first-interface/source-order dependence.
                for property in &properties {
                    if property.declared_directly {
                        continue;
                    }
                    for relation in &property.property_constraints {
                        constraints.push(match relation {
                            PropertyConstraintShape::Key => {
                                Constraint::Key(vec![property.name.clone()])
                            }
                            PropertyConstraintShape::Unique => {
                                Constraint::Unique(vec![property.name.clone()])
                            }
                            PropertyConstraintShape::Index => {
                                Constraint::Index(vec![property.name.clone()])
                            }
                        });
                    }
                }

                nodes.push(NodeShape {
                    name: node.name.clone(),
                    rename_from: rename_from_annotation(
                        &node.annotations,
                        &format!("node {}", node.name),
                    )?
                    .map(str::to_string),
                    annotations: canonical_annotations(&node.annotations),
                    implements,
                    properties,
                    constraints: canonical_constraints(&constraints),
                });
            }
            SchemaDecl::Edge(edge) => {
                edges.push(EdgeShape {
                    name: edge.name.clone(),
                    rename_from: rename_from_annotation(
                        &edge.annotations,
                        &format!("edge {}", edge.name),
                    )?
                    .map(str::to_string),
                    from_type: edge.from_type.clone(),
                    to_type: edge.to_type.clone(),
                    cardinality: edge.cardinality.clone(),
                    annotations: canonical_annotations(&edge.annotations),
                    properties: canonical_properties(&edge.name, &edge.properties, |_| {
                        (true, Vec::new())
                    })?,
                    constraints: canonical_constraints(&edge.constraints),
                });
            }
        }
    }

    interfaces.sort_by(|left, right| left.name.cmp(&right.name));
    nodes.sort_by(|left, right| left.name.cmp(&right.name));
    edges.sort_by(|left, right| left.name.cmp(&right.name));

    let shape = SchemaShape {
        interfaces,
        nodes,
        edges,
    };
    validate_shape(&shape)?;
    Ok(shape)
}

pub fn schema_shape_json(shape: &SchemaShape) -> Result<String> {
    serde_json::to_string(shape)
        .map_err(|error| CompilerError::Catalog(format!("serialize schema shape error: {error}")))
}

pub fn schema_shape_pretty_json(shape: &SchemaShape) -> Result<String> {
    serde_json::to_string_pretty(shape)
        .map_err(|error| CompilerError::Catalog(format!("serialize schema shape error: {error}")))
}

pub fn schema_shape_hash(shape: &SchemaShape) -> Result<String> {
    let json = schema_shape_json(shape)?;
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn canonical_properties(
    owner_name: &str,
    properties: &[PropDecl],
    provenance: impl Fn(&PropDecl) -> (bool, Vec<ShapePropertyRef>),
) -> Result<Vec<PropertyShape>> {
    let mut result = properties
        .iter()
        .map(|property| {
            let (declared_directly, satisfies) = provenance(property);
            let mut shape = property_shape(property, declared_directly)?;
            shape.satisfies_interface_properties = satisfies;
            Ok(shape)
        })
        .collect::<Result<Vec<_>>>()?;
    result.sort_by(|left, right| left.name.cmp(&right.name));
    if result.windows(2).any(|pair| pair[0].name == pair[1].name) {
        return Err(CompilerError::Catalog(format!(
            "duplicate property in schema shape for '{owner_name}'"
        )));
    }
    Ok(result)
}

fn canonical_interface_property<'a>(
    node_name: &str,
    property_name: &str,
    contributions: &[InterfacePropertyOrigin],
    interfaces: &HashMap<&str, &'a crate::schema::ast::InterfaceDecl>,
) -> Result<&'a PropDecl> {
    let mut candidates = Vec::with_capacity(contributions.len());
    for contribution in contributions {
        let interface = interfaces
            .get(contribution.interface_name.as_str())
            .ok_or_else(|| {
                CompilerError::Catalog(format!(
                    "node '{node_name}' property '{property_name}' references missing interface '{}'",
                    contribution.interface_name
                ))
            })?;
        let property = interface
            .properties
            .iter()
            .find(|property| property.name == contribution.property_name)
            .ok_or_else(|| {
                CompilerError::Catalog(format!(
                    "node '{node_name}' property '{property_name}' references missing interface property '{}.{}'",
                    contribution.interface_name, contribution.property_name
                ))
            })?;
        candidates.push(property);
    }
    let first = candidates.first().copied().ok_or_else(|| {
        CompilerError::Catalog(format!(
            "injected property '{node_name}.{property_name}' has no interface contributor"
        ))
    })?;
    let first_semantics = property_semantic_key(first)?;
    for candidate in candidates.iter().skip(1) {
        if property_semantic_key(candidate)? != first_semantics {
            return Err(CompilerError::Catalog(format!(
                "interfaces contributing '{node_name}.{property_name}' have incompatible property semantics"
            )));
        }
    }
    Ok(first)
}

fn property_semantic_key(property: &PropDecl) -> Result<String> {
    serde_json::to_string(&property_shape(property, false)?)
        .map_err(|error| CompilerError::Catalog(format!("serialize property shape error: {error}")))
}

fn property_shape(property: &PropDecl, declared_directly: bool) -> Result<PropertyShape> {
    let mut prop_type = property.prop_type.clone();
    if let Some(values) = &mut prop_type.enum_values {
        values.sort();
        values.dedup();
    }
    let mut property_constraints = property
        .annotations
        .iter()
        .filter_map(|annotation| match annotation.name.as_str() {
            "key" if annotation.value.is_none() => Some(PropertyConstraintShape::Key),
            "unique" if annotation.value.is_none() => Some(PropertyConstraintShape::Unique),
            "index" if annotation.value.is_none() => Some(PropertyConstraintShape::Index),
            _ => None,
        })
        .collect::<Vec<_>>();
    property_constraints.sort();
    property_constraints.dedup();
    let embed_source = property
        .annotations
        .iter()
        .find(|annotation| annotation.name == "embed")
        .and_then(|annotation| {
            annotation.value.as_ref().map(|source| EmbedSourceShape {
                property_name: source.clone(),
                model: annotation.kwargs.get("model").cloned(),
            })
        });
    Ok(PropertyShape {
        name: property.name.clone(),
        rename_from: rename_from_annotation(
            &property.annotations,
            &format!("property {}", property.name),
        )?
        .map(str::to_string),
        prop_type,
        annotations: canonical_annotations(&property.annotations),
        property_constraints,
        embed_source,
        declared_directly,
        satisfies_interface_properties: Vec::new(),
    })
}

fn canonical_annotations(annotations: &[Annotation]) -> Vec<Annotation> {
    let mut result = annotations
        .iter()
        .filter(|annotation| {
            !matches!(
                annotation.name.as_str(),
                "rename_from" | "embed" | "key" | "unique" | "index"
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    result.sort_by_key(|annotation| {
        serde_json::to_string(annotation).expect("Annotation serialization is infallible")
    });
    result.dedup();
    result
}

pub(crate) fn canonical_constraints(constraints: &[Constraint]) -> Vec<Constraint> {
    let mut result = constraints
        .iter()
        .cloned()
        .map(normalize_constraint)
        .collect::<Vec<_>>();
    result.sort_by_key(constraint_sort_key);
    result.dedup();
    result
}

fn normalize_constraint(constraint: Constraint) -> Constraint {
    match constraint {
        Constraint::Key(mut columns) => {
            columns.sort();
            columns.dedup();
            Constraint::Key(columns)
        }
        Constraint::Unique(mut columns) => {
            columns.sort();
            columns.dedup();
            Constraint::Unique(columns)
        }
        Constraint::Index(mut columns) => {
            columns.sort();
            columns.dedup();
            Constraint::Index(columns)
        }
        other => other,
    }
}

pub(crate) fn constraint_sort_key(constraint: &Constraint) -> String {
    match constraint {
        Constraint::Key(columns) => format!("key:{}", columns.join(",")),
        Constraint::Unique(columns) => format!("unique:{}", columns.join(",")),
        Constraint::Index(columns) => format!("index:{}", columns.join(",")),
        Constraint::Range { property, min, max } => {
            format!("range:{property}:{min:?}:{max:?}")
        }
        Constraint::Check { property, pattern } => format!("check:{property}:{pattern}"),
    }
}

fn validate_shape(shape: &SchemaShape) -> Result<()> {
    let interface_names = shape
        .interfaces
        .iter()
        .map(|interface| interface.name.as_str())
        .collect::<BTreeSet<_>>();
    let node_names = shape
        .nodes
        .iter()
        .map(|node| node.name.as_str())
        .collect::<BTreeSet<_>>();
    if interface_names.len() != shape.interfaces.len() {
        return Err(CompilerError::Catalog(
            "duplicate interface name in schema shape".to_string(),
        ));
    }
    if node_names.len() != shape.nodes.len() {
        return Err(CompilerError::Catalog(
            "duplicate node name in schema shape".to_string(),
        ));
    }
    let mut edge_names = BTreeSet::new();
    for edge in &shape.edges {
        if !edge_names.insert(edge.name.as_str()) {
            return Err(CompilerError::Catalog(format!(
                "duplicate edge name '{}' in schema shape",
                edge.name
            )));
        }
        if !node_names.contains(edge.from_type.as_str())
            || !node_names.contains(edge.to_type.as_str())
        {
            return Err(CompilerError::Catalog(format!(
                "edge '{}' has an unresolved endpoint",
                edge.name
            )));
        }
    }
    let interface_properties = shape
        .interfaces
        .iter()
        .flat_map(|interface| {
            interface
                .properties
                .iter()
                .map(move |property| (interface.name.as_str(), property.name.as_str()))
        })
        .collect::<BTreeSet<_>>();
    for node in &shape.nodes {
        for interface in &node.implements {
            if !interface_names.contains(interface.as_str()) {
                return Err(CompilerError::Catalog(format!(
                    "node '{}' implements unknown interface '{}'",
                    node.name, interface
                )));
            }
        }
        for property in &node.properties {
            for satisfied in &property.satisfies_interface_properties {
                if !interface_properties.contains(&(
                    satisfied.owner_name.as_str(),
                    satisfied.property_name.as_str(),
                )) {
                    return Err(CompilerError::Catalog(format!(
                        "node property '{}.{}' has an unresolved interface-property contribution",
                        node.name, property.name
                    )));
                }
            }
            if let Some(embed) = &property.embed_source
                && !node
                    .properties
                    .iter()
                    .any(|candidate| candidate.name == embed.property_name)
            {
                return Err(CompilerError::Catalog(format!(
                    "property '{}.{}' embeds unknown source property '{}'",
                    node.name, property.name, embed.property_name
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parser::parse_schema;

    #[test]
    fn shape_hash_ignores_source_order_comments_and_rename_hints() {
        let left = parse_schema("node Person {\n name: String @key\n age: I32?\n}\n").unwrap();
        let right = parse_schema(
            "// noise\nnode Person @rename_from(\"OldPerson\") {\n age: I32?\n name: String @key @rename_from(\"old_name\")\n}\n",
        )
        .unwrap();
        let left = compile_schema_shape(&left).unwrap();
        let right = compile_schema_shape(&right).unwrap();
        assert_eq!(
            schema_shape_hash(&left).unwrap(),
            schema_shape_hash(&right).unwrap()
        );
    }

    #[test]
    fn shape_compilation_revalidates_rename_hint_structure() {
        let mut schema = parse_schema("node Person @rename_from(\"OldPerson\") {}").unwrap();
        let SchemaDecl::Node(node) = &mut schema.declarations[0] else {
            unreachable!()
        };
        node.annotations[0]
            .kwargs
            .insert("typo".to_string(), "ignored".to_string());

        let error = compile_schema_shape(&schema).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not accept keyword arguments")
        );
    }

    #[test]
    fn shape_preserves_direct_and_all_interface_contributors() {
        let schema = parse_schema(
            r#"
interface A { name: String }
interface B { name: String }
node Injected implements B, A {}
node Direct implements A, B { name: String }
"#,
        )
        .unwrap();
        let shape = compile_schema_shape(&schema).unwrap();
        let injected = &shape
            .nodes
            .iter()
            .find(|node| node.name == "Injected")
            .unwrap()
            .properties[0];
        assert!(!injected.declared_directly);
        assert_eq!(injected.satisfies_interface_properties.len(), 2);
        assert_eq!(injected.satisfies_interface_properties[0].owner_name, "A");
        let direct = &shape
            .nodes
            .iter()
            .find(|node| node.name == "Direct")
            .unwrap()
            .properties[0];
        assert!(direct.declared_directly);
        assert_eq!(direct.satisfies_interface_properties.len(), 2);
    }
}
