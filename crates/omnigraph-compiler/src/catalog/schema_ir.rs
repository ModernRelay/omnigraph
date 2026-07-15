//! Accepted, identity-bearing schema representation.
//!
//! Source names describe schema shape; they are not identity.  This module is
//! the only place that mints stable schema identities.  Evolution resolves a
//! desired identity-free [`SchemaShape`] against one accepted [`SchemaIR`],
//! preserving identities only for exact matches and explicit, same-kind,
//! same-owner renames.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::error::{CompilerError, Result, SchemaIdentityError};
use crate::schema::ast::{Annotation, Cardinality, Constraint, ConstraintBound};
use crate::types::PropType;

use super::schema_shape::{
    EdgeShape, EmbedSourceShape, InterfaceShape, NodeShape, PropertyConstraintShape, PropertyShape,
    SchemaShape, ShapePropertyRef, constraint_sort_key, schema_shape_hash,
};

pub const SCHEMA_IR_VERSION: u32 = 2;

/// Opaque namespace for every numeric identity in one graph root.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SchemaIdentityDomain(String);

impl SchemaIdentityDomain {
    pub fn new() -> Self {
        Self(Ulid::new().to_string())
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        let parsed = Ulid::from_string(value)
            .map_err(|error| SchemaIdentityError::InvalidDomain(format!("'{value}': {error}")))?;
        let canonical = parsed.to_string();
        if canonical != value {
            return Err(SchemaIdentityError::InvalidDomain(format!(
                "'{value}' is not a canonical uppercase ULID (expected '{canonical}')"
            ))
            .into());
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SchemaIdentityDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SchemaIdentityDomain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SchemaIdentityDomain {
    type Err = CompilerError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl<'de> Deserialize<'de> for SchemaIdentityDomain {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

macro_rules! identity_newtype {
    ($name:ident, $label:literal) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(NonZeroU64);

        impl $name {
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }

        impl TryFrom<u64> for $name {
            type Error = CompilerError;

            fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
                NonZeroU64::new(value).map(Self).ok_or_else(|| {
                    SchemaIdentityError::ZeroId {
                        entity: $label.to_string(),
                        value,
                    }
                    .into()
                })
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

identity_newtype!(StableTypeId, "stable type id");
identity_newtype!(StablePropertyId, "stable property id");
identity_newtype!(TableIncarnationId, "table incarnation id");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaIR {
    pub ir_version: u32,
    pub schema_identity_domain: SchemaIdentityDomain,
    /// The next value to consume.  Type, property, and incarnation IDs share
    /// this monotonically increasing, no-reuse sequence.
    pub next_identity_id: u64,
    pub interfaces: Vec<InterfaceIR>,
    pub nodes: Vec<NodeIR>,
    pub edges: Vec<EdgeIR>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterfaceIR {
    pub name: String,
    pub type_id: StableTypeId,
    pub properties: Vec<PropertyIR>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeIR {
    pub name: String,
    pub type_id: StableTypeId,
    pub table_incarnation_id: TableIncarnationId,
    pub annotations: Vec<Annotation>,
    pub implements: Vec<TypeRefIR>,
    pub properties: Vec<PropertyIR>,
    pub constraints: Vec<ConstraintIR>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeIR {
    pub name: String,
    pub type_id: StableTypeId,
    pub table_incarnation_id: TableIncarnationId,
    pub from_type: TypeRefIR,
    pub to_type: TypeRefIR,
    pub cardinality: Cardinality,
    pub annotations: Vec<Annotation>,
    pub properties: Vec<PropertyIR>,
    pub constraints: Vec<ConstraintIR>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyIR {
    pub name: String,
    pub property_id: StablePropertyId,
    pub prop_type: PropType,
    pub annotations: Vec<Annotation>,
    pub property_constraints: Vec<PropertyConstraintShape>,
    pub embed_source: Option<EmbedSourceIR>,
    pub declared_directly: bool,
    pub satisfies_interface_properties: Vec<PropertyRefIR>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TypeRefIR {
    pub type_id: StableTypeId,
    pub type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PropertyRefIR {
    pub owner_type_id: StableTypeId,
    pub owner_type_name: String,
    pub property_id: StablePropertyId,
    pub property_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SystemFieldRefIR {
    pub stable_table_id: StableTypeId,
    pub table_type_name: String,
    pub table_incarnation_id: TableIncarnationId,
    pub role: SystemFieldRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemFieldRole {
    Id,
    Src,
    Dst,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldRefIR {
    Property(PropertyRefIR),
    System(SystemFieldRefIR),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConstraintIR {
    Key {
        fields: Vec<FieldRefIR>,
    },
    Unique {
        fields: Vec<FieldRefIR>,
    },
    Index {
        fields: Vec<FieldRefIR>,
    },
    Range {
        field: FieldRefIR,
        min: Option<ConstraintBound>,
        max: Option<ConstraintBound>,
    },
    Check {
        field: FieldRefIR,
        pattern: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedSourceIR {
    pub source: PropertyRefIR,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaIdentityDiagnosticKind {
    InertInitializationRenameHint,
    IgnoredExactMatchRenameHint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaIdentityDiagnostic {
    pub kind: SchemaIdentityDiagnosticKind,
    pub entity: String,
    pub hint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaResolution {
    pub schema_ir: SchemaIR,
    pub diagnostics: Vec<SchemaIdentityDiagnostic>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TypeKind {
    Interface,
    Node,
    Edge,
}

impl TypeKind {
    fn label(self) -> &'static str {
        match self {
            Self::Interface => "interface",
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }
}

#[derive(Clone, Copy)]
struct AcceptedType<'a> {
    kind: TypeKind,
    name: &'a str,
    type_id: StableTypeId,
    incarnation: Option<TableIncarnationId>,
    properties: &'a [PropertyIR],
}

#[derive(Debug, Clone, Copy)]
struct AssignedType {
    type_id: StableTypeId,
    incarnation: Option<TableIncarnationId>,
    matched: bool,
}

struct Allocator {
    next: u64,
}

impl Allocator {
    fn new(next: u64) -> Result<Self> {
        if next == 0 {
            return Err(SchemaIdentityError::ZeroId {
                entity: "next_identity_id".to_string(),
                value: next,
            }
            .into());
        }
        Ok(Self { next })
    }

    fn allocate(&mut self) -> Result<u64> {
        if self.next == u64::MAX {
            return Err(SchemaIdentityError::AllocatorExhausted.into());
        }
        let allocated = self.next;
        self.next += 1;
        Ok(allocated)
    }
}

/// Resolve a brand-new graph.  Rename hints cannot import identity and are
/// reported as inert diagnostics.
pub fn initialize_schema_ir(
    schema_identity_domain: SchemaIdentityDomain,
    shape: &SchemaShape,
) -> Result<SchemaResolution> {
    resolve(schema_identity_domain, 1, None, shape)
}

/// Resolve evolution against the one accepted identity authority.
pub fn resolve_schema_ir(accepted: &SchemaIR, shape: &SchemaShape) -> Result<SchemaResolution> {
    validate_schema_ir(accepted)?;
    resolve(
        accepted.schema_identity_domain.clone(),
        accepted.next_identity_id,
        Some(accepted),
        shape,
    )
}

fn resolve(
    domain: SchemaIdentityDomain,
    next_identity_id: u64,
    accepted: Option<&SchemaIR>,
    shape: &SchemaShape,
) -> Result<SchemaResolution> {
    let accepted_types = accepted.map(accepted_types).unwrap_or_default();
    let accepted_by_key = accepted_types
        .iter()
        .map(|entry| ((entry.kind, entry.name), *entry))
        .collect::<HashMap<_, _>>();
    let accepted_by_name = accepted_types.iter().fold(
        HashMap::<&str, Vec<AcceptedType<'_>>>::new(),
        |mut map, entry| {
            map.entry(entry.name).or_default().push(*entry);
            map
        },
    );
    // Only the explicit initialization entry point may treat rename hints as
    // inert. An accepted IR with no currently-live declarations can still
    // carry retired IDs below its high-water mark; evolution must not let a
    // hint resurrect those identities.
    let evolving = accepted.is_some();
    let mut diagnostics = Vec::new();
    let mut assignments = BTreeMap::<(TypeKind, String), AssignedType>::new();
    let mut consumed_types = HashSet::new();

    for (kind, name, rename_from) in shape_types(shape) {
        let exact = accepted_by_key.get(&(kind, name.as_str())).copied();
        let matched = if let Some(exact) = exact {
            if let Some(hint) = rename_from {
                diagnostics.push(SchemaIdentityDiagnostic {
                    kind: SchemaIdentityDiagnosticKind::IgnoredExactMatchRenameHint,
                    entity: format!("{}:{name}", kind.label()),
                    hint: hint.to_string(),
                });
            }
            Some(exact)
        } else if let Some(hint) = rename_from {
            if !evolving {
                diagnostics.push(SchemaIdentityDiagnostic {
                    kind: SchemaIdentityDiagnosticKind::InertInitializationRenameHint,
                    entity: format!("{}:{name}", kind.label()),
                    hint: hint.to_string(),
                });
                None
            } else {
                let candidates = accepted_by_name.get(hint).cloned().unwrap_or_default();
                if candidates.is_empty() {
                    return resolution_error(format!(
                        "{} '{name}' declares @rename_from(\"{hint}\") but no accepted declaration has that name",
                        kind.label()
                    ));
                }
                let same_kind = candidates
                    .iter()
                    .filter(|candidate| candidate.kind == kind)
                    .copied()
                    .collect::<Vec<_>>();
                if same_kind.len() != 1 {
                    return resolution_error(format!(
                        "{} '{name}' cannot rename accepted '{hint}' across declaration kinds",
                        kind.label()
                    ));
                }
                Some(same_kind[0])
            }
        } else {
            None
        };
        if let Some(matched) = matched {
            if !consumed_types.insert(matched.type_id) {
                return resolution_error(format!(
                    "accepted {} '{}' is consumed by more than one desired declaration",
                    matched.kind.label(),
                    matched.name
                ));
            }
            assignments.insert(
                (kind, name),
                AssignedType {
                    type_id: matched.type_id,
                    incarnation: matched.incarnation,
                    matched: true,
                },
            );
        } else {
            assignments.insert(
                (kind, name),
                AssignedType {
                    // Replaced in the canonical type-allocation pass.
                    type_id: StableTypeId::try_from(1)?,
                    incarnation: None,
                    matched: false,
                },
            );
        }
    }

    let mut allocator = Allocator::new(next_identity_id)?;
    for assignment in assignments.values_mut().filter(|entry| !entry.matched) {
        assignment.type_id = StableTypeId::try_from(allocator.allocate()?)?;
    }

    let mut property_assignments = BTreeMap::<(StableTypeId, String), StablePropertyId>::new();
    let mut consumed_properties = HashSet::new();
    let mut pending_new_properties = Vec::new();
    for (kind, owner_name, properties) in shape_properties(shape) {
        let owner = assignments[&(kind, owner_name.to_string())];
        let accepted_owner = accepted_types
            .iter()
            .find(|candidate| candidate.type_id == owner.type_id);
        for property in properties {
            let matched = match accepted_owner {
                Some(accepted_owner) => {
                    let exact = accepted_owner
                        .properties
                        .iter()
                        .find(|candidate| candidate.name == property.name);
                    if let Some(exact) = exact {
                        if let Some(hint) = &property.rename_from {
                            diagnostics.push(SchemaIdentityDiagnostic {
                                kind: SchemaIdentityDiagnosticKind::IgnoredExactMatchRenameHint,
                                entity: format!("{}:{owner_name}.{}", kind.label(), property.name),
                                hint: hint.clone(),
                            });
                        }
                        Some(exact)
                    } else if let Some(hint) = &property.rename_from {
                        let local = accepted_owner
                            .properties
                            .iter()
                            .filter(|candidate| candidate.name == *hint)
                            .collect::<Vec<_>>();
                        if local.len() != 1 {
                            let elsewhere = accepted_types.iter().any(|candidate| {
                                candidate.type_id != accepted_owner.type_id
                                    && candidate.properties.iter().any(|p| p.name == *hint)
                            });
                            let detail = if elsewhere {
                                "property moves across stable owners are not supported"
                            } else {
                                "no accepted property with that name exists under the stable owner"
                            };
                            return resolution_error(format!(
                                "property '{owner_name}.{}' declares @rename_from(\"{hint}\"): {detail}",
                                property.name
                            ));
                        }
                        Some(local[0])
                    } else {
                        None
                    }
                }
                None => {
                    if let Some(hint) = &property.rename_from {
                        if evolving {
                            return resolution_error(format!(
                                "new owner '{}:{owner_name}' cannot import property identity with @rename_from(\"{hint}\")",
                                kind.label()
                            ));
                        }
                        diagnostics.push(SchemaIdentityDiagnostic {
                            kind: SchemaIdentityDiagnosticKind::InertInitializationRenameHint,
                            entity: format!("{}:{owner_name}.{}", kind.label(), property.name),
                            hint: hint.clone(),
                        });
                    }
                    None
                }
            };
            if let Some(matched) = matched {
                if !consumed_properties.insert(matched.property_id) {
                    return resolution_error(format!(
                        "accepted property id {} is consumed more than once",
                        matched.property_id
                    ));
                }
                property_assignments
                    .insert((owner.type_id, property.name.clone()), matched.property_id);
            } else {
                pending_new_properties.push((owner.type_id, property.name.clone()));
            }
        }
    }
    pending_new_properties.sort();
    for (owner_id, property_name) in pending_new_properties {
        property_assignments.insert(
            (owner_id, property_name),
            StablePropertyId::try_from(allocator.allocate()?)?,
        );
    }

    let mut pending_incarnations = assignments
        .iter()
        .filter(|((kind, _), assignment)| {
            matches!(kind, TypeKind::Node | TypeKind::Edge) && assignment.incarnation.is_none()
        })
        .map(|(key, assignment)| (assignment.type_id, key.clone()))
        .collect::<Vec<_>>();
    pending_incarnations.sort_by_key(|(type_id, _)| *type_id);
    for (_, key) in pending_incarnations {
        assignments.get_mut(&key).unwrap().incarnation =
            Some(TableIncarnationId::try_from(allocator.allocate()?)?);
    }

    let interfaces = shape
        .interfaces
        .iter()
        .map(|interface| {
            let assigned = assignments[&(TypeKind::Interface, interface.name.clone())];
            Ok(InterfaceIR {
                name: interface.name.clone(),
                type_id: assigned.type_id,
                properties: build_properties(
                    assigned.type_id,
                    &interface.name,
                    &interface.properties,
                    &assignments,
                    &property_assignments,
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let nodes = shape
        .nodes
        .iter()
        .map(|node| {
            let assigned = assignments[&(TypeKind::Node, node.name.clone())];
            let incarnation = assigned.incarnation.expect("nodes receive incarnations");
            let mut implements = node
                .implements
                .iter()
                .map(|name| type_ref(TypeKind::Interface, name, &assignments))
                .collect::<Result<Vec<_>>>()?;
            // References are canonicalized by immutable identity. Diagnostic
            // names may move across lexical order as interfaces are added or
            // renamed; they must not control accepted-IR ordering.
            implements.sort();
            Ok(NodeIR {
                name: node.name.clone(),
                type_id: assigned.type_id,
                table_incarnation_id: incarnation,
                annotations: node.annotations.clone(),
                implements,
                properties: build_properties(
                    assigned.type_id,
                    &node.name,
                    &node.properties,
                    &assignments,
                    &property_assignments,
                )?,
                constraints: build_constraints(
                    TypeKind::Node,
                    assigned.type_id,
                    &node.name,
                    incarnation,
                    &node.constraints,
                    &property_assignments,
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let edges = shape
        .edges
        .iter()
        .map(|edge| {
            let assigned = assignments[&(TypeKind::Edge, edge.name.clone())];
            let incarnation = assigned.incarnation.expect("edges receive incarnations");
            Ok(EdgeIR {
                name: edge.name.clone(),
                type_id: assigned.type_id,
                table_incarnation_id: incarnation,
                from_type: type_ref(TypeKind::Node, &edge.from_type, &assignments)?,
                to_type: type_ref(TypeKind::Node, &edge.to_type, &assignments)?,
                cardinality: edge.cardinality.clone(),
                annotations: edge.annotations.clone(),
                properties: build_properties(
                    assigned.type_id,
                    &edge.name,
                    &edge.properties,
                    &assignments,
                    &property_assignments,
                )?,
                constraints: build_constraints(
                    TypeKind::Edge,
                    assigned.type_id,
                    &edge.name,
                    incarnation,
                    &edge.constraints,
                    &property_assignments,
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let schema_ir = SchemaIR {
        ir_version: SCHEMA_IR_VERSION,
        schema_identity_domain: domain,
        next_identity_id: allocator.next,
        interfaces,
        nodes,
        edges,
    };
    validate_schema_ir(&schema_ir)?;
    Ok(SchemaResolution {
        schema_ir,
        diagnostics,
    })
}

fn accepted_types(ir: &SchemaIR) -> Vec<AcceptedType<'_>> {
    ir.interfaces
        .iter()
        .map(|entry| AcceptedType {
            kind: TypeKind::Interface,
            name: &entry.name,
            type_id: entry.type_id,
            incarnation: None,
            properties: &entry.properties,
        })
        .chain(ir.nodes.iter().map(|entry| AcceptedType {
            kind: TypeKind::Node,
            name: &entry.name,
            type_id: entry.type_id,
            incarnation: Some(entry.table_incarnation_id),
            properties: &entry.properties,
        }))
        .chain(ir.edges.iter().map(|entry| AcceptedType {
            kind: TypeKind::Edge,
            name: &entry.name,
            type_id: entry.type_id,
            incarnation: Some(entry.table_incarnation_id),
            properties: &entry.properties,
        }))
        .collect()
}

fn shape_types(shape: &SchemaShape) -> Vec<(TypeKind, String, Option<&str>)> {
    shape
        .interfaces
        .iter()
        .map(|entry| (TypeKind::Interface, entry.name.clone(), None))
        .chain(shape.nodes.iter().map(|entry| {
            (
                TypeKind::Node,
                entry.name.clone(),
                entry.rename_from.as_deref(),
            )
        }))
        .chain(shape.edges.iter().map(|entry| {
            (
                TypeKind::Edge,
                entry.name.clone(),
                entry.rename_from.as_deref(),
            )
        }))
        .collect()
}

fn shape_properties<'a>(shape: &'a SchemaShape) -> Vec<(TypeKind, &'a str, &'a [PropertyShape])> {
    shape
        .interfaces
        .iter()
        .map(|entry| {
            (
                TypeKind::Interface,
                entry.name.as_str(),
                entry.properties.as_slice(),
            )
        })
        .chain(shape.nodes.iter().map(|entry| {
            (
                TypeKind::Node,
                entry.name.as_str(),
                entry.properties.as_slice(),
            )
        }))
        .chain(shape.edges.iter().map(|entry| {
            (
                TypeKind::Edge,
                entry.name.as_str(),
                entry.properties.as_slice(),
            )
        }))
        .collect()
}

fn type_ref(
    kind: TypeKind,
    name: &str,
    assignments: &BTreeMap<(TypeKind, String), AssignedType>,
) -> Result<TypeRefIR> {
    let assigned = assignments
        .get(&(kind, name.to_string()))
        .ok_or_else(|| SchemaIdentityError::Resolution(format!("unknown {kind:?} '{name}'")))?;
    Ok(TypeRefIR {
        type_id: assigned.type_id,
        type_name: name.to_string(),
    })
}

fn build_properties(
    owner_type_id: StableTypeId,
    owner_name: &str,
    properties: &[PropertyShape],
    assignments: &BTreeMap<(TypeKind, String), AssignedType>,
    property_assignments: &BTreeMap<(StableTypeId, String), StablePropertyId>,
) -> Result<Vec<PropertyIR>> {
    properties
        .iter()
        .map(|property| {
            let property_id = property_assignments[&(owner_type_id, property.name.clone())];
            let embed_source = property
                .embed_source
                .as_ref()
                .map(|embed| {
                    Ok::<_, CompilerError>(EmbedSourceIR {
                        source: property_ref(
                            owner_type_id,
                            owner_name,
                            &embed.property_name,
                            property_assignments,
                        )?,
                        model: embed.model.clone(),
                    })
                })
                .transpose()?;
            let mut satisfies_interface_properties = property
                .satisfies_interface_properties
                .iter()
                .map(|reference| {
                    let interface = assignments
                        .get(&(TypeKind::Interface, reference.owner_name.clone()))
                        .ok_or_else(|| {
                            SchemaIdentityError::Resolution(format!(
                                "unknown interface '{}'",
                                reference.owner_name
                            ))
                        })?;
                    property_ref(
                        interface.type_id,
                        &reference.owner_name,
                        &reference.property_name,
                        property_assignments,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            satisfies_interface_properties.sort();
            Ok(PropertyIR {
                name: property.name.clone(),
                property_id,
                prop_type: property.prop_type.clone(),
                annotations: property.annotations.clone(),
                property_constraints: property.property_constraints.clone(),
                embed_source,
                declared_directly: property.declared_directly,
                satisfies_interface_properties,
            })
        })
        .collect()
}

fn property_ref(
    owner_type_id: StableTypeId,
    owner_name: &str,
    property_name: &str,
    property_assignments: &BTreeMap<(StableTypeId, String), StablePropertyId>,
) -> Result<PropertyRefIR> {
    let property_id = property_assignments
        .get(&(owner_type_id, property_name.to_string()))
        .copied()
        .ok_or_else(|| {
            SchemaIdentityError::Resolution(format!(
                "unknown property reference '{owner_name}.{property_name}'"
            ))
        })?;
    Ok(PropertyRefIR {
        owner_type_id,
        owner_type_name: owner_name.to_string(),
        property_id,
        property_name: property_name.to_string(),
    })
}

fn build_constraints(
    kind: TypeKind,
    owner_type_id: StableTypeId,
    owner_name: &str,
    incarnation: TableIncarnationId,
    constraints: &[Constraint],
    properties: &BTreeMap<(StableTypeId, String), StablePropertyId>,
) -> Result<Vec<ConstraintIR>> {
    constraints
        .iter()
        .map(|constraint| {
            let field = |name: &str| {
                field_ref(
                    kind,
                    owner_type_id,
                    owner_name,
                    incarnation,
                    name,
                    properties,
                )
            };
            let fields = |names: &[String]| {
                names
                    .iter()
                    .map(|name| field(name))
                    .collect::<Result<Vec<_>>>()
            };
            Ok(match constraint {
                Constraint::Key(names) => ConstraintIR::Key {
                    fields: fields(names)?,
                },
                Constraint::Unique(names) => ConstraintIR::Unique {
                    fields: fields(names)?,
                },
                Constraint::Index(names) => ConstraintIR::Index {
                    fields: fields(names)?,
                },
                Constraint::Range { property, min, max } => ConstraintIR::Range {
                    field: field(property)?,
                    min: min.clone(),
                    max: max.clone(),
                },
                Constraint::Check { property, pattern } => ConstraintIR::Check {
                    field: field(property)?,
                    pattern: pattern.clone(),
                },
            })
        })
        .collect()
}

fn field_ref(
    kind: TypeKind,
    owner_type_id: StableTypeId,
    owner_name: &str,
    incarnation: TableIncarnationId,
    name: &str,
    properties: &BTreeMap<(StableTypeId, String), StablePropertyId>,
) -> Result<FieldRefIR> {
    let system_role = match name {
        "id" => Some(SystemFieldRole::Id),
        "src" if kind == TypeKind::Edge => Some(SystemFieldRole::Src),
        "dst" if kind == TypeKind::Edge => Some(SystemFieldRole::Dst),
        _ => None,
    };
    if let Some(role) = system_role {
        return Ok(FieldRefIR::System(SystemFieldRefIR {
            stable_table_id: owner_type_id,
            table_type_name: owner_name.to_string(),
            table_incarnation_id: incarnation,
            role,
        }));
    }
    Ok(FieldRefIR::Property(property_ref(
        owner_type_id,
        owner_name,
        name,
        properties,
    )?))
}

fn resolution_error<T>(message: String) -> Result<T> {
    Err(SchemaIdentityError::Resolution(message).into())
}

pub fn schema_ir_json(ir: &SchemaIR) -> Result<String> {
    serde_json::to_string(ir)
        .map_err(|error| CompilerError::Catalog(format!("serialize schema ir error: {error}")))
}

pub fn schema_ir_pretty_json(ir: &SchemaIR) -> Result<String> {
    serde_json::to_string_pretty(ir)
        .map_err(|error| CompilerError::Catalog(format!("serialize schema ir error: {error}")))
}

pub fn schema_ir_hash(ir: &SchemaIR) -> Result<String> {
    let json = schema_ir_json(ir)?;
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

/// Project accepted identity authority back to canonical source semantics.
/// This never allocates or reconstructs identities from names.
pub fn schema_shape_from_ir(ir: &SchemaIR) -> Result<SchemaShape> {
    validate_schema_ir(ir)?;
    let interfaces = ir
        .interfaces
        .iter()
        .map(|interface| InterfaceShape {
            name: interface.name.clone(),
            properties: interface
                .properties
                .iter()
                .map(property_shape_from_ir)
                .collect(),
        })
        .collect();
    let nodes = ir
        .nodes
        .iter()
        .map(|node| {
            let mut implements = node
                .implements
                .iter()
                .map(|reference| reference.type_name.clone())
                .collect::<Vec<_>>();
            // SchemaShape is identity-free and canonical by source names.
            // Accepted IR uses identity order, so crossing the boundary must
            // re-establish the shape's name order before hashing.
            implements.sort();
            implements.dedup();
            NodeShape {
                name: node.name.clone(),
                rename_from: None,
                annotations: node.annotations.clone(),
                implements,
                properties: node.properties.iter().map(property_shape_from_ir).collect(),
                constraints: node.constraints.iter().map(constraint_from_ir).collect(),
            }
        })
        .collect();
    let edges = ir
        .edges
        .iter()
        .map(|edge| EdgeShape {
            name: edge.name.clone(),
            rename_from: None,
            from_type: edge.from_type.type_name.clone(),
            to_type: edge.to_type.type_name.clone(),
            cardinality: edge.cardinality.clone(),
            annotations: edge.annotations.clone(),
            properties: edge.properties.iter().map(property_shape_from_ir).collect(),
            constraints: edge.constraints.iter().map(constraint_from_ir).collect(),
        })
        .collect();
    Ok(SchemaShape {
        interfaces,
        nodes,
        edges,
    })
}

pub fn schema_shape_hash_from_ir(ir: &SchemaIR) -> Result<String> {
    schema_shape_hash(&schema_shape_from_ir(ir)?)
}

fn property_shape_from_ir(property: &PropertyIR) -> PropertyShape {
    let mut satisfies_interface_properties = property
        .satisfies_interface_properties
        .iter()
        .map(|reference| ShapePropertyRef {
            owner_name: reference.owner_type_name.clone(),
            property_name: reference.property_name.clone(),
        })
        .collect::<Vec<_>>();
    satisfies_interface_properties.sort();
    satisfies_interface_properties.dedup();
    PropertyShape {
        name: property.name.clone(),
        rename_from: None,
        prop_type: property.prop_type.clone(),
        annotations: property.annotations.clone(),
        property_constraints: property.property_constraints.clone(),
        embed_source: property
            .embed_source
            .as_ref()
            .map(|embed| EmbedSourceShape {
                property_name: embed.source.property_name.clone(),
                model: embed.model.clone(),
            }),
        declared_directly: property.declared_directly,
        satisfies_interface_properties,
    }
}

pub(crate) fn constraint_from_ir(constraint: &ConstraintIR) -> Constraint {
    let field_name = |field: &FieldRefIR| match field {
        FieldRefIR::Property(reference) => reference.property_name.clone(),
        FieldRefIR::System(reference) => match reference.role {
            SystemFieldRole::Id => "id".to_string(),
            SystemFieldRole::Src => "src".to_string(),
            SystemFieldRole::Dst => "dst".to_string(),
        },
    };
    match constraint {
        ConstraintIR::Key { fields } => Constraint::Key(fields.iter().map(field_name).collect()),
        ConstraintIR::Unique { fields } => {
            Constraint::Unique(fields.iter().map(field_name).collect())
        }
        ConstraintIR::Index { fields } => {
            Constraint::Index(fields.iter().map(field_name).collect())
        }
        ConstraintIR::Range { field, min, max } => Constraint::Range {
            property: field_name(field),
            min: min.clone(),
            max: max.clone(),
        },
        ConstraintIR::Check { field, pattern } => Constraint::Check {
            property: field_name(field),
            pattern: pattern.clone(),
        },
    }
}

/// Fail closed on malformed or hand-authored identity authority.
pub fn validate_schema_ir(ir: &SchemaIR) -> Result<()> {
    if ir.ir_version != SCHEMA_IR_VERSION {
        return invalid_ir(format!(
            "unsupported ir_version {} (expected {SCHEMA_IR_VERSION})",
            ir.ir_version
        ));
    }
    SchemaIdentityDomain::parse(ir.schema_identity_domain.as_str())?;
    if ir.next_identity_id == 0 {
        return invalid_ir("next_identity_id must be nonzero".to_string());
    }

    let types = accepted_types(ir);
    let mut all_ids = HashMap::<u64, String>::new();
    let mut max_id = 0;
    let mut type_by_id = HashMap::new();
    let mut type_names = HashSet::new();
    for entry in &types {
        insert_global_id(
            &mut all_ids,
            entry.type_id.get(),
            format!("{} type '{}'", entry.kind.label(), entry.name),
        )?;
        max_id = max_id.max(entry.type_id.get());
        if !type_names.insert((entry.kind, entry.name)) {
            return invalid_ir(format!(
                "duplicate {} name '{}'",
                entry.kind.label(),
                entry.name
            ));
        }
        type_by_id.insert(entry.type_id, *entry);
        if let Some(incarnation) = entry.incarnation {
            insert_global_id(
                &mut all_ids,
                incarnation.get(),
                format!("table incarnation for '{}{}'", entry.name, ""),
            )?;
            max_id = max_id.max(incarnation.get());
        }
        let mut property_names = HashSet::new();
        for property in entry.properties {
            if !property_names.insert(property.name.as_str()) {
                return invalid_ir(format!(
                    "duplicate property '{}.{}'",
                    entry.name, property.name
                ));
            }
            insert_global_id(
                &mut all_ids,
                property.property_id.get(),
                format!("property '{}.{}'", entry.name, property.name),
            )?;
            max_id = max_id.max(property.property_id.get());
            validate_annotations(
                &property.annotations,
                &format!("{}.{}", entry.name, property.name),
            )?;
            if let Some(values) = &property.prop_type.enum_values {
                ensure_sorted_unique(values.iter().map(String::as_str), "enum values")?;
            }
            if property
                .property_constraints
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            {
                return invalid_ir(format!(
                    "property constraints for '{}.{}' are not sorted and unique",
                    entry.name, property.name
                ));
            }
        }
    }
    if ir.next_identity_id <= max_id {
        return invalid_ir(format!(
            "next_identity_id {} does not exceed allocated id {max_id}",
            ir.next_identity_id
        ));
    }
    ensure_sorted_unique(
        ir.interfaces.iter().map(|entry| entry.name.as_str()),
        "interfaces",
    )?;
    ensure_sorted_unique(ir.nodes.iter().map(|entry| entry.name.as_str()), "nodes")?;
    ensure_sorted_unique(ir.edges.iter().map(|entry| entry.name.as_str()), "edges")?;

    let property_by_id = types
        .iter()
        .flat_map(|owner| {
            owner
                .properties
                .iter()
                .map(move |property| (property.property_id, (*owner, property)))
        })
        .collect::<HashMap<_, _>>();
    for owner in &types {
        ensure_sorted_unique(
            owner
                .properties
                .iter()
                .map(|property| property.name.as_str()),
            &format!("properties of '{}'", owner.name),
        )?;
        for property in owner.properties {
            if owner.kind == TypeKind::Interface
                && (!property.declared_directly
                    || !property.satisfies_interface_properties.is_empty())
            {
                return invalid_ir(format!(
                    "interface property '{}.{}' has invalid concrete provenance",
                    owner.name, property.name
                ));
            }
            if owner.kind == TypeKind::Node
                && !property.declared_directly
                && property.satisfies_interface_properties.is_empty()
            {
                return invalid_ir(format!(
                    "injected node property '{}.{}' has no interface provenance",
                    owner.name, property.name
                ));
            }
            if let Some(embed) = &property.embed_source {
                validate_property_ref(
                    &embed.source,
                    Some(owner.type_id),
                    &type_by_id,
                    &property_by_id,
                )?;
            }
            let mut previous = None;
            for reference in &property.satisfies_interface_properties {
                validate_property_ref(reference, None, &type_by_id, &property_by_id)?;
                let target = type_by_id[&reference.owner_type_id];
                if target.kind != TypeKind::Interface {
                    return invalid_ir(format!(
                        "property '{}.{}' satisfaction target is not an interface property",
                        owner.name, property.name
                    ));
                }
                if previous.as_ref().is_some_and(|value| *value >= reference) {
                    return invalid_ir(format!(
                        "interface satisfaction links for '{}.{}' are not sorted and unique",
                        owner.name, property.name
                    ));
                }
                previous = Some(reference);
            }
        }
    }
    for node in &ir.nodes {
        validate_annotations(&node.annotations, &node.name)?;
        validate_type_refs(&node.implements, TypeKind::Interface, &type_by_id)?;
        let implemented = node
            .implements
            .iter()
            .map(|reference| reference.type_id)
            .collect::<HashSet<_>>();
        for property in &node.properties {
            for reference in &property.satisfies_interface_properties {
                if !implemented.contains(&reference.owner_type_id) {
                    return invalid_ir(format!(
                        "node property '{}.{}' satisfies an interface the node does not implement",
                        node.name, property.name
                    ));
                }
                let (_, interface_property) = property_by_id[&reference.property_id];
                if interface_property.prop_type != property.prop_type {
                    return invalid_ir(format!(
                        "node property '{}.{}' is incompatible with satisfied interface property '{}.{}'",
                        node.name,
                        property.name,
                        reference.owner_type_name,
                        reference.property_name
                    ));
                }
            }
        }
        for interface_id in implemented {
            let interface = type_by_id[&interface_id];
            for interface_property in interface.properties {
                let satisfied = node.properties.iter().any(|property| {
                    property
                        .satisfies_interface_properties
                        .iter()
                        .any(|reference| {
                            reference.owner_type_id == interface_id
                                && reference.property_id == interface_property.property_id
                        })
                });
                if !satisfied {
                    return invalid_ir(format!(
                        "node '{}' does not link an effective property to interface contract '{}.{}'",
                        node.name, interface.name, interface_property.name
                    ));
                }
            }
        }
        validate_constraint_order(&node.constraints, &node.name)?;
        validate_constraints(
            TypeKind::Node,
            node.type_id,
            node.table_incarnation_id,
            &node.constraints,
            &type_by_id,
            &property_by_id,
        )?;
    }
    for edge in &ir.edges {
        validate_annotations(&edge.annotations, &edge.name)?;
        for property in &edge.properties {
            if !property.declared_directly
                || !property.satisfies_interface_properties.is_empty()
                || property.embed_source.is_some()
            {
                return invalid_ir(format!(
                    "edge property '{}.{}' has invalid interface provenance",
                    edge.name, property.name
                ));
            }
        }
        validate_type_ref(&edge.from_type, TypeKind::Node, &type_by_id)?;
        validate_type_ref(&edge.to_type, TypeKind::Node, &type_by_id)?;
        validate_constraint_order(&edge.constraints, &edge.name)?;
        validate_constraints(
            TypeKind::Edge,
            edge.type_id,
            edge.table_incarnation_id,
            &edge.constraints,
            &type_by_id,
            &property_by_id,
        )?;
    }
    Ok(())
}

fn validate_annotations(annotations: &[Annotation], entity: &str) -> Result<()> {
    for annotation in annotations {
        if matches!(
            annotation.name.as_str(),
            "rename_from" | "embed" | "key" | "unique" | "index"
        ) {
            return invalid_ir(format!(
                "typed annotation @{} leaked into opaque annotations for '{entity}'",
                annotation.name
            ));
        }
    }
    let keys = annotations
        .iter()
        .map(|annotation| {
            serde_json::to_string(annotation).expect("Annotation serialization is infallible")
        })
        .collect::<Vec<_>>();
    if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return invalid_ir(format!(
            "annotations for '{entity}' are not sorted and unique"
        ));
    }
    Ok(())
}

fn validate_constraint_order(constraints: &[ConstraintIR], entity: &str) -> Result<()> {
    let keys = constraints
        .iter()
        .map(|constraint| constraint_sort_key(&constraint_from_ir(constraint)))
        .collect::<Vec<_>>();
    if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return invalid_ir(format!(
            "constraints for '{entity}' are not sorted and unique"
        ));
    }
    Ok(())
}

fn validate_type_refs(
    references: &[TypeRefIR],
    expected: TypeKind,
    types: &HashMap<StableTypeId, AcceptedType<'_>>,
) -> Result<()> {
    let mut previous = None;
    for reference in references {
        validate_type_ref(reference, expected, types)?;
        if previous.as_ref().is_some_and(|value| *value >= reference) {
            return invalid_ir("type references are not sorted and unique".to_string());
        }
        previous = Some(reference);
    }
    Ok(())
}

fn validate_type_ref(
    reference: &TypeRefIR,
    expected: TypeKind,
    types: &HashMap<StableTypeId, AcceptedType<'_>>,
) -> Result<()> {
    let target = types.get(&reference.type_id).ok_or_else(|| {
        SchemaIdentityError::InvalidIr(format!(
            "type reference {} points to a missing ID",
            reference.type_id
        ))
    })?;
    if target.kind != expected || target.name != reference.type_name {
        return invalid_ir(format!(
            "type reference name/id mismatch for '{}'",
            reference.type_name
        ));
    }
    Ok(())
}

fn validate_property_ref(
    reference: &PropertyRefIR,
    expected_owner: Option<StableTypeId>,
    types: &HashMap<StableTypeId, AcceptedType<'_>>,
    properties: &HashMap<StablePropertyId, (AcceptedType<'_>, &PropertyIR)>,
) -> Result<()> {
    let (owner, property) = properties.get(&reference.property_id).ok_or_else(|| {
        SchemaIdentityError::InvalidIr(format!(
            "property reference {} points to a missing ID",
            reference.property_id
        ))
    })?;
    if expected_owner.is_some_and(|expected| expected != reference.owner_type_id)
        || owner.type_id != reference.owner_type_id
        || owner.name != reference.owner_type_name
        || property.name != reference.property_name
        || !types.contains_key(&reference.owner_type_id)
    {
        return invalid_ir(format!(
            "property reference name/id/owner mismatch for '{}.{}'",
            reference.owner_type_name, reference.property_name
        ));
    }
    Ok(())
}

fn validate_constraints(
    kind: TypeKind,
    owner_id: StableTypeId,
    incarnation: TableIncarnationId,
    constraints: &[ConstraintIR],
    types: &HashMap<StableTypeId, AcceptedType<'_>>,
    properties: &HashMap<StablePropertyId, (AcceptedType<'_>, &PropertyIR)>,
) -> Result<()> {
    for constraint in constraints {
        let fields: Vec<&FieldRefIR> = match constraint {
            ConstraintIR::Key { fields }
            | ConstraintIR::Unique { fields }
            | ConstraintIR::Index { fields } => fields.iter().collect(),
            ConstraintIR::Range { field, .. } | ConstraintIR::Check { field, .. } => vec![field],
        };
        for field in fields {
            match field {
                FieldRefIR::Property(reference) => {
                    validate_property_ref(reference, Some(owner_id), types, properties)?;
                }
                FieldRefIR::System(reference) => {
                    if reference.stable_table_id != owner_id
                        || reference.table_incarnation_id != incarnation
                        || types[&owner_id].name != reference.table_type_name
                        || (kind == TypeKind::Node
                            && !matches!(reference.role, SystemFieldRole::Id))
                        || !matches!(kind, TypeKind::Node | TypeKind::Edge)
                    {
                        return invalid_ir("invalid system-field reference".to_string());
                    }
                }
            }
        }
    }
    Ok(())
}

fn insert_global_id(ids: &mut HashMap<u64, String>, id: u64, entity: String) -> Result<()> {
    if let Some(previous) = ids.insert(id, entity.clone()) {
        return invalid_ir(format!(
            "identity value {id} is reused by '{previous}' and '{entity}'"
        ));
    }
    Ok(())
}

fn ensure_sorted_unique<'a>(values: impl IntoIterator<Item = &'a str>, entity: &str) -> Result<()> {
    let mut previous = None;
    for value in values {
        if previous.is_some_and(|previous| previous >= value) {
            return invalid_ir(format!("{entity} are not sorted and unique"));
        }
        previous = Some(value);
    }
    Ok(())
}

fn invalid_ir<T>(message: String) -> Result<T> {
    Err(SchemaIdentityError::InvalidIr(message).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema_shape::compile_schema_shape;
    use crate::catalog::{build_catalog, build_catalog_from_ir};
    use crate::schema::parser::parse_schema;

    fn domain() -> SchemaIdentityDomain {
        SchemaIdentityDomain::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
    }

    fn initialize(source: &str) -> SchemaIR {
        let parsed = parse_schema(source).unwrap();
        let shape = compile_schema_shape(&parsed).unwrap();
        initialize_schema_ir(domain(), &shape).unwrap().schema_ir
    }

    #[test]
    fn allocation_is_canonical_and_shared_across_identity_kinds() {
        let left = initialize(
            "interface Z { p: String }\nnode B implements Z {}\nnode A {}\nedge E: A -> B {}",
        );
        let right = initialize(
            "edge E: A -> B {}\nnode A {}\nnode B implements Z {}\ninterface Z { p: String }",
        );
        assert_eq!(left, right);
        assert_eq!(left.interfaces[0].type_id.get(), 1);
        assert_eq!(left.nodes[0].type_id.get(), 2);
        assert_eq!(left.nodes[1].type_id.get(), 3);
        assert_eq!(left.edges[0].type_id.get(), 4);
        assert_eq!(left.interfaces[0].properties[0].property_id.get(), 5);
        assert_eq!(left.nodes[1].properties[0].property_id.get(), 6);
        assert_eq!(left.nodes[0].table_incarnation_id.get(), 7);
        assert_eq!(left.next_identity_id, 10);
    }

    #[test]
    fn evolved_references_use_identity_order_and_shape_projection_restores_name_order() {
        let accepted = initialize("interface Z { name: String } node N implements Z {}");
        let desired_shape = compile_schema_shape(
            &parse_schema(
                "interface A { name: String } interface Z { name: String } node N implements A, Z {}",
            )
            .unwrap(),
        )
        .unwrap();
        let resolved = resolve_schema_ir(&accepted, &desired_shape)
            .unwrap()
            .schema_ir;

        // Z retains the older (smaller) identity even though A is lexically
        // first. Accepted references therefore use Z,A identity order.
        assert_eq!(
            resolved.nodes[0]
                .implements
                .iter()
                .map(|reference| reference.type_name.as_str())
                .collect::<Vec<_>>(),
            vec!["Z", "A"]
        );
        assert_eq!(
            resolved.nodes[0].properties[0]
                .satisfies_interface_properties
                .iter()
                .map(|reference| reference.owner_type_name.as_str())
                .collect::<Vec<_>>(),
            vec!["Z", "A"]
        );

        let projected = schema_shape_from_ir(&resolved).unwrap();
        assert_eq!(projected.nodes[0].implements, vec!["A", "Z"]);
        assert_eq!(
            projected.nodes[0].properties[0]
                .satisfies_interface_properties
                .iter()
                .map(|reference| reference.owner_name.as_str())
                .collect::<Vec<_>>(),
            vec!["A", "Z"]
        );
        assert_eq!(
            schema_shape_hash(&projected).unwrap(),
            schema_shape_hash(&desired_shape).unwrap()
        );
    }

    #[test]
    fn explicit_renames_preserve_type_property_and_incarnation_ids() {
        let accepted = initialize("node Person { name: String age: I32? }");
        let desired = parse_schema(
            "node Human @rename_from(\"Person\") { full_name: String @rename_from(\"name\") age: I32? }",
        )
        .unwrap();
        let desired = compile_schema_shape(&desired).unwrap();
        let resolved = resolve_schema_ir(&accepted, &desired).unwrap().schema_ir;
        assert_eq!(accepted.nodes[0].type_id, resolved.nodes[0].type_id);
        assert_eq!(
            accepted.nodes[0].table_incarnation_id,
            resolved.nodes[0].table_incarnation_id
        );
        assert_eq!(
            accepted.nodes[0].properties[1].property_id,
            resolved.nodes[0].properties[1].property_id
        );
        assert_eq!(accepted.next_identity_id, resolved.next_identity_id);
    }

    #[test]
    fn runtime_composite_key_order_survives_lexically_crossing_property_renames() {
        let accepted = initialize(
            r#"
node Pair {
  alpha: String
  zeta: String
  @key(alpha, zeta)
}
"#,
        );
        let accepted_catalog = build_catalog_from_ir(&accepted).unwrap();
        assert_eq!(
            accepted_catalog.node_types["Pair"].key.as_deref(),
            Some(&["alpha".to_string(), "zeta".to_string()][..])
        );

        let desired = compile_schema_shape(
            &parse_schema(
                r#"
node Pair {
  aaaa: String @rename_from("zeta")
  zzzz: String @rename_from("alpha")
  @key(aaaa, zzzz)
}
"#,
            )
            .unwrap(),
        )
        .unwrap();
        let resolved = resolve_schema_ir(&accepted, &desired).unwrap().schema_ir;

        let ir_key_names = resolved.nodes[0]
            .constraints
            .iter()
            .find_map(|constraint| match constraint {
                ConstraintIR::Key { fields } => Some(
                    fields
                        .iter()
                        .map(|field| match field {
                            FieldRefIR::Property(reference) => reference.property_name.as_str(),
                            FieldRefIR::System(_) => {
                                panic!("node key unexpectedly uses system field")
                            }
                        })
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            ir_key_names,
            ["aaaa", "zzzz"],
            "schema-shape normalization is intentionally lexical"
        );

        let resolved_catalog = build_catalog_from_ir(&resolved).unwrap();
        assert_eq!(
            resolved_catalog.node_types["Pair"].key.as_deref(),
            Some(&["zzzz".to_string(), "aaaa".to_string()][..]),
            "runtime tuple order must follow preserved property identity, not renamed spelling"
        );
    }

    #[test]
    fn exact_name_wins_and_stale_hint_is_diagnostic_only() {
        let accepted = initialize("node Person { name: String }");
        let desired = parse_schema(
            "node Person @rename_from(\"Missing\") { name: String @rename_from(\"old\") }",
        )
        .unwrap();
        let resolution =
            resolve_schema_ir(&accepted, &compile_schema_shape(&desired).unwrap()).unwrap();
        assert_eq!(resolution.diagnostics.len(), 2);
        assert_eq!(accepted, resolution.schema_ir);
    }

    #[test]
    fn identity_bearing_references_survive_rename() {
        let accepted = initialize(
            r#"
interface Named { name: String }
node Person implements Named { vector: Vector(3) @embed("name") }
edge Knows: Person -> Person { @unique(src, dst) }
"#,
        );
        let desired = parse_schema(
            r#"
interface Named { name: String }
node Human @rename_from("Person") implements Named {
  vector: Vector(3) @embed("name")
}
edge Relates: Human -> Human @rename_from("Knows") { @unique(src, dst) }
"#,
        )
        .unwrap();
        let resolved = resolve_schema_ir(&accepted, &compile_schema_shape(&desired).unwrap())
            .unwrap()
            .schema_ir;
        assert_eq!(
            resolved.edges[0].from_type.type_id,
            resolved.nodes[0].type_id
        );
        assert_eq!(resolved.edges[0].from_type.type_name, "Human");
        assert_eq!(
            resolved.nodes[0].properties[0].satisfies_interface_properties[0].property_id,
            resolved.interfaces[0].properties[0].property_id
        );
        assert_eq!(
            schema_shape_hash_from_ir(&resolved).unwrap(),
            schema_shape_hash(&compile_schema_shape(&desired).unwrap()).unwrap()
        );
    }

    #[test]
    fn drop_then_readd_mints_new_identity_and_incarnation() {
        let first = initialize("node Person { name: String }");
        let empty = resolve_schema_ir(
            &first,
            &compile_schema_shape(&parse_schema("").unwrap()).unwrap(),
        )
        .unwrap()
        .schema_ir;
        let second = resolve_schema_ir(
            &empty,
            &compile_schema_shape(&parse_schema("node Person { name: String }").unwrap()).unwrap(),
        )
        .unwrap()
        .schema_ir;
        assert_ne!(first.nodes[0].type_id, second.nodes[0].type_id);
        assert_ne!(
            first.nodes[0].table_incarnation_id,
            second.nodes[0].table_incarnation_id
        );
        assert_ne!(
            first.nodes[0].properties[0].property_id,
            second.nodes[0].properties[0].property_id
        );
    }

    #[test]
    fn rejects_cross_kind_and_cross_owner_renames() {
        let accepted = initialize("node A { p: String } node B { q: String }");
        let cross_kind = compile_schema_shape(
            &parse_schema("node B { q: String } edge E: B -> B @rename_from(\"A\") {}").unwrap(),
        )
        .unwrap();
        assert!(resolve_schema_ir(&accepted, &cross_kind).is_err());

        let cross_owner = compile_schema_shape(
            &parse_schema("node A {} node B { moved: String @rename_from(\"p\") }").unwrap(),
        )
        .unwrap();
        assert!(resolve_schema_ir(&accepted, &cross_owner).is_err());
    }

    #[test]
    fn validation_rejects_duplicate_ids_allocator_regression_and_v1() {
        let ir = initialize("node A { p: String }");
        let mut duplicate = ir.clone();
        duplicate.nodes[0].properties[0].property_id =
            StablePropertyId::try_from(duplicate.nodes[0].type_id.get()).unwrap();
        assert!(validate_schema_ir(&duplicate).is_err());
        let mut regressed = ir.clone();
        regressed.next_identity_id = regressed.nodes[0].type_id.get();
        assert!(validate_schema_ir(&regressed).is_err());
        let mut v1 = ir;
        v1.ir_version = 1;
        assert!(validate_schema_ir(&v1).is_err());
    }

    #[test]
    fn allocator_overflow_is_typed() {
        let mut accepted = initialize("node A {}");
        accepted.next_identity_id = u64::MAX;
        let desired = compile_schema_shape(&parse_schema("node A {} node B {}").unwrap()).unwrap();
        let error = resolve_schema_ir(&accepted, &desired).unwrap_err();
        assert!(matches!(
            error,
            CompilerError::SchemaIdentity(SchemaIdentityError::AllocatorExhausted)
        ));
    }

    #[test]
    fn catalog_is_directly_bound_to_the_exact_ir() {
        let source = parse_schema("node Person { name: String @key }").unwrap();
        let source_catalog = build_catalog(&source).unwrap();
        assert!(!source_catalog.is_identity_bound());

        let ir = initialize_schema_ir(domain(), &compile_schema_shape(&source).unwrap())
            .unwrap()
            .schema_ir;
        let catalog = build_catalog_from_ir(&ir).unwrap();
        assert!(catalog.is_identity_bound());
        assert_eq!(catalog.bound_schema_ir(), Some(&ir));
        assert_eq!(catalog.type_id("Person"), Some(ir.nodes[0].type_id));
        assert_eq!(
            catalog.table_incarnation_id("Person"),
            Some(ir.nodes[0].table_incarnation_id)
        );
        assert_eq!(
            catalog.property_id("Person", "name"),
            Some(ir.nodes[0].properties[0].property_id)
        );
        assert_eq!(catalog.node_types["Person"].key_property(), Some("name"));
    }

    #[test]
    fn catalog_generic_identity_accessors_refuse_cross_kind_name_ambiguity() {
        let ir = initialize(
            "node Shared { value: String } edge Shared: Shared -> Shared { value: String edge_only: String }",
        );
        let catalog = build_catalog_from_ir(&ir).unwrap();

        assert_eq!(catalog.type_id("Shared"), None);
        assert_eq!(catalog.table_incarnation_id("Shared"), None);
        assert_eq!(catalog.property_id("Shared", "value"), None);
        assert_eq!(catalog.property_id("Shared", "edge_only"), None);
        assert_eq!(catalog.node_type_id("Shared"), Some(ir.nodes[0].type_id));
        assert_eq!(catalog.edge_type_id("Shared"), Some(ir.edges[0].type_id));
        assert_eq!(
            catalog.node_table_incarnation_id("Shared"),
            Some(ir.nodes[0].table_incarnation_id)
        );
        assert_eq!(
            catalog.edge_table_incarnation_id("Shared"),
            Some(ir.edges[0].table_incarnation_id)
        );
        assert_eq!(
            catalog.node_property_id("Shared", "value"),
            Some(ir.nodes[0].properties[0].property_id)
        );
        assert_eq!(
            catalog.edge_property_id("Shared", "value"),
            Some(
                ir.edges[0]
                    .properties
                    .iter()
                    .find(|property| property.name == "value")
                    .unwrap()
                    .property_id
            )
        );
        assert_eq!(
            catalog.edge_property_id("Shared", "edge_only"),
            Some(
                ir.edges[0]
                    .properties
                    .iter()
                    .find(|property| property.name == "edge_only")
                    .unwrap()
                    .property_id
            )
        );
    }

    #[test]
    fn removing_one_interface_contributor_preserves_effective_property_identity() {
        let accepted = initialize(
            "interface A { name: String } interface B { name: String } node Person implements A, B {}",
        );
        let accepted_property = &accepted.nodes[0].properties[0];
        assert_eq!(accepted_property.satisfies_interface_properties.len(), 2);

        let desired = compile_schema_shape(
            &parse_schema(
                "interface A { name: String } interface B { name: String } node Person implements B {}",
            )
            .unwrap(),
        )
        .unwrap();
        let resolved = resolve_schema_ir(&accepted, &desired).unwrap().schema_ir;
        assert_eq!(
            resolved.nodes[0].properties[0].property_id,
            accepted_property.property_id
        );
        assert_eq!(
            resolved.nodes[0].properties[0]
                .satisfies_interface_properties
                .len(),
            1
        );
    }

    #[test]
    fn initialization_rename_hints_are_inert_and_not_persisted() {
        let source = parse_schema(
            "node Person @rename_from(\"Old\") { name: String @rename_from(\"old_name\") }",
        )
        .unwrap();
        let shape = compile_schema_shape(&source).unwrap();
        let resolution = initialize_schema_ir(domain(), &shape).unwrap();
        assert_eq!(resolution.diagnostics.len(), 2);
        assert!(resolution.schema_ir.nodes[0].annotations.is_empty());
        assert!(
            resolution.schema_ir.nodes[0].properties[0]
                .annotations
                .is_empty()
        );
    }
}
