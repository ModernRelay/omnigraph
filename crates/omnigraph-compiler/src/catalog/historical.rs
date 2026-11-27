//! A read at an earlier commit sees the schema accepted there (RFC GQ 3,
//! axiom A2): each property under its current name through its stable
//! identity, and only the properties whose identities exist at the commit.
//! [`Catalog::at_commit`] is the one place that decides that shape, before
//! anything is planned, so the type checker refuses a property the commit
//! does not hold and a whole node expands to the properties it does.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::Schema;

use super::schema_ir::{PropertyIR, SchemaIR, StablePropertyId};
use super::{Catalog, CatalogIdentity, EdgeType, NodeType};
use crate::error::{CompilerError, Result};

/// Why a current property is absent at a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Absence {
    /// The property was added after the commit.
    AddedSince,
    /// The commit holds a property of that name, which was dropped since;
    /// the current one is a new identity added again under the same name.
    AddedAgainSince,
}

/// The commit a historical catalog describes, and what it leaves out of the
/// current schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalView {
    commit: String,
    /// Per node, edge or interface name: each current property the commit
    /// does not hold, and why.
    absent: HashMap<String, HashMap<String, Absence>>,
    /// Per node or edge name: each property the commit holds under another
    /// name, current name to the name stored at the commit.
    renamed: HashMap<String, HashMap<String, String>>,
}

impl HistoricalView {
    /// The commit, as the read named it.
    pub fn commit(&self) -> &str {
        &self.commit
    }

    /// Why `owner.property` is absent at the commit, when the current schema
    /// declares it and the commit does not hold it.
    pub fn absence(&self, owner: &str, property: &str) -> Option<Absence> {
        self.absent.get(owner)?.get(property).copied()
    }

    /// The name the commit stores `owner.property` under: its current name
    /// unless it was renamed since.
    pub fn stored_name<'a>(&'a self, owner: &str, property: &'a str) -> &'a str {
        self.renamed
            .get(owner)
            .and_then(|renamed| renamed.get(property))
            .map_or(property, String::as_str)
    }

    /// The type error naming `owner.property` at this commit, when it is
    /// absent there.
    pub fn refusal(&self, owner: &str, property: &str) -> Option<String> {
        let why = match self.absence(owner, property)? {
            Absence::AddedSince => "it was added after that commit".to_string(),
            Absence::AddedAgainSince => format!(
                "the `{property}` that commit holds was dropped since, and `{property}` was added again as a new property"
            ),
        };
        Some(format!(
            "`{owner}` has no property `{property}` at commit `{}`: {why}",
            self.commit
        ))
    }
}

impl Catalog {
    /// The historical view of a catalog built by [`Self::at_commit`].
    pub fn historical(&self) -> Option<&HistoricalView> {
        self.historical.as_deref()
    }

    /// The type error for a current property absent at the commit a
    /// historical catalog describes.
    pub fn absent_at_commit(&self, owner: &str, property: &str) -> Option<String> {
        self.historical()?.refusal(owner, property)
    }

    /// This identity-bound catalog as a read at `commit` sees it, given
    /// `at`, the schema accepted at that commit: every node and edge type the
    /// commit holds by table identity keeps the current properties whose
    /// stable identities `at` declares for it, under their current names,
    /// and loses the others; an interface loses a property any implementer
    /// lost. A type the commit does not hold is left whole: its table is
    /// absent from the commit's snapshot, which refuses it by itself.
    pub fn at_commit(&self, at: &SchemaIR, commit: &str) -> Result<Catalog> {
        let current = self.bound_schema_ir().ok_or_else(|| {
            CompilerError::Plan(
                "a historical catalog is derived from an identity-bound catalog".to_string(),
            )
        })?;
        let mut out = self.clone();
        let mut absent: HashMap<String, HashMap<String, Absence>> = HashMap::new();
        let mut renamed: HashMap<String, HashMap<String, String>> = HashMap::new();
        for node in &current.nodes {
            let Some(then) = at.nodes.iter().find(|then| {
                then.type_id == node.type_id
                    && then.table_incarnation_id == node.table_incarnation_id
            }) else {
                continue;
            };
            let (gone, moved) = compare(&node.properties, &then.properties);
            if let Some(node_type) = out.node_types.get_mut(&node.name) {
                restrict_node(node_type, &gone);
            }
            record(&mut absent, &mut renamed, &node.name, gone, moved);
        }
        for edge in &current.edges {
            let Some(then) = at.edges.iter().find(|then| {
                then.type_id == edge.type_id
                    && then.table_incarnation_id == edge.table_incarnation_id
            }) else {
                continue;
            };
            let (gone, moved) = compare(&edge.properties, &then.properties);
            if let Some(edge_type) = out.edge_types.get_mut(&edge.name) {
                restrict_edge(edge_type, &gone);
            }
            record(&mut absent, &mut renamed, &edge.name, gone, moved);
        }
        for (name, interface) in &mut out.interfaces {
            let mut lost: HashMap<String, Absence> = HashMap::new();
            for node in out.node_types.values() {
                if !node.implements.iter().any(|i| i == name) {
                    continue;
                }
                for (property, why) in absent.get(&node.name).into_iter().flatten() {
                    if interface.properties.contains_key(property) {
                        lost.entry(property.clone()).or_insert(*why);
                    }
                }
            }
            for property in lost.keys() {
                interface.properties.remove(property);
            }
            if !lost.is_empty() {
                absent.insert(name.clone(), lost);
            }
        }
        out.historical = Some(Arc::new(HistoricalView {
            commit: commit.to_string(),
            absent,
            renamed,
        }));
        debug_assert!(matches!(out.identity, CatalogIdentity::Bound(_)));
        Ok(out)
    }
}

/// The current properties `then` does not hold by identity, each with why,
/// and the ones it holds under another name (current name, name then).
fn compare(
    now: &[PropertyIR],
    then: &[PropertyIR],
) -> (HashMap<String, Absence>, HashMap<String, String>) {
    let by_id: HashMap<StablePropertyId, &str> = then
        .iter()
        .map(|p| (p.property_id, p.name.as_str()))
        .collect();
    let names_then: HashSet<&str> = then.iter().map(|p| p.name.as_str()).collect();
    let mut gone = HashMap::new();
    let mut moved = HashMap::new();
    for property in now {
        match by_id.get(&property.property_id) {
            Some(name) if *name == property.name => {}
            Some(name) => {
                moved.insert(property.name.clone(), (*name).to_string());
            }
            None if names_then.contains(property.name.as_str()) => {
                gone.insert(property.name.clone(), Absence::AddedAgainSince);
            }
            None => {
                gone.insert(property.name.clone(), Absence::AddedSince);
            }
        }
    }
    (gone, moved)
}

fn record(
    absent: &mut HashMap<String, HashMap<String, Absence>>,
    renamed: &mut HashMap<String, HashMap<String, String>>,
    owner: &str,
    gone: HashMap<String, Absence>,
    moved: HashMap<String, String>,
) {
    if !gone.is_empty() {
        absent.insert(owner.to_string(), gone);
    }
    if !moved.is_empty() {
        renamed.insert(owner.to_string(), moved);
    }
}

/// `schema` without the fields named in `gone`.
fn without(schema: &Schema, gone: &HashMap<String, Absence>) -> Arc<Schema> {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| !gone.contains_key(field.name()))
        .cloned()
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn keeps(group: &[String], gone: &HashMap<String, Absence>) -> bool {
    group.iter().all(|property| !gone.contains_key(property))
}

fn restrict_node(node: &mut NodeType, gone: &HashMap<String, Absence>) {
    if gone.is_empty() {
        return;
    }
    node.properties.retain(|name, _| !gone.contains_key(name));
    node.unique_constraints.retain(|group| keeps(group, gone));
    node.indices.retain(|group| keeps(group, gone));
    node.index_declarations
        .retain(|name, _| !gone.contains_key(name));
    node.range_constraints
        .retain(|constraint| !gone.contains_key(&constraint.property));
    node.check_constraints
        .retain(|constraint| !gone.contains_key(&constraint.property));
    node.embed_sources
        .retain(|name, source| !gone.contains_key(name) && !gone.contains_key(&source.source));
    node.full_text_analysis
        .retain(|name, _| !gone.contains_key(name));
    node.full_text_fingerprints
        .retain(|name, _| !gone.contains_key(name));
    node.vector_distances
        .retain(|name, _| !gone.contains_key(name));
    node.blob_properties.retain(|name| !gone.contains_key(name));
    node.arrow_schema = without(&node.arrow_schema, gone);
}

fn restrict_edge(edge: &mut EdgeType, gone: &HashMap<String, Absence>) {
    if gone.is_empty() {
        return;
    }
    edge.properties.retain(|name, _| !gone.contains_key(name));
    edge.unique_constraints.retain(|group| keeps(group, gone));
    edge.indices.retain(|group| keeps(group, gone));
    edge.full_text_fingerprints
        .retain(|name, _| !gone.contains_key(name));
    edge.vector_distances
        .retain(|name, _| !gone.contains_key(name));
    edge.blob_properties.retain(|name| !gone.contains_key(name));
    edge.arrow_schema = without(&edge.arrow_schema, gone);
}
