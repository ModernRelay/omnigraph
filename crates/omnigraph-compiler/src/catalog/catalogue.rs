//! The engine catalogue: the built-in node types a query reads the running
//! binary's registries through. A catalogue type is spelled with the `@`
//! prefix that marks engine-owned metadata (`$p.@id`, `$e.@type`), is
//! read-only, is never persisted, and never enters a user schema, its
//! accepted `SchemaIR`, its hash or its stored property identities. This
//! module declares the five types' shapes; the engine supplies their rows.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::NodeType;
use super::schema_ir::SystemColumns;
use crate::types::{PropType, ScalarType};

/// The character that opens every catalogue type name.
pub const CATALOGUE_TYPE_PREFIX: char = '@';

/// The reserved graph id a catalogue-only query is addressed to.
pub const CATALOGUE_SCOPE: &str = "og_catalog";

/// The prefix reserved for system graph ids, `og_catalog` among them.
pub const SYSTEM_GRAPH_ID_PREFIX: &str = "og_";

/// The value type of one catalogue column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogueProp {
    String,
    I64,
    I64List,
}

impl CatalogueProp {
    pub fn prop_type(self) -> PropType {
        match self {
            Self::String => PropType::scalar(ScalarType::String, false),
            Self::I64 => PropType::scalar(ScalarType::I64, false),
            Self::I64List => PropType::list_of(ScalarType::I64, false),
        }
    }
}

/// One catalogue type: its name, its key column and its columns in
/// declaration order. The key is the first column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogueType {
    pub name: &'static str,
    pub columns: &'static [(&'static str, CatalogueProp)],
}

/// One row per distinct physical operator name the engine executes, with
/// the input counts that operator admits.
pub const OPERATOR: CatalogueType = CatalogueType {
    name: "@Operator",
    columns: &[
        ("name", CatalogueProp::String),
        ("inputs", CatalogueProp::I64List),
    ],
};

/// One row per planner pass, at its position in explain's reporting order.
pub const PASS: CatalogueType = CatalogueType {
    name: "@Pass",
    columns: &[
        ("name", CatalogueProp::String),
        ("position", CatalogueProp::I64),
    ],
};

/// One row per session setting, with its kind, default and scope.
pub const SETTING: CatalogueType = CatalogueType {
    name: "@Setting",
    columns: &[
        ("name", CatalogueProp::String),
        ("kind", CatalogueProp::String),
        ("default", CatalogueProp::String),
        ("scope", CatalogueProp::String),
    ],
};

/// The one row describing the binary: its release version and the internal
/// storage format it writes and the oldest it still serves.
pub const ENGINE: CatalogueType = CatalogueType {
    name: "@Engine",
    columns: &[
        ("version", CatalogueProp::String),
        ("current_internal_schema_version", CatalogueProp::I64),
        ("min_supported_internal_schema_version", CatalogueProp::I64),
    ],
};

/// One row per logical plan node kind, with its change-feed and merge
/// routing coverage.
pub const LOGICAL_KIND: CatalogueType = CatalogueType {
    name: "@LogicalKind",
    columns: &[
        ("name", CatalogueProp::String),
        ("coverage", CatalogueProp::String),
    ],
};

/// Every catalogue type, in the order `schema show` lists them.
pub const CATALOGUE_TYPES: &[CatalogueType] = &[OPERATOR, PASS, SETTING, ENGINE, LOGICAL_KIND];

/// Whether `name` is spelled as a catalogue type, declared or not.
pub fn is_catalogue_type_name(name: &str) -> bool {
    name.starts_with(CATALOGUE_TYPE_PREFIX)
}

/// The catalogue type `name` denotes, if one is declared.
pub fn catalogue_type(name: &str) -> Option<&'static CatalogueType> {
    CATALOGUE_TYPES.iter().find(|ty| ty.name == name)
}

impl CatalogueType {
    /// The key column, the first declared column.
    pub fn key(&self) -> &'static str {
        self.columns[0].0
    }

    /// The type as the query checker and planner see it, with the graph's
    /// own identity-column spelling so catalogue rows join user rows.
    pub fn node_type(&self, columns: SystemColumns) -> NodeType {
        let properties = self
            .columns
            .iter()
            .map(|(name, prop)| ((*name).to_string(), prop.prop_type()))
            .collect::<HashMap<_, _>>();
        let mut fields = vec![Field::new(columns.id, DataType::Utf8, false)];
        fields.extend(self.columns.iter().map(|(name, prop)| {
            let prop = prop.prop_type();
            Field::new(*name, prop.to_arrow(), prop.nullable)
        }));
        NodeType {
            name: self.name.to_string(),
            implements: Vec::new(),
            properties,
            key: Some(vec![self.key().to_string()]),
            unique_constraints: Vec::new(),
            indices: Vec::new(),
            range_constraints: Vec::new(),
            check_constraints: Vec::new(),
            embed_sources: HashMap::new(),
            blob_properties: Default::default(),
            arrow_schema: Arc::new(Schema::new(fields)),
        }
    }

    /// The `.gq`-style declaration of the type, for `schema show` and the
    /// shipped `catalogue.gq`.
    pub fn declaration(&self) -> String {
        let mut text = format!("node {} {{\n", self.name);
        for (index, (name, prop)) in self.columns.iter().enumerate() {
            let key = if index == 0 { " @key" } else { "" };
            text.push_str(&format!(
                "    {name}: {}{key}\n",
                prop.prop_type().display_name()
            ));
        }
        text.push('}');
        text
    }
}

/// The catalogue types keyed by name, with the graph's identity spelling.
pub fn catalogue_node_types(columns: SystemColumns) -> HashMap<String, NodeType> {
    CATALOGUE_TYPES
        .iter()
        .map(|ty| (ty.name.to_string(), ty.node_type(columns)))
        .collect()
}

/// The `schema show` text of every catalogue type, one declaration per
/// type, separated by blank lines.
pub fn catalogue_declarations() -> String {
    CATALOGUE_TYPES
        .iter()
        .map(CatalogueType::declaration)
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_catalogue_type_is_keyed_on_a_string_first_column() {
        for ty in CATALOGUE_TYPES {
            assert!(is_catalogue_type_name(ty.name), "{}", ty.name);
            assert_eq!(ty.columns[0].1, CatalogueProp::String, "{}", ty.name);
            assert_eq!(catalogue_type(ty.name), Some(ty));
            let node = ty.node_type(super::super::schema_ir::SYSTEM_COLUMNS_V3);
            assert_eq!(node.key.as_deref(), Some(&[ty.key().to_string()][..]));
            assert_eq!(node.arrow_schema.fields().len(), ty.columns.len() + 1);
            assert_eq!(node.arrow_schema.field(0).name(), "__id");
        }
        let mut names: Vec<_> = CATALOGUE_TYPES.iter().map(|ty| ty.name).collect();
        names.dedup();
        assert_eq!(names.len(), CATALOGUE_TYPES.len());
    }

    #[test]
    fn declaration_spells_the_key_and_the_list_type() {
        assert_eq!(
            OPERATOR.declaration(),
            "node @Operator {\n    name: String @key\n    inputs: [I64]\n}"
        );
    }
}
