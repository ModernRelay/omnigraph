use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::types::{Direction, PropType};

/// Query-only column carrying a bound edge's canonical schema type name.
pub const EDGE_TYPE_COLUMN: &str = "~edge_type";

/// Read-only metadata spelling for a bound edge's concrete schema type.
pub const EDGE_TYPE_META: &str = "@type";

/// One concrete member of a traversal, oriented from its query source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeMember {
    pub edge_type: String,
    pub direction: Direction,
}

/// The resolved types selected against one captured catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeSelection {
    Named(EdgeMember),
    Alternation(Vec<EdgeMember>),
    Wildcard(Vec<EdgeMember>),
}

impl EdgeSelection {
    pub fn members(&self) -> &[EdgeMember] {
        match self {
            Self::Named(member) => std::slice::from_ref(member),
            Self::Alternation(members) | Self::Wildcard(members) => members,
        }
    }

    /// The member of a named traversal; selectors retain their provenance.
    pub fn named(&self) -> Option<&EdgeMember> {
        match self {
            Self::Named(member) => Some(member),
            Self::Alternation(_) | Self::Wildcard(_) => None,
        }
    }

    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard(_))
    }

    pub fn reversed(&self) -> Self {
        let reverse = |member: &EdgeMember| EdgeMember {
            edge_type: member.edge_type.clone(),
            direction: match member.direction {
                Direction::Out => Direction::In,
                Direction::In => Direction::Out,
                Direction::Both => Direction::Both,
            },
        };
        match self {
            Self::Named(member) => Self::Named(reverse(member)),
            Self::Alternation(members) => Self::Alternation(members.iter().map(reverse).collect()),
            Self::Wildcard(members) => Self::Wildcard(members.iter().map(reverse).collect()),
        }
    }
}

/// The common type of a property present on every selected edge type.
/// Missing members, missing properties and incompatible value types return `None`.
pub fn common_edge_property(
    catalog: &Catalog,
    type_names: &[String],
    property: &str,
) -> Option<PropType> {
    let (first, rest) = type_names.split_first()?;
    let mut common = catalog
        .lookup_edge_by_name(first)?
        .properties
        .get(property)?
        .clone();
    for name in rest {
        let prop = catalog
            .lookup_edge_by_name(name)?
            .properties
            .get(property)?;
        if common.scalar != prop.scalar || common.list != prop.list {
            return None;
        }
        common.nullable |= prop.nullable;
        common.enum_values = match (&common.enum_values, &prop.enum_values) {
            (Some(left), Some(right)) => Some(
                left.iter()
                    .chain(right)
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            ),
            _ => None,
        };
    }
    Some(common)
}

/// Every compatible common property, ordered lexicographically by property name.
pub fn common_edge_properties(
    catalog: &Catalog,
    type_names: &[String],
) -> BTreeMap<String, PropType> {
    let Some(first) = type_names
        .first()
        .and_then(|name| catalog.lookup_edge_by_name(name))
    else {
        return BTreeMap::new();
    };
    first
        .properties
        .keys()
        .filter_map(|name| {
            common_edge_property(catalog, type_names, name).map(|prop| (name.clone(), prop))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build_catalog, schema::parser::parse_schema, types::ScalarType};

    #[test]
    fn common_properties_join_nullable_and_enum_domains_issue_659() {
        let schema = parse_schema("node Person { name: String } edge Knows: Person -> Person edge Likes: Person -> Person").unwrap();
        let mut catalog = build_catalog(&schema).unwrap();
        catalog
            .edge_types
            .get_mut("Knows")
            .unwrap()
            .properties
            .extend([
                ("label".into(), PropType::enum_type(vec!["a".into()], false)),
                ("missing".into(), PropType::scalar(ScalarType::I32, false)),
                (
                    "conflicting".into(),
                    PropType::scalar(ScalarType::I32, false),
                ),
            ]);
        catalog
            .edge_types
            .get_mut("Likes")
            .unwrap()
            .properties
            .extend([
                ("label".into(), PropType::enum_type(vec!["b".into()], true)),
                (
                    "conflicting".into(),
                    PropType::list_of(ScalarType::I32, false),
                ),
            ]);
        let names = vec!["Knows".into(), "Likes".into()];
        let properties = common_edge_properties(&catalog, &names);
        assert_eq!(properties.len(), 1);
        assert_eq!(
            properties["label"],
            PropType::enum_type(vec!["a".into(), "b".into()], true)
        );
        assert!(common_edge_property(&catalog, &[], "label").is_none());
        catalog
            .edge_types
            .get_mut("Likes")
            .unwrap()
            .properties
            .insert("label".into(), PropType::scalar(ScalarType::String, false));
        assert_eq!(
            common_edge_property(&catalog, &names, "label")
                .unwrap()
                .enum_values,
            None
        );
    }

    #[test]
    fn reversed_selection_preserves_kind_and_each_direction_issue_659() {
        let selection = EdgeSelection::Alternation(vec![
            EdgeMember {
                edge_type: "Knows".into(),
                direction: Direction::Out,
            },
            EdgeMember {
                edge_type: "Likes".into(),
                direction: Direction::In,
            },
            EdgeMember {
                edge_type: "Related".into(),
                direction: Direction::Both,
            },
        ]);
        let named = EdgeSelection::Named(selection.members()[0].clone());
        assert_eq!(named.reversed().members()[0].direction, Direction::In);
        assert_eq!(named.reversed().reversed(), named);
        assert_eq!(selection.reversed().reversed(), selection);
        assert_eq!(
            selection
                .reversed()
                .members()
                .iter()
                .map(|member| member.direction)
                .collect::<Vec<_>>(),
            vec![Direction::In, Direction::Out, Direction::Both]
        );
        assert_eq!(
            EdgeSelection::Wildcard(vec![]).reversed(),
            EdgeSelection::Wildcard(vec![])
        );
    }
}
