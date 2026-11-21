//! Schema constructs a binary before GQ 3 cannot read back.
//!
//! A graph stores its accepted `.pg` source beside the IR, and a binary
//! before GQ 3 re-parses that source on every open. Once an apply stores a
//! construct its parser or feature set refuses, that binary refuses the whole
//! graph, and only a backup taken before the upgrade restores it.
//! [`ONE_WAY_CONSTRUCTS`] is the one table of those constructs: `schema plan`,
//! `schema apply` and `cluster plan` report each match of a candidate source
//! through [`one_way_constructs`], and the cross-version owner proves every
//! row's example against the genuine predecessor binary.

use pest::Parser as _;
use pest::iterators::Pair;
use serde::{Deserialize, Serialize};

use super::parser::{Rule, SchemaParser};
use crate::error::{CompilerError, Result};

/// The line every report of one-way constructs carries.
pub const ONE_WAY_NOTE: &str = "binaries before this release refuse the whole graph after this apply, and only a backup taken before the upgrade restores it";

/// One property declaration as the one-way rows read it: its syntax, before
/// any desugaring that would forget how it was written (an explicit `l2`
/// parses to the omitted distance).
#[derive(Debug, Default)]
pub struct PropertySyntax {
    /// The option names written in `Vector(n, ...)`.
    pub vector_options: Vec<String>,
    /// The property's type is a list.
    pub list: bool,
    /// A plain `@index` follows the property.
    pub plain_index: bool,
    /// The option names of `@index(fulltext, ...)` after the property, when
    /// one is written.
    pub full_text_options: Option<Vec<String>>,
}

/// A row of [`ONE_WAY_CONSTRUCTS`].
pub struct OneWayRow {
    /// The stable name a report carries.
    pub id: &'static str,
    /// The construct, as an operator reads it.
    pub description: &'static str,
    /// Whether a property declaration writes the construct.
    pub matches: fn(&PropertySyntax) -> bool,
    /// A schema without the construct, and the same schema with it: applying
    /// `after` over `before` stores the construct through a plan with a step.
    pub before: &'static str,
    pub after: &'static str,
}

/// Every construct a binary before GQ 3 refuses in a stored schema.
pub const ONE_WAY_CONSTRUCTS: &[OneWayRow] = &[
    OneWayRow {
        id: "vector_distance",
        description: "an explicit `distance` on a Vector, `l2` included",
        matches: |p| p.vector_options.iter().any(|o| o == "distance"),
        before: "node Doc { slug: String @key }\n",
        after: "node Doc { slug: String @key emb: Vector(2, distance = \"l2\")? }\n",
    },
    OneWayRow {
        id: "list_index",
        description: "`@index` on a List property",
        matches: |p| p.list && p.plain_index,
        before: "node Doc { slug: String @key tags: [String] }\n",
        after: "node Doc { slug: String @key tags: [String] @index }\n",
    },
    OneWayRow {
        id: "full_text_index",
        description: "`@index(fulltext)`, with or without options",
        matches: |p| p.full_text_options.is_some(),
        before: "node Doc { slug: String @key title: String @index }\n",
        after: "node Doc { slug: String @key title: String @index(fulltext) }\n",
    },
    OneWayRow {
        id: "full_text_analyzer",
        description: "an `analyzer` in `@index(fulltext, ...)`",
        matches: |p| {
            p.full_text_options
                .as_ref()
                .is_some_and(|o| o.iter().any(|o| o == "analyzer"))
        },
        before: "node Doc { slug: String @key title: String }\n",
        after: "node Doc { slug: String @key title: String @index(fulltext, analyzer = \"standard_v1\") }\n",
    },
    OneWayRow {
        id: "full_text_scorer",
        description: "a `scorer` in `@index(fulltext, ...)`",
        matches: |p| {
            p.full_text_options
                .as_ref()
                .is_some_and(|o| o.iter().any(|o| o == "scorer"))
        },
        before: "node Doc { slug: String @key title: String }\n",
        after: "node Doc { slug: String @key title: String @index(fulltext, scorer = \"none\") }\n",
    },
];

/// One match of a [`ONE_WAY_CONSTRUCTS`] row in a schema source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneWayConstruct {
    /// The row's `id`.
    pub construct: String,
    /// The row's description.
    pub description: String,
    /// The interface, node or edge declaring the property.
    pub type_name: String,
    pub property: String,
    /// Where the property's declaration starts, 1-based.
    pub line: usize,
    pub column: usize,
}

/// Every one-way construct `source` writes, in source order, a property
/// matching several rows once per row.
pub fn one_way_constructs(source: &str) -> Result<Vec<OneWayConstruct>> {
    let file = SchemaParser::parse(Rule::schema_file, source)
        .map_err(|e| CompilerError::Parse(e.to_string()))?;
    let mut found = Vec::new();
    for decl in file.flatten() {
        if !matches!(
            decl.as_rule(),
            Rule::interface_decl | Rule::node_decl | Rule::edge_decl
        ) {
            continue;
        }
        let mut inner = decl.into_inner();
        let Some(type_name) = inner.next().map(|name| name.as_str().to_string()) else {
            continue;
        };
        for prop in inner.filter(|pair| pair.as_rule() == Rule::prop_decl) {
            let (line, column) = prop.as_span().start_pos().line_col();
            let (property, syntax) = property_syntax(prop);
            for row in ONE_WAY_CONSTRUCTS {
                if (row.matches)(&syntax) {
                    found.push(OneWayConstruct {
                        construct: row.id.to_string(),
                        description: row.description.to_string(),
                        type_name: type_name.clone(),
                        property: property.clone(),
                        line,
                        column,
                    });
                }
            }
        }
    }
    Ok(found)
}

fn property_syntax(prop: Pair<'_, Rule>) -> (String, PropertySyntax) {
    let mut inner = prop.into_inner();
    let name = inner
        .next()
        .map(|n| n.as_str().to_string())
        .unwrap_or_default();
    let mut syntax = PropertySyntax::default();
    for item in inner {
        match item.as_rule() {
            Rule::type_ref => {
                for part in item.into_inner().flatten() {
                    match part.as_rule() {
                        Rule::list_type => syntax.list = true,
                        Rule::vector_type => {
                            syntax.vector_options = part
                                .into_inner()
                                .filter(|option| option.as_rule() == Rule::annotation_kwarg)
                                .filter_map(|option| option.into_inner().next())
                                .map(|key| key.as_str().to_string())
                                .collect();
                        }
                        _ => {}
                    }
                }
            }
            Rule::index_annotation => {
                syntax.full_text_options = Some(
                    item.into_inner()
                        .filter(|arg| arg.as_rule() == Rule::annotation_kwarg)
                        .filter_map(|arg| arg.into_inner().next())
                        .map(|key| key.as_str().to_string())
                        .collect(),
                );
            }
            Rule::annotation
                if item.clone().into_inner().next().map(|n| n.as_str()) == Some("index") =>
            {
                syntax.plain_index = true;
            }
            _ => {}
        }
    }
    (name, syntax)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(source: &str) -> Vec<String> {
        one_way_constructs(source)
            .unwrap()
            .into_iter()
            .map(|c| c.construct)
            .collect()
    }

    #[test]
    fn each_row_matches_its_own_example_and_not_its_baseline() {
        for row in ONE_WAY_CONSTRUCTS {
            assert!(
                ids(row.after).contains(&row.id.to_string()),
                "{} misses its example",
                row.id
            );
            assert!(ids(row.before).is_empty(), "{} baseline matches", row.id);
            crate::schema::parser::parse_schema(row.before).unwrap();
            crate::schema::parser::parse_schema(row.after).unwrap();
        }
    }

    #[test]
    fn constructs_outside_the_table_are_not_reported() {
        let source = r#"
interface Named { name: String @key }
node Doc implements Named {
    name: String @key
    title: String @index
    flag: Bool @index
    n: I64? @index
    emb: Vector(4) @index @embed("title")
    tags: [String]
    @index(tags)
}
edge Cites: Doc -> Doc { note: String? @index }
"#;
        crate::schema::parser::parse_schema(source).unwrap();
        assert_eq!(ids(source), Vec::<String>::new());
    }

    #[test]
    fn a_property_reports_every_row_it_writes_with_its_position() {
        let source = "node Doc {\n  slug: String @key\n  body: String @index(fulltext, analyzer = \"english_v1\", scorer = \"bm25_v1\")\n  emb: Vector(2, distance = \"cosine\")\n}\n";
        let found = one_way_constructs(source).unwrap();
        let rows: Vec<_> = found
            .iter()
            .map(|c| (c.construct.as_str(), c.property.as_str(), c.line, c.column))
            .collect();
        assert_eq!(
            rows,
            [
                ("full_text_index", "body", 3, 3),
                ("full_text_analyzer", "body", 3, 3),
                ("full_text_scorer", "body", 3, 3),
                ("vector_distance", "emb", 4, 3),
            ]
        );
        assert!(found.iter().all(|c| c.type_name == "Doc"));
    }
}
