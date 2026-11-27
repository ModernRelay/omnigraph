//! Schema constructs an earlier binary cannot read back.
//!
//! A graph stores its accepted `.pg` source beside the IR, and a binary
//! re-parses that source on every open. Once an apply stores a construct an
//! earlier binary's parser or feature set refuses, that binary refuses the
//! whole graph until a later apply stores none of them (where the schema
//! allows the change) or the backup taken before the upgrade is restored.
//! [`ONE_WAY_CONSTRUCTS`] is the one table of those constructs, each row
//! naming the first release that reads its construct and matching either a
//! property's syntax or a whole declaration's, so a later release registers
//! its constructs as rows: `schema plan`, `schema apply` and `cluster plan`
//! report each match of a candidate source through [`one_way_constructs`],
//! with [`one_way_note`], and the cross-version owner proves every row's
//! example against the genuine predecessor binary.

use pest::Parser as _;
use pest::iterators::Pair;
use serde::{Deserialize, Serialize};

use super::parser::{Rule, SchemaParser};
use crate::error::{CompilerError, Result};

/// The line a report of one-way constructs carries, given each one's first
/// release; `None` when there is none: a binary before the latest of those
/// releases refuses the whole graph.
pub fn one_way_note<'a>(first_releases: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let release = first_releases
        .into_iter()
        .max_by_key(|release| release_order(release))?;
    Some(format!(
        "binaries before {release} refuse the whole graph after this apply, until an apply stores none of these constructs or the backup taken before the upgrade is restored"
    ))
}

/// A release's `major.minor.patch` as numbers, so `0.10.0` follows `0.9.0`.
/// A part that is not a number orders first; the table's test refuses one.
fn release_order(release: &str) -> [u64; 3] {
    let mut parts = release.split('.').map(|part| part.parse().unwrap_or(0));
    [(); 3].map(|()| parts.next().unwrap_or(0))
}

/// The release that ships GQ 3, the first to read every construct of
/// today's rows.
const GQ_3_RELEASE: &str = "0.18.0";

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

/// One interface, node or edge declaration as the one-way rows read it: its
/// syntax around its properties.
#[derive(Debug)]
pub struct DeclarationSyntax {
    /// `interface`, `node` or `edge`.
    pub kind: &'static str,
    /// The interfaces an `implements` clause names.
    pub implements: Vec<String>,
    /// The names of the annotations on the declaration itself.
    pub annotations: Vec<String>,
    /// An edge's written source and target types.
    pub endpoints: Option<(String, String)>,
}

/// What a row of [`ONE_WAY_CONSTRUCTS`] matches.
#[derive(Clone, Copy)]
pub enum OneWayMatch {
    /// A property declaration that writes the construct.
    Property(fn(&PropertySyntax) -> bool),
    /// A whole declaration that writes the construct.
    Declaration(fn(&DeclarationSyntax) -> bool),
}

/// A row of [`ONE_WAY_CONSTRUCTS`].
#[derive(Clone, Copy)]
pub struct OneWayRow {
    /// The stable name a report carries.
    pub id: &'static str,
    /// The construct, as an operator reads it.
    pub description: &'static str,
    /// The first release that reads the construct: every earlier binary
    /// refuses a graph storing it.
    pub first_release: &'static str,
    /// The syntax that writes the construct.
    pub matches: OneWayMatch,
    /// A schema without the construct, and the same schema with it: applying
    /// `after` over `before` stores the construct through a plan with a step.
    pub before: &'static str,
    pub after: &'static str,
}

/// Every construct an earlier binary refuses in a stored schema.
pub const ONE_WAY_CONSTRUCTS: &[OneWayRow] = &[
    OneWayRow {
        id: "vector_distance",
        first_release: GQ_3_RELEASE,
        description: "an explicit `distance` on a Vector, `l2` included",
        matches: OneWayMatch::Property(|p| p.vector_options.iter().any(|o| o == "distance")),
        before: "node Doc { slug: String @key }\n",
        after: "node Doc { slug: String @key emb: Vector(2, distance = \"l2\")? }\n",
    },
    OneWayRow {
        id: "list_index",
        first_release: GQ_3_RELEASE,
        description: "`@index` on a List property",
        matches: OneWayMatch::Property(|p| p.list && p.plain_index),
        before: "node Doc { slug: String @key tags: [String] }\n",
        after: "node Doc { slug: String @key tags: [String] @index }\n",
    },
    OneWayRow {
        id: "full_text_index",
        first_release: GQ_3_RELEASE,
        description: "`@index(fulltext)`, with or without options",
        matches: OneWayMatch::Property(|p| p.full_text_options.is_some()),
        before: "node Doc { slug: String @key title: String @index }\n",
        after: "node Doc { slug: String @key title: String @index(fulltext) }\n",
    },
    OneWayRow {
        id: "full_text_analyzer",
        first_release: GQ_3_RELEASE,
        description: "an `analyzer` in `@index(fulltext, ...)`",
        matches: OneWayMatch::Property(|p| {
            p.full_text_options
                .as_ref()
                .is_some_and(|o| o.iter().any(|o| o == "analyzer"))
        }),
        before: "node Doc { slug: String @key title: String }\n",
        after: "node Doc { slug: String @key title: String @index(fulltext, analyzer = \"standard_v1\") }\n",
    },
    OneWayRow {
        id: "full_text_scorer",
        first_release: GQ_3_RELEASE,
        description: "a `scorer` in `@index(fulltext, ...)`",
        matches: OneWayMatch::Property(|p| {
            p.full_text_options
                .as_ref()
                .is_some_and(|o| o.iter().any(|o| o == "scorer"))
        }),
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
    /// The row's first release: binaries before it refuse the graph.
    pub first_release: String,
    /// The interface, node or edge declaring the construct.
    pub type_name: String,
    /// The property writing it; absent for a declaration's construct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    /// Where the matched declaration starts, 1-based.
    pub line: usize,
    pub column: usize,
}

/// Every one-way construct `source` writes, in source order: a
/// declaration's own before its properys', and a declaration or property
/// matching several rows once per row.
pub fn one_way_constructs(source: &str) -> Result<Vec<OneWayConstruct>> {
    constructs_of(source, ONE_WAY_CONSTRUCTS)
}

fn constructs_of(source: &str, rows: &[OneWayRow]) -> Result<Vec<OneWayConstruct>> {
    let file = SchemaParser::parse(Rule::schema_file, source)
        .map_err(|e| CompilerError::Parse(e.to_string()))?;
    let mut found = Vec::new();
    let report = |row: &OneWayRow, type_name: &str, property: Option<&str>, at: (usize, usize)| {
        OneWayConstruct {
            construct: row.id.to_string(),
            description: row.description.to_string(),
            first_release: row.first_release.to_string(),
            type_name: type_name.to_string(),
            property: property.map(str::to_string),
            line: at.0,
            column: at.1,
        }
    };
    for decl in file.flatten() {
        let kind = match decl.as_rule() {
            Rule::interface_decl => "interface",
            Rule::node_decl => "node",
            Rule::edge_decl => "edge",
            _ => continue,
        };
        let at = decl.as_span().start_pos().line_col();
        let items: Vec<Pair<'_, Rule>> = decl.into_inner().collect();
        let Some(type_name) = items.first().map(|name| name.as_str().to_string()) else {
            continue;
        };
        let declaration = declaration_syntax(kind, &items);
        for row in rows {
            if let OneWayMatch::Declaration(matches) = row.matches
                && matches(&declaration)
            {
                found.push(report(row, &type_name, None, at));
            }
        }
        for prop in items
            .into_iter()
            .filter(|pair| pair.as_rule() == Rule::prop_decl)
        {
            let at = prop.as_span().start_pos().line_col();
            let (property, syntax) = property_syntax(prop);
            for row in rows {
                if let OneWayMatch::Property(matches) = row.matches
                    && matches(&syntax)
                {
                    found.push(report(row, &type_name, Some(&property), at));
                }
            }
        }
    }
    Ok(found)
}

/// The declaration syntax of `items`, a declaration's parts after its kind
/// keyword: its name first.
fn declaration_syntax(kind: &'static str, items: &[Pair<'_, Rule>]) -> DeclarationSyntax {
    let names = |pair: &Pair<'_, Rule>| -> Vec<String> {
        pair.clone()
            .into_inner()
            .map(|name| name.as_str().to_string())
            .collect()
    };
    let type_names: Vec<String> = items
        .iter()
        .skip(1)
        .take_while(|pair| pair.as_rule() == Rule::type_name)
        .map(|pair| pair.as_str().to_string())
        .collect();
    DeclarationSyntax {
        kind,
        implements: items
            .iter()
            .filter(|pair| pair.as_rule() == Rule::implements_clause)
            .flat_map(names)
            .collect(),
        annotations: items
            .iter()
            .filter(|pair| pair.as_rule() == Rule::annotation)
            .filter_map(|pair| pair.clone().into_inner().next())
            .map(|name| name.as_str().to_string())
            .collect(),
        endpoints: match type_names.as_slice() {
            [source, target] if kind == "edge" => Some((source.clone(), target.clone())),
            _ => None,
        },
    }
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

    /// Every row names the first release that reads its construct, a
    /// `major.minor.patch` no later than the open release cycle
    /// (`changelog.d/release.json`): no row is first read by a release that
    /// is not being built.
    #[test]
    fn each_row_names_a_release_no_later_than_the_open_cycle() {
        let config = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../changelog.d/release.json"),
        )
        .unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        let open = config["version"].as_str().unwrap().trim_start_matches('v');
        for row in ONE_WAY_CONSTRUCTS {
            let parts: Vec<_> = row.first_release.split('.').collect();
            assert!(
                parts.len() == 3 && parts.iter().all(|part| part.parse::<u64>().is_ok()),
                "{} names `{}`, not major.minor.patch",
                row.id,
                row.first_release
            );
            assert!(
                release_order(row.first_release) <= release_order(open),
                "{} names {}, after the open cycle {open}",
                row.id,
                row.first_release
            );
        }
    }

    /// A row can match a whole declaration: its kind, its `implements`, its
    /// own annotations, an edge's written endpoints. A declaration's match
    /// names no property and sits at the declaration, before the matches
    /// of its properties.
    #[test]
    fn a_row_matches_a_whole_declaration() {
        let rows = [
            OneWayRow {
                id: "declared_implements",
                description: "a node that implements an interface",
                first_release: "0.19.0",
                matches: OneWayMatch::Declaration(|d| d.kind == "node" && !d.implements.is_empty()),
                before: "",
                after: "",
            },
            OneWayRow {
                id: "edge_into_doc",
                description: "an edge into `Doc`",
                first_release: "0.19.0",
                matches: OneWayMatch::Declaration(|d| {
                    d.endpoints.as_ref().is_some_and(|(_, to)| to == "Doc")
                }),
                before: "",
                after: "",
            },
            ONE_WAY_CONSTRUCTS[0],
        ];
        let source = "interface Named { name: String }\nnode Doc implements Named {\n  name: String @key\n  emb: Vector(2, distance = \"l2\")?\n}\nedge Cites: Doc -> Doc\n";
        crate::schema::parser::parse_schema(source).unwrap();
        let found = constructs_of(source, &rows).unwrap();
        let named: Vec<_> = found
            .iter()
            .map(|c| {
                (
                    c.construct.as_str(),
                    c.type_name.as_str(),
                    c.property.as_deref(),
                    c.line,
                    c.column,
                )
            })
            .collect();
        assert_eq!(
            named,
            [
                ("declared_implements", "Doc", None, 2, 1),
                ("vector_distance", "Doc", Some("emb"), 4, 3),
                ("edge_into_doc", "Cites", None, 6, 1),
            ]
        );
        // The note names the latest release among the matches.
        assert_eq!(
            one_way_note(found.iter().map(|c| c.first_release.as_str())).unwrap(),
            "binaries before 0.19.0 refuse the whole graph after this apply, until an apply stores none of these constructs or the backup taken before the upgrade is restored"
        );
        assert_eq!(one_way_note([]), None);
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
            .map(|c| {
                (
                    c.construct.as_str(),
                    c.property.as_deref().unwrap(),
                    c.line,
                    c.column,
                )
            })
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
        assert!(found.iter().all(|c| c.first_release == GQ_3_RELEASE));
        assert_eq!(
            one_way_note(found.iter().map(|c| c.first_release.as_str())).unwrap(),
            "binaries before 0.18.0 refuse the whole graph after this apply, until an apply stores none of these constructs or the backup taken before the upgrade is restored"
        );
    }
}
