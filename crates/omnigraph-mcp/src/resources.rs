//! Embedded reference resources and graph resource names.

use rmcp::model::ErrorData;

pub(super) const GRAPHS_RESOURCE: &str = "omnigraph://graphs";
pub(super) const BEST_PRACTICES_INDEX: &str = "omnigraph://best-practices/index";

/// One skill reference served as a resource. The body is the agent skill's
/// own `references/<key>.md`, embedded by `omnigraph-skill`; the description
/// is what a model reads at `resources/list` time to decide whether to fetch it.
pub(super) struct BestPractice {
    pub(super) key: &'static str,
    pub(super) title: &'static str,
    pub(super) description: &'static str,
}

impl BestPractice {
    pub(super) fn uri(&self) -> String {
        format!("omnigraph://best-practices/{}", self.key)
    }

    /// The embedded reference, or `None` when the skill no longer carries it
    /// as UTF-8; a unit test keeps every listed key present.
    pub(super) fn body(&self) -> Option<&'static str> {
        let path = format!("references/{}.md", self.key);
        omnigraph_skill::file(&path).and_then(|file| std::str::from_utf8(file.contents).ok())
    }
}

/// The references an MCP caller can act on. The skill's other references
/// cover the CLI, cluster configuration, server policy and upgrades, which are
/// operator work rather than MCP tools.
pub(super) const BEST_PRACTICES: &[BestPractice] = &[
    BestPractice {
        key: "queries",
        title: "GQ query best practices",
        description: "Read before writing a query for `query` or `mutate`. Covers .gq grammar, choosing a declared query by name, parameter declarations, the PascalCase to lowerCamelCase edge casing trap, search operators that need a trailing `limit`, and the non-nullable lint rule.",
    },
    BestPractice {
        key: "data",
        title: "Data load patterns",
        description: "Read before calling `load`. Covers choosing `merge`, `append` or `overwrite`, the branch, load, verify, merge loop for large or risky writes, embeddings that `merge` does not recompute, and when to use `mutate` instead.",
    },
    BestPractice {
        key: "schema",
        title: "Schema authoring and evolution",
        description: "Read to interpret `schema` and .pg declarations. Covers decorators, inline enums, stable renames, and add-optional then backfill evolution. Schema changes are operator-owned through cluster apply, not an MCP tool.",
    },
    BestPractice {
        key: "search",
        title: "Vector and full-text search",
        description: "Read before using `nearest`, `bm25` or `rrf`. Covers scoping first and ranking second, schema-declared embeddings, and the trailing `limit` requirement. Match vector dimensions and models to the live schema; CLI examples are not MCP tools.",
    },
    BestPractice {
        key: "changes",
        title: "Commits, change feeds and branch statements",
        description: "Read before `commits_changes`, `changes_poll` or a conditional `mutate`. Covers exact read and write receipts, 412 refusals, paginated feeds, baseline resets, and branch statement outcomes.",
    },
];

pub(super) fn best_practices_index() -> String {
    let mut text = String::from(
        "# OmniGraph best-practices index\n\nThese references come from the OmniGraph agent skill shipped with this server.\n\n| Resource | Read before |\n|---|---|\n",
    );
    for topic in BEST_PRACTICES {
        text.push_str(&format!("| `{}` | {} |\n", topic.uri(), topic.description));
    }
    text
}

/// A resource URI this server answers.
pub(super) enum GraphResource {
    Graphs,
    Schema(String),
    Branches(String),
    BestPracticesIndex,
    BestPractice(&'static BestPractice),
}

impl GraphResource {
    pub(super) fn parse(uri: &str) -> Option<Self> {
        if uri == GRAPHS_RESOURCE {
            return Some(Self::Graphs);
        }
        if uri == BEST_PRACTICES_INDEX {
            return Some(Self::BestPracticesIndex);
        }
        if let Some(key) = uri.strip_prefix("omnigraph://best-practices/") {
            return BEST_PRACTICES
                .iter()
                .find(|topic| topic.key == key)
                .map(Self::BestPractice);
        }
        let rest = uri.strip_prefix("omnigraph://graphs/")?;
        let (graph, kind) = rest.split_once('/')?;
        if graph.is_empty() {
            return None;
        }
        match kind {
            "schema" => Some(Self::Schema(graph.to_owned())),
            "branches" => Some(Self::Branches(graph.to_owned())),
            _ => None,
        }
    }

    pub(super) fn static_text(&self) -> Option<Result<String, ErrorData>> {
        match self {
            Self::BestPracticesIndex => Some(Ok(best_practices_index())),
            Self::BestPractice(topic) => Some(topic.body().map(str::to_owned).ok_or_else(|| {
                ErrorData::internal_error("the embedded skill does not carry this reference", None)
            })),
            Self::Graphs | Self::Schema(_) | Self::Branches(_) => None,
        }
    }
}

/// Graph-backed resources, dispatched through the same host handlers as tools.
#[derive(Debug)]
pub enum ResourceRequest {
    Graphs,
    Schema(String),
    Branches(String),
}

/// A resource's MIME type and bounded-by-the-transport body.
pub type ResourceOutput = (&'static str, String);

impl GraphResource {
    pub(super) fn into_request(self) -> Option<ResourceRequest> {
        match self {
            Self::Graphs => Some(ResourceRequest::Graphs),
            Self::Schema(name) => Some(ResourceRequest::Schema(name)),
            Self::Branches(name) => Some(ResourceRequest::Branches(name)),
            Self::BestPracticesIndex | Self::BestPractice(_) => None,
        }
    }
}
