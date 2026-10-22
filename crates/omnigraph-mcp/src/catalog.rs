//! MCP tool descriptions, gates, annotations and JSON schemas.

use crate::McpSettings;
use rmcp::model::{Tool, ToolAnnotations};
use serde_json::{Value, json};
use std::sync::Arc;

/// Which boot setting, if any, lists and admits a tool.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Gate {
    Always,
    Adhoc,
    Writes,
}

impl Gate {
    fn open(self, settings: &McpSettings) -> bool {
        match self {
            Gate::Always => true,
            Gate::Adhoc => settings.allow_adhoc,
            Gate::Writes => settings.allow_writes,
        }
    }
}

/// What a tool does to the graph, as MCP annotation hints.
#[derive(Clone, Copy)]
enum Effect {
    Read,
    /// Creates state without replacing any.
    Additive,
    Destructive {
        idempotent: bool,
    },
}

pub(super) struct ToolSpec {
    pub(super) name: &'static str,
    title: &'static str,
    description: &'static str,
    input: fn() -> Value,
    /// Success shape, for tools whose result is fixed. The declared output
    /// schema also admits the error envelope: clients validate any structured
    /// content against it, including an `isError` result's.
    output: Option<fn() -> Value>,
    effect: Effect,
    pub(super) gate: Gate,
}

const WRITE_CONTRACT: &str = " Never retried by the server. If the result says `outcome_unknown`, the write may or may not have happened: read the target and reconcile before replaying.";

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "health",
        title: "Server health",
        description: "Return the server's status, version and the storage format version it serves.",
        input: no_arguments_schema,
        output: Some(health_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "graphs",
        title: "List graphs",
        description: "List the graphs this server serves: each graph's `graph_id` and display name. Every other tool takes one of these ids as `graph`. Listing a graph does not mean your credential may read it. A credential that is not an identity needs the `graph_list` permission.",
        input: no_arguments_schema,
        output: Some(graphs_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "schema",
        title: "Read a graph's schema",
        description: "Return a graph's schema as `.pg` source (`schema_source`): its node and edge types, properties with their types, keys, edge endpoints and declared embeddings. Call this before writing any query: GQ names types, properties and edges exactly as declared here, and edges are matched in lowerCamelCase.",
        input: graph_schema,
        output: Some(schema_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "queries",
        title: "List stored reads",
        description: "List a graph's stored reads: each `name`, its description and instruction, and typed `params` (`name`, `kind`, `nullable`; `item_kind` for lists, `vector_dim` for vectors). A non-nullable parameter is required. Run one with `query`. Invocation is authorized separately, so a listed read can still be refused.",
        input: graph_schema,
        output: Some(queries_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "query",
        title: "Run a stored read",
        description: "Run a stored read by `name` with `params` keyed by its declared parameter names (see `queries`). Reads `branch`, `main` when omitted. Returns `rows`, `row_count` and the read's `graph_commit_id`. Stored mutations are refused.",
        input: query_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "read",
        title: "Run a GQ read",
        description: "Run a GQ read query you write. Call `schema` first. Declare every value as a typed parameter (`query people($min: I32) { match { $p: Person $p.age >= $min } return { $p.name } }`) and pass the values in `params`; pick one declared query with `name` when the source declares several. Reads `branch` (default `main`) or a `snapshot`, never both. Search operators (`nearest`, `bm25`, `rrf`) need a trailing `limit`. Also answers `branch list`, `branch diff <source> [into <target>]`, `branch merge-base …`, `branch merge … dry run` and `explain query …`, sent without `branch`, `snapshot`, `name` or `params`; and `explain load csv …`, which requires `change` permission before probing source metadata and answers the accepted load plan without reading payload bytes or publishing (`branch` allowed, never `snapshot`). Mutations and branch writes are refused. Returns `rows`, `row_count` and `graph_commit_id`.",
        input: read_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Adhoc,
    },
    ToolSpec {
        name: "snapshot",
        title: "Branch snapshot",
        description: "Return a branch's current snapshot (default `main`): the graph manifest version, the storage format version and every node and edge table with its entity count. Use it to judge a graph's size before querying.",
        input: branch_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "access",
        title: "Your access",
        description: "Return what your credential may do on a graph: your `actor_id`, the actions allowed whatever the branch (`graph_wide`), and, for a `branch` you name, the actions allowed on it (`actions`). Name a `target_branch` too to learn whether you may create it from `branch` or merge `branch` into it. Check this before a write instead of learning from a refusal. Branches are judged by name and need not exist.",
        input: access_schema,
        output: Some(access_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "branches_list",
        title: "List branches",
        description: "List a graph's branch names.",
        input: graph_schema,
        output: Some(branches_output_schema),
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "commits_list",
        title: "List commits",
        description: "List a branch's commits (default `main`), newest first, each with its `graph_commit_id`, parents, actor and time. Pass `limit` to page a long history: the page names `head_commit_id` and continues with its `next_page_token` as `page_token`, keeping the same `branch`.",
        input: commits_list_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "commits_get",
        title: "Get a commit",
        description: "Return one commit by its `graph_commit_id`.",
        input: commit_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "commits_changes",
        title: "A commit's entity changes",
        description: "Return one bounded page of a commit's exact entity changes relative to its first parent, each with `before` and `after`. Filter by `kind`, `type` and `op`. Continue with the returned `next_page_token` as `page_token`, keeping the same filters; a page token is not a feed cursor.",
        input: commit_changes_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "changes_poll",
        title: "Poll a change feed",
        description: "Read one bounded page of a branch's change feed (default `main`), at least once, in first-parent commit order. Start from `start` (`now`, the default, `beginning`, or `after:<commit id>`), a durable `cursor` from an earlier terminal page, or a `page_token`; these three are mutually exclusive. Keep filters unchanged across pages. Only a terminal page's `cursor` is durable: apply its complete commit blocks idempotently by `graph_commit_id`, then keep the cursor. A 410 `change_feed_gap` needs a new baseline, which this tool does not take.",
        input: changes_poll_schema,
        output: None,
        effect: Effect::Read,
        gate: Gate::Always,
    },
    ToolSpec {
        name: "mutate",
        title: "Run a GQ mutation",
        description: "Run a GQ mutation (`insert`, `update`, `delete`) on `branch` (default `main`) as one atomic commit, with every value passed as a declared, typed parameter in `params`. For read-modify-write, pass the `graph_commit_id` of your read as `if_graph_commit`: if the branch moved, the call fails with 412 and nothing is written; read again and reconsider, never retry without it. Returns affected counts and the exact `commit` (null for a successful no-op). Also runs `branch create`, `branch delete` and `branch merge` statements, sent without `branch`, `name` or `params`.",
        input: mutate_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "load",
        title: "Load NDJSON",
        description: "Load NDJSON records (`{\"type\": \"<Type>\", \"data\": {...}}` per line) into `branch` (default `main`) as one commit. `mode`: `merge` upserts by key, `append` refuses an existing key, `overwrite` replaces the supplied types. Without `from` the branch must exist; with `from` a missing branch is created from it. The whole call must fit in 64 KiB; split larger loads into separate commits.",
        input: load_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "branches_create",
        title: "Create a branch",
        description: "Create branch `name` from `from` (default `main`). Fails with 409 if it already exists.",
        input: branch_create_schema,
        output: None,
        effect: Effect::Additive,
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "branches_delete",
        title: "Delete a branch",
        description: "Delete branch `name`. Its commits stay reachable only through other branches. Fails with 404 if it does not exist.",
        input: branch_delete_schema,
        output: None,
        effect: Effect::Destructive { idempotent: true },
        gate: Gate::Writes,
    },
    ToolSpec {
        name: "branches_merge",
        title: "Merge a branch",
        description: "Merge branch `source` into `target` (default `main`). A merge that publishes returns its exact commit; an already up-to-date merge returns none. A conflict fails with 409 and `merge_conflicts`, writing nothing. To preview, run `branch merge <source> into <target> dry run` with `read`.",
        input: branch_merge_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
        gate: Gate::Writes,
    },
];

pub(super) fn spec(settings: &McpSettings, name: &str) -> Option<&'static ToolSpec> {
    TOOLS
        .iter()
        .find(|spec| spec.name == name && spec.gate.open(settings))
}

pub(super) fn instructions(settings: &McpSettings) -> String {
    let mut text = String::from(
        "OmniGraph serves typed property graphs. Every call runs under the applied policy for your credential; a listed tool or graph can still be refused.\n\
         1. Call `graphs` for graph ids. With a credential that is not an identity, it needs a `graph_list` permit, which a server without a cluster policy refuses; then ask the user for the graph id.\n\
         2. Call `schema` for the graph (or read `omnigraph://graphs/{graph}/schema`) before writing a query: it declares the types, properties, keys and edges a query may name.\n\
         3. Prefer a stored read: `queries` lists them with typed parameters, and `query` runs one.\n",
    );
    if settings.allow_adhoc {
        text.push_str(
            "4. Otherwise write a GQ read and run it with `read`, with every value passed as a declared, typed parameter. `read` refuses mutations.\n",
        );
    }
    if settings.allow_writes {
        text.push_str(
            "Writes: `mutate`, `load` and the `branches_*` tools each publish at most one commit. For read-modify-write, pass your read's `graph_commit_id` to `mutate` as `if_graph_commit`; a 412 means nothing was written. The server never retries a write. A result with `outcome_unknown`, or no response at all (a client timeout, a cancellation or a dropped connection), means the write may or may not have happened: read the target or list its commits and reconcile before replaying, and replay a `mutate` with `if_graph_commit`. Schema and deployment changes are operator-owned and not available here.\n",
        );
    }
    text.push_str(
        "Reads carry their `graph_commit_id`. A result is limited to 1 MiB and a read to 30 seconds; narrow the query with filters or `limit` when a result is too large. A refused call returns `isError` with the HTTP `status` and the server's `error` body, or an MCP `code` and `message`.",
    );
    text
}

pub(super) fn tools(settings: &McpSettings) -> Vec<Tool> {
    TOOLS
        .iter()
        .filter(|spec| spec.gate.open(settings))
        .map(|spec| {
            let annotations = match spec.effect {
                Effect::Read => ToolAnnotations::new().read_only(true),
                Effect::Additive => ToolAnnotations::new()
                    .read_only(false)
                    .destructive(false)
                    .idempotent(false),
                Effect::Destructive { idempotent } => ToolAnnotations::new()
                    .read_only(false)
                    .destructive(true)
                    .idempotent(idempotent),
            };
            let description = if spec.gate == Gate::Writes {
                format!("{}{WRITE_CONTRACT}", spec.description)
            } else {
                spec.description.to_owned()
            };
            let mut tool = Tool::new(spec.name, description, object((spec.input)()))
                .with_title(spec.title)
                .with_annotations(annotations.open_world(false));
            if let Some(output) = spec.output {
                tool = tool.with_raw_output_schema(Arc::new(object(json!({
                    "type": "object",
                    "anyOf": [output(), error_schema()],
                }))));
            }
            tool
        })
        .collect()
}

pub(super) fn object(value: Value) -> serde_json::Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => unreachable!("tool schemas are JSON object literals"),
    }
}

pub(super) fn text_schema(description: &str) -> Value {
    json!({"type": "string", "minLength": 1, "description": description})
}

pub(super) fn graph_argument() -> Value {
    text_schema("Graph id, from `graphs`.")
}

pub(super) fn arguments(properties: Value, required: &[&str]) -> Value {
    let mut properties = object(properties);
    properties.insert("graph".into(), graph_argument());
    let mut required: Vec<&str> = required.to_vec();
    required.insert(0, "graph");
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

pub(super) fn no_arguments_schema() -> Value {
    json!({"type": "object", "properties": {}, "additionalProperties": false})
}

pub(super) fn graph_schema() -> Value {
    arguments(json!({}), &[])
}

pub(super) fn branch_schema() -> Value {
    arguments(
        json!({"branch": text_schema("Branch. Defaults to `main`.")}),
        &[],
    )
}

pub(super) fn access_schema() -> Value {
    arguments(
        json!({
            "branch": text_schema("A branch whose actions to report."),
            "target_branch": text_schema("With `branch`, the branch it would be created into or merged into."),
        }),
        &[],
    )
}

pub(super) fn commits_list_schema() -> Value {
    arguments(
        json!({
            "branch": text_schema("Branch. Defaults to `main`."),
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Commits in this page; without it and `page_token`, the whole history."},
            "page_token": text_schema("The previous page's `next_page_token`."),
        }),
        &[],
    )
}

pub(super) fn commit_schema() -> Value {
    arguments(
        json!({"commit_id": text_schema("A `graph_commit_id`.")}),
        &["commit_id"],
    )
}

pub(super) fn query_schema() -> Value {
    arguments(
        json!({
            "name": text_schema("Stored read name, from `queries`."),
            "params": {"type": "object", "description": "Values keyed by the read's declared parameter names."},
            "branch": text_schema("Branch to read. Defaults to `main`."),
        }),
        &["name"],
    )
}

pub(super) fn read_schema() -> Value {
    arguments(
        json!({
            "query": text_schema("GQ source declaring one or more read queries, or one branch read or `explain` statement (`explain query …` or `explain load csv …`)."),
            "name": text_schema("Which declared query to run when `query` declares several."),
            "params": {"type": "object", "description": "Values keyed by the query's declared parameter names, without `$`."},
            "branch": text_schema("Branch to read. Defaults to `main`. Not with `snapshot`."),
            "snapshot": text_schema("Graph commit id to read instead of a branch head."),
        }),
        &["query"],
    )
}

pub(super) fn change_filter_properties() -> serde_json::Map<String, Value> {
    object(json!({
        "kind": {"type": "array", "items": {"type": "string", "enum": ["node", "edge"]}, "description": "Only these entity kinds."},
        "type": {"type": "array", "items": {"type": "string", "minLength": 1}, "description": "Only these schema type names."},
        "op": {"type": "array", "items": {"type": "string", "enum": ["insert", "update", "delete"]}, "description": "Only these operations."},
        "page_token": text_schema("The previous page's `next_page_token`."),
        "limit": {"type": "integer", "minimum": 1, "description": "Maximum changes in this page."},
    }))
}

pub(super) fn commit_changes_schema() -> Value {
    let mut properties = change_filter_properties();
    properties.insert("commit_id".into(), text_schema("A `graph_commit_id`."));
    arguments(Value::Object(properties), &["commit_id"])
}

pub(super) fn changes_poll_schema() -> Value {
    let mut properties = change_filter_properties();
    properties.insert(
        "branch".into(),
        text_schema("Branch whose feed to read. Defaults to `main`."),
    );
    properties.insert(
        "cursor".into(),
        text_schema("A durable cursor from an earlier terminal page."),
    );
    properties.insert(
        "start".into(),
        json!({"type": "string", "pattern": "^(now|beginning|after:.+)$", "description": "`now` (default), `beginning`, or `after:<commit id>`."}),
    );
    arguments(Value::Object(properties), &[])
}

pub(super) fn mutate_schema() -> Value {
    arguments(
        json!({
            "query": text_schema("GQ source declaring one or more mutations, or one branch write statement."),
            "name": text_schema("Which declared mutation to run when `query` declares several."),
            "params": {"type": "object", "description": "Values keyed by the mutation's declared parameter names, without `$`."},
            "branch": text_schema("Branch to write. Defaults to `main`."),
            "if_graph_commit": text_schema("Write only while the branch head is still this `graph_commit_id`."),
        }),
        &["query"],
    )
}

pub(super) fn load_schema() -> Value {
    arguments(
        json!({
            "branch": text_schema("Branch to load into. Defaults to `main`."),
            "from": text_schema("Create `branch` from this branch when it does not exist."),
            "mode": {"type": "string", "enum": ["merge", "append", "overwrite"], "description": "How existing keys are handled."},
            "data": text_schema("NDJSON: one `{\"type\": \"<Type>\", \"data\": {...}}` record per line."),
        }),
        &["mode", "data"],
    )
}

pub(super) fn branch_create_schema() -> Value {
    arguments(
        json!({
            "name": text_schema("The new branch's name."),
            "from": text_schema("Branch to fork; the new branch starts at its head. Defaults to `main`."),
            "from_commit": text_schema("A published graph commit id to start the branch at instead of a branch head; excludes `from`."),
        }),
        &["name"],
    )
}

pub(super) fn branch_delete_schema() -> Value {
    arguments(
        json!({"name": text_schema("The branch to delete.")}),
        &["name"],
    )
}

pub(super) fn branch_merge_schema() -> Value {
    arguments(
        json!({
            "source": text_schema("The branch to merge."),
            "target": text_schema("The branch that receives the merge. Defaults to `main`."),
        }),
        &["source"],
    )
}

/// The `isError` envelope: the error body an HTTP route answers under
/// `error`, with the HTTP `status` the same failure answers with; a refusal
/// only MCP makes carries no status.
pub(super) fn error_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {"type": "integer"},
            "error": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"},
                    "code": {"type": "string"},
                    "disposition": {"type": "object"},
                },
                "required": ["error", "code", "disposition"],
            },
        },
        "required": ["error"],
        "additionalProperties": false,
    })
}

pub(super) fn health_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "version": {"type": "string"},
            "internal_schema_version": {"type": "integer"},
            "source_version": {"type": "string"},
        },
        "required": ["status", "version", "internal_schema_version"],
    })
}

pub(super) fn graphs_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"graphs": {"type": "array", "items": {
            "type": "object",
            "properties": {"graph_id": {"type": "string"}, "display_name": {"type": "string"}},
            "required": ["graph_id", "display_name"],
        }}},
        "required": ["graphs"],
    })
}

pub(super) fn access_output_schema() -> Value {
    let actions = json!({
        "type": "array",
        "items": {"type": "string", "enum": [
            "read", "export", "change", "schema_apply", "branch_create", "branch_delete",
            "branch_merge", "commit_publish", "commit_drop", "invoke_query"
        ]},
    });
    json!({
        "type": "object",
        "properties": {
            "actor_id": {"type": ["string", "null"]},
            "graph_id": {"type": "string"},
            "graph_wide": actions,
            "branch": {"type": ["string", "null"]},
            "target_branch": {"type": ["string", "null"]},
            "actions": actions,
        },
        "required": ["actor_id", "graph_id", "graph_wide", "branch", "target_branch", "actions"],
    })
}

pub(super) fn branches_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"branches": {"type": "array", "items": {"type": "string"}}},
        "required": ["branches"],
    })
}

pub(super) fn schema_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_source": {"type": "string"},
            "system_columns": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "src": {"type": "string"}, "dst": {"type": "string"}},
                "required": ["id", "src", "dst"],
            },
        },
        "required": ["schema_source"],
    })
}

pub(super) fn queries_output_schema() -> Value {
    let kind = json!({"type": "string", "enum": ["string", "bool", "int", "bigint", "float", "date", "datetime", "blob", "vector", "list"]});
    json!({
        "type": "object",
        "properties": {"queries": {"type": "array", "items": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "tool_name": {"type": "string"},
                "description": {"type": "string"},
                "instruction": {"type": "string"},
                "mutation": {"type": "boolean"},
                "params": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "kind": kind,
                        "item_kind": kind,
                        "vector_dim": {"type": "integer"},
                        "nullable": {"type": "boolean"},
                    },
                    "required": ["name", "kind", "nullable"],
                }},
            },
            "required": ["name", "tool_name", "mutation", "params"],
        }}},
        "required": ["queries"],
    })
}
