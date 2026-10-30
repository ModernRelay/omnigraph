//! MCP tool descriptions, annotations and JSON schemas.

use rmcp::model::{Tool, ToolAnnotations};
use serde_json::{Value, json};
use std::sync::Arc;

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
}

impl ToolSpec {
    /// Whether the tool can publish: it carries the write contract, the write
    /// deadline and the write result disposition.
    pub(super) fn writes(&self) -> bool {
        !matches!(self.effect, Effect::Read)
    }
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
    },
    ToolSpec {
        name: "graphs",
        title: "List graphs",
        description: "List the graphs this server serves: each graph's `graph_id` and display name. Every other tool takes one of these ids as `graph`. Listing a graph does not mean your credential may read it. A credential that is not an identity needs the `graph_list` permission.",
        input: no_arguments_schema,
        output: Some(graphs_output_schema),
        effect: Effect::Read,
    },
    ToolSpec {
        name: "schema",
        title: "Read a graph's schema",
        description: "Return a graph's schema as `.pg` source (`schema_source`): its node and edge types, properties with their types, keys, edge endpoints and declared embeddings. Call this before writing any query: GQ names types, properties and edges exactly as declared here, and edges are matched in lowerCamelCase.",
        input: graph_schema,
        output: Some(schema_output_schema),
        effect: Effect::Read,
    },
    ToolSpec {
        name: "queries",
        title: "List stored queries",
        description: "List a graph's stored queries: each `name`, its description and instruction, whether it is a `mutation`, and typed `params` (`name`, `kind`, `nullable`; `item_kind` for lists, `vector_dim` for vectors). A non-nullable parameter is required. Run a stored read with `query`. Run a stored mutation with `mutate`, passing its `name` and no `query`. Invocation is authorized separately, so a listed query can still be refused.",
        input: graph_schema,
        output: Some(queries_output_schema),
        effect: Effect::Read,
    },
    ToolSpec {
        name: "query",
        title: "Run a stored read",
        description: "Run a stored read by `name` with `params` keyed by its declared parameter names (see `queries`). Reads `branch`, `main` when omitted. Returns `rows`, `row_count` and the read's `graph_commit_id`, plus `ranking` when the read orders by a search. Stored mutations are refused here; `mutate` runs them, and policy can still refuse it.",
        input: query_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "read",
        title: "Run a GQ read",
        description: "Run a GQ read query you write. Call `schema` first. Declare every value as a typed parameter (`query people($min: I32) { match { $p: Person $p.age >= $min } return { $p.name } }`) and pass the values in `params`; pick one declared query with `name` when the source declares several. Reads `branch` (default `main`) or a `snapshot`, never both. Search operators (`nearest`, `bm25`, `rrf`) need a trailing `limit`. Also answers `branch list`, `branch diff <source> [into <target>]`, `branch merge-base …`, `branch merge … dry run` and `explain query …`, sent without `branch`, `snapshot`, `name` or `params`; and `explain load csv …`, which requires `change` permission before probing source metadata and answers the accepted load plan without reading payload bytes or publishing (`branch` allowed, never `snapshot`). Mutations and branch writes are refused. Returns `rows`, `row_count` and `graph_commit_id`, plus `ranking` for a search order: `exhaustive: false` means the vector search left index partitions unsearched, so a nearer row may exist.",
        input: read_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "snapshot",
        title: "Branch snapshot",
        description: "Return a branch's current snapshot (default `main`): the graph manifest version, the storage format version and every node and edge table with its entity count. Use it to judge a graph's size before querying.",
        input: branch_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "access",
        title: "Your access",
        description: "Return what your credential may do on a graph: your `actor_id`, the actions allowed whatever the branch (`graph_wide`), and, for a `branch` you name, the actions allowed on it (`actions`). Name a `target_branch` too to learn whether you may create it from `branch` or merge `branch` into it. Check this before a write instead of learning from a refusal. Branches are judged by name and need not exist.",
        input: access_schema,
        output: Some(access_output_schema),
        effect: Effect::Read,
    },
    ToolSpec {
        name: "branches_list",
        title: "List branches",
        description: "List a graph's branch names. With `detailed`, also each readable branch's head commit, the branch and commit it started from, and when it was created and last moved.",
        input: branches_list_schema,
        output: Some(branches_output_schema),
        effect: Effect::Read,
    },
    ToolSpec {
        name: "branches_diff",
        title: "What a branch changed",
        description: "Return one bounded page of what branch `source` changed since its merge base with `target` (default `main`): the net entity changes from that base to the source head, each with `before` and `after`, which is what `branches_merge` would bring. Changes `target` made after the base do not appear. Filter by `kind`, `type` and `op`, and continue with the returned `next_page_token` as `page_token`, keeping the other arguments.",
        input: branch_diff_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "branches_merge_base",
        title: "Merge base of two branches",
        description: "Return the commit a merge of branch `source` into `target` (default `main`) would compare against, with the two heads it was found from (`source_commit_id`, `target_commit_id`). Reads lineage only.",
        input: merge_base_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "commits_list",
        title: "List commits",
        description: "List a branch's commits (default `main`), newest first, each with its `graph_commit_id`, parents, actor and time. Pass `limit` to page a long history: the page names `head_commit_id` and continues with its `next_page_token` as `page_token`, keeping the same `branch`. Pass `listing` as `unpublished` for the proposals written `as commit` that await `commits_publish`, or `unattached` for snapshots written `as unattached branch`, whose recorded target is `branch`; these listings take no `limit` or `page_token`.",
        input: commits_list_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "commits_get",
        title: "Get a commit",
        description: "Return one commit by its `graph_commit_id`.",
        input: commit_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "commits_changes",
        title: "A commit's entity changes",
        description: "Return one bounded page of a commit's exact entity changes relative to its first parent, each with `before` and `after`. Filter by `kind`, `type` and `op`. Continue with the returned `next_page_token` as `page_token`, keeping the same filters; a page token is not a feed cursor.",
        input: commit_changes_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "diff",
        title: "Diff two read targets",
        description: "Return one bounded page of the net entity changes between `from` and `to`, each a branch name (its head now) or a graph commit id (published, an unpublished proposal or an unattached snapshot), each change with `before` and `after`. The first page names the two commits it resolved; continue with the returned `next_page_token` as `page_token`, keeping the other arguments, and every page answers for those same commits. Filter by `kind`, `type` and `op`. A side you cannot read is reported as not found.",
        input: diff_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "changes_poll",
        title: "Poll a change feed",
        description: "Read one bounded page of a branch's change feed (default `main`), at least once, in first-parent commit order. Start from `start` (`now`, the default, `beginning`, or `after:<commit id>`), a durable `cursor` from an earlier terminal page, or a `page_token`; these three are mutually exclusive. Keep filters unchanged across pages. Only a terminal page's `cursor` is durable: apply its complete commit blocks idempotently by `graph_commit_id`, then keep the cursor. A 410 `change_feed_gap` needs a new baseline, a full snapshot stream that MCP does not carry: capture it over HTTP or with the CLI.",
        input: changes_poll_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "blobs_stat",
        title: "Describe a Blob cell",
        description: "Describe one non-null Blob cell, selected by `entity` (`node` or `edge`), `type`, `id` and `property`, on `branch` (default `main`) or at a `snapshot`, never both: a `managed` value's `size` and strong `etag`, or an `external` value's `uri`, with the resolved `target`. Reads no payload bytes, and payloads are not served over MCP. A null cell or an unknown entity is not found.",
        input: blobs_stat_schema,
        output: None,
        effect: Effect::Read,
    },
    ToolSpec {
        name: "mutate",
        title: "Run a GQ mutation",
        description: "Run a GQ mutation (`insert`, `update`, `delete`) on `branch` (default `main`) as one atomic commit, with every value passed as a declared, typed parameter in `params`. To run a stored mutation from `queries` instead, pass its `name` and `params` without `query`. For read-modify-write, pass the `graph_commit_id` of your read as `if_graph_commit`: if the branch moved, the call fails with 412 and nothing is written; read again and reconsider, never retry without it. Returns affected counts and the exact `commit` (null for a successful no-op). Also runs `branch create`, `branch delete` and `branch merge` statements, sent without `branch`, `name` or `params`.",
        input: mutate_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
    },
    ToolSpec {
        name: "load",
        title: "Load NDJSON",
        description: "Load NDJSON records (`{\"type\": \"<Type>\", \"data\": {...}}` per line) into `branch` (default `main`) as one commit. `mode`: `merge` upserts by key, `append` refuses an existing key, `overwrite` replaces the supplied types. Without `from` the branch must exist; with `from` a missing branch is created from it. The whole call must fit in 64 KiB; split larger loads into separate commits.",
        input: load_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
    },
    ToolSpec {
        name: "blobs_clear",
        title: "Clear a Blob cell",
        description: "Set one nullable Blob cell, selected by `entity` (`node` or `edge`), `type`, `id` and `property`, to null on `branch` (default `main`) as one commit. A cell that is already null publishes nothing and returns `commit: null`. Pass the `etag` from `blobs_stat` as `if_match`, or `*` to require a value: if the cell no longer matches, the call fails with 412 and nothing is written.",
        input: blobs_clear_schema,
        output: None,
        effect: Effect::Destructive { idempotent: true },
    },
    ToolSpec {
        name: "branches_create",
        title: "Create a branch",
        description: "Create branch `name` from `from` (default `main`). Fails with 409 if it already exists.",
        input: branch_create_schema,
        output: None,
        effect: Effect::Additive,
    },
    ToolSpec {
        name: "branches_delete",
        title: "Delete a branch",
        description: "Delete branch `name`. Its commits stay reachable only through other branches. Fails with 404 if it does not exist.",
        input: branch_delete_schema,
        output: None,
        effect: Effect::Destructive { idempotent: true },
    },
    ToolSpec {
        name: "branches_merge",
        title: "Merge a branch",
        description: "Merge branch `source` into `target` (default `main`). A merge that publishes returns its exact commit; an already up-to-date merge returns none. A conflict fails with 409 and `merge_conflicts`, writing nothing. To preview, run `branch merge <source> into <target> dry run` with `read`.",
        input: branch_merge_schema,
        output: None,
        effect: Effect::Destructive { idempotent: false },
    },
    ToolSpec {
        name: "commits_publish",
        title: "Publish a proposal",
        description: "Publish an unpublished commit, one written `as commit` (list them with `commits_list` and `listing: unpublished`), onto the branch it recorded, and return it as published. A commit already published is returned as it stands. If that branch moved past the commit's parent, the call fails with 412 and the commit can only be dropped. An unattached snapshot never publishes (409).",
        input: commit_schema,
        output: None,
        effect: Effect::Destructive { idempotent: true },
    },
    ToolSpec {
        name: "commits_drop",
        title: "Drop a proposal or snapshot",
        description: "Drop an unpublished commit or an unattached snapshot by its `graph_commit_id`, together with every unattached descendant. Irreversible. A published commit is never dropped (409). Returns an empty object.",
        input: commit_schema,
        output: None,
        effect: Effect::Destructive { idempotent: true },
    },
];

pub(super) fn spec(name: &str) -> Option<&'static ToolSpec> {
    TOOLS.iter().find(|spec| spec.name == name)
}

/// The instructions every MCP session receives.
pub(super) const INSTRUCTIONS: &str = concat!(
    "OmniGraph serves typed property graphs. Every call runs under the applied policy for your credential; a listed tool or graph can still be refused.\n",
    "1. Call `graphs` for graph ids. With a credential that is not an identity, it needs a `graph_list` permit, which a server without a cluster policy refuses; then ask the user for the graph id.\n",
    "2. Call `schema` for the graph (or read `omnigraph://graphs/{graph}/schema`) before writing a query: it declares the types, properties, keys and edges a query may name.\n",
    "3. Prefer a stored read: `queries` lists stored queries with typed parameters, and `query` runs a stored read.\n",
    "4. Otherwise write a GQ read and run it with `read`, with every value passed as a declared, typed parameter. `read` refuses mutations.\n",
    "Writes: `mutate`, `load`, `blobs_clear`, `branches_create`, `branches_delete`, `branches_merge`, `commits_publish` and `commits_drop` each publish at most one commit. `mutate` with `name` and no `query` runs a stored mutation from `queries`. For read-modify-write, pass your read's `graph_commit_id` to `mutate` as `if_graph_commit`; a 412 means nothing was written. The server never retries a write. A result with `outcome_unknown`, or no response at all (a client timeout, a cancellation or a dropped connection), means the write may or may not have happened: read the target or list its commits and reconcile before replaying, and replay a `mutate` with `if_graph_commit`. Schema and deployment changes are operator-owned and not available here.\n",
    "Reads carry their `graph_commit_id`. A result is limited to 1 MiB and a read to 30 seconds; narrow the query with filters or `limit` when a result is too large. A refused call returns `isError` with the HTTP `status` and the server's `error` body, or an MCP `code` and `message`.",
);

pub(super) fn tools() -> Vec<Tool> {
    TOOLS
        .iter()
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
            let description = if spec.writes() {
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

pub(super) fn branches_list_schema() -> Value {
    arguments(
        json!({"detailed": {"type": "boolean", "description": "`true` adds `details`: for each branch the caller may read, its `head` commit, the branch (`from`) and commit (`from_commit_id`) it started from, and `created_at` and `updated_at`. Defaults to `false`."}}),
        &[],
    )
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
            "listing": {"type": "string", "enum": ["history", "unpublished", "unattached"], "description": "`history` (default), or the `unpublished` proposals or `unattached` snapshots whose recorded target is `branch`. Only `history` pages."},
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

pub(super) fn diff_schema() -> Value {
    let mut properties = change_filter_properties();
    properties.insert(
        "from".into(),
        text_schema("Where the diff starts: a branch name (its head now) or a graph commit id."),
    );
    properties.insert(
        "to".into(),
        text_schema("Where the diff ends, a branch name or a graph commit id."),
    );
    arguments(Value::Object(properties), &["from", "to"])
}

pub(super) fn branch_diff_schema() -> Value {
    let mut properties = change_filter_properties();
    properties.insert(
        "source".into(),
        text_schema("The branch whose changes to report."),
    );
    properties.insert(
        "target".into(),
        text_schema("The branch `source` is compared against. Defaults to `main`."),
    );
    arguments(Value::Object(properties), &["source"])
}

pub(super) fn merge_base_schema() -> Value {
    arguments(
        json!({
            "source": text_schema("The branch that would be merged."),
            "target": text_schema("The branch that would receive the merge. Defaults to `main`."),
        }),
        &["source"],
    )
}

/// One Blob cell's selector, named as the Blob routes' query parameters.
pub(super) fn blob_cell_properties() -> serde_json::Map<String, Value> {
    object(json!({
        "entity": {"type": "string", "enum": ["node", "edge"], "description": "Whether `type` is a node or an edge type."},
        "type": text_schema("The node or edge type, from `schema`."),
        "id": text_schema("The entity's id within `type`: a node's key, or an edge's id."),
        "property": text_schema("A Blob property of `type`."),
    }))
}

pub(super) fn blobs_stat_schema() -> Value {
    let mut properties = blob_cell_properties();
    properties.insert(
        "branch".into(),
        text_schema("Branch to read. Defaults to `main`. Not with `snapshot`."),
    );
    properties.insert(
        "snapshot".into(),
        text_schema("Graph commit id to read instead of a branch head."),
    );
    arguments(
        Value::Object(properties),
        &["entity", "type", "id", "property"],
    )
}

pub(super) fn blobs_clear_schema() -> Value {
    let mut properties = blob_cell_properties();
    properties.insert(
        "branch".into(),
        text_schema("Branch to write. Defaults to `main`."),
    );
    properties.insert(
        "if_match".into(),
        text_schema("The route's `If-Match`: `*` to require a value, or strong entity tags such as an `etag` from `blobs_stat`, one of which must be the cell's current one."),
    );
    arguments(
        Value::Object(properties),
        &["entity", "type", "id", "property"],
    )
}

pub(super) fn mutate_schema() -> Value {
    arguments(
        json!({
            "query": text_schema("GQ source declaring one or more mutations, or one branch write statement. Omit it to run the stored mutation `name`."),
            "name": text_schema("With `query`, which declared mutation to run when it declares several. Without `query`, the stored mutation to run, from `queries`."),
            "params": {"type": "object", "description": "Values keyed by the mutation's declared parameter names, without `$`."},
            "branch": text_schema("Branch to write. Defaults to `main`."),
            "if_graph_commit": text_schema("Write only while the branch head is still this `graph_commit_id`."),
        }),
        &[],
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
        "properties": {
            "branches": {"type": "array", "items": {"type": "string"}},
            "details": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "head": {"type": "object"},
                    "from": {"type": "string"},
                    "from_commit_id": {"type": "string"},
                    "created_at": {"type": "integer"},
                    "updated_at": {"type": "integer"},
                },
                "required": ["name", "head", "updated_at"],
            }},
        },
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
