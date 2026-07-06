# RFC 0002: Graph visualization export (`export --format graph-json` + `omnigraph viz`)

| | |
|---|---|
| **Status** | Proposed |
| **Author(s)** | Shreya Sharma (@shreya-sharma_data) |
| **Discussion** | None yet — opened directly on the public RFC track (open to external contributors per [README](README.md)) |
| **Implementation** | TBD |

## Summary

Give operators a first-class way to *see* a graph. Add a stable
`export --format graph-json` that emits a documented `{ nodes, edges, schema }`
interchange shape, and a convenience `omnigraph viz` that renders it as a single
**self-contained, dependency-free interactive HTML file** (force-directed
node-link diagram; filter by type, hover to highlight a node's neighborhood,
click for its properties and every relationship in/out). Both are read-only,
honor existing branch/snapshot addressing and Cedar `read` policy, and add no
on-disk or wire-format surface.

## Motivation

Today there is no way to look at a graph. `export` streams row-oriented JSONL
(one record per node/edge), which is perfect for reload/bridging but useless for
comprehension — you cannot answer "what does this graph *look like*, and how is
this node connected?" without hand-writing a converter and wiring up an external
tool every time. Every operator building a company-brain / agent-memory graph
hits this on day one.

Concretely, the current gap forces a multi-step ritual for something that should
be one command:

1. `omnigraph export` → JSONL (and see the sharp edge below).
2. Hand-write a script to reshape JSONL into some tool's node/edge model.
3. Import into Cytoscape/Gephi/a bespoke D3 page, re-doing type coloring and
   layout each time.

This RFC argues the long-run liability of *not* having this: visualization is a
recurring, universal need for a knowledge-graph product, and leaving it to
ad-hoc per-user scripts means everyone re-solves it, inconsistently, and the
graph model's own semantics (typed nodes, directed edges, branches) never show
up in the picture.

**A sharp edge this also fixes.** `omnigraph export` currently fails on a graph
created by `init` + `load` with:

```
storage read failed for '.../graph.omni/_schema.pg':
  Object ... /_schema.pg not found (os error 2)
```

`export` reads schema from a `_schema.pg` sidecar that `init` does not write, so
export is unusable out of the box unless the operator manually drops the schema
file next to the datasets. The schema already lives in the catalog/`__manifest`;
the read path proposed here reconstructs types from there, removing the sidecar
dependency (see Reference-level design). This is the originating pain point.

**Prototype (proof of concept).** A working prototype exists: a self-contained
HTML renderer fed by `export` output, exercised on a 27-node / 46-edge
"company-brain" graph (People, Teams, Projects, Technologies, Documents,
Decisions). It renders fully offline (no CDN — a hard requirement, see below),
runs a force layout in vanilla JS/SVG, colors nodes by type and sizes them by
degree, highlights a node's neighborhood on hover, and shows a node's full
in/out relationship list on click. It confirms the approach is viable in a
single ~25 KB file with zero runtime dependencies. This RFC proposes folding
that capability into the CLI as a supported, tested surface.

## Guide-level explanation

Two new read-only surfaces on the existing addressing model (`--store` / a
positional URI / `--server --graph` / `--branch` / `--snapshot`):

**1. A stable graph-shaped export.**

```bash
# emit a { nodes, edges, schema } document for any branch or snapshot
omnigraph export --store ./graph.omni --format graph-json > graph.json
omnigraph export --server intel-dev --graph spike --branch review --format graph-json
```

`graph-json` is a documented interchange shape that external tools (Cytoscape,
Gephi via a thin adapter, D3/Sigma, or the bundled renderer) can consume without
re-deriving node/edge structure. Shape:

```json
{
  "graph":  { "branch": "main", "snapshot": "01K…", "node_count": 27, "edge_count": 46 },
  "schema": { "node_types": ["Person", "Team", …], "edge_types": ["MemberOf", …] },
  "nodes":  [ { "id": "carol@nw.io", "type": "Person", "props": { "name": "Carol Diaz", … } } ],
  "edges":  [ { "type": "MemberOf", "source": "carol@nw.io", "target": "platform", "props": {} } ]
}
```

**2. A one-command interactive picture.**

```bash
omnigraph viz --store ./graph.omni --out graph.html      # writes a self-contained HTML file
omnigraph viz --server intel-dev --graph spike --branch agent/ingest-42 --out review.html
```

`viz` reads the same `graph-json` internally and writes one self-contained HTML
file (no external requests — CDN-free, works on an air-gapped host, matching
the project's on-prem posture). Opening it: nodes colored by type with a legend
(click a type to filter it in/out), hover to highlight a node and its immediate
neighbors, click to pin a details panel listing the node's properties and every
edge in and out (`→ Uses Rust`, `← WorksOn Carol Diaz`), plus drag / zoom / pan.

Because it addresses through the normal read path, you can visualize **any
branch** or a **time-travel snapshot** — e.g. render an agent's isolated branch
before merge, or diff two branches by eye.

## Reference-level design

- **Crate / placement:** `omnigraph-cli`. `graph-json` is a new `--format` on the
  existing `export` command (alongside `jsonl`); `viz` is a new sibling command
  in the same command family. No engine (`omnigraph`) API change is required —
  both consume the existing snapshot read + catalog surface the current `export`
  already uses.
- **Read path:** open a read-only `Snapshot` at the resolved branch/snapshot
  (existing behavior). Enumerate node/edge types from the **catalog derived from
  `__manifest`** (not a `_schema.pg` sidecar), scan each type's dataset, and
  project rows to `{id, type, props}` / `{type, source, target, props}`. This
  removes the current sidecar dependency that breaks `export` on freshly
  `init`'d graphs. Everything runs under one snapshot, so the picture is a
  consistent point-in-time view (no torn reads).
- **`graph-json` shape:** as above; versioned implicitly by a top-level
  `"format": "omnigraph.graph-json/v1"` field so the interchange contract can
  evolve without breaking consumers. Blob columns are emitted as a size/omitted
  marker rather than inline bytes (a picture doesn't need the payload).
- **`viz` rendering:** a template embedded in the binary (`include_str!`) with
  the `graph-json` injected at a single placeholder; force-directed layout,
  interaction, and styling are vanilla JS/SVG with **no external dependencies**
  and **no network requests** (strict-CSP / offline friendly). Output is one
  `.html` file. Large graphs: `viz` warns and suggests `--format graph-json`
  into a dedicated tool above a node/edge threshold (rendering budget, not a
  hard cap).
- **Policy:** on a served graph, `viz`/`graph-json` are the `read` action and go
  through the same Cedar gate as any read; no new action is introduced. On a
  direct `--store` graph they behave like `export` does today.
- **Errors:** unreadable/again-quarantined graph surfaces the existing typed
  read errors; `--out` path collisions follow the same overwrite/`--force`
  convention as other file-writing commands.

## Invariants & deny-list check

No Hard Invariant in [../dev/invariants.md](../dev/invariants.md) is weakened.

- **Read-only:** no mutation, no HEAD advance, no manifest write; a single
  snapshot is held for the read (snapshot-isolation preserved).
- **No format/substrate change:** no on-disk or wire format changes; no new
  substrate dependency. `graph-json` is an *output* interchange shape, not a
  storage format.
- **Policy is engine-wide:** the read goes through the same Cedar `read` gate;
  no new action or bypass is added.
- **Deny-list:** touches none of it — this is additive, read-only CLI surface.

## Drawbacks & alternatives

- **Do nothing / external tools only.** Operators keep hand-rolling converters
  per tool. Rejected: it's a universal day-one need and the graph's own
  semantics (types, direction, branches) never make it into the view. A stable
  `graph-json` at minimum removes the reshape tax even if `viz` is dropped.
- **Ship a web UI in `omnigraph-server`.** A live server-rendered graph browser.
  Rejected for now: much larger surface, conflicts with the cluster-only /
  policy-scoped serving model, and pulls a front-end stack into a Rust-only
  substrate. A static file from the CLI is far cheaper and covers the core need;
  a server UI could be a later RFC that builds on `graph-json`.
- **Embed a JS graph library** (Cytoscape.js/Sigma) in the template. Rejected as
  the default: it either needs a CDN (violates offline/on-prem) or vendoring a
  large bundle. A compact vanilla renderer keeps the file small and
  dependency-free; the `graph-json` output remains the escape hatch for anyone
  who wants a heavyweight tool.
- **`graph-json` scope.** Keeping it a stable, documented shape is a small
  ongoing compatibility commitment; the `format` version field bounds that cost.

## Reversibility

High. `viz` (HTML output) is a pure convenience and trivially removable. The one
semi-durable surface is the `graph-json` interchange shape — versioned via its
`format` field so it can evolve or be deprecated without breaking existing
consumers. No on-disk, wire, or substrate commitment, so evidence demand is low
(a CLI feature, not a format decision). The `_schema.pg`→catalog read change is
strictly a bug fix that makes `export` work as documented.

## Unresolved questions

- Should `graph-json` be a new `--format` on `export`, or its own `graph export`
  subcommand? (This RFC proposes the former to minimize surface.)
- The node/edge threshold at which `viz` should refuse/warn and defer to an
  external tool — pick a default from real large-graph rendering behavior.
- Should the sidecar-removal (`_schema.pg` → catalog read) ship independently as
  a bug fix ahead of this RFC, since `export` is broken out of the box today?
- Optional follow-ups (out of scope here): edge-label rendering density controls,
  a two-branch visual diff mode, and a GEXF/Cytoscape adapter alongside
  `graph-json`.
