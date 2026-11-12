# Onboarding: Set Up Omnigraph for a New User

Follow this page when a user asks you to set up Omnigraph for them, for example
after pasting the onboarding prompt from omnigraph.dev. It takes them from
nothing installed to a working local graph they can query, then points to the
next step that fits what they want to build.

Work through the steps in order. Ask the user only where a step says so; take
the default otherwise and tell them what you chose. Run each command yourself
and check its output before moving on.

## 1. Install the CLI

Check first whether it is already installed:

```bash
omnigraph version
```

If the command is missing, install it. On macOS and Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

or with Homebrew, if the user prefers it:

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

The script installs into `~/.local/bin`. Your shell does not reread the
user's startup files, so if `omnigraph` is still not found, put the directory
on `PATH` for this session and tell the user to add the same line to their
shell profile:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Prebuilt Linux archives need glibc 2.39 or newer (Ubuntu 24.04, Debian 13).
Windows has a PowerShell installer; older Linux and Intel macOS build from
source. Both are in the [install guide](../../../docs/user/install.md).

Run `omnigraph version` again. It prints the CLI version, the storage format
it writes (`internal-schema`) and the HTTP contract it speaks.

## 2. Load the skill that matches the CLI

The binary carries the copy of this skill written for its own commands. Export
it where you load skills, so later sessions use the version that matches the
installed CLI:

```bash
omnigraph skill export ~/.claude/skills/omnigraph --force --json
```

Use your own skills directory if you are not Claude Code. If the exported
`SKILL.md` differs from the copy you are reading, follow the exported one from
here on (see [version-matched skill](commands.md#version-matched-skill)).

## 3. Ask what the graph is for

Ask the user one question: what should this graph hold, and who or what will
read and write it? Typical answers:

| The user wants | Start from |
|---|---|
| A shared memory for a team and its agents: decisions, claims, action items with their sources | the `company-brain` cookbook |
| A personal knowledge base: projects, people, notes | the `second-brain` cookbook |
| A work queue for coding agents | the `beads` cookbook |
| Issues, decisions, specs and releases of a software project | the `dev-graph` cookbook |
| Market or competitive research | the `industry-intel` or `pharma-intel` cookbook |
| Deals and portfolio tracking | the `vc-os` cookbook |
| Requirements, budgets and verification of a hardware program | the `hardware-program` cookbook |
| Something else | a schema of their own (step 4) |

The [cookbooks](https://github.com/ModernRelay/omnigraph-cookbooks) are full
cluster deployments with seed data, stored queries and policies. If one fits,
tell the user, and offer either to follow its README or to start with the small
local graph below and grow it. Start small unless they ask for the full
deployment.

The cookbooks pin a CLI and server release, named at the top of their README.
Follow a cookbook only when `omnigraph version` reports that release, or after
installing it ([release channels](../../../docs/user/install.md#release-channels) shows how to
pin a version). Otherwise borrow its schema and queries for the local graph
below.

## 4. Design a first schema

In a new project directory, write two files.

First the questions, as `.gq` queries in `queries.gq`: three to five reads the
user will actually run. Then the schema in `schema.pg` with the node and edge
types those queries need, each node with a stable `@key`. Keep it small: a
first schema has a handful of types, and adding types and nullable properties
later is cheap. Follow the [design flow](schema-design.md#design-flow) and the
syntax in [schema](schema.md).

A minimal example:

```pg
node Source {
  slug: String @key
  title: String
}

node Claim {
  slug: String @key
  statement: String
}

edge Supports: Source -> Claim
```

```gq
query sources_for_claim($claim: String) {
  match { $source: Source, $c: Claim, $source supports $c }
  where $c.slug = $claim
  return { $source.title as source }
  order { source asc }
}
```

Comments are `//`, never `#`. Lint both together until clean:

```bash
omnigraph lint --schema schema.pg --query queries.gq
```

Show the user the schema and queries and adjust them before going on.

## 5. Create the graph and load data

Create a local graph next to the schema:

```bash
omnigraph init --schema schema.pg graph.omni
```

Write a few real records as JSONL in `seed.jsonl`, taken from what the user
told you or from files they point you to. Nodes and edges can share a file:

```jsonl
{"type":"Claim","data":{"slug":"lower-latency","statement":"The migration reduced request latency."}}
{"type":"Source","data":{"slug":"load-test","title":"Load test report"}}
{"edge":"Supports","from":"load-test","to":"lower-latency"}
```

```bash
omnigraph load --data seed.jsonl --mode merge --target ./graph.omni
```

`--mode` is required; `merge` upserts by key. See [data changes](data.md) for
the other modes and for `mutate`.

## 6. Query it

```bash
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' --format table --target ./graph.omni
```

Run each query from step 4 and show the user the results.

## 7. Show a branch

Branches are how agents change the graph safely. Load new data on a branch,
read it there, then merge. `branch` commands take the branch name as their
positional argument, so they address the graph with `--target`:

```bash
omnigraph load --data more.jsonl --from main --branch review/first-change \
  --mode merge --target ./graph.omni
omnigraph branch diff review/first-change --into main --target ./graph.omni
omnigraph branch merge review/first-change --into main --target ./graph.omni
```

Check each command's `--help` if the installed CLI differs. See
[data changes](data.md) for the review workflow.

## 8. Leave the project ready for the next session

- Write a short `CLAUDE.md` (or `AGENTS.md`) in the project: where the schema,
  queries and graph live, the `--target`, and the rules from this skill that
  matter for it.
- Add `.env.omni`, `.claude/` and `*.omni/` to `.gitignore`; commit
  `schema.pg`, `queries.gq`, `seed.jsonl` and the agent file (see
  [what to commit](../SKILL.md#what-to-commit)).

## 9. Next steps

Tell the user what they have, then offer the step that fits:

- **More data:** bulk loads and imports in [data changes](data.md); CSV through
  `load csv` statements.
- **Search:** embeddings and vector, keyword and hybrid ranking in
  [search](search.md).
- **A shared graph for a team or for agents over MCP:** a cluster served by
  `omnigraph-server`, on local disk or S3, in [cluster](cluster.md) and
  [server and policy](server-policy.md). `omnigraph mcp` connects an agent to a
  served or managed target, not to a local file.
- **Omnigraph Cloud, managed for them:** `omnigraph login --create-profile NAME`
  signs in to `https://platform.omnigraph.dev`; see
  [operator config and credentials](commands.md#operator-config--credentials)
  and the [operator reference](../../../docs/user/cli/targets.md).

Point them to the [quickstart](../../../docs/user/quickstart.md) and the
[Slack community](https://join.slack.com/t/omnigraphworkspace/shared_invite/zt-3wfpglyxj-lHvJGhuySPfqLtN35uJZNw)
for questions.
