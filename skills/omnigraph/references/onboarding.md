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

Check that your agent can see the exported skill before going on. If it reads
skills only when it starts, tell the user the exact step to resume, for
example: restart the agent in this directory and say "continue the Omnigraph
onboarding from step 3".

## 3. Find the use case

Users arrive in one of two states: they already know what the graph is for, or
they want to see what it can do first. Do not make the second kind invent a use
case on the spot, and do not walk the first kind through a catalogue.

If the user's request or the project you are in already says what the graph is
for (for example "set up Omnigraph for our sales notes"), take that as the use
case and go to 3a. Otherwise ask one question: do they already have something
in mind for the graph, or would they like to see what people build with it?

### 3a. They have a use case

Ask what the graph should hold, who or what will read and write it, and two or
three questions they want it to answer. The questions become the queries of
step 4. If a cookbook in the table under 3b is close, say so: its schema is a
starting point to trim, not a template to adopt whole.

### 3b. They want to see what is possible

Show this table and ask them to pick one, or to describe their own once they
have seen the options (then continue with 3a):

| Use case | What it holds | A question it answers | Cookbook |
|---|---|---|---|
| Company brain | Teams, projects, what people said and decided, who owes what, with the exact quote behind each | Who owes what, and which words is each decision based on? | `company-brain` |
| Second brain | People, places, projects, notes, tasks, habits and media | What did Sarah recommend that I haven't read yet? | `second-brain` |
| Agent work queue | Issues for coding agents, what waits on what, who claimed it | What work is ready, and who has it? | `beads` |
| Software project | Issues, epics, decisions, specs, PRs and releases | What's ready to work on, and what's blocking everything else? | `dev-graph` |
| Market research | Signals and the patterns they form or contradict | Which signals contradict this pattern? | `industry-intel` |
| Competitive intelligence | Trials, filings and deals, weighed against your own assumptions | Which signals contradict my thesis? | `pharma-intel` |
| Venture capital | Deal flow, diligence, decisions, portfolio and network | Which questions are still open on this deal? | `vc-os` |
| Hardware program | Requirements, architecture, budgets, tests, risks and reviews | Does the mass budget still close? | `hardware-program` |

When they pick one, offer to load it so they can see a real graph before
designing their own. Every [cookbook](https://github.com/ModernRelay/omnigraph-cookbooks)
folder has `schema.pg`, `queries/` and `seed.jsonl`, with `seed.md` describing
the seed. Copy the chosen one into a new project directory:

```bash
mkdir my-graph && cd my-graph
git clone --depth 1 https://github.com/ModernRelay/omnigraph-cookbooks /tmp/omnigraph-cookbooks
cp -R /tmp/omnigraph-cookbooks/second-brain/{schema.pg,queries,seed.jsonl,seed.md} .
```

Then lint, create and load it as in steps 4 and 5, with the cookbook's files in
place of new ones, and run two or three of its read queries that answer
questions like the one in the table. Afterwards ask whether to keep it as the
starting point, trimmed to what they need, or to start fresh in step 4 with
what they have learned. Either way, ask the 3a questions before step 4.

### Cookbook versions

The cookbooks are full cluster deployments with seed data, stored queries and
policies. Start with the small local graph in this guide unless the user asks
for the full deployment. Each cookbook pins a CLI and server release, named at
the top of its README. Follow its README only when `omnigraph version` reports
that release, or after installing it
([release channels](../../../docs/user/install.md#release-channels) shows how to pin a
version). Its schema and queries can still be borrowed for a local graph; if
`lint` refuses them on the installed CLI, tell the user and fix them or start
from step 4.

Whichever path you took, note the use case and the user's questions: step 8
writes them into the project's agent file.

## 4. Design a first schema

In a new project directory, write two files. If the user kept a cookbook in
step 3b, edit its files instead: drop the types their questions don't need.

First the questions, as `.gq` queries in `queries.gq`: the user's questions
from step 3, three to five reads they will actually run. Then the schema in `schema.pg` with the node and edge
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
  match {
    $source: Source
    $c: Claim { slug: $claim }
    $source supports $c
  }
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

The graph is only useful if every later session uses it: an agent that does
not know about it answers from memory and keeps facts in chat or notes, and the
graph falls out of date. Write an agent file that makes it the canonical store
for what it holds.

- Write `AGENTS.md` in the project, which most coding agents read, and for
  Claude Code a `CLAUDE.md` whose only line is `@AGENTS.md`. Adapt this
  section, filling in the use case, the user's questions from step 3, the
  cookbook it started from if any, and the real paths and `--target`:

  ```markdown
  ## Omnigraph: the canonical store for <what it holds>

  <One sentence on what the graph is for.> It answers questions such as:
  <the user's questions>. Facts about <people, projects, decisions…> live in
  the graph at `--target ./graph.omni`, not in chat, notes or this file.

  - Read the schema before any query or write:
    `omnigraph schema show --target ./graph.omni`. Use only the types,
    properties and edges it declares. Queries are in `queries.gq`.
  - Pull context first: when a request touches anything the graph holds,
    query it and answer from what it returns. If it returns nothing, say so
    instead of guessing.
  - Suggest recording what is new: when the user mentions a fact the graph
    should hold, or you find one it lacks or contradicts, offer to add it.
    Look the entity up by its key before creating another.
  - Write on a branch, never straight to main: `omnigraph branch create
    agent/<topic> --target ./graph.omni`, then `mutate` or `load` with
    `--branch agent/<topic>`. Show the user `omnigraph branch diff
    agent/<topic> --target ./graph.omni`, and merge with `omnigraph branch
    merge agent/<topic> --into main --delete-branch --target ./graph.omni`
    only after they approve.
  - Load the omnigraph skill (`omnigraph skill show`) before changing the
    schema, bulk loading, or retrying a write whose outcome is unknown.
  ```

- If the graph should be the canonical store outside this project too, as a
  second brain or a company brain usually is, offer to add the same section to
  the user's global agent file (`~/.claude/CLAUDE.md` for Claude Code,
  `~/.codex/AGENTS.md` for Codex), with the graph's absolute path as the
  `--target`. Ask first: it changes every session they run.
- Add `.env.omni`, `.claude/` and `*.omni/` to `.gitignore`; commit
  `schema.pg`, `queries.gq`, `seed.jsonl` and the agent files (see
  [what to commit](../SKILL.md#what-to-commit)).

Do not tell the user setup is complete until each of these holds, and say
which one fails if one does:

- `omnigraph version` runs in a new shell, not only in this session's `PATH`.
- The exported skill is in a directory your agent loads skills from.
- `omnigraph lint --schema schema.pg --query <file>` passes for every query
  file (`queries.gq`, or each file in a cookbook's `queries/`).
- Each query from step 4 returns rows from the graph.
- `AGENTS.md` names the use case and the `--target`, and carries the
  canonical-store rules: read the schema, pull context first, suggest writes,
  write on a branch.

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
