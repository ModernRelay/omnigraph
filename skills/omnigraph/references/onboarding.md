# Onboarding: Set Up Omnigraph for a New User

Follow this page when a user asks you to set up Omnigraph for them, for example
after pasting the onboarding prompt from omnigraph.dev. It takes them from
nothing installed to a working local graph they can query, then points to the
next step that fits what they want to build.

Work through the steps in order. Ask the user only where a step says so; take
the default otherwise and tell them what you chose. Run each command yourself
and check its output before moving on.

## In short

<!-- omnigraph.dev's /llms.txt prints this section as its setup steps. Keep it
in step with the steps below, and keep the heading: the site's build fails
without it. -->

1. Install the CLI and check that `omnigraph version` runs (step 1).
2. Export the skill that matches the installed CLI with
   `omnigraph skill export ~/.claude/skills/omnigraph --force --json` (use
   your agent's skills directory if it is not Claude Code), then read its
   `SKILL.md`. `omnigraph skill show` prints it without installing anything
   (step 2).
3. Find the use case. If the user has none in mind yet, show them what people
   build: each use case has a cookbook you can load into a local graph for them
   to try (step 3).
4. Write the user's questions as `.gq` queries first, then the schema they
   need, and lint both before you create the graph, load it and query it
   (steps 4 to 7).
5. Register the graph as a named target with `omnigraph target add`, so
   `omnigraph target list` shows it from any directory (step 8).
6. Ask to add a section on all the user's graphs to their global agent file
   (`~/.claude/CLAUDE.md` or `~/.codex/AGENTS.md`): every session runs
   `omnigraph target list` at least, reads a graph's schema before using it,
   pulls context from the graphs, and offers to record what should outlast the
   session on a branch the user approves before it merges into main (step 8).
7. Make the graph the canonical store for its use case in the project's
   `AGENTS.md`, with the same rules and the target's name (step 8).

Setup is complete only when `omnigraph version` runs in a new shell, the
exported skill is in a directory your agent loads skills from, `omnigraph lint`
passes, the queries run against the graph, `omnigraph target list` shows the
graph, the global agent file lists it (or the user declined), and the
project's `AGENTS.md` names the use case and the target and carries those
rules. If your agent must restart to load the skill, tell the user the exact
step to resume.

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
the seed. Copy the chosen one into a trial directory of its own, apart from the
project, setting `COOKBOOK` to the folder named in the table:

```bash
COOKBOOK=second-brain   # the cookbook the user picked
SRC=$(mktemp -d)
git clone --depth 1 https://github.com/ModernRelay/omnigraph-cookbooks "$SRC"
mkdir omnigraph-trial && cd omnigraph-trial
cp -R "$SRC/$COOKBOOK"/{schema.pg,queries,seed.jsonl,seed.md} .
```

Then lint, create and load it as in steps 4 and 5, with the cookbook's files in
place of new ones, and run two or three of its read queries that answer
questions like the one in the table. Afterwards ask whether to keep it as the
starting point, trimmed to what they need, or to start fresh in step 4 with
what they have learned. Either way, ask the 3a questions before step 4, and
build the user's graph in the project directory, not in `omnigraph-trial`.

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

In the project directory, write two files. If the user kept a cookbook in
step 3b, copy its `schema.pg`, `queries/` and `seed.jsonl` from
`omnigraph-trial` instead and trim them: drop the types their questions don't
need, and the seed records of those types. Step 5 then creates the project's
own graph from the trimmed schema; the trial graph keeps the full cookbook and
can be deleted.

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

Run each query from step 4 and show the user the results. A query that runs
but returns no rows is fine when the data holds no answer yet; say so, and
check with the user that it is expected.

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

An agent that does not know a graph exists answers from memory and keeps facts
in chat or notes, and the graph falls behind. Make every later session find the
graph, read it first and write back to it.

- **Name the graph**, so `omnigraph target list` shows it in every session and
  directory. Pick a short name for what it holds (`brain` below). Tell the user
  first: `target add` rewrites `~/.omnigraph/config.yaml` and drops its
  comments.

  ```bash
  echo "{\"kind\":\"direct\",\"scope\":\"graph\",\"uri\":\"file://$PWD/graph.omni\"}" \
    | omnigraph target add brain
  omnigraph target list
  ```

- **The user's global agent file.** Ask to add this section to
  `~/.claude/CLAUDE.md` (Claude Code) or `~/.codex/AGENTS.md` (Codex). It
  covers all their graphs, not only this one, and changes every session they
  run. If the section is already there, add the new graph to its list.

  ```markdown
  ## Omnigraph: my graphs are context and durable memory

  My Omnigraph graphs hold knowledge and state that outlast a session. They
  are the targets in `~/.omnigraph/config.yaml`:

  - `brain`: <what it holds>

  Rules:

  - At the start of every session, run `omnigraph target list` to see which
    graphs exist. Do at least this, even when the task looks unrelated.
  - Read a graph's schema before querying or writing it:
    `omnigraph schema show --target <name>`. A server target can hold several
    graphs; `omnigraph graphs list --target <name>` lists them for `--graph`.
  - Pull context first: when a request touches anything a graph holds, query
    it and answer from what it returns. If it returns nothing, say so.
  - Write back what should last: offer to record facts, decisions and state
    worth keeping beyond this session in the graph, not in chat, notes or
    memory files. Look the entity up by its key first, write on a branch, and
    merge into main only after I approve.
  - Load the omnigraph skill (`omnigraph skill show`) before changing a
    schema, bulk loading, or retrying a write whose outcome is unknown.
  ```

- **The project's agent file.** Write `AGENTS.md`, which most coding agents
  read, and for Claude Code a `CLAUDE.md` whose only line is `@AGENTS.md`.
  Fill in the use case, the user's questions from step 3, the cookbook it
  started from if any, and the target's name:

  ```markdown
  ## Omnigraph: the canonical store for <what it holds>

  <One sentence on what the graph is for.> It answers questions such as:
  <the user's questions>. Facts about <people, projects, decisions…> live in
  the graph at `--target brain`, not in chat, notes or this file.

  - Read the schema before any query or write:
    `omnigraph schema show --target brain`. Use only the types, properties
    and edges it declares. Queries are in `queries.gq`.
  - Pull context first: when a request touches anything the graph holds,
    query it and answer from what it returns. If it returns nothing, say so
    instead of guessing.
  - Suggest recording what is new: when the user mentions a fact the graph
    should hold, or you find one it lacks or contradicts, offer to add it.
    Look the entity up by its key before creating another.
  - Write on a branch, never straight to main: `omnigraph branch create
    agent/<topic> --target brain`, then `mutate` or `load` with
    `--branch agent/<topic>`. Show the user `omnigraph branch diff
    agent/<topic> --target brain`, and merge with `omnigraph branch merge
    agent/<topic> --into main --delete-branch --target brain` only after
    they approve.
  - Load the omnigraph skill (`omnigraph skill show`) before changing the
    schema, bulk loading, or retrying a write whose outcome is unknown.
  ```

- Add `.env.omni`, `.claude/` and `*.omni/` to `.gitignore`; commit
  `schema.pg`, `queries.gq`, `seed.jsonl` and the agent files (see
  [what to commit](../SKILL.md#what-to-commit)).

Do not tell the user setup is complete until each of these holds, and say
which one fails if one does:

- `omnigraph version` runs in a new shell, not only in this session's `PATH`.
- The exported skill is in a directory your agent loads skills from.
- `omnigraph lint --schema schema.pg --query <file>` passes for every query
  file (`queries.gq`, or each file in a cookbook's `queries/`).
- Each query from step 4 runs against the graph without error, and any empty
  result is one the user agreed is expected.
- `omnigraph target list` shows the graph under its name.
- The global agent file lists the graph and carries the rules above, or the
  user declined.
- The project's `AGENTS.md` names the use case and the target, and carries the
  canonical-store rules: read the schema, pull context first, suggest writes,
  write on a branch.

## 9. Next steps

Tell the user what they have, then offer the step that fits:

- **See it in Studio:** Omnigraph Studio, the desktop app, shows the schema,
  each type as a table, a node's links as a tree or canvas, and a branch's
  changes before the merge, and agents can steer it over MCP. See
  [the next section](#see-the-graph-in-studio).
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

### See the graph in Studio

Studio opens only a graph that `omnigraph-server` serves, not `./graph.omni`.
The user installs Studio from its
[releases](https://github.com/ModernRelay/omnigraph-studio/releases/latest);
step 1's installer already put `omnigraph-server` beside the CLI. Declare the
graph in a `cluster.yaml` next to the schema:

```yaml
version: 1
graphs:
  knowledge:
    schema: schema.pg
    queries: queries.gq
```

Create a served copy in `./served` and load the seed again, since
`cluster apply` creates the graph empty. Each write leaves the cluster locked:
release it with the `lock_id` from
`omnigraph cluster status --target ./served --json` before the next write and
before the server starts.

```bash
mkdir served
omnigraph cluster apply --target ./served --config . --json
omnigraph cluster force-unlock <LOCK_ID> --target ./served
omnigraph load --data seed.jsonl --mode merge --target ./served --graph knowledge
omnigraph cluster force-unlock <LOCK_ID> --target ./served
```

Run the server in a process that keeps running (a background job or a second
terminal), on loopback only, because `--unauthenticated` lets every request in:

```bash
omnigraph-server --cluster "file://$PWD/served" --bind 127.0.0.1:8080 --unauthenticated
```

Register it and open Studio. Tell the user first: `target add` rewrites
`~/.omnigraph/config.yaml` and drops its comments.

```bash
echo '{"kind":"server","endpoint":"http://127.0.0.1:8080","auth":{"provider":"anonymous"}}' \
  | omnigraph target add local
omnigraph studio --target local --graph knowledge
```

`--graph` is required: a server without a cluster policy refuses to list its
graphs. Without Studio installed, `omnigraph studio` prints the link and fails
with `studio_unavailable`. `./served` is a second graph, so writes through
`--target local` land there, not in `./graph.omni`; if the user keeps it, use
`local` instead of `brain` in both agent files and add `served/` to
`.gitignore`. See
[Open Studio](../../../docs/user/cli/studio.md) and [cluster](cluster.md).

### Show the user things in Studio

From 0.4.8, Studio serves MCP tools that open views in its window: a type's
table, a node's page, a canvas or tree from a node, the schema's map, a branch's
review or history, a commit, or a query tab that is not run. They never write,
run queries or return property values, so read and write with the CLI and use
Studio to show the user what you found. Agents in Studio's own panel have the
tools already.

An agent outside Studio needs **Settings › Agent › Let agents outside Studio
show things in Studio** on. It ships off in 0.4.8 and on in later releases, and
only the user changes it. Studio then offers a Claude Code command and a Codex
`config.toml` entry to copy. You can run the same command; it reads the token
from a file, so the token never appears on screen (macOS path):

```bash
D="$HOME/Library/Application Support/Omnigraph Studio/ui-tools"
claude mcp add --transport http omnigraph-studio "$(jq -r .url "$D/endpoint.json")" \
  --header "Authorization: Bearer $(cat "$D/token")"
```

Most agents load MCP servers when a session starts, so the user starts a new
session before you use the tools. In 0.4.8 the address and token change each
time Studio starts, so after a restart run `claude mcp remove omnigraph-studio`
and add it again; later releases keep both by default (port 24187, the token in
the system keychain).

Call `studio_windows` first: it gives each window's id, server, graph and
branch, and every other tool takes a window id. `graph_open_type` opens a table,
`graph_open_branch` a branch's review against main, and `tools/list` describes
the rest. If Studio is connected, add a rule to the agent file: before asking
the user to approve a merge, open the branch's review with `graph_open_branch`.

Point them to the [quickstart](../../../docs/user/quickstart.md) and the
[Slack community](https://join.slack.com/t/omnigraphworkspace/shared_invite/zt-3wfpglyxj-lHvJGhuySPfqLtN35uJZNw)
for questions.
