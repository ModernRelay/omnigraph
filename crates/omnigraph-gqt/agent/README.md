# The GQ agent instrument

**Audience:** anyone changing GQ's surface, its diagnostics or the agent skill,
and anyone deciding the GQ 3 gate
**Authority:** what this directory measures and how to rerun it; the gate it
serves is stage M0 of the GQ 3 RFC (`docs/rfcs/2026-11-07-gq-3.md`,
section 11 and `blocked_on` item 2)

An agent is GQ's main author, and what it costs is round trips. This
instrument asks a headless agent, given one reference card per edition, to
write the query for each of 75 tasks, and judges every answer through the
compiler's own front door. It is rerunnable and pinned: the agent model and
both cards are fixed by `manifest.json`, the tasks and their ground truths are
committed, and the judge is the compiler and the engine at the commit being
measured. Its numbers are evidence for a language decision, not a CI gate.

## What is here

| Path | What it holds |
|---|---|
| `tasks/` | the 45-task competence instrument: 39 kinds of query from the RFC's before-and-after corpus and the six M0 composition tasks (`m1` to `m6`), over the RFC's section 8 schema |
| `corpus/` | the 30-task agent corpus: realistic requests over three GQT datasets, `s*` (social), `a*` (attribution) and `r*` (reading); 10 of them need several edition-2 queries |
| `datasets/` | each dataset's `schema.pg` and `seed.jsonl` (GQT's seed format) |
| `seeds/` | seeded answers: a ground truth with one injected error per refusal class, per edition |
| `cards/` | the reference card each edition's agent reads |
| `manifest.json` | the pinned agent model and each card's SHA-256 |
| `run.py` | the runner: calls the agent, asks `omnigraph-gqt-agent` to judge, writes results |
| `results/` | one dated directory per run of the runner, when committed |

The judge is the `omnigraph-gqt-agent` binary of this crate
(`src/bin/omnigraph-gqt-agent.rs`, `src/agent/`). The runner never parses a
task or a query itself.

### Datasets

`section8` is the schema of the RFC's section 8 in edition-2 spelling, with a
seed written for the 45 tasks: null cities and ages, a company with no
employees, a `knows` cycle, an uncited author, documents without a vector and
distances with no ties. The other three are copies of GQT fixtures, kept as
copies so that the instrument's data stays fixed while the GQT corpus is
rewritten at the edition break:

| Dataset | Copied from `crates/omnigraph-gqt/cases/` | What it exercises |
|---|---|---|
| `social` | `issue_954_count_distinct.gqt` | aggregation, nulls, distinct counts, writes over likes |
| `attribution` | `v2/planner/anti_join_key_set_from_a_bounded_binding.gqt` | traversal, sums, anti-joins, an edge property, writes fed by reads |
| `reading` | `v2/planner/issue_789_nearest_and_rrf_rank_a_reached_binding.gqt` | BM25 and vector search, fusion, citations, writes fed by reads |

Both editions' tasks run against the same schema text. The section 4 schema
spellings (`@index(fulltext, analyzer = …)`, `Vector(n, distance = …)`) are a
separate stage B feature and are not measured here.

### A task file

A `.task` file is sections in the shape of a `.gqt` case:

| Section | Holds |
|---|---|
| `--- task` | `id` (the file's name), `dataset`, `kind`; `answer: one` or `fewest` (the reply the prompt asks for); `composition` (why the task composes, required when edition 2 needs several queries); `combine: concat` when edition 2's reads together form the answer |
| `--- intent` | the request in one or two sentences, parameters named in prose as `$name` |
| `--- params` | the parameter values the ground truths and the answers run with |
| `--- edition2` | one query of the edition-2 ground truth; several sections are several queries, run in order. `bind <param>=<step>:<column>` passes every value of an earlier step's column as a list parameter, and `each <param>=<step>:<column>` runs the step once per value: the client glue, stated |
| `--- edition3` | an edition-3 ground truth, without the edition line; the first is canonical and the others are spellings the RFC makes equally canonical |
| `--- check` | for a write: the read whose rows, after the write, judge it |
| `--- expect ordered\|unordered` | the rows the edition-2 ground truth returns on the seed, recorded by `omnigraph-gqt-agent record --write`; `live` instead of rows for rows the engine owns (its catalogue) |
| `--- note` | why the task is written the way it is |

In the corpus, a task without `--- edition2` is one edition 2 cannot
express even with client glue (`a06`, an aggregate of an aggregate). In the
45 tasks, as in the RFC's comparison, a task without it is one edition 2
cannot express in one query (26 of the 45 have one); their notes say where
several queries would do.

## What each number means

`run.py` writes `records.jsonl` (one record per agent call, with the prompt's
digest, the raw reply, the extracted answer and the judge's verdict, each
declaration's typed IR by its SHA-256),
`meta.json` (model, card digests, `claude` version, git commit), and
`summary.md` and `summary.json`, per edition, run and set:

| Number | Meaning |
|---|---|
| First try accepted | the first answer passes the edition's front door: the parser, then the type checker and lowering of every declaration, as the engine compiles a source |
| First try correct | of the judged first answers, the correct ones. Edition 2: an answer of one declaration whose rows on the seed equal the ground truth's, by position (column names may differ), in order when the task orders; for a write, the check's rows after it. Edition 3: typed IR equal to a ground truth's, after renaming every variable, alias and declaration name by order of first appearance, since nothing in edition 3 executes yet |
| Correct, shared tasks | the same, over the tasks edition 2 expresses in one query, where both editions are judged automatically: the comparison the RFC's gate makes |
| Unjudged | accepted answers that are not judged automatically: an edition-2 answer of several declarations (its glue is prose) or a task edition 2 cannot express. Read them in `records.jsonl` |
| Several queries | accepted answers of more than one declaration |
| Repair with card accepted | of the refused first answers, those accepted after one repair turn that carries the card, the schema, the task, the answer and the diagnostic |
| Repair without card accepted | the same with the task, the answer and the diagnostic only: the diagnostics measured alone |
| Correct after repair | correctness of the first answer, or of its repair with the card when it was refused |
| Seeded repairs | of the seeded answers (`seeds/`), those accepted, and those correct, after one repair turn from the diagnostic alone |
| Tokens | mean input and output tokens of the first call |

The RFC's gate reads: edition 3's first-try acceptance and its correctness on
the shared tasks at or above edition 2's, with the seeded-repair rate
reported, on the pinned model and cards.

The diagnostic an agent sees in a repair turn is the compiler's serialized
four-field diagnostic, as `omnigraph lint --json` and an HTTP refusal carry
it.

## Running it

```bash
cargo build --locked -p omnigraph-gqt --bin omnigraph-gqt-agent
python3 crates/omnigraph-gqt/agent/run.py run --editions 2,3 --runs 3
python3 crates/omnigraph-gqt/agent/run.py run --editions 2 --runs 1 --only t01,s04 --label smoke
python3 crates/omnigraph-gqt/agent/run.py summarize crates/omnigraph-gqt/agent/results/<dir>
```

The runner needs a logged-in `claude` (Claude Code) on `PATH`. Every call is a
fresh `claude -p` in an empty directory with `--tools ""`, `--safe-mode` and
`--no-session-persistence`, so the card is the only GQ reference the agent
sees and no session carries over. The model is `manifest.json`'s; a reply
that reports any other model stops the run. Before calling the agent, the
runner checks both cards against their pins. Each run of an edition makes
one agent call per task, two more for each refused first answer and one per
seed; `--resume` continues an interrupted results directory.

The judge alone:

```bash
B=target/debug/omnigraph-gqt-agent
$B tasks                                  # every task as JSON
echo '{"id":1,"task":"t14","text":"query staff($slug: String) { … }"}' | $B judge --edition 2
$B record --check                         # the recorded rows still hold
$B record --write                         # re-record them after an intended change
$B cards --check                          # the cards match their pins and the skill
$B cards --write                          # regenerate the edition-2 card and both pins
```

## How it stays valid

The `agent::tests` module of this crate's library, run by the ordinary GQT
job, checks that every edition-2 ground truth is accepted and returns its
recorded rows on its seed, every edition-2 seed is refused with its code,
both cards match their pins, the edition-2 card regenerates byte for byte
from `skills/omnigraph`, and the judge compares rows and typed IR as stated
above. A change to the skill's query sections therefore fails that test until
`cards --write` regenerates the card, which also moves its pin: results
measured with the older card keep the older digest in their `meta.json`.

The same module checks that every edition-3 ground truth, alternatives
included, is accepted by the edition-3 front end (the parser and the type
checker), and every edition-3 seed refused with its code.

## Writing a task

Keep the intent in the words an agent's user would write, name every
parameter and every output column the answer must produce, and choose
parameters whose rows separate the right answer from the near misses (a
window cut before or after a traversal, a group that is empty, a tie). Add
the edition-2 ground truth with the fewest queries edition 2 needs, stating
the glue with `bind` or `each`, run `record --write`, and read the recorded
rows against the intent before committing them.
