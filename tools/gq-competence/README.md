# GQ in-context competence instrument

Measures how well a model that was never trained on GQ writes it from three
inputs it would have in production: the graph's schema, the one-page
[card](card.md), and its own errors. It is the instrument the composition RFC
names as the tie-break for spellings and the gate for new stages
(`docs/rfcs/2026-09-18-gq-composition-and-language-evolution.md`, "In-context
competence"), and Phase B of RFC 0048's rollout.

Ground truth is computed by exact queries at a pinned graph commit, so the
graph verifies itself; no human labels are needed for correctness.

## Metrics

Per task and model: `success` (the submitted answer equals the truth),
`first_try_valid` (the first query ran without error), `turns_to_correct`
(index of the first query whose rows equal the truth), queries and errors per
task with the diagnostic codes behind each repair, tokens (with cache reads),
and wall time. Rows compare by value, so column aliases and order do not
count against a model.

## Running

```bash
# 1. pin the commit every query runs against
omnigraph query -e 'query q() { match { $p: Person } return { count($p) as n } }' \
  --server personal --graph personal --json | jq -r .graph_commit_id

# 2. compute the truth (no model call)
uv run tools/gq-competence/run.py truth --tasks tasks.yaml --truth truth.json --snapshot <commit>

# 3. run a model (needs ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN, or `ant auth login`)
uv run tools/gq-competence/run.py run --tasks tasks.yaml --truth truth.json --snapshot <commit> \
  --model claude-opus-5 --effort high --repeats 3

# 4. summarize any results file
uv run tools/gq-competence/run.py report tools/gq-competence/results/<file>.jsonl
```

`run` refuses a truth file computed at a different commit. Every query the
model runs is pinned with `--snapshot`, and a result whose `graph_commit_id`
differs is reported as drift, so a concurrent write cannot change an answer
mid-run.

## Task files

A task file is YAML with one entry per task. Keep task files for real graphs
outside the repository when they name real people; `tasks.example.yaml` shows
the shape against the quickstart schema.

```yaml
tasks:
  - id: engineers_at_acme         # stable id, used in results
    kind: set                     # scalar | set (order-free rows) | list (ordered rows)
    question: Which people work at the organization with slug acme? Return their slugs.
    truth_query: |
      query t($s: String) { match { $p: Person  $p worksAt $o  $o: Organization { slug: $s } } return { $p.slug } }
    truth_params: { s: acme }
```

Write questions the way a caller would ask them and let the truth query be the
exact answer; the point is to measure the distance between the two.

## Comparing spellings

To decide between two spellings (for example `metric(a, f)` and `a.f`), give
each its own card, run the same tasks with the same model, effort, schema and
snapshot, and compare `first_try_validity` and `turns_to_correct`. Hold
everything else fixed within a comparison; a model revision reopens it. Keep
failed trajectories: the `queries` array in each result record is the
evidence.

## What it does not measure

Retrieval quality (relevance), resource behavior, or anything the schema does
not expose. A green run says the model can express the question in the
language given the card; it says nothing about the answer's usefulness.
