# Analysis of the 2026-11-09 run

`summary.md` beside this file is generated from `records.jsonl` by
`run.py summarize`; this page reads it. Model `claude-sonnet-5-5`, Claude Code
2.1.316, cards `edition2.md` `0f791365b296` and `edition3.md` `52014d3aa9ba`,
instrument at commit `43ead3eb` on the edition-3 front end of PR 1316
(`81d69a1d`). Three runs per edition, 75 tasks each (the 45-task competence
instrument and the 30-task corpus), every condition: 631 agent calls, $5.62
for edition 2 and $2.71 for edition 3 at list price. After the run, each
record's typed IR was replaced by its SHA-256 with `run.py`'s `compact`, which
the runner now applies as it writes (`meta.json`'s runner digest is of the
runner before that change).

## The numbers, per edition, over three runs

| | Edition 2 | Edition 3 |
|---|---|---|
| First try accepted, 75 tasks | 68–69 | 74–75 |
| First try accepted, 45 shared tasks | 43 | 44–45 |
| First try correct, 45 shared tasks, the RFC's measures (edition 2 by rows, edition 3 by typed IR) | 39–40 | 23–25 |
| First try correct, 45 shared tasks, both by typed IR | 28 | 23–25 |
| First try correct, 45 shared tasks, run 1 read by hand (below) | 39 by rows | 39 equivalent, 2 alternative, 4 extra column, 0 wrong |
| Refused first answers per run | 6–7 | 0–1 |
| Repaired with the card | 6 of 6–7 | 1 of 1 |
| Repaired without the card | 1–2 of 6–7 | 0 of 1 |
| Seeded repairs accepted (diagnostic alone) | 16–17 of 18 | 23–24 of 29 |
| Seeded repairs correct | 16–17 of 18 | 21 of 29 |
| Answers of several queries on the 16 composition tasks | 13–14 | 1 (`m4`, a helper and its caller, as asked) |
| Mean input / output tokens, first try | 9,163 / 315–328 | 7,588 / 183–203 |

Shared tasks are the 45 that edition 2 expresses in one query (26 of the
competence tasks, 19 of the corpus), where both editions are judged without a
person.

## The gate

The RFC gate is edition 3's first-try acceptance and correctness at or above
edition 2's on the shared tasks, with the seeded-repair rate reported.

- **Acceptance: passes.** 44–45 of 45 against 43; over all 75 tasks 74–75
  against 68–69.
- **Correctness, as the measures are defined: fails.** 23–25 against 39–40.
  The two numbers do not measure the same thing. Edition 2 is judged by rows
  on the seed, so any query that returns the asked rows counts; edition 3 does
  not execute, so it is judged by typed-IR equality after renaming, which
  counts every other spelling of the same answer as a miss. Judged the same
  way, by typed IR, edition 2 scores 28 and edition 3 23–25: still lower, by
  three to five tasks.
- **Read by hand, the gap closes.** The 22 edition-3 misses of run 1 on the
  shared tasks: 16 are the asked answer in another spelling, 4 add a column
  the task did not ask for, 2 keep an empty group the grouped truth drops (on
  this seed the rows agree), and none is wrong. That puts edition 3 at 39 of
  45 by hand against edition 2's 39 by rows. Edition 2's 5–6 misses per run
  are `t02` (an extra column), `t13` (refused: the bare `in` edge), `t37`
  (written to the branch, not an unattached snapshot), `t38` (no query) and
  one or two answers of several queries to a one-query task (`a02`, `a07`,
  `t27`), left unjudged. One reader, who wrote the ground truths, not
  blinded.

Verdict: acceptance passes; correctness fails as measured and ties when read
by hand. The gate as written cannot be decided by a machine until edition 3
executes (then both editions are judged by rows) or the normalisation absorbs
the spellings below.

### What the strict comparison counted as a miss (run 1, shared tasks)

| Kind of difference | Tasks |
|---|---|
| An `order` the task did not ask for | `a07`, `a10`, `r03`, `r08`, `s08` |
| `let $n = count { … }` and a filter, for the count block in the filter | `t18`, `s08` |
| A block count per row where the truth groups (keeps empty groups; same rows here) | `a02`, `s03` |
| `return distinct` where the rows are already distinct | `t11`, `a03`, `r09` |
| Clause order of two binders (`$d "in" $x` before `$x: Collection`) | `t13` |
| A redundant clause (`$p.city is not null` under `count(distinct …)`, a second triple over the bound edge) | `t16`, `t34` |
| Ordering by the return alias instead of the drawn column | `t25` |
| `bm25` inside a wrapper read with its own `order` and `limit` | `t27`, `r01` |
| An unanchored triple where the truth binds the type (`$p likesComment $c`) | `s07` |
| A column the task did not ask for (`$p.age`, `$distance`, `$score`) | `t02`, `t24`, `t29`, `r05` |

## Where edition 3 gained and lost, by task

Gained, in edition 3's favour regardless of the measure:

- **Composition.** On the 16 composition tasks edition 2 answered with two
  queries and prose glue 13–14 times per run; edition 3 answered every one in
  one query, apart from `m4`, which asks for a helper and its caller.
- **Forms edition 2 lacks.** Arithmetic (`t08`), the conditional rows
  (`t09`), `union` (`t21`), unbounded reachability (`t12`), a write with a
  computed value (`t35`), the catalogue (`t38`), the per-group threshold
  (`m2`): edition 2 refused or could not express them; edition 3 answered
  them, correct by typed IR in most runs.
- **The reserved edge.** Edition 2's agent wrote `$d in $x` bare in `t13`,
  `m2` and `m6` and was refused with `T7` every run; edition 3's agent quoted
  it every run.
- **`t37`** (an unattached snapshot): edition 2 wrote to the branch in all
  three runs; edition 3 was right in all three.

Lost, under the strict typed-IR measure only (each listed above): `a03`,
`a07`, `a10`, `r01`, `r03`, `r05`, `r08`, `r09`, `s03`, `s07`, `s08`, `t11`,
`t16`, `t18`, `t24`, `t25`, `t27`, `t29`, `t34`. No task was lost to a refusal
or to a wrong answer in run 1's hand reading; `t34` was refused once, in run 3 (`Q015`), and repaired with the card.

## The codes behind every repair

- **Edition 2, first answers refused (19 over three runs):** `T7` 8 times,
  every one the bare `in` edge (`t13`, `m2`, `m6`); `Q001` 4 (arithmetic in
  `t08`, edition 2's `where` on a statement in `t35`); `T1` 4 (an interface
  as a node type in `t07` and `t21`); and three prose-only answers to `t38`
  with no code. With the card
  every refusal but one was repaired; without it 5 of 19, mostly by
  re-spelling into another refusal (`Q001`, `T1`, `T6`).
- **Edition 3, first answers refused:** one, `t34` in run 3 (`Q015`),
  repaired with the card and not without it.
- **Edition 3 seeds the diagnostic alone did not repair:**
  - `T65`, a ranking in `order` or `return` (0 of 6): the fix prints a
    template, `from nearest(<population>, $x.field, $q, k: <limit>)  and
    order { $distance }`; without the card the agent wrote `from` after the
    `match` block, with the node variable as the population.
  - `T8`, `avg(count($p))` (0 of 3): no fix is printed.
  - SQL's `union` between two reads (0 of 3): the generic `Q001`, which names
    neither the construct nor the form.
  - `count(*)` (0 of 3): `Q014` prints `count($p)`, which answers another
    question than the task's `count(distinct $p.city)`; the seed asks more
    than any diagnostic can give.
  - `having` and `'…'`/`==` (3 of 3 accepted, 0 of 3 correct): the repairs
    parse but answer with another shape.
  - `T68` (1 of 3 accepted, 0 correct by typed IR; the accepted repair renames
    the populations and is equivalent).
- **Edition 2 seeds:** 16–17 of 18 repaired; the bare `in` edge (`T7`) was
  not repaired from the diagnostic alone in any run.
