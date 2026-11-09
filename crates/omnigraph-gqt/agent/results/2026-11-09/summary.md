# Agent instrument results: 2026-11-09

Model `claude-sonnet-5-5`, 2.1.316 (Claude Code), git `43ead3eb2d754e5d6df31aa786bfe629bb76b594`, started 2026-11-09T07:01:28+00:00.
Card digests: `edition2.md` `0f791365b296`, `edition3.md` `52014d3aa9ba`.
README.md beside the runner defines every column. Not a CI gate.

| Edition | Run | Set | First try accepted | First try correct | Correct, shared tasks | Unjudged | Several queries | Repair with card accepted | Repair without card accepted | Correct after repair | Tokens in | Tokens out |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2 | 1 | tasks | 38/45 | 22/24 | 22/24 | 14 | 3 | 6/7 | 1/7 | 22/25 | 9223 | 325 |
| 2 | 1 | corpus | 30/30 | 17/17 | 17/17 | 13 | 12 | - | - | 17/17 | 9074 | 332 |
| 2 | 2 | tasks | 39/45 | 21/23 | 21/23 | 16 | 4 | 6/6 | 2/6 | 22/25 | 9223 | 314 |
| 2 | 2 | corpus | 30/30 | 18/18 | 18/18 | 12 | 11 | - | - | 18/18 | 9074 | 317 |
| 2 | 3 | tasks | 39/45 | 22/24 | 22/24 | 15 | 5 | 6/6 | 2/6 | 23/26 | 9223 | 320 |
| 2 | 3 | corpus | 30/30 | 18/18 | 18/18 | 12 | 11 | - | - | 18/18 | 9074 | 327 |
| 3 | 1 | tasks | 45/45 | 28/45 | 16/26 | 0 | 2 | - | - | 28/45 | 7648 | 170 |
| 3 | 1 | corpus | 30/30 | 12/30 | 7/19 | 0 | 0 | - | - | 12/30 | 7499 | 230 |
| 3 | 2 | tasks | 45/45 | 30/45 | 18/26 | 0 | 2 | - | - | 30/45 | 7648 | 184 |
| 3 | 2 | corpus | 30/30 | 12/30 | 7/19 | 0 | 0 | - | - | 12/30 | 7499 | 231 |
| 3 | 3 | tasks | 44/45 | 29/44 | 17/25 | 0 | 2 | 1/1 | 0/1 | 30/45 | 7648 | 177 |
| 3 | 3 | corpus | 30/30 | 13/30 | 8/19 | 0 | 0 | - | - | 13/30 | 7499 | 192 |

| Seeded repairs | Accepted | Correct |
|---|---|---|
| e2-r1 | 17/18 | 17/17 |
| e2-r2 | 17/18 | 17/17 |
| e2-r3 | 16/18 | 16/16 |
| e3-r1 | 24/29 | 21/24 |
| e3-r2 | 23/29 | 21/23 |
| e3-r3 | 23/29 | 21/23 |

## Spread over runs

Counts per run; a range is the lowest and highest run. Shared tasks are those edition 2 expresses in one query, where both editions are judged automatically.

| Edition | Set | Tasks | First try accepted | First try correct | Shared: accepted | Shared: correct | Refused first | Repaired with card | Repaired without card | Seeded accepted | Seeded correct | Tokens in | Tokens out |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2 | tasks | 45 | 38–39 (mean 38.7) | 21–22 (mean 21.7) | 24 of 26 | 21–22 (mean 21.7) of 26 | 6–7 (mean 6.3) | 6 | 1–2 (mean 1.7) | - | - | 9223 | 314–325 (mean 319.7) |
| 2 | corpus | 30 | 30 | 17–18 (mean 17.7) | 19 of 19 | 17–18 (mean 17.7) of 19 | 0 | 0 | 0 | - | - | 9074 | 317–332 (mean 325.3) |
| 2 | all | 75 | 68–69 (mean 68.7) | 39–40 (mean 39.3) | 43 of 45 | 39–40 (mean 39.3) of 45 | 6–7 (mean 6.3) | 6 | 1–2 (mean 1.7) | 16–17 (mean 16.7) of 18 | 16–17 (mean 16.7) of 18 | 9163 | 315–328 (mean 322.0) |
| 3 | tasks | 45 | 44–45 (mean 44.7) | 28–30 (mean 29.0) | 25–26 (mean 25.7) of 26 | 16–18 (mean 17.0) of 26 | 0–1 (mean 0.3) | 0–1 (mean 0.3) | 0 | - | - | 7648 | 170–184 (mean 177.0) |
| 3 | corpus | 30 | 30 | 12–13 (mean 12.3) | 19 of 19 | 7–8 (mean 7.3) of 19 | 0 | 0 | 0 | - | - | 7499 | 192–231 (mean 217.7) |
| 3 | all | 75 | 74–75 (mean 74.7) | 40–42 (mean 41.3) | 44–45 (mean 44.7) of 45 | 23–25 (mean 24.3) of 45 | 0–1 (mean 0.3) | 0–1 (mean 0.3) | 0 | 23–24 (mean 23.3) of 29 | 21 of 29 | 7588 | 183–203 (mean 193.3) |

## The RFC gate

On the 45 shared tasks, per run: edition 3 first-try accepted [45, 45, 44] against edition 2's [43, 43, 43], correct [23, 25, 25] against [39, 39, 40]. Acceptance at or above edition 2's: yes; correctness at or above: no. Verdict: **fail** (means over the runs).

## By task

First-try correct over the runs, per edition; `n/j` is correct of judged, `-` not judged automatically (no edition-2 truth, or an edition-2 answer of several queries; `q` is the most queries an edition-2 answer used). Rows where the editions differ, or where either edition was refused or wrong at least once.

| Task | Kind | Edition 2 | Edition 3 | Difference |
|---|---|---|---|---|
| a02 | two hops from a keyed node, counted | -, q2 | 0/3 |  |
| a03 | filter on an edge property | 3/3 | 0/3 | lost |
| a05 | delete nodes selected by an anti-join | -, q2 | 2/3 |  |
| a06 | an aggregate of an aggregate | - | 0/3 |  |
| a07 | filter on a sum | 2/2, q2 | 0/3 | lost |
| a08 | a top-1 read feeding a traversal | -, q2 | 0/3 |  |
| a10 | count per value of a property | 3/3 | 0/3 | lost |
| r01 | lexical ranking with the score | 3/3 | 1/3 | lost |
| r02 | a vector window, then a traversal from the hits | -, q2 | 0/3 |  |
| r03 | a vector window over a traversal's population | 3/3 | 0/3 | lost |
| r04 | a lexical cut across the corpus, then a traversal | -, q2 | 1/3 |  |
| r05 | hybrid search by fusion | 3/3 | 0/3 | lost |
| r07 | insert edges fed by a traversal | -, q2 | 0/3 |  |
| r08 | count per destination | 3/3 | 0/3 | lost |
| r09 | bounded multi-hop | 3/3 | 0/3 | lost |
| r10 | count with zero for an empty group | -, q2 | 0/3 |  |
| s03 | two aggregates over one traversal | 3/3 | 0/3 | lost |
| s07 | distinct count under an existence test | 3/3 | 0/3 | lost |
| s08 | filter on a count | 3/3 | 0/3 | lost |
| m2 | count block compared with another binding's value | -, 2 refused, q2 | 3/3 |  |
| m4 | reuse of a declared read with arguments | -, q2 | 0/3 |  |
| m5 | a window over documents, then a traversal from the hits | -, q2 | 2/3 |  |
| m6 | window versus population | -, 3 refused | 0/3 |  |
| t02 | comparison filter | 0/3 | 0/3 |  |
| t07 | interface binding | -, 3 refused | 2/3 |  |
| t08 | arithmetic | -, 3 refused | 3/3 |  |
| t09 | conditional and default values | - | 2/3 |  |
| t11 | either orientation | 3/3 | 1/3 | lost |
| t13 | edge whose name is a reserved word | -, 3 refused | 0/3 |  |
| t16 | distinct counts | 3/3 | 0/3 | lost |
| t18 | count block with a parameter bound | 3/3 | 1/3 | lost |
| t20 | outer join | - | 0/3 |  |
| t21 | union of two reads | -, 1 refused | 3/3 |  |
| t22 | correlated subquery per group | - | 0/3 |  |
| t23 | parameterised limit with a default | - | 0/3 |  |
| t24 | nearest window | 3/3 | 1/3 | lost |
| t25 | nearest with the distance returned | 3/3 | 0/3 | lost |
| t27 | lexical ranking | 2/2, q2 | 0/3 | lost |
| t29 | hybrid, fused | 3/3 | 0/3 | lost |
| t30 | hybrid, weighted | - | 0/3 |  |
| t34 | write whose targets come from a traversal | 3/3 | 0/2, 1 refused | lost |
| t35 | write with a computed value | -, 1 refused | 3/3 |  |
| t37 | unattached branch snapshot | 0/3 | 3/3 | gained |
| t38 | the engine catalogue | -, 3 refused | 3/3 |  |

## Codes behind every repair

Every refused first answer, the code it was refused with, and whether one repair turn was accepted with the card and without it; then the seeded answers by code.

| Edition | Run | Task | Code | With card | Without card |
|---|---|---|---|---|---|
| 2 | 1 | m2 | T7 | accepted | refused (T1) |
| 2 | 1 | m6 | T7 | accepted | accepted |
| 2 | 1 | t07 | T1 | accepted | refused (Q001) |
| 2 | 1 | t08 | Q001 | accepted | refused (Q001) |
| 2 | 1 | t13 | T7 | refused (T7) | refused (T1) |
| 2 | 1 | t35 | Q001 | accepted | refused (Q001) |
| 2 | 1 | t38 | none | accepted, wrong | refused (T1) |
| 2 | 2 | m2 | T7 | accepted | accepted |
| 2 | 2 | m6 | T7 | accepted | accepted |
| 2 | 2 | t07 | T1 | accepted | refused (Q001) |
| 2 | 2 | t08 | Q001 | accepted | refused (Q001) |
| 2 | 2 | t13 | T7 | accepted, correct | refused (Q001) |
| 2 | 2 | t38 | none | accepted, wrong | refused (T6) |
| 2 | 3 | m6 | T7 | accepted | accepted |
| 2 | 3 | t07 | T1 | accepted | refused (Q001) |
| 2 | 3 | t08 | Q001 | accepted | refused (Q001) |
| 2 | 3 | t13 | T7 | accepted, correct | refused (Q001) |
| 2 | 3 | t21 | T1 | accepted | accepted |
| 2 | 3 | t38 | none | accepted, wrong | refused (T6) |
| 3 | 3 | t34 | Q015 | accepted, correct | refused (Q013) |

| Edition | Seed code | Class | Accepted | Correct |
|---|---|---|---|---|
| 2 | Q001 | a limit given as a parameter | 3/3 | 3/3 |
| 2 | Q001 | single-quoted strings | 3/3 | 3/3 |
| 2 | Q002 | a declaration without its parameter list | 3/3 | 3/3 |
| 2 | T1 | an unknown node type | 3/3 | 3/3 |
| 2 | T12 | an insert without a required property | 3/3 | 3/3 |
| 2 | T14 | an undeclared parameter in a write | 3/3 | 3/3 |
| 2 | T17 | nearest ordering without a limit | 3/3 | 3/3 |
| 2 | T22 | an undirected traversal over an edge between two types | 2/3 | 2/3 |
| 2 | T25 | two result columns with one name | 3/3 | 3/3 |
| 2 | T27 | a full-text call on an unindexed property | 3/3 | 3/3 |
| 2 | T33 | a projected rank that does not repeat the ordering | 3/3 | 3/3 |
| 2 | T4 | an unknown edge type | 3/3 | 3/3 |
| 2 | T42 | an order key that names nothing returned | 3/3 | 3/3 |
| 2 | T5 | a traversal with no bound end | 3/3 | 3/3 |
| 2 | T6 | an unknown property | 3/3 | 3/3 |
| 2 | T7 | a parameter of the wrong type in a comparison | 3/3 | 3/3 |
| 2 | T7 | a reserved edge name written bare | 0/3 | 0/3 |
| 2 | T9 | an uncorrelated negation block | 3/3 | 3/3 |
| 3 | Q001 | `union` between two reads (SQL) | 0/3 | 0/3 |
| 3 | Q002 | a declaration without its parameter list | 3/3 | 3/3 |
| 3 | Q005 | a write body wrapped in `match { }` | 3/3 | 3/3 |
| 3 | Q007 | an arrow traversal (Cypher) | 3/3 | 3/3 |
| 3 | Q008 | `delete T where` (edition 2's spelling) | 3/3 | 3/3 |
| 3 | Q008 | `update T set … where` (edition 2's spelling) | 3/3 | 3/3 |
| 3 | Q009 | the `mutation` keyword (GraphQL) | 3/3 | 3/3 |
| 3 | Q010 | a comparison inside binding braces | 3/3 | 3/3 |
| 3 | Q012 | `CASE … END` (SQL) | 3/3 | 3/3 |
| 3 | Q012 | `let … in` (a row-binding `let`) | 3/3 | 3/3 |
| 3 | Q014 | `count(*)` (SQL) | 0/3 | 0/3 |
| 3 | Q014 | `having` (SQL) | 3/3 | 0/3 |
| 3 | Q014 | `optional` without braces (Cypher) | 3/3 | 3/3 |
| 3 | Q014 | `where` (SQL, Cypher) | 3/3 | 3/3 |
| 3 | Q014 | a `null` literal in a read | 3/3 | 3/3 |
| 3 | Q014 | an uppercase keyword | 3/3 | 3/3 |
| 3 | Q014 | single-quoted strings and `==` | 3/3 | 0/3 |
| 3 | Q016 | a minus spaced on one side only | 3/3 | 3/3 |
| 3 | Q019 | a space before a call's parenthesis | 3/3 | 3/3 |
| 3 | T17 | `nearest` without `k` | 3/3 | 3/3 |
| 3 | T65 | a ranking in `order` (edition 2's spelling) | 0/3 | 0/3 |
| 3 | T65 | a ranking in `return` and `order` (edition 2's spelling) | 0/3 | 0/3 |
| 3 | T68 | two rankings drawn into one scope | 1/3 | 0/3 |
| 3 | T7 | `contains` over a list property (edition 2's spelling) | 3/3 | 3/3 |
| 3 | T7 | a reserved edge name written bare | 3/3 | 3/3 |
| 3 | T7 | infix `contains` and `starts_with` (edition 2's spelling) | 3/3 | 3/3 |
| 3 | T71 | a positional option (edition 2's `fuzzy` budget) | 3/3 | 3/3 |
| 3 | T74 | a drawn column that is neither a variable nor aliased | 3/3 | 3/3 |
| 3 | T8 | an aggregate of an aggregate in one `return` | 0/3 | 0/3 |
