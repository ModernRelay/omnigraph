#!/usr/bin/env python3
"""Generate the branch-ops cost-curve GQT cases (autoresearch 2026-11-02).

Each case builds a history with one-row mutations on main (every one of them
a measured step, so the mutate cost curve comes for free), optionally many
dead branches or many tables, then runs a fixed measured tail of the branch
and write operations: warm on the same handle, then cold after a restart.

Run the cases with
  target/debug/omnigraph-gqt <dir> --measure --artifacts <out>
The cases live outside the corpus (their budgets exceed the corpus cap).
"""

import sys
from pathlib import Path

RUNNER = """--- runner
timeout_ms: {timeout}
environments:
  - target: omnigraph-engine-dst
    storage: in-memory-object-store
    seeds: [0]
"""


def schema(tables: int) -> str:
    out = ["--- schema", "node Person {", "    name: String @key", "    age: I32", "}",
           "node Company {", "    name: String @key", "}",
           "edge WorksAt: Person -> Company"]
    for t in range(tables - 3):
        out += [f"node Extra{t} {{", "    name: String @key", "    v: I32", "}"]
    return "\n".join(out) + "\n"


def seed(tables: int) -> str:
    rows = [
        '{"type":"Person","data":{"name":"alice","age":0}}',
        '{"type":"Person","data":{"name":"bob","age":0}}',
        '{"type":"Company","data":{"name":"acme"}}',
        '{"edge":"WorksAt","from":"alice","to":"acme","data":{}}',
    ]
    for t in range(tables - 3):
        rows.append(f'{{"type":"Extra{t}","data":{{"name":"x","v":0}}}}')
    return "--- seed\n" + "\n".join(rows) + "\n"


def bump(branch: str, who: str, n: str) -> str:
    header = "--- mutate" + (f" branch: {branch}" if branch != "main" else "")
    return f"""{header}
query bump_{who}($n: I32) {{
    update Person set {{ age: $n }} where name = "{who}"
}}
--- params
{{"n": {n}}}
--- expect affected: nodes=1 edges=0
"""


def branch(stmt: str, outcome: str = "ok") -> str:
    expect = "--- expect ok" if outcome == "ok" else f"--- expect outcome: {outcome}"
    return f"--- mutate\n{stmt}\n\n{expect}\n"


def load(mode: str, branch_name: str, prefix: str) -> str:
    return f"""--- load generate: v1 seed: 0 mode: {mode} branch: {branch_name}
tables:
  - kind: node
    name: Person
    rows: 10
    commits: 1
    columns:
      name: {{kind: key, prefix: {prefix}-, width: 2}}
      age: {{kind: ordinal, start: 0, step: 1}}
--- expect ok
"""


def insert(i: int) -> str:
    return f"""--- mutate
query add_person($name: String, $n: I32) {{
    insert Person {{ name: $name, age: $n }}
}}
--- params
{{"name": "h{i:05d}", "n": {i}}}
--- expect affected: nodes=1 edges=0
"""


def case(depth: int, dead: int, tables: int, mode: str = "update", live: int = 0) -> str:
    timeout = 600000
    parts = ["# issue: none\n", f"# notes: autoresearch branch-ops cost curve: depth={depth} dead={dead} tables={tables}\n",
             RUNNER.format(timeout=timeout), schema(tables), seed(tables),
             # Release defaults: a debug build defaults both to `verify`, which runs both paths.
             "--- mutate\nset merge_lineage = on;\nset change_lineage = on;\n\n--- expect ok\n"]
    # History: one-row mutations on main, each a measured step.
    if depth and mode == "insert":
        # Unrolled so each commit is its own measured step.
        parts += [insert(i) for i in range(1, depth + 1)]
    elif depth:
        parts.append(f"--- loop $i 1 {depth + 1}\n" + bump("main", "bob", "${i}") + "--- endloop\n")
    # Dead branches: distinct names, created then deleted.
    for k in range(dead):
        parts.append(branch(f"branch create dead{k}"))
        parts.append(branch(f"branch delete dead{k}"))
    # Live branches: created and left alive.
    for k in range(live):
        parts.append(branch(f"branch create live{k}"))
    tail = [
        ("T01 mutate main warm", bump("main", "bob", "-1")),
        ("T02 branch create warm", branch("branch create feat")),
        ("T03 mutate branch warm", bump("feat", "alice", "-2")),
        ("T04 load append main warm", load("append", "main", "a")),
        ("T05 load merge branch warm", load("merge", "feat", "m")),
        ("T06 load merge main warm", load("merge", "main", "a")),
        ("T07 load append branch warm", load("append", "feat", "b")),
        ("T08 branch merge threeway warm", branch("branch merge feat into main", "merged")),
        ("T09 branch delete warm", branch("branch delete feat")),
        ("T10 branch create ff", branch("branch create ff")),
        ("T11 mutate branch ff", bump("ff", "alice", "-3")),
        ("T12 branch merge fastforward warm", branch("branch merge ff into main", "fast_forward")),
        ("T13 branch delete ff", branch("branch delete ff")),
        ("T14 restart", "--- restart\n"),
        ("T15 mutate main cold", bump("main", "bob", "-4")),
        ("T16 restart", "--- restart\n"),
        ("T17 branch create cold", branch("branch create c")),
        ("T18 mutate branch c", bump("c", "alice", "-5")),
        ("T19 restart", "--- restart\n"),
        ("T20 branch merge cold", branch("branch merge c into main", "fast_forward")),
        ("T21 restart", "--- restart\n"),
        ("T22 branch delete cold", branch("branch delete c")),
        ("T23 restart", "--- restart\n"),
        ("T24 load append main cold", load("append", "main", "z")),
    ]
    for _, text in tail:
        parts.append(text)
    return "\n".join(parts)


def main() -> None:
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    plan = [
        ("d0000", 0, 0, 3),
        ("d0032", 32, 0, 3),
        ("d0256", 256, 0, 3),
        ("d2048", 2048, 0, 3),
        ("d0032_dead0064", 32, 64, 3),
        ("d0032_dead0256", 32, 256, 3),
        ("d0032_tables32", 32, 0, 32),
        ("i0000", 0, 0, 3, "insert"),
        ("i0032", 32, 0, 3, "insert"),
        ("i0256", 256, 0, 3, "insert"),
        ("i1024", 1024, 0, 3, "insert"),
        ("i2048", 2048, 0, 3, "insert"),
        ("i0256_tables32", 256, 0, 32, "insert"),
        ("d0032_live0064", 32, 0, 3, "update", 64),
        ("d0032_live0256", 32, 0, 3, "update", 256),
    ]
    only = set(sys.argv[2:])
    for name, depth, dead, tables, *mode in plan:
        if only and name not in only:
            continue
        (out / f"{name}.gqt").write_text(case(depth, dead, tables, *mode))


if __name__ == "__main__":
    main()
