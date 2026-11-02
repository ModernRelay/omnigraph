#!/usr/bin/env python3
"""Generate the node-delete cost-curve GQT cases (autoresearch 2026-11-02).

Each case builds a history, then runs a fixed measured tail of delete forms on
main and on a named branch: warm on the handle that built the history, then
cold after `--- restart`.

Dimensions (one per case family):
  pNNNN   NNNN one-row Person commits and NNNN one-row Knows commits
          (per-commit fragments in the node table and in an incident edge table)
  uNNNN   NNNN one-row updates of one Person (commit depth, no fragment growth)
  xNNNN   NNNN four-row Person commits, then one one-row delete in each
          (NNNN deletion files accumulated before the tail)
  kNNNN   the hub node has NNNN Knows edges (degree), no history
  tNN     NN extra edge types incident to Person, 32 history commits
  deadNNN / liveNNN  NNN dead or live branches, 32 history commits

usage: gen_cases.py <out-dir> [case ...]
Run with: target/debug/omnigraph-gqt <case.gqt> --measure --artifacts <dir>
The cases live outside the corpus (their budgets exceed the corpus cap).
"""

import json
import sys
from pathlib import Path

RUNNER = """--- runner
timeout_ms: 600000
environments:
  - target: omnigraph-engine-dst
    storage: in-memory-object-store
    seeds: [0]
"""

HUB_DEGREE = 64


def schema(extra_edge_types: int, wide: bool = False, indexed: bool = False,
           other_nodes: int = 0, other_edges: int = 0) -> str:
    person = ["node Person {", "    name: String @key", "    age: I32"]
    if wide:
        person += ["    bio: String?", "    photo: Blob?"]
    if indexed:
        person += ["    text: String? @index", "    emb: Vector(8)? @index", "    @index(age)"]
    person.append("}")
    out = ["--- schema"] + person + [
           "node Company {", "    name: String @key", "}",
           "edge WorksAt: Person -> Company",
           "edge Knows: Person -> Person"]
    for t in range(extra_edge_types):
        out.append(f"edge Rel{t}: Person -> Company")
    for t in range(other_nodes):
        out += [f"node Other{t} {{", "    name: String @key", "}"]
    for t in range(other_edges):
        out.append(f"edge CoRel{t}: Company -> Company")
    return "\n".join(out) + "\n"


def person(name, age=0):
    return json.dumps({"type": "Person", "data": {"name": name, "age": age}})


def edge(kind, a, b):
    return json.dumps({"edge": kind, "from": a, "to": b, "data": {}})


def filler(i, size, wide, indexed):
    data = {"name": f"r{i:07d}", "age": 1000000 + i}
    if wide:
        data["bio"] = ("w%07d" % i) * (16384 // 8)
        data["photo"] = "base64:" + "QUJD" * (16384 // 3)
    if indexed:
        data["text"] = f"filler row number {i} about graphs and deletes"
        data["emb"] = [((i * 7 + d) % 97) / 97.0 for d in range(8)]
    return json.dumps({"type": "Person", "data": data})


def seed(extra_edge_types: int, hub_degree: int, size: int = 0, wide: bool = False,
         indexed: bool = False, op_size: int = 0, other_nodes: int = 0,
         other_edges: int = 0) -> str:
    rows = [json.dumps({"type": "Company", "data": {"name": "acme"}}),
            json.dumps({"type": "Company", "data": {"name": "co2"}})]
    for i in range(size):
        rows.append(filler(i, size, wide, indexed))
    for i in range(size):
        rows.append(edge("Knows", f"r{i:07d}", f"r{(i + 1) % size:07d}"))
    for i in range(op_size):
        rows.append(person(f"n{i:06d}", 900000))
        rows.append(edge("Knows", f"n{i:06d}", "bob"))
    for t in range(other_nodes):
        rows.append(json.dumps({"type": f"Other{t}", "data": {"name": "o"}}))
    for t in range(other_edges):
        rows.append(edge(f"CoRel{t}", "acme", "co2"))
    for n in ["alice", "bob", "carol", "dave", "erin", "frank", "gina", "hub", "hub2", "zed"]:
        rows.append(person(n))
    for i in range(5):
        rows.append(person(f"p{i}", -1))
    targets = max(hub_degree, HUB_DEGREE)
    for i in range(targets):
        rows.append(person(f"k{i:05d}"))
    rows.append(edge("WorksAt", "alice", "acme"))
    for a, b in [("alice", "bob"), ("dave", "bob"), ("erin", "bob"), ("frank", "bob"),
                 ("bob", "k00000")]:
        rows.append(edge("Knows", a, b))
    for i in range(hub_degree):
        rows.append(edge("Knows", "hub", f"k{i:05d}"))
    for i in range(HUB_DEGREE):
        rows.append(edge("Knows", "hub2", f"k{i:05d}"))
    for t in range(extra_edge_types):
        rows.append(edge(f"Rel{t}", "zed", "acme"))
    return "--- seed\n" + "\n".join(rows) + "\n"


def history_per_commit(n: int) -> str:
    """n one-row Knows commits, then n one-row Person commits.

    The edges come first, between seed nodes (self loops on the hub's
    targets), so their endpoint validation reads only the seed fragment and
    building the history stays linear in n.
    """
    return f"""--- load generate: v1 seed: 0 mode: append branch: main
tables:
  - kind: edge
    name: Knows
    rows: {n}
    commits: {n}
    from: {{kind: endpoint, prefix: k, width: 5, population: {HUB_DEGREE}, distribution: {{kind: ordinal}}}}
    to: {{kind: endpoint, prefix: k, width: 5, population: {HUB_DEGREE}, distribution: {{kind: ordinal}}}}
    columns: {{}}
  - kind: node
    name: Person
    rows: {n}
    commits: {n}
    columns:
      name: {{kind: key, prefix: h, width: 5}}
      age: {{kind: ordinal, start: 0, step: 1}}
--- expect ok
"""


def history_updates(n: int) -> str:
    return f"""--- loop $i 1 {n + 1}
--- mutate
query bump($n: I32) {{
    update Person set {{ age: $n }} where name = "zed"
}}
--- params
{{"n": ${{i}}}}
--- expect affected: nodes=1 edges=0
--- endloop
"""


def history_deletion_files(n: int) -> str:
    """n four-row Person commits, then delete one row of each fragment."""
    return f"""--- loop $i 0 {n}
--- mutate
query add_four($a: String, $b: String, $c: String, $d: String) {{
    insert Person {{ name: $a, age: 2000000 }}
    insert Person {{ name: $b, age: 2000000 }}
    insert Person {{ name: $c, age: 2000000 }}
    insert Person {{ name: $d, age: 2000000 }}
}}
--- params
{{"a": "xa${{i}}", "b": "xb${{i}}", "c": "xc${{i}}", "d": "xd${{i}}"}}
--- expect affected: nodes=4 edges=0
--- endloop

--- loop $i 0 {n}
--- mutate
query drop_one($k: String) {{
    delete Person where name = $k
}}
--- params
{{"k": "xa${{i}}"}}
--- expect affected: nodes=1 edges=0
--- endloop
"""


def branches(n: int, dead: bool) -> str:
    parts = []
    for k in range(n):
        name = f"{'dead' if dead else 'live'}{k}"
        parts.append(f"--- mutate\nbranch create {name}\n\n--- expect ok\n")
        if dead:
            parts.append(f"--- mutate\nbranch delete {name}\n\n--- expect ok\n")
    return "\n".join(parts)


def delete_by_key(who: str, nodes: int, edges: int, branch: str = "main", tag: str = "") -> str:
    header = "--- mutate" + (f" branch: {branch}" if branch != "main" else "")
    return f"""{header}
query delete_one{tag}($k: String) {{
    delete Person where name = $k
}}
--- params
{{"k": "{who}"}}
--- expect affected: nodes={nodes} edges={edges}
"""


TAIL_NAMES = [
    "T01 delete key no-edges main warm",
    "T02 delete key 2-edges main warm",
    "T03 delete key hub main warm",
    "T04 delete predicate 5 main warm",
    "T05 delete edge by src main warm",
    "T06 branch create b",
    "T07 delete key 1-edge branch warm",
    "T08 delete key hub2 branch warm",
    "T09 restart",
    "T10 delete key 1-edge main cold",
    "T11 restart",
    "T12 delete key 1-edge branch cold",
    "T13 restart",
    "T14 delete key no-edges main cold",
    "T15 load csv overwrite Company main",
    "T16 delete predicate N main",
]


def tail(hub_degree: int, op_size: int, indexed: bool) -> str:
    steps = [
        delete_by_key("carol", 1, 0),
        delete_by_key("alice", 1, 2),
        delete_by_key("hub", 1, hub_degree),
        """--- mutate
query delete_negative() {
    delete Person where age < 0
}
--- expect affected: nodes=5 edges=0
""",
        """--- mutate
query delete_edges_of($who: String) {
    delete Knows where @src = $who
}
--- params
{"who": "bob"}
--- expect affected: nodes=0 edges=1
""",
        "--- mutate\nbranch create b\n\n--- expect ok\n",
        delete_by_key("dave", 1, 1, "b"),
        delete_by_key("hub2", 1, HUB_DEGREE, "b"),
        "--- restart\n",
        delete_by_key("erin", 1, 1),
        "--- restart\n",
        delete_by_key("frank", 1, 1, "b"),
        "--- restart\n",
        delete_by_key("gina", 1, 0),
        """--- csv co.csv
name
acme
co2
globex

--- mutate
load csv "co.csv" into node:Company mode overwrite header

--- expect ok
""",
    ]
    if op_size:
        steps.append(f"""--- mutate
query delete_many() {{
    delete Person where age = 900000
}}
--- expect affected: nodes={op_size} edges={op_size}
""")
    else:
        steps.append("--- mutate\nset change_lineage = on;\n\n--- expect ok\n")
    assert len(steps) == len(TAIL_NAMES)
    if indexed:
        # A search step makes GQT build the declared indexes after the seed.
        steps.append("""--- query
query probe($q: String) {
    match { $p: Person
            search($p.text, $q) }
    return { $p.name }
    order { $p.name asc }
    limit 1
}
--- params
{"q": "zzzz-not-present"}
--- expect ordered
--- expect shape
p.name: String
""")
    return "\n".join(steps)


def case(name: str, *, per_commit=0, updates=0, delfiles=0, hub=HUB_DEGREE,
         extra_types=0, dead=0, live=0, size=0, wide=False, indexed=False,
         op_size=0, other_nodes=0, other_edges=0) -> str:
    parts = ["# issue: none\n",
             f"# notes: autoresearch node-delete cost curve {name}\n",
             RUNNER, schema(extra_types, wide, indexed, other_nodes, other_edges),
             seed(extra_types, hub, size, wide, indexed, op_size, other_nodes, other_edges),
             # Release defaults: a debug build defaults both to `verify`.
             "--- mutate\nset merge_lineage = on;\nset change_lineage = on;\n\n--- expect ok\n"]
    if per_commit:
        parts.append(history_per_commit(per_commit))
    if updates:
        parts.append(history_updates(updates))
    if delfiles:
        parts.append(history_deletion_files(delfiles))
    if dead:
        parts.append(branches(dead, True))
    if live:
        parts.append(branches(live, False))
    parts.append(tail(hub, op_size, indexed))
    return "\n".join(parts)


PLAN = {
    "p0000": dict(),
    "p0032": dict(per_commit=32),
    "p0256": dict(per_commit=256),
    "p1024": dict(per_commit=1024),
    "u0512": dict(updates=512),
    "x0064": dict(delfiles=64),
    "x0256": dict(delfiles=256),
    "k0016": dict(hub=16),
    "k1024": dict(hub=1024),
    "t16": dict(per_commit=32, extra_types=16),
    "dead256": dict(per_commit=32, dead=256),
    "live256": dict(per_commit=32, live=256),
    # data size: rows in Person and in Knows, one seed fragment each
    "s001k": dict(size=1000),
    "s010k": dict(size=10000),
    "s100k": dict(size=100000),
    # row width: 16 KiB string and 64 KiB Blob per filler row
    "w0256": dict(size=256, wide=True),
    "w1024": dict(size=1024, wide=True),
    # declared indexes (BTREE, full text, vector) over the filler rows
    "i001k": dict(size=1000, indexed=True),
    "i010k": dict(size=10000, indexed=True),
    "i100k": dict(size=100000, indexed=True),
    # operation size: one predicate deleting N nodes, each with one edge
    "n0001": dict(op_size=1),
    "n0016": dict(op_size=16),
    "n0256": dict(op_size=256),
    "n4096": dict(op_size=4096),
    # untouched tables: node types and edge types not incident to Person
    "o32": dict(other_nodes=32, other_edges=16),
}


F_TAIL_NAMES = [
    "F01 delete key 1-edge main warm",
    "F02 delete key 1-edge main warm again",
    "F03 delete predicate 4 main warm",
    "F04 restart",
    "F05 delete key 1-edge main cold",
    "F06 branch create b",
    "F07 delete key 1-edge branch warm",
]


def fragmented_case(name: str, n: int, indexed: bool, uncovered: int = 0) -> str:
    """Person in n one-row commits (n fragments), its Knows self loops in one
    commit; with `indexed`, a search probe makes GQT build every declared
    index (id BTREEs, edge src/dst BTREEs, the full-text index) over them."""
    person = ["node Person {", "    name: String @key", "    age: I32",
              "    text: String? @index", "}"]
    schema = "\n".join(["--- schema"] + person + [
        "node Company {", "    name: String @key", "}",
        "edge WorksAt: Person -> Company",
        "edge Knows: Person -> Person"]) + "\n"
    seed = f"""--- seed generate: v1 seed: 0
tables:
  - kind: node
    name: Person
    rows: {n}
    commits: {n}
    columns:
      name: {{kind: key, prefix: f, width: 5}}
      age: {{kind: ordinal, start: 0, step: 1}}
      text: {{kind: literal, value: "filler text"}}
  - kind: node
    name: Company
    rows: 1
    commits: 1
    columns:
      name: {{kind: key, prefix: c, width: 1}}
  - kind: edge
    name: Knows
    rows: {n}
    commits: 1
    from: {{kind: endpoint, prefix: f, width: 5, population: {n}, distribution: {{kind: ordinal}}}}
    to: {{kind: endpoint, prefix: f, width: 5, population: {n}, distribution: {{kind: ordinal}}}}
    columns: {{}}
"""
    probe = """--- query
query probe($q: String) {
    match { $p: Person
            search($p.text, $q) }
    return { $p.name }
    order { $p.name asc }
    limit 1
}
--- params
{"q": "zzzz-not-present"}
--- expect ordered
--- expect shape
p.name: String
"""
    def d(key, branch="main"):
        return delete_by_key(key, 1, 1, branch)
    steps = [
        d("f00001"),
        d("f00002"),
        f"""--- mutate
query delete_low() {{
    delete Person where age > {n - 6} and age < {n - 1}
}}
--- expect affected: nodes=4 edges=4
""",
        "--- restart\n",
        d("f00003"),
        "--- mutate\nbranch create b\n\n--- expect ok\n",
        d("f00004", "b"),
    ]
    assert len(steps) == len(F_TAIL_NAMES)
    parts = ["# issue: none\n", f"# notes: autoresearch node-delete cost curve {name}\n",
             RUNNER, schema, seed,
             "--- mutate\nset merge_lineage = on;\nset change_lineage = on;\n\n--- expect ok\n"]
    if indexed:
        parts.append(probe)
    if uncovered:
        # One-row Person commits after the indexes: fragments the id BTREE
        # does not cover.
        parts.append(f"""--- load generate: v1 seed: 1 mode: append branch: main
tables:
  - kind: node
    name: Person
    rows: {uncovered}
    commits: {uncovered}
    columns:
      name: {{kind: key, prefix: u, width: 5}}
      age: {{kind: ordinal, start: 5000000, step: 1}}
      text: {{kind: literal, value: "filler text"}}
--- expect ok
""")
    parts += steps
    return "\n".join(parts)


F_PLAN = {
    "f0032": (32, True), "f0256": (256, True), "f1024": (1024, True),
    "g0032": (32, False), "g0256": (256, False), "g1024": (1024, False),
    "f0256u064": (256, True, 64), "f0256u256": (256, True, 256),
}


def main() -> None:
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    only = set(sys.argv[2:])
    for name, kw in PLAN.items():
        if only and name not in only:
            continue
        (out / f"{name}.gqt").write_text(case(name, **kw))
    for name, args in F_PLAN.items():
        if only and name not in only:
            continue
        (out / f"{name}.gqt").write_text(fragmented_case(name, *args))


if __name__ == "__main__":
    main()
