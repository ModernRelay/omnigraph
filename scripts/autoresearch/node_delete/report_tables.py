#!/usr/bin/env python3
"""Growth-curve tables for the node-delete report: one table per dimension,
rows = operations, columns = dimension values, cells = `base → final`
requests (and KiB read with --bytes).

usage: report_tables.py <base-dir> <final-dir> [--bytes]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from compare import rows  # noqa: E402

OPS = [
    ("T01", "one node, no edges, main warm"),
    ("T02", "one node, 2 edges, main warm"),
    ("T03", "hub node (64 edges unless stated), main warm"),
    ("T04", "predicate, 5 nodes, main warm"),
    ("T05", "edge delete by @src, main warm"),
    ("T07", "one node, 1 edge, branch warm (first write)"),
    ("T08", "hub2 (64 edges), branch warm"),
    ("T10", "one node, 1 edge, main cold"),
    ("T12", "one node, 1 edge, branch cold"),
    ("T14", "one node, no edges, main cold"),
    ("T15", "load csv overwrite of Company"),
]
F_OPS = [
    ("F01", "one node, 1 edge, main warm"),
    ("F02", "one node, 1 edge, main warm (second)"),
    ("F03", "predicate, 4 nodes, main warm"),
    ("F05", "one node, 1 edge, main cold"),
    ("F07", "one node, 1 edge, branch warm"),
]

DIMENSIONS = [
    ("Per-commit fragments (N one-row Person commits and N one-row Knows commits)",
     [("0", "p0000"), ("32", "p0032"), ("256", "p0256"), ("1024", "p1024")], OPS),
    ("Commit depth without fragment growth (one-row updates of one node)",
     [("0", "p0000"), ("512", "u0512")], OPS),
    ("Deletion files from earlier deletes (N four-row fragments, one row deleted in each)",
     [("0", "p0000"), ("64", "x0064"), ("256", "x0256")], OPS),
    ("Rows in Person and in Knows (one fragment each)",
     [("~80", "p0000"), ("1k", "s001k"), ("10k", "s010k"), ("100k", "s100k")], OPS),
    ("Row width (filler rows with a 16 KiB string and a 16 KiB Blob)",
     [("1k narrow", "s001k"), ("256 wide", "w0256"), ("1024 wide", "w1024")], OPS),
    ("Declared indexes (BTREE, full-text, vector) over N rows, one fragment",
     [("1k", "i001k"), ("10k", "i010k"), ("100k", "i100k")], OPS),
    ("Indexed Person fragments (N one-row commits, then indexes built)",
     [("32", "f0032"), ("256", "f0256"), ("1024", "f1024")], F_OPS),
    ("Uncovered fragments after the index (256 indexed fragments, then N one-row commits)",
     [("0", "f0256"), ("64", "f0256u064"), ("256", "f0256u256")], F_OPS),
    ("The same fragments without indexes",
     [("32", "g0032"), ("256", "g0256"), ("1024", "g1024")], F_OPS),
    ("Edges on the hub node (T03)",
     [("16", "k0016"), ("64", "p0000"), ("1024", "k1024")], OPS),
    ("Incident edge types (32 history commits)",
     [("2", "p0032"), ("18", "t16")], OPS),
    ("Untouched tables (32 node types, 16 edge types not incident to Person)",
     [("0", "p0000"), ("48", "o32")], OPS),
    ("Branches (32 history commits)",
     [("none", "p0032"), ("256 dead", "dead256"), ("256 live", "live256")], OPS),
    ("Nodes deleted by one predicate, each with one edge (T16)",
     [("1", "n0001"), ("16", "n0016"), ("256", "n0256"), ("4096", "n4096")],
     [("T16", "predicate deleting N nodes and N edges, main warm")]),
]


def main():
    base, final = sys.argv[1], sys.argv[2]
    show_bytes = "--bytes" in sys.argv
    for title, columns, ops in DIMENSIONS:
        data = {}
        for _, case in columns:
            try:
                data[case] = (rows(base, case), rows(final, case))
            except Exception:  # noqa: BLE001
                data[case] = None
        print(f"**{title}** (requests, baseline → final" + (", KiB read" if show_bytes else "") + ")\n")
        print("| operation | " + " | ".join(label for label, _ in columns) + " |")
        print("|---|" + "---:|" * len(columns))
        for step, label in ops:
            cells = []
            for _, case in columns:
                if data[case] is None:
                    cells.append("n/a")
                    continue
                a, b = data[case]
                name = next(n for n in a if n.startswith(step + " "))
                cell = f"{a[name]['total']} → {b[name]['total']}"
                if show_bytes:
                    cell += f" ({a[name]['bytes'] // 1024} → {b[name]['bytes'] // 1024})"
                cells.append(cell)
            print(f"| {step} {label} | " + " | ".join(cells) + " |")
        print()


if __name__ == "__main__":
    main()
