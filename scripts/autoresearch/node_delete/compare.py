#!/usr/bin/env python3
"""Compare two node-delete sweeps step by step.

usage: compare.py <base-dir> <new-dir> [case ...] [--steps T01,T10] [--bytes]
Prints `requests base -> new` (and bytes read) per case and tail step.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from analyze import load, table_names, tail_rows  # noqa: E402
from gen_cases import F_TAIL_NAMES, TAIL_NAMES  # noqa: E402


def rows(d, case):
    text = open(os.path.join(os.environ.get("CASES", "/src/ndwork/cases"), case + ".gqt")).read()
    if case[0] in "fg":
        return tail_rows(load(os.path.join(d, case)), table_names(text), 0, F_TAIL_NAMES)
    return tail_rows(load(os.path.join(d, case)), table_names(text),
                     1 if "search($p.text" in text else 0)


def main():
    args = sys.argv[1:]
    base, new = args[0], args[1]
    steps = None
    if "--steps" in args:
        steps = args[args.index("--steps") + 1].split(",")
    show_bytes = "--bytes" in args
    cases = [a for a in args[2:] if not a.startswith("--") and (steps is None or a not in ",".join(steps))]
    if not cases:
        cases = sorted(c for c in os.listdir(new) if os.path.isdir(os.path.join(new, c))
                       and os.path.isdir(os.path.join(base, c)))
    all_names = F_TAIL_NAMES if cases and cases[0][0] in "fg" else TAIL_NAMES
    names = [n for n in all_names if "restart" not in n
             and (steps is None or n.split()[0] in steps)]
    print("| case | " + " | ".join(n.split()[0] for n in names) + " |")
    print("|---|" + "---:|" * len(names))
    for c in cases:
        try:
            a, b = rows(base, c), rows(new, c)
        except Exception as e:  # noqa: BLE001
            print(f"| {c} | error {e} |")
            continue
        cells = []
        for n in names:
            x, y = a[n], b[n]
            cell = f"{x['total']} → {y['total']}"
            if show_bytes:
                cell += f" ({x['bytes'] // 1024}→{y['bytes'] // 1024} KiB)"
            cells.append(cell)
        print(f"| {c} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
