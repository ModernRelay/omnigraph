#!/usr/bin/env python3
"""Check (or rewrite) the engine catalogue inventories in
docs/user/queries/catalogue.md against the rows a built omnigraph CLI answers
under the `og_catalog` scope.

The binary is named explicitly so the check never queries an installed CLI
of another version; no store is initialized and no credentials are read.

    python3 scripts/check-catalogue-docs.py --binary target/debug/omnigraph
    python3 scripts/check-catalogue-docs.py --binary target/debug/omnigraph --write
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys

DOC = pathlib.Path(__file__).resolve().parent.parent / "docs" / "user" / "queries" / "catalogue.md"

INVENTORIES = {
    "operators": (
        "query rows() { match { $o: @Operator } return { $o.name as name, $o.inputs as inputs } order { $o.name } }",
        ["name", "inputs"],
    ),
    "passes": (
        "query rows() { match { $p: @Pass } return { $p.position as position, $p.name as name } order { $p.position } }",
        ["position", "name"],
    ),
    "settings": (
        "query rows() { match { $s: @Setting } return { $s.name as name, $s.kind as kind, $s.default as default, $s.scope as scope } order { $s.name } }",
        ["name", "kind", "default", "scope"],
    ),
    "logical_kinds": (
        "query rows() { match { $k: @LogicalKind } return { $k.name as name, $k.coverage as coverage } order { $k.name } }",
        ["name", "coverage"],
    ),
}


def rows_of(binary: pathlib.Path, query: str) -> list[dict]:
    result = subprocess.run(
        [str(binary), "query", "--target", "og_catalog", "-e", query, "--json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(f"{binary} refused the catalogue query")
    return json.loads(result.stdout)["rows"]


def cell(value) -> str:
    if isinstance(value, list):
        return "[" + ", ".join(str(item) for item in value) + "]"
    return "" if value is None else str(value)


def table(columns: list[str], rows: list[dict]) -> str:
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for row in rows:
        lines.append("| " + " | ".join(cell(row.get(column)) for column in columns) + " |")
    return "\n".join(lines)


def replace_block(text: str, name: str, body: str) -> tuple[str, bool]:
    start = f"<!-- catalogue:{name} -->"
    end = f"<!-- /catalogue:{name} -->"
    head, sep, rest = text.partition(start)
    if not sep:
        raise SystemExit(f"{DOC} has no block {start}")
    current, sep, tail = rest.partition(end)
    if not sep:
        raise SystemExit(f"{DOC} has no block end {end}")
    wanted = f"\n{body}\n"
    return head + start + wanted + end + tail, current == wanted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--binary", required=True, type=pathlib.Path, help="the omnigraph CLI to query")
    parser.add_argument("--write", action="store_true", help="rewrite the blocks instead of checking them")
    args = parser.parse_args()
    if not args.binary.is_file():
        raise SystemExit(f"{args.binary} is not a file")
    text = DOC.read_text(encoding="utf-8")
    drifted = []
    for name, (query, columns) in INVENTORIES.items():
        body = table(columns, rows_of(args.binary, query))
        text, same = replace_block(text, name, body)
        if not same:
            drifted.append(name)
    if args.write:
        DOC.write_text(text, encoding="utf-8")
        print(f"rewrote {len(drifted)} block(s): {', '.join(drifted) or 'none changed'}")
        return 0
    if drifted:
        print(
            f"{DOC}: inventories drift from the binary: {', '.join(drifted)}; "
            f"rerun with --write",
            file=sys.stderr,
        )
        return 1
    print("catalogue inventories match the binary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
