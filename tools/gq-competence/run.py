#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["anthropic>=1.0", "pyyaml>=6"]
# ///
"""In-context competence instrument for the GQ query language.

Measures how well a model that has never been trained on GQ writes it from a
schema, a one-page card and its own errors: first-try validity, turns to the
first correct query, task success, tokens, and the diagnostic behind each
repair. Ground truth is computed by exact queries at a pinned graph commit,
so the graph is its own verifier.

    uv run tools/gq-competence/run.py truth  --tasks tasks.yaml --snapshot <commit>
    uv run tools/gq-competence/run.py run    --tasks tasks.yaml --snapshot <commit> --model claude-opus-5
    uv run tools/gq-competence/run.py report results/<run>.jsonl

`run` needs an Anthropic credential (ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN or
an `ant auth login` profile). `truth` and `report` do not call a model.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
ERROR_CODE = re.compile(r"\b(Q\d{3}|T\d{1,2}|L\d{3})\b")

# --------------------------------------------------------------------------- graph


@dataclass
class Graph:
    server: str
    graph: str
    snapshot: str
    timeout: float = 60.0

    def query(self, source: str, params: dict | None = None) -> tuple[bool, dict | str]:
        """Run one ad-hoc read at the pinned commit. Returns (ok, rows-or-error)."""
        cmd = [
            "omnigraph", "query", "-e", source,
            "--server", self.server, "--graph", self.graph,
            "--snapshot", self.snapshot, "--json",
        ]
        if params:
            cmd += ["--params", json.dumps(params)]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        except subprocess.TimeoutExpired:
            return False, f"timeout after {self.timeout:.0f}s"
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout).strip()
            return False, err
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return False, f"unparseable output: {proc.stdout[:400]}"
        if payload.get("graph_commit_id") not in (None, self.snapshot):
            return False, f"snapshot drift: {payload.get('graph_commit_id')} != {self.snapshot}"
        return True, payload


def schema_text(graph: Graph) -> str:
    proc = subprocess.run(
        ["omnigraph", "schema", "show", "--server", graph.server, "--graph", graph.graph],
        capture_output=True, text=True, timeout=120, check=True,
    )
    return proc.stdout


# --------------------------------------------------------------------------- truth


def _row_key(row: object) -> str:
    """A row compares by its values only, so `$o.kind` and `kind` are the same
    column and column order does not matter. A one-column row is its value."""
    if isinstance(row, dict):
        values = [json.dumps(v, sort_keys=True) for v in row.values()]
        return values[0] if len(values) == 1 else json.dumps(sorted(values))
    return json.dumps(row, sort_keys=True)


def normalize(kind: str, rows: list[dict]) -> object:
    """Project query rows onto the task's answer shape so a model's query and
    the truth query compare on values, not column names."""
    if kind == "scalar":
        if len(rows) == 1 and len(rows[0]) == 1:
            return next(iter(rows[0].values()))
        return None
    if kind == "set":
        return sorted({_row_key(r) for r in rows})
    if kind == "list":
        return [_row_key(r) for r in rows]
    raise ValueError(f"unknown answer kind {kind}")


def normalize_answer(kind: str, answer: object) -> object:
    """Normalize a submitted answer the same way as rows."""
    if kind == "scalar":
        if isinstance(answer, list) and len(answer) == 1:
            answer = answer[0]
        return answer
    if not isinstance(answer, list):
        return None
    if kind == "set":
        return sorted({_row_key(a) for a in answer})
    if kind == "list":
        return [_row_key(a) for a in answer]
    raise ValueError(kind)


def equal(kind: str, a: object, b: object) -> bool:
    if kind == "scalar":
        try:
            return float(a) == float(b)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return a == b
    return a == b


def compute_truth(tasks: list[dict], graph: Graph) -> dict[str, object]:
    truth = {}
    for task in tasks:
        ok, payload = graph.query(task["truth_query"], task.get("truth_params"))
        if not ok:
            raise SystemExit(f"truth query for {task['id']} failed: {payload}")
        truth[task["id"]] = normalize(task["kind"], payload["rows"])  # type: ignore[index]
        print(f"{task['id']}: {json.dumps(truth[task['id']])[:120]}")
    return truth


# --------------------------------------------------------------------------- model run

SYSTEM_TEMPLATE = """You answer questions about a property graph by writing GQ queries and running them with the `run_gq` tool. Read the card and the schema; both are complete. Every query runs against one pinned snapshot, so results are stable within this task.

When you have the answer, call `submit` exactly once with `answer_json`, a JSON value as a string. For a count, submit the number (`"72"`). For "which X" questions, submit a JSON list of slugs or the requested values. For a table, submit a JSON list of objects with the requested keys. Prefer one query that answers the question over several; pass values through `params_json` rather than inlining them.

<card>
{card}
</card>

<schema>
{schema}
</schema>
"""

TOOLS = [
    {
        "name": "run_gq",
        "description": "Run one GQ read query against the graph at the pinned snapshot. Returns the result rows as JSON, or the compiler/engine error text.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "A complete GQ source with exactly one query declaration."},
                "params_json": {"type": "string", "description": "A JSON object with the query's $parameters, as a string; \"{}\" when none."},
            },
            "required": ["query", "params_json"],
            "additionalProperties": False,
        },
    },
    {
        "name": "submit",
        "description": "Submit the final answer. Call once, after the result you rely on has come back from run_gq.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {"answer_json": {"type": "string", "description": "The answer as a JSON value in a string: a number, a string, or a list (of values or objects)."}},
            "required": ["answer_json"],
            "additionalProperties": False,
        },
    },
]

MAX_ROWS_RETURNED = 60


@dataclass
class Trial:
    task: str
    model: str
    queries: list[dict] = field(default_factory=list)
    submitted: object = None
    success: bool = False
    first_try_valid: bool | None = None
    turns_to_correct: int | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    wall_s: float = 0.0
    stop: str = ""


def run_task(client, model: str, effort: str, system: list, task: dict, truth: object,
             graph: Graph, max_turns: int) -> Trial:
    trial = Trial(task=task["id"], model=model)
    messages = [{"role": "user", "content": task["question"]}]
    started = time.time()
    for turn in range(max_turns):
        response = client.messages.create(
            model=model,
            max_tokens=16000,
            system=system,
            tools=TOOLS,
            output_config={"effort": effort},
            messages=messages,
        )
        u = response.usage
        trial.input_tokens += u.input_tokens
        trial.output_tokens += u.output_tokens
        trial.cache_read_tokens += getattr(u, "cache_read_input_tokens", 0) or 0
        if response.stop_reason == "refusal":
            trial.stop = "refusal"
            break
        tool_uses = [b for b in response.content if b.type == "tool_use"]
        if response.stop_reason != "tool_use" or not tool_uses:
            trial.stop = response.stop_reason or "end_turn"
            break
        messages.append({"role": "assistant", "content": response.content})
        results = []
        done = False
        for block in tool_uses:
            if block.name == "submit":
                try:
                    trial.submitted = json.loads(block.input.get("answer_json", "null"))
                except json.JSONDecodeError:
                    trial.submitted = block.input.get("answer_json")
                trial.success = equal(task["kind"], normalize_answer(task["kind"], trial.submitted), truth)
                trial.stop = "submit"
                results.append({"type": "tool_result", "tool_use_id": block.id, "content": "recorded"})
                done = True
                continue
            source = block.input.get("query", "")
            try:
                params = json.loads(block.input.get("params_json") or "{}")
            except json.JSONDecodeError:
                params = {}
            ok, payload = graph.query(source, params)
            record = {"turn": turn + 1, "query": source, "params": params, "ok": ok}
            if ok:
                rows = payload["rows"]  # type: ignore[index]
                record["rows"] = len(rows)
                record["correct"] = equal(task["kind"], normalize(task["kind"], rows), truth)
                if record["correct"] and trial.turns_to_correct is None:
                    trial.turns_to_correct = len(trial.queries) + 1
                shown = rows[:MAX_ROWS_RETURNED]
                content = json.dumps({"row_count": len(rows), "rows": shown,
                                      "truncated": len(rows) > MAX_ROWS_RETURNED})
                results.append({"type": "tool_result", "tool_use_id": block.id, "content": content})
            else:
                record["error"] = str(payload)[:2000]
                record["codes"] = sorted(set(ERROR_CODE.findall(str(payload))))
                results.append({"type": "tool_result", "tool_use_id": block.id,
                                "content": str(payload)[:4000], "is_error": True})
            if trial.first_try_valid is None:
                trial.first_try_valid = ok
            trial.queries.append(record)
        messages.append({"role": "user", "content": results})
        if done:
            break
    else:
        trial.stop = "max_turns"
    trial.wall_s = time.time() - started
    return trial


def cmd_run(args) -> None:
    import anthropic

    graph = Graph(args.server, args.graph, args.snapshot)
    tasks = yaml.safe_load(Path(args.tasks).read_text())["tasks"]
    truth_path = Path(args.truth)
    truth = json.loads(truth_path.read_text())
    if truth.get("snapshot") != args.snapshot:
        raise SystemExit(f"truth was computed at {truth.get('snapshot')}, not {args.snapshot}; rerun `truth`")
    card = (HERE / "card.md").read_text()
    schema = schema_text(graph)
    system = [{"type": "text", "text": SYSTEM_TEMPLATE.format(card=card, schema=schema),
               "cache_control": {"type": "ephemeral"}}]
    client = anthropic.Anthropic()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    out = out_dir / f"{stamp}-{args.model}-{args.effort}.jsonl"
    selected = [t for t in tasks if not args.only or t["id"] in args.only]
    with out.open("w") as fh:
        fh.write(json.dumps({"meta": {"model": args.model, "effort": args.effort, "snapshot": args.snapshot,
                                      "card_sha": _sha(card), "schema_sha": _sha(schema),
                                      "max_turns": args.max_turns, "started": stamp}}) + "\n")
        for task in selected:
            for rep in range(args.repeats):
                trial = run_task(client, args.model, args.effort, system, task, truth["answers"][task["id"]],
                                 graph, args.max_turns)
                rec = {"task": trial.task, "rep": rep, "model": trial.model, "success": trial.success,
                       "first_try_valid": trial.first_try_valid, "turns_to_correct": trial.turns_to_correct,
                       "n_queries": len(trial.queries), "n_errors": sum(1 for q in trial.queries if not q["ok"]),
                       "codes": sorted({c for q in trial.queries for c in q.get("codes", [])}),
                       "stop": trial.stop, "input_tokens": trial.input_tokens, "output_tokens": trial.output_tokens,
                       "cache_read_tokens": trial.cache_read_tokens, "wall_s": round(trial.wall_s, 1),
                       "submitted": trial.submitted, "queries": trial.queries}
                fh.write(json.dumps(rec) + "\n")
                fh.flush()
                print(f"{task['id']:<28} ok={trial.success!s:<5} first_try={trial.first_try_valid!s:<5} "
                      f"turns_to_correct={trial.turns_to_correct} queries={len(trial.queries)} "
                      f"errors={rec['n_errors']} codes={rec['codes']} stop={trial.stop}")
    print(f"wrote {out}")
    report(out)


def _sha(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode()).hexdigest()[:12]


def cmd_truth(args) -> None:
    graph = Graph(args.server, args.graph, args.snapshot)
    tasks = yaml.safe_load(Path(args.tasks).read_text())["tasks"]
    answers = compute_truth(tasks, graph)
    Path(args.truth).write_text(json.dumps({"snapshot": args.snapshot, "answers": answers}, indent=1) + "\n")
    print(f"wrote {args.truth} for {len(answers)} tasks at {args.snapshot}")


def report(path: Path) -> None:
    rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    meta = rows[0]["meta"]
    trials = rows[1:]
    n = len(trials)
    if not n:
        print("no trials")
        return
    ftv = [t["first_try_valid"] for t in trials if t["first_try_valid"] is not None]
    ttc = [t["turns_to_correct"] for t in trials if t["turns_to_correct"]]
    codes: dict[str, int] = {}
    for t in trials:
        for c in t["codes"]:
            codes[c] = codes.get(c, 0) + 1
    print(f"\nmodel={meta['model']} effort={meta['effort']} snapshot={meta['snapshot']} trials={n}")
    print(f"  task success        {sum(t['success'] for t in trials)}/{n}")
    print(f"  first-try validity  {sum(ftv)}/{len(ftv)}")
    print(f"  turns to correct    mean {sum(ttc)/len(ttc):.2f} over {len(ttc)} solved (never: {n-len(ttc)})")
    print(f"  queries per task    mean {sum(t['n_queries'] for t in trials)/n:.2f}")
    print(f"  errors per task     mean {sum(t['n_errors'] for t in trials)/n:.2f}; by code {codes}")
    print(f"  tokens per task     in {sum(t['input_tokens'] for t in trials)//n} out {sum(t['output_tokens'] for t in trials)//n} "
          f"(cache read {sum(t['cache_read_tokens'] for t in trials)//n})")
    print(f"  wall per task       {sum(t['wall_s'] for t in trials)/n:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("truth", "run"):
        p = sub.add_parser(name)
        p.add_argument("--tasks", default=str(HERE / "tasks.yaml"))
        p.add_argument("--truth", default=str(HERE / "truth.json"))
        p.add_argument("--server", default="personal")
        p.add_argument("--graph", default="personal")
        p.add_argument("--snapshot", required=True, help="graph_commit_id every query is pinned to")
        p.set_defaults(func=cmd_truth if name == "truth" else cmd_run)
    run = sub.choices["run"]
    run.add_argument("--model", default="claude-opus-5")
    run.add_argument("--effort", default="high", choices=["low", "medium", "high", "xhigh", "max"])
    run.add_argument("--max-turns", type=int, default=8)
    run.add_argument("--repeats", type=int, default=1)
    run.add_argument("--only", nargs="*", default=None, help="task ids to run")
    run.add_argument("--out", default=str(HERE / "results"))
    rp = sub.add_parser("report")
    rp.add_argument("path")
    rp.set_defaults(func=lambda a: report(Path(a.path)))
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
