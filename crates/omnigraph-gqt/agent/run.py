#!/usr/bin/env python3
"""Run the GQ agent instrument: a headless agent answers every task, and
`omnigraph-gqt-agent` judges every answer. See README.md beside this file.

    python3 crates/omnigraph-gqt/agent/run.py run --editions 2,3 --runs 3
    python3 crates/omnigraph-gqt/agent/run.py summarize crates/omnigraph-gqt/agent/results/<dir>

Standard library only. Every agent call is a fresh `claude -p` process in an
empty directory with no tools and no customizations; no session is resumed.
The model comes from manifest.json, and a call that reports any other model
stops the run.
"""

import argparse
import concurrent.futures as futures
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
DEFAULT_BIN = os.path.join(ROOT, "target", "debug", "omnigraph-gqt-agent")

FIRST = """You are writing GQ, the query language of the Omnigraph graph database. The reference card below is the only GQ documentation available; follow it exactly.

<reference_card>
{card}
</reference_card>

The graph's schema:

```pg
{schema}
```

Task: {intent}{signature}

{rule}"""

REPAIR_WITH_CARD = """You are writing GQ, the query language of the Omnigraph graph database. The reference card below is the only GQ documentation available; follow it exactly.

<reference_card>
{card}
</reference_card>

The graph's schema:

```pg
{schema}
```

Task: {intent}{signature}

Your previous answer was:

```gq
{answer}
```

The GQ compiler refused it with this diagnostic:

```json
{diagnostic}
```

{rule}"""

# The diagnostics measured alone: no card and no schema, only the task, the
# refused text and what the compiler said about it. The seeded condition uses
# the same prompt over a ground truth with one injected error.
REPAIR_BARE = """You are writing GQ, the query language of the Omnigraph graph database.

Task: {intent}{signature}

Your previous answer was:

```gq
{answer}
```

The GQ compiler refused it with this diagnostic:

```json
{diagnostic}
```

{rule}"""

RULE_ONE = "Reply with exactly one GQ query in a single fenced ```gq code block and nothing else."
RULE_FEWEST = (
    "Reply with GQ only, in fenced ```gq code blocks and nothing else. Answer with a single query if the "
    "language can express the task; if it cannot, write the fewest queries that together answer it, each in "
    "its own block in the order they run, with a `//` comment saying what the caller does with the results."
)
RULE_REPAIR_ONE = "Reply with the corrected query in a single fenced ```gq code block and nothing else."
RULE_REPAIR_FEWEST = "Reply with the corrected GQ only, in fenced ```gq code blocks and nothing else."

EDITION_LINE = re.compile(r"^[ \t]*gq[ \t]+\d+[ \t]*;[ \t]*\n?", re.M)
FENCE = re.compile(r"```[ \t]*([A-Za-z0-9_-]*)[ \t]*\n(.*?)```", re.S)

CONDITIONS = ("first", "repair_card", "repair_bare", "seeded")


class Refusal(Exception):
    """A condition under which the run must not continue."""


def tool(bin_path, *args, stdin=None):
    p = subprocess.run([bin_path, "--root", HERE, *args], input=stdin, capture_output=True, text=True)
    if p.returncode != 0:
        raise Refusal(f"omnigraph-gqt-agent {' '.join(args)} failed:\n{p.stderr.strip()}")
    return p.stdout


def json_lines(text):
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def sha256(text):
    return hashlib.sha256(text.encode()).hexdigest()


def git_head():
    p = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
    return p.stdout.strip() if p.returncode == 0 else None


class Agent:
    """One fresh headless `claude -p` call per prompt."""

    def __init__(self, model, claude):
        self.model = model
        self.claude = claude
        self.workdir = tempfile.mkdtemp(prefix="gq-agent-")  # empty: the agent sees only the prompt
        self.stop = threading.Event()

    def version(self):
        p = subprocess.run([self.claude, "--version"], capture_output=True, text=True)
        return p.stdout.strip()

    def call(self, prompt):
        if self.stop.is_set():
            raise Refusal("the run was stopped")
        env = {k: v for k, v in os.environ.items()
               if k not in ("CLAUDECODE", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_CHILD_SESSION",
                            "CLAUDE_CODE_ENTRYPOINT")}
        cmd = [self.claude, "-p", prompt, "--output-format", "json", "--model", self.model,
               "--tools", "", "--safe-mode", "--no-session-persistence"]
        last = None
        for attempt in range(3):
            started = time.time()
            p = subprocess.run(cmd, cwd=self.workdir, env=env, stdin=subprocess.DEVNULL,
                               capture_output=True, text=True, timeout=900)
            try:
                out = json.loads(p.stdout)
            except json.JSONDecodeError:
                last = f"non-JSON output: {p.stdout[-500:]} {p.stderr[-500:]}"
                time.sleep(10 * (attempt + 1))
                continue
            if out.get("is_error"):
                last = f"agent error: {out.get('result')}"
                time.sleep(15 * (attempt + 1))
                continue
            models = sorted(out.get("modelUsage", {}).keys())
            if models != [self.model]:
                self.stop.set()
                raise Refusal(f"the agent reported models {models}, not the pinned {self.model}; "
                              "the run is refused")
            usage = out.get("usage", {})
            return {
                "result": out.get("result", ""),
                "models": models,
                "tokens_in": usage.get("input_tokens", 0) + usage.get("cache_creation_input_tokens", 0)
                + usage.get("cache_read_input_tokens", 0),
                "tokens_out": usage.get("output_tokens", 0),
                "cost_usd": out.get("total_cost_usd"),
                "seconds": round(time.time() - started, 1),
                "session_id": out.get("session_id"),
                "transport_retries": attempt,
            }
        raise RuntimeError(f"the agent call failed three times: {last}")


def extract(result, edition):
    """The answer's GQ: every fenced block in order, joined; edition 3 gets
    exactly one edition line at the top, whatever the agent wrote."""
    blocks = [body for _, body in FENCE.findall(result)]
    fenced = bool(blocks)
    if not blocks:
        blocks = [result]
    wrote = [m.group(0).strip() for b in blocks for m in EDITION_LINE.finditer(b)]
    if edition == 3:
        body = "\n\n".join(EDITION_LINE.sub("", b).strip("\n") for b in blocks)
        text = "gq 3;\n" + body.strip("\n") + "\n"
    else:
        text = "\n\n".join(b.strip("\n") for b in blocks) + "\n"
    return {"text": text, "blocks": len(blocks), "fenced": fenced, "edition_lines_written": wrote}


def bare_answer(text):
    return EDITION_LINE.sub("", text, count=1).rstrip("\n") if text.startswith("gq ") else text.rstrip("\n")


def judge(bin_path, edition, jobs):
    if not jobs:
        return {}
    stdin = "\n".join(json.dumps({"id": i, "task": t, "text": x}) for i, (t, x) in enumerate(jobs)) + "\n"
    rows = json_lines(tool(bin_path, "judge", "--edition", str(edition), stdin=stdin))
    return {row["id"]: row for row in rows}


def signature(task):
    """The parameters the task's query takes, stated so the agent does not
    guess a type the typed IR depends on."""
    return f"\n\nThe task's parameters, as the query declares them: `{task['signature']}`." if task["signature"] else ""


def rule(task, repair):
    if task["answer"] == "fewest":
        return RULE_REPAIR_FEWEST if repair else RULE_FEWEST
    return RULE_REPAIR_ONE if repair else RULE_ONE


class Writer:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()

    def write(self, record):
        with self.lock, open(self.path, "a") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")


def key(record):
    return (record["edition"], record["run"], record["condition"], record.get("seed") or record["task"])


def run_edition(args, agent, bin_path, edition, run, tasks, seeds, card, writer, done):
    by_id = {t["id"]: t for t in tasks}

    def ask(prompt):
        reply = agent.call(prompt)
        return reply, extract(reply["result"], edition)

    def base(condition, task, prompt, reply, answer):
        return {"edition": edition, "run": run, "condition": condition, "task": task["id"],
                "set": task["set"], "dataset": task["dataset"], "kind": task["kind"],
                "edition2_queries": task["edition2_queries"],
                "prompt_sha256": sha256(prompt),
                "agent": {k: v for k, v in reply.items() if k != "result"}, "raw": reply["result"],
                "answer": answer}

    def parallel(work):
        out = []
        with futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            pending = {pool.submit(fn): label for label, fn in work}
            for f in futures.as_completed(pending):
                out.append(f.result())
        return out

    # First try.
    todo = [t for t in tasks if (edition, run, "first", t["id"]) not in done]

    def first(task):
        prompt = FIRST.format(card=card, schema=task["schema"].rstrip("\n"), intent=task["intent"], signature=signature(task),
                              rule=rule(task, False))
        reply, answer = ask(prompt)
        return base("first", task, prompt, reply, answer)

    firsts = parallel([(t["id"], (lambda t=t: first(t))) for t in todo])
    verdicts = judge(bin_path, edition, [(r["task"], r["answer"]["text"]) for r in firsts])
    for i, record in enumerate(firsts):
        record["judge"] = verdicts[i]
        writer.write(record)
        done[key(record)] = record
        print(f"e{edition} r{run} first {record['task']}: accepted={record['judge'].get('accepted')} "
              f"correct={record['judge'].get('correct')}", flush=True)

    # Repairs of refused first answers, with and without the card.
    work = []
    for task in tasks:
        first_record = done.get((edition, run, "first", task["id"]))
        if not first_record or first_record["judge"].get("accepted") is not False:
            continue
        for condition in ("repair_card", "repair_bare"):
            if (edition, run, condition, task["id"]) in done:
                continue
            template = REPAIR_WITH_CARD if condition == "repair_card" else REPAIR_BARE
            prompt = template.format(card=card, schema=task["schema"].rstrip("\n"), intent=task["intent"], signature=signature(task),
                                     answer=bare_answer(first_record["answer"]["text"]),
                                     diagnostic=first_record["judge"]["rendered"], rule=rule(task, True))

            def repair(task=task, condition=condition, prompt=prompt):
                reply, answer = ask(prompt)
                return base(condition, task, prompt, reply, answer)
            work.append((task["id"], repair))
    repairs = parallel(work)
    verdicts = judge(bin_path, edition, [(r["task"], r["answer"]["text"]) for r in repairs])
    for i, record in enumerate(repairs):
        record["judge"] = verdicts[i]
        writer.write(record)
        done[key(record)] = record

    # Seeded repairs: a ground truth with one injected error, the diagnostic alone.
    seed_todo = [s for s in seeds if (edition, run, "seeded", s["id"]) not in done]
    seed_verdicts = judge(bin_path, edition, [(s["task"], s["text"]) for s in seed_todo])
    work = []
    for i, seed in enumerate(seed_todo):
        verdict = seed_verdicts[i]
        if verdict.get("accepted") is not False:
            raise Refusal(f"seed {seed['id']} is not refused by the front door: {verdict}")
        task = by_id[seed["task"]]
        prompt = REPAIR_BARE.format(intent=task["intent"], signature=signature(task), answer=bare_answer(seed["text"]),
                                    diagnostic=verdict["rendered"], rule=rule(task, True))

        def seeded(seed=seed, task=task, prompt=prompt, verdict=verdict):
            reply, answer = ask(prompt)
            record = base("seeded", task, prompt, reply, answer)
            record.update({"seed": seed["id"], "class": seed["class"], "seed_code": verdict.get("diagnostic", {}).get("code")})
            return record
        work.append((seed["id"], seeded))
    seeded_records = parallel(work)
    verdicts = judge(bin_path, edition, [(r["task"], r["answer"]["text"]) for r in seeded_records])
    for i, record in enumerate(seeded_records):
        record["judge"] = verdicts[i]
        writer.write(record)
        done[key(record)] = record


def cmd_run(args):
    bin_path = args.agent_bin
    if args.build:
        subprocess.run(["cargo", "build", "--locked", "-p", "omnigraph-gqt", "--bin", "omnigraph-gqt-agent"],
                       cwd=ROOT, check=True)
    if not os.path.exists(bin_path):
        raise Refusal(f"{bin_path} is missing: build it with "
                      "`cargo build --locked -p omnigraph-gqt --bin omnigraph-gqt-agent`, or pass --build")
    if not shutil.which(args.claude):
        raise Refusal(f"`{args.claude}` is not on PATH")
    tool(bin_path, "cards", "--check")  # the cards match their pins and the skill
    manifest = json.load(open(os.path.join(HERE, "manifest.json")))
    editions = [int(e) for e in args.editions.split(",")]
    if not set(editions) <= {2, 3}:
        raise Refusal(f"editions are 2 and 3, not {args.editions}")
    tasks = json_lines(tool(bin_path, "tasks"))
    if args.only:
        keep = set(args.only.split(","))
        tasks = [t for t in tasks if t["id"] in keep]
    sets = set(args.sets.split(","))
    tasks = [t for t in tasks if t["set"] in sets]
    agent = Agent(manifest["model"], args.claude)

    label = args.label or datetime.date.today().isoformat()
    out = args.out or os.path.join(HERE, "results", label)
    records_path = os.path.join(out, "records.jsonl")
    done = {}
    if os.path.exists(out):
        if not args.resume:
            raise Refusal(f"{out} exists; pass --resume to continue it or --label for a new directory")
        for record in json_lines(open(records_path).read()) if os.path.exists(records_path) else []:
            done[key(record)] = record
    os.makedirs(out, exist_ok=True)
    meta = {
        "started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "git_head": git_head(),
        "model": manifest["model"],
        "cards": manifest["cards"],
        "claude": agent.version(),
        "editions": editions,
        "runs": args.runs,
        "sets": sorted(sets),
        "tasks": len(tasks),
        "runner_sha256": sha256(open(__file__).read()),
    }
    with open(os.path.join(out, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
        f.write("\n")
    writer = Writer(records_path)
    for edition in editions:
        card = open(os.path.join(HERE, "cards", f"edition{edition}.md")).read()
        seeds = json_lines(tool(bin_path, "seeds", "--edition", str(edition)))
        if args.only:
            seeds = [s for s in seeds if s["task"] in {t["id"] for t in tasks}]
        for run in range(1, args.runs + 1):
            run_edition(args, agent, bin_path, edition, run, tasks, seeds, card, writer, done)
    summarize(out)


# ---------------------------------------------------------------- summary

def ratio(n, d):
    return f"{n}/{d}" if d else "-"


def summarize(out):
    records = json_lines(open(os.path.join(out, "records.jsonl")).read())
    meta = json.load(open(os.path.join(out, "meta.json")))
    by = {}
    for r in records:
        by.setdefault((r["edition"], r["run"]), []).append(r)
    rows = []
    numbers = {}
    for (edition, run), recs in sorted(by.items()):
        first = {r["task"]: r for r in recs if r["condition"] == "first"}
        repair_card = {r["task"]: r for r in recs if r["condition"] == "repair_card"}
        repair_bare = {r["task"]: r for r in recs if r["condition"] == "repair_bare"}
        seeded = [r for r in recs if r["condition"] == "seeded"]
        for subset in ("tasks", "corpus"):
            fs = [r for r in first.values() if r["set"] == subset]
            if not fs:
                continue
            accepted = [r for r in fs if r["judge"].get("accepted") is True]
            judged = [r for r in fs if r["judge"].get("correct") is not None]
            correct = [r for r in judged if r["judge"]["correct"] is True]
            shared = [r for r in judged if r["edition2_queries"] == 1]
            shared_correct = [r for r in shared if r["judge"]["correct"] is True]
            unjudged = [r for r in accepted if r["judge"].get("correct") is None]
            refused = [r for r in fs if r["judge"].get("accepted") is False]
            rc = [repair_card[r["task"]] for r in refused if r["task"] in repair_card]
            rb = [repair_bare[r["task"]] for r in refused if r["task"] in repair_bare]
            final = [repair_card.get(r["task"], r) for r in fs]
            final_correct = [r for r in final if r["judge"].get("correct") is True]
            final_judged = [r for r in final if r["judge"].get("correct") is not None]
            multi = [r for r in accepted if (r["judge"].get("queries") or 0) > 1]
            tok_in = sum(r["agent"]["tokens_in"] for r in fs) / len(fs)
            tok_out = sum(r["agent"]["tokens_out"] for r in fs) / len(fs)
            row = {
                "edition": edition, "run": run, "set": subset,
                "first_accepted": ratio(len(accepted), len(fs)),
                "first_correct": ratio(len(correct), len(judged)),
                "first_correct_shared": ratio(len(shared_correct), len(shared)),
                "unjudged": len(unjudged),
                "multi_query": len(multi),
                "repair_card_accepted": ratio(sum(1 for r in rc if r["judge"].get("accepted")), len(rc)),
                "repair_bare_accepted": ratio(sum(1 for r in rb if r["judge"].get("accepted")), len(rb)),
                "final_correct": ratio(len(final_correct), len(final_judged)),
                "tokens_in": round(tok_in), "tokens_out": round(tok_out),
            }
            rows.append(row)
            numbers[f"e{edition}-r{run}-{subset}"] = row
        if seeded:
            numbers[f"e{edition}-r{run}-seeded"] = {
                "accepted": ratio(sum(1 for r in seeded if r["judge"].get("accepted")), len(seeded)),
                "correct": ratio(sum(1 for r in seeded if r["judge"].get("correct") is True),
                                 sum(1 for r in seeded if r["judge"].get("correct") is not None)),
            }
    lines = [
        f"# Agent instrument results: {os.path.basename(out)}",
        "",
        f"Model `{meta['model']}`, {meta['claude']}, git `{meta['git_head']}`, started {meta['started']}.",
        "Card digests: " + ", ".join(f"`{k}` `{v[:12]}`" for k, v in sorted(meta["cards"].items())) + ".",
        "README.md beside the runner defines every column. Not a CI gate.",
        "",
        "| Edition | Run | Set | First try accepted | First try correct | Correct, shared tasks | Unjudged "
        "| Several queries | Repair with card accepted | Repair without card accepted | Correct after repair "
        "| Tokens in | Tokens out |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(f"| {r['edition']} | {r['run']} | {r['set']} | {r['first_accepted']} | {r['first_correct']} "
                     f"| {r['first_correct_shared']} | {r['unjudged']} | {r['multi_query']} | {r['repair_card_accepted']} "
                     f"| {r['repair_bare_accepted']} | {r['final_correct']} | {r['tokens_in']} | {r['tokens_out']} |")
    seeded_rows = [(k, v) for k, v in sorted(numbers.items()) if k.endswith("-seeded")]
    if seeded_rows:
        lines += ["", "| Seeded repairs | Accepted | Correct |", "|---|---|---|"]
        for k, v in seeded_rows:
            lines.append(f"| {k[:-7]} | {v['accepted']} | {v['correct']} |")
    analysis = analyse(records)
    lines += analysis["lines"]
    numbers["gate"] = analysis["gate"]
    with open(os.path.join(out, "summary.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(numbers, f, indent=2, sort_keys=True)
        f.write("\n")
    print("\n".join(lines))


def spread(values):
    if not values:
        return "-"
    lo, hi = min(values), max(values)
    mean = sum(values) / len(values)
    return f"{lo}" if lo == hi else f"{lo}–{hi} (mean {mean:.1f})"


def analyse(records):
    """The cross-run sections: spread over runs, the RFC gate on the shared
    tasks, gains and losses by task, and the codes behind every repair."""
    editions = sorted({r["edition"] for r in records})
    runs = sorted({r["run"] for r in records})
    first = {(r["edition"], r["run"], r["task"]): r for r in records if r["condition"] == "first"}
    tasks = {}
    for r in records:
        tasks.setdefault(r["task"], r)
    lines = ["", "## Spread over runs", "",
             "Counts per run; a range is the lowest and highest run. Shared tasks are those edition 2 "
             "expresses in one query, where both editions are judged automatically.", "",
             "| Edition | Set | Tasks | First try accepted | First try correct | Shared: accepted | Shared: correct "
             "| Refused first | Repaired with card | Repaired without card | Seeded accepted | Seeded correct "
             "| Tokens in | Tokens out |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    per_run = {}
    for edition in editions:
        for subset in ("tasks", "corpus", "all"):
            cols = {k: [] for k in ("n", "acc", "cor", "sacc", "scor", "ref", "rc", "rb", "sa", "sc", "ti", "to", "sn")}
            for run in runs:
                fs = [r for (e, n, _), r in first.items() if e == edition and n == run
                      and (subset == "all" or r["set"] == subset)]
                if not fs:
                    continue
                ids = {r["task"] for r in fs}
                rep = [r for r in records if r["edition"] == edition and r["run"] == run and r["task"] in ids]
                shared = [r for r in fs if r["edition2_queries"] == 1]
                seeded = [r for r in records if r["edition"] == edition and r["run"] == run
                          and r["condition"] == "seeded"] if subset == "all" else []
                cols["n"].append(len(fs))
                cols["acc"].append(sum(1 for r in fs if r["judge"].get("accepted") is True))
                cols["cor"].append(sum(1 for r in fs if r["judge"].get("correct") is True))
                cols["sn"].append(len(shared))
                cols["sacc"].append(sum(1 for r in shared if r["judge"].get("accepted") is True))
                cols["scor"].append(sum(1 for r in shared if r["judge"].get("correct") is True))
                cols["ref"].append(sum(1 for r in fs if r["judge"].get("accepted") is False))
                cols["rc"].append(sum(1 for r in rep if r["condition"] == "repair_card" and r["judge"].get("accepted")))
                cols["rb"].append(sum(1 for r in rep if r["condition"] == "repair_bare" and r["judge"].get("accepted")))
                if seeded:
                    cols["sa"].append(sum(1 for r in seeded if r["judge"].get("accepted")))
                    cols["sc"].append(sum(1 for r in seeded if r["judge"].get("correct") is True))
                cols["ti"].append(round(sum(r["agent"]["tokens_in"] for r in fs) / len(fs)))
                cols["to"].append(round(sum(r["agent"]["tokens_out"] for r in fs) / len(fs)))
            if not cols["n"]:
                continue
            per_run[(edition, subset)] = cols
            seeds_n = len({r["seed"] for r in records if r["edition"] == edition and r["condition"] == "seeded"})
            lines.append(
                f"| {edition} | {subset} | {cols['n'][0]} | {spread(cols['acc'])} | {spread(cols['cor'])} "
                f"| {spread(cols['sacc'])} of {cols['sn'][0]} | {spread(cols['scor'])} of {cols['sn'][0]} "
                f"| {spread(cols['ref'])} | {spread(cols['rc'])} | {spread(cols['rb'])} "
                f"| {spread(cols['sa']) + (f' of {seeds_n}' if cols['sa'] else '')} "
                f"| {spread(cols['sc']) + (f' of {seeds_n}' if cols['sc'] else '')} "
                f"| {spread(cols['ti'])} | {spread(cols['to'])} |")

    gate = {}
    if (2, "all") in per_run and (3, "all") in per_run:
        e2, e3 = per_run[(2, "all")], per_run[(3, "all")]
        mean = lambda xs: sum(xs) / len(xs)
        acc_ok = mean(e3["sacc"]) >= mean(e2["sacc"])
        cor_ok = mean(e3["scor"]) >= mean(e2["scor"])
        gate = {
            "shared_tasks": e2["sn"][0],
            "edition2_accepted": e2["sacc"], "edition3_accepted": e3["sacc"],
            "edition2_correct": e2["scor"], "edition3_correct": e3["scor"],
            "acceptance_at_or_above": acc_ok, "correctness_at_or_above": cor_ok,
            "verdict": "pass" if acc_ok and cor_ok else "fail",
        }
        lines += ["", "## The RFC gate", "",
                  f"On the {e2['sn'][0]} shared tasks, per run: edition 3 first-try accepted {e3['sacc']} against "
                  f"edition 2's {e2['sacc']}, correct {e3['scor']} against {e2['scor']}. Acceptance at or above "
                  f"edition 2's: {'yes' if acc_ok else 'no'}; correctness at or above: {'yes' if cor_ok else 'no'}. "
                  f"Verdict: **{gate['verdict']}** (means over the runs)."]

    lines += ["", "## By task", "",
              "First-try correct over the runs, per edition; `n/j` is correct of judged, `-` not judged "
              "automatically (no edition-2 truth, or an edition-2 answer of several queries; `q` is the "
              "most queries an edition-2 answer used). Rows where the editions differ, or where either "
              "edition was refused or wrong at least once.", "",
              "| Task | Kind | Edition 2 | Edition 3 | Difference |", "|---|---|---|---|---|"]
    for tid in sorted(tasks, key=lambda t: (tasks[t]["set"], t)):
        cells, scores = [], {}
        for edition in (2, 3):
            rs = [first[(edition, run, tid)] for run in runs if (edition, run, tid) in first]
            judged = [r for r in rs if r["judge"].get("correct") is not None]
            ok = sum(1 for r in judged if r["judge"]["correct"] is True)
            refused = sum(1 for r in rs if r["judge"].get("accepted") is False)
            most = max((r["judge"].get("queries") or 0 for r in rs), default=0)
            cell = f"{ok}/{len(judged)}" if judged else "-"
            if refused:
                cell += f", {refused} refused"
            if edition == 2 and most > 1:
                cell += f", q{most}"
            cells.append(cell if rs else "")
            scores[edition] = (ok, len(judged), refused, len(rs))
        (o2, j2, f2, n2), (o3, j3, f3, n3) = scores[2], scores[3]
        if j2 and j3:
            diff = "gained" if o3 / j3 > o2 / j2 else "lost" if o3 / j3 < o2 / j2 else ""
        else:
            diff = ""
        imperfect = (j2 and o2 < j2) or (j3 and o3 < j3) or f2 or f3
        if diff or imperfect:
            lines.append(f"| {tid} | {tasks[tid]['kind']} | {cells[0]} | {cells[1]} | {diff} |")

    lines += ["", "## Codes behind every repair", "",
              "Every refused first answer, the code it was refused with, and whether one repair turn was "
              "accepted with the card and without it; then the seeded answers by code.", "",
              "| Edition | Run | Task | Code | With card | Without card |", "|---|---|---|---|---|---|"]
    repairs = {(r["edition"], r["run"], r["task"], r["condition"]): r for r in records
               if r["condition"] in ("repair_card", "repair_bare")}
    for (edition, run, tid), r in sorted(first.items()):
        if r["judge"].get("accepted") is not False:
            continue
        code = (r["judge"].get("diagnostic") or {}).get("code") or "none"
        def verdict(cond):
            rr = repairs.get((edition, run, tid, cond))
            if not rr:
                return ""
            j = rr["judge"]
            if not j.get("accepted"):
                return f"refused ({(j.get('diagnostic') or {}).get('code') or 'none'})"
            return {True: "accepted, correct", False: "accepted, wrong", None: "accepted"}[j.get("correct")]
        lines.append(f"| {edition} | {run} | {tid} | {code} | {verdict('repair_card')} | {verdict('repair_bare')} |")
    lines += ["", "| Edition | Seed code | Class | Accepted | Correct |", "|---|---|---|---|---|"]
    seeded = {}
    for r in records:
        if r["condition"] == "seeded":
            seeded.setdefault((r["edition"], r.get("seed_code") or "none", r["class"]), []).append(r)
    for (edition, code, cls), rs in sorted(seeded.items()):
        lines.append(f"| {edition} | {code} | {cls} | {sum(1 for r in rs if r['judge'].get('accepted'))}/{len(rs)} "
                     f"| {sum(1 for r in rs if r['judge'].get('correct') is True)}/{len(rs)} |")
    return {"lines": lines, "gate": gate}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="ask the agent every task and judge every answer")
    r.add_argument("--editions", default="2,3", help="comma-separated: 2, 3 or both")
    r.add_argument("--runs", type=int, default=3)
    r.add_argument("--sets", default="tasks,corpus", help="comma-separated: tasks, corpus or both")
    r.add_argument("--only", default="", help="comma-separated task ids")
    r.add_argument("--workers", type=int, default=5)
    r.add_argument("--label", default="", help="the results directory's name; default today's date")
    r.add_argument("--out", default="", help="the results directory; default results/<label>")
    r.add_argument("--resume", action="store_true", help="continue an existing results directory")
    r.add_argument("--agent-bin", default=DEFAULT_BIN)
    r.add_argument("--build", action="store_true", help="build omnigraph-gqt-agent first")
    r.add_argument("--claude", default="claude")
    s = sub.add_parser("summarize", help="rewrite summary.md and summary.json of a results directory")
    s.add_argument("dir")
    args = ap.parse_args()
    try:
        if args.command == "run":
            cmd_run(args)
        else:
            summarize(args.dir)
    except Refusal as e:
        print(f"refused: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
