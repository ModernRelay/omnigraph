#!/usr/bin/env python3
"""Every required context must report on the merge queue's branch.

Branch protection requires the contexts listed in `.github/branch-protection.json`
on the merge queue's temporary branch as well as on the pull request. A
workflow that owns one of them and does not list `merge_group` under `on:`
never reports there, and the queue holds every entry until its timeout removes
it, with no red check anywhere. This check maps each required context to the
workflow whose job carries that display name (the job id when a job has no
`name:`) and refuses: a context no job names; a context two jobs or two
workflows name; an owning workflow whose `on:` mapping (block or flow form,
quoted keys accepted, comments ignored, only the mapping's own event keys
counted) lacks `merge_group`, or whose `merge_group` carries a `branches:` /
`branches-ignore:` filter that, evaluated as GitHub's ordered glob patterns,
does not admit `main`; and an owning job whose condition reads the event
(`github.event_name`, `github.event`, `github.ref`, `github.ref_name`,
`github.ref_type`, `github.head_ref`, `github.base_ref`) or is the literal
`false` without comparing `github.event_name` to `'merge_group'`, which would
report `skipped` on the queue where the pull request run had been a real gate
(a condition that compares the event name to `merge_group` is a decision,
admitting or excluding, and passes). Scalars are decoded first: quotes, block
scalars (`>`, `|`, with indicators), plain multi-line continuations, and
flow-form jobs. Not covered: a job skipped through a `needs:` on a skipped
job, a step-level `if:` inside an owning job, `paths:` filters, and anchors or
aliases.

The second half pins the `CI Gate` wiring (docs/dev/branch-protection.md): the
policy's contexts are exactly the four in `OWNERS`, each owned by the named
job of the named workflow; `ci.yml`'s `ci_gate` job runs under `if: always()`
with no `continue-on-error`, its `needs:` equals the job table in
`scripts/ci_gate.py`, every needed job uses a block mapping without
`continue-on-error`, and its
steps are exactly a pinned unconditional checkout and an unconditional
`python3 scripts/ci_gate.py` step with explicit `shell: bash` and the four
expected `env:` values;
and `workflow_action_pins`, the job that runs this check, has no condition, no
dependencies and no error suppression, and runs this script and the gate's
self-test as unconditional steps. Those last checks exist because a gate
without `always()` reports `skipped` when a dependency fails, and GitHub
counts a skip as green: the validator's own context stays required so its red
cannot be hidden by the gate it checks. Run from the repository root;
`--self-test` runs the in-memory refusal and acceptance cases first.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / ".github" / "workflows"
POLICY = REPO_ROOT / ".github" / "branch-protection.json"
CI_WORKFLOW = ".github/workflows/ci.yml"
GATE_JOB = "ci_gate"
GATE_CONTEXT = "CI Gate"
VALIDATOR_JOB = "workflow_action_pins"
VALIDATOR_CONTEXT = "Check Workflow Action Pins"
OWNERS = {
    GATE_CONTEXT: (CI_WORKFLOW, GATE_JOB),
    VALIDATOR_CONTEXT: (CI_WORKFLOW, VALIDATOR_JOB),
    "GQ Logic Tests": (".github/workflows/gq-logic-tests.yml", "gq_logic_tests"),
    "Fix Regression Gate": (".github/workflows/fix-regression-gate.yml", "fix_regression_gate"),
}
GATE_COMMAND = "python3 scripts/ci_gate.py"
GATE_ENV = {
    "NEEDS_JSON": "${{ toJSON(needs) }}",
    "EVENT_NAME": "${{ github.event_name }}",
    "EVENT_REF": "${{ github.ref }}",
    "RUN_FULL_CI": "${{ needs.classify_changes.outputs.run_full_ci }}",
}
VALIDATOR_COMMANDS = ("python3 scripts/check-merge-group-triggers.py", "python3 scripts/ci_gate.py --self-test")
PINNED_CHECKOUT = re.compile(r"^actions/checkout@[0-9a-f]{40}$")
KEY_LINE = re.compile(r"^(\s*)[\"']?([\w-]+)[\"']?:(?:[ \t]+(.*))?$")
EVENT_READ = re.compile(r"github\.(?:event_name|event\b|ref\b|ref_name|ref_type|head_ref|base_ref)")
MERGE_GROUP_DECISION = re.compile(r"github\.event_name\s*[!=]=\s*[\"']merge_group[\"']")
BLOCK_SCALAR = re.compile(r"^[>|][0-9+-]*$")
QUOTE_OPENERS = " \t:[{,"


def strip_comments(text: str) -> list[str]:
    """Lines of the workflow with CRLF folded and unquoted `#` comments removed."""
    lines = []
    for line in text.replace("\r\n", "\n").split("\n"):
        out, quote = [], None
        for i, ch in enumerate(line):
            if quote:
                if ch == quote:
                    quote = None
            elif ch in "\"'" and (i == 0 or line[i - 1] in QUOTE_OPENERS):
                quote = ch
            elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
                break
            out.append(ch)
        lines.append("".join(out).rstrip())
    return lines


def unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def children(lines: list[str], start: int, end: int, level: int | None = None) -> list[tuple[str, str, int, int]]:
    """Direct child keys of lines[start:end]: (key, inline value, first line, end line)."""
    body = [(i, l) for i, l in enumerate(lines[start:end], start) if l.strip()]
    if not body:
        return []
    if level is None:
        level = min(indent(l) for _, l in body)
    found: list[tuple[str, str, int, int]] = []
    for i, l in body:
        m = KEY_LINE.match(l)
        if m and len(m.group(1)) == level:
            found.append((m.group(2), (m.group(3) or "").strip(), i, end))
    for n, (key, inline, first, _) in enumerate(found):
        stop = found[n + 1][2] if n + 1 < len(found) else end
        found[n] = (key, inline, first, stop)
    return found


def scalar(lines: list[str], inline: str, first: int, stop: int) -> str:
    """A key's decoded value: block scalar joined, plain continuation joined, flow value completed."""
    rest = [l.strip() for l in lines[first + 1:stop] if l.strip()]
    if BLOCK_SCALAR.match(inline):
        return " ".join(rest)
    value = inline
    if value[:1] in "[{":
        depth = 0
        for piece in [value, *rest]:
            if piece is not value:
                value += " " + piece
            depth += piece.count("[") + piece.count("{") - piece.count("]") - piece.count("}")
            if depth <= 0:
                break
        return value
    if value and rest and not any(KEY_LINE.match(l) for l in lines[first + 1:stop] if l.strip()):
        value = " ".join([value, *rest])
    return unquote(value)


def flow_items(value: str) -> list[str]:
    """Top-level items of a flow `[...]` list or `{...}` mapping, split on depth-0 commas."""
    inner = value.strip()
    if not inner or inner[0] not in "[{":
        return []
    inner = inner[1:-1] if inner[-1] in "]}" else inner[1:]
    items, buf, depth, quote = [], [], 0, None
    for ch in inner:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "\"'":
            quote = ch
        elif ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    items.append("".join(buf))
    return [i.strip() for i in items if i.strip()]


def flow_mapping(value: str) -> dict[str, str]:
    """Keys → raw values of a flow mapping; a flow list yields its items as keys."""
    out: dict[str, str] = {}
    for item in flow_items(value):
        key, sep, val = item.partition(":")
        out[unquote(key)] = val.strip() if sep else ""
    return out


def flow_list(value: str) -> list[str]:
    return [unquote(i) for i in flow_items(value)] if value[:1] == "[" else [unquote(value)]


def list_values(lines: list[str], inline: str, first: int, stop: int) -> list[str]:
    """Items of a flow list (possibly multi-line) or `- a` lines below the key."""
    if inline:
        return flow_list(scalar(lines, inline, first, stop))
    child, value = next(
        ((i, line.strip()) for i, line in enumerate(lines[first + 1:stop], first + 1) if line.strip()),
        (stop, ""),
    )
    if value.startswith("["):
        return flow_list(scalar(lines, value, child, stop))
    return [unquote(l.strip()[1:]) for l in lines[first + 1:stop] if l.strip().startswith("-")]


def glob_matches(pattern: str, ref: str) -> bool:
    """GitHub filter-pattern match (workflow-syntax cheat sheet): `**` crosses `/`,
    `*` matches zero or more characters but not `/`, `?` and `+` quantify the
    PRECEDING character (zero-or-one, one-or-more), `[...]` is a character
    range, `\\` escapes; a leading quantifier is a literal."""
    regex = ""
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if pattern.startswith("**", i):
            regex += ".*"
            i += 2
        elif ch == "*":
            regex += "[^/]*"
            i += 1
        elif ch in "?+" and regex:
            regex += ch
            i += 1
        elif ch == "[":
            close = pattern.find("]", i + 1)
            if close == -1:
                regex += re.escape(ch)
                i += 1
            else:
                regex += "[" + pattern[i + 1:close].replace("\\", "\\\\") + "]"
                i = close + 1
        elif ch == "\\" and i + 1 < len(pattern):
            regex += re.escape(pattern[i + 1])
            i += 2
        else:
            regex += re.escape(ch)
            i += 1
    try:
        return re.fullmatch(regex, ref) is not None
    except re.error:
        return False


def filters_admit(branches: list[str] | None, ignore: list[str] | None, ref: str = "main") -> bool:
    """Whether ordered `branches:` patterns (last match wins, `!` negates) and `branches-ignore:` admit ref."""
    admitted = True
    if branches is not None:
        admitted = False
        for pattern in branches:
            negated = pattern.startswith("!")
            if glob_matches(pattern[1:] if negated else pattern, ref):
                admitted = not negated
    if ignore is not None and any(glob_matches(p, ref) for p in ignore):
        admitted = False
    return admitted


def has_merge_group_trigger(lines: list[str]) -> tuple[bool, str]:
    """(ok, reason) for the `on:` mapping (`true:` is YAML 1.1's spelling of the same key)."""
    on = [c for c in children(lines, 0, len(lines), 0) if c[0] in ("on", "true")]
    if not on:
        return False, "no on: mapping"
    _, inline, first, stop = on[0]
    branches = ignore = None
    if inline:
        events = flow_mapping(scalar(lines, inline, first, stop))
        if "merge_group" not in events:
            return False, "no merge_group in the on: mapping"
        config = flow_mapping(events["merge_group"]) if events["merge_group"][:1] == "{" else {}
    else:
        match = [c for c in children(lines, first + 1, stop) if c[0] == "merge_group"]
        if not match:
            return False, "no merge_group in the on: mapping"
        _, mg_inline, mg_first, mg_stop = match[0]
        config = flow_mapping(mg_inline) if mg_inline[:1] == "{" else {}
        for sub, value, sfirst, sstop in children(lines, mg_first + 1, mg_stop):
            if sub in ("branches", "branches-ignore"):
                config[sub] = ""
                items = list_values(lines, value, sfirst, sstop)
                branches, ignore = (items, ignore) if sub == "branches" else (branches, items)
    if "branches" in config and config["branches"]:
        branches = flow_list(config["branches"])
    if "branches-ignore" in config and config["branches-ignore"]:
        ignore = flow_list(config["branches-ignore"])
    if not filters_admit(branches, ignore):
        return False, "merge_group has a branch filter that does not admit main"
    return True, ""


def jobs(lines: list[str]) -> list[tuple[str, str, str | None]]:
    """(display name, job id, job-level `if:` expression) for every job of one workflow."""
    top = [c for c in children(lines, 0, len(lines), 0) if c[0] == "jobs"]
    if not top:
        return []
    _, _, first, stop = top[0]
    found: list[tuple[str, str, str | None]] = []
    for key, inline, jfirst, jstop in children(lines, first + 1, stop):
        display, condition = key, None
        if inline[:1] == "{":
            mapping = flow_mapping(scalar(lines, inline, jfirst, jstop))
            display = unquote(mapping.get("name", key)) or key
            condition = unquote(mapping["if"]) if "if" in mapping else None
        else:
            for sub, value, sfirst, sstop in children(lines, jfirst + 1, jstop):
                if sub == "name":
                    display = scalar(lines, value, sfirst, sstop)
                elif sub == "if":
                    condition = scalar(lines, value, sfirst, sstop)
        found.append((display, key, condition))
    return found


def validate(workflows: dict[str, str], contexts: list[str]) -> list[str]:
    failures: list[str] = []
    owners: dict[str, list[tuple[str, str, str | None]]] = {context: [] for context in contexts}
    parsed = {path: strip_comments(text) for path, text in workflows.items()}
    for path, lines in parsed.items():
        for display, key, condition in jobs(lines):
            if display in owners:
                owners[display].append((path, key, condition))
    for context, found in owners.items():
        if not found:
            failures.append(f"required context {context!r} is named by no workflow job")
            continue
        if len(found) > 1:
            where = ", ".join(f"{path}:{key}" for path, key, _ in found)
            failures.append(f"required context {context!r} is named by more than one job: {where}")
            continue
        path, key, expr = found[0]
        ok, reason = has_merge_group_trigger(parsed[path])
        if not ok:
            failures.append(f"{path}: job {key!r} owns {context!r} but {reason}")
        if expr is not None and not MERGE_GROUP_DECISION.search(expr):
            if unquote(expr.strip("${} ")) == "false":
                failures.append(f"{path}: job {key!r} owns {context!r} and is never run (if: false)")
            elif EVENT_READ.search(expr):
                failures.append(f"{path}: job {key!r} owns {context!r} and its condition reads the event without deciding on merge_group")
    return failures


def job_bounds(lines: list[str]) -> dict[str, tuple[int, int]]:
    """Job id → (first line, end line) for every block-form job of one workflow."""
    top = [c for c in children(lines, 0, len(lines), 0) if c[0] == "jobs"]
    if not top:
        return {}
    _, _, first, stop = top[0]
    bounds = {}
    for key, inline, jfirst, jstop in children(lines, first + 1, stop):
        body = next((line for line in lines[jfirst + 1:jstop] if line.strip()), "")
        # A flow mapping may start on the header or on the following line.
        # job_keys only understands block mappings; never partially inspect one.
        if not inline and KEY_LINE.match(body):
            bounds[key] = (jfirst, jstop)
    return bounds


def job_keys(lines: list[str], first: int, stop: int) -> dict[str, tuple[str, int, int]]:
    """Top-level keys of a block-form job → (inline value, first line, end line)."""
    return {key: (inline, kfirst, kstop) for key, inline, kfirst, kstop in children(lines, first + 1, stop)}


def steps(lines: list[str], first: int, stop: int) -> list[dict[str, str]]:
    """Each `- ` item of a `steps:` list as decoded scalars; `env:` entries flattened to `env.NAME`."""
    level = None
    starts: list[int] = []
    for i in range(first + 1, stop):
        line = lines[i]
        if not line.strip():
            continue
        if line.lstrip().startswith("- "):
            if level is None:
                level = indent(line)
            if indent(line) == level:
                starts.append(i)
    items: list[dict[str, str]] = []
    for n, start in enumerate(starts):
        end = starts[n + 1] if n + 1 < len(starts) else stop
        block = lines[start:end]
        head = block[0]
        block = [head[:level] + "  " + head[level + 2:], *block[1:]]
        item: dict[str, str] = {}
        for key, inline, kfirst, kstop in children(block, 0, len(block), level + 2):
            if key == "env":
                for name, value, efirst, estop in children(block, kfirst + 1, kstop):
                    item[f"env.{name}"] = scalar(block, value, efirst, estop)
            else:
                item[key] = scalar(block, inline, kfirst, kstop)
        items.append(item)
    return items


def validate_gate(workflows: dict[str, str], contexts: list[str], table: dict[str, str]) -> list[str]:
    """Failures of the `CI Gate` wiring; `table` is `scripts/ci_gate.py`'s JOBS."""
    failures: list[str] = []
    parsed = {path: strip_comments(text) for path, text in workflows.items()}
    expected = set(OWNERS)
    for context in sorted(expected - set(contexts)):
        failures.append(f"policy lacks required context {context!r}")
    for context in sorted(set(contexts) - expected):
        failures.append(f"policy requires {context!r}, which is not one of the four gate-era contexts")
    for context, (path, job) in OWNERS.items():
        if path not in parsed:
            failures.append(f"{path}: missing, must own {context!r}")
            continue
        owners = [key for display, key, _ in jobs(parsed[path]) if display == context]
        if owners != [job]:
            failures.append(f"{path}: {context!r} must be owned by job {job!r}, found {owners}")
    if CI_WORKFLOW not in parsed:
        return failures
    lines = parsed[CI_WORKFLOW]
    bounds = job_bounds(lines)
    if GATE_JOB not in bounds:
        return [*failures, f"{CI_WORKFLOW}: no block-form job {GATE_JOB!r}"]
    gate = job_keys(lines, *bounds[GATE_JOB])
    condition = scalar(lines, *gate["if"]) if "if" in gate else ""
    if unquote(condition.strip("${} ")) != "always()":
        failures.append(f"{GATE_JOB}: if must be always(), found {condition!r}")
    if "continue-on-error" in gate:
        failures.append(f"{GATE_JOB}: continue-on-error is not allowed")
    needs = set(list_values(lines, *gate["needs"])) if "needs" in gate else set()
    for job in sorted(set(table) - needs):
        failures.append(f"{GATE_JOB}: needs lacks {job!r}, which ci_gate.py's table lists")
    for job in sorted(needs - set(table)):
        failures.append(f"{GATE_JOB}: needs {job!r}, which ci_gate.py's table does not list")
    for job in sorted(needs & set(table)):
        if job not in bounds:
            failures.append(f"{GATE_JOB}: needs {job!r}, which {CI_WORKFLOW} does not define as a block-form job")
        elif "continue-on-error" in job_keys(lines, *bounds[job]):
            failures.append(f"{job}: continue-on-error is not allowed on a job the gate needs")
    gate_steps = steps(lines, gate["steps"][1], gate["steps"][2]) if "steps" in gate else []
    if len(gate_steps) != 2:
        failures.append(f"{GATE_JOB}: must have exactly two steps, a checkout and the verdict, found {len(gate_steps)}")
    else:
        checkout, verdict = gate_steps
        if not PINNED_CHECKOUT.match(checkout.get("uses", "")):
            failures.append(f"{GATE_JOB}: first step must be a SHA-pinned actions/checkout, found {checkout.get('uses')!r}")
        if verdict.get("run") != GATE_COMMAND:
            failures.append(f"{GATE_JOB}: second step must run exactly {GATE_COMMAND!r}, found {verdict.get('run')!r}")
        if verdict.get("shell") != "bash":
            failures.append(f"{GATE_JOB}: verdict shell must be explicitly 'bash', found {verdict.get('shell')!r}")
        bound = {key[4:]: value for key, value in verdict.items() if key.startswith("env.")}
        if bound != GATE_ENV:
            failures.append(f"{GATE_JOB}: verdict env must be exactly {GATE_ENV}, found {bound}")
        for label, step in (("checkout", checkout), ("verdict", verdict)):
            for key in ("if", "continue-on-error"):
                if key in step:
                    failures.append(f"{GATE_JOB}: the {label} step carries {key}")
    if VALIDATOR_JOB not in bounds:
        return [*failures, f"{CI_WORKFLOW}: no block-form job {VALIDATOR_JOB!r}"]
    validator = job_keys(lines, *bounds[VALIDATOR_JOB])
    for key in ("if", "needs", "continue-on-error"):
        if key in validator:
            failures.append(f"{VALIDATOR_JOB}: {key} is not allowed, the validator runs unconditionally")
    validator_steps = steps(lines, validator["steps"][1], validator["steps"][2]) if "steps" in validator else []
    for command in VALIDATOR_COMMANDS:
        found = [step for step in validator_steps if command in step.get("run", "")]
        if not found:
            failures.append(f"{VALIDATOR_JOB}: no step runs {command!r}")
        for step in found:
            for key in ("if", "continue-on-error"):
                if key in step:
                    failures.append(f"{VALIDATOR_JOB}: the step running {command!r} carries {key}")
    return failures


def load() -> tuple[dict[str, str], list[str], dict[str, str]]:
    workflows = {
        str(path.relative_to(REPO_ROOT)): path.read_text(encoding="utf-8")
        for path in sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))
    }
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from ci_gate import JOBS

    return workflows, policy["required_status_checks"]["contexts"], JOBS


def self_test() -> None:
    a = "on:\n  pull_request:\n  merge_group:\njobs:\n  x:\n    name: Ctx A\n    if: (github.event_name == 'pull_request' || github.event_name == 'merge_group')\n    runs-on: u\n"
    b = "on:\n  pull_request_target:\n  merge_group:\njobs:\n  y:\n    name: Ctx B\n    runs-on: u\n"
    full = "(github.event_name == 'pull_request' || github.event_name == 'merge_group')"
    ctx = ["Ctx A", "Ctx B"]

    def run(a_text: str = a, b_text: str = b, contexts: list[str] = ctx, **extra: str) -> list[str]:
        return validate({"a.yml": a_text, "b.yml": b_text, **extra}, contexts)

    def refused(fragment: str, *args: str, **kw: str) -> None:
        assert any(fragment in f for f in run(*args, **kw)), (fragment, run(*args, **kw))

    def accepted(*args: str, **kw: str) -> None:
        assert run(*args, **kw) == [], run(*args, **kw)

    def b_on(on_text: str) -> str:
        return on_text + "jobs:\n  y:\n    name: Ctx B\n    runs-on: u\n"

    def b_mg(config: str) -> str:
        return b.replace("  merge_group:\n", config)

    accepted()
    refused("no merge_group in the on: mapping", a, b.replace("  merge_group:\n", ""))
    refused("reads the event without deciding on merge_group", a.replace(" || github.event_name == 'merge_group'", ""))
    refused("named by no workflow job", a, b, ["Ctx C"])
    refused("more than one job", a, b, ["Ctx A"], **{"c.yml": a})
    refused("more than one job", a.replace("    runs-on: u\n", "    runs-on: u\n  x2:\n    name: Ctx A\n    runs-on: u\n"))
    accepted(a.replace(full, "github.event_name != 'pull_request' && github.event_name != 'merge_group'"))
    for expr in ("github.event_name == 'push'", "github.ref == 'refs/heads/main'", "github.head_ref != ''", "github.base_ref == 'main'", "startsWith(github.ref_name, 'v')", "github.ref_type == 'tag'"):
        refused("reads the event", a.replace(full, expr))
    for expr in ("false", "'false'", "${{ false }}"):
        refused("never run", a.replace(full, expr))
    refused("reads the event", a.replace(f"    if: {full}\n", "    if: >\n      github.event_name == 'pull_request'\n      && needs.x.outputs.y == 'true'\n"))
    refused("reads the event", a.replace(f"    if: {full}\n", "    if: >2\n      github.event_name == 'pull_request'\n"))
    accepted(a.replace(f"    if: {full}\n", "    if: >\n      github.event_name == 'pull_request'\n      || github.event_name == 'merge_group'\n"))
    accepted(a.replace(f"    if: {full}\n", "    if: (github.event_name == 'pull_request' ||\n      github.event_name == 'merge_group')\n"))
    refused("reads the event", a.replace(f"    if: {full}\n", "    if: github.event_name == 'pull_request' &&\n      needs.x.outputs.y == 'true'\n"))
    accepted(a.replace(full, "needs.x.outputs.y == 'true'"))
    refused("reads the event", a.replace(full, "github.event_name == 'pull_request'  # merge_group later"))
    refused("reads the event", a.replace(full, "github.event_name == 'pull_request' && github.event.inputs.merge_group == 'x'"))
    refused("reads the event", a.replace(full, "github.event_name == 'pull_request' || github.ref_name == 'merge_group'"))
    refused("no merge_group in the on: mapping", a, b.replace("  merge_group:\n", "").replace("jobs:\n", "env:\n  merge_group: placeholder\njobs:\n"))
    accepted(a, b_mg("  \"merge_group\":\n"))
    accepted(a, b.replace("on:\n", "on:  # triggers\n"))
    refused("no merge_group in the on: mapping", a, b.replace("  merge_group:\n", "").replace("on:\n", "on:  # merge_group later\n"))
    accepted(a, b_on("on: [pull_request_target, merge_group]\n"))
    refused("no merge_group in the on: mapping", a, b_on("on: [pull_request_target]  # merge_group\n"))
    accepted(a, b_on("on: {\n  pull_request_target: null,\n  merge_group: null,\n}\n"))
    refused("no merge_group in the on: mapping", a, b_on("on: {pull_request_target: null, push: {branches: [main, merge_group]}, workflow_dispatch: null}\n"))
    refused("no merge_group in the on: mapping", a, b_on("on: {pull_request_target: null, workflow_dispatch: {inputs: {why: {description: merge_group}}}}\n"))
    accepted(a, b_on("on: {pull_request_target: null, merge_group: {branches: ['**']}}\n"))
    refused("does not admit main", a, b_on("on: {pull_request_target: null, merge_group: {branches: [release]}}\n"))
    accepted(a, b.replace("on:\n", "true:\n"))
    accepted(a, b_mg("  # queue\n  merge_group:\n"))
    accepted(a, b_mg("  merge_group:\n    branches: [main]\n"))
    accepted(a, b_mg("  merge_group:\n    branches: ['**']\n"))
    accepted(a, b_mg("  merge_group:\n    branches:\n      - main\n      - release/*\n"))
    accepted(a, b_mg("  merge_group: {branches: [main]}\n"))
    accepted(a, b_mg("  merge_group:\n    branches-ignore: [release]\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches: [release]\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches: [main-old]\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches: ['**', '!main']\n"))
    accepted(a, b_mg("  merge_group:\n    branches: ['!main', '**']\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches-ignore: [main]\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches-ignore: ['m*']\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches: ['mai?']\n"))
    accepted(a, b_mg("  merge_group:\n    branches: ['ma?in']\n"))
    accepted(a, b_mg("  merge_group:\n    branches: ['main+']\n"))
    accepted(a, b_mg("  merge_group:\n    branches: ['ma[a-z]n']\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches: ['ma[0-9]n']\n"))
    accepted(a, b_mg("  merge_group:\n    branches: ['m*']\n"))
    refused("does not admit main", a, b_mg("  merge_group:\n    branches: ['releases/*']\n"))
    assert glob_matches("**/main", "feature/main") and not glob_matches("*/main", "a/b/main")
    accepted(a.replace("  x:\n", "  format-check:\n"))
    accepted(a.replace("  x:\n", "  'x':\n"))
    accepted("on:\n  pull_request:\n  merge_group:\njobs:\n  x: {name: Ctx A, if: '" + full + "', runs-on: u}\n")
    refused("reads the event", "on:\n  pull_request:\n  merge_group:\njobs:\n  x: {name: Ctx A, if: 'github.event_name == ''pull_request''', runs-on: u}\n")
    accepted(a.replace("    name: Ctx A\n", "    name: Ctx A  # required context\n"))
    accepted(a.replace("    name: Ctx A\n", "    name: \"Ctx A # main\"\n"), b, ["Ctx A # main", "Ctx B"])
    accepted(a.replace("    name: Ctx A\n", "    name: Ctx A's  # c\n"), b, ["Ctx A's", "Ctx B"])
    accepted(a.replace("    name: Ctx A\n", ""), b, ["x", "Ctx B"])
    accepted(a.replace("\n", "\r\n"))
    accepted(a.replace("jobs:\n", "jobs:  # all jobs\n"))
    accepted(a.replace("    name", "      name").replace("    if", "      if").replace("    runs-on", "      runs-on").replace("  x:", "    x:"))
    self_test_gate()
    print("self-test ok")


GATE_TABLE = {"classify_changes": "always", "fmt": "pr_queue_full_ci", "test": "full_ci"}
SHA = "0123456789abcdef0123456789abcdef01234567"
GATE_CI = f"""on:
  pull_request:
  merge_group:
  push:
jobs:
  classify_changes:
    name: Classify Changes
    runs-on: u
  workflow_action_pins:
    name: Check Workflow Action Pins
    runs-on: u
    steps:
      - name: Checkout source
        uses: actions/checkout@{SHA} # v5.0.1
      - name: Check required contexts report on the merge queue
        run: python3 scripts/check-merge-group-triggers.py --self-test
      - name: Check the CI Gate verdict
        run: python3 scripts/ci_gate.py --self-test
  fmt:
    name: Format (rustfmt)
    needs: classify_changes
    if: (github.event_name == 'pull_request' || github.event_name == 'merge_group') && needs.classify_changes.outputs.run_full_ci == 'true'
    runs-on: u
  test:
    name: Test Workspace
    needs: classify_changes
    if: needs.classify_changes.outputs.run_full_ci == 'true'
    runs-on: u
  azurite_integration:
    name: Azurite Azure Integration
    if: github.event_name != 'merge_group'
    runs-on: u
  ci_gate:
    name: CI Gate
    needs: [classify_changes, fmt, test]
    if: always()
    runs-on: u
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout source
        uses: actions/checkout@{SHA} # v5.0.1

      - name: Judge the blocking jobs
        shell: bash
        env:
          NEEDS_JSON: ${{{{ toJSON(needs) }}}}
          EVENT_NAME: ${{{{ github.event_name }}}}
          EVENT_REF: ${{{{ github.ref }}}}
          RUN_FULL_CI: ${{{{ needs.classify_changes.outputs.run_full_ci }}}}
        run: python3 scripts/ci_gate.py
"""
GATE_GQ = "on:\n  pull_request:\n  merge_group:\njobs:\n  gq_logic_tests:\n    name: GQ Logic Tests\n    if: always()\n    runs-on: u\n"
GATE_FIX = "on:\n  pull_request_target:\n  merge_group:\njobs:\n  fix_regression_gate:\n    name: Fix Regression Gate\n    runs-on: u\n"
GATE_CONTEXTS = list(OWNERS)


def self_test_gate() -> None:
    def run(ci: str = GATE_CI, contexts: list[str] = GATE_CONTEXTS, table: dict[str, str] = GATE_TABLE, **extra: str) -> list[str]:
        workflows = {CI_WORKFLOW: ci, OWNERS["GQ Logic Tests"][0]: GATE_GQ, OWNERS["Fix Regression Gate"][0]: GATE_FIX, **extra}
        return validate(workflows, contexts) + validate_gate(workflows, contexts, table)

    def refused(fragment: str, *args, **kw) -> None:
        assert any(fragment in f for f in run(*args, **kw)), (fragment, run(*args, **kw))

    def accepted(*args, **kw) -> None:
        assert run(*args, **kw) == [], run(*args, **kw)

    def gate(old: str, new: str) -> str:
        head, _, body = GATE_CI.partition("  ci_gate:\n")
        assert old in body, old
        return head + "  ci_gate:\n" + body.replace(old, new)

    def validator(old: str, new: str) -> str:
        head, _, rest = GATE_CI.partition("  workflow_action_pins:\n")
        body, _, tail = rest.partition("  fmt:\n")
        assert old in body, old
        return head + "  workflow_action_pins:\n" + body.replace(old, new) + "  fmt:\n" + tail

    accepted()
    refused("policy lacks required context 'CI Gate'", GATE_CI, [c for c in GATE_CONTEXTS if c != GATE_CONTEXT])
    refused("policy lacks required context 'Check Workflow Action Pins'", GATE_CI, [c for c in GATE_CONTEXTS if c != VALIDATOR_CONTEXT])
    refused("not one of the four gate-era contexts", GATE_CI, [*GATE_CONTEXTS, "Test Workspace"])
    refused("must be owned by job 'ci_gate'", gate("    name: CI Gate\n", "    name: CI gate\n"))
    refused("must be owned by job 'ci_gate'", GATE_CI.replace("  ci_gate:\n", "  ci_gate2:\n"))
    refused("must be owned by job 'workflow_action_pins'", validator("    name: Check Workflow Action Pins\n", "    name: Check Pins\n"))
    refused("must be owned by job 'gq_logic_tests'", **{OWNERS["GQ Logic Tests"][0]: GATE_GQ.replace("gq_logic_tests:", "gq:")})
    refused("if must be always()", gate("    if: always()\n", ""))
    refused("if must be always()", gate("    if: always()\n", "    if: success()\n"))
    refused("if must be always()", gate("    if: always()\n", "    if: always() && github.event_name == 'merge_group'\n"))
    accepted(gate("    if: always()\n", "    if: ${{ always() }}\n"))
    accepted(gate("    if: always()\n", "    if: 'always()'\n"))
    refused("continue-on-error is not allowed", gate("    if: always()\n", "    if: always()\n    continue-on-error: true\n"))
    refused("needs lacks 'test'", gate("    needs: [classify_changes, fmt, test]\n", "    needs: [classify_changes, fmt]\n"))
    refused("needs 'azurite_integration', which ci_gate.py's table does not list", gate("    needs: [classify_changes, fmt, test]\n", "    needs: [classify_changes, fmt, test, azurite_integration]\n"))
    refused("needs lacks 'test'", gate("    needs: [classify_changes, fmt, test]\n", ""))
    accepted(gate("    needs: [classify_changes, fmt, test]\n", "    needs:\n      - classify_changes\n      - fmt\n      - test\n"))
    refused("needs 'lint', which .github/workflows/ci.yml does not define", gate("    needs: [classify_changes, fmt, test]\n", "    needs: [classify_changes, fmt, test, lint]\n"), GATE_CONTEXTS, {**GATE_TABLE, "lint": "always"})
    refused("needs lacks 'lint'", GATE_CI, GATE_CONTEXTS, {**GATE_TABLE, "lint": "always"})
    refused("continue-on-error is not allowed on a job the gate needs", GATE_CI.replace("    name: Test Workspace\n", "    name: Test Workspace\n    continue-on-error: true\n"))
    start = GATE_CI.index("  test:\n")
    stop = GATE_CI.index("  azurite_integration:\n", start)
    for suppression in ("", "continue-on-error: true, "):
        fields = f'name: Test Workspace, runs-on: u, {suppression}steps: [{{run: "exit 1"}}]'
        for job in (
            "  test: {" + fields + "}\n",
            "  test: {\n    " + fields + "\n  }\n",
            "  test:\n    {" + fields + "}\n",
        ):
            refused("needs 'test', which .github/workflows/ci.yml does not define as a block-form job", GATE_CI[:start] + job + GATE_CI[stop:])
    accepted(GATE_CI.replace("    name: Azurite Azure Integration\n", "    name: Azurite Azure Integration\n    continue-on-error: true\n"))
    for shell in ("", "        shell: bash -c true {0}\n", "        shell: sh\n"):
        refused("verdict shell must be explicitly 'bash'", gate("        shell: bash\n", shell))
    defaults = "    defaults:\n      run:\n        shell: bash -c true {0}\n"
    accepted(gate("    steps:\n", defaults + "    steps:\n"))
    accepted(GATE_CI.replace("jobs:\n", "defaults:\n  run:\n    shell: bash -c true {0}\njobs:\n"))
    refused("the checkout step carries if", gate("        uses: actions/checkout", "        if: always()\n        uses: actions/checkout"))
    refused("the verdict step carries if", gate("        run: python3 scripts/ci_gate.py\n", "        if: github.event_name == 'pull_request'\n        run: python3 scripts/ci_gate.py\n"))
    refused("the verdict step carries continue-on-error", gate("        run: python3 scripts/ci_gate.py\n", "        continue-on-error: true\n        run: python3 scripts/ci_gate.py\n"))
    refused("exactly two steps", gate("      - name: Judge the blocking jobs\n", "      - name: Noop\n        run: true\n      - name: Judge the blocking jobs\n"))
    refused("exactly two steps", gate("\n      - name: Judge the blocking jobs\n        shell: bash\n        env:\n          NEEDS_JSON: ${{ toJSON(needs) }}\n          EVENT_NAME: ${{ github.event_name }}\n          EVENT_REF: ${{ github.ref }}\n          RUN_FULL_CI: ${{ needs.classify_changes.outputs.run_full_ci }}\n        run: python3 scripts/ci_gate.py\n", ""))
    refused("must run exactly 'python3 scripts/ci_gate.py'", gate("        run: python3 scripts/ci_gate.py\n", "        run: python3 scripts/ci_gate.py --lenient\n"))
    refused("must run exactly 'python3 scripts/ci_gate.py'", gate("        run: python3 scripts/ci_gate.py\n", "        run: |\n          python3 scripts/ci_gate.py || true\n"))
    refused("must run exactly 'python3 scripts/ci_gate.py'", gate("        run: python3 scripts/ci_gate.py\n", "        run: true\n"))
    refused("verdict env must be exactly", gate("          RUN_FULL_CI: ${{ needs.classify_changes.outputs.run_full_ci }}\n", "          RUN_FULL_CI: 'false'\n"))
    refused("verdict env must be exactly", gate("          RUN_FULL_CI: ${{ needs.classify_changes.outputs.run_full_ci }}\n", ""))
    refused("verdict env must be exactly", gate("          EVENT_REF: ${{ github.ref }}\n", "          EVENT_REF: ${{ github.ref }}\n          EXTRA: x\n"))
    refused("verdict env must be exactly", gate("          NEEDS_JSON: ${{ toJSON(needs) }}\n", "          NEEDS_JSON: '{}'\n"))
    refused("first step must be a SHA-pinned actions/checkout", gate(f"        uses: actions/checkout@{SHA} # v5.0.1\n", "        uses: actions/checkout@v5\n"))
    refused("first step must be a SHA-pinned actions/checkout", gate(f"        uses: actions/checkout@{SHA} # v5.0.1\n", "        run: git clone .\n"))
    refused("workflow_action_pins: needs is not allowed", validator("    runs-on: u\n", "    runs-on: u\n    needs: classify_changes\n"))
    refused("workflow_action_pins: if is not allowed", validator("    runs-on: u\n", "    runs-on: u\n    if: github.event_name == 'pull_request'\n"))
    refused("workflow_action_pins: continue-on-error is not allowed", validator("    runs-on: u\n", "    runs-on: u\n    continue-on-error: true\n"))
    refused("no step runs 'python3 scripts/check-merge-group-triggers.py'", validator("        run: python3 scripts/check-merge-group-triggers.py --self-test\n", "        run: true\n"))
    refused("no step runs 'python3 scripts/ci_gate.py --self-test'", validator("        run: python3 scripts/ci_gate.py --self-test\n", "        run: python3 scripts/ci_gate.py\n"))
    refused("the step running 'python3 scripts/ci_gate.py --self-test' carries if", validator("        run: python3 scripts/ci_gate.py --self-test\n", "        if: false\n        run: python3 scripts/ci_gate.py --self-test\n"))
    refused("the step running 'python3 scripts/check-merge-group-triggers.py' carries continue-on-error", validator("        run: python3 scripts/check-merge-group-triggers.py --self-test\n", "        continue-on-error: true\n        run: python3 scripts/check-merge-group-triggers.py --self-test\n"))
    accepted(validator("        run: python3 scripts/check-merge-group-triggers.py --self-test\n", "        run: |\n          python3 scripts/check-merge-group-triggers.py --self-test\n          python3 scripts/check-ci-cells.py --self-test\n"))
    refused("no block-form job 'ci_gate'", GATE_CI.partition("  ci_gate:\n")[0])
    refused("no block-form job 'workflow_action_pins'", GATE_CI.replace("  workflow_action_pins:\n", "  workflow_action_pins_old:\n"))
    accepted(GATE_CI.replace("\n", "\r\n"))
    accepted(gate("      - name: Checkout source\n", "      # pinned\n      - name: Checkout source\n"))


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        self_test()
    workflows, contexts, table = load()
    failures = validate(workflows, contexts) + validate_gate(workflows, contexts, table)
    for failure in failures:
        print(f"::error::{failure}")
    if failures:
        return 1
    print(f"ok: {len(contexts)} required contexts report on merge_group; {GATE_CONTEXT} needs {len(table)} jobs as ci_gate.py lists")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
