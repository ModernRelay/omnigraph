#!/usr/bin/env python3
"""Two checks on the change classes `ci.yml`'s `classify_changes` job emits
(docs/dev/ci.md, Pull-request gates).

1. Replay: extracts the `Classify changed paths` shell from `ci.yml`, swaps
   its `git diff` for a fixture path list and asserts the three outputs
   (`run_full_ci`, `run_gqt`, `run_deployment`) per fixture, so the class
   predicate has a pinned oracle instead of a reviewer's reading. One more
   fixture keeps the real `git diff` and runs it inside a throwaway
   repository whose `main` moved on after the PR branch forked: the
   classifier must diff from the merge base, so the branch does not inherit
   main's newer engine files and lose its skip.
2. Reader closure, lexical: no string literal in a Rust or TOML file under
   `crates/` or `tools/` spells a class path (the `.gqt` corpus directory;
   `Dockerfile`, `.dockerignore`, `docker/`, `deploy/`, root-relative or
   `../`-relative) unless the file is a listed reader whose covering job is
   named beside it. Comments are not read: a mention is not a dependency. A
   path assembled from pieces at runtime (`Path::new("deploy").join(..)`) is
   invisible here; a reader of that shape is the reviewer's to list (ci.md,
   Changing CI rule 3). A class is sound only while the set of jobs reading
   its paths is closed; this sweep is the literal-spelling half of that
   promise for compiled code, scripts and workflows are reviewed by hand.

Run from the repository root; exit 0 exactly when both checks pass.
`--self-test` proves the sweep goes red on planted readers (root-relative and
`../`-relative, Rust and TOML), stays green on an allowlisted reader and on
comment-only mentions, and refuses a near-miss of an exact allowlist entry.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

CI = Path(".github/workflows/ci.yml")
STEP = re.compile(
    r"^      - name: Classify changed paths\n.*?^        run: \|\n((?:^          [^\n]*\n|^\n)+)",
    re.MULTILINE | re.DOTALL,
)
MAPFILE = "mapfile -t changed < <("
# The git invocation may carry `-c` options (core.quotePath); the range form
# is the contract: merge base of base and head, renames uncollapsed.
GIT_DIFF = re.compile(r'git (?:-c \S+ )*diff --name-only --no-renames --merge-base "\$base" "\$head"')
# A `read` loop also serves bash 3.2, which has no mapfile.
READ_LOOP = 'changed=()\nwhile IFS= read -r line; do changed+=("$line"); done < <('

ALL_TRUE = ("true", "true", "true")
CASES_ONLY = ("false", "true", "false")
DOCS_ONLY = ("false", "false", "false")
# (name, event, changed paths, expected run_full_ci / run_gqt / run_deployment)
FIXTURES = [
    ("cases only", "pull_request", ["crates/omnigraph-gqt/cases/omitted_required_param_refused.gqt"], CASES_ONLY),
    ("cases + docs", "pull_request", ["crates/omnigraph-gqt/cases/a.gqt", "docs/dev/ci.md", "README.md"], CASES_ONLY),
    ("case in a subdirectory", "pull_request", ["crates/omnigraph-gqt/cases/drafts/a.gqt"], CASES_ONLY),
    ("docs only", "pull_request", ["docs/user/index.md", "LICENSE", "LICENSE.md"], DOCS_ONLY),
    ("docs with a non-ASCII name", "pull_request", ["docs/user/café.md"], DOCS_ONLY),
    ("deployment only", "pull_request", ["deploy/azure/foundation.bicep", "Dockerfile", ".dockerignore", "docker/entrypoint.sh"], ("false", "false", "true")),
    ("cases + deployment", "pull_request", ["crates/omnigraph-gqt/cases/a.gqt", "deploy/azure/runtime.bicep"], ("false", "true", "true")),
    ("cases + engine", "pull_request", ["crates/omnigraph-gqt/cases/a.gqt", "crates/omnigraph/src/lib.rs"], ALL_TRUE),
    ("non-gqt file under cases", "pull_request", ["crates/omnigraph-gqt/cases/README.md"], ALL_TRUE),
    ("gqt harness source", "pull_request", ["crates/omnigraph-gqt/src/lib.rs"], ALL_TRUE),
    ("workflow file", "pull_request", [".github/workflows/ci.yml"], ALL_TRUE),
    ("script", "pull_request", ["scripts/install.sh"], ALL_TRUE),
    ("docs image", "pull_request", ["docs/user/diagram.png"], ALL_TRUE),
    ("Cargo.lock", "pull_request", ["Cargo.lock"], ALL_TRUE),
    ("empty diff", "pull_request", [], ALL_TRUE),
    ("workflow_dispatch", "workflow_dispatch", ["docs/user/index.md"], ALL_TRUE),
]

SWEEP_ROOTS = ("crates", "tools")
SWEEP_SUFFIXES = {".rs", ".toml"}
# A class path spelled from the repository root or through `../` hops; a
# segment inside another path (`crates/x/deploy/`, a URL) is not one.
RELATIVE = r"(?<![\w/.-])(?:\.\./)*(?:\./)?"
# class name -> (pattern a reader would carry, allowlist: `dir/` prefix or exact file -> covering job)
CLASSES = {
    "GQT cases": (
        re.compile(RELATIVE + r"(?:crates/)?omnigraph-gqt/cases(?![\w-])"),
        {
            # The crate that owns the corpus; every reader in it runs under
            # `GQ Logic Tests`, gated on `run_gqt`.
            "crates/omnigraph-gqt/": "GQ Logic Tests",
            # The seam guard counts a case's `at:` name as arming a seam, so
            # `GQT (ordinary)` runs this test on `run_gqt` beside
            # `Test Workspace`'s engine-input run.
            "crates/omnigraph-seams/tests/failpoint_names_guard.rs": "GQ Logic Tests (ordinary) + Test Workspace",
        },
    ),
    "deployment": (
        re.compile(RELATIVE + r"(?:Dockerfile(?![\w.-])|\.dockerignore(?![\w.-])|docker/|deploy/)"),
        {},
    ),
}


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def commit(repo: Path, path: str, text: str, message: str) -> str:
    file = repo / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(text)
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=replay", "-c", "user.email=replay@example.invalid", "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def stale_branch_repo(repo: Path) -> tuple[str, str]:
    """A PR branch that adds one case while main gained an engine edit.

    Returns (base, head) as GitHub sets them: base is main's tip, head is
    the branch tip. A tree diff between the two lists the engine file; a
    merge-base diff lists only the case."""
    git(repo, "init", "-q", "-b", "main")
    commit(repo, "crates/omnigraph/src/lib.rs", "v1\n", "engine v1")
    git(repo, "switch", "-q", "-c", "pr")
    head = commit(repo, "crates/omnigraph-gqt/cases/new_case.gqt", "# issue: none\n", "add case")
    git(repo, "switch", "-q", "main")
    base = commit(repo, "crates/omnigraph/src/lib.rs", "v2\n", "engine v2")
    two_dot = git(repo, "diff", "--name-only", "--no-renames", base, head).splitlines()
    if "crates/omnigraph/src/lib.rs" not in two_dot:
        sys.exit("stale-branch fixture is not stale: the tree diff lists no engine file")
    return base, head


def run_script(script: str, cwd: Path, env: dict[str, str], out: Path) -> tuple[subprocess.CompletedProcess[str], tuple[str | None, ...]]:
    out.write_text("")
    run = subprocess.run(["bash", "-c", script], cwd=cwd, env=env, capture_output=True, text=True, timeout=120)
    got = dict(line.split("=", 1) for line in out.read_text().splitlines() if "=" in line)
    return run, (got.get("run_full_ci"), got.get("run_gqt"), got.get("run_deployment"))


def report(name: str, run: subprocess.CompletedProcess[str], triple: tuple[str | None, ...], expected: tuple[str, ...]) -> bool:
    ok = run.returncode == 0 and triple == expected
    print(f"{'ok  ' if ok else 'FAIL'} replay {name}: full_ci/gqt/deployment = {triple} (expected {expected})")
    if run.returncode != 0:
        print(run.stderr)
    return ok


def replay() -> int:
    match = STEP.search(CI.read_text(encoding="utf-8"))
    if not match:
        print(f"FAIL: classify step not found in {CI}")
        return 1
    script = "\n".join(line[10:] for line in match.group(1).splitlines())
    if MAPFILE not in script or not GIT_DIFF.search(script):
        print("FAIL: the classifier's git diff line was not found; update MAPFILE/GIT_DIFF")
        return 1
    real_git = script.replace(MAPFILE, READ_LOOP)
    file_list = GIT_DIFF.sub('cat "$CHANGED_FILE"', real_git)
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp, "out")
        changed = Path(tmp, "changed")
        base_env = {**os.environ, "REF_TYPE": "branch", "BEFORE_SHA": "", "GITHUB_OUTPUT": str(out)}
        for name, event, paths, expected in FIXTURES:
            changed.write_text("".join(p + "\n" for p in paths), encoding="utf-8")
            env = {**base_env, "EVENT_NAME": event, "PR_BASE_SHA": "base", "PR_HEAD_SHA": "head",
                   "GITHUB_SHA": "head", "CHANGED_FILE": str(changed)}
            run, triple = run_script(file_list, Path.cwd(), env, out)
            failures += not report(name, run, triple, expected)

        repo = Path(tmp, "repo")
        repo.mkdir()
        base, head = stale_branch_repo(repo)
        env = {**base_env, "EVENT_NAME": "pull_request", "PR_BASE_SHA": base, "PR_HEAD_SHA": head, "GITHUB_SHA": head}
        run, triple = run_script(real_git, repo, env, out)
        failures += not report("stale branch, cases only, main ahead (real git)", run, triple, CASES_ONLY)
    return failures


RAW_OPEN = re.compile(r'r(#*)"')


def rust_strings(text: str) -> list[tuple[int, str]]:
    """(1-based line, contents) of every string literal, comments skipped.

    Handles `//` and nested `/* */` comments, `"…"` with escapes, `r#"…"#`
    raw strings (also as `b"…"` / `br"…"`), and the char literals that could
    open a false string (`'"'`, `'\\''`)."""
    out: list[tuple[int, str]] = []
    i, n, line = 0, len(text), 1
    while i < n:
        c = text[i]
        if c == "\n":
            line += 1
            i += 1
        elif text.startswith("//", i):
            end = text.find("\n", i)
            i = n if end < 0 else end
        elif text.startswith("/*", i):
            depth, i = 1, i + 2
            while i < n and depth:
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    line += text[i] == "\n"
                    i += 1
        elif c == '"':
            start_line, i = line, i + 1
            begin = i
            while i < n and text[i] != '"':
                if text[i] == "\\":
                    i += 1
                line += text[i] == "\n" if i < n else 0
                i += 1
            out.append((start_line, text[begin:i]))
            i += 1
        elif c == "r" and (m := RAW_OPEN.match(text, i)) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_") or text[i - 1] == "b"):
            hashes = m.group(1)
            start_line, i = line, m.end()
            end = text.find('"' + hashes, i)
            end = n if end < 0 else end
            out.append((start_line, text[i:end]))
            line += text.count("\n", i, end)
            i = end + 1 + len(hashes)
        elif c == "'":
            if text.startswith("'\"'", i):
                i += 3
            elif text.startswith("'\\", i):
                end = text.find("'", i + 2)
                i = end + 1 if end > 0 else i + 1
            else:
                i += 1
        else:
            i += 1
    return out


def toml_strings(text: str) -> list[tuple[int, str]]:
    """(1-based line, contents) of every TOML string, `#` comments skipped."""
    out: list[tuple[int, str]] = []
    i, n, line = 0, len(text), 1
    while i < n:
        c = text[i]
        if c == "\n":
            line += 1
            i += 1
        elif c == "#":
            end = text.find("\n", i)
            i = n if end < 0 else end
        elif text.startswith('"""', i) or text.startswith("'''", i):
            quote = text[i:i + 3]
            start_line, i = line, i + 3
            end = text.find(quote, i)
            end = n if end < 0 else end
            out.append((start_line, text[i:end]))
            line += text.count("\n", i, end)
            i = end + 3
        elif c in "\"'":
            start_line, i = line, i + 1
            begin = i
            while i < n and text[i] != c and text[i] != "\n":
                if c == '"' and text[i] == "\\":
                    i += 1
                i += 1
            out.append((start_line, text[begin:i]))
            i += 1
        else:
            i += 1
    return out


def listed(rel: str, readers: dict[str, str]) -> bool:
    """A `dir/` entry covers its subtree; a file entry covers that file only."""
    return any(rel.startswith(entry) if entry.endswith("/") else rel == entry for entry in readers)


def sweep(root: Path) -> list[str]:
    """Every (class, file:line) where a string literal in compiled-code text
    spells a class path and the file is not one of that class's listed readers."""
    violations = []
    for sweep_root in SWEEP_ROOTS:
        for path in sorted((root / sweep_root).rglob("*")):
            if path.suffix not in SWEEP_SUFFIXES or not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            text = path.read_text(encoding="utf-8", errors="replace")
            literals = rust_strings(text) if path.suffix == ".rs" else toml_strings(text)
            for class_name, (pattern, readers) in CLASSES.items():
                if listed(rel, readers):
                    continue
                for line, literal in literals:
                    for match in pattern.finditer(literal):
                        violations.append(f"{class_name}: {rel}:{line} spells a class path ({match.group(0)!r}) and is not a listed reader")
    return violations


def closure() -> int:
    violations = sweep(Path("."))
    for violation in violations:
        print(f"FAIL: {violation}")
    if violations:
        print("A job outside the class reads its paths: list the reader with its covering job, or make the path engine input (docs/dev/ci.md, Pull-request gates).")
        return 1
    readers = sum(len(r) for _p, r in CLASSES.values())
    print(f"ok: reader closure holds over {'/'.join(SWEEP_ROOTS)} string literals ({len(CLASSES)} classes, {readers} listed readers)")
    return 0


def plant(root: Path, rel: str, text: str) -> None:
    file = root / rel
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(text)


def self_test() -> int:
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        red = {
            "crates/a/src/lib.rs": 'const CASE: &str = include_str!("../../omnigraph-gqt/cases/a.gqt");\n',
            "crates/b/src/lib.rs": 'const IMG: &str = include_str!("../../../deploy/azure/main.bicep");\n',
            "crates/c/src/lib.rs": 'let p = "../docker/entrypoint.sh";\n',
            "crates/d/src/lib.rs": 'let p = "deploy/azure/main.bicep"; // root-relative\n',
            "crates/e/src/lib.rs": 'let p = r#"Dockerfile"#;\n',
            "crates/f/Cargo.toml": '[package]\nname = "f"\n[[bin]]\nname = "x"\npath = "../deploy/x.rs"\n',
            "crates/omnigraph-seams/tests/failpoint_names_guard.rs.extra.rs": 'let d = "../omnigraph-gqt/cases";\n',
        }
        green = {
            "crates/omnigraph-gqt/src/lib.rs": 'const DIR: &str = "crates/omnigraph-gqt/cases";\n',
            "crates/omnigraph-seams/tests/failpoint_names_guard.rs": 'let d = "../omnigraph-gqt/cases";\n',
            "crates/g/src/lib.rs": '// Deployment setup is documented in deploy/azure/README.md.\n/// Cases live in crates/omnigraph-gqt/cases.\n/* see docker/entrypoint.sh */\nfn noop() {}\n',
            "crates/h/src/lib.rs": 'let url = "https://example.invalid/deploy/azure"; let seg = "crates/x/deploy/y"; let dev = "Dockerfile.dev"; let boot = "bootstrap.Dockerfile";\n',
            "crates/i/Cargo.toml": '[package]\nname = "i" # built from deploy/azure, see docker/\n',
            "tools/j/src/lib.rs": "let c = '\"'; let s = \"nothing\"; // deploy/azure after a char literal\n",
        }
        for rel, text in {**red, **green}.items():
            plant(root, rel, text)
        (root / "tools").mkdir(exist_ok=True)
        found = sweep(root)
        checks = [(f"planted reader {rel} is red", any(f"{rel}:" in v for v in found)) for rel in red]
        checks += [(f"{rel} is green", not any(f"{rel}:" in v for v in found)) for rel in green]
        checks.append((f"exactly {len(red)} violations", len(found) == len(red)))
        for name, ok in checks:
            failures += not ok
            print(f"{'ok  ' if ok else 'FAIL'} self-test {name}")
        if len(found) != len(red):
            print("\n".join(found))
    return failures


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return 1 if self_test() else 0
    if argv:
        print(__doc__)
        return 2
    failures = replay() + closure()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
