"""Regression owner for check-pr-title.py; also run by its --self-test."""

from contextlib import redirect_stdout
from dataclasses import FrozenInstanceError
import importlib.util
import io
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("check-pr-title.py")
SPEC = importlib.util.spec_from_file_location("pr_title", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
m = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = m
SPEC.loader.exec_module(m)
C, F = m.Code, m.Field
MAX_LEN = m.MAX_LEN

ACCEPTED = (
    "feat(server): return exact merge publication receipts",
    "fix(merge): decide fast_forward by the target's state",
    "refactor(blob,schema)!: remove --allow-data-loss",
    "docs: correct the ledger v2 upgrade note",
    "rfc: search plan truth on engine v2",
    "fix(server): follow RFC 9110 for 503 retries",
    "release: v0.12.0",
    "revert: drop list membership",
    "docs: fix a broken link in the upgrade guide",
    "test(gq): cover inherited indexes after fast-forward",
    "ci: run slow GQT cases in a nightly workflow",
    "build: bump lance to 11.0.1",
    "fix: reject `DateTime` digits",
    "build: bump Lance to 11.0.1",
    "perf(catalog): release settled commits to immutable `__history` extents",
    "feat(gqt): add `--- concurrent` block, ordered sessions on one handle under DST",
    "fix!: 2-phase cursor keeps wide rows out of SortExec",
    "feat(engine): " + "x" * (MAX_LEN - len("feat(engine): ")),
    "fix: " + "é" * (MAX_LEN - len("fix: ")),
)

REFUSED = ('fix (#898)',
 'Update Readme',
 'Enable server-owned v2 deployments without restart',
 'detached table commits replace recovery sidecars',
 'imp',
 'review',
 'Check docs conflict markers',
 'Clarify infrastructure support in README',
 'Merge pull request #680 from ModernRelay/codex/self-service-cluster-lifecycle',
 "Merge remote-tracking branch 'origin/main' into codex/revert-663-actor-provenance",
 'RFC 0066: Server lifecycle and online deployment',
 'compiler: the diagnostics contract for refused queries',
 'engine: shared schema gate and the write critical section',
 'snapshot: read the storage-format stamp from the snapshot it returns',
 'cli: extend managed deadlines and prepare bounded transports',
 'auth: use cluster identities and applied policy for authorization',
 'bench: concurrent-writes throughput diagnostic',
 'merge: integrate main and allocate managed lifecycle RFC 0061',
 'chore(release): prepare notes for v0.13.1',
 'Governance: issue-first intake, Discussions retired',
 'GQ: add edge alternation and wildcard traversal',
 'RFC: Compatibility surfaces',
 'docs(rfc): align RFC 0068 with detached-only tables',
 'docs(rfcs): name new RFCs by creation date and slug',
 'rfc: RFC 0047, search plan truth on engine v2',
 '',
 ' feat: leading space',
 'feat: trailing space ',
 'feat:',
 'feat(server)!:',
 'feat:no space',
 'feat:  two spaces',
 'fix: \tCapital description',
 'fix: \xa0Capital description',
 'fix: \rCapital description',
 'fix:\ttab after the colon',
 'feat: two\nlines',
 'feat(server\n): newline in the scope',
 'feat(server\xa0): no-break space in the scope',
 'feat(): empty scope',
 'feat(Server): capital scope',
 'feat(server, api): space in scope',
 'feat(server)!!: double bang',
 'feat(server) : space before colon',
 'Feat: capital type',
 'FIX: capital type',
 'feat: Capital description',
 'feat: ends with a period.',
 'chore: tidy',
 'style: reformat',
 'WIP',
 '[WIP] feat: draft',
 'Revert "feat(gq): add list membership"',
 'feat(engine): xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
 'fix: éééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééé')

SQUASH_SUBJECTS = (
    ("feat(server): own writes, bound admission, and coordinate shutdown (#824)\n\n* body line\n", "feat(server): own writes, bound admission, and coordinate shutdown"),
    ("release: v0.12.0 (#877)", "release: v0.12.0"),
    ("fix (#898)", "fix"),
    ("fix: keep a (#12) inside the text (#900)", "fix: keep a (#12) inside the text"),
    ("Update Readme (#881)\n", "Update Readme"),
    ("", ""),
)




# Every code has an exact original-title slice, including insertion points.
CASES = (
    ("", C.EMPTY_TITLE, F.TITLE, 0, 0, ""),
    (" fix: ok", C.LEADING_WHITESPACE, F.TITLE, 0, 1, " "),
    ("fix: ok ", C.TRAILING_WHITESPACE, F.TITLE, 7, 8, " "),
    ("fix: a\tb", C.NON_SPACE_WHITESPACE, F.TITLE, 6, 7, "\t"),
    ("fix: " + "x" * 96, C.TITLE_TOO_LONG, F.TITLE, 100, 101, "x"),
    ('Revert "x"', C.REVERT_FORMAT, F.PREFIX, 0, 8, 'Revert "'),
    ("1fix: ok", C.MISSING_TYPE, F.TYPE, 0, 1, "1"),
    ("Fix: ok", C.TYPE_CASE, F.TYPE, 0, 3, "Fix"),
    ("chore: ok", C.UNKNOWN_TYPE, F.TYPE, 0, 5, "chore"),
    ("fix", C.MISSING_COLON, F.PREFIX, 3, 3, ""),
    ("fix2: ok", C.UNEXPECTED_PREFIX, F.PREFIX, 3, 4, "2"),
    ("fix : ok", C.PREFIX_WHITESPACE, F.PREFIX, 3, 4, " "),
    ("fix(api: ok", C.UNCLOSED_SCOPE, F.SCOPE, 3, 4, "("),
    ("fix((api)): ok", C.NESTED_SCOPE, F.SCOPE, 4, 5, "("),
    ("fix(): ok", C.EMPTY_SCOPE_NAME, F.SCOPE, 4, 4, ""),
    ("fix(Api): ok", C.SCOPE_CHARACTER, F.SCOPE, 4, 5, "A"),
    ("fix!!: ok", C.REPEATED_BANG, F.BREAKING, 4, 5, "!"),
    ("fix!(api): ok", C.MISPLACED_BANG, F.BREAKING, 3, 4, "!"),
    ("docs(rfc): ok", C.RFC_TYPE, F.TYPE, 0, 9, "docs(rfc)"),
    ("fix:", C.EMPTY_DESCRIPTION, F.DESCRIPTION, 4, 4, ""),
    ("fix:ok", C.COLON_SPACING, F.DESCRIPTION, 4, 4, ""),
    ("fix: Ok", C.DESCRIPTION_CASE, F.DESCRIPTION, 5, 6, "O"),
    ("fix: ok.", C.TRAILING_PERIOD, F.DESCRIPTION, 7, 8, "."),
)
LONG_TITLE = (
    "fix(merge)!: find the merge base over a contracted lineage, "
    "fork and merge points only, not commit by commit"
)


class RuleTests(unittest.TestCase):
    def test_existing_policy_fixtures(self):
        for title in ACCEPTED:
            with self.subTest(title=title):
                self.assertTrue(m.diagnose(title).valid)
        for title in REFUSED:
            with self.subTest(title=title):
                self.assertFalse(m.diagnose(title).valid)

    def test_every_code_exact_range_and_field(self):
        self.assertEqual({row[1] for row in CASES}, set(C))
        for title, code, field, start, end, found in CASES:
            with self.subTest(code=code):
                report = m.diagnose(title)
                self.assertEqual(len(report.errors), 1)
                error = report.errors[0]
                self.assertIs(error.code, code)
                self.assertIs(error.code.field, field)
                self.assertEqual(error.span, m.Span(start, end))
                self.assertEqual(error.found, found)
                self.assertEqual(title[start:end], found)
                self.assertTrue(error.expected and error.message and error.fix)

    def test_all_permitted_types_scope_bang_and_description_classes(self):
        for type_, scope, bang, desc in itertools.product(
            m.TYPES, ("", "(api)", "(a-b,c_2)", "(123)"), ("", "!"),
            ("lowercase", "éclair", "ǅtitle", "2-phase", "'quoted'", "中文", "🙂text", "_text", ".internal period"),
        ):
            title = f"{type_}{scope}{bang}: {desc}"
            with self.subTest(title=title):
                self.assertTrue(m.diagnose(title).valid)

    def test_length_boundaries_use_code_points(self):
        for char, size in itertools.product(("x", "é", "🙂"), (99, 100, 101, 108)):
            title = "fix: " + char * (size - 5)
            with self.subTest(char=char, size=size):
                report = m.diagnose(title)
                self.assertEqual(report.valid, size <= 100)
                data = report.to_json()
                self.assertEqual((data["length"], data["max_length"], data["excess"]), (size, 100, max(0, size - 100)))
                if size > 100:
                    self.assertEqual(report.errors[0].span, m.Span(100, size))

    def test_length_failure_exact_facts_and_repair(self):
        report = m.diagnose(LONG_TITLE)
        self.assertEqual(len(report.errors), 1)
        self.assertEqual(report.errors[0], m.Diagnostic(
            C.TITLE_TOO_LONG, m.Span(100, 108), LONG_TITLE[100:], "at most 100 characters",
            "108 characters; at most 100 (8 over the limit)", "Shorten the title by at least 8 characters.",
        ))
        self.assertEqual(m.diagnose("fix: " + "x" * 96).errors[0].fix, "Shorten the title by at least 1 character.")

    def test_empty_whitespace_and_original_offsets(self):
        for title, expected in (
            ("   ", [(C.LEADING_WHITESPACE, 0, 3), (C.EMPTY_TITLE, 3, 3)]),
            ("\t", [(C.LEADING_WHITESPACE, 0, 1), (C.NON_SPACE_WHITESPACE, 0, 1), (C.EMPTY_TITLE, 1, 1)]),
            (" fix: Ok ", [(C.LEADING_WHITESPACE, 0, 1), (C.TRAILING_WHITESPACE, 8, 9), (C.DESCRIPTION_CASE, 6, 7)]),
            ("fix: ", [(C.TRAILING_WHITESPACE, 4, 5), (C.EMPTY_DESCRIPTION, 4, 4)]),
        ):
            with self.subTest(title=title):
                self.assertEqual([(e.code, e.span.start, e.span.end) for e in m.diagnose(title).errors], expected)

    def test_every_python_non_space_whitespace_character(self):
        for point in range(sys.maxunicode + 1):
            char = chr(point)
            if char.isspace() and char != " ":
                title = f"fix: a{char}b{char}c"
                with self.subTest(point=point):
                    errors = m.diagnose(title).errors
                    self.assertEqual([e.code for e in errors], [C.NON_SPACE_WHITESPACE] * 2)
                    self.assertEqual([e.span for e in errors], [m.Span(6, 7), m.Span(8, 9)])
                    self.assertIn(f"U+{point:04X}", errors[0].message)

    def test_scope_names_and_delimiters(self):
        for title, codes in (
            ("fix(,api): ok", [C.EMPTY_SCOPE_NAME]),
            ("fix(api,): ok", [C.EMPTY_SCOPE_NAME]),
            ("fix(api,,db): ok", [C.EMPTY_SCOPE_NAME]),
            ("fix(api, db): ok", [C.SCOPE_CHARACTER]),
            ("fix(é): ok", [C.SCOPE_CHARACTER]),
            ("fix(api)(db): ok", [C.UNEXPECTED_PREFIX]),
            ("fix(api)!(db): ok", [C.UNEXPECTED_PREFIX]),
            ("fix): ok", [C.UNEXPECTED_PREFIX]),
            ("fix((api): ok", [C.NESTED_SCOPE, C.UNCLOSED_SCOPE]),
            ("fix(api:db): ok", [C.SCOPE_CHARACTER]),
        ):
            with self.subTest(title=title):
                self.assertEqual([e.code for e in m.diagnose(title).errors], codes)

    def test_prefix_and_missing_tokens(self):
        for title, codes in (
            ("fix(api)", [C.MISSING_COLON]),
            ("fix!", [C.MISSING_COLON]),
            ("fix description", [C.MISSING_COLON]),
            ("fix(api) : ok", [C.PREFIX_WHITESPACE]),
            ("fix(api)! : ok", [C.PREFIX_WHITESPACE]),
            ("fix (api): ok", [C.PREFIX_WHITESPACE]),
            ("fix !: ok", [C.PREFIX_WHITESPACE]),
            ("fix! (api): ok", [C.PREFIX_WHITESPACE, C.MISPLACED_BANG]),
            ("fix(api)!!!: ok", [C.REPEATED_BANG]),
            ("fix(api)!x: ok", [C.UNEXPECTED_PREFIX]),
            ("fix(api)!:", [C.EMPTY_DESCRIPTION]),
            ("fix:  ok", [C.COLON_SPACING]),
            ("fix:\tok", [C.NON_SPACE_WHITESPACE, C.COLON_SPACING]),
            ("fix: \tOk.", [C.NON_SPACE_WHITESPACE, C.COLON_SPACING, C.DESCRIPTION_CASE, C.TRAILING_PERIOD]),
        ):
            with self.subTest(title=title):
                self.assertEqual([e.code for e in m.diagnose(title).errors], codes)
        self.assertEqual(m.diagnose("fix: \tok").errors[-1].found, " \t")

    def test_rfc_and_revert_special_forms(self):
        for title, codes in (
            ("DOCS(rfc)!: ok", [C.TYPE_CASE, C.RFC_TYPE]),
            ("docs(rfcs): ok", [C.RFC_TYPE]),
            ("docs(RFC): ok", [C.SCOPE_CHARACTER]),
            ('Revert "fix: text"', [C.REVERT_FORMAT]),
            ('revert "fix: text"', [C.UNEXPECTED_PREFIX]),
            ("revert: undo x", []),
            ("rfc!: proposal", []),
        ):
            with self.subTest(title=title):
                self.assertEqual([e.code for e in m.diagnose(title).errors], codes)

    def test_description_boundaries(self):
        for title, codes in (
            ("fix: Éclair", [C.DESCRIPTION_CASE]),
            ("fix: ǅtitle", []),
            ("fix: .", [C.TRAILING_PERIOD]),
            ("fix: dots...", [C.TRAILING_PERIOD]),
            ("fix: full stop。", []),
            ("fix: literal \x1b[31m text", []),
            ("fix: internal.period", []),
        ):
            with self.subTest(title=title):
                self.assertEqual([e.code for e in m.diagnose(title).errors], codes)

    def test_all_independent_failures_in_stable_order(self):
        title = " FIX(Api):  Capital" + "x" * 100 + ". "
        self.assertEqual([e.code for e in m.diagnose(title).errors], [
            C.LEADING_WHITESPACE, C.TRAILING_WHITESPACE, C.TITLE_TOO_LONG,
            C.TYPE_CASE, C.SCOPE_CHARACTER, C.COLON_SPACING, C.DESCRIPTION_CASE, C.TRAILING_PERIOD,
        ])

    def test_squash_extraction(self):
        cases = (*SQUASH_SUBJECTS,
                 ("fix: accepted (#123)\r\n\r\nbody", "fix: accepted"),
                 ("\nbody", ""), (" \t ", ""),
                 ("  fix: kept (#12) inside (#13)  ", "fix: kept (#12) inside"),
                 ("fix: keep (#abc)", "fix: keep (#abc)"))
        for subject, expected in cases:
            with self.subTest(subject=subject):
                self.assertEqual(m.squash_title(subject), expected)
        report = m.diagnose(m.squash_title("Fix: title (#12)\nbody"), m.InputKind.SQUASH_SUBJECT)
        self.assertIs(report.input_kind, m.InputKind.SQUASH_SUBJECT)
        self.assertEqual(report.errors[0].span, m.Span(0, 3))

    def test_unclosed_scope_marks_opening_instead_of_inventing_an_insertion(self):
        for title in ("fix(api", "fix(api: ok", "fix(api!: ok", "fix((api): ok"):
            with self.subTest(title=title):
                error = next(e for e in m.diagnose(title).errors if e.code is C.UNCLOSED_SCOPE)
                self.assertEqual((error.span, error.found), (m.Span(3, 4), "("))


class ContractTests(unittest.TestCase):
    def test_runtime_types_and_invariants(self):
        for start, end, exception in ((True, 2, TypeError), (0, 1.0, TypeError), (-1, 0, ValueError), (2, 1, ValueError)):
            with self.subTest(start=start, end=end):
                with self.assertRaises(exception):
                    m.Span(start, end)
        valid = m.diagnose(LONG_TITLE)
        error = valid.errors[0]
        with self.assertRaises(FrozenInstanceError):
            error.code = C.EMPTY_TITLE
        values = (error.code, error.span, error.found, error.expected, error.message, error.fix)
        for index, value, exception in (
            (0, "title-too-long", TypeError), (1, (100, 108), TypeError),
            (2, 8, TypeError), (2, "", ValueError),
            (3, "", ValueError), (4, "", ValueError), (5, "", ValueError),
        ):
            args = list(values)
            args[index] = value
            with self.subTest(index=index, value=value), self.assertRaises(exception):
                m.Diagnostic(*args)
        for title, kind, errors, exception in (
            (None, m.InputKind.TITLE, (), TypeError),
            ("x", "title", (), TypeError),
            ("x", m.InputKind.TITLE, [], TypeError),
            ("x", m.InputKind.TITLE, ("error",), TypeError),
            ("x", m.InputKind.TITLE, (error,), ValueError),
            (LONG_TITLE[:-1] + "z", m.InputKind.TITLE, (error,), ValueError),
        ):
            with self.subTest(title=title, kind=kind), self.assertRaises(exception):
                m.Report(title, kind, errors)
        for title, kind in ((None, m.InputKind.TITLE), ("x", "title")):
            with self.assertRaises(TypeError):
                m.diagnose(title, kind)

    def test_typed_json_fields_and_schema_closed_sets(self):
        schema = m.report_schema()
        report_keys = set(m.ReportJSON.__annotations__)
        error_keys = set(m.DiagnosticJSON.__annotations__)
        self.assertEqual(set(schema["required"]), report_keys)
        self.assertEqual(set(schema["properties"]), report_keys)
        self.assertFalse(schema["additionalProperties"])
        error_schema = schema["properties"]["errors"]["items"]
        self.assertEqual(set(error_schema["required"]), error_keys)
        self.assertFalse(error_schema["additionalProperties"])
        self.assertEqual(set(error_schema["properties"]["code"]["enum"]), {code.value for code in C})
        self.assertEqual(set(error_schema["properties"]["field"]["enum"]), {field.value for field in F})
        self.assertEqual(len(error_schema["allOf"]), len(C))
        for title in (*ACCEPTED, *(row[0] for row in CASES)):
            with self.subTest(title=title):
                report = m.diagnose(title)
                data = json.loads(json.dumps(report.to_json()))
                self.assertEqual(set(data), report_keys)
                self.assertIs(type(data["valid"]), bool)
                self.assertEqual(data["valid"], not data["errors"])
                self.assertEqual(data["title"], title)
                self.assertEqual(data["length"], len(title))
                self.assertEqual(data["offset_unit"], "unicode-code-point")
                self.assertEqual(data["schema_version"], 1)
                for error in data["errors"]:
                    self.assertEqual(set(error), error_keys)
                    self.assertEqual(error["found"], title[error["span"]["start"]:error["span"]["end"]])
                    self.assertEqual(error["field"], C(error["code"]).field.value)

    def test_text_and_annotation_for_every_code(self):
        for title, code, _, _, _, _ in CASES:
            with self.subTest(code=code):
                report = m.diagnose(title)
                plain = m.text_report(report)
                colored = m.text_report(report, True)
                self.assertNotIn("\x1b", plain)
                self.assertEqual(m.re.sub(r"\x1b\[[0-9;]+m", "", colored), plain)
                self.assertIn(f"[{code.value}]", plain)
                self.assertIn(report.errors[0].fix, plain)
                annotation = m.annotation(report.errors[0])
                self.assertIn(f"[{code.value}]", annotation)
                self.assertEqual(len(annotation.splitlines()), 1)
        self.assertIn("PASS", m.text_report(m.diagnose("fix: ok")))
        self.assertIn("\x1b[1;32m", m.text_report(m.diagnose("fix: ok"), True))
        for span, expected in ((m.Span(0, 0), "insertion at offset 0"), (m.Span(0, 1), "character 1"), (m.Span(1, 3), "characters 2-3")):
            self.assertEqual(m.location(span), expected)

    def test_hostile_text_is_escaped(self):
        for title in ("fix(%0A\x1b[31m): ok\r\n::error::injected", "fix: literal \x1b[31m text"):
            report = m.diagnose(title)
            output = m.text_report(report)
            self.assertNotIn("\x1b", output)
            self.assertNotIn("\r", output)
            self.assertNotIn("\n::error::injected", output)
        error = m.Diagnostic(C.UNKNOWN_TYPE, m.Span(0, 1), "x", "fix", "a%\r\nb", "repair")
        self.assertIn("a%25%0D%0Ab", m.annotation(error))

    def test_color_precedence(self):
        for mode, tty, actions, no_color in itertools.product(m.Color, (False, True), ("", "false", "true"), (None, "", "1")):
            environment = {"GITHUB_ACTIONS": actions}
            if no_color is not None:
                environment["NO_COLOR"] = no_color
            expected = mode is m.Color.ALWAYS or (
                mode is m.Color.AUTO and not no_color and (tty or actions == "true")
            )
            with self.subTest(mode=mode, tty=tty, actions=actions, no_color=no_color):
                with patch.dict(os.environ, environment, clear=True), patch.object(sys.stdout, "isatty", return_value=tty):
                    self.assertEqual(m.use_color(mode), expected)

    def test_emit_modes_and_annotations(self):
        for title, format_, color, actions in itertools.product(
            ("fix: ok", LONG_TITLE, "FIX(Api):  Capital."), m.OutputFormat, m.Color, (False, True),
        ):
            report = m.diagnose(title)
            output = io.StringIO()
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "true" if actions else ""}, clear=True), redirect_stdout(output):
                code = m.emit(report, format_, color)
            text = output.getvalue()
            self.assertEqual(code, int(not report.valid))
            if format_ is m.OutputFormat.JSON:
                self.assertEqual(json.loads(text), report.to_json())
                self.assertNotIn("\x1b", text)
                self.assertNotIn("\n::error", text)
            else:
                self.assertEqual(len([line for line in text.splitlines() if line.startswith("::error")]), len(report.errors) if actions else 0)
        with self.assertRaises(TypeError):
            m.use_color("auto")
        with self.assertRaises(TypeError):
            m.emit(m.diagnose("fix: ok"), "json", m.Color.NEVER)


class CLITests(unittest.TestCase):
    def run_cli(self, *args):
        environment = {k: v for k, v in os.environ.items() if k not in ("NO_COLOR", "GITHUB_ACTIONS")}
        return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, env=environment, check=False)

    def test_valid_invalid_and_squash(self):
        for args, status in (
            (("--title", "fix: ok"), 0),
            (("--title", LONG_TITLE, "--color", "never"), 1),
            (("--title", "", "--format", "json"), 1),
            (("--squash-subject", "fix: ok (#123)\n\nbody", "--format", "json"), 0),
        ):
            with self.subTest(args=args):
                result = self.run_cli(*args)
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertEqual(result.stderr, "")
                if "--format" in args:
                    data = json.loads(result.stdout)
                    self.assertEqual(data["valid"], status == 0)
                    if "--squash-subject" in args:
                        self.assertEqual(data["title"], "fix: ok")
                        self.assertEqual(data["input_kind"], "squash-subject")

    def test_invalid_cli_usage(self):
        for args in (
            (), ("--title",), ("--title", "fix: ok", "--squash-subject", "fix: ok"),
            ("--title", "fix: ok", "--format", "yaml"), ("--title", "fix: ok", "--color", "sometimes"),
            ("--self-test", "--title", "fix: ok"), ("--schema", "--self-test"), ("--schema", "--title", "x"),
        ):
            with self.subTest(args=args):
                result = self.run_cli(*args)
                self.assertEqual(result.returncode, 2)
                self.assertIn("error:", result.stderr)

    def test_schema_and_help(self):
        schema = self.run_cli("--schema")
        self.assertEqual(schema.returncode, 0, schema.stderr)
        self.assertEqual(json.loads(schema.stdout), m.report_schema())
        help_ = self.run_cli("--help")
        self.assertEqual(help_.returncode, 0)
        for flag in ("--schema", "--format", "--color", "--self-test"):
            self.assertIn(flag, help_.stdout)

    def test_main_dispatch_and_missing_test_owner(self):
        for args, status in ((["--title", "fix: ok"], 0),
                             (["--squash-subject", "fix: ok (#12)", "--format", "json"], 0),
                             (["--title", LONG_TITLE], 1), (["--schema"], 0)):
            with self.subTest(args=args), redirect_stdout(io.StringIO()):
                self.assertEqual(m.main(args), status)
        with patch.object(m, "self_test", return_value=0):
            self.assertEqual(m.main(["--self-test"]), 0)
        with patch.object(m.unittest.defaultTestLoader, "discover", return_value=unittest.TestSuite()):
            with self.assertRaisesRegex(RuntimeError, "not discovered"):
                m.self_test()


if __name__ == "__main__":
    unittest.main()
