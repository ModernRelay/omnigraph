# Schema lint

The migration planner emits **code-tagged diagnostics** for every schema change it rejects. Codes have the form `OG-XXX-NNN` and identify the rule (not the message); operators reference them in suppression directives, severity overrides, and CI reports.

This page is the catalog of codes shipped today. The chassis behind it is tracked in [MR-694](https://linear.app/modernrelay/issue/MR-694).

## What's shipped in v0

- Stable code attached to every rejection the planner emits (today: 5 of 17 paths — the rest carry `code: None` and are tagged as future work).
- Code appears in the user-visible error message: `[OG-DS-104] removing property 'Person.age' is not supported …`.
- CLI `omnigraph schema plan` shows the code on `unsupported change …` lines.
- Tests in `tests/schema_apply.rs` assert on codes, not on free-text prose.

## What's not shipped yet

- Severity configuration in `omnigraph.yaml` (planned: `lint: { OG-DS-103: error }`).
- `@allow(OG-XXX-NNN, "rationale")` suppression directives.
- Pre-migration checks (the `migration_check { … }` block — MR-941).
- The CD / VE / LK / NM families (MR-942..945).
- CI integration (MR-946).
- Cost-class annotations (MR-944).

See the parent chassis issue (MR-694) for the design and the per-family sub-issues for what's planned.

## Code catalog (v0)

The chassis defines ten families. Today only DS and MF have emitted codes. The remaining families are reserved for future PRs.

| Code | Family | Tier | Default severity | Meaning |
|---|---|---|---|---|
| `OG-DS-101` | Destructive | destructive | error | drop graph type with rows (reserved; not yet emitted) |
| `OG-DS-102` | Destructive | destructive | error | drop node type with rows |
| `OG-DS-103` | Destructive | destructive | error | drop edge type with rows |
| `OG-DS-104` | Destructive | destructive | error | drop property with rows |
| `OG-DS-105` | Destructive | destructive | error | drop populated vector column (reserved) |
| `OG-MF-103` | Maybe-fail | validated | error | add required property without `@default` to populated type |
| `OG-MF-104` | Maybe-fail | validated | error | tighten nullable to non-nullable (reserved) |
| `OG-MF-106` | Maybe-fail | destructive | error | narrowing scalar type |

The full code catalog source of truth lives in `crates/omnigraph-compiler/src/lint/codes.rs`. CI-level invariants (uniqueness, format, family coverage) are unit-tested in the same module.

## Families

The ten chassis families:

| Prefix | Family | Status |
|---|---|---|
| **DS** | Destructive (data-loss) | shipped, v0 |
| **MF** | Maybe-fail / data-dependent | shipped, v0 |
| **CD** | Constraint deletion (relaxation warning) | tracked in MR-942 |
| **BC** | Backward-incompatible (rename) | implicit in `@rename_from`; codify later |
| **NM** | Naming conventions | tracked in MR-945 |
| **OW** | Ownership (per-resource Cedar) | tracked in MR-722 |
| **NL** | Non-linear (branch-merge divergence) | stubbed in MR-947 |
| **VE** | Vector / embedding | tracked in MR-943 |
| **ED** | Edge / graph topology | tracked in MR-701, MR-943 |
| **LK** | Lock duration / cost | tracked in MR-944 |

## Prior art

The chassis is modeled on [Atlas's `sqlcheck` analyzers](https://atlasgo.io/lint/analyzers) (DS / MF / CD / BC / NM families). Atlas was the direct inspiration for stable codes, per-rule severity, suppression directives with rationale, and pre-migration checks. omnigraph adapts the chassis to a typed-IR substrate (no SQL injection vector, no per-engine locking, native vector / edge / embedding types Atlas doesn't have).
