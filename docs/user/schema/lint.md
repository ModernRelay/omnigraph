# Schema lint

The migration planner emits **code-tagged diagnostics** for every schema change it rejects. Codes have the form `OG-XXX-NNN` and identify the rule (not the message); operators reference them in suppression directives, severity overrides, and CI reports.

This page is the catalog of codes shipped today.

## What's shipped

- Stable code attached to every rejection the planner emits (today: 5 of 17 paths — the rest are tagged as future work).
- Code appears in the user-visible error message: `[OG-DS-104] removing property 'Person.age' is not supported …`.
- CLI `omnigraph schema plan` shows the code on `unsupported change …` lines.

## What's not shipped yet

- Severity configuration (planned: `lint: { OG-DS-103: error }`).
- `@allow(OG-XXX-NNN, "rationale")` suppression directives.
- Pre-migration checks (the `migration_check { … }` block).
- The CD / VE / LK / NM families.
- CI integration.
- Cost-class annotations.

## Code catalog

The chassis defines ten families. Today only DS and MF have emitted codes. The remaining families are reserved for future releases.

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

## Families

The ten chassis families:

| Prefix | Family | Status |
|---|---|---|
| **DS** | Destructive (data-loss) | shipped |
| **MF** | Maybe-fail / data-dependent | shipped |
| **CD** | Constraint deletion (relaxation warning) | planned |
| **BC** | Backward-incompatible (rename) | implicit in `@rename_from`; codify later |
| **NM** | Naming conventions | planned |
| **OW** | Ownership (per-resource Cedar) | planned |
| **NL** | Non-linear (branch-merge divergence) | planned |
| **VE** | Vector / embedding | planned |
| **ED** | Edge / graph topology | planned |
| **LK** | Lock duration / cost | planned |

## Prior art

The chassis is modeled on [Atlas's `sqlcheck` analyzers](https://atlasgo.io/lint/analyzers) (DS / MF / CD / BC / NM families). Atlas was the direct inspiration for stable codes, per-rule severity, suppression directives with rationale, and pre-migration checks. omnigraph adapts the chassis to a typed-IR substrate (no SQL injection vector, no per-engine locking, native vector / edge / embedding types Atlas doesn't have).
