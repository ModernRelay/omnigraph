# Documentation guide

Documentation is part of OmniGraph's public contract. Keep each fact in one
place, at the narrowest audience that needs it.

## Content ownership

| Location | Owns | Does not own |
|---|---|---|
| `docs/user/` | Supported behavior, concepts, workflows, configuration, limits, and operator action | Code structure, recovery protocols, test evidence, design history |
| `docs/dev/` | Current architecture, invariants, support boundaries, and how to change/test the system | Roadmaps, stale implementation plans, release history |
| `docs/rfcs/` | Proposals, decisions, rationale, alternatives, evidence, and disposition | The current user manual or a second implementation reference |
| `docs/releases/` | User-visible changes by release | Evergreen instructions |
| Code and tests | Exact types, fields, constants, and executable assertions | Long-form product guidance |
| Issue tracker | Open work, sequencing, and ownership | Current architecture |

Git history is the archive for superseded drafts. Do not keep a stale design in
`docs/dev/` merely because it may be interesting later.

## User documentation

Write for someone using the CLI, HTTP API, or deployment—not for a contributor
reading the implementation.

- Lead with the task or guarantee.
- Show one canonical command or request, then link to the reference.
- Explain public consequences of atomicity, recovery, and indexing without
  naming internal tables, structs, sidecars, or protocol generations.
- Document supported behavior only. Put planned work in an issue or draft RFC.
- State limits when they change what a user must do; omit internal tuning and
  implementation evidence.
- Give a concept one owner. Other pages use a sentence and a link rather than a
  second explanation.
- Prefer current canonical names. Mention deprecated aliases only in a compact
  compatibility section.

Most authored user pages should fit in 80–220 lines. Longer reference material
must earn its size and should be generated from the defining schema when
practical.

## Developer documentation

Developer docs answer three questions: what is true now, why must it remain
true, and where should a change be made and tested?

- Describe stable components and flows, not mutable source line numbers.
- Link to code for exact serialized shapes and constants.
- Link to an RFC for rationale, rejected alternatives, or historical evidence.
- Keep benchmarks and migration evidence with the release or RFC that used
  them; do not grow an evergreen ledger.
- Remove a plan after it lands. Transfer only the durable outcome into current
  architecture or invariants.
- Name unsupported boundaries plainly without presenting a speculative design
  as current architecture.

## RFCs

Every project RFC lives directly under `docs/rfcs/` and follows the metadata,
filename, lifecycle, and template in [the RFC guide](../rfcs/README.md). There
is one namespace and one lifecycle for public and maintainer-authored RFCs.

An RFC remains a decision record after implementation. Update its disposition,
but keep day-to-day instructions in user or developer docs.

## Review checklist

Before merging documentation:

1. Verify behavior against current code, CLI help, OpenAPI, and existing tests.
2. Check whether another page already owns the concept.
3. Remove future promises and internal detail from user docs.
4. Remove stale plans, evidence ledgers, and duplicated RFC content from
   developer docs.
5. Use relative Markdown links and run the documentation checks.
6. Read the rendered diff for examples, headings, and scanability.

```bash
bash scripts/check-agents-md.sh
python3 scripts/check-docs.py
typos   # from the repository root; exemptions in .typos.toml
```
