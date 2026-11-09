# Edition-3 test data

Copies of the GQ 3 RFC's evidence, which the compiler's edition-3 tests
read, stored as `.txt` so the documentation link check does not follow the
asset's links to an RFC that is not merged. The RFC is a draft
(https://github.com/ModernRelay/omnigraph/pull/1302) on the `rfc/gq-3`
branch of `ragnorc/omnigraph`; both copies are from its commit `d0155e50`.

| File | Source at `d0155e50` | Read by |
|---|---|---|
| `2026-11-07-gq-3-before-and-after.txt` | `docs/rfcs/assets/2026-11-07-gq-3-before-and-after.md`, byte for byte | `parser3_tests`, `typeck3_tests` |
| `2026-11-07-gq-3-rfc-examples.txt` | the fenced `gq` blocks of `docs/rfcs/2026-11-07-gq-3.md`, in order | `parser3_tests::rfc_section_8_examples_parse` |
| `gq3-redteam/` | the red-team corpus of agent-written edition-3 texts, each with its expected outcome in its header | `parser3_redteam` |

Refresh a copy when the RFC changes, and update the commit above.
