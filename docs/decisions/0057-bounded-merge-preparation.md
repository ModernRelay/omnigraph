# Accepted-context reuse and bounded merge preparation

**Decides:** a merge opens each existing target table from the source and target captures its attempt already accepted, and may prepare up to four non-Blob tables at once before effects, while validation, table effects and the one graph publication stay serial.
**Contract:** keeps E1, E2, E5, merge:2, merge:3
**Code:** `crates/omnigraph/src/exec/merge.rs` (`prepare_existing_merge_target`, `PreparedExistingMergeTarget`, `MergeValidationBudget`)
**Cited as:** RFC 0057

## Context

A merge's existing-target preflight called the generic mutation opener, which resolved the branch again and revalidated the schema contract for every touched table, although the merge had already captured that authority and revalidates it before effects. Independent tables were also prepared one after another, so their storage waits did not overlap. Neither cost changes which graph state the merge publishes.

## Decision

- An existing target table opens from the captured `WriteTxn`: its manifest entry, dataset path and native binding, pinned at the captured version. No branch name is resolved again; a logical-name lookup could select a replacement incarnation.
- The opened handle is the one the effect phase stages from; no mutable head is reopened after it is checked.
- Every operation-level fence stays: the schema permit held for the whole merge, the final authority revalidation of both inputs, and the identity-compatibility checks. A failed authority check recaptures the whole view.
- The constraint-validation budget is one operation-wide allowance across all tables.

## Rejected

- **A global schema cache:** adds lifetime and invalidation concerns and does not reuse the accepted native binding.
- **Parallel durable commits or shorter locks:** changes the recovery and interleaving obligations for no measured need.
- **Lance merge-insert in place of the comparison:** an upsert supplies no graph ancestry, three-way conflict semantics or multi-table validation.
- **Persistent catalog trees and precomputed merge-base structures:** address flat catalog and history cost, but need far broader storage and authority changes.

## Not built

- Concurrent preparation of up to four eligible non-Blob tables, in fixed ordered windows whose results join in canonical table order, with Blob preflight and source-state adoption as serial barriers. Every table is prepared serially.
- Workers that write only their own scratch staging and settle before release on error or cancellation; the validation budget shared, never split per worker.
- A parallel retained-bytes allowance whose pressure retries a window at width two, then one, with width one keeping the serial limits.
- A hard width of one at the HTTP entry until a request-independent owner can drain on disconnect.
