---
rfc: "0054"
title: "Default graph actor provenance"
track: maintainer
status: rejected
implementation: removed
authors:
  - Codex
created: 2026-09-06
updated: 2026-09-06
discussion: https://github.com/ModernRelay/omnigraph/issues/661
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0054: Default graph actor provenance

## Decision

Withdraw automatic graph actor materialization and restore the behavior before
[PR #663](https://github.com/ModernRelay/omnigraph/pull/663). Authenticated
actor identity, authorization and commit attribution remain. Applications own
any `Actor`, `OmniActor`, or other provenance node declared in their schema;
these names acquire no system meaning.

This is a withdrawal of the implementation, not a replacement actor design.
Configurable naming and an explicit actor-update command are deferred. No new
migration, background materialization, or managed-only write path is introduced.
The RFC number remains allocated as a durable decision record.

## Original design and reason for withdrawal

The original implementation added a protected `OmniActor` binding to accepted
schema IR, enabled it by default for new graphs, and joined actor-row creation
to the existing atomic content-write protocol. Per-graph configuration and
schema planning controlled enablement. Existing graphs required an explicit
migration, and disabling retained the binding and rows.

That change imposed compatibility costs beyond the intended provenance benefit:

- A bound graph used schema IR version 3, including after opt-out, so older
  readers could not open it.
- New required public fields and enum variants broke source compatibility for
  library consumers.
- Plain `schema show` gained commentary and no longer emitted reusable `.pg`
  source alone.
- Cluster observation could persist `actor_provenance: false` into a legacy
  ledger even without enabling the feature; older readers rejected that field.
- Protected rows could appear in exports while ordinary load refused them.
  Some attributed schema writes also acquired a new refusal condition.

Renaming the built-in alone would leave these costs in place. Withdrawing the
feature restores the existing contracts without committing to a second design.
The unrelated recovery-test failpoint isolation fix from the same PR remains.

## Restored contracts

- New graphs use schema IR version 2 and only their declared schema types.
- Writes publish customer content through the existing graph commit protocol;
  they do not create actor rows automatically.
- Authenticated HTTP writes still derive their actor from the verified bearer
  credential. Current policy and signed grant checks remain mandatory under
  [RFC 0053](0053-offline-data-token-verification.md).
- Managed execution can retain `principal:<principal_id>` as commit attribution
  without creating a corresponding node. Direct execution keeps its explicit
  actor attribution.
- CLI schema output, public option/output types, cluster configuration and
  ledger serialization return to their pre-feature shapes.

## Compatibility boundary

The withdrawal does not downgrade stored data. A graph already written with
schema IR version 3 is unsupported and must be refused before graph effects;
its binding must never be silently stripped or reinterpreted as version 2.
Likewise, actor configuration fields and actor-extended cluster ledgers are
not accepted by the restored strict readers. Disabling the old feature was
not a downgrade and does not make those artifacts compatible.

Before replacing a binary that used this implementation, inspect the accepted
schema and cluster ledger. Existing version-2 graphs with unextended ledgers
need no actor migration. A version-3 graph or actor-extended ledger requires a
separate, explicit recovery decision; this revert supplies no in-place
conversion. Ordinary customer tables named `Actor` or `OmniActor` are unrelated
to that format boundary and retain normal schema, load and export behavior.

## Validation ownership

Extend the existing owners in [testing](../dev/testing.md): compiler and engine
lifecycle for version-2 defaults and version-3 refusal before effects; cluster
state tests for legacy serialization; CLI schema/config tests for reusable
stdout; and existing engine/server authorization and attribution tests.
Run write, schema, merge, recovery and deterministic simulation suites to
verify removal of the additional participant preserves graph atomicity.

## Decision log

- **2026-09-06:** Accepted and implemented the original default-materialization
  design in PR #663.
- **2026-09-06:** Maintainer requested its reversion after compatibility review.
  Withdraw the original default, protected binding, actor options and format
  extension. This disposition replaces the original implementation contract;
  its full design remains in Git history. Keep authentication, permissions,
  commit attribution and independent fixes. Defer further actor design work.
