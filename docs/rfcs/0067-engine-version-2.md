---
rfc: "0067"
title: "Engine version 2"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-09-14
updated: 2026-09-14
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0067: Engine version 2

> Number provisional: recheck the registry and open reservations before publication.

## Summary

This is a placeholder. It names ***engine version 2***, the next shape of the
OmniGraph engine, as a list of components, so that each component's own RFC
has one parent to cite. It decides nothing about any component and changes no
behavior.

## Motivation

Several component RFCs are in preparation, the planner first. Each needs one
parent that names engine version 2, so the term is defined once and the
components are listed in one place. This document is that parent and nothing
more; every design question belongs to a component RFC.

## User and operational behavior

None. Nothing changes until a component RFC is accepted.

## Design

The table lists the components that have no RFC yet. Components an accepted
RFC already owns (the write protocol, conflict fencing, recovery, schema
identity) are not repeated here.

| Component | Owning RFC |
|---|---|
| Planner and optimizer | not yet proposed |
| Execution engine | not yet proposed |
| Memory management and admission control | not yet proposed |

A component RFC replaces its "not yet proposed" cell with its number when it
lands. Adding or removing a row is a Decision-log entry here.

## Invariants

None affected. Each component RFC lists its own.

## Compatibility and reversibility

None affected. Reverting this document is deleting it.

## Alternatives

Let each component RFC restate the whole scope. Rejected: one parent is cheaper
than one restatement per component.

## Evidence and tests

None required.

## Rollout

Merge as `draft`. `implementation` tracks the component table only.

## Unresolved questions

None. Each component's questions belong to its own RFC.

## Decision log

- 2026-09-14: proposed as a placeholder listing the textbook database
  components that have no RFC yet, none described.
