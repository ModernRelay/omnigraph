---
rfc: "NNNN"
title: "Short descriptive title"
track: maintainer
status: draft
implementation: not-started
authors:
  - Name or handle
created: YYYY-MM-DD
updated: YYYY-MM-DD
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC NNNN: Short descriptive title

Delete every instruction paragraph when it has been answered. Keep the RFC
focused on the decision and evidence; implementation walkthroughs belong in
developer docs unless they are necessary to define the contract.

## Summary

State the decision in one or two paragraphs. Name the behavior or contract that
will exist if accepted and the boundary that will not change.

## Motivation

Describe the concrete problem, evidence that it exists, and why solving it is
worth the long-term maintenance cost. Explain why an issue or local refactor is
not enough.

## User and operational behavior

Show the observable behavior first: commands, APIs, errors, compatibility,
failure posture, and operator responsibilities. Omit code structure unless it
changes the contract.

## Design

Specify the durable architecture, data or protocol shapes, authority boundaries,
and important sequencing. Separate inherited Lance behavior from behavior
OmniGraph adds.

## Invariants

List the affected [architectural invariants](../dev/invariants.md) and explain
why none is weakened. Call out every relevant deny-list item and any known gap
that changes.

## Compatibility and reversibility

Cover storage and wire compatibility, migration, downgrade/refusal behavior,
support boundaries, and the practical cost of reverting this decision.

## Alternatives

Record the strongest alternatives, including doing nothing, and the evidence or
liability that ruled each one out.

## Evidence and tests

Name existing test owners to extend, new evidence that is genuinely required,
fixed acceptance thresholds, and upstream surfaces that were surveyed. Follow
[the testing map](../dev/testing.md) and, for Lance-dependent work,
[the Lance reading protocol](../dev/lance.md).

## Rollout

Give ordered, independently safe phases. State what can ship at each stop, what
remains unavailable, and how `implementation` will advance.

## Unresolved questions

List only decisions that must be settled before acceptance. Evidence gates
belong in `blocked_on`; optional future work belongs out of scope.

## Decision log

Record dated material review outcomes and later amendments here. Do not create
a separate review, final, or pre-merge copy of the RFC.
