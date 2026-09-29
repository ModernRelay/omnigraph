# Designing Ontologies for Agents

When many agents read and write one graph, the ontology stops being
documentation and becomes the coordination layer. It does three jobs a
human-facing schema never had to do:

- **An address**: where a fact goes, so two agents that observe the same thing
  write it to the same place.
- **A contract**: what a stored value means, so an agent reading it understands
  what the writer meant.
- **A substrate**: the paths along which agents reason, so a question becomes a
  traversal instead of a fresh search.

People repair ambiguity with judgment; agents do not, and every ambiguity
multiplies by the number of writers. The design goal that follows from this is
**convergence**: independent agents given the same input should produce the
same graph, and read the same meaning back. Each principle below serves that
goal. They complement Gruber's criteria and the provenance rule in
[`SKILL.md`](../SKILL.md) and the authoring rules in [`schema.md`](schema.md).

## Contents

1. [Design identity first](#1-design-identity-first)
2. [Kinds are types; roles are edges](#2-kinds-are-types-roles-are-edges)
3. [Put a fact on what determines it](#3-put-a-fact-on-what-determines-it)
4. [Keep independent questions on independent axes](#4-keep-independent-questions-on-independent-axes)
5. [Represent assertions, not only truth](#5-represent-assertions-not-only-truth)
6. [Store what was observed or decided; compute the rest](#6-store-what-was-observed-or-decided-compute-the-rest)
7. [Close vocabularies where agents must converge](#7-close-vocabularies-where-agents-must-converge)
8. [Make meaning enforceable, and keep the rules in the graph](#8-make-meaning-enforceable-and-keep-the-rules-in-the-graph)
9. [Compose around a shared core](#9-compose-around-a-shared-core)
10. [Shape the graph for the questions agents ask](#10-shape-the-graph-for-the-questions-agents-ask)
11. [Let independence decide granularity](#11-let-independence-decide-granularity)
12. [Write names and descriptions for a reader without context](#12-write-names-and-descriptions-for-a-reader-without-context)
13. [Keep meanings stable over time](#13-keep-meanings-stable-over-time)
14. [Measure convergence](#14-measure-convergence)

## 1. Design identity first

Identity is the coordinate system. Two agents meet at a fact only if they give
it the same address, and most coordination failures are identity failures:
duplicates that split what is known about one thing, or merges that fuse two
things.

- Derive identity from the thing itself (a natural key, or a key built from
  what the thing is), never from who wrote it or when.
- Identity survives a rename and does not survive drop-and-recreate.
- Entities and statements both need a *find before create* step. Statements are
  harder: the same proposition arrives in different words.

**In Omnigraph:** use `@key` on a semantic slug or a composite `@key(a, b)`;
give edges `@key(@src, @dst[, prop])` so repeated inserts upsert instead of
duplicating, and load with `--mode merge`. Put an `@embed` vector on text that
agents must deduplicate, populate it with `omnigraph embed`, and have writers
run a `nearest(...)` lookup before inserting. Use `@rename_from` so a rename
keeps identity and history.

## 2. Kinds are types; roles are edges

Kinds rarely change; roles change constantly. A customer becomes a partner, a
hypothesis becomes a refuted claim. Typing a role (`Objection`, `Evidence`,
`FormerCustomer`) forces re-typing, and re-typing breaks identity. Give a thing
its own node type only when at least one holds:

- it means something different (true or false versus not, evidence versus
  illustration, a document versus a proposition);
- it has its own identity and lifecycle, referenced from many places;
- a check you care about is wrong without it.

**In Omnigraph:** express roles as typed edges (`AttacksClaim`, `CustomerOf`)
and let queries find nodes by the edges they have. Use interfaces for shape
that several kinds genuinely share, not to simulate roles.

## 3. Put a fact on what determines it

This is normalization applied to meaning: a property belongs on the smallest
set of things that determines its value. Confidence depends on who holds a
claim, so it belongs on the holder-to-claim relation, not on the claim. A price
depends on the contract and the date, not only on the product. A misplaced fact
produces contradictions, and many concurrent writers multiply them.

**In Omnigraph:** edges carry properties, so a relationship-dependent fact goes
on the edge: `edge Holds: Party -> Claim { stance: enum(accepts, rejects),
strength: enum(weak, moderate, strong)?, as_of: Date? }`. When the relationship
itself has several participants, a lifecycle, or must be pointed at, promote it
to a node (see [`SKILL.md`](../SKILL.md#provenance-is-structural-multi-agent-source-of-truth)).

## 4. Keep independent questions on independent axes

Identity, content, who said it, why to believe it, how sure and for whom, when
it was true, when it was recorded, and who may see it can each change without
the others. Fold two into one field or edge and changing one silently changes
the other. For agents, provenance and time are not metadata: they are how one
agent decides whether to trust what another wrote.

**In Omnigraph:** give each axis its own edge or property: a `StatedIn` edge
to a `Source` for attribution, separate edges or nodes for justification,
`DateTime` properties for valid time, and the commit history for record time.
Do not overload one enum such as `relation: enum(asserts, supports)` to mean
both "who says it" and "why believe it".

## 5. Represent assertions, not only truth

Agents are fallible writers. The graph must be able to say that someone
claimed X, from some source, with some confidence, and that someone else
disputes it. Contradictions must be representable rather than resolved by the
last write, and changes should be superseded and kept, not overwritten. A model
that can hold only "the truth" forces every agent to settle disputes before
writing, which is exactly where errors enter unseen.

**In Omnigraph:** model assertions as nodes linked to their sources and
holders, and link replacements with a `Supersedes` edge instead of editing in
place. Propose changes that need judgment on a branch and merge them after
review; every commit stays readable with `commit list` and `snapshot`, so a
wrong fact can be traced to the write that introduced it.

## 6. Store what was observed or decided; compute the rest

Derived facts stored as data drift out of sync the moment anyone writes, and
with many writers that is constantly. Whether a claim is contested, how many
sources support it, a rollup, a status that follows from other facts: these are
queries. A stored copy that can be recomputed is a second source of truth.

**In Omnigraph:** keep derived views as stored queries and aliases, and have
agents follow `omnigraph changes poll` to refresh anything they cache. Store a
status only when it records a decision someone made (retracted, approved), not
a condition the graph already implies.

## 7. Close vocabularies where agents must converge

A closed vocabulary with crisp, testable definitions makes agents agree; free
text guarantees they will not. Keep a visible catch-all, though, and watch it:
when the default bucket fills up, a category is missing. Extend deliberately:
agents propose new values or types, owners accept them. Commit to as little as
possible, but define tightly whatever you commit to.

**In Omnigraph:** prefer `enum(...)` with a description of each value over
`String`. Enum widening applies in place, so adding a value is cheap; removing
one is not. Count usage per value (`count(...)` grouped by the enum) to spot a
swelling catch-all. Route agent-proposed schema changes through `schema plan`
(or `cluster plan`) and a human-approved apply.

## 8. Make meaning enforceable, and keep the rules in the graph

Meaning that is not checked drifts. Enforce every structural rule the schema
can express. Semantic rules the schema cannot express ("a reason has exactly
one target", "no circular support", "an observation used as evidence has a
source") should live as data agents can read and apply, not in a document they
never see.

**In Omnigraph:** use `@key`, `@unique`, `@card`, `@range` and `@check` for
structure. Store semantic rules as nodes, for example `node Policy { slug:
String @key, statement: String, severity: enum(error, warning, info), status:
enum(active, draft, retired) }`, and give agents stored queries using `not {
... }` negation to find what breaks them.

## 9. Compose around a shared core

A small core of shared identities (people, organizations, products, places)
with domain modules attached lets each domain own its part while everything
meets on the same entities. A new module should attach without changing the
existing ones. Outside vocabularies and frameworks enter as mappings and
views, not as new core types. Two ontologies compose when they share
identities, not when they share a top-level taxonomy.

**In Omnigraph:** keep core entity types and their keys stable, attach
module-specific types by edges to them, and reference other graphs by slug
(for example an `atlas_ref: String?` property) rather than copying their
nodes.

## 10. Shape the graph for the questions agents ask

The ontology is where reasoning happens. The questions agents ask most should
be short traversals, and relationships an agent would otherwise reconstruct at
run time should be stored as links. A link written once saves every later
reader a search that might miss it: it turns recall into enumeration. If an
important question needs text search or a long chain of inference, a relation
is missing.

**In Omnigraph:** write the questions first as `.gq` queries and lint them
against the schema. Scope with traversal, then rank with `nearest`, `bm25` or
`rrf` inside the scoped set (see [`search.md`](search.md)).

## 11. Let independence decide granularity

Split a node when its parts can be true, false, disputed, updated or cited
independently; otherwise keep it whole. Too coarse, and agents cannot point at
the part they mean. Too fine, and every write becomes a modelling decision on
which agents diverge.

**In Omnigraph:** a value that is disputed or cited on its own becomes a node
with its own key; a value that only ever changes with its parent stays a
property.

## 12. Write names and descriptions for a reader without context

An agent reads the schema as instructions. One word should mean one thing,
descriptions should say when to use a type and when not to, and nothing should
rely on context the reader does not have. An ambiguous name is interpreted
differently by each agent, and nothing reconciles them.

**In Omnigraph:** put `@description("...")` on properties and
`@instruction("...")` on node and edge types; agents see them in
`schema show`. Name edges for their meaning (`AuthoredBy`, not `RelatedTo`).

## 13. Keep meanings stable over time

Never repurpose a field: deprecate it and add a new one, because agents built
against the old meaning keep writing it. A meaning that shifts silently
corrupts every agent and every stored record that relied on it.

**In Omnigraph:** rename with `@rename_from`, add replacements as new nullable
properties, backfill, then drop the old property with a planned `schema
apply` (see [`schema.md`](schema.md#rename-dont-replace)).

## 14. Measure convergence

Looseness is measurable, so measure it before committing. Give two independent
agents the same input and compare what they write. Agreement shows where the
ontology is crisp; disagreement points to the fix:

| Symptom | Missing |
|---|---|
| One value absorbs most writes | a category |
| Agents choose consistently but differently | a rule |
| Both force the same content into an awkward shape | a type |

Duplicate rate, rule-violation rate, and the share of key questions answerable
by traversal are the other gauges.

**In Omnigraph:** load each agent's output into its own branch or scratch
graph, and compare with the same aggregate queries you use in production.

---

The short form: an ontology for agents succeeds when writing becomes
deterministic and reading becomes unambiguous. Every free choice at write time
is a coordination cost later; every relation not stored is a reasoning cost
later.
