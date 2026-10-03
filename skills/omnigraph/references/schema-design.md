# Schema Design

Omnigraph schemas are ontologies. This page is the one home for designing
them: the classic criteria, a design flow, and the principles that matter once
many agents read and write the same graph. For syntax, decorators and
evolution mechanics, see [`schema.md`](schema.md).

## Contents

- [Foundations: Gruber's five criteria](#foundations-grubers-five-criteria)
- [Too loose, too tight](#too-loose-too-tight)
- [Design flow](#design-flow)
- [Designing for many agents](#designing-for-many-agents)
  1. [Design identity first](#1-design-identity-first)
  2. [Kinds are types; roles are edges](#2-kinds-are-types-roles-are-edges)
  3. [Put a fact on what determines it](#3-put-a-fact-on-what-determines-it)
  4. [Keep independent questions on independent axes](#4-keep-independent-questions-on-independent-axes)
  5. [Make provenance structural](#5-make-provenance-structural)
  6. [Layer derived knowledge over raw sources](#6-layer-derived-knowledge-over-raw-sources)
  7. [Store what was observed or decided; compute the rest](#7-store-what-was-observed-or-decided-compute-the-rest)
  8. [Use narrow types and close vocabularies](#8-use-narrow-types-and-close-vocabularies)
  9. [Enforce meaning, and keep the rules in the graph](#9-enforce-meaning-and-keep-the-rules-in-the-graph)
  10. [Compose around a shared core, from general primitives](#10-compose-around-a-shared-core-from-general-primitives)
  11. [Optimize for retrieval: recall and precision](#11-optimize-for-retrieval-recall-and-precision)
  12. [Let independence decide granularity](#12-let-independence-decide-granularity)
  13. [Write the schema for a reader without context](#13-write-the-schema-for-a-reader-without-context)
  14. [Keep meanings stable over time](#14-keep-meanings-stable-over-time)
  15. [Measure convergence](#15-measure-convergence)

## Foundations: Gruber's five criteria

The canonical criteria from Gruber's *Toward Principles for the Design of
Ontologies Used for Knowledge Sharing* (Int. J. Human-Computer Studies
43:907–928) apply directly to `.pg` files.

1. **Clarity**: definitions communicate intended meaning unambiguously,
   independent of social or computational context. In Omnigraph: precise type
   names, narrow enums over `String`, `@check`/`@range` for stated invariants.
   A reviewer should understand the domain from the schema alone.
2. **Coherence**: inferences the schema sanctions are consistent with the
   domain. Gruber's trap: defining quantity as a `(magnitude, unit)` pair makes
   `6 feet ≠ 2 yards` even though they describe the same length. In Omnigraph:
   watch for `@card`, `@unique` and edge directionality that let the schema
   distinguish things the domain treats as equal.
3. **Extendibility**: the schema supports specialization without revising
   existing definitions. In Omnigraph: interfaces for shared shape, enums left
   open where the domain genuinely admits more, identifiers modelled through
   mappings rather than units or formats baked into the entity.
4. **Minimal encoding bias**: choices made for notation or implementation
   convenience do not leak into the model. In Omnigraph: don't type dates as
   `String` because a source API returns strings; separate a conceptual entity
   (a publication date, a person) from its surface encoding (a year integer, a
   name string) when both matter.
5. **Minimal ontological commitment**: make as few claims about the world as
   the use case requires. In Omnigraph: no required properties, closed enums or
   `@card(1..1)` "in case". Tightening later is a rebuild, not an in-place
   `schema apply`: adding `@key`/`@unique`/`@range`/`@check`, changing
   cardinality, and `T?` → `T` are refused; only `@index` additions, nullable
   additions, enum widening, renames and drops apply in place.

The criteria trade off: Clarity wants tight definitions, Minimal Commitment
wants weak ones. Gruber's resolution is to decide conservatively what to model
and, having decided a distinction is worth making, give it the tightest
possible definition.

## Too loose, too tight

A schema fails in two opposite ways:

- **Too loose**: the same meaning can be written in more than one valid way.
  Independent writers produce different graphs, duplicates accumulate, and a
  query finds only what happened to be written its way.
- **Too tight**: some meaning cannot be written at all. Writers force it into
  the nearest shape, park it in a note, or drop it, and the loss is silent.

The goal is both properties at once: everything that matters can be
expressed, and there is exactly one way to express it.

These are not two ends of one slider. A missing category makes an enum too
tight and, at the same time, turns its most generic value into a dumping
ground. A property placed on the wrong node blocks what should be expressible
and invites parallel workarounds. Adding or removing constraints fixes
neither; putting the distinction where it lives fixes both. The lever is where
constraints sit, not how many there are: tight on identity, kinds, roles and
shared vocabulary, which writers must converge on; open at declared extension
points where the domain genuinely varies, with a review path for extending
them.

**Example:** `status: String` is too loose: agents write "closed", "Closed",
"done" and "won" for one state. An enum with no `paused` value is too tight:
agents file paused deals as `lost`, and the error is silent. The fix for both
is the same enum with the missing value added.

Measure the two failures separately. Looseness shows up as disagreement
between independent writers given the same input. Tightness shows up as
content they report they could not express, or both forced into the same
awkward shape (see [principle 15](#15-measure-convergence)).

**In Omnigraph:** loosening applies in place (enum widening, nullable
additions); tightening is a rebuild, and loose data written in the meantime
must be reconciled first. When many agents write, lean tight where convergence
matters and loosen deliberately when the evidence shows a real need.

## Design flow

1. Write the questions the graph must answer as `.gq` queries first.
2. Entities, and the stable key of each.
3. Layers: which types are raw spans, extracted facts and synthesized
   conclusions, and how each links to what it came from.
4. Relationships worth their own edge, named for their meaning.
5. Enum candidates, narrow types, and which properties are truly required.
6. Uniqueness, bounds and cardinality.
7. Search needs: which text gets `@embed`, which properties get `@index`.
8. Provenance: who asserts what, from which source.
9. Rules the schema cannot express, as `GraphPolicy` nodes (principle 9).
10. Shared shape into interfaces.
11. An evolution plan: what may change in place and what would be a rebuild.

Lint the queries against the schema after each step.

## Designing for many agents

When many agents read and write one graph, the schema stops being
documentation and becomes the coordination layer. It does three jobs a
human-facing schema never had to do:

- **An address**: where a fact goes, so two agents that observe the same thing
  write it to the same place.
- **A contract**: what a stored value means, so an agent reading it understands
  what the writer meant.
- **A substrate**: the paths along which agents reason, so a question becomes a
  traversal instead of a fresh search.

People repair ambiguity with judgment; agents do not, and every ambiguity
multiplies by the number of writers. The design goal that follows is
**convergence**: independent agents given the same input should produce the
same graph and read the same meaning back. Every free choice at write time is
a coordination cost later; every relation not stored is a reasoning cost later.

### 1. Design identity first

Identity is the coordinate system. Two agents meet at a fact only if they give
it the same address, and most coordination failures are identity failures:
duplicates that split what is known about one thing, or merges that fuse two
things.

- Derive identity from the thing itself (a natural key, or a key built from
  what the thing is), never from who wrote it, when, or an internal row id.
- Identity survives a rename and does not survive drop-and-recreate.
- Entities and statements both need a *find before create* step. Statements are
  harder: the same proposition arrives in different words.

**In Omnigraph:** use `@key` on a semantic slug or a composite `@key(a, b)`;
give edges `@key(@src, @dst[, prop])` so repeated inserts upsert instead of
duplicating, and load with `--mode merge`. Put an `@embed` vector on text that
agents must deduplicate, populate it with `omnigraph embed`, and have writers
run a `nearest(...)` lookup before inserting. Use `@rename_from` so a rename
keeps identity and history.

**Example:**

```pg
node Customer {
    slug: String @key            // derived from the legal entity: "acme-gmbh", never a writer's UUID
    name: String
    name_embedding: Vector(3072)? @embed("name")   // writers run nearest() on it before inserting
}
edge Attended: Person -> Meeting { @key(@src, @dst) }   // recording the same attendance twice upserts
```

### 2. Kinds are types; roles are edges

Kinds rarely change; roles change constantly. A customer becomes a partner, a
hypothesis becomes a refuted claim. Typing a role (`Objection`, `Evidence`,
`FormerCustomer`) forces re-typing, and re-typing breaks identity. Give a thing
its own node type only when at least one holds:

- it means something different (true or false versus not, evidence versus
  illustration, a document versus a proposition);
- it has its own identity and lifecycle, referenced from many places;
- a check you care about is wrong without it.

Model meaning, not tables: a relationship is an edge, not a join table or a
foreign-key string, and ORM habits (a `type` column, a generic `Entity` node)
hide the domain from every reader.

**In Omnigraph:** express roles as typed edges (`AttacksClaim`, `CustomerOf`)
and let queries find nodes by the edges they have.

**Example:** a former customer is not a kind of organization; it is an
organization whose customer relationship ended.

```pg
// avoid: node FormerCustomer { ... }
edge CustomerOf: Organization -> Organization {
    since: Date
    until: Date?        // set when the relationship ends; no re-typing
}
```

### 3. Put a fact on what determines it

This is normalization applied to meaning: a property belongs on the smallest
set of things that determines its value. Confidence depends on who holds a
claim, so when many parties hold one claim it belongs on the holder-to-claim
relation, not on the claim. A price depends on the contract and the date, not
only on the product. A misplaced fact produces contradictions, and many
concurrent writers multiply them.

**In Omnigraph:** edges carry properties, so a relationship-dependent fact goes
on the edge: `edge Holds: Party -> Claim { stance: enum(accepts, rejects),
strength: enum(weak, moderate, strong)?, as_of: Date? }`. When the relationship
itself has several participants, a lifecycle, or must be pointed at, promote it
to a node.

**Example:** a unit price depends on the contract, not only on the product.

```pg
// avoid: node Product { price: F64 }
edge Prices: Contract -> Product {
    unit_price: F64
    valid_from: Date
}
```

### 4. Keep independent questions on independent axes

Identity, content, who said it, why to believe it, how sure and for whom, when
it was true, when it was recorded, and who may see it can each change without
the others. Fold two into one field or edge and changing one silently changes
the other.

**In Omnigraph:** give each axis its own edge or property: a `StatedIn` edge
to a `Source` for attribution, separate edges or nodes for justification,
`DateTime` properties for valid time, and the commit history for record time.
Do not overload one enum such as `relation: enum(asserts, supports)` to mean
both "who says it" and "why believe it".

**Example:**

```pg
// avoid: edge Cites: Assertion -> Document { relation: enum(says_so, supports) }
edge StatedIn: Assertion -> Document { quote: String? }   // who says it
edge Supports: Assertion -> Assertion                     // why believe it
```

### 5. Make provenance structural

When a graph is canonical truth across agents, every assertion must answer
*who said it, when, based on what evidence*. This is the guarantee Gruber's
criteria don't cover: his agents shared vocabulary; ours must also share
attribution. Without it, agents cannot reconcile contradictory assertions,
retract facts when a source is discredited, replay the graph at a past point,
or tell high-evidence facts from speculation.

Represent assertions, not only truth. Agents are fallible writers: the graph
must be able to say that someone claimed X, from some source, and that someone
else disputes it. Contradictions must be representable rather than resolved by
the last write, and changes superseded and kept rather than overwritten.

**In Omnigraph:** model provenance as a `Claim` node linked by typed edges to
the asserted fact, an `Actor` and a `Source`. A `Claim` records one actor's
assertion, so scalar facts such as `asserted_at: DateTime` and an optional
`confidence: F64` belong on it; properties cannot be node-typed. Never stash
provenance in a free-text `source: String` or a metadata dump: structural
provenance is queryable and migratable, free-form provenance is neither. Link
replacements with a `Supersedes` edge, propose changes that need judgment on a
branch and merge them after review, and use `commit list` and `snapshot` to
trace a wrong fact to the write that introduced it.

**Example:**

```pg
node Claim {
    slug: String @key
    statement: String
    asserted_at: DateTime
    confidence: F64?
}
edge ClaimAbout: Claim -> Customer
edge AssertedBy: Claim -> Actor
edge FromSource: Claim -> Source
edge Supersedes: Claim -> Claim   // the newer claim points at the one it replaces
```

### 6. Layer derived knowledge over raw sources

People curate a model; agents grow one. An agent-maintained graph is closer to
an index than to a catalogue: agents continuously read raw material
(transcripts, documents, messages, tickets) and write structured knowledge on
top of it, and every later agent reads both. Design the layers explicitly:

- **Raw**: what was captured, kept as captured and never rewritten by
  interpretation, split into addressable spans (a transcript segment, a
  document chunk) so later layers can point at exactly what they came from.
- **Extracted**: entities, events and assertions an agent pulled out of the
  raw layer, each linked to the span it came from and to the extractor that
  produced it (model and prompt version).
- **Synthesized**: conclusions, summaries and judgments built from extracted
  facts, linked to what they rest on.

Lineage makes enrichment compound and stay correctable. A better model
re-extracts from the same spans; a discredited span takes down everything
built on it; and an answer can be traced from a conclusion down to the words
that support it. The layers also differ in what may be rebuilt. Raw material
and human decisions are primary and are never regenerated. Machine extractions
are materialized derivations: stored because they are expensive, but
reproducible from raw plus the extractor, so key them deterministically (span
plus extractor) and re-runs stay idempotent.

**In Omnigraph:** model raw spans as nodes (`TranscriptSegment`, `Chunk`) keyed
on their source and position; give extracted nodes an edge to their span and
an `extracted_by` property; link synthesized nodes to what they derive from.
Drive enrichment from `omnigraph changes poll`, and run a re-extraction on a
branch so its effect can be reviewed before it replaces the old layer.

**Example:**

```pg
node TranscriptSegment {                    // raw: kept as captured
    slug: String @key                       // transcript slug + position
    text: String
    start_ms: I64
}
node Assertion {                            // extracted
    slug: String @key                       // span + extractor, so re-runs upsert
    statement: String
    extracted_by: String                    // model and prompt version
    embedding: Vector(3072)? @embed("statement")
}
edge ExtractedFrom: Assertion -> TranscriptSegment @card(1..)
node Conclusion {                           // synthesized
    slug: String @key
    statement: String
}
edge DerivedFrom: Conclusion -> Assertion @card(1..)
```

### 7. Store what was observed or decided; compute the rest

Derived facts stored as data drift out of sync the moment anyone writes, and
with many writers that is constantly. Whether a claim is contested, how many
sources support it, a rollup, a status that follows from other facts: these are
queries. A stored copy that can be recomputed is a second source of truth.

**In Omnigraph:** keep derived views as stored queries and aliases, and have
agents follow `omnigraph changes poll` to refresh anything they cache. Store a
status only when it records a decision someone made (retracted, approved), not
a condition the graph already implies.

**Example:** store `retracted_at` because someone decided it; compute how
well supported an assertion is instead of storing a `support_count`.

```gq
query support_count($slug: String) {
    match {
        $a: Assertion { slug: $slug }
        $s supports $a
    }
    return { count($s) as supporting }
}
```

### 8. Use narrow types and close vocabularies

A closed vocabulary with crisp, testable definitions makes agents agree; free
text guarantees they will not. Use the narrowest type that fits: `Date` over
`String` for dates, `enum` over `String` for states. Keep a visible catch-all
in open-ended enums and watch it: when the default bucket fills up, a category
is missing. Decide optionality deliberately: `T?` → `T` later is a rebuild,
and a new required property needs a backfill plan (see
[`schema.md`](schema.md#required-properties-need-a-backfill-plan)).

**In Omnigraph:** describe each enum value, and remember that enum widening
applies in place while removing a value does not. Count usage per value (an
aggregate over the enum property; grouping is implicit) to spot a swelling
catch-all. Route agent-proposed vocabulary or schema changes through `schema
plan` (or `cluster plan`) and a human-approved apply.

**Example:**

```pg
node Deal {
    slug: String @key
    stage: enum(prospecting, discovery, pilot, contract, won, lost, other)   // `other` is the visible catch-all
    close_date: Date?                                                         // Date, not String
}
```

```gq
query stage_mix() {
    match { $d: Deal }
    return { $d.stage, count($d) as deals }   // a growing `other` means a stage is missing
}
```

### 9. Enforce meaning, and keep the rules in the graph

The schema is the contract: every invariant the schema can express belongs in
it, not in application code. Meaning that is not checked drifts. Rules the
schema cannot express ("a reason has exactly one target", "no circular
support", "an observation used as evidence has a source", "our claims avoid
rejected terms") belong in the graph as data, where every agent can read them,
not in a document agents never see.

Give every graph that agents share a `GraphPolicy` node type. It is one of the
most useful node types in any agent-maintained graph: rules become versioned,
reviewable data that changes without redeploying anything, and agents can
validate or lint the graph against them.

```pg
node GraphPolicy
    @instruction("A rule this graph must satisfy. Read active policies before writing; lint the graph against them and fix what breaks.")
{
    slug: String @key
    name: String
    statement: String @description("The rule, and how to fix a break")
    rationale: String?
    severity: enum(error, warning, info) @index
    check_query: String? @description("Stored query that returns breaks, when the rule is mechanical")
    status: enum(active, draft, retired) @index
}
```

Agents use it in three ways:

- **Before writing**, read the active policies, so writes conform from the
  start.
- **To lint**, run each policy's `check_query` for mechanical rules (usually a
  `not { ... }` negation that returns the rows that break it), and apply the
  `statement` with judgment for rules a query cannot express.
- **To fix**, correct small breaks directly and propose larger ones on a branch
  for review.

`GraphPolicy` governs what the graph must satisfy. It is separate from the
server's Cedar policy, which governs who may perform which actions (see
[`server-policy.md`](server-policy.md)).

**In Omnigraph:** use `@key`, `@unique`, `@card`, `@range` and `@check` for
structure, `GraphPolicy` nodes for everything else, and a stored query per
mechanical rule.

**Example:** "every strategic customer has an owner" is conditional
cardinality, which `@card` cannot express, so it becomes a policy with a check
query:

```gq
// GraphPolicy pol-strategic-has-owner, check_query: strategic_without_owner
query strategic_without_owner() {
    match {
        $c: Customer { tier: "strategic" }
        not { $c accountOwner $p }
    }
    return { $c.slug }
}
```

### 10. Compose around a shared core, from general primitives

A small core of shared identities (people, organizations, products, places)
with domain modules attached lets each domain own its part while everything
meets on the same entities. A new module should attach without changing the
existing ones. Outside vocabularies and frameworks enter as mappings and
views, not as new core types. Two ontologies compose when they share
identities, not when they share a top-level taxonomy.

Modules add to the core and never modify it. When sales and support each need
their own view of a customer, each attaches its own facet node or edge rather
than widening the shared `Customer` type, so the core stays small and stable
and a module can be added or removed without touching the others.

Compose relations the same way. A few general primitives that combine, where
a relation can target another relation, express new kinds of statement
without new types. One `Reason` node with a `polarity` (pro or con) that
targets either a claim or another reason yields support, objection, backing
and undercut; four special-purpose edge types would express less and compose
with nothing.

**In Omnigraph:** keep core entity types and their keys stable and attach
module-specific types by edges to them. Use interfaces when three or more node
types genuinely share a property contract. Reference other graphs by slug (for
example an `atlas_ref: String?` property) rather than copying their nodes.

**Example:**

```pg
// core, shared by every module
node Person { slug: String @key }
node Organization { slug: String @key }
// sales module
node Deal { slug: String @key }
edge DealWith: Deal -> Organization @card(1..1)
// support module
node Ticket { slug: String @key }
edge RaisedBy: Ticket -> Person
// a module's view of a core entity is a facet, not new core properties
node SupportProfile {
    slug: String @key
    sla: enum(standard, priority)
}
edge ProfileOf: SupportProfile -> Organization @card(1..1)
```

### 11. Optimize for retrieval: recall and precision

Agents answer from what they retrieve, and many agent failures are retrieval
failures: they reason well over what they find but miss what they do not.
Design for both halves of retrieval.

- **Recall**: store as links the relationships an agent would otherwise search
  for, so that from any node an agent sees everything that bears on it: the
  assertions about it, the evidence behind them, the objections against them.
  A link written once turns recall into enumeration, and the agent can tell
  when it has everything. Where a link must exist, require it, so a missing
  link is a visible gap instead of a silent miss.
- **Precision**: specific edge types and filterable properties (time, status,
  source kind, confidence) let an agent take exactly the part of a
  neighbourhood it needs; a generic `RelatedTo` edge or a hub node returns
  everything and therefore nothing. Make retrievable units the size of an
  answer (a claim, a segment), not a whole document.
- **Together**: traverse to scope the candidate set completely, then rank
  inside it.

If an important question needs text search over the whole graph or a long
chain of inference, a relation is missing. Test retrieval like any other
requirement: a set of questions with known answers, scored for what an agent
found and what it missed. Search is a schema decision too: decide up front
which text is embedded and which properties are indexed.

**In Omnigraph:** write the questions first as `.gq` queries and lint them
against the schema. Use `@card(1..)` for links that must exist. Scope with
traversal and filters, then rank with `nearest`, `bm25` or `rrf` inside the
scoped set (see [`search.md`](search.md)), and put `@embed` on the unit you
want back, not on a whole document.

**Example:** scope completely by traversal, then rank inside the scope.

```gq
query customer_evidence($slug: String, $q: String) {
    match {
        $c: Customer { slug: $slug }
        $a: Assertion
        $a about $c
    }
    return { $a.statement }
    order { nearest($a.embedding, $q) }
    limit 10
}
```

### 12. Let independence decide granularity

Split a node when its parts can be true, false, disputed, updated or cited
independently; otherwise keep it whole. Too coarse, and agents cannot point at
the part they mean. Too fine, and every write becomes a modelling decision on
which agents diverge.

**In Omnigraph:** a value that is disputed or cited on its own becomes a node
with its own key; a value that only ever changes with its parent stays a
property.

**Example:** when clauses are negotiated, cited or disputed one by one,
`Contract.terms: String` becomes `node Clause { kind: enum(...), text: String }`
linked to its contract; `Contract.signed_at` stays a property, because it
never changes on its own.

### 13. Write the schema for a reader without context

An agent reads the schema as instructions, and a human reviewer should
understand the domain from it alone. One word should mean one thing,
descriptions should say when to use a type and when not to, and nothing should
rely on context the reader does not have. An ambiguous name is interpreted
differently by each agent, and nothing reconciles them.

**In Omnigraph:** put `@description("...")` on properties and
`@instruction("...")` on node and edge types; agents see them in
`schema show`. Name edges for their meaning (`AuthoredBy`, not `RelatedTo`),
and keep enums explicit and keys obvious.

**Example:**

```pg
node Deal
    @instruction("One sales opportunity with one customer. Create it when the customer shows buying intent; never one per meeting.")
{
    slug: String @key
    stage: enum(prospecting, discovery, pilot, won, lost, other) @description("Pipeline stage; `other` only when no stage fits, with a note")
}
edge OwnedBy: Deal -> Person   // named for its meaning; never RelatedTo
```

### 14. Keep meanings stable over time

Never repurpose a field: deprecate it and add a new one, because agents built
against the old meaning keep writing it. A meaning that shifts silently
corrupts every agent and every stored record that relied on it. Migrations are
intentional: rename rather than drop and re-add.

**In Omnigraph:** rename with `@rename_from`, add replacements as new nullable
properties, backfill, then drop the old property with a planned `schema
apply` (see [`schema.md`](schema.md#rename-dont-replace)).

**Example:** `Deal.value` held forecasts and later booked revenue. Split
the meaning instead of repurposing the field.

```pg
node Deal {
    slug: String @key
    forecast_value: F64? @rename_from("value")
    booked_value: F64?
}
```

### 15. Measure convergence

Looseness is measurable, so measure it before committing. Give two independent
agents the same input and compare what they write. Agreement shows where the
schema is crisp; disagreement points to the fix:

| Symptom | Missing |
|---|---|
| One value absorbs most writes | a category |
| Agents choose consistently but differently | a rule |
| Both force the same content into an awkward shape | a type |
| Writers report content they cannot express | an extension point, or a property on the wrong node |

Duplicate rate, `GraphPolicy` breaks per write, and the share of key questions
answerable by traversal are the other gauges.

**In Omnigraph:** load each agent's output into its own branch or scratch
graph, and compare with the same aggregate queries you use in production.

**Example:** two agents extract the same twenty transcripts onto branches
`enc-a` and `enc-b`. Where they disagree on `stage`, write a rule; if both
send paused deals to `other`, add a `paused` value; if both squeeze clauses
into a note, add a `Clause` type.
