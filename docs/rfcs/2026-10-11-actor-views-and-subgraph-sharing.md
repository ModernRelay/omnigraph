---
rfc: "2026-10-11-actor-views-and-subgraph-sharing"
title: "Actor views and subgraph sharing"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-11
updated: 2026-10-11
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Measure full-text ranking quality under view-independent statistics (public and uniform) against index-wide statistics on a checked-in fixture, and choose the restricted-actor statistics source."
  - "Measure readable-set construction at the target scale (10^5 principals, 10^6 securables, group depth 16) on main, warm and cold, and confirm the limits in Bounds."
---

# RFC: Actor views and subgraph sharing

> A term set in ***bold italics*** is defined at that spot; it is plain text
> afterwards. Every `file:line` anchor into OmniGraph is at commit `6e31d0ab`;
> every anchor into Lance is at crate `lance` 11.0.0 and its sibling crates, the
> versions in `Cargo.lock`, written as `lance-11.0.0/<path>`.

## Summary

OmniGraph authorizes today at graph × branch class × action. An actor can read
every row of a branch or none of it. This RFC adds an ***actor view***: the
subgraph a given actor may see, defined by declarations in the schema, rules in
the applied cluster policy, and membership facts stored as graph data. Every
read an actor makes is answered over its view, and every write it makes may only
create or change rows its view lets it write.

The observable contract is ***non-interference***: an actor's answer to any
query equals the answer the same query would give on a graph that physically
contained only that actor's view. Counts, aggregates, traversals, search ranks
and errors are all computed on the view, never filtered afterwards.

The contract is enforced at one place per mechanism, the ***visibility door***.
Every operator that reads rows, edges, statistics or indexes on a secured type
passes through it, and a differential test proves the contract for every
operator the engine has.

The same construct serves external sharing. A ***share*** is the view of a
share principal. It is served live through the server, or materialized into a
separate graph that a reconciler keeps current from the source's change feed.

What does not change: Cedar remains the action gate; a graph with no security
declarations behaves exactly as today; anyone holding storage credentials still
sees everything, as today.

## Motivation

### The current boundary

The policy model has graph-scoped actions with an optional branch class:
`ResourceScope` is "branch-grained on purpose", and per-type and per-row scope
were reserved for a later `predicate_for` hook
(`crates/omnigraph-policy/src/lib.rs:964-990`). The user guide states the limit:
"Per-entity and per-property authorization is not currently supported"
(`docs/user/operations/policy.md`). Rules are allow-only, grouped by static actor
lists in YAML, and compiled to Cedar text (`crates/omnigraph-policy/src/lib.rs:825-866`).
Reads are authorized only in HTTP handlers; the engine's read doors take no
actor (`crates/omnigraph/src/exec/query_doors.rs:41-170`).

### A company brain

A graph that ingests Slack, email, documents and CRM records for agents to
reason over holds data with thousands of overlapping audiences. Nearly all of
those audiences already exist in the source systems: channel membership, folder
ACLs, thread participants, record ownership. They change daily. A graph-wide
read grant forces a choice between two bad options: give agents everything, or
split the brain into many graphs that can no longer be joined.

Three further needs appear in every comparable product surveyed (Appendix B):

- **Barriers.** Legal ethical walls, finance information barriers and Purview
  Information Barriers are deny rules that override grants.
- **Discovery separate from access.** Microsoft 365 separates who may open an
  item, whether search and Copilot may surface it, and whether Copilot may
  process it.
- **Derived content.** A summary of a private thread must not be more visible
  than the thread.

### External sharing

A firm needs to give a client, partner or auditor a deliberately chosen part of
the graph: a deal room, a matter, a project. Today the only unit is a whole
graph, copied by export and load with no lineage, no incremental update and no
revocation story.

### Why an RFC

This is a cross-cutting authorization boundary with new schema, policy, query
and HTTP contracts. A partial implementation that filters only some operators
is worse than none: it gives the impression of confidentiality while leaking
through the operators it missed. The design must fix the contract, the
enforcement points and the evidence together.

## The contract

### Non-interference

For an actor `a` with view `V(a)` and any read `q`:

```text
answer(q, G, a) = answer(q, materialize(V(a)), unrestricted)
```

`materialize(V(a))` is a graph containing exactly the types, properties, rows
and edges `V(a)` admits. The equality covers rows, row order where the query
orders, counts, aggregates, search ranks, errors and the result schema. It
holds at the snapshot the read targets, under the authorization state captured
for the request.

Writes obey the dual rule: a write by `a` succeeds only if every row it creates,
changes or removes is writable in `V(a)`, and its outcome reveals nothing
outside `V(a)` beyond the disclosures listed in [Writes](#writing-under-a-view).

### Silent filtering

Reads use silent filtering: a query is evaluated over the view without being
refused. This is the model of Postgres row security, Oracle VPD, Oracle Label
Security, Neo4j, Stardog and Elasticsearch. Refusing queries not provably
answerable from authorized views (the "Non-Truman" model of Rizvi et al., 2004)
requires query-equivalence reasoning that does not scale to ad-hoc agent
queries.

Silent filtering can mislead: an average over the view is not the company's
average. Two mitigations are part of the contract:

1. A query result carries a `view` object whose `restricted` field is `true`
   when the actor's view excludes anything from a type the plan read, or a
   member of an abstract type the plan bound. It is computed from the view and
   the plan, never from data, so it reveals only a policy fact the actor may
   know.
2. A caller that needs a whole answer asks for one. A stored query annotated
   `@complete`, or a request with `"complete": true`, is refused with
   `view_incomplete` whenever `restricted` would be `true`. The refusal carries
   no type name, count or row, so it discloses exactly what `restricted`
   discloses. An interface-bound UI component uses this to show "no access"
   instead of a silently partial calendar.
3. Answers that must be complete for an actor who cannot see the rows behind
   them are served by stored queries that run with their definer's rights,
   reviewed through cluster configuration
   ([Answer capabilities](#answer-capabilities)).

A name the view hides (a type, an edge type or a property) does not exist for
the actor. A query naming it fails with the same error, byte for byte, as a
query naming something that was never declared.

### Tiers

Visibility has three monotone tiers per row:

| Tier | Meaning |
|---|---|
| ***read*** | The row exists in the actor's view. |
| ***discover*** | The row may be returned by search operators (`nearest`, `bm25`, `rrf`, text predicates). |
| ***ground*** | The row is in the view of automation principals and delegated agent credentials. |

`discover ⊆ read` and `ground ⊆ read` always. A securable can withdraw a row
from discovery or grounding; no setting can make discovery exceed read access.

### What is not promised

- **Storage holders.** Lance files contain every row. Anyone who can read the
  graph's objects sees everything, as today. A recipient who needs storage-level
  access gets a materialized share with its own storage.
- **Timing.** Execution time can depend on hidden rows. Timing channels are out
  of scope for live access; they are one reason external parties get
  materialized shares.
- **Key existence for writers.** A writer of a keyed type can learn whether a
  key exists in that type ([D2](#d2-writes-that-collide-with-hidden-rows)).
- **Edge existence on delete.** Deleting a node that has edges the actor cannot
  see is refused, which reveals that such an edge exists.

Every other channel listed in [Surfaces](#surfaces) is closed or refused.

## User and operational behavior

### Declaring what is securable

Security is declared in the schema, because it binds to stable schema identity
and must survive renames:

```pg
// The graph identity of an authenticated actor.
node Principal @principal {
  actor: String @key            // "principal:<sub>", or a static-token actor id
}

node Group @group {
  slug: String @key
}

// A securable owns content rows.
node Space @securable {
  slug: String @key
  name: String
  hidden_from_search: Bool? @restricts("discover")
  hidden_from_agents: Bool? @restricts("ground")
}

edge MemberOf: Principal -> Group @membership
edge SubgroupOf: Group -> Group @membership
edge Grants: Group -> Space @grant {
  relation: enum(viewer, editor, owner)
}
edge Screens: Group -> Space @barrier

node Message {
  source_ref: String @key       // opaque source-system id
  text: String
  space: String @secured_by("Space")
  author_email: String? @sensitivity("pii")
}

node Salary @sensitivity("compensation") {
  person: String @key
  amount: I64
}
```

| Annotation | On | Meaning |
|---|---|---|
| `@principal` | node type | Rows are actor identities. Exactly one key property, matched against the authenticated actor id. At most one per graph. |
| `@group` | node type | Rows are groups of principals. |
| `@securable` | node type | Rows own content. |
| `@membership` | edge type | Principal → group or group → group. Membership is transitive through these edges. |
| `@grant` | edge type | Principal or group → securable, with a `relation` enum property whose values are a subset of `viewer`, `editor`, `owner`. |
| `@barrier` | edge type | Principal or group → securable. Members never see the securable, whatever they are granted. |
| `@barrier_exception` | edge type | Principal → securable with a `until: DateTime` property: an audited, time-bound crossing of a barrier. |
| `@secured_by("T")` | `String` or `String?` property | The row belongs to the securable of type `T` whose key equals this value. One per type. A null value hides the row from every restricted actor, which lets an existing type be secured by adding a nullable property and backfilling it. `T` is a `@securable` node type, or ([Polymorphic types](#polymorphic-types)) an interface whose members are all `@securable` and which declares interface-scope `@unique` on their key. |
| `@sensitivity("c")` | property or type | The property, or the whole type, is visible only to actors granted class `c`. |
| `@restricts("discover" \| "ground")` | Bool property of a securable | When `true`, the securable's rows leave that tier. |
| `@provenance` | edge type | Derived row → source row ([Derived content](#derived-content)). |

A type without `@secured_by` and without `@sensitivity` is ***unsecured***: any
actor with `read` sees all of it, as today. Global entities such as `Person` and
`Company` are usually unsecured; their sensitive facts are protected through
`@sensitivity` properties and secured edge-adjacent content.

Relations are ordered: `viewer` reads, `editor` also writes rows of the
securable, `owner` also manages the securable's grants. An edge visible to an
actor needs both endpoints visible; edge types do not carry their own
securable in this RFC.

Schema apply validates the declarations: one `@principal` type; `@membership`,
`@grant` and `@barrier` endpoints of the right kinds; `@secured_by` naming a
`@securable` type; no `@secured_by` on a list, Blob, vector or key property.
Adding these annotations sets the schema feature `actor-views`, which builds
that do not implement this RFC refuse to open
([Compatibility](#compatibility-and-reversibility)). The `@secured_by` property
gets a BTREE index.

A schema lint warns when a secured type is keyed on a property that looks
human-meaningful (an email, a name), because writers can probe keys.

### Granting access

Membership, grants and barriers are ordinary rows and edges on `main`:

```gq
query grant_space($group: String, $space: String) {
  insert Grants { from: $group, to: $space, relation: "viewer" }
}
```

Writing a row or edge of a `@principal`, `@group`, `@securable`, `@membership`,
`@grant`, `@barrier` or `@barrier_exception` type requires the new graph action
`grant`. These ***authorization types*** are writable only on `main`; a branch
write that touches one is refused, and so is a merge whose diff touches one.
Without that rule a branch forked before a revocation would bring the revoked
grant back when merged.

Connectors that mirror source ACLs run as service principals holding `grant`.
They map a Slack channel, a Drive folder or a CRM territory to a `Space` and
its members to `MemberOf` and `Grants` edges. Groups are not flattened into
user lists; Glean documents why ("a flattened ACL is stale the moment someone
joins a team").

### Policy configuration

The graph policy bundle gains version 2 with a `views` block:

```yaml
version: 2
groups:
  staff: [principal:alice, principal:bob, principal:slack-connector]
  hr: [principal:alice]
  connectors: [principal:slack-connector]
  platform-admins: [principal:root]
rules:
  - id: staff-read
    allow: { actors: { group: staff }, actions: [read, invoke_query] }
  - id: connectors-write
    allow: { actors: { group: connectors }, actions: [change, grant] }
views:
  everyone: all-staff            # key of the @group every reader belongs to
  classes:
    pii: [hr]
    compensation: [hr]
  unrestricted: [platform-admins]
  limits:
    max_groups: 4096
    max_securables: 100000
```

- `everyone` names a `@group` row that implicitly contains every actor the
  `read` rule admits. Grants to it are company-wide.
- `classes` maps each sensitivity class to cluster policy groups. Property and
  type confidentiality changes rarely and is reviewed, so it lives with the
  reviewed rules; row membership changes daily and lives in the graph.
- `unrestricted` lists cluster groups whose view is the whole graph. It is the
  break-glass and service-account path; every unrestricted read is audited.
- `limits` bound one actor's view ([Bounds](#bounds)).

Cluster apply validates `views` against the target graph's accepted schema,
in the same three places that already validate stored queries against the
catalog (`crates/omnigraph-cluster/src/deployment/execution.rs:782-800`,
`crates/omnigraph-server/src/deployment.rs:676`,
`crates/omnigraph-server/src/registry.rs:644`). An unknown class or group fails
the apply. Version-1 bundles keep their meaning on graphs without
`actor-views` declarations. Binding a version-1 bundle to a graph with those
declarations fails the apply, so a secured graph is never served without the
rules that give its declarations meaning.

New graph actions: `grant` (write authorization types), `reclassify` (change a
row's `@secured_by` value), `declassify` (write a `@provenance`-exempt derived
row or a declassification edge), `explain_full` (unredacted explain and
profile). Cedar remains the gate for these, as for all actions.

### Reading under a view

Queries need no change. The actor's view decides what they see:

```gq
query recent($topic: String) {
  match {
    $m: Message
    $m.text contains $topic
  }
  return { $m.source_ref, $m.text }
  order { $m.source_ref desc }
  limit 20
}
```

For a restricted actor this returns at most 20 rows from `Message` rows whose
`space` is in the actor's readable set. The response carries
`"view": { "restricted": true }`. `count`, `sum` and every aggregate run over
the same rows. A traversal never passes through a node the actor cannot see,
including intermediate nodes of a multi-hop pattern. `nearest`, `bm25` and
`rrf` rank only visible rows that are also in the discover tier and return
`min(k, visible)` results.

Restricted actors get full-text scores computed from view-independent corpus
statistics ([Full-text statistics](#full-text-statistics)).

A query that names a type or property hidden by `@sensitivity` fails with the
unknown-name error a misspelling would produce.

### Writing under a view

For an actor whose view of a secured type is restricted:

| Operation | Rule |
|---|---|
| Insert of a keyed row | If a row with the derived id exists and is not writable, the insert is refused with `key_conflict`, without an entity id or any value the actor did not send. Otherwise it upserts as today. |
| Insert without `@secured_by` value | Refused with `securable_required`. |
| New or changed row | Its `@secured_by` value must name a securable where the actor holds `editor` or `owner`. |
| Change of the `@secured_by` value | Needs `reclassify` and `editor` on both the old and the new securable. |
| Update or delete statement | Matches only rows in the view. A matched row that is visible but not writable refuses the statement with `view_write_denied`. |
| Delete that would cascade to an edge the actor cannot see | Refused with `delete_blocked`. This discloses that such an edge exists. |
| Edge insert | Both endpoints must be visible; a hidden endpoint fails exactly as a missing one. |
| `load --mode overwrite` | Refused when the actor's view of any type in the batch is restricted. |
| Branch merge | Refused unless every row in the merge diff is writable by the actor. Conflicts are reported without entity ids. |
| Constraint violations | Reported by constraint and property name only. `@unique` omits the other holder's id; `@card` omits the count. |

Unrestricted actors keep today's messages.

### History, change feed and export

Authorization always comes from current authority: rules from the applied
policy, membership and grants from `main`'s current head. Revocation therefore
covers history, and joining a group shows that group's history. Relaxing a
policy exposes history too; BigQuery documents the same consequence.

In the first releases a restricted actor cannot read a historical snapshot, the
change feed, a commit's changes, an export or a baseline of any secured type its
view restricts. These refuse with `view_restricted`. Unrestricted actors and
share reconcilers are unaffected. A later phase serves view-filtered history
([Phase 7](#rollout)).

### Surfaces

| Surface | Under a restricted view |
|---|---|
| `POST /query`, `POST /queries/{name}`, MCP `query` | Answered over the view. |
| `explain` | Omits row estimates, statistics, dataset paths, versions and index facts for restricted types unless the actor holds `explain_full`. |
| `profile` | Requires `explain_full`. |
| `GET /schema` | The schema view: hidden types and properties are absent. |
| Structured catalog route and schema hash on reads ([Polymorphic types](#polymorphic-types)) | The schema view, and the view's digest in place of the schema hash. |
| `GET /queries`, MCP `queries` | Only queries that typecheck against the schema view. |
| `GET /snapshot` | Omits `entity_count` and dataset paths for restricted types. |
| `GET /commits`, `/commits/{id}`, `/commits/{id}/changes`, `GET /changes`, `POST /changes/baseline`, `POST /export` | `view_restricted` until Phase 7. |
| `GET`/`HEAD /blob` | Only for a visible row; a hidden row answers exactly as a missing one. |
| Write responses | As in [Writing under a view](#writing-under-a-view). |
| Denial messages | Do not echo the actor or branch. |

### Agents and delegated credentials

An agent acting for a user presents a ***delegated credential***: the user's
principal as subject, the agent's principal as actor (the RFC 8693 `act`
claim), an audience bound to this server, and optional ***caveats*** that only
narrow access: securable allow-lists, type allow-lists, an action subset,
branches and an expiry. The effective view is

```text
V(user) ∩ V(agent) ∩ caveats
```

with the `ground` tier applied. Tokens whose principal kind is `automation`
(the existing `PrincipalKind::Automation`,
`crates/omnigraph-server/src/data_tokens.rs:26-29`) also get the `ground` tier.
Caveats compile into the same typed view as rules; there is no second
evaluator. The MCP surface accepts only tokens whose audience is this server and
never passes a token upstream.

### Barriers

A `@barrier` edge from a group to a securable removes the securable from every
member's readable set after grants are applied: effective access is grants
minus barriers. No grant overrides a barrier. A `@barrier_exception` edge with
an `until` time restores one principal's access until that time; the crossing
is audited. Barriers apply to derived content through provenance
([Derived content](#derived-content)): a summary of two sides of a wall is hidden
from anyone walled off from either side.

### Integrity

A securable may declare an `enum` property annotated `@integrity` whose values
are ordered from least to most trusted. Rows inherit their securable's
integrity. A query response reports the lowest integrity among the secured
rows it returned, `"view": { "integrity": "external" }`, so an agent runtime can
refuse to act on low-integrity content. OmniGraph does not track what an agent
does with the answer; it provides the label.

### Derived content

A derived row (a summary, a chunk, an agent note) declares its sources with
`@provenance` edges written in the same mutation. Its visibility is the
intersection of its own securable and every source's securable: the row is
visible only to actors who can see all of them. Only declared sources count;
retrieved but unused context is not a source. An owner of every source
securable may write a declassification edge that cuts one provenance edge out
of the intersection; it needs `declassify` and is audited.

### Sharing

A share is the view of a ***share principal***: a `@principal` row whose grants
define what the share contains. Three delivery modes use that one definition:

| Mode | Recipient reads | Revocation | Retention and audit |
|---|---|---|---|
| ***live*** | Our server, under the share principal's view | Immediate | Ours; we see every read |
| ***hosted copy*** | A separate graph we host, kept current by a reconciler | Immediate; the copy is deleted | Ours; we see every read |
| ***delivered copy*** | A separate graph in storage the recipient controls | Stops future updates only | The recipient's |

Live shares inherit every side channel of live access, so they are for trusted
collaborators. External parties get copies. Shares are declared in cluster
configuration:

```yaml
shares:
  acme-deal-room:
    source: knowledge
    principal: share:acme
    mode: hosted
    target: acme-deal-room       # graph id of the copy
    terms: odrl/acme-agreement.json
```

The copy's schema is the source's schema view for the share principal. Ids are
preserved, so rows map back to the source.

#### Answer capabilities

A stored query may declare `run_as: definer`. It then runs with the view of the
principal that defined it, and a caller needs only `invoke_query` on it. This is
how a recipient gets a complete aggregate without the rows behind it. Because
repeated parameterized queries can isolate individuals, a definer query must
declare a domain for every parameter, cannot filter on a key property by a
caller-supplied value, and logs every invocation's parameters for its owner.
Minimum group sizes alone do not prevent differencing; AWS Clean Rooms and
Snowflake both say so. Aggregate answers to external principals can draw on a
per-recipient privacy budget in a later phase.

#### Write-back

A recipient of a hosted copy writes on a branch of the copy. The reconciler
maps that branch into a proposal branch on the source, checks that every changed
row lies inside the share and is writable by the share principal, and labels
the rows with the share's integrity. An owner reviews and merges. Revoking the
share deletes its proposal branches.

### Vaults

A ***vault*** is a compartment kept in its own graph or cluster rather than
secured by rows. Use rows for the long tail of channels, folders, deals and
matters. Use a separate graph when a defect in row-level enforcement would be
unacceptable, retention differs, or the compartment is large and mostly searched
alone. Use a separate cluster when the threat includes holders of storage
credentials, when compliance requires key separation, or when a recipient gets
storage access: every graph in one cluster shares one storage root and one set
of credentials today ([Appendix A](#appendix-a-validated-assumptions), H5).

### Operator tools

```bash
omnigraph policy explain-view --graph knowledge --actor principal:alice
omnigraph policy who-can-see --graph knowledge --type Message --id <id>
omnigraph policy diff-view --graph knowledge --config ./proposed
```

`explain-view` prints the actor's groups, readable securables (bounded),
classes and barriers with the facts that produced them. `who-can-see` inverts
it for one row. `diff-view` reports how many securables change visibility for
how many actors before a policy apply.

## Design

### Authority

Three sources, each owning one kind of fact:

| Fact | Owner | Changes | Versioned by |
|---|---|---|---|
| What is securable and how (annotations) | Accepted schema contract | Rarely; schema apply | The contract in `__manifest` |
| Who gets which class, who is unrestricted, limits | Applied cluster policy | Reviewed; cluster apply | Policy digest |
| Membership, grants, barriers, securable assignment | Graph rows on `main` | Daily; `grant` writes | `main`'s manifest pins of the authorization tables |

Cluster policy groups and graph groups are different concepts. Cluster groups
gate actions and classes; graph groups gate rows. Neither mirrors the other.

### Capturing a view

A request resolves its actor at the server as today. The engine gains read
doors that take the actor; invariant 10 extends from writes to reads, so
embedded and CLI-direct readers obey the same view when a policy is installed.

A read captures two things under one schema permit, in this order:

1. ***Authorization state***: the applied policy digest and the `__manifest`
   pins on `main` of every authorization table, each pin being the table's
   identity, published dataset version, staged version and transaction id.
2. ***Data snapshot***: the read target, as today.

Snapshot-target reads already load `main`'s live contract under the schema
permit and then discard that `main` snapshot
(`crates/omnigraph/src/db/omnigraph.rs:1009-1024`); this design keeps it. A
branch read probes `main`'s version once more. The capture is one new function,
because the schema gate is not reentrant (`crates/omnigraph/src/db/write_queue.rs:455-458`).

The ***authorization key*** is `(policy digest, authorization pins, actor,
caveats)`. It is deliberately not `main`'s head commit: every unrelated write
and every maintenance run advances the head, while the pins move only when an
authorization table is written.

Because authorization comes from the freshest `main`, an authorization state is
always at least as new as the data it filters. Zanzibar's "new enemy" problems
(content added after a revocation, or a revocation ordered after a move) cannot
occur: the revocation is visible before any later content.

### Computing the readable set

From the authorization state the engine computes, for actor `a`:

1. `P`: the `@principal` row whose key equals `a`. If absent, `P` is empty.
2. `G`: `P`'s groups, the transitive closure over `@membership` edges, plus the
   `everyone` group when `a` passes `read`.
3. `R0`: securables with a `@grant` edge from `P ∪ G` at relation `viewer` or
   higher. `W0` and `O0` likewise at `editor` and `owner`.
4. `B`: securables with a `@barrier` edge from `P ∪ G`, minus those with an
   unexpired `@barrier_exception` from `P`.
5. `R = R0 − B`, `W = W0 − B`, `O = O0 − B`.
6. Tier sets: `D = R` minus securables with `@restricts("discover")` true;
   for a grounded actor, `R` and `W` exclude securables with
   `@restricts("ground")` true.
7. For a delegated credential, intersect with the agent's sets and the caveats.

The closure runs as an internal plan on `main`'s snapshot. GQ's bounded
multi-hop traversal (`$g subgroupOf{1,16} $h`, `crates/omnigraph-compiler/src/query/query.pest:124,130`)
requires a maximum and equal endpoint types, which group-to-group membership
satisfies; principal-to-group membership is one hop. Cycles are handled by the
traversal's visited set (`crates/omnigraph/src/engine/graph.rs:1395-1404`).

The result is an immutable ***view spec***: the schema view, `R`, `W`, `O`, `D`,
the class set, the integrity map, the earliest barrier-exception expiry, and a
digest. It is cached by authorization key. An expiry inside the spec ends its
validity at that time.

### Bounds

`|G| ≤ max_groups`, `|R| ≤ max_securables` and membership depth ≤ 16. Exceeding
a limit refuses the request with `view_limit_exceeded`; the engine never
truncates a view. The view cache is an LRU bounded in bytes and lives outside
`invalidate_read_caches` (`crates/omnigraph/src/db/omnigraph.rs:2026-2034`),
whose flush on every observed advance of `main` would otherwise discard views
keyed by immutable state. Reads by external principals get per-actor read
admission, extending the existing per-actor write caps
(`crates/omnigraph-server/src/workload.rs:15-19`).

### The visibility door in the plan

The view enters the plan in three places.

**Schema view.** The query typechecks against a catalog with hidden types,
edge types and properties removed. The catalog is memoized per (contract,
schema-view digest), because the compiled-query cache matches catalogs by
`Arc` identity (`crates/omnigraph/src/exec/query_doors.rs:318-341`) and the
accepted-catalog memo holds one slot (`crates/omnigraph/src/runtime_cache.rs:288-333`).
The engine context receives the same view catalog as the planner, since the
output-schema checks reject a mix. Whole-node returns (`return $p`) build their
struct from the catalog's node-object members
(`crates/omnigraph/src/engine/expr.rs:79-90`), so hidden properties never
appear. A hidden property that is part of a key is still read for identity but
is never projected.

**Row predicate.** Every scan of a secured type whose `R` is not everything gets
a view conjunct, a new typed IR leaf `ViewSet { type_id, tier }`. The leaf is
resolved at lowering from the engine context's view spec to an `IN` list over
the `@secured_by` column. It is not a GQ parameter: a replayed plan could carry
swapped parameter values, while the view leaf is always resolved from the
replaying request's own view. The conjunct is placed directly into the scan
spec at plan construction, inside `Predicate::Gq`, because
`Predicate::gq_filters()` drops every other variant
(`crates/omnigraph-planner/src/logical.rs:125-140`).

**Coverage validator.** A new check runs in `engine::execute`, beside
`validate_aggregate_specs` and `validate_output_schemas`, so every door
including replay passes it (`crates/omnigraph/src/engine/mod.rs:409-418`). It
refuses a plan that:

- scans a restricted secured type without its view conjunct;
- uses `MetadataCount` on a restricted type (its lowering ignores the scan filter,
  `crates/omnigraph/src/engine/lower.rs:473-487`);
- uses the CSR, the `count`/`not` degree shortcut
  (`crates/omnigraph/src/engine/lower.rs:230-277`) or the CSR-derived search gates on
  a restricted type;
- expands into a restricted type without a destination view mask.

The bound plan's assumptions gain the view digest. Replay recomputes the view for
the replaying actor and refuses if the digest differs, so a saved plan cannot be
replayed by another actor or after a revocation.

### Scans and search

The view conjunct lowers to `col IN (...)` through `ir_expr_to_df_expr`
(`crates/omnigraph/src/engine/scan.rs:1093-1119`) and reaches Lance through
`Scanner::filter_expr`, which takes a DataFusion expression without SQL parsing
(`lance-11.0.0/src/dataset/scanner.rs:1585`). With a BTREE on the `@secured_by`
column, Lance evaluates the list as an exact `SargableQuery::IsIn` with no
length limit, and the filtered read skips fragments without candidates
(`lance-index-11.0.0/src/scalar/expression.rs:348-352`;
`lance-11.0.0/src/io/exec/filtered_read.rs:163-206`). The planner already sets
`prefilter(true)` on every scan with a filter
(`crates/omnigraph/src/engine/scan.rs:248-251`), so vector and full-text search
rank only rows that pass the view conjunct.

Today a `@index` on a String builds a full-text index, not a BTREE
(`crates/omnigraph/src/db/omnigraph/table_ops.rs:582-605`). The
`@secured_by` annotation maps its property to a BTREE.

The engine does not use Lance's row-address prefilter for views in this RFC.
On a plain scan the mask demotes the filter so it loses its scalar index
(`lance-11.0.0/src/dataset/scanner.rs:3298-3306`), and the mask is in stable
row-id space for one exact table version. A per-view mask cache is a later
optimization behind its own surface guard ([Rollout](#rollout), Phase 6).

**Vector search.** Lance has no automatic switch to flat search for a selective
prefilter; its late search widens up to every partition, and when the prefilter
admits at most `k` rows the unreached ones come back at distance `+inf`
(`lance-11.0.0/src/io/exec/knn.rs:1710-1782`). The planner instead chooses from
the visible row count `V`, obtained by an index-answered count:

- **Exact**: when `V × dimensions ≤ 8·10^6` (about 10,000 rows at 768
  dimensions, tunable), scan the visible rows with the index bypassed. Lance
  honors the filter on that path only with `prefilter(true)`
  (`lance-11.0.0/src/dataset/scanner.rs:5015-5019`), which the engine sets.
- **Indexed**: otherwise use the IVF index with probes set up front to about
  `k · partitions / (s · N) × 2`, where `s` is the visible fraction.
- **Fallback**: if that probe count exceeds half the partitions, use exact.

The existing overfetch and exact-pass loop (`crates/omnigraph/src/engine/mod.rs:473-555`)
remains the safety net. Whole-table counts such as `nearest_dataset_rows`
(`crates/omnigraph/src/engine/scan.rs:338-346`) never leave the engine for a
restricted actor.

### Full-text statistics

Lance's BM25 scorer takes its corpus statistics from the whole index, including
rows the prefilter removed and deleted rows (`lance-index-11.0.0/src/scalar/inverted/index/search.rs:53-63`;
`lance-index-11.0.0/src/scalar/inverted/scorer.rs:197`). With stable row ids,
compaction does not remap the index, so deleted postings stay until the index is
optimized or rebuilt (`lance-11.0.0/src/dataset/optimize.rs:2580`). A restricted
actor's scores therefore encode term frequencies of hidden documents. Hiding the
score is not enough: the STRESS attack recovered document frequencies from rank
order alone. Its precondition, write access to the same index for calibration,
is common in a company brain.

Restricted actors' full-text search on a secured type uses view-independent
statistics. Lance accepts external statistics below the scanner:
`MatchQueryExec`, `PhraseQueryExec`, `CompoundQueryExec` and
`FlatMatchQueryExec` take a `MemBM25Scorer` through `with_base_scorer`
(`lance-11.0.0/src/io/exec/fts.rs:624, 2175, 3058, 3485`). The engine builds
that plan itself for restricted actors. Two statistics sources are candidates:

- ***Public statistics***: corpus statistics over rows of securables granted to
  the `everyone` group, built by explicit index reconciliation and stored as
  derived state keyed by index identity and the `everyone` group's grants.
- ***Uniform statistics***: fixed document count, average length and document
  frequency. They leak nothing and rank by term frequency and length only.

Public statistics are the proposal and uniform statistics are the fail-closed
fallback when the artifact is missing or stale. The choice is gated on ranking
measurements (`blocked_on`). The flat path adds its own filtered input rows to a
supplied scorer (`lance-index-11.0.0/src/scalar/inverted/flat_search.rs:548-559`);
because the engine filters by expression, those rows are visible rows, which
adds no leak. For OR queries, Lance's WAND switches to a flat search when the
allow-list is at most 10% of the index
(`lance-index-11.0.0/src/scalar/inverted/wand.rs:201-206`), so small views are
cheap.

Unrestricted actors keep index-wide statistics.

### Traversal

Traversal reads edge tables by key and never reads intermediate node tables
(`crates/omnigraph/src/engine/graph.rs:1132-1472`). Edge reads carry no filter
(`crates/omnigraph/src/engine/graph.rs:1647-1662`). The destination of each hop
is masked:

- **Indexed mode**: after each hop, the frontier's ids of a restricted secured
  type are intersected with the view by one indexed read of that node table
  (`__id IN frontier AND securable IN R`). The final destination scan already
  carries the view conjunct through `hydrate_nodes`, which refuses unsupported
  predicates (`crates/omnigraph/src/engine/operators/scan/input.rs:270-284`).
- **CSR mode**: refused for restricted secured types until a per-view ordinal
  mask exists. The CSR assigns dense ordinals per node type from edge endpoints
  only, per index instance, and is cached by edge-table versions with no actor
  (`crates/omnigraph/src/graph_index/mod.rs:168-245`;
  `crates/omnigraph/src/runtime_cache.rs:24-57`). A mask must bind to the
  instance and to node-table versions.

Wildcard traversal already expands only over edge types whose declared
endpoints match (`crates/omnigraph-compiler/src/query/typecheck.rs:1981-1989`),
so the schema view prunes hidden edge types. Query bindings are concrete node
types today (`crates/omnigraph-compiler/src/query/typecheck.rs:1785-1790`);
[Polymorphic types](#polymorphic-types) covers abstract bindings.

`Omnigraph::graph_index()` returns the whole topology
(`crates/omnigraph/src/db/omnigraph.rs:2681`) and becomes crate-private.

### Writes

The write check runs in end-of-attempt validation, where the engine has the
staged after rows, the deleted ids (cascades and overwrite removals included),
and a committed state pinned to the transaction base with a batched lookup by
id (`crates/omnigraph/src/validate.rs:391-433`). The validation points are
`validate_staged_mutation` (`crates/omnigraph/src/exec/mutation.rs:886-904`),
`validate_changeset` (`crates/omnigraph/src/loader/mod.rs:958-959`) and merge
evaluation (`crates/omnigraph/src/exec/merge.rs:3433-3436`). `commit_all`
revalidates the whole transaction under the schema, branch and table locks
before any effect (`crates/omnigraph/src/exec/staging.rs:920-984`), so the checked
state is exactly the state publication replaces. Effects staged before a refusal
are detached and unreachable.

The committed state gains a lookup of the committed `@secured_by` value by id,
which supplies the before image for upserts, deletes and overwrite removals.
Lance's merge-insert cannot express "update if writable, else fail" atomically:
`WhenMatched::UpdateIf` turns a false condition into a no-op
(`lance-11.0.0/src/dataset/write/merge_insert.rs:425-449`;
`lance-11.0.0/src/dataset/write/merge_insert/assign_action.rs:107-139`). The
check therefore belongs to OmniGraph's validation, not to Lance.

Today an upsert reports `affected_nodes: 1` whether it inserted or updated, and
delete counts include rows matched by id regardless of visibility
(`crates/omnigraph/src/exec/mutation.rs:1645-1661, 1717-1741`). Under a view the
statement's match runs over the view, so counts are view counts.

Errors carry their foreign facts (another row's id, a count, an ETag, another
type's version) as structured fields. The server strips those fields for a
restricted actor. Today they are formatted into messages
(`crates/omnigraph/src/validate.rs:1259, 1295-1296`) and returned verbatim
(`crates/omnigraph-server/src/lib.rs:1357`).

Writes to authorization types are checked in the same validation: the actor
needs `grant`, the branch must be `main`, and grant edges into a securable need
`owner` on it.

### History

A read of a snapshot target captures authorization from `main` and data from
the snapshot, as above. Historical queries already use the live contract with
tables rebound by stable identity
(`crates/omnigraph-catalog/src/lib.rs:608-656`), so annotations bound to stable
identity apply to history without new mechanism. The first releases refuse
historical reads of restricted secured types; a later phase admits a historical
row only when its securable is readable both at the snapshot and, if the row
still exists, at `main`'s head, so a row moved from a public to a private space
stays hidden in old snapshots.

Property binding on historical reads is by name today
(`crates/omnigraph/src/engine/typed_value.rs:383-420`), unlike Blob reads, which
compare stable property ids (`crates/omnigraph/src/blob.rs:1541-1550`). A
`@sensitivity` property dropped and re-added under the same name must not expose
its old values; historical reads must bind properties by stable identity before
property views serve history.

### Change feed and export

A view-filtered change feed filters where rows are emitted
(`crates/omnigraph/src/changes/enumerate.rs:132, 179`). Three requirements:
the view digest joins the cursor and page-token digest
(`crates/omnigraph/src/changes/token.rs:283`); an update across a visibility
boundary is emitted as an insert or a delete; equality across vintages compares
projected rows, so a change to a hidden property emits nothing.

### Server surfaces

Every route in [Surfaces](#surfaces) passes the actor to an engine door that
applies the view, or refuses with `view_restricted`. MCP tools call the same
handlers (`crates/omnigraph-server/src/mcp.rs:272-310`) and need no separate
change. Explain redaction strips the fields that carry whole-table facts:
statistics and assumptions (`crates/omnigraph/src/engine/explain.rs:236, 272`),
per-node row estimates (`crates/omnigraph-planner/src/physical.rs:363-370`) and
index facts (`crates/omnigraph-planner/src/gate.rs:58-71`).

A graph with `actor-views` declarations requires an applied version-2 policy.
The server refuses to serve it in unauthenticated mode, which today allows every
graph action (`crates/omnigraph-server/src/handlers.rs:651-668`), and with static
tokens and no policy, which today allows every read
(`crates/omnigraph-server/src/handlers.rs:661`).

### Polymorphic types

The draft RFC for polymorphic types
([ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884),
`2026-10-07-polymorphic-types.md`) makes interfaces and inline unions bindable,
adds polymorphic edge endpoints with `__src_type` and `__dst_type` tag columns,
and leaves per-type policy to the `predicate_for` work this RFC defines. The two
designs compose as follows.

**Member sets come from the schema view.** An abstract binding resolves its
member set against the actor's view catalog, so a member hidden by
`@sensitivity` is absent, exactly as it is absent from `materialize(V(a))`.
Visibility is decided per concrete type; `implements` neither grants nor
removes it, which keeps that RFC's rule that `implements` never confers a
permission. `$x is T`, `@type` and `@type_id` comparisons naming a hidden type
fail with the unknown-name error. Result schemas still never depend on the
member set: an abstract binding is typed from the interface declaration, and an
interface property hidden by `@sensitivity` is hidden for every member (below).

This replaces that RFC's `policy_denied_member` refusal and its
`policy_filtered` marker. Refusing a query because an interface includes a
hidden member leaks the member's existence, and the refusal's only escape, a
type test that removes the member, requires the caller to name a type it is not
allowed to know. A stored query shared by actors with different views could
never be written that way. Completeness is served instead by `view.restricted`
and the opt-in `@complete` refusal, which reveal no member.

**Security annotations on interfaces.** Under that RFC, property annotations on
an interface property apply to every implementor that inherits it.
`@sensitivity` and `@secured_by` follow that rule with two additions:

- A member that redeclares an inherited property may not drop either
  annotation. This is a refusal, not the lint warning that RFC gives other
  dropped annotations, because dropping it would declassify the member.
- A member may not add `@sensitivity` to an inherited property. Declaring it on
  the interface keeps the property's visibility uniform across members, so
  `$x.p` on an abstract binding is either visible for the whole binding or
  hidden for it.

Type-level `@sensitivity` belongs to concrete types and is not inherited
through `implements`.

**Declassification by migration.** A schema-apply step that removes
`@sensitivity` or `@secured_by` from any concrete type or property, by any
route, is a declassification step. The routes are deleting the annotation,
removing an interface property, removing `implements`, and narrowing an
endpoint that changes which annotated property a member inherits. The plan
lists each such step, and apply requires `declassify` in addition to
`schema_apply`.

**Polymorphic securables.** A row has one `@secured_by` value, a key string. A
key must name one securable even when securables come in several types (a Slack
channel, a Drive folder, a CRM account). `@secured_by` therefore names either
one concrete `@securable` type or an interface whose members are all
`@securable` and that declares interface-scope `@unique` on their key, which
that RFC adds in its phase 3. Interface-scope uniqueness rests on read-set
validation and the exact graph-head precondition rather than a fence; a
concurrent duplicate fails publication.

**Authorization edges.** With polymorphic endpoints one edge type covers both
kinds of subject:

```pg
interface Securable { slug: String @key  @unique(slug) }
node Space @securable implements Securable { name: String }
node Channel @securable implements Securable { topic: String? }

edge MemberOf: Principal | Group -> Group @membership
edge Grants: Principal | Group -> Securable @grant {
  relation: enum(viewer, editor, owner)
}
```

Membership closure becomes one bounded multi-hop traversal from the principal
across `MemberOf`, which that RFC's set-based multi-hop rule admits, instead of
a one-hop step followed by a group-to-group recursion. The readable-set plan
reads grant rows by tag through that RFC's single endpoint resolver.

**Union scans and polymorphic expansion.** Every arm of a union scan whose
member is a restricted secured type carries its own view conjunct, placed during
that RFC's per-arm resolution. The coverage validator inspects arms. A
`count` over an abstract binding may sum per-member metadata counts only for
unrestricted members. A polymorphic expansion groups each hop's frontier by tag,
as that RFC's interner already keys by `(type, id)`, and masks each restricted
member type as in [Traversal](#traversal).

**Graph index.** That RFC's phase 3 moves the graph index to one global ordinal
space, with each node type at a fixed offset. Per-view masks
([Rollout](#rollout), Phase 6) target that space: one bitmap per view and index
instance, instead of one per node type.

**Search.** That RFC refuses `bm25` on a multi-member binding because each
table's statistics differ. Uniform statistics are identical across tables, so
for restricted actors that reason disappears. This RFC does not admit
multi-member `bm25`; a later change may.

**Catalog route, schema hash, descriptors and feeds.** The structured catalog
route that RFC adds serves the schema view: member lists, properties and
annotations of visible types only. Reads by a restricted actor carry the
digest of the schema view where they would carry `schema_ir_hash`, so clients
cache type names per view and a change to a hidden type is not observable.
Operation descriptors list visible members. In Phase 7, interface filters on
the change feed expand over the view, and a `ChangeScopeMembershipChanged`
refusal names only visible members.

**History.** Both RFCs refuse abstract bindings on historical targets for now.

**Order of implementation.** Whichever lands second carries the integration: if
union scans land first, this RFC's Phase 2 extends the coverage validator to
arms and expansions; if views land first, that RFC's union scan adds the
per-arm view conjunct when it resolves arms.

### Shares

A share principal is an ordinary `@principal` row; its grants are the share's
content. Live shares need nothing else.

A ***share reconciler*** maintains a copy. It reads the source's view-filtered
change feed as the share principal, applies inserts and updates with
`load --mode merge` and deletions with a mutation, and records the source cursor
in a row of the target graph in the same publication as the data, so a restart
resumes exactly. Because a mutation cannot mix constructive and destructive
changes (`crates/omnigraph/src/exec/mutation.rs:666-683`) and load has no
delete mode, one source commit becomes a branch on the target, written in two
steps and merged once. A `409 schema_boundary` or `410 change_feed_gap` triggers
a new baseline. The reconciler is an idempotent function of the source's
accepted state, not a queue. Before each sync it re-checks the share's
agreement and stops when the share is revoked or its terms lapse, as the Eclipse
Dataspace Components policy monitor does.

Hosted copies live in a graph of the same cluster. Delivered copies need a
cluster the recipient controls, because graphs in one cluster share storage and
credentials.

### Lance surfaces relied on

| Surface | Use | Guard |
|---|---|---|
| `Scanner::filter_expr` with `prefilter(true)` | View conjunct on every scan, vector and full-text search | New guard: a String BTREE `IsIn` is exact and prefilters KNN and WAND |
| BTREE `SargableQuery::IsIn` | `securable IN R` | Same guard; list sizes 1 to 10^4 |
| `MatchQueryExec::with_base_scorer` and siblings | View-independent BM25 statistics | New guard: supplied statistics change scores; masked rows never enter the heap |
| KNN late search and `+inf` placeholders | Exact-pass safety net | Existing KNN fence in `lance_surface_guards.rs` |
| `count_rows` through `CountPushdown` | Visible count `V` for vector planning | New guard: an exact BTREE count equals a filtered scan count |

## Decisions

### D1. Rule language

Rules are declarative annotations plus a fixed relationship vocabulary,
compiled by OmniGraph into its typed IR. Cedar remains the action gate.

Cedar's partial evaluation and type-aware partial evaluation are experimental
in 4.11.2, the version in the lockfile, and remained so in 4.13.0. Their
residuals cannot express graph traversal: `resource in Space::"x"` means a
transitive descendant, which would have to be precomputed anyway. OPA's
partial evaluation has the same limit, and its translation to SQL strings
produced an injection fixed in v1.19.0 (July 2026); Dgraph had a query
injection that bypassed its ACLs (CVE-2026-41327). Invariant 9 already forbids
string-built semantics. Cedar's formal analysis remains attractive; a Cedar
front end can target the same view spec later.

### D2. Writes that collide with hidden rows

Refuse, with contentless errors, and accept key-existence disclosure to
writers of a keyed type. Systems either keep a separate copy per label
(Accumulo, Oracle RDF), which breaks key identity; refuse (Stardog, Memgraph,
Dgraph, Oracle Label Security), which discloses existence; or exempt the write
(TigerGraph upserts), which overwrites hidden rows. Postgres documents that
constraint checks "always bypass row security". Opaque keys on secured content
types make probing impractical; the lint enforces the guidance.

### D3. Silent filtering versus refusal

Silent filtering with a restricted marker, definer-rights stored queries for
complete answers, and the closing of every whole-table channel. The full-text
statistics decision is part of D3 and is gated on measurement.

### D4. Historical snapshots

Current rules and membership always. Historical reads of restricted secured
types are refused at first; the dual-label rule follows. Snowflake applies
current membership to time travel; Databricks applies current ABAC policies to
history and fails closed when a policy column is missing; BigQuery refuses
history to all but administrators on tables that ever had row policies. None
evaluates past policy.

### D5. Vaults

Rows for the long tail; separate graphs where a defect would be unacceptable or
statistics must be isolated; separate clusters where storage holders, key
separation or delivered copies are involved. Per-graph storage roots,
credentials and keys inside one cluster are a gap this RFC names and does not
fill.

## Invariants

1. **Respect the substrate.** The view uses Lance's scalar indexes, prefilter,
   KNN and FTS execution and the public base-scorer hook. No storage primitive
   is added. Index state stays derived.
2. **One publication door.** Membership, grants and securable assignment are
   graph rows published through `__manifest`. Share copies publish through load
   and mutate. No side table holds authorization facts.
3. **One coherent view.** A request captures its authorization state and its
   data snapshot once; a retry captures both again. The plan pins the view
   digest. Authorization is never older than the data it filters.
4. **Publish once.** Write checks run inside attempt validation and the commit
   revalidation; a refused write publishes nothing.
5. **Crash convergence.** No new durable pre-publication effect. The share
   cursor publishes with the data it describes.
6. **Stable identity.** Annotations bind to stable type and property identity.
   Historical property binding must move to stable identity before property
   views serve history.
7. **Derived acceleration.** View specs, public statistics, CSR masks and row
   masks are derived state keyed by immutable versions. Missing or stale derived
   state falls back to a slower exact path or refuses; it never widens a view.
8. **Loud integrity.** Every refusal is typed. A view is never truncated
   silently.
9. **Typed semantics.** The view is a typed IR leaf resolved from the engine
   context. Caveats compile to the same structure. No filter is built as a
   string.
10. **Boundary and engine.** Actors resolve at the server. The engine enforces
    views on reads as well as writes when a policy is installed.
11. **Bounded.** Groups, securables, depth, caches and external reads are
    bounded; exceeding a bound refuses.
12. **One source of truth.** Membership exists only in the graph; classes only
    in policy. Connectors mirror source ACLs into the graph, which is then the
    authority; nothing caches a second copy.
13. **Evidence.** See [Evidence and tests](#evidence-and-tests).

Deny-list: the share reconciler is an idempotent reconciler from accepted
state, not a job queue. Public statistics are built by explicit reconciliation,
not inline on content writes. No process-local lock fences anything new.

## Compatibility and reversibility

- **Opt-in.** Graphs without the new annotations and version-1 policies behave
  as today.
- **Schema feature.** The annotations set `actor-views` in the contract's
  required features. Builds refuse unknown features
  (`crates/omnigraph-compiler/src/catalog/schema_ir.rs:99-123, 1402-1406`).
  Without that, an older build would treat the annotations as metadata
  (`crates/omnigraph-compiler/src/schema/parser.rs:1074`) and serve the graph
  unfiltered.
- **Schema apply.** Adding or removing these annotations is a new plan step;
  today any annotation change other than `@description` and `@instruction` is
  refused (`crates/omnigraph-compiler/src/catalog/schema_plan.rs:985-1001`). Removing
  `@secured_by` from a type declassifies it and requires `config_manage`.
- **Policy.** Version-2 bundles are refused by older builds, which refuse
  unknown versions and fields.
- **Wire.** The `view` response object, the new error codes and the redacted
  explain fields are additive; `openapi.json` is regenerated.
- **Storage.** No format change beyond contract content and a BTREE on each
  `@secured_by` property, which is derived.
- **Reversal.** Removing the declarations through schema apply returns a graph
  to graph-wide authorization. Views are derived, so nothing else needs
  migration.

## Alternatives

- **Do nothing; one graph per audience.** Splits the brain into graphs that
  cannot be joined and multiplies cache budgets per graph.
- **Cedar or OPA residuals compiled to filters.** Rejected under D1.
- **An external Zanzibar service** (SpiceDB with Materialize, OpenFGA). A second
  store with its own consistency tokens and dual-write problem; list APIs are
  bounded (OpenFGA ListObjects defaults to 1,000 results in 3 s).
- **Per-row ACL lists** (Accumulo, LabelList). Every row carries principals;
  Lance's LabelList index looks labels up one at a time
  (`lance-index-11.0.0/src/scalar/label_list.rs:140-144`), and grant changes
  rewrite rows. Securable indirection makes a grant one edge.
- **One dataset per securable.** Hard isolation, but table count would follow
  securable count into the hundreds of thousands.
- **Encryption per securable.** Breaks indexes and search; deferred to vault
  infrastructure.
- **Row-address masks as the primary mechanism.** Lose the scalar index on plain
  scans and are valid for one table version; kept as a later cache.
- **Post-filtering.** Leaks through counts, ranks and errors; the cause of every
  incident in Appendix B.
- **Branch per share.** A branch inherits its source's data and cannot hold a
  subset.
- **Live-only sharing** (Snowflake, Delta Sharing). Exposes live side channels to
  outside parties; kept as one mode for trusted collaborators.

## Evidence and tests

### Owners to extend

| Behavior | Owner |
|---|---|
| Annotation parsing, validation, IR, features, lint | In-source compiler tests; `schema_plan` tests |
| Policy version 2, classes, unrestricted, validation | `omnigraph-policy` in-source tests |
| Engine read and write enforcement | `crates/omnigraph/tests/policy_engine_chassis.rs` |
| Non-interference across operators | `crates/omnigraph/tests/proptest_equivalence.rs`: for generated graphs and policies, a restricted actor's result equals an unrestricted result on the materialized view |
| Restricted vector and full-text search | `crates/omnigraph/tests/search.rs` |
| Traversal masks | `crates/omnigraph/tests/integration/traversal_indexed.rs` |
| Write rules | `crates/omnigraph/tests/writes.rs` |
| History refusal and dual labels | `crates/omnigraph/tests/integration/point_in_time.rs` |
| View-filtered change feed | `crates/omnigraph/tests/integration/changes.rs` |
| Replay refusal on view change | `crates/omnigraph/tests/integration/engine_v2_plan_replay.rs` |
| Lance facts in the table above | `crates/omnigraph/tests/lance_surface_guards.rs` |
| HTTP surfaces and redaction | `crates/omnigraph-server/tests` `auth_policy` suite; OpenAPI |
| Embedded and remote parity | `crates/omnigraph-cli/tests` parity matrix |
| Grant writes concurrent with reads | `omnigraph-dst` scenario |

GQT gains an actor section so view semantics become logic tests; until then the
Rust owners above carry them.

### Acceptance thresholds

- An unrestricted actor's queries make no more object-store requests than
  today (the warm read cost instrument).
- A restricted read with a warm view cache adds no object-store request on
  `main` and one version probe on another branch.
- A restricted scan of a secured type with a covered BTREE adds at most the
  index reads of one `IsIn` evaluation.
- Restricted vector search: recall@10 ≥ 0.95 against view ground truth at
  visible fractions 100%, 10%, 1%, 0.1% and 0.01%, and with views negatively
  correlated with the query; latency within 2× unrestricted at ≥ 10%, and
  within the exact-scan cost at ≤ 1%.
- Restricted full-text search: nDCG@10 with the chosen statistics source within
  10% of index-wide statistics on the fixture.
- A grant or revocation is one row write and governs the next request.

### Upstream surfaces surveyed

Lance 11.0.0: `dataset/scanner.rs`, `io/exec/knn.rs`, `io/exec/fts.rs`,
`io/exec/filtered_read.rs`, `io/exec/count_pushdown.rs`, `dataset/optimize.rs`,
`dataset/write/merge_insert.rs`, `lance-index` `scalar/expression.rs`,
`scalar/btree.rs`, `scalar/bitmap.rs`, `scalar/label_list.rs`,
`scalar/zonemap.rs`, `scalar/inverted/*`, `lance-select` `mask.rs`. Pages read
in full: the BTREE, Bitmap, LabelList and full-text index formats, and the
performance guide.

## Rollout

Each phase ships independently and leaves the system safe.

| Phase | Ships | Still unavailable |
|---|---|---|
| 0. Hardening | Engine read doors take an actor; invariant 10 covers reads; `MetadataCount` refuses a filtered spec; `Predicate` consumers become exhaustive; errors carry foreign facts as structured fields. | No new contract. |
| 1. Schema views | `@sensitivity`, policy `classes` and `unrestricted`, schema view and memoized view catalogs, `/schema` and `/queries` filtering, explain redaction. | Row security. |
| 2. Row reads | Authorization types, `@secured_by` with BTREE, authorization capture and view cache, readable set with barriers, view leaf, coverage validator, view digest in replay, masked indexed traversal, adaptive vector planning, uniform full-text statistics for restricted actors, history and feed refusal. | Restricted actors cannot write secured types; CSR is off for restricted views. |
| 3. Row writes | Write check, `grant` and `reclassify`, authorization types main-only, merge rule, error redaction. | — |
| 4. Agents | Delegated credentials and caveats, `ground` and `discover` tiers, integrity labels, MCP audience binding. | — |
| 5. Search quality | Public statistics artifact, recall sampling. | — |
| 6. Performance | Per-view CSR ordinal masks, row-mask cache after its guard. | — |
| 7. History and feeds | Dual-label historical reads; view-filtered change feed, commits and export. | — |
| 8. Derived content | `@provenance`, intersection visibility, declassification edges. | — |
| 9. Sharing | Share principals, live shares, share reconciler for hosted and delivered copies, definer-rights stored queries, write-back proposals, per-actor read admission. | Privacy budgets. |
| 10. Vault infrastructure | Separate RFC: per-graph storage roots, credentials and keys; non-discoverable graphs; a shared cache budget; read-many, write-one cross-graph queries. | — |

`implementation` moves to `in-progress` with Phase 0 and to `partial` when
Phase 2 ships.

Every phase lands with its user-guide changes, a `changelog.d` note when it is
user-visible, and the regenerated `openapi.json` when the wire changes. Each
exit criterion below is checked by the owner named in
[Evidence and tests](#evidence-and-tests).

### Phase 0: read-path hardening

Makes the engine able to enforce reads and removes shapes that would let a
later filter be dropped. No contract changes.

- Add actor-taking variants of every read door: `query`, `run_query_at`,
  `explain_query`, `query_inspected`, `replay_bound_plan`
  (`crates/omnigraph/src/exec/query_doors.rs`), `snapshot_of`,
  `commit_changes_page`, `poll_change_feed`, `get_commit`, `list_commits`
  (`crates/omnigraph/src/db/omnigraph.rs`), the export and baseline cuts
  (`crates/omnigraph/src/db/omnigraph/export.rs`) and `read_blob_at`
  (`crates/omnigraph/src/blob.rs`). Handlers pass the resolved actor. When a
  policy is installed, the engine applies the `read` and `export` gates itself.
- Make `Predicate::gq_filters()` match exhaustively, so a new predicate variant
  is a compile error rather than a silent drop.
- Make the `MetadataCount` lowering refuse a scan spec that carries a filter.
- Carry foreign facts in errors as structured fields (other-row id, count,
  ETag, another table's version). Rendering stays as today.
- Make `Omnigraph::graph_index()` crate-private.

Exit: the canonical workspace gates pass; `policy_engine_chassis.rs` covers an
embedded read denied by policy; no wire change.

### Phase 1: schema views

Ships type- and property-level confidentiality.

- Compiler: validate `@sensitivity`; store it in a typed IR form bound to
  stable type and property ids, like constraints; derive the `actor-views`
  feature; add schema-apply plan steps that add or remove a sensitivity class.
- Policy: parse version 2 `views.classes`, `views.unrestricted` and
  `views.limits`; validate them against the graph's contract where stored
  queries are validated today.
- Engine: build the schema part of the view spec from the actor's cluster
  groups; memoize view catalogs per (contract, schema-view digest); give the
  accepted-catalog memo more than one slot; key compiled queries by the view
  catalog.
- Server: serve the schema view on `/schema`; filter `/queries` and MCP
  `queries`; redact explain; add the `view` response object; answer
  `view_restricted` on history, feeds and export for any actor whose view hides
  something, until Phase 7.
- GQT: add an actor section to the case format, so view semantics are logic
  tests from this phase on.

Exit: the non-interference property in `proptest_equivalence.rs` holds for
schema views (the materialized view drops hidden types and columns); a hidden
name and an undeclared name produce identical errors.

### Phase 2: row reads

Ships row-level read confidentiality. Restricted actors cannot yet write
secured types; they are refused with `view_write_denied`.

- Compiler: validate `@principal`, `@group`, `@securable`, `@membership`,
  `@grant`, `@barrier`, `@barrier_exception`, `@secured_by` and `@restricts`;
  add the `OG-OW` lint codes for keyed secured types; map `@secured_by`
  properties to a BTREE in `node_prop_index_kind`.
- Engine, capture: one function that takes the schema permit, captures the
  authorization pins on `main` and the data snapshot, and keeps the `main`
  snapshot that snapshot-target reads already load.
- Engine, readable set: the internal membership plan, barrier subtraction,
  tier sets and limits; a byte-bounded view cache outside
  `invalidate_read_caches`.
- Engine, plan: the `ViewSet` IR leaf and its lowering; view conjuncts placed
  into scan specs at plan construction; the coverage validator in
  `engine::execute`; the view digest in plan assumptions, checked on replay.
- Engine, operators: per-hop frontier intersection in indexed traversal;
  refusal of CSR, degree shortcuts and CSR-derived search gates for restricted
  types; vector planning from the visible count; full-text search for restricted
  actors through a hand-built plan with uniform statistics; Blob reads checked
  against row visibility.
- Server: refuse to serve an `actor-views` graph in unauthenticated mode or with
  static tokens and no policy.
- Completeness: `view.restricted`, the `@complete` stored-query annotation, the
  `complete` request option and `view_incomplete`.
- Polymorphic types, if union scans have landed: view conjuncts per arm,
  validator coverage of arms and polymorphic expansions, member sets from the
  schema view, the view digest in place of the schema hash.
- Lance guards: exact String BTREE `IsIn` prefiltering KNN and WAND; supplied
  base-scorer statistics change scores; `CountPushdown` on an exact BTREE count.

Exit: the non-interference property holds over scans, aggregates, single- and
multi-hop traversal, `count` and `not` blocks, anti-joins, and vector, full-text
and fused search at visible fractions from 100% to 0.01%; the cost and recall
thresholds hold; a DST scenario interleaving grant writes with reads never
returns a row its request's authorization state hides.

### Phase 3: row writes

Ships writes by restricted actors.

- Committed state: batched lookup of the committed `@secured_by` value by id.
- Checks in `validate_staged_mutation`, `validate_changeset` and merge
  evaluation: writable before and after images, `securable_required`,
  `reclassify`, overwrite refusal, cascade refusal, edge endpoint visibility,
  view-scoped matching and counts.
- Authorization types: `grant` action, `main`-only writes, `owner` on the target
  securable for grant edges, refusal of branch writes and merges that touch them.
- Server: strip foreign facts from errors for restricted actors;
  `key_conflict` without an entity id.

Exit: `writes.rs` and `policy_engine_chassis.rs` cover every row of the write
table; each refusal leaves the manifest head, table pins and lineage unchanged.

### Phase 4: agents, tiers and integrity

- Delegated credentials: subject, actor and audience; caveats compiled into the
  view spec; acceptance of the RFC 8693 `act` claim only for credentials of this
  form.
- The `ground` tier for automation principals and delegated credentials; the
  `discover` tier in search operators.
- `@integrity` and the response's integrity floor.
- MCP: audience check; no token passthrough.

Exit: the server identity suites cover delegated credentials and caveats;
`search.rs` covers the `discover` tier.

### Phase 5: search quality

- Public statistics built by explicit index reconciliation, stored as derived
  state, with uniform statistics as the fallback.
- A recall sampling instrument for restricted vector search.

Exit: the ranking threshold holds on the fixture.

### Phase 6: performance

- Per-view ordinal masks over a CSR instance, keyed by the instance and the
  node-table versions; CSR mode re-enabled for restricted types. If the
  polymorphic types RFC's global ordinal index has landed, one bitmap per view
  covers every node type.
- An optional per-view row-mask cache, after a guard covers Lance's
  `CountPushdown` with a row mask.

Exit: traversal and search cost instruments show no regression against
Phase 2 at equal results.

### Phase 7: history and feeds

- Historical reads bind properties by stable identity (prerequisite).
- Dual-label historical reads.
- View-filtered change feed: view digest in cursors, visibility transitions as
  inserts and deletes, projected equality.
- View-filtered commits, commit changes, export and baseline.

Exit: `point_in_time.rs` and `changes.rs` cover moved rows, revoked members and
hidden-property changes; cursors refuse a changed view.

### Phase 8: derived content

- `@provenance`; intersection visibility through compound securables; the
  `declassify` action and declassification edges.
- Recomputation of dependants in the same publication when a source is
  reclassified, bounded and refused over the bound.

Exit: the non-interference property holds with provenance chains.

### Phase 9: sharing

- `shares` in cluster configuration; share principals; live shares.
- The share reconciler for hosted and delivered copies, with the cursor row,
  baseline handling and agreement checks.
- Definer-rights stored queries with parameter domains and invocation logs.
- Write-back proposals from a copy's branch to a source proposal branch.
- Per-actor read admission.

Exit: a hosted copy equals the materialized view of its share principal after
every reconciled commit; revocation deletes the copy and its proposals.

### Phase 10: vault infrastructure

A separate RFC: per-graph storage roots, credentials and keys in one cluster;
non-discoverable graphs; a cache budget shared across graphs; read-many,
write-one queries across graphs.

## Unresolved questions

1. Which full-text statistics restricted actors get: public or uniform
   (`blocked_on`).
2. Whether the `discover` tier also covers traversal fan-out, or only search
   operators as specified.
3. How compound securables for derived content are represented: a system type
   or a user-declared `@securable` type with a member list.
4. In Phase 7, whether commits with no visible change are listed to restricted
   actors.
5. Whether edge types need their own `@secured_by` in a later RFC.
6. Whether design partners accept key-existence disclosure to writers, or need
   securable-scoped uniqueness.
7. Whether stored queries bound by UI components should default to `@complete`.
   The polymorphic types RFC chose refusal for interface members a reader may
   not see; this RFC proposes filtering with a signal and opt-in refusal, and
   that RFC's Policy section needs the matching amendment before either is
   accepted.

## Decision log

- 2026-10-11: Drafted. The design was checked against the code at `6e31d0ab`
  and Lance 11.0.0; Appendix A records every assumption with its verdict.
  Prior art is in Appendix B.
- 2026-10-11: Aligned with the polymorphic types draft
  (ModernRelay/omnigraph#884). Added
  [Polymorphic types](#polymorphic-types): abstract member sets come from the
  schema view; `policy_denied_member` and `policy_filtered` are replaced by
  `view.restricted` and an opt-in `@complete` refusal; security annotations on
  interface properties cannot be dropped or added per member; removing one by
  any migration route is declassification; `@secured_by` may name an interface
  of securables with interface-scope `@unique`; authorization edges may use
  polymorphic endpoints; per-view graph-index masks target the global ordinal
  space. `@secured_by` admits a nullable property, null hiding the row from
  restricted actors.

## Appendix A: validated assumptions

Verdicts: ✓ confirmed, ◐ partly true (the design uses the corrected form),
✗ false (the design does not rely on it).

### Lance 11.0.0

| Claim | Verdict | Evidence |
|---|---|---|
| `filter_expr` bypasses SQL; `prefilter` defaults to false | ✓ | `dataset/scanner.rs:1585, 1286` |
| Row-address prefilter honored by KNN, indexed FTS, flat FTS, plain scan | ◐ | Mask is in `_rowid` space; applied after scoring on flat FTS; demotes the filter on plain scans (`scanner.rs:1466-1470, 4995, 3298-3306`) |
| BTREE and Bitmap `IsIn` exact, unbounded, NULL disables index | ✓ | `lance-index` `scalar/expression.rs:348-352`; `btree.rs:3347`; `bitmap.rs:1848` |
| LabelList looks labels up sequentially | ✓ | `scalar/label_list.rs:140-144` |
| Indexed FTS masks before the top-k heap | ✓ | `scalar/inverted/wand.rs:2299` |
| BM25 statistics are index-wide, including filtered and deleted rows | ✓ | `scalar/inverted/index/search.rs:53-63`; `scorer.rs:197` |
| External BM25 statistics can be supplied | ◐ | Only below the scanner, `with_base_scorer` (`io/exec/fts.rs:624, 2175, 3058, 3485`) |
| No automatic flat switch for selective KNN; `+inf` placeholders | ✓ | `io/exec/knn.rs:1710-1782` |
| `count_rows(None)` is manifest-only | ✓ | `dataset/fragment.rs:1366-1392` |
| Mask plus `count_rows` | ◐ | `CountPushdown` expects a `ScalarIndexExec` (`io/exec/count_pushdown.rs:228-235`); untested, guarded before use |
| Compaction materializes deletions above 10%; stable ids skip index remap | ✓ | `dataset/optimize.rs:322, 2580` |
| Merge-insert cannot fail on a conditional match | ✓ | `merge_insert.rs:425-449`; `assign_action.rs:107-139` |
| WAND switches to flat at ≤ 10% allow-list | ✓ | `wand.rs:201-206` |

### Engine read path

| Claim | Verdict | Evidence |
|---|---|---|
| Non-pushable conjuncts become an in-memory filter after `nearest` top-k | ✓ | `crates/omnigraph-planner/src/optimizer.rs:1426-1461` |
| Lowering silently skips conjuncts | ◐ | Guarded by `validate_plan_values` (`crates/omnigraph/src/engine/typed_value.rs:826-833`), except `MetadataCount` (`engine/lower.rs:473-487`) |
| `prefilter(true)` on every filtered scan | ✓ | `crates/omnigraph/src/engine/scan.rs:248-251` |
| List parameters lower to `in_list` | ✓ | `scan.rs:1093-1119` |
| BFS never reads intermediate nodes; edge reads unfiltered | ✓ | `crates/omnigraph/src/engine/graph.rs:1132-1472, 1647-1662` |
| CSR is a whole-graph cache with no actor | ✓ | `crates/omnigraph/src/graph_index/mod.rs:168-245`; `runtime_cache.rs:24-57` |
| Degree shortcut and `MetadataCount` require an unfiltered spec | ✓ | `engine/lower.rs:230-277`; `optimizer.rs:1603-1647` |
| Interfaces cause polymorphic scans | ✗ | Bindings are concrete (`typecheck.rs:1785-1790`) |
| `Predicate::gq_filters()` keeps every variant | ✗ | Drops non-`Gq` variants (`crates/omnigraph-planner/src/logical.rs:125-140`) |
| Engine read doors take no actor | ✓ | `crates/omnigraph/src/exec/query_doors.rs:41-170` |
| Explain exposes whole-table facts | ✓ | `engine/explain.rs:236, 272`; `physical.rs:363-370`; `gate.rs:58-71` |

### Writes and schema

| Claim | Verdict | Evidence |
|---|---|---|
| Keyed insert is a full-row upsert | ✓ | `crates/omnigraph/src/exec/mutation.rs:425-426, 1314-1318`; `table_store.rs:3442-3443` |
| Overwrite replaces all rows of a type | ✓ | `crates/omnigraph/src/loader/mod.rs:765-772, 939-957` |
| Delete cascades and counts hidden edges | ✓ | `mutation.rs:1717-1741` |
| Constraint messages name other rows | ✓ | `crates/omnigraph/src/validate.rs:1259, 1295-1296` |
| A before image exists at validation | ◐ | Not held; obtainable from the committed state (`validate.rs:391-433`) |
| `@index` on a String builds a BTREE | ✗ | Builds full-text (`table_ops.rs:582-605`) |
| Unknown annotations are refused | ✗ | Kept as metadata (`parser.rs:1074`); features are refused (`schema_ir.rs:1402-1406`) |
| Schema apply is main-only | ✓ | `crates/omnigraph/src/db/omnigraph/schema_apply.rs:153-162` |
| Edge endpoints have BTREEs | ✓ | `table_ops.rs:874-890` |

### Server, policy and cluster

| Claim | Verdict | Evidence |
|---|---|---|
| Reads authorized only in handlers; MCP shares them | ✓ | `crates/omnigraph-server/src/handlers.rs`; `mcp.rs:272-310` |
| Static tokens without policy allow reads; open mode allows all graph actions | ✓ | `handlers.rs:651-668` |
| Signed and OIDC actors without policy are denied | ✓ | `handlers.rs:622-626` |
| Delegation claims accepted | ✗ | OIDC rejects `act` and `impersonator` (`oidc_identity.rs:346-347`) |
| Principal kind exists | ✓ | `data_tokens.rs:26-29` |
| Policy is allow-only, compiled with `format!` | ✓ | `crates/omnigraph-policy/src/lib.rs:236-240, 825-866` |
| Per-actor limits exist | ◐ | Writes only (`workload.rs:15-19`) |
| Durable read audit exists | ✗ | Decision logs only when a policy is installed (`handlers.rs:575-589`) |
| Snapshot reads use current policy | ✓ | `handlers.rs:1764-1784` |
| Historical reads rebind tables by stable identity | ✓ | `crates/omnigraph-catalog/src/lib.rs:608-656` |
| One snapshot per query | ✓ | `crates/omnigraph/src/db/graph_coordinator.rs:52` |
| Graphs in a cluster share storage and credentials | ✓ | `crates/omnigraph-cluster/src/store.rs:240-242`; `crates/omnigraph-storage/src/lib.rs:1247` |
| Per-graph Lance caches have no shared budget | ✓ | `crates/omnigraph-core/src/lance_access.rs:38-42` |
| Read caches flush on every observed advance of `main` | ✓ | `crates/omnigraph/src/db/omnigraph.rs:2026-2034` |

## Appendix B: prior art consulted

- Cedar: type-aware partial evaluation, RFC 0095; changelog through 4.13.0;
  level validation; SymCC. https://github.com/cedar-policy/rfcs/blob/main/text/0095-type-aware-partial-evaluation.md
- OPA partial evaluation fragment and v1.19.0 SQL injection fix.
  https://www.openpolicyagent.org/docs/filtering/fragment
- Zanzibar (USENIX ATC 2019), new enemy problem and Leopard.
  https://www.usenix.org/system/files/atc19-pang.pdf
- SpiceDB consistency and Materialize; OpenFGA ListObjects limits.
  https://authzed.com/docs/spicedb/concepts/consistency
- OpenID AuthZEN Authorization API 1.0 (final, 2026-01-12).
  https://openid.net/specs/authorization-api-1_0.html
- PostgreSQL row security, `USING`, `WITH CHECK`, constraint covert channels.
  https://www.postgresql.org/docs/current/ddl-rowsecurity.html
- Oracle Label Security and Db2 LBAC; Sandhu and Jajodia on polyinstantiation.
- Accumulo cell visibility. https://accumulo.apache.org/docs/2.x/security/authorizations
- MarkLogic compartments and element-level security.
- Neo4j fine-grained access control, TRAVERSE versus READ, documented limits.
  https://neo4j.com/docs/operations-manual/current/authentication-authorization/limitations/
- SurrealDB advisories GHSA-vjjx-rfw4-rmfc and CVE-2026-49997; Dgraph
  CVE-2026-41327; TigerGraph row policy exemptions; Stardog and AllegroGraph.
- Rizvi, Mendelzon, Sudarshan, Roy, SIGMOD 2004; Kanza et al., EDBT 2006;
  Fong, CODASPY 2011.
- Elasticsearch document-level security and global statistics; Büttcher and
  Clarke, FAST 2005; STRESS, IEEE S&P 2017.
  https://stressattack.github.io/paper/sp17stress.pdf
- Filtered vector search: ACORN (SIGMOD 2024), Filtered-DiskANN (WWW 2023),
  Curator, HoneyBee (SIGMOD 2026); Qdrant, Weaviate, pgvector, OpenSearch and
  Elasticsearch cut-offs.
- Microsoft Purview: label inheritance for Copilot, Restricted Content
  Discovery, DLP for Copilot, Information Barriers.
  https://learn.microsoft.com/en-us/purview/ai-m365-copilot
- iManage Security Policy Manager; Harvey and Intapp ethical walls.
  https://www.harvey.ai/blog/agents-and-ethical-walls
- Apache Atlas classification propagation; Ranger tag policies; Unity Catalog
  ABAC and DENY; Snowflake tag propagation.
- FIDES (arXiv 2505.23643), CaMeL (arXiv 2503.18813), design patterns for LLM
  agents (arXiv 2506.08837).
- Snowflake row access policies, secure views and secure data sharing; BigQuery
  time travel with row-level security; Databricks ABAC with time travel and
  Delta Sharing.
- Slack Connect, Teams shared channels, AWS Clean Rooms analysis rules and
  differential privacy, Snowflake aggregation policies, Solid WAC and ACP, ODRL
  2.2, Eclipse Dataspace Components policy monitor, DoltHub forks, lakeFS
  hooks.
- RFC 8693 token exchange; MCP authorization 2026-07-28; Biscuit; Macaroons;
  UCAN.
