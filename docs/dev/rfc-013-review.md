# RFC-013 Review — Node Identity (Surrogate + Mutable Natural Key)

**Reviews:** [rfc-013-node-identity-surrogate-key.md](rfc-013-node-identity-surrogate-key.md)
(PR #287, Status: Proposed)
**Reviewer verdict:** strong proposal; merge after promoting three gaps to
explicit open questions/drawbacks. Docs-only, low risk to land.

This is a maintainer review companion for RFC-013. It records the substantive
findings so the RFC's design decisions and their open edges are tracked
alongside the proposal, not lost in PR comment threads. Items marked **must
address before Accepted** change what an implementer has to resolve.

## What the RFC gets right

- **Derive-then-freeze is the correct load-bearing choice.** Making the
  surrogate a deterministic function of the initial `@key` (rather than a random
  ULID) is what gives both merge convergence (two branches inserting the same
  natural key derive the same surrogate and unify) and zero data migration
  (existing keyed nodes already store `id == @key`, so nothing on disk moves
  until the first post-upgrade rename). Both claims hold.
- **The prior-art survey is accurate.** The two-layer split (immutable reference
  target + separate mutable unique key) is the dominant pattern across
  Neo4j / Datomic / Dgraph / HelixDB, and Dolt's random-UUID-duplicates-on-merge
  guidance is correctly read as a cautionary tale rather than a recommendation.
- **The invariants check is honest.** It correctly classifies the logical
  surrogate as the source-of-truth string and Lance stable row ids as a derived
  physical accelerator (invariant 15 / the dec-918 boundary), and identifies the
  cross-version-uniqueness tightening as the single breaking-direction change.

## Findings

### 1. Surrogate-reuse collision after a rename — **must address before Accepted**

Not raised by the automated reviewers. Derive-then-freeze makes the surrogate a
deterministic function of the *initial* `@key`, and `id` must be unique (it is
the edge-resolution and merge-matching key). A rename frees the natural key but
leaves the surrogate burned:

1. Insert X with `@key = "alice"` → `id = "alice"`.
2. Rename X: `@key := "alice2"`. `id` stays `"alice"`; the natural key `"alice"`
   is now free.
3. Insert Y with `@key = "alice"` → derives surrogate `"alice"` → **collides
   with X's frozen `id`.**

Outcome is either a confusing hard failure (the natural key is free, yet the
insert is rejected on surrogate uniqueness) or — worse — Y is conflated with X
(corruption). This is a *new* failure mode the design introduces: today
`delete + reinsert` reuses a key cleanly because the row is actually gone, but a
rename leaves the surrogate live.

It directly qualifies the headline promise. The old name is free in the
*natural-key* namespace but permanently consumed in the *surrogate* namespace,
and because new inserts derive the surrogate from the initial `@key`, a
renamed-away name can never seed a new entity. This is intrinsic to any
"deterministic-from-initial-key" surrogate, so it cannot be designed away
without giving up merge convergence — it belongs as an explicit **Drawback**,
and the engine behavior on the collision (loud rejection vs. something else)
must be specified.

The RFC's existing Drawbacks cover cross-*branch* non-unification but not this
same-*branch* reuse case.

### 2. Endpoint-resolution precedence is undefined — **must address before Accepted**

Edge authoring accepts "a surrogate `id` **or** a natural-key value," resolved
"through the unique index." During the window where the two namespaces overlap
(a frozen `id` and a current `@key` can be the same string on different rows),
the resolution order is load-bearing and unspecified. State whether resolution
is id-first or natural-key-first, and what happens when a reference matches both.

### 3. Composite `@key` → scalar surrogate encoding is unspecified — **must address before Accepted**

(Also raised by Greptile; reinforced here.) `loader::composite_unique_key`
returns a separator-free `Vec<String>` *specifically* to avoid the ambiguity
collisions that a flattened scalar key reintroduces (see invariants.md, unique
constraints row). Mapping that tuple onto a scalar `id` string therefore needs a
deterministic **and injective** encoding — and merge convergence depends on both
properties holding identically at insert, update, and resolution time. The RFC
should specify the encoding (e.g. a canonical, injective serialization) or name
it a hard open question, not leave it implicit in Open Question 3.

### 4. Rollout step 4 atomicity — **should state explicitly**

(Also raised by Greptile.) Step 4 removes the `@key`-update rejection and lands
cross-version uniqueness (iss-714) together. These must ship as one unit:
removing the rejection alone opens a window where `@key` can change freely while
uniqueness is enforced only intra-batch, admitting duplicate natural keys
against committed rows. Add one sentence to the Rollout making the atomicity a
requirement, not an ordering accident — especially because cross-version
uniqueness is itself a still-open engine gap (invariants.md), so step 4 inherits
that risk.

### 5. Internal ticket identifiers — minor consistency nit

Greptile flags the `iss-*` / `MR-*` / `dec-*` slugs as violating the
audience-neutral docs rule (AGENTS.md rule 5). Downweighted: the maintainer
dev-track RFCs (`rfc-00N-*`) already use `MR-*` references in the dev index, so
this is consistent with the established team-internal track rather than a
regression. The dev-graph `iss-<slug>` references are more opaque; the RFC
mostly glosses them inline already. Worth a light pass, not a blocker.

## Recommendation

Land it as a `Proposed` RFC — capturing the design and its open edges is exactly
what the dev track is for — **after** promoting findings 1–3 to named
drawbacks/open questions and adding the one-sentence atomicity note (4). Finding
1 is the most important: it is the standout gap the automated reviews missed and
it materially conditions the "`@key` is now mutable" promise. The merge-matching
choice (derive-then-freeze) remains the right call and the least reversible
piece, so it correctly earns the ADR the RFC already proposes.
