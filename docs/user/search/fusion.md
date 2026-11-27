# Fusion

`rrf(arm, arm, …, k: 60, weights: [w, …])` fuses rankings with Reciprocal Rank
Fusion. A `from` draws it into `match` like any ranking and names the fused
score with `as`:

```gq
query hybrid($vector: Vector(4), $text: String) {
  match {
    let $all = { match { $d: Document } return { $d } },
    from rrf(nearest($all, $d.embedding, $vector, k: 10), bm25($all, $d.body, $text), k: 60) as $score,
  }
  return { $d.slug, $d.title }
  order { $score desc }
  limit 10
}
```

## Arms

An `rrf` takes 2 to 16 arms over one node column, of the same name and type
in every arm. An arm is one of:

| Arm | Ranked by |
|---|---|
| `nearest(rel, $d.f, $q, k: n)` | its distance, ascending, over its window of `n` targets |
| `distance(rel, $d.f, $q)` | the exact distance, ascending, over every row of `rel` |
| `bm25(rel, $d.f, $q)` | the exact `bm25_v1` score, descending, over the matching rows of `rel` |
| `rrf(…)` | its fused score, descending |
| `{ match { … } return { $d } order { … } }` | its own `order`: each target at its first row in that order, the tie rule breaking equal keys |

Each arm ranks the distinct targets it selected from 1, equal scores going to
the smaller identity, so a target its population reaches several times takes
one rank and adds one vote. A target's fused score is `weight / (k + rank)`
summed over the arms that selected it, in written order, as an `F64`; an arm
that did not select it adds nothing. Two targets tied in every arm get the
same fused score, and the read's `order` and its tie rule order them.

## Populations

Every arm runs as the same ranking runs standing alone, over its own
population: a `nearest` arm fills its window of `k` from the targets its
population admits, with the same prefilter gate, probe ladder and overfetch,
and `distance` and `bm25` score every row of theirs exactly. The arms may rank
different populations of the same node column:

```gq
query recent_or_relevant($q: Vector(4), $text: String) {
  match {
    let $recent = { match { $d: Document } where $d.year >= 2025 return { $d } },
    let $all = { match { $d: Document } return { $d } },
    from rrf(nearest($recent, $d.embedding, $q, k: 10), bm25($all, $d.body, $text), { match { $d: Document } return { $d } order { $d.year desc } limit 20 }, weights: [2.0, 1.0, 0.5]) as $score,
  }
  return { $d.slug }
  order { $score desc }
  limit 10
}
```

The result is one row per target with the target column and the fused score,
and no other column of any population. Elements and a `where` written after
the `from` run over those rows and refill nothing; a read that needs a
population column draws it again there, correlated on the target:

```gq fragment
let $pop = { match { $d: Doc, $t: Tag, $d tagged $t } return { $d } },
from rrf(nearest($pop, $d.embedding, $q, k: 10), bm25($pop, $d.body, $s)) as $score,
$t: Tag,
$d tagged $t,
```

## Refusals and limits

Two arms that rank the same population, property and query are refused
(`T75`), since `weights` expresses emphasis, and so are arms that rank two
different bindings: the fused target would be a pair. Fewer than 2 or more
than 16 arms, or a `weights` list of another length, is `T75`; a `k` outside
1 to 1,000,000 or a weight outside (0, 1,000,000] is `T71`, and a `k`
parameter bound outside that range refuses the read when it runs. Every arm
row and every fused target counts against the read's memory and
`read_work_limit`: exhaustion refuses the read and never truncates its
answer.

A fused read reports `"by": "rrf"` in its
[`ranking`](index.md#what-a-ranked-read-reports). It is exhaustive when every
`nearest` arm searched everything over its own population; its `distance` and
`bm25` arms always are. `explain` prints one `Fuse` node over the arms, each
the plan its ranking runs standing alone ([Explain](../queries/explain.md)).
