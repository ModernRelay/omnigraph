- A ranked `from` names its score with `as`: `from nearest($pop, $d.embedding, $q, k: 20) as $distance`. Without `as` it is refused with `T85`, whose fix writes `as $distance` or `as $score`; so is a `from` over a `let` bound to a ranking, and an `as` on a `from` whose relation carries no unnamed score. See [search][gq-3-t85-guide].

[gq-3-t85-guide]: ../docs/user/search/index.md
