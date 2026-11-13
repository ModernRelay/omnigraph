- A ranked `from` names its score with `as`: `from nearest($pop, $d.embedding, $q, k: 20) as $distance`. Without `as` it is refused with `T85`, whose fix writes the old column name (`as $distance`, `as $score`); an `as` on a `from` that ranks nothing is `T85` too. See [search][gq-3-t85-guide].

[gq-3-t85-guide]: ../docs/user/search/index.md
