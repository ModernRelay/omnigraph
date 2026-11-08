- Every read plan is checked against the query before it runs, and `explain`
  reports what the check covered in a `validation` field: `exact_subset` for a
  single-binding query with property filters, an optional leading `bm25()`,
  property sort keys and a limit, whose every rewrite was checked, and
  `invariants_only` for every other query. See the [explain
  guide][plan-acceptance-explain].

[plan-acceptance-explain]: ../docs/user/queries/explain.md
