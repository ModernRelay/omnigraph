- Query explain now lists declared result column names and types for projections, aggregates and metadata counts. Saved bound plans use version 3 and retain complete node-object types for replay validation; regenerate version 1 or 2 plans. See the [explain guide][typed-result-columns].

[typed-result-columns]: ../docs/user/queries/explain.md
