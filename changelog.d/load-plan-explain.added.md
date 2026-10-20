- `explain` accepts a `load csv` statement and prints the planner's document
  for it, the same shape and `explain_version` 6 a query's explain prints,
  without reading a payload byte; see the [explain guide][load-plan-explain-guide].
  An overwrite load runs that plan through the engine's load operators.
  A GQT load step takes an `--- expect plan` section, checked against the
  executed load's document ([plan expectations][load-plan-gqt-guide]).

[load-plan-explain-guide]: ../docs/user/queries/explain-load.md
[load-plan-gqt-guide]: ../crates/omnigraph-gqt/README.md#plan-expectations
