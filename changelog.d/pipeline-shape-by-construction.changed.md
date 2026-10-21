- Whether an operator holds its input is one planner fact, `PipelineShape`,
  read by explain's `pipelines`, the push executor and the engine alike: the
  engine builds every operator's output stream through the wrapper of that
  shape (`Breaker`, `Streaming`, or `Source` for a leaf), so a breaker cannot
  emit before its body returns and a streaming operator never holds its
  input, and a join drains the side the planner names through one
  `CollectedSide` before its streamed side executes. The lowering no longer
  compares a declared emission against the plan's cut, except for
  DataFusion's own `AggregateExec`. The printed explain documents do not
  change.
