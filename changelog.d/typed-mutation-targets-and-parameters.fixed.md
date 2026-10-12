- Mutation execution uses the node or edge target retained by the compiler.
  Every declared parameter is completed and validated before any statement
  runs, including unused parameters and mutations matching no rows. Missing
  required parameters are refused; omitted nullable parameters from Rust
  callers bind to null. Expected-head checks keep their existing precedence,
  and `now()` keeps one value across retries.
