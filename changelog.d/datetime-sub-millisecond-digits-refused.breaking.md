- Clients that send `DateTime` strings with more than three fractional digits
  must truncate them to milliseconds, unless the extra digits are zeros: such
  a string used to load and match at its millisecond and now fails the
  request. Python's `datetime.isoformat()` writes microseconds by default; use
  `isoformat(timespec="milliseconds")`. A stored or cluster-declared query
  whose source holds such a `datetime(...)` literal now fails type checking,
  so its graph fails startup until the literal is trimmed. A Rust caller that
  binds a `DateTime` parameter through `ParamMap` must bind a
  `Literal::DateTime` (a `Literal::List` of them for `[DateTime]`, or
  `Literal::Null` on a nullable parameter); a `Literal::String` or any other
  literal is refused, as for `Date`. A parameter named `__nanograph_now` is
  refused: the name is reserved for the clock `now()` reads, and a value bound
  under it used to replace that clock without any check.
