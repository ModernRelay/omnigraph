- An `update` evaluates its assignments before it opens or scans its table,
  so a null assigned to a non-nullable property of any type, `Blob` included,
  is refused even when the predicate matches no row. Such an update returned
  0 affected rows before.
