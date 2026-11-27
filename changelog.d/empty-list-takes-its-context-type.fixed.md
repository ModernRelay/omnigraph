- `[]` takes its element type from any context, not only an assigned list
  property: the other side of a comparison or `in`, `if` or `coalesce`. With
  no context it is refused (`T7`).
