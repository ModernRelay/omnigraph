- A filter or mutation `where` that tests a nullable property twice with `=`, `!=` or `in` no longer selects, updates or deletes rows whose value is null. See the [query guide][null-rows-in-tests].

[null-rows-in-tests]: ../docs/user/queries/index.md#boolean-expressions-and-nulls
