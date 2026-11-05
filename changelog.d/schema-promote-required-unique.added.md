- `schema apply` can make a nullable property required and add node `@unique` to a populated type; it checks stored rows first and refuses, changing nothing, on a null or duplicate ([#721][721]).

[721]: https://github.com/ModernRelay/omnigraph/issues/721
