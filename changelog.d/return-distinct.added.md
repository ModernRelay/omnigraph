- `return distinct { … }` keeps one row per distinct projected tuple: repeated matches and equal values on different entities collapse, nulls compare equal, and deduplication precedes `order` and `limit`. Order keys after any leading search function must be projected values or aliases; remaining projected values break ties. A search function without subsequent keys selects the input population but leaves the distinct results unordered. See [Return, order, and limit][return-distinct-guide].

[return-distinct-guide]: ../docs/user/queries/index.md#return-order-and-limit
