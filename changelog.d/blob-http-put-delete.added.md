- `PUT` and `DELETE /graphs/{graph_id}/blob` replace or clear one Blob value
  of an existing node or edge, each as one graph commit under the `change`
  action. `PUT` takes a raw `application/octet-stream` body of at most 32 MiB,
  inclusive (`415` for another media type, `413` over the limit, refused
  before reading when `Content-Length` declares it, `408` past the body
  deadline). Both take `branch`, default `main`, refuse `snapshot`, and accept
  `If-Match`: `*` or a list of entity tags. A failed `If-Match` is a `412`
  with the new `blob_precondition_failure.current_etag` detail and an `ETag`
  header naming the cell's current value. Success returns a receipt with the
  selector, branch, `kind` (`managed` or `null`), `size` and `etag`, the actor
  and the exact `commit`, which is `null` only when clearing a cell that was
  already null. A `PUT` also returns the `ETag` header.
