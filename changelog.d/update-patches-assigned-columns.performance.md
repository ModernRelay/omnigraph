- An `update` that assigns only scalars on a type with Blob or vector properties writes just those columns when that is cheaper, keeping stored external Blob references unread. See the [Blob guide][update-column-patch].

[update-column-patch]: ../docs/user/blobs.md
