- `AppState::local` also serves a cluster's storage root to the same account on loopback, applying the cluster's own policies and stored queries for one actor and granting only `read` on a graph the cluster binds no policy to. Read-only takes no admission and forbids every write action in each policy; read-write takes the cluster's serving admission, which `serve_local` releases on a clean shutdown and `LocalServing::release` releases for a server that never served. See [public embedding APIs][local-cluster-server].

[local-cluster-server]: ../docs/dev/control-plane.md#public-embedding-apis
