- `omnigraph-server` can serve one standalone graph to clients of the same account on loopback. `AppState::local` opens it read-only or read-write behind a generated bearer token and a generated policy for one actor, `serve_local` runs the server's shutdown lifecycle on a listener the caller bound and reports an uncertain outcome instead of exiting, and `install_log_subscriber` applies the server's log filter to another writer. See [public embedding APIs][local-server-embedding].

[local-server-embedding]: ../docs/dev/control-plane.md#public-embedding-apis
