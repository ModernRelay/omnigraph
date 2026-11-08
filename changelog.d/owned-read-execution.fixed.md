- Disconnected HTTP reads and cancelled or timed-out MCP reads retain their input and concurrency reservations until execution finishes. Shutdown waits for these reads, and query execution panics wait for registered graph workers before propagating. See [server admission and shutdown][owned-read-admission].

[owned-read-admission]: ../docs/user/deployment.md
