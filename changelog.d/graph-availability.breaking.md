- Update HTTP integrations with the server: graph inventory now has one `graphs` list with availability, and readiness uses ready/blocked counts instead of the removed `quarantined` fields; `served_graph_count` counts the whole registry. Graphs that fail startup remain visible to authorized callers with a sanitized failure category, and authorized requests to blocked graphs return 503 instead of 404. See [deployment status][graph-availability-status]; a 503 does not authorize replaying a write.

[graph-availability-status]: ../docs/user/deployment.md
