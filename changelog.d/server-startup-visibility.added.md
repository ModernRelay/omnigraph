- Server health, readiness and authorized graph inventory are available while graphs load. Ready graphs can serve immediately; `--require-all-graphs` keeps graph admission closed until the entire startup succeeds. Readiness adds `loading_graph_count`, and inventory adds `loading` with action `wait_for_startup`; startup attempts remain owned through shutdown. See [deployment][startup-visibility].

[startup-visibility]: ../docs/user/deployment.md
