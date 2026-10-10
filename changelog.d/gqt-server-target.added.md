- The GQT runner executes a case against a running `omnigraph-server` with `--server <URL> --graph <ID>` (optional `--token`): every step travels to the route that already serves it and is judged from the answer; a case opts in by declaring the `omnigraph-server` target, which stays unselected without `--server`. The served conformance test runs every opted-in corpus case through an in-process server and requires the in-process verdict; see the [runner guide][gqt-server-target-guide].

[gqt-server-target-guide]: ../crates/omnigraph-gqt/README.md#explicit-execution
