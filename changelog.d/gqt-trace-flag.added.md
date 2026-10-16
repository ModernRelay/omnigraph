- `omnigraph-gqt --trace` records GQT steps, assertions, every decision seam crossing with the decider's answer, and available engine spans/events in a bounded JSONL file per DST attempt, one flat row per line with the owning step on every row and a checked-in schema. Trace files survive worker failures and remain separate from replay comparisons. See the [GQT runner guide][gqt-trace-guide].

[gqt-trace-guide]: ../crates/omnigraph-gqt/README.md#run-and-reproduce
