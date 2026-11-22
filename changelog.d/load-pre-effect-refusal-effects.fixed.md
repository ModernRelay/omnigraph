- A load refused before its first effect, such as a malformed row or a refused `load csv` source, reports `effects: none` and `action: fix`, not `unknown` and `reconcile`. See [failed writes][load-refusal-effects].

[load-refusal-effects]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
