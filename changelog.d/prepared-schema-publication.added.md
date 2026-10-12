- Embedded callers can prepare schema changes before execution, receive the
  operation's exact commit receipt, and reconcile retained publication evidence
  without applying again. An unknown reconciliation outcome does not authorize
  retry; see the [graph write protocol][prepared-schema-writes].

[prepared-schema-writes]: ../docs/dev/writes.md
