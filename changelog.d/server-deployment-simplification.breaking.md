- Cluster plan now always observes without taking the writer lock, and migration
  previews use the same captured preparation as apply. The removed `--observe`,
  `--lifecycle` and `--schema-correction` flags are rejected. Apply supports graph
  creation, physical graph deletion and configuration changes. Removing a graph
  declaration and applying deletes its managed storage and retained history;
  interrupted deletion resumes through the original deployment ID. There is no
  unregister mode. Adoption, missing-root recreation, drift acceptance and catalog
  repair are no longer apply modes.

- Deployment activation is reported from the current process runtime snapshot,
  without a second ledger write after activation. Durable results no longer store
  `activation` or `restart_required`. Finish outstanding deployments with their
  originating build and run the stopped-ledger upgrade before starting the new
  build on a ledger containing those completed-result fields. Conversion preserves
  graph data and exact achieved receipts.

- The CLI also rejects the obsolete `repair --confirm` and `repair --force`
  controls. Repair continues diagnosing foreign linear table commits without
  adopting them.
