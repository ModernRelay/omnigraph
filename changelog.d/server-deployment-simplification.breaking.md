- Cluster plan now observes without taking the writer lock; apply rechecks
  authority and execution eligibility before effects. The removed `--observe`,
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
  graph data and exact achieved receipts. Normal reads identify qualified old
  receipts with `ledger_upgrade_required` and the stopped-upgrade command.
  Unrelated blocked graphs no longer invalidate activation of an independent
  deployment. The drain timeout bounds admitted-request drainage; owned deployment
  preparation and completion continue under the process shutdown boundary.

- The CLI also rejects the obsolete `repair --confirm` and `repair --force`
  controls. Repair continues diagnosing foreign linear table commits without
  adopting them.
