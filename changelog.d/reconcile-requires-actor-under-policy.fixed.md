- Deployment recovery that a graph's policy refuses, including one with no `--as`, now refuses `policy_denied` before taking the lock, not `deployment_outcome_unknown`. See [recovery][reconcile-actor].

[reconcile-actor]: ../docs/user/clusters/index.md#inspect-and-recover-a-deployment
