- Servers retain per-graph request ownership while pausing and resuming the same serving bindings, including disconnected operations and streamed responses. Authorized graph inventory reports `transitioning` and `wait_for_transition`; schema and stored-query deployments still require restart. See [deployment status][serving-transition-status].

[serving-transition-status]: ../docs/user/deployment.md
