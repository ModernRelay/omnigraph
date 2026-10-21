- **`omnigraph access` says who you are at a target and what you may do.** It
  reports the actor and the actions the graph's policy grants, graph-wide, on
  `--branch` and from it to `--target-branch`. A server target answers through
  its access route; for a direct graph or root target the CLI decides with the
  cluster's applied policy for the declared actor, and a standalone graph
  allows every action once it opens. A store that holds no graph refuses with
  `not_found`; catalogue and managed targets refuse. See
  [what you may do at a target][cli-access-command].

[cli-access-command]: ../docs/user/cli/targets.md#what-you-may-do-at-a-target
