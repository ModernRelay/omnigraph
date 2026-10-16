- `GET /graphs/{graph_id}/branches?detail=true` and `omnigraph branch list
  --detail` report each branch's head commit, the branch and commit it was
  created from, the actor that created it and when, and when it last moved,
  beside the names. A client no longer reads each branch's whole commit list
  for these: the listing reads each branch's manifest once, or twice once the
  branch has written a commit, whatever its history. Branch creation now
  records its actor on the branch; a branch created earlier lists
  `actor_id: null`. A branch deleted and created again reports its new
  creation and actor. See [Branches][branch-list-details].

[branch-list-details]: ../docs/user/branching/index.md#branch-workflow
