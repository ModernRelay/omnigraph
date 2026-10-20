- A branch named `merge`, `diff` or `merge-base` can be deleted over HTTP.
  `DELETE /graphs/{graph_id}/branches/<name>` reached the branch action of the
  same name and answered 405; one route now serves both, deleting the branch
  on DELETE and running the action on its own method. See
  [branches][branch-delete-guide].

[branch-delete-guide]: ../docs/user/branching/index.md
