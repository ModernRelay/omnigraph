- A branch named `merge`, `diff` or `merge-base` can be deleted over HTTP.
  `DELETE /graphs/{graph_id}/branches/<name>` reached the static route of the
  same name and answered 405; each of those routes now deletes the branch it
  spells. See [branches][branch-delete-guide].

[branch-delete-guide]: ../docs/user/branching/index.md
