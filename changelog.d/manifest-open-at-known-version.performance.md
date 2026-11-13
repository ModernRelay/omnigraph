- A read at a graph commit the engine already knows (a read at a past commit,
  a diff or change page from a commit, the change feed's check of a named
  branch, and `cleanup`'s retained versions) opens `__manifest` at that
  version instead of at its latest first. On S3-compatible storage this saves
  one listing of `_versions/` and one GET per read: on main by opening at the
  version, on a named branch by checking the branch out at the version.
  `cleanup` reads each retained version through the branch it already holds
  instead of reopening `__manifest`. Results and errors are unchanged.
