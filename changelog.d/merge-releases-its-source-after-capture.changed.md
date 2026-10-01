- Writes and deletes on a merge's source branch no longer wait for the merge.
  A merge holds its source branch only while it captures the source, then
  publishes that captured state; changes made to the source afterwards reach
  the target with the next merge. Merges from one source into several
  targets now run at the same time. Merges into one target still run one
  after another.
