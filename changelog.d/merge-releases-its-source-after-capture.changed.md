- Writes and deletes on a merge's source branch no longer wait for the whole
  merge. A merge holds its source branch only while it captures the source,
  then publishes that captured state; changes made to the source afterwards
  reach the target with the next merge. A merge still waiting for its target,
  because another merge into that target is running, can hold its source
  until it starts. Merges from one source into several targets now run at
  the same time. Merges into one target still run one after another.
