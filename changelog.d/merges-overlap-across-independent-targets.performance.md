- Merges into independent target branches on one handle no longer queue
  behind each other. A merge that misses the handle's single-entry authority
  cache now reopens that branch's coordinator without holding the cache's
  lock, so concurrent merges into different targets overlap; merges into the
  same target still serialize.
