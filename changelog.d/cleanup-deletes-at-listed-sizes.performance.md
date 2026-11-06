- `cleanup` no longer sends a HEAD request before each file it deletes; it
  takes the file's size from the listing it already made.
