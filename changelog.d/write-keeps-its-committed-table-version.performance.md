- A write no longer re-reads the table version it just committed. After a
  successful publication the writer keeps the version it committed in the
  read-handle cache under its new pin, so the next write or read of that
  table on the same handle opens nothing, and an insert on one table makes
  two object-store requests fewer.
