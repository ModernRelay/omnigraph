- A write lists `__manifest` branch refs once less per table it touches. A
  write with a captured transaction no longer checks the schema-apply
  sentinel as it opens each table; the capture and the check under the gates
  before any effect still run, so an insert on one table makes one
  object-store request fewer.
