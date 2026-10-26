- `omnigraph optimize` supports registered server targets with an explicitly
  selected graph. `--no-wait` returns after acceptance, `--operation-id` observes
  an existing operation without resubmission, and `--timeout` bounds observation
  without cancelling server work. The CLI retains the original operation ID
  through uncertain delivery; direct-storage optimize keeps its existing output.
  Administrators can reconcile an interrupted original ID with
  `--writers-stopped` on an explicit direct-root target after writer and
  accepted-I/O exclusion. This never runs another optimization or unlocks a
  held owner.
