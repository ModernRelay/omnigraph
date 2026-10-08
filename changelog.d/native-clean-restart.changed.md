- Servers on local storage and S3 release their writer admission after a proved
  clean shutdown, so the next server can start normally. Uncertain storage work
  still blocks clean restart. Initial bootstrap receipts are one-use; later
  starts use ordinary `--cluster`. Older tools refuse the new released S3 lock
  record. See [shutdown and writer ownership][native-clean-restart].

[native-clean-restart]: ../docs/user/deployment.md#writer-topology
