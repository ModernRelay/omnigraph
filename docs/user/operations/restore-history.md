# Restore a graph with its history

A full-history restore registers one graph in a newly bootstrapped cluster.
The new cluster keeps its own identity, permissions for configuration changes,
and deployment ledger. The graph keeps its schema identity, branch heads and
original commit IDs. An ordinary graph creation still requires an empty path.

1. Select an immutable backup point. Verify it in a fresh process that has
   read access to the backup and no access to the original store. Run
   `omnigraph cluster verify-restore GRAPH --target BACKUP_ROOT --json` and
   save its result. Capture a complete, sorted inventory of graph objects
   with exact object versions, sizes and SHA-256 checksums.
2. Bootstrap the destination with zero graphs. Read its ledger ID and next
   deployment sequence using `omnigraph cluster status --target DEST --json`.
   Prepare a JSON file with `version: 1`, the destination `canonical_root`,
   `ledger_id`, `sequence`, `graph`, the inventory's
   `object_manifest_sha256` and the verified `history` result. Retain its
   exact bytes. The digest includes whitespace and the final newline.
3. As the destination storage administrator, run
   `omnigraph cluster prepare-restore --target DEST --descriptor receipt.json --json`.
   This reserves the graph path by competing with ordinary initialization for
   the same exclusive claim. Save the returned `restore_from` digest.
4. A trusted storage preparer copies only the inventoried graph objects
   into the reserved destination and compares every checksum. Keep the
   destination birth claim intact. Once staging writes are closed, run
   `verify-restore` against the destination with access restricted to that copy.
   Require the same history hash, schema identity, branch heads and commit
   count. Seal the exact descriptor as
   `__cluster/restores/<restore_from>.json` using create-if-absent.
   Keep uncertain storage effects and incomplete copies unsealed.
5. Declare the graph and its effective schema, queries and policies in
   the destination configuration. Add `restore_from: <digest>` to that
   graph's entry. Run the normal preview and authorized deployment.
6. Through the destination endpoint, read the branch heads, historical
   commits and stored queries and compare them with the backup. Retain the
   original copy until the destination is verified.

A foreign ledger, changed sequence, different schema identity, incomplete
copy, unsupported reference outside the restored graph, or missing seal
prevents registration. An accepted deployment with an uncertain response
is looked up by its original deployment ID.
