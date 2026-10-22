# Data protection

OmniGraph does not encrypt what it writes. It has no client-side or field-level
encryption: graph data, history, managed Blob bytes, schemas, stored queries and
policy bundles are stored in the cluster root as written. Protecting them at
rest and in transit is the job of the storage service and the network in front
of the server.

## Encryption at rest

Use the storage service's encryption:

- **Amazon S3.** Default encryption on the bucket (SSE-S3 or SSE-KMS) applies
  to every object OmniGraph writes, with no OmniGraph setting. With a
  customer-managed KMS key, the credentials OmniGraph uses also need permission
  to use that key. Per-request settings such as `AWS_SERVER_SIDE_ENCRYPTION`
  are not needed, and CLI [targets](../cli/targets.md) refuse them.
- **Azure Blob Storage.** Azure Storage encrypts Blob data at rest by default.
- **Local filesystem.** Put the root on an encrypted volume.

## Encryption in transit

`omnigraph-server` serves plain HTTP. Terminate TLS in front of it, as
described under [Container](../deployment.md#container). Storage traffic uses
HTTPS unless you configure an `http://` endpoint for local development.

## Data sent to external services

The only external service OmniGraph sends text to is the embedding provider. A
text `nearest` query sends its query text, and the offline `omnigraph embed`
command sends the values it embeds. Stored graph data is never sent. See
[network egress](../search/embeddings.md#network-egress) for when a provider is
called and how a graph selects one.

## Erasure

Deleting data or dropping a property changes the current graph only. Earlier
commits keep the old values readable until [cleanup](maintenance.md#cleanup)
removes them, and the bytes can remain in storage files after that. To erase a
dropped property's values, follow the procedure in
[schema changes](../schema/index.md#schema-changes).

This is not secure erasure. Object-store versioning, retention policies and
backups can keep copies that OmniGraph does not manage.
