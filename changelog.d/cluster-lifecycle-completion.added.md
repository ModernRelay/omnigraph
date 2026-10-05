- Cluster apply supports policy grants/revocations and rebinding, embedding-provider changes, and external-Blob rules on existing graphs without restarting the server. Explicit lifecycle input supports graph removal with retained storage, exact adoption, missing-root recreation and targeted catalog repair; plan performs the same effect-free checks as apply, and unrelated unavailable graphs do not block independent changes. See [cluster operations][cluster-lifecycle-completion].

[cluster-lifecycle-completion]: ../docs/user/clusters/index.md
