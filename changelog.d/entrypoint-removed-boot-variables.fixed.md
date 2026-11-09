- The container entrypoint refuses `OMNIGRAPH_TARGET_URI`, `OMNIGRAPH_CONFIG` and `OMNIGRAPH_TARGET` by name, pointing to `OMNIGRAPH_CLUSTER`, instead of starting a server that rejects the arguments they produced. See [container][entrypoint-cluster].

[entrypoint-cluster]: ../docs/user/deployment.md#container
