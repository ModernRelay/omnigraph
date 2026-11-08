- Cluster reports, stored-query inspection, policy tools and graph listing now return ordinary output errors instead of panicking on a broken pipe. Direct apply preserves the original deployment identity and retained lock when diagnostic or receipt delivery fails, allowing its one attempt to finish without replay; served apply refuses submission if its deployment ID cannot be delivered. See [deployment recovery][cli-cluster-output-recovery].

[cli-cluster-output-recovery]: ../docs/user/clusters/index.md#inspect-and-recover-a-deployment
