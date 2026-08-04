# OmniGraph Azure reference architecture image prompt

This is the reproducible source prompt for
`0029-azure-reference-architecture.png`. It follows the target-platform diagram
workflow from `msftse/azure-architecture-proposal` and is intentionally limited
to the first supported public-cloud topology in RFC 0029.

```text
Use case: infographic-diagram
Asset type: architecture figure for an open-source RFC and GitHub pull request
Primary request: Create a clean, technically precise Azure reference
architecture diagram for a mutable OmniGraph company-brain backend. Make the
single-writer admission boundary visually unmistakable.

Style/medium: Official Microsoft Azure reference-architecture style; light
neutral #F7F9FC background; crisp flat vector-like rendering; rounded white
service cards; thin 1-2 px connectors; no gradients, shadows, glass, 3D, or
decorative clutter. Use official Azure product icons for Azure Container Apps,
Azure Container Registry, Azure Blob Storage, Managed Identities, Microsoft
Entra ID, and Log Analytics. Use #0078D4 for Azure structure, OmniGraph ink
#1B1B1F for labels, and OmniGraph red #D71921 only for the writer-admission
safety boundary. Wide landscape composition, about 1800x1000, highly legible at
100% zoom.

Composition: Arrange left to right inside one subtle boundary labeled "Azure
resource group".

1. Far left: a small "Clients" card with browser and agent/API icons.
2. Center: one larger "Azure Container Apps" boundary. Inside it, place two
   separate cards:
   - "OmniGraph server" with sublabel "HTTPS ingress · bearer auth" and a small
     badge "1 replica target (sizing only)".
   - "Bootstrap job" with sublabel "import · apply · verify".
3. Between the two Container Apps cards and storage, place a prominent red
   outlined gate labeled "Writer admission lease" with a lock icon and the
   exact sublabel "At most one OmniGraph child". Both the server and bootstrap
   job must connect through this gate before reaching storage. Make it clear
   that an extra pre-warmed replica can exist but cannot pass the gate.
4. Right: an "Azure Blob Storage" card with sublabel "Anonymous access
   disabled".
   Inside it show two compartments:
   - "Cluster root (az://)" with small labels "ledger · manifests · Lance data".
   - "Infinite lease blob", visibly separate from the cluster root and stored
     in a reserved container-level admission namespace.
5. Above the Container Apps boundary: "Azure Container Registry" with sublabel
   "Immutable image digest".
6. Below: "User-assigned managed identity" and "Microsoft Entra ID".
7. Lower right: "Log Analytics" with sublabel "App + job logs".

Connections and exact arrow labels:
- Clients to OmniGraph server: solid arrow, "HTTPS requests".
- Azure Container Registry to both server and job: solid arrows, "Image pull".
- Server to Writer admission lease: solid arrow, "Acquire before open".
- Bootstrap job to Writer admission lease: solid arrow, "Acquire before write".
- Writer admission lease to Infinite lease blob: solid red arrow, "Exclusive lease".
- Server through the gate to Cluster root (az://): solid arrow, "Query + mutate".
- Bootstrap job through the gate to Cluster root (az://): solid arrow,
  "Import + apply".
- Microsoft Entra ID to user-assigned managed identity: dashed arrow,
  "Workload identity".
- Managed identity to Azure Blob Storage: dashed arrow,
  "Container-scoped RBAC".
- Managed identity to Azure Container Registry: dashed arrow, "AcrPull".
- Azure Container Apps boundary (representing both server and job) to Log
  Analytics: one solid arrow, "App + job logs".

Legend: solid line = request/data/runtime control flow; dashed line = identity
and RBAC. Add a small security strip with exactly: "Shared key off · Anonymous
Blob access off · ACR admin off".

Constraints: Keep total service-node count under 12. Preserve every quoted
label verbatim and spell "OmniGraph" exactly. Clearly show that replica count is
not the correctness mechanism; the infinite Blob lease gates child-process
admission. Do not depict the lease as protecting every graph blob. Do not show
multi-writer scale-out, Azure Files, Key Vault, AKS, VMs, private endpoints,
VNet integration, sovereign clouds, AWS services, or a second data store. No
marketing slogans, no fake resource names, no subscription IDs, no title
watermark, and no unlabeled arrows.
```
