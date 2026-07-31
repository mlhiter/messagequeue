# Information Architecture

The management interface is an operational application, not a marketing site.
Its first screen is the user's cluster list and current system state.

## Primary Navigation

- Clusters
- Operations, future v0.2 lifecycle surface
- Platform status, future operator-only surface

## Cluster List

The entry route is the cluster list. It presents name, readiness, effective
version, broker count, storage, workspace, and last transition in a dense
table. The primary action is the header-level create action, available whenever
the management API is ready. Each row is a navigation link to the dedicated
detail page. Filters cover status, engine, and workspace when the user can
access more than one workspace.

## Create Cluster

The creation flow asks for topology, broker and controller resources, storage,
version, deletion policy, and optional monitoring or user-console settings. It
shows the resulting resource footprint and risky settings before submission.
Raw Strimzi YAML is not part of the primary flow.

## Cluster Detail

The detail page is a separate route, not a right-hand panel attached to the
list. Tabs are ordered by the current vertical-slice workflow:

1. Overview: desired and observed state, endpoints, topology, and recent events
2. Connections: secret-free client configuration metadata and copyable endpoints
3. Logs: pod, container, time range, live follow, previous container, and search
4. Metrics: resources, broker health, partition health, throughput, and lag
5. Settings: deletion policy and the delete action

The following detail tabs are v0.2 surfaces, not part of the current v0.1
closed loop:

- Operations: scaling, restart, upgrade, suspension, and operation history
- Advanced settings: advanced engine settings and future lifecycle controls

The optional Kafka console is opened as a separate user-facing workspace after
the v0.2 console integration exists. The management UI may show its readiness
and an explicit open action, but does not embed or imitate it.

## Required States

Every list and detail view must define loading, empty, provisioning, updating,
ready, degraded, suspended, deleting, failed, permission-denied, and partial
observability states. Desired state and observed state must be visually and
semantically distinct.

## Responsive Behavior

Desktop favors dense tables and dedicated detail pages. Narrow viewports
collapse secondary columns into row details and keep destructive actions in an
explicit menu. Logs and metric charts retain stable controls and never overlap
content.
