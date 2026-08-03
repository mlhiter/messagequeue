# Information Architecture

The management interface is an operational application, not a marketing site.
Its first screen is the user's instance list and current system state.

## Primary Navigation

- Instances
- Operations, future v0.2 lifecycle surface
- Platform status, future operator-only surface

## Instance List

The entry route is the instance list. It presents name, readiness, effective
version, broker count, storage, workspace, and last transition in a dense
table. The primary action is the header-level create action, available whenever
the management API is ready. Each row is a navigation link to the dedicated
detail page. Filters cover status, engine, and workspace when the user can
access more than one workspace.

## Create Instance

The creation flow starts from a resource profile, then shows the resulting
footprint before submission. Development is the default profile and matches the
checked-in development example: 1 broker, 500m CPU, 1Gi memory, 10Gi storage per
broker, and Retain deletion policy. Standard requests 3 brokers, 1 CPU, 2Gi
memory, 20Gi storage per broker, and Retain deletion policy. Custom unlocks the
broker, CPU, memory, storage, storage class, and deletion-policy fields while
the API caps each broker at 8 CPU, 64Gi memory, and 1024Gi storage. Raw Strimzi
YAML is not part of the primary flow.

## Instance Detail

The detail page is a separate route, not a right-hand panel attached to the
list. Tabs are ordered by the current vertical-slice workflow:

1. Overview: health summary, observed readiness, broker readiness, connection
   availability, resource footprint, storage/deletion policy, failure reason,
   conditions, and recent events
2. Connections: secret-free internal and external host, port, connection string,
   authentication metadata, and copyable fields
3. Logs: automatically loaded broker logs, refresh/retry, and scoped degraded
   states when logs are unavailable
4. Monitoring: automatically loaded CPU, memory, storage, throughput, consumer
   lag, partition health, and scoped degraded states when metrics are
   unavailable

Delete, update, pause, and resume controls belong in the route header beside
the back control and in the list-row more menu, not inside the detail content
card. Deletion is available whenever the management API is ready and still
requires explicit confirmation. Update, pause, and resume remain disabled or
explanatory until backend lifecycle contracts are implemented.

The following detail tabs are v0.2 surfaces, not part of the current v0.1
closed loop:

- Operations: scaling, restart, upgrade, suspension, and operation history
- Advanced configuration: advanced engine settings and future lifecycle controls

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
