# Roadmap

The roadmap is organized as independently usable releases. Each release must be
deployable and reversible without requiring the next release.

## Foundation

- Establish repository contracts, architecture, and ownership boundaries.
- Define `messagequeue.sealos.io/v1alpha1` and its status conditions.
- Pin compatible Strimzi, Kafka, and Kubernetes versions.
- Select frontend and backend frameworks through the first vertical slice.

Exit criteria: a reviewed API contract and a test plan that another engineer
can implement without redefining ownership or tenancy.

## Release 0.1: Standalone Kafka Control Plane

Status: the first deployable slice is running on cluster 62. The remaining
items below are intentionally tracked rather than implied by the current
release.

- Install Strimzi and the MessageQueue controller independently of Sealos and
  KubeBlocks codebases.
- Create, list, inspect, and delete development and production Kafka topologies.
- Provide internal connection information and generated client credentials.
- Expose external listener enable/disable, observed status, and live logs in
  the first-party management UI; expose a fixed metrics contract with an
  explicit degraded state until the platform metrics adapter is connected.

Exit criteria: a user can create Kafka, produce and consume a test message, find
an actionable failure, and delete or retain storage according to policy.

## Release 0.2: Operations and User Console

- Add broker scaling, storage expansion, rolling restart, supported version
  upgrades, suspension, and recovery.
- Add historical logs, Kafka health metrics, consumer lag, and alert state.
- Deploy Kafbat UI as an optional, isolated user-facing console.
- Enforce workspace roles, network policies, credential rotation, quota, and
  account suspension behavior.

Exit criteria: lifecycle and user-console operations pass tenant-isolation,
failure, rollback, and browser acceptance tests.

## Later Engines and Connectivity

- Add additional Kafka listener shapes or address strategies after the current
  server-owned external-access path is validated against quota, TLS, and cost.
- Add RabbitMQ through its upstream Kubernetes operator and an engine-specific
  API block.
- Treat legacy KubeBlocks Kafka migration as a separate data-migration project,
  not controller adoption.
