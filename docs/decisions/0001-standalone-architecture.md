# 0001: Standalone Architecture

- Status: Accepted
- Date: 2026-07-27

## Context

Kafka support previously existed inside a database application and depended on
KubeBlocks. MessageQueue needs an independent lifecycle, repository, release,
user experience, and Kubernetes runtime while still integrating with Sealos
workspace and platform services.

## Decision

- Maintain all MessageQueue application code in this repository.
- Use Strimzi as the Kafka Kubernetes operator.
- Build the management frontend and backend as first-party components.
- Treat Kafbat UI as an optional user-facing Kafka console only.
- Keep user Kafka resources in workspace namespaces.
- Keep shared monitoring control resources in `messagequeue-system` and use the
  platform observability stack for storage and queries.
- Do not adopt or reconcile KubeBlocks-managed Kafka resources.

## Consequences

The product can release independently and does not inherit database-provider or
KubeBlocks resource semantics. It must own installation, version compatibility,
authentication, account suspension, observability contracts, and rollback.
Legacy Kafka instances require coexistence or an explicit data-migration project.
