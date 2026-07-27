# Product

## Register

product

## Users

The primary users are Sealos workspace owners and managers who need to deploy
and operate Kafka without managing raw operator resources. Developers consume
connection information, logs, metrics, and a user-facing Kafka console within
the permissions of their workspace. Platform operators install and maintain
the shared controller, Strimzi, and observability integrations.

## Product Purpose

MessageQueue provides a focused Kubernetes message-broker control plane. It
turns a small, explicit product API into secure Kafka clusters, lifecycle
operations, credentials, logs, and metrics. Success means users can provision,
diagnose, and safely change Kafka without understanding Strimzi internals, while
platform operators retain predictable ownership, quota, billing, and rollback
behavior.

Kafka is the first engine. RabbitMQ can be added through its own operator after
the Kafka contract is stable. Existing KubeBlocks-managed clusters are outside
the runtime ownership boundary and are not adopted automatically.

## Brand Personality

Restrained, reliable, operational. The interface should feel calm during an
incident, precise during routine changes, and honest about asynchronous state.

## Anti-references

- A marketing landing page with oversized headlines, decorative gradients, or
  feature-card grids.
- A generic Kubernetes YAML editor that exposes operator details as the main
  workflow.
- A copy of the existing database provider information architecture.
- Kafbat UI presented as the MessageQueue management interface.
- A dark, blue-heavy operations dashboard that uses color as decoration rather
  than status.

## Design Principles

1. Show observed state, not optimistic intent. Every operation must expose its
   progress, last transition, and actionable failure reason.
2. Keep common workflows short. Creating a cluster, finding credentials, and
   opening logs or metrics should not require Kubernetes knowledge.
3. Make risk explicit. Destructive changes, version upgrades, storage policy,
   and public access require clear impact before confirmation.
4. Preserve tenant boundaries. Workspace identity and permission checks are
   server-owned and visible data never crosses namespaces.
5. Degrade locally. Missing logs, metrics, or the optional Kafka console must
   not make the cluster control plane unusable.

## Accessibility & Inclusion

Target WCAG 2.1 AA. All management workflows must support keyboard navigation,
visible focus, non-color status cues, screen-reader labels, reduced motion, and
text contrast appropriate for extended operational use.
