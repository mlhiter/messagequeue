# 0004: External Access Is a Separate Server-Owned Write Path, and Monitoring Uses Fixed Keys

- Status: Accepted
- Date: 2026-08-04

## Context

Users need a direct way to enable external access without exposing Strimzi
listener details, and the monitoring surface must stay useful when the metrics
backend is missing or partially unavailable.

## Decision

- Keep external access out of the create flow and expose it through
  `PUT /api/v1/messagequeues/{name}/external-access`.
- Accept only `{ "enabled": true|false }` from the browser.
- Keep listener type, node address selection, ports, and advertised addresses
  server-owned.
- Validate direct CR writes for external listener shape and bootstrap
  alternative names with CRD and controller-side bounds.
- Map UI metric cards to fixed server-owned keys and fixed backend query
  templates.
- Return degraded metric responses when the metrics provider is unavailable or
  returns incomplete data.

## Consequences

The browser stays simple, raw listener details never become user-controlled
input, and monitoring can degrade locally without blocking Kafka management.
Backend query logic can evolve without changing the browser contract.
Kubernetes credential access remains split: the backend only gets
per-instance resourceName-limited Secret access, while the controller has a
workspace-scoped Secret grant solely to satisfy RBAC escalation checks when it
creates those Roles.
