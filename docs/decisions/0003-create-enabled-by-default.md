# 0003: Create And Lifecycle Writes Are Default Product Capabilities

- Status: Accepted
- Date: 2026-07-31

## Context

MessageQueue is a management product, not a read-only viewer. Cluster creation
and deletion are part of the core workflow on the public Sealos Desktop entry.
The previous release used deployment flags to disable writes while cluster 62
was running a fixed single-workspace fallback identity.

Keeping a feature flag for creation made the product look incomplete and let
deployment configuration override a capability that should be present whenever
the management API is healthy.

## Decision

- Remove the `MESSAGEQUEUE_ALLOW_CREATE` backend setting.
- Remove the `CREATE_ENABLED` frontend/container setting.
- Keep create and delete controls available whenever the management API is
  ready.
- Keep namespace authority server-owned. The browser must never supply the
  workspace namespace through URLs, headers, bodies, or status fields.
- Continue using Kubernetes authorization and the backend's workspace identity
  as the write boundary.

## Consequences

Cluster 62 and default chart installs expose create/delete without an extra
feature flag. If the API is unavailable, the UI can still show clearly labelled
demo data, but write controls remain unavailable because the API cannot accept
the request.

When the Sealos session/workspace adapter replaces the fixed `ns-admin`
fallback, it must preserve the same server-owned namespace boundary rather than
reintroducing a create feature flag.
