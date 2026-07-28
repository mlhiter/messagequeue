# 0002: Public Desktop Entry Is Read-Only Until Workspace Identity Is Connected

- Status: Accepted
- Date: 2026-07-28

## Context

Cluster 62 exposes the MessageQueue management UI through a public HTTPS
Ingress and registers it as a Sealos Desktop iframe application. The backend
still uses a fixed single-workspace fallback namespace while the real Sealos
session and workspace adapter is not connected end-to-end.

Exposing create or lifecycle writes through that fallback would let a browser
request mutate Kafka resources without proving the caller's workspace identity.

## Decision

- Register the public Desktop app and allow read-only management views.
- Keep `MESSAGEQUEUE_ALLOW_CREATE=false` in the backend by default.
- Keep `CREATE_ENABLED=false` in the frontend by default and hide create entry
  points in the public Desktop UI.
- Treat the frontend flag as a usability hint only; the backend remains the
  authority for write permission.
- Enable create and lifecycle writes only after the backend derives workspace
  identity from an authenticated Sealos session or equivalent server-owned
  kubeconfig context.

## Consequences

Users can open the MessageQueue app from Desktop, inspect the current Kafka
state, and use read-only operational surfaces while identity integration is
unfinished. Provisioning remains blocked on the public route, preventing the
single-workspace fallback from becoming an authorization bypass.

The next implementation step is to replace the fallback identity with the real
session/workspace adapter, add permission checks for writes, and then enable
the create flow through release values.
