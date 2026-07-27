# Agent Instructions

## Repository Purpose

This repository owns the standalone MessageQueue product. Read `PRODUCT.md`,
`DESIGN.md`, and `docs/architecture.md` before changing product or runtime
behavior.

## Hard Boundaries

- Do not add KubeBlocks APIs, packages, images, manifests, or compatibility
  code to the runtime path.
- Do not place MessageQueue application code in the Sealos monorepo.
- Integrate with Sealos only through published application, session,
  kubeconfig, Kubernetes, quota, billing, and observability contracts.
- The management UI is first-party code. Kafbat UI is an optional user-facing
  Kafka console and is not a management UI dependency.
- Strimzi owns Kafka workload generation. Do not hand-roll Kafka StatefulSets
  or implement another Kafka operator.
- User workloads belong in the user's workspace namespace. Shared scrape and
  alert definitions belong in the MessageQueue system namespace. Monitoring
  storage remains platform infrastructure.

## Security

- Derive workspace identity from an authenticated server-side session or
  kubeconfig. Never trust a namespace supplied only by a URL or browser body.
- Never return Kafka passwords, private keys, or kubeconfigs to browser logs,
  metrics labels, or status fields.
- Do not expose raw PromQL or LogsQL query execution to clients. Use fixed,
  server-owned query identifiers and inject namespace and cluster selectors.
- Disable dynamic configuration in the user-facing Kafka console. Generate its
  configuration from controller-owned Secrets.
- Do not perform database writes unless the user explicitly requests them.

## Delivery

- Production container images target `linux/amd64` unless ARM is explicitly
  requested.
- Pin operator, application, and console images by version and digest for
  releases. Never deploy `latest`.
- Keep frontend, backend, controller, and deployment changes in separate
  ownership boundaries with contract tests between them.
- Every reconciliation path requires unit tests and envtest coverage. Every
  user-facing workflow requires browser or API acceptance coverage.
- Missing metrics or historical logs must degrade the affected view without
  blocking Kafka provisioning.

## Documentation

- Update `PRODUCT.md` for product boundary changes.
- Update `DESIGN.md` after real UI tokens and components exist.
- Record architectural decisions in `docs/decisions/`.
- Update `docs/runbook.md` for deployment, rollback, or incident behavior.
