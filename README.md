# MessageQueue

MessageQueue is a standalone control plane for running message brokers on
Kubernetes. The first supported engine is Apache Kafka, orchestrated by
Strimzi. RabbitMQ is a planned engine, not part of the first release.

> [!NOTE]
> This repository is in its foundation stage. It currently records product,
> architecture, security, and operational contracts before application code is
> scaffolded.

## Scope

MessageQueue will provide:

- A first-party management UI for provisioning and operating Kafka clusters.
- A backend API for authenticated Kubernetes, metrics, and log access.
- A Kubernetes controller that translates `MessageQueue` resources into
  Strimzi resources.
- Cluster lifecycle, credentials, logs, metrics, and safe failure handling.
- An optional user-facing Kafka console based on Kafbat UI.

MessageQueue does not use KubeBlocks and does not live in the Sealos monorepo.
Sealos integration is limited to public application, authentication,
Kubernetes, quota, and billing contracts.

## Architecture

```text
Management UI -> Backend API -> MessageQueue CR -> Controller -> Strimzi -> Kafka
                       |                                  |
                       +-> metrics and logs               +-> optional Kafbat UI
```

See [docs/architecture.md](docs/architecture.md) for resource ownership and
namespace boundaries.

## Repository Layout

```text
frontend/      First-party management interface
backend/       Authentication, Kubernetes, log, and metric APIs
controller/    MessageQueue reconciliation and Strimzi translation
deploy/        Helm charts and installation assets
docs/          Architecture, information architecture, references, and runbook
```

## Project Documentation

- [PRODUCT.md](PRODUCT.md): users, purpose, and product principles
- [DESIGN.md](DESIGN.md): seed design direction for the management UI
- [ROADMAP.md](ROADMAP.md): independently deliverable product stages
- [docs/architecture.md](docs/architecture.md): system and security architecture
- [docs/ia.md](docs/ia.md): management UI information architecture
- [docs/references.md](docs/references.md): upstream projects and dependency boundaries
- [docs/runbook.md](docs/runbook.md): deployment and incident boundaries
- [docs/decisions/0001-standalone-architecture.md](docs/decisions/0001-standalone-architecture.md):
  accepted repository and runtime ownership decision

## Development Status

The next implementation milestone is a vertical slice that creates a
`MessageQueue` resource, reconciles a single-node Kafka development cluster,
and exposes status through the management UI. Runtime dependencies and build
commands will be added with that slice rather than guessed during repository
initialization.

## Checks

Run the repository foundation checks before committing changes:

```bash
make check
```
