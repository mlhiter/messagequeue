# MessageQueue

MessageQueue is a standalone control plane for running message brokers on
Kubernetes. The first supported engine is Apache Kafka, orchestrated by
Strimzi. RabbitMQ is a planned engine, not part of the first release.

> [!NOTE]
> The first Kafka vertical slice is implemented and has been smoke-tested on
> cluster 62. Metrics storage integration and the optional user console remain
> follow-up work.

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
- [docs/decisions/](docs/decisions/): accepted architecture and rollout decisions

## Development Status

Release `0.1.9` provides a deployable Kafka control plane: Strimzi `0.46.0`
reconciles Kafka `3.9.0`/`4.0.0` KRaft resources, the backend exposes namespaced
status/log/metrics contracts plus create/delete and secret-free
client-configuration contracts. The first-party UI uses a dedicated cluster
list page plus per-cluster detail pages for connections, logs, metrics, and
settings. The cluster-62 smoke path creates `ns-admin/kafka-dev`, verifies SCRAM
produce/consume, and registers an HTTPS `MessageQueue` iframe entry on Sealos
Desktop.

The management UI now uses a DevBox-style list-first shell: a compact top
header, dense table rows, and a dedicated per-cluster detail page. It follows
the Sealos Desktop language setting through the Desktop SDK protocol, with
Chinese as the standalone fallback. Kafka creation defaults to the development
profile from `deploy/examples/messagequeue-dev.yaml`: 1 broker, 500m CPU, 1Gi
memory, 10Gi storage, and Retain deletion policy; standard and custom profiles
make larger resource requests explicit before submission.

Known limits are deliberate: metrics currently return a bounded degraded state
until the platform VictoriaMetrics adapter is connected, historical logs are
not implemented, the current cluster-62 deployment uses a single server-owned
workspace identity for `ns-admin`, and Kafbat is not deployed by this chart.

## Checks

Run the repository foundation checks before committing changes:

```bash
make check
```
