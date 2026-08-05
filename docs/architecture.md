# Architecture

## System Boundary

MessageQueue is an independent deployable system. Its repository, releases,
images, CRDs, controller, backend, and frontend are owned here. It does not
import KubeBlocks APIs or rely on code being added to the Sealos monorepo.

```text
Browser
  |
  v
Management UI -> Backend API -> Kubernetes API
                       |              |
                       |              v
                       |       MessageQueue CR
                       |              |
                       |              v
                       |       MessageQueue Controller
                       |              |
                       |              v
                       |       Strimzi CRs -> Kafka
                       |
                       +-> VictoriaMetrics / VictoriaLogs

Optional user path: authenticated entry -> Kafbat UI -> Kafka
```

## Components

### Management UI

The first-party interface owns instance creation, lifecycle operations,
credentials, status, logs, metrics, and operation history. It never delegates
management workflows to Kafbat UI.

### Backend API

The backend authenticates the Sealos session, derives the workspace namespace,
performs permission checks, talks to Kubernetes, and exposes fixed metric and
log query contracts. It must not accept arbitrary namespaces, PromQL, or LogsQL
from browsers. External listener enablement is a separate write path:
`PUT /api/v1/messagequeues/{name}/external-access` accepts only an enabled or
disabled intent, while listener type, node address selection, and advertised
addresses stay server-owned.

### MessageQueue Controller

The controller reconciles the namespaced `MessageQueue` product resource into
Strimzi `Kafka`, `KafkaNodePool`, and `KafkaUser` resources plus supporting
metrics ConfigMaps, Secrets, and NetworkPolicies. Strimzi, not this controller,
owns Kafka Pods, Services, StatefulSets, and rolling-update mechanics, while
the controller owns the product-level listener intent and observed external
bootstrap metadata.

### User-Facing Kafka Console

Kafbat UI is optional and provides topic, partition, message, consumer-group,
offset, and Kafka ACL operations. It is isolated from the management UI and
receives controller-generated, read-only configuration. Dynamic configuration
is disabled.

## Resource Placement

### Workspace Namespace

- `MessageQueue`, `Kafka`, `KafkaNodePool`, and `KafkaUser` resources
- Kafka workloads, Services, PVCs, Secrets, NetworkPolicies, and metrics
  ConfigMaps
- Optional Kafbat Deployment, Service, and configuration Secret
- Metrics endpoints and Strimzi Kafka Exporter workloads

### MessageQueue System Namespace

- Management frontend and backend
- MessageQueue controller
- Shared `VMPodScrape` or equivalent scrape definitions that select managed
  Kafka workloads across namespaces and relabel `namespace` and
  `strimzi_io_cluster`
- Shared `VMRule` definitions grouped by workspace and cluster labels
- Optional `app.sealos.io/v1 App` registration for the Desktop launcher that
  points at the public HTTPS Ingress

### Platform Observability Namespace

- VictoriaMetrics storage, query, and collection services
- VictoriaLogs, log collectors, VMAlert, and notification infrastructure

No per-instance monitoring control resource is required in the workspace
namespace. Instance deletion naturally removes metric endpoints and log sources.

## API Ownership

The product API will use `messagequeue.sealos.io/v1alpha1`. `MessageQueue`
remains namespaced and records desired engine, topology, resources, storage,
authentication, listener, monitoring, console, suspension, and deletion-policy
settings. Listener intent is server-owned and can be enabled through the
`external-access` endpoint. Status records observed generation, effective
version, endpoints, external endpoints, non-secret references, topology, and
Kubernetes-style conditions.

Kafka is the only accepted engine in v1alpha1. Engine-specific fields are kept
under a Kafka block so RabbitMQ can be added later without forcing common
semantics where the brokers differ.

## Security Model

- Workspace identity is derived server-side from authenticated context.
- Kubernetes authorization remains the final permission check.
- Secrets are referenced by name in status and fetched only for authorized
  server-side operations. Ordinary `client-config` responses remain secret-free;
  the explicit credentials route derives the client and CA Secret names from the
  authenticated MessageQueue resource, returns `Cache-Control: no-store`, and
  is used only for reveal/copy workflows. Backend Secret reads come from
  per-instance resourceName-limited Roles; the controller's Secret grant is
  workspace-scoped only to satisfy Kubernetes RBAC escalation while creating
  those Roles, not cluster-wide.
- Public desktop installs expose create and lifecycle writes by default. The
  backend's server-owned workspace identity decides the target namespace; the
  browser never supplies namespace authority.
- Broker authentication uses TLS, SCRAM-SHA-512, and Kafka ACLs by default.
- Kafbat has no public Service or Kubernetes API token. Network policy permits
  only approved ingress and Kafka-related egress.
- Metrics and log queries are selected by server-owned identifiers and are
  always constrained by workspace namespace and cluster labels.
- Monitoring degrades locally when the metrics provider is absent or returns
  incomplete data, and that fallback must not block Kafka reconciliation.
- Suspension scales down only owner-referenced KafkaNodePools, keeps credential
  RBAC reconciled, and removes ready replicas and connection endpoints from
  status until resume completes.

## Failure and Rollback

Observability and Kafbat failures degrade their own features but do not block
Kafka reconciliation. Rolling back the UI, backend, or MessageQueue controller
must not delete Strimzi resources. Strimzi and its CRDs must remain installed
while managed Kafka resources exist. Kafka version downgrades are not treated as
a general rollback mechanism.
