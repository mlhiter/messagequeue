# MessageQueue Backend

The backend is a small authenticated BFF for the standalone MessageQueue
control plane. It talks to the namespaced `MessageQueue` Kubernetes resource
and keeps the browser away from Kubernetes paths, Secrets, PromQL, and LogsQL.

The production binary uses the in-cluster ServiceAccount token and CA. The
workspace namespace is supplied by an authenticated server-side session
adapter (`ContextIdentityProvider`) or, for a single-workspace installation,
the server environment (`MESSAGEQUEUE_WORKSPACE_NAMESPACE`). A namespace in a
URL, query string, header, or JSON body is never used.

## Run

```bash
go test ./...
go vet ./...
go build -o messagequeue-backend .
```

Required production environment:

- `MESSAGEQUEUE_WORKSPACE_NAMESPACE`: server-owned workspace namespace for the
  single-workspace fallback. Multi-tenant deployments should inject an
  `Identity` into the request context instead.
- `KUBERNETES_SERVICE_HOST`: Kubernetes API host. The port defaults to `443`.
- `MESSAGEQUEUE_USER_ID`: server-side identity label (defaults to
  `messagequeue-backend` in the single-workspace fallback).
- `MESSAGEQUEUE_METRICS_URL`: optional VictoriaMetrics or VMAUTH base URL.
  When omitted, monitoring returns an explicit bounded degraded response. For
  a vmselect or vmauth deployment, point the URL at the `/select/0/prometheus/api/v1`
  prefix so the backend can append `query_range`.
- `MESSAGEQUEUE_EXTERNAL_LISTENER_TYPE`: server-owned external listener type;
  defaults to and currently accepts only `nodeport`.
- `MESSAGEQUEUE_EXTERNAL_NODE_ADDRESS_TYPE`: server-owned Strimzi NodePort
  address preference; defaults to `InternalIP` and accepts documented
  Kubernetes node address types.

The ServiceAccount token and CA are read from the standard in-cluster paths.
`MESSAGEQUEUE_SERVICE_ACCOUNT_TOKEN`, `MESSAGEQUEUE_SERVICE_ACCOUNT_CA`, and
`MESSAGEQUEUE_LISTEN_ADDR` can override those paths/address for a controlled
deployment. The process exits when Kubernetes credentials are absent; the
namespace fallback only applies when `MESSAGEQUEUE_WORKSPACE_NAMESPACE` is
configured.

## API contract

All API routes are under `/api/v1/messagequeues` and require server-side
identity. List, detail, status, and `client-config` responses never include
Secret `data`, passwords, private keys, or kubeconfigs. The observed status may
include a non-sensitive Secret name reference. Secret material is available
only through the explicit credentials route, which derives Secret names from
the authenticated resource and returns `Cache-Control: no-store`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process health; does not require workspace identity. Returns `{"service":"messagequeue","status":"ok"}` with `Cache-Control: no-store`. |
| `GET` | `/api/v1/messagequeues` | List resources in the authenticated namespace. |
| `POST` | `/api/v1/messagequeues` | Create a Kafka resource in the authenticated namespace. |
| `GET` | `/api/v1/messagequeues/{name}` | Return spec and observed status. |
| `DELETE` | `/api/v1/messagequeues/{name}` | Delete the authenticated namespace's `MessageQueue` resource. Kubernetes owner references and the selected deletion policy decide follow-on Strimzi/PVC cleanup. |
| `GET` | `/api/v1/messagequeues/{name}/status` | Return observed status only. |
| `PUT` | `/api/v1/messagequeues/{name}/external-access` | Idempotently enable or disable the server-configured external listener. The only accepted body is `{"enabled":true|false}`; namespace, listener type, ports, and advertised addresses are never browser-controlled. |
| `PUT` | `/api/v1/messagequeues/{name}/suspension` | Idempotently pause or resume reconciliation with exactly `{"suspended":true|false}`. It merge-patches only `spec.suspend`. |
| `GET` | `/api/v1/messagequeues/{name}/client-config` | Return secret-free client connection metadata: TLS-preferred internal `bootstrapServers`, observed `externalBootstrapServers`, username, auth mechanism, security protocol, transport, and Secret name references. External addresses are returned only after the controller observes them in status. |
| `GET` | `/api/v1/messagequeues/{name}/client-credentials` | Explicitly return authorized client credentials for copy/reveal workflows: username, password, CA certificate, Bootstrap servers, auth mechanism, security protocol, and Secret references. The route returns `Cache-Control: no-store` and never accepts Secret names from the browser. |
| `GET` | `/api/v1/messagequeues/-/quota` | Return workspace quota data for the authenticated namespace. If the workspace quota resource is missing, the response degrades without blocking instance creation. The backend ServiceAccount must be able to `get` `resourcequotas` in the workspace namespace for this route and create preflight to work. |
| `GET` | `/api/v1/messagequeues/{name}/logs` | Return bounded pod logs. |
| `GET` | `/api/v1/messagequeues/{name}/metrics` | Return one fixed monitoring series per request. |

Create accepts the following product-level shape. `engine` must be `kafka`;
Kafka version/replicas, deletion policy, resources, and storage are translated
to the controller's `MessageQueueSpec`. The request decoder rejects unknown
fields. The first-party UI may send the equivalent flat form with top-level
`engine`, `kafka.version`, `kafka.brokers`, `kafka.cpu`, `kafka.memory`,
`kafka.storageGi`, `kafka.storageClass`, `deletionPolicy`, `monitoring`, and
`console`; integration flags are intentionally not written into the Kubernetes
CR. Browser requests may not set `spec.storage.deleteClaim` directly; deletion
safety is derived from the product-level `deletionPolicy`.

The accepted Kafka versions match the controller contract: `3.9.0` and
`4.0.0`. An omitted version/replica/storage value is defaulted by the
controller. Create rejects operator-only fields such as `spec.topology`,
`spec.suspend`, and `spec.storage.deleteClaim`; topology and claim safety are
owned by the controller and product deletion policy. Broker requests are capped
per broker at 8 CPU, 64Gi memory, and 1024Gi storage before Kubernetes quota is
consulted. Create and delete are first-class API operations; they require a
server-side workspace identity and never accept namespace authority from the
browser.

External access is a separate operation from create. The backend merge-patches
only `spec.kafka.listeners.external`, preserving all unrelated desired state.
It writes the operator-configured `nodeport` listener type and preferred node
address type alongside the enabled intent; the controller reports usable
addresses through `status.externalEndpoints`.

```json
{
  "name": "orders",
  "spec": {
    "engine": "kafka",
    "kafka": { "version": "3.9.0", "replicas": 1 },
    "resources": { "cpu": "500m", "memory": "1Gi" },
    "storage": { "size": "10Gi", "className": "fast" },
    "deletionPolicy": "Retain"
  }
}
```

Log requests accept only `component=broker|controller|operator` and a bounded
`tailLines` value from 1 through 5000 (default 200). The Kubernetes log adapter
first proves the `MessageQueue` exists in the authenticated namespace; operator
logs degrade until a system log adapter exists. Metric requests accept exactly
one `key` from this server-owned set:

`broker_count`, `partition_health`, `throughput`, `consumer_lag`, `cpu`,
`memory`, `storage`. The management UI may combine multiple fixed-key requests
to render one Monitoring tab, but it must never send PromQL.

The API never accepts a raw log or metric query. When
`MESSAGEQUEUE_METRICS_URL` is configured, the backend performs fixed
30-minute VictoriaMetrics `query_range` requests at a 30-second step with
server-injected namespace and instance selectors. Missing, empty, invalid, or
unavailable metrics return a bounded degraded response, so an observability
outage does not block instance management.

## Structure

- `main.go`: HTTP routing, identity boundary, and in-cluster startup.
- `types.go`: request/response contract with secret-free views.
- `store_kubernetes.go`: namespaced Kubernetes and bounded pod-log adapter.
- `store_memory.go`: test/local store and explicit degraded metrics provider.
- `metrics.go`: fixed-key metric adapter boundary.
