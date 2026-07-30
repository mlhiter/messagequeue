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

The ServiceAccount token and CA are read from the standard in-cluster paths.
`MESSAGEQUEUE_SERVICE_ACCOUNT_TOKEN`, `MESSAGEQUEUE_SERVICE_ACCOUNT_CA`, and
`MESSAGEQUEUE_LISTEN_ADDR` can override those paths/address for a controlled
deployment. The process exits rather than starting unauthenticated when the
server-side namespace or Kubernetes credentials are absent.

## API contract

All API routes are under `/api/v1/messagequeues` and require server-side
identity. Secret `data`, passwords, private keys, and kubeconfigs are not
represented by the response types. The observed status may include a
non-sensitive Secret name reference so an authorized server operation can
resolve credentials without exposing their values to the browser.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process health; does not require workspace identity. Returns `{"service":"messagequeue","status":"ok"}` with `Cache-Control: no-store`. |
| `GET` | `/api/v1/messagequeues` | List resources in the authenticated namespace. |
| `POST` | `/api/v1/messagequeues` | Create a Kafka resource in the authenticated namespace when writes are enabled. Public desktop installs keep this disabled until the Sealos session/workspace adapter is connected. |
| `GET` | `/api/v1/messagequeues/{name}` | Return spec and observed status. |
| `DELETE` | `/api/v1/messagequeues/{name}` | Delete the authenticated namespace's `MessageQueue` resource when writes are enabled. Kubernetes owner references and the selected deletion policy decide follow-on Strimzi/PVC cleanup. |
| `GET` | `/api/v1/messagequeues/{name}/status` | Return observed status only. |
| `GET` | `/api/v1/messagequeues/{name}/client-config` | Return secret-free client connection metadata: bootstrap servers, username, auth mechanism, transport, and Secret name reference. |
| `GET` | `/api/v1/messagequeues/{name}/logs` | Return bounded pod logs. |
| `GET` | `/api/v1/messagequeues/{name}/metrics` | Return one fixed metric series. |

Create accepts the following product-level shape. `engine` must be `kafka`;
Kafka version/replicas, deletion policy, resources, and storage are translated
to the controller's `MessageQueueSpec`. The request decoder rejects unknown
fields. The first-party UI may send the equivalent flat form with top-level
`engine`, `kafka.version`, `kafka.brokers`, `kafka.storageGi`,
`kafka.storageClass`, `deletionPolicy`, `monitoring`, and `console`; integration
flags are intentionally not written into the Kubernetes CR. Browser requests may
not set `spec.storage.deleteClaim` directly; deletion safety is derived from
the product-level `deletionPolicy`.

The accepted Kafka versions match the controller contract: `3.9.0` and
`4.0.0`. An omitted version/replica/storage value is defaulted by the
controller. When `MESSAGEQUEUE_ALLOW_CREATE=false`, create and delete requests
return `403` and the public Desktop entry remains read-only.

```json
{
  "name": "orders",
  "spec": {
    "engine": "kafka",
    "kafka": { "version": "3.9.0", "replicas": 1 },
    "resources": { "cpu": "1", "memory": "2Gi" },
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
`memory`, `storage`.

The API never accepts a raw log or metric query. A missing VictoriaMetrics or
VictoriaLogs dependency is returned as a bounded degraded response, so an
observability outage does not block cluster management.

## Structure

- `main.go`: HTTP routing, identity boundary, and in-cluster startup.
- `types.go`: request/response contract with secret-free views.
- `store_kubernetes.go`: namespaced Kubernetes and bounded pod-log adapter.
- `store_memory.go`: test/local store and explicit degraded metrics provider.
- `metrics.go`: fixed-key metric adapter boundary.
