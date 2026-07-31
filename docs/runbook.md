# Runbook

This runbook covers the deployable Kafka vertical slice. The tested release
uses Strimzi `0.46.0`, Kafka `3.9.0`, and MessageQueue image tag `v0.1.9`.

## Deployment Order

1. Verify Kubernetes version, default StorageClass, quota, and observability
   capabilities. The backend Role in the workspace namespace must be able to
   `get` `resourcequotas`; otherwise the quota note and create preflight will
   fall back to a permission-denied state.
2. Install compatible Strimzi CRDs and the cluster operator.
3. Install the MessageQueue CRD and controller.
4. Install the backend and management UI, then register the application entry.
5. Install shared scrape and alert definitions when the target monitoring CRDs
   are available.
6. Create a development `MessageQueue` and validate produce, consume, live
   logs, safe client configuration metadata, delete behavior, and the explicit
   metrics-degraded state. Suspension, recovery, scaling, storage expansion,
   and upgrade workflows remain v0.2 follow-up checks.

## Health Checks

The stable HTTP health endpoint is `/healthz`. The backend and frontend return
`200` with `{"service":"messagequeue","status":"ok"}` and
`Cache-Control: no-store`; backend `/readyz` remains only as a compatibility
alias. The Helm chart uses `/healthz` for startup, liveness, and readiness
probes on the controller, backend, and frontend pods.

Use these resource-level checks after deployment:

```bash
kubectl get messagequeues.messagequeue.sealos.io -A
kubectl -n <workspace> get kafka,kafkanodepool,kafkauser
kubectl -n <workspace> get pods,pvc,networkpolicy
kubectl -n messagequeue-system get deploy,pods
```

For cluster 62, use only the explicitly approved kubeconfig:

```bash
KUBECONFIG_PATH=~/.kube/62 \
APPLY=1 ./deploy/cluster-62-smoke.sh
KUBECONFIG_PATH=~/.kube/62 \
WORKSPACE_NAMESPACE=ns-admin CLUSTER_NAME=kafka-dev \
./deploy/kafka-roundtrip-smoke.sh
```

The round-trip command mounts the generated `kafka-dev-client` password into a
temporary Job and prints only a success marker. It must not be changed to dump
Secret data. The Job requests `250m/512Mi` and limits `1 CPU/1Gi`; this is
required on the test cluster because the `ns-admin` LimitRange defaults Java
containers to `50m/64Mi`.

A healthy cluster has a current `observedGeneration`, a true `Ready` condition,
ready Strimzi resources, expected broker and controller Pods, bound PVCs, and
a working authenticated client. Metrics may still show an explicit degraded
state until the platform VictoriaMetrics adapter is connected; this does not
make Kafka management unhealthy.

For Sealos Desktop delivery, additionally verify `app-system/messagequeue`,
the `messagequeue.192.168.0.62.nip.io` Ingress, `/logo.svg`, and a real iframe
open from the Desktop. A public HTTP 200 alone does not prove the desktop entry
or embedded management workflow works.

The public Desktop entry exposes create and delete by default. On cluster 62,
the backend currently derives the writable workspace from the server-owned
`ns-admin` fallback. When replacing that fallback with the Sealos
session/workspace adapter, keep namespace authority server-derived and verify
create/delete against the authenticated workspace before rollout.

## Degraded Dependencies

| Failure | Expected behavior |
| --- | --- |
| Metrics backend unavailable | Kafka remains manageable; monitoring shows a scoped degraded state. |
| Historical logs unavailable | Live Kubernetes logs remain available. |
| Kafbat unavailable | Cluster lifecycle and native management remain available. |
| Strimzi operator unavailable | Reconciliation reports a dependency failure and avoids unsafe workload edits. |
| Quota exhausted | Creation or scaling reports the Kubernetes rejection without partial success claims. |

## Suspension and Account State

Suspension must scale broker and controller node pools down in a tested order,
retain PVCs, and preserve desired replicas for recovery. Namespace debt or
account-deletion state must stop recreation loops. Never delete Strimzi-owned
workloads directly as a suspension mechanism.

## Rollback

- Pause new user-initiated cluster creation while rolling back incompatible
  control-plane changes; do not reintroduce a create feature flag for this.
- Roll back frontend, backend, and MessageQueue controller images independently.
- Keep Strimzi and all CRDs installed while Kafka resources exist.
- Do not use Kafka binary downgrade as a routine rollback.
- Deleting or scaling down Kafbat does not affect Kafka data.
- Verify retained PVC policy before deleting a `MessageQueue` or namespace.

## Incident Evidence

Collect the `MessageQueue` status and generation, Strimzi conditions, related
Kubernetes events, controller logs, Pod readiness and restarts, PVC state, and
the exact fixed metric or log query identifier. Do not collect or paste Secret
payloads, kubeconfigs, or client passwords into tickets.
