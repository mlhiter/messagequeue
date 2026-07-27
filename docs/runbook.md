# Runbook

This runbook defines operational boundaries before deployment manifests exist.
Concrete release commands and image digests will be added with the first
deployable vertical slice.

## Deployment Order

1. Verify Kubernetes version, default StorageClass, quota, and observability
   capabilities.
2. Install compatible Strimzi CRDs and the cluster operator.
3. Install the MessageQueue CRD and controller.
4. Install the backend and management UI, then register the application entry.
5. Install shared scrape and alert definitions when the target monitoring CRDs
   are available.
6. Create a development `MessageQueue` and validate produce, consume, logs,
   metrics, suspension, recovery, and deletion policy.

## Health Checks

Use these resource-level checks after the corresponding manifests exist:

```bash
kubectl get messagequeues.messagequeue.sealos.io -A
kubectl -n <workspace> get kafka,kafkanodepool,kafkauser
kubectl -n <workspace> get pods,pvc,networkpolicy
kubectl -n messagequeue-system get deploy,pods
```

A healthy cluster has a current `observedGeneration`, a true `Ready` condition,
ready Strimzi resources, expected broker and controller Pods, bound PVCs, a
working authenticated client, and successful fixed-query metric responses.

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

- Disable new cluster creation before rolling back incompatible control-plane
  changes.
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
