# Strimzi prerequisite

MessageQueue delegates Kafka workload reconciliation to Strimzi. This
directory pins Strimzi `0.46.0`; it is a prerequisite and is not part of the
MessageQueue Helm release. This operator bundle maps Kafka `3.9.0` and `4.0.0`;
the development example defaults to `3.9.0`. Keep the operator installed while any
`Kafka`, `KafkaNodePool`, or `KafkaUser` resources exist.

The Kustomization applies the upstream release into the MessageQueue system
namespace (`messagequeue-system`) and leaves generated Kafka workloads in the
workspace namespace selected by each resource. Review the rendered upstream
manifest in a change review before applying it to a production cluster.

```bash
kubectl apply -k deploy/strimzi
kubectl -n messagequeue-system rollout status deployment/strimzi-cluster-operator --timeout=10m
kubectl -n messagequeue-system get pods -l name=strimzi-cluster-operator
```

The Kustomize overlay changes the upstream example's namespace-derived watch
scope to all namespaces, fixes its ServiceAccount subjects to
`messagequeue-system`, and installs cluster-wide bindings for the upstream
`namespaced`, `watched`, and `entity-operator` ClusterRoles. The last binding
is required by Kubernetes RBAC escalation checks when Strimzi creates a
KafkaTopic/KafkaUser Role in a workspace namespace. Do not copy
Strimzi-generated Kafka StatefulSets into this repository. Strimzi owns those
resources and their rolling-update behavior.

To upgrade an existing installation without deleting or restarting resources:

```bash
kubectl --kubeconfig ~/.kube/62 apply -k deploy/strimzi
```
