# Deployment

This directory contains the independent installation surface for MessageQueue.
It deliberately does not live in, or require changes to, the Sealos monorepo.

## Layout

- `charts/messagequeue`: the control-plane Helm chart. Install it into the
  `messagequeue-system` namespace. The chart owns the MessageQueue CRD and the
  controller, backend, frontend, RBAC, Services, and optional shared scrape
  resources.
- `strimzi`: a pinned upstream Strimzi prerequisite. Strimzi's operator runs in
  the system namespace, while its Kafka, KafkaNodePool, and KafkaUser resources
  are created in workspace namespaces by the MessageQueue controller.
- `examples/messagequeue-dev.yaml`: a development resource to apply in a
  workspace namespace after the control plane and Strimzi are ready.
- `cluster-62-smoke.sh`: a non-destructive cluster-62 preflight/render script.
  Set `APPLY=1` to install or upgrade the control plane and create the smoke
  workspace; the script never deletes an existing resource. It also registers
  the `app-system/messagequeue` desktop entry and configures the public HTTPS
  host `messagequeue.192.168.0.62.nip.io`.
- `kafka-roundtrip-smoke.sh`: creates a short-lived, resource-bounded Kafka
  client Job, mounts only the generated client password, and verifies a real
  SCRAM produce/consume round-trip without printing Secret data.
- `docker-bake.hcl`: the release build definition. Every target is explicitly
  `linux/amd64` and uses versioned tags; a release pipeline must replace the
  empty digest values with the digests produced by the registry.

## Namespace contract

The Helm release namespace is the MessageQueue system namespace (default:
`messagequeue-system`). The chart never creates Kafka workloads in that
namespace. A workspace namespace owns `MessageQueue`, Strimzi resources,
Kafka Pods, Services, PVCs, Secrets, NetworkPolicies, and optional Kafbat UI
resources. Shared `ServiceMonitor`/`VMServiceScrape` definitions, when enabled,
are owned by the system release; VictoriaMetrics/VictoriaLogs storage remains
platform infrastructure.

## Install order

```bash
workspace_namespace="ns-admin"
kubectl create namespace messagequeue-system --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "$workspace_namespace" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k deploy/strimzi
helm upgrade --install messagequeue deploy/charts/messagequeue \
  --namespace messagequeue-system --create-namespace \
  --set backend.workspaceNamespace="$workspace_namespace" --wait
kubectl apply -n "$workspace_namespace" -f deploy/examples/messagequeue-dev.yaml
```

Strimzi and MessageQueue versions are pinned. Do not use an upstream `latest`
URL or image tag. For production, set
`global.imagePolicy.requireDigest=true` and provide a `sha256:` digest for each
application image in a release values file.

The public release defaults are `ghcr.io/mlhiter/messagequeue-*`. Override all
repositories and the version without editing the chart:

```bash
helm upgrade --install messagequeue deploy/charts/messagequeue \
  --namespace messagequeue-system \
  --set images.controller.repository=<registry>/messagequeue-controller \
  --set images.backend.repository=<registry>/messagequeue-backend \
  --set images.frontend.repository=<registry>/messagequeue-frontend \
  --set images.controller.tag=v0.1.9 \
  --set images.backend.tag=v0.1.9 \
  --set images.frontend.tag=v0.1.9
```

`docker buildx bake -f deploy/docker-bake.hcl --push` defaults test builds to
the configured Aliyun registry and publishes only `linux/amd64`. Set
`REGISTRY=ghcr.io/mlhiter` for a public release build.

## Cluster 62 smoke test

```bash
APPLY=1 ./deploy/cluster-62-smoke.sh
```

Without `APPLY=1`, the script only checks the cluster, renders the chart, and
prints the exact resources that would be submitted. It uses the safe local
kubeconfig `~/.kube/62` by default; override with `KUBECONFIG_PATH` when a
different, explicitly approved kubeconfig is needed. The smoke workspace
defaults to the existing `ns-admin`; override with `WORKSPACE_NAMESPACE`.

After the control plane is ready, run the client round-trip independently:

```bash
KUBECONFIG_PATH=/Users/mlhiter/.kube/62 \
WORKSPACE_NAMESPACE=ns-admin \
CLUSTER_NAME=kafka-dev \
./deploy/kafka-roundtrip-smoke.sh
```

The test Job requests `250m/512Mi` and limits `1 CPU/1Gi` because the test
cluster's `ns-admin` LimitRange defaults containers to `50m/64Mi`, which is too
small for two Kafka Java CLI processes. The Job is TTL-cleaned; its test topic
is intentionally retained for review.

The script enables the chart's optional Sealos `App` registration. The App is
stored in `app-system`, points to the same HTTPS Ingress as the management UI,
and uses `/logo.svg` from the frontend image. If `wildcard-cert` is absent from
`messagequeue-system`, the script copies the platform certificate from
`TLS_SOURCE_NAMESPACE` (default `dbprovider-frontend`) without printing its
data.

The public Desktop entry exposes create and delete by default. On cluster 62,
the backend uses the fixed server-owned workspace fallback for `ns-admin`; do
not accept namespace values from the browser when replacing this with the
Sealos session/workspace adapter.
