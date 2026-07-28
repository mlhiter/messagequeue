#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kubeconfig_path="${KUBECONFIG_PATH:-$HOME/.kube/62}"
system_namespace="${SYSTEM_NAMESPACE:-messagequeue-system}"
workspace_namespace="${WORKSPACE_NAMESPACE:-ns-admin}"
release_name="${RELEASE_NAME:-messagequeue}"
apply="${APPLY:-0}"
chart_dir="$repo_root/deploy/charts/messagequeue"
image_registry="${IMAGE_REGISTRY:-crpi-7jr40k6elhldekqp.cn-hangzhou.personal.cr.aliyuncs.com/mlhiter}"
image_tag="${IMAGE_TAG:-v0.1.4}"

if [[ ! -r "$kubeconfig_path" ]]; then
  echo "kubeconfig is not readable: $kubeconfig_path" >&2
  exit 1
fi

kubectl_cmd=(kubectl --kubeconfig "$kubeconfig_path")
helm_cmd=(helm --kubeconfig "$kubeconfig_path")

echo "== cluster 62 preflight =="
"${kubectl_cmd[@]}" version --output=yaml | sed -n '1,32p'
"${kubectl_cmd[@]}" get namespace "$system_namespace" "$workspace_namespace" --ignore-not-found
"${kubectl_cmd[@]}" get storageclass

echo "== render chart (no cluster mutation) =="
"${helm_cmd[@]}" template "$release_name" "$chart_dir" \
  --namespace "$system_namespace" \
  --include-crds \
  --set global.systemNamespace="$system_namespace" \
  --set backend.workspaceNamespace="$workspace_namespace" \
  --set images.controller.repository="$image_registry/messagequeue-controller" \
  --set images.backend.repository="$image_registry/messagequeue-backend" \
  --set images.frontend.repository="$image_registry/messagequeue-frontend" \
  --set images.controller.tag="$image_tag" \
  --set images.backend.tag="$image_tag" \
  --set images.frontend.tag="$image_tag" \
  --set global.imagePolicy.requireDigest=false \
  > /tmp/messagequeue-cluster-62-rendered.yaml
echo "rendered manifest: /tmp/messagequeue-cluster-62-rendered.yaml"
"${kubectl_cmd[@]}" apply --dry-run=client \
  -f /tmp/messagequeue-cluster-62-rendered.yaml >/dev/null

if [[ "$apply" != "1" ]]; then
  cat <<EOF

Dry run complete. No cluster resources were changed.
Set APPLY=1 to apply the pinned Strimzi prerequisite, install/upgrade the
control plane, create the smoke workspace if absent, and apply the development
MessageQueue resource. This script never deletes resources.
EOF
  exit 0
fi

echo "== apply pinned Strimzi prerequisite =="
"${kubectl_cmd[@]}" create namespace "$system_namespace" --dry-run=client -o yaml | "${kubectl_cmd[@]}" apply -f -
"${kubectl_cmd[@]}" create namespace "$workspace_namespace" --dry-run=client -o yaml | "${kubectl_cmd[@]}" apply -f -
"${kubectl_cmd[@]}" apply -k "$repo_root/deploy/strimzi"
"${kubectl_cmd[@]}" -n "$system_namespace" rollout status deployment/strimzi-cluster-operator --timeout=10m

echo "== install MessageQueue CRD =="
"${kubectl_cmd[@]}" apply -f "$chart_dir/crds/messagequeue.sealos.io_messagequeues.yaml"

echo "== install control plane =="
"${helm_cmd[@]}" upgrade --install "$release_name" "$chart_dir" \
  --namespace "$system_namespace" --create-namespace \
  --set global.systemNamespace="$system_namespace" \
  --set backend.workspaceNamespace="$workspace_namespace" \
  --set backend.userID=cluster-62-smoke \
  --set images.controller.repository="$image_registry/messagequeue-controller" \
  --set images.backend.repository="$image_registry/messagequeue-backend" \
  --set images.frontend.repository="$image_registry/messagequeue-frontend" \
  --set images.controller.tag="$image_tag" \
  --set images.backend.tag="$image_tag" \
  --set images.frontend.tag="$image_tag" \
  --wait --timeout 10m

echo "== apply workspace example =="
"${kubectl_cmd[@]}" apply -n "$workspace_namespace" -f "$repo_root/deploy/examples/messagequeue-dev.yaml"

echo "== rollout and resource checks =="
"${kubectl_cmd[@]}" -n "$system_namespace" rollout status deployment/"${release_name}-messagequeue-controller" --timeout=10m
"${kubectl_cmd[@]}" -n "$system_namespace" rollout status deployment/"${release_name}-messagequeue-backend" --timeout=10m
"${kubectl_cmd[@]}" -n "$system_namespace" rollout status deployment/"${release_name}-messagequeue-frontend" --timeout=10m
"${kubectl_cmd[@]}" -n "$workspace_namespace" get messagequeues,kafka,kafkanodepools,kafkausers,pods,pvc

cat <<EOF

Cluster-62 smoke apply completed. To roll back only the control-plane release:
  helm --kubeconfig "$kubeconfig_path" -n "$system_namespace" rollback "$release_name"
No delete operation is performed by this script; review retained PVC and
MessageQueue deletion-policy behavior before any manual cleanup.
EOF
