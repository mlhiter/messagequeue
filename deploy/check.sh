#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
chart_dir="$repo_root/deploy/charts/messagequeue"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

for command_name in helm kubectl docker rg; do
  if ! command -v "$command_name" >/dev/null; then
    echo "required command is not available: $command_name" >&2
    exit 1
  fi
done

helm lint "$chart_dir"
helm template messagequeue "$chart_dir" \
  --namespace messagequeue-system \
  --set backend.workspaceNamespace=ns-admin \
  --include-crds > "$temporary_dir/messagequeue.yaml"
kubectl apply --dry-run=client -f "$temporary_dir/messagequeue.yaml" >/dev/null

helm template messagequeue "$chart_dir" \
  --namespace messagequeue-system \
  --set backend.workspaceNamespace=ns-admin \
  --set observability.vmPodScrape.enabled=true \
  --api-versions operator.victoriametrics.com/v1beta1 \
  --include-crds > "$temporary_dir/messagequeue-observability.yaml"

extract_rendered_resource() {
  local kind="$1"
  local name="$2"
  local manifest_path="$3"

  awk -v target_kind="$kind" -v target_name="$name" '
    function flush() {
      if (doc ~ "\nkind: " target_kind "\n" && doc ~ "\n  name: " target_name "\n") {
        printf "%s", doc
        found = 1
        exit
      }
      doc = ""
    }
    /^---$/ {
      flush()
      next
    }
    {
      doc = doc $0 "\n"
    }
    END {
      if (!found) {
        flush()
      }
    }
  ' "$manifest_path"
}

backend_role_manifest="$(extract_rendered_resource Role messagequeue-messagequeue-backend "$temporary_dir/messagequeue.yaml")"
controller_role_manifest="$(extract_rendered_resource ClusterRole messagequeue-messagequeue-controller "$temporary_dir/messagequeue.yaml")"
controller_workspace_role_manifest="$(extract_rendered_resource Role messagequeue-messagequeue-controller-credential-grant "$temporary_dir/messagequeue.yaml")"
if [[ -z "$backend_role_manifest" ]]; then
  echo "backend Role is missing from rendered chart" >&2
  exit 1
fi
if [[ -z "$controller_role_manifest" ]]; then
  echo "controller ClusterRole is missing from rendered chart" >&2
  exit 1
fi
if [[ -z "$controller_workspace_role_manifest" ]]; then
  echo "controller workspace credential grant Role is missing from rendered chart" >&2
  exit 1
fi

if [[ "$backend_role_manifest" != *'resources: ["messagequeues"]'* || "$backend_role_manifest" != *'verbs: ["get", "list", "watch", "create", "patch", "delete"]'* ]]; then
  echo "backend Role cannot patch MessageQueue resources for external access" >&2
  exit 1
fi
if [[ "$backend_role_manifest" == *'resources: ["secrets"]'* ]]; then
  echo "backend Role must not get every Secret in the workspace namespace" >&2
  exit 1
fi
if ! rg -U -q 'MESSAGEQUEUE_BACKEND_SERVICE_ACCOUNT_NAME(.|\n)*messagequeue-messagequeue-backend' "$temporary_dir/messagequeue.yaml"; then
  echo "controller must know the backend ServiceAccount for per-instance credential RBAC" >&2
  exit 1
fi
if [[ "$controller_role_manifest" == *'resources: ["secrets"]'* ]]; then
  echo "controller ClusterRole must not read Secrets cluster-wide" >&2
  exit 1
fi
if [[ "$controller_workspace_role_manifest" != *'resources: ["secrets"]'* || "$controller_workspace_role_manifest" != *'verbs: ["get"]'* ]]; then
  echo "controller must be able to grant per-instance Secret get permissions in the workspace namespace" >&2
  exit 1
fi
if ! rg -U -q 'kind: VMPodScrape(.|\n)*namespaceSelector:\n[[:space:]]+matchNames:\n[[:space:]]+- "?ns-admin"?(.|\n)*key: messagequeue.sealos.io/managed(.|\n)*- "true"(.|\n)*key: strimzi.io/component-type(.|\n)*- kafka(.|\n)*- kafka-exporter' "$temporary_dir/messagequeue-observability.yaml"; then
  echo "Kafka VMPodScrape must select only MessageQueue broker and exporter pods" >&2
  exit 1
fi
for crd_path in \
  "$repo_root/controller/config/crd/bases/messagequeue.sealos.io_messagequeues.yaml" \
  "$chart_dir/crds/messagequeue.sealos.io_messagequeues.yaml"; do
  if rg -U -q 'replicas:\n[[:space:]]+type: integer\n[[:space:]]+format: int32\n[[:space:]]+minimum: 1\n[[:space:]]+maximum: 9\n[[:space:]]+default: 1' "$crd_path"; then
    echo "MessageQueue CRD must not default nested kafka.replicas over compatibility spec.replicas: $crd_path" >&2
    exit 1
  fi
  if ! rg -U -q 'bootstrapAlternativeNames:\n[[:space:]]+type: array\n[[:space:]]+maxItems: 16\n[[:space:]]+items:\n[[:space:]]+type: string\n[[:space:]]+maxLength: 253\n[[:space:]]+pattern:' "$crd_path"; then
    echo "MessageQueue CRD must bound external bootstrap alternative names: $crd_path" >&2
    exit 1
  fi
done

kubectl kustomize "$repo_root/deploy/strimzi" > "$temporary_dir/strimzi.yaml"
if rg -n 'namespace: myproject' "$temporary_dir/strimzi.yaml"; then
  echo "Strimzi render still contains the upstream example namespace" >&2
  exit 1
fi
if ! rg -U -q 'name: STRIMZI_NAMESPACE\n[[:space:]]+value: ""' "$temporary_dir/strimzi.yaml"; then
  echo "Strimzi is not configured to watch all workspace namespaces" >&2
  exit 1
fi
if [[ "$(rg -c 'name: strimzi-cluster-operator-(namespaced|watched)-all|name: strimzi-cluster-operator-entity-operator-delegation-all' "$temporary_dir/strimzi.yaml")" -ne 3 ]]; then
  echo "Strimzi cluster-wide workspace bindings are incomplete" >&2
  exit 1
fi
if ! rg -U -q 'name: strimzi-cluster-operator-entity-operator-delegation-all(.|\n)*roleRef:(.|\n)*name: strimzi-entity-operator' "$temporary_dir/strimzi.yaml"; then
  echo "Strimzi cannot delegate KafkaTopic/KafkaUser permissions in workspaces" >&2
  exit 1
fi

docker buildx bake -f "$repo_root/deploy/docker-bake.hcl" --print > "$temporary_dir/bake.json"
if [[ "$(rg -c 'linux/amd64' "$temporary_dir/bake.json")" -ne 3 ]]; then
  echo "every application build target must be linux/amd64" >&2
  exit 1
fi

if rg -n 'image:[[:space:]]+[^[:space:]]+:latest|tag:[[:space:]]+latest' "$repo_root/deploy"; then
  echo "latest image references are forbidden" >&2
  exit 1
fi

bash -n "$repo_root/deploy/cluster-62-smoke.sh"
bash -n "$repo_root/deploy/kafka-roundtrip-smoke.sh"
echo "deployment checks passed"
