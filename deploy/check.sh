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
