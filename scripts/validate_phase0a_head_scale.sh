#!/usr/bin/env bash
set -euo pipefail

namespace=sceptre-ray-head-scale-smoke
manifest=infra/k3d/raycluster-head-scale-smoke.yaml

kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$namespace" create serviceaccount phase-0a-ray-head \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$namespace" patch serviceaccount phase-0a-ray-head \
  --type=merge -p '{"automountServiceAccountToken":false}' >/dev/null

start_epoch=$(date +%s)
latencies=()
for index in $(seq -w 0 14); do
  cluster_started=$(date +%s)
  sed "s/PLACEHOLDER/${index}/g" "$manifest" | kubectl apply -f -
  kubectl -n "$namespace" wait \
    --for=jsonpath='{.status.state}'=ready \
    "raycluster/phase-0a-head-${index}" --timeout=180s
  latencies+=("$(( $(date +%s) - cluster_started ))")
done

cluster_count=$(kubectl -n "$namespace" get rayclusters -o json | jq '.items | length')
head_count=$(kubectl -n "$namespace" get pods \
  -l app.kubernetes.io/name=phase-0a-ray-head-scale -o json | jq '.items | length')
ready_count=$(kubectl -n "$namespace" get pods \
  -l app.kubernetes.io/name=phase-0a-ray-head-scale -o json |
  jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length')

test "$cluster_count" -eq 15
test "$head_count" -eq 15
test "$ready_count" -eq 15
jq_latencies=$(printf '%s\n' "${latencies[@]}" | jq -s 'sort')
jq -n \
  --argjson clusters "$cluster_count" \
  --argjson heads "$head_count" \
  --argjson ready "$ready_count" \
  --argjson latencies "$jq_latencies" \
  --argjson elapsed "$(( $(date +%s) - start_epoch ))" \
  '{clusters: $clusters, head_pods: $heads, ready_head_pods: $ready,
    startup_seconds: $latencies, elapsed_seconds: $elapsed, status: "passed"}'
