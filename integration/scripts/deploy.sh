#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CLUSTER_NAME="data-pump-integration"
IMAGE_NAME="data-pump-integration-test:local"
NAMESPACE="data-pump-integration-test"

echo "==> Building Docker image: ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" -f "${ROOT_DIR}/integration/app/Dockerfile" "${ROOT_DIR}"

echo "==> Importing image into k3d cluster..."
k3d image import "${IMAGE_NAME}" -c "${CLUSTER_NAME}"

echo "==> Applying Kubernetes manifests..."
kubectl apply -f "${ROOT_DIR}/integration/k8s/namespace.yaml"
kubectl apply -f "${ROOT_DIR}/integration/k8s/postgres.yaml"

echo "==> Waiting for PostgreSQL to be ready..."
kubectl rollout status deployment/postgres -n "${NAMESPACE}" --timeout=120s
kubectl wait --for=condition=ready pod -l app=postgres -n "${NAMESPACE}" --timeout=120s

echo "==> Deploying ConfigMap and test app..."
kubectl apply -f "${ROOT_DIR}/integration/k8s/configmap.yaml"
kubectl apply -f "${ROOT_DIR}/integration/k8s/test-app.yaml"

echo "==> Waiting for test app pods to be ready..."
kubectl rollout status deployment/data-pump-test -n "${NAMESPACE}" --timeout=180s

echo "==> Deploy complete"
kubectl get pods -n "${NAMESPACE}"
