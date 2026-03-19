#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="data-pump-integration"

echo "==> Setting up k3d cluster: ${CLUSTER_NAME}"

# install k3d if missing
if ! command -v k3d &>/dev/null; then
  echo "==> Installing k3d..."
  curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
fi

# install kubectl if missing
if ! command -v kubectl &>/dev/null; then
  echo "==> Installing kubectl..."
  if [[ "$(uname -s)" == "Linux" ]]; then
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl && sudo mv kubectl /usr/local/bin/
  elif [[ "$(uname -s)" == "Darwin" ]]; then
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/darwin/arm64/kubectl"
    chmod +x kubectl && sudo mv kubectl /usr/local/bin/
  fi
fi

# delete existing cluster if present (clean slate)
if k3d cluster list | grep -q "${CLUSTER_NAME}"; then
  echo "==> Deleting existing cluster..."
  k3d cluster delete "${CLUSTER_NAME}"
fi

# create cluster
echo "==> Creating k3d cluster..."
k3d cluster create "${CLUSTER_NAME}" \
  --agents 1 \
  --wait \
  --timeout 120s

# verify
echo "==> Verifying cluster..."
kubectl cluster-info
kubectl get nodes

echo "==> k3d cluster '${CLUSTER_NAME}' is ready"
