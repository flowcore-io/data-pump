#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="data-pump-integration"

echo "==> Tearing down k3d cluster: ${CLUSTER_NAME}"

if k3d cluster list 2>/dev/null | grep -q "${CLUSTER_NAME}"; then
  k3d cluster delete "${CLUSTER_NAME}"
  echo "==> Cluster deleted"
else
  echo "==> Cluster not found, nothing to tear down"
fi
