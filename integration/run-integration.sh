#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "  Data Pump K8s Integration Test"
echo "=========================================="

# ensure teardown on exit
trap '${SCRIPT_DIR}/scripts/teardown.sh || true' EXIT

# 1. Setup k3d cluster
echo ""
echo ">>> Step 1: Setup k3d cluster"
bash "${SCRIPT_DIR}/scripts/setup-k3d.sh"

# 2. Build & deploy
echo ""
echo ">>> Step 2: Build & deploy"
bash "${SCRIPT_DIR}/scripts/deploy.sh"

# 3. Verify
echo ""
echo ">>> Step 3: Verify"
bash "${SCRIPT_DIR}/scripts/verify.sh"

# teardown happens via trap
echo ""
echo ">>> Integration test complete"
