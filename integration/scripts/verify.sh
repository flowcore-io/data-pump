#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="data-pump-integration-test"
TOTAL_EVENTS="${TOTAL_EVENTS:-100}"
TIMEOUT=180
POLL_INTERVAL=5

echo "==> Verification starting (expecting ${TOTAL_EVENTS} events, timeout ${TIMEOUT}s)"

# 1. Wait for all 3 pods to be Ready
echo "==> Waiting for all pods to be Ready..."
kubectl wait --for=condition=ready pod -l app=data-pump-test -n "${NAMESPACE}" --timeout=120s
PODS=$(kubectl get pods -n "${NAMESPACE}" -l app=data-pump-test -o jsonpath='{.items[*].metadata.name}')
POD_COUNT=$(echo "${PODS}" | wc -w | tr -d ' ')
echo "==> ${POD_COUNT} pods ready: ${PODS}"

if [[ "${POD_COUNT}" -lt 3 ]]; then
  echo "FAIL: Expected 3 pods, got ${POD_COUNT}"
  exit 1
fi

# 2. Poll PG until all events are processed
PG_POD=$(kubectl get pods -n "${NAMESPACE}" -l app=postgres -o jsonpath='{.items[0].metadata.name}')
echo "==> Polling PostgreSQL (pod: ${PG_POD}) for processed events..."

ELAPSED=0
while [[ ${ELAPSED} -lt ${TIMEOUT} ]]; do
  COUNT=$(kubectl exec -n "${NAMESPACE}" "${PG_POD}" -- \
    psql -U postgres -d datapump_test -t -A -c \
    "SELECT COUNT(DISTINCT event_id) FROM processed_events" 2>/dev/null || echo "0")
  COUNT=$(echo "${COUNT}" | tr -d '[:space:]')

  echo "   [${ELAPSED}s] Processed events: ${COUNT}/${TOTAL_EVENTS}"

  if [[ "${COUNT}" -ge "${TOTAL_EVENTS}" ]]; then
    echo "==> All ${TOTAL_EVENTS} events processed!"
    break
  fi

  sleep ${POLL_INTERVAL}
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

if [[ ${ELAPSED} -ge ${TIMEOUT} ]]; then
  echo "FAIL: Timed out waiting for events to be processed (got ${COUNT}/${TOTAL_EVENTS})"
  echo "==> Pod logs:"
  for POD in ${PODS}; do
    echo "--- ${POD} ---"
    kubectl logs -n "${NAMESPACE}" "${POD}" --tail=50 || true
  done
  exit 1
fi

# 3. Assert distribution across multiple pods
DISTINCT_PODS=$(kubectl exec -n "${NAMESPACE}" "${PG_POD}" -- \
  psql -U postgres -d datapump_test -t -A -c \
  "SELECT COUNT(DISTINCT pod_name) FROM processed_events")
DISTINCT_PODS=$(echo "${DISTINCT_PODS}" | tr -d '[:space:]')

echo "==> Events distributed across ${DISTINCT_PODS} pod(s)"

if [[ "${DISTINCT_PODS}" -lt 2 ]]; then
  echo "FAIL: Events were only processed by ${DISTINCT_PODS} pod(s), expected >= 2"
  exit 1
fi

# 4. Log distribution
echo "==> Event distribution by pod:"
kubectl exec -n "${NAMESPACE}" "${PG_POD}" -- \
  psql -U postgres -d datapump_test -t -c \
  "SELECT pod_name, COUNT(*) as event_count FROM processed_events GROUP BY pod_name ORDER BY event_count DESC"

# 5. Assert exactly 1 leader
echo "==> Checking leader election..."
LEADER_COUNT=0
for POD in ${PODS}; do
  HEALTH=$(kubectl exec -n "${NAMESPACE}" "${POD}" -- \
    curl -s http://localhost:8080/health 2>/dev/null || echo '{}')
  IS_LEADER=$(echo "${HEALTH}" | grep -o '"isLeader":[a-z]*' | cut -d: -f2)
  echo "   ${POD}: isLeader=${IS_LEADER}"
  if [[ "${IS_LEADER}" == "true" ]]; then
    LEADER_COUNT=$((LEADER_COUNT + 1))
  fi
done

echo "==> Leader count: ${LEADER_COUNT}"
if [[ "${LEADER_COUNT}" -ne 1 ]]; then
  echo "FAIL: Expected exactly 1 leader, got ${LEADER_COUNT}"
  exit 1
fi

echo ""
echo "=========================================="
echo "  ALL INTEGRATION TESTS PASSED"
echo "=========================================="
