#!/usr/bin/env bash

# custom healthcheck for tasks
check_connector_health() {
  local status_response
  local connector_state
  local non_running_tasks

  #curl -s -w "%{http_code}" http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/status 2>/dev/null
  status_response=$(curl -s -w "%{http_code}" -o /tmp/connector_status.json http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/status 2>/dev/null)
  http_code="${status_response: -3}"

  if [[ "$http_code" != "200" ]]; then
    echo "$(date) - Error: Cannot get connector status (HTTP: $http_code)"
    return 1
  fi

  if [[ ! -f /tmp/connector_status.json ]]; then
    echo "$(date) - Error: Status file not found"
    return 1
  fi

  connector_state=$(cat /tmp/connector_status.json | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
  non_running_tasks=$(cat /tmp/connector_status.json | grep -o '"state":"[^"]*"' | grep -vc '"state":"RUNNING"')

  echo "$(date) - Connector state: $connector_state, Non-running tasks: $non_running_tasks"

  if [[ "$connector_state" != "RUNNING" ]] || [[ "$non_running_tasks" -gt 0 ]]; then
    return 1
  fi

  return 0
}

restart_connector() {
  echo "$(date) - Attempting to restart connector ${CLICKHOUSE_SINK_CONNECTOR_NAME}..."

#  pause_response=$(curl -s -w "%{http_code}" -X PUT http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/pause 2>/dev/null)
#  pause_http_code="${pause_response: -3}"
#
#  if [[ "$pause_http_code" == "202" ]]; then
#    echo "$(date) - Connector paused successfully"
#    sleep 5
#  else
#    echo "$(date) - Warning: Failed to pause connector (HTTP: $pause_http_code)"
#  fi

  restart_response=$(curl -s -w "%{http_code}" -X POST http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/restart 2>/dev/null)
  restart_http_code="${restart_response: -3}"

  if [[ "$restart_http_code" == "204" ]]; then
    echo "$(date) - Connector restart initiated successfully"
  else
    echo "$(date) - Warning: Failed to restart connector (HTTP: $restart_http_code)"
  fi

  sleep 5

#  resume_response=$(curl -s -w "%{http_code}" -X PUT http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/resume 2>/dev/null)
#  resume_http_code="${resume_response: -3}"
#
#  if [[ "$resume_http_code" == "202" ]]; then
#    echo "$(date) - Connector resumed successfully"
#  else
#    echo "$(date) - Warning: Failed to resume connector (HTTP: $resume_http_code)"
#  fi
}

# health monitoring loop
HEALTH_CHECK_INTERVAL=${HEALTH_CHECK_INTERVAL:-30}
RESTART_ATTEMPTS=0
MAX_RESTART_ATTEMPTS=${MAX_RESTART_ATTEMPTS:-5}

echo "$(date) - Starting connector health monitoring (check interval: ${HEALTH_CHECK_INTERVAL}s)"

while true; do
  if check_connector_health; then
    echo "$(date) - Connector is healthy"
    RESTART_ATTEMPTS=0
  else
    echo "$(date) - Connector health check failed"

    if [[ $RESTART_ATTEMPTS -lt $MAX_RESTART_ATTEMPTS ]]; then
      RESTART_ATTEMPTS=$((RESTART_ATTEMPTS + 1))
      echo "$(date) - Restart attempt $RESTART_ATTEMPTS of $MAX_RESTART_ATTEMPTS"

      restart_connector

      # Wait longer after restart before next health check
      echo "$(date) - Waiting for connector to recover..."
      sleep 30
    else
      echo "$(date) - Error: Maximum restart attempts ($MAX_RESTART_ATTEMPTS) reached"
      echo "$(date) - Manual intervention may be required"

      # Reset counter and continue monitoring (or exit based on your preference)
      RESTART_ATTEMPTS=0
      sleep 60  # Wait longer before next attempt

      sleep 20
    fi
  fi

  sleep "$HEALTH_CHECK_INTERVAL"
done
