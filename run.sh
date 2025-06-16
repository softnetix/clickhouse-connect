#!/usr/bin/env bash

# Launch Kafka Connect
/etc/confluent/docker/run &

# Wait for Kafka Connect listener
echo "Waiting for Kafka Connect Worker to start listening on localhost ⏳"
while : ; do
  curl_status=$(curl -s -o /dev/null -w %{http_code} http://localhost:8084/connectors)
  echo -e $(date) " Kafka Connect listener HTTP state: " $curl_status " (waiting for 200)"
  if [ $curl_status -eq 200 ] ; then
    break
  fi
  sleep 5
done

# check last created table - avoiding restarts
while true; do
  response=$(curl -s -u root:root --data "EXISTS $CLICKHOUSE_DATABASE.$LAST_CREATED_TABLE_NAME" "http://clickhouse-db:8123")

  if [[ "$response" == "1" ]]; then
    echo "Table $CLICKHOUSE_DATABASE.$LAST_CREATED_TABLE_NAME exists."
    break
  else
    echo "Waiting for table $CLICKHOUSE_DATABASE.$LAST_CREATED_TABLE_NAME to be created..."
    sleep 2
  fi
done

topics=$(echo "$TOPIC2TABLEMAP" | awk -F ',' '{for (i=1; i<=NF; i++) print $i}' | awk -F '=' '{print $1}' | tr '\n' ',' | sed 's/,$//')

map=$(echo "$TOPIC2TABLEMAP" | awk -F ',' -v env_code="$ENVIRONMENT_CODE" '{for (i=1; i<=NF; i++) print env_code "__"$i}' | tr '\n' ',' | sed 's/,$//')

CLICKHOUSE_SINK_CONNECTOR_CONFIG=$(cat <<EOF
{
  "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
  "name": "${CLICKHOUSE_SINK_CONNECTOR_NAME}",
  "tasks.max": "${CONNECT_TASKS_MAX}",
  "topics": "$topics",
  "ssl": false,
  "hostname": "${CLICKHOUSE_HOSTNAME}",
  "database": "${CLICKHOUSE_DATABASE}",
  "username": "${CLICKHOUSE_USERNAME}",
  "password": "${CLICKHOUSE_PASSWORD}",
  "port": "${CLICKHOUSE_PORT}",
  "worker.sync.timeout.ms": 30000,
  "key.converter.schemas.enable": "${CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE}",
  "key.converter": "${CONNECT_KEY_CONVERTER}",
  "value.converter.schemas.enable": "${CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE}",
  "value.converter": "${CONNECT_VALUE_CONVERTER}",
  "exactlyOnce": "${CONNECT_SINK_EXACTLY_ONCE}",
  "clickhouseSettings": "${CLICKHOUSE_SETTINGS}",
  "schemas.enable": false,
  "errors.tolerance": "${CONNECT_ERRORS_TOLERANCE}",
  "errors.log.enable": "${CONNECT_ERRORS_LOG_ENABLE}",
  "errors.log.include.messages": "${CONNECT_ERRORS_LOG_INCLUDE_MESSAGES}",
  "errors.deadletterqueue.topic.name": "${CONNECT_ERRORS_DEADLETTERQUEUE_TOPIC_NAME}",
  "errors.deadletterqueue.topic.replication.factor": "${CONNECT_ERRORS_DEADLETTERQUEUE_TOPIC_REPLICATION_FACTOR}",
  "errors.deadletterqueue.context.headers.enable": "${CONNECT_ERRORS_DEADLETTERQUEUE_CONTEXT_HEADERS_ENABLE}",
  "tableRefreshInterval": "${CLICKHOUSE_TABLE_REFRESH_INTERVAL}",
  "consumer.override.fetch.min.bytes": "${CONSUMER_OVERRIDE_FETCH_MIN_BYTES}",
  "consumer.override.fetch.max.bytes": "${CONSUMER_OVERRIDE_FETCH_MAX_BYTES}",
  "consumer.override.fetch.max.wait.ms": "${CONSUMER_OVERRIDE_FETCH_MAX_WAIT_MS}",
  "consumer.override.max.poll.records": "${CONSUMER_OVERRIDE_MAX_POLL_RECORDS}",
  "consumer.override.max.partition.fetch.bytes": "${CONSUMER_OVERRIDE_MAX_PARTITION_FETCH_BYTES}",
  "consumer.override.enable.auto.commit": "${CONSUMER_OVERRIDE_ENABLE_AUTO_COMMIT}",
  "consumer.override.auto.offset.reset": "earliest",
  "consumer.override.metadata.max.age.ms": "30000",
  "consumer.override.retry.backoff.ms": "1000",
  "consumer.override.reconnect.backoff.ms": "1000",
  "consumer.override.reconnect.backoff.max.ms": "10000",
  "errors.retry.timeout": "300000",
  "errors.retry.delay.max.ms": "60000",
  "jdbcConnectionProperties": "?socket_timeout=30000",
  "topic2TableMap": "$map",
  "transforms": "AddPrefix",
  "transforms.AddPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.AddPrefix.regex": ".*",
  "transforms.AddPrefix.replacement": "${ENVIRONMENT_CODE}__\$0"
}
EOF
)

check_connector_health() {
  local status_response
  local connector_state
  local failed_tasks

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
  failed_tasks=$(cat /tmp/connector_status.json | grep -c '"state":"FAILED"')

  echo "$(date) - Connector state: $connector_state, Failed tasks: $failed_tasks"

  if [[ "$connector_state" == "FAILED" ]] || [[ "$failed_tasks" -gt 0 ]]; then
    return 1
  fi

  return 0
}

restart_connector() {
  echo "$(date) - Attempting to restart connector ${CLICKHOUSE_SINK_CONNECTOR_NAME}..."

  pause_response=$(curl -s -w "%{http_code}" -X PUT http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/pause 2>/dev/null)
  pause_http_code="${pause_response: -3}"

  if [[ "$pause_http_code" == "202" ]]; then
    echo "$(date) - Connector paused successfully"
    sleep 5
  else
    echo "$(date) - Warning: Failed to pause connector (HTTP: $pause_http_code)"
  fi

  restart_response=$(curl -s -w "%{http_code}" -X POST http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/restart 2>/dev/null)
  restart_http_code="${restart_response: -3}"

  if [[ "$restart_http_code" == "204" ]]; then
    echo "$(date) - Connector restart initiated successfully"
  else
    echo "$(date) - Warning: Failed to restart connector (HTTP: $restart_http_code)"
  fi

  sleep 5

  resume_response=$(curl -s -w "%{http_code}" -X PUT http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/resume 2>/dev/null)
  resume_http_code="${resume_response: -3}"

  if [[ "$resume_http_code" == "202" ]]; then
    echo "$(date) - Connector resumed successfully"
  else
    echo "$(date) - Warning: Failed to resume connector (HTTP: $resume_http_code)"
  fi
}

create_or_update_connector() {
  echo -e "\n--\n+> Starting to configure ClickHouse Sink Connector"

  # Try to create/update connector
  config_response=$(curl -s -w "%{http_code}" -X PUT -H "Content-Type:application/json" \
    http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/config \
    -d "${CLICKHOUSE_SINK_CONNECTOR_CONFIG}" 2>/dev/null)

  config_http_code="${config_response: -3}"

  if [[ "$config_http_code" == "200" ]] || [[ "$config_http_code" == "201" ]]; then
    echo "$(date) - Connector configured successfully (HTTP: $config_http_code)"
    return 0
  else
    echo "$(date) - Error: Failed to configure connector (HTTP: $config_http_code)"
    echo "Response: ${config_response%???}"
    return 1
  fi
}

# initial connector setup
create_or_update_connector
initial_setup_result=$?

if [[ $initial_setup_result -ne 0 ]]; then
  echo "$(date) - Error: Initial connector setup failed"
  exit 1
fi

# wait for connector to initialize
echo "$(date) - Waiting for connector to initialize..."
sleep 10

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

      # Optional: Send alert or notification here
      # You can add webhook call, email notification, etc.

      # Reset counter and continue monitoring (or exit based on your preference)
      RESTART_ATTEMPTS=0
      sleep 60  # Wait longer before next attempt
    fi
  fi

  sleep $HEALTH_CHECK_INTERVAL
done