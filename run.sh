# Launch Kafka Connect
/etc/confluent/docker/run &
#
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
  "jdbcConnectionProperties": "?socket_timeout=30000",
  "topic2TableMap": "$map",
  "transforms": "AddPrefix",
  "transforms.AddPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.AddPrefix.regex": ".*",
  "transforms.AddPrefix.replacement": "${ENVIRONMENT_CODE}__\$0"
}
EOF
)

echo -e "\n--\n+> Starting to configure ClickHouse Sink Connector"
# shellcheck disable=SC2090
curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/config -d "${CLICKHOUSE_SINK_CONNECTOR_CONFIG}"
sleep infinity
