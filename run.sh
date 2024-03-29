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

CLICKHOUSE_SINK_CONNECTOR_NAME=clickhouse-sink-connector

CLICKHOUSE_SINK_CONNECTOR_CONFIG="{
           \"connector.class\": \"com.clickhouse.kafka.connect.ClickHouseSinkConnector\",
           \"name\": \"${CLICKHOUSE_SINK_CONNECTOR_NAME}\",
           \"tasks.max\": 15,
           \"topics\": \"DATA_WAREHOUSE__PLAYER_BET_TRANSACTION_DETAILS,DATA_WAREHOUSE__PLAYER,DATA_WAREHOUSE__PLAYER_DETAILS,DATA_WAREHOUSE__PLAYER_AFFILIATE,DATA_WAREHOUSE__PLAYER_LINKED_AFFILIATE,DATA_WAREHOUSE__PLAYER_BONUS\",
           \"ssl\": false,
           \"hostname\": \"${CLICKHOUSE_HOSTNAME}\",
           \"database\": \"${CLICKHOUSE_DATABASE}\",
           \"username\": \"${CLICKHOUSE_USERNAME}\",
           \"password\": \"${CLICKHOUSE_PASSWORD}\",
           \"port\": \"${CLICKHOUSE_PORT}\",
           \"key.converter.schemas.enable\": false,
           \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
           \"value.converter.schemas.enable\": false,
           \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
           \"exactlyOnce\": true,
           \"schemas.enable\": false,
           \"errors.tolerance\": \"all\",
           \"tableRefreshInterval\": 6,
           \"topic2TableMap\": \"DATA_WAREHOUSE__PLAYER_BET_TRANSACTION_DETAILS=player_bet_transaction_details_distributed,DATA_WAREHOUSE__PLAYER=player_distributed,DATA_WAREHOUSE__PLAYER_DETAILS=player_details_distributed,DATA_WAREHOUSE__PLAYER_AFFILIATE=player_affiliate_distributed,DATA_WAREHOUSE__PLAYER_LINKED_AFFILIATE=player_linked_affiliate_distributed,DATA_WAREHOUSE__PLAYER_BONUS=player_bonus_distributed\"
       }"

echo -e "\n--\n+> Starting to configure ClickHouse Sink Connector"
# shellcheck disable=SC2090
curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8084/connectors/${CLICKHOUSE_SINK_CONNECTOR_NAME}/config -d "${CLICKHOUSE_SINK_CONNECTOR_CONFIG}"
sleep infinity
