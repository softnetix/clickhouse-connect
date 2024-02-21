FROM confluentinc/cp-kafka-connect:7.1.0-1-ubi8

ENV CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components"

RUN confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:v1.0.10

COPY ./run.sh /run.sh

ENTRYPOINT ["bash", "/run.sh"]
