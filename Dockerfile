FROM confluentinc/cp-kafka-connect:7.8.3-1-ubi8.amd64

ENV CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components"

RUN confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:v1.3.1

COPY ./run.sh /run.sh
COPY ./healthcheck.sh /healthcheck.sh

ENTRYPOINT ["bash", "/run.sh"]
