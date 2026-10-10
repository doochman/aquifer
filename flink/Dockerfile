FROM flink:1.20.1-scala_2.12-java17

# Iceberg's Flink 1.20 runtime exists from 1.7.0 on; the 1.6.0 REST catalog server speaks the same protocol.
ARG ICEBERG_VERSION=1.7.1
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG HADOOP_VERSION=3.3.6

USER root
# Connector jars come from Maven Central and are checked against their published SHA-1 files.
RUN set -eu; \
    cd /opt/flink/lib; \
    central=https://repo1.maven.org/maven2; \
    fetch() { f="$(basename "$1")"; curl -fsSLo "$f" "$1"; echo "$(curl -fsSL "$1.sha1" | cut -d' ' -f1)  $f" | sha1sum -c -; }; \
    fetch "$central/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"; \
    fetch "$central/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    fetch "$central/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    fetch "$central/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    fetch "$central/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar"
USER flink
