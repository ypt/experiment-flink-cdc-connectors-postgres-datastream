FROM flink:1.12.1-scala_2.11

# Copy sql-client script
COPY sql-client/ /opt/sql-client
RUN mkdir -p /opt/sql-client/lib

# Download connector libraries
RUN wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.12.1/flink-json-1.12.1.jar; \
  # wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.12.1/flink-sql-connector-kafka_2.11-1.12.1.jar; \
  # wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.12.1/flink-sql-connector-elasticsearch7_2.11-1.12.1.jar; \
  wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.12.1/flink-connector-jdbc_2.11-1.12.1.jar; \
  wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.2.14/postgresql-42.2.14.jar; \
  # pulsar-flink dependencies
  wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/io/streamnative/connectors/pulsar-flink-connector-2.11/1.12.2.0/pulsar-flink-connector-2.11-1.12.2.0.jar; \
  wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/org/apache/pulsar/pulsar-client-all/2.7.2/pulsar-client-all-2.7.2.jar; \
  # https://github.com/apache/pulsar/issues/6982
  wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/org/apache/pulsar/protobuf-shaded/2.1.0-incubating/protobuf-shaded-2.1.0-incubating.jar; \
  # [ERROR] Could not execute SQL statement. Reason:
  # java.lang.ClassNotFoundException: org.slf4j.bridge.SLF4JBridgeHandler
  wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/org/slf4j/jul-to-slf4j/1.7.31/jul-to-slf4j-1.7.31.jar; \
  # wget -P /opt/sql-client/lib/ https://dl.bintray.com/streamnative/maven/org/apache/pulsar/protobuf-shaded/2.6.0-1df9882/protobuf-shaded-2.6.0-1df9882.jar; \
  # wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/io/streamnative/connectors/pulsar-flink-connector-origin/1.12.2.0/pulsar-flink-connector-origin-1.12.2.0.jar; \
  # wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/io/streamnative/connectors/flink-protobuf/2.7.6/flink-protobuf-2.7.6.jar; \
  # wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.1/protobuf-java-3.11.1.jar; \
  # Comment out the below to use local build mounted via docker volume instead of this
  wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/com/alibaba/ververica/flink-sql-connector-postgres-cdc/1.2.0/flink-sql-connector-postgres-cdc-1.2.0.jar;

# Copy configuration
COPY conf/* /opt/flink/conf/

WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME /opt/sql-client

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
