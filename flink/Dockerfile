FROM flink:1.19.3-scala_2.12-java11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Cliente Hadoop para permitir acesso ao HDFS.
RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# Conector SQL Flink -> HBase 2.2.
RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hbase-2.2/4.0.0-1.19/flink-sql-connector-hbase-2.2-4.0.0-1.19.jar \
    -o /opt/flink/lib/flink-sql-connector-hbase-2.2-4.0.0-1.19.jar

USER flink