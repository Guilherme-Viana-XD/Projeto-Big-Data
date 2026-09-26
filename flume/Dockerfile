FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 AS hadoop

FROM bde2020/flume

COPY --from=hadoop /opt/hadoop-3.2.1/share/hadoop/common/ /opt/hadoop-client/common/
COPY --from=hadoop /opt/hadoop-3.2.1/share/hadoop/hdfs/ /opt/hadoop-client/hdfs/

RUN rm -f /usr/local/apache-flume/current/lib/guava-*.jar

COPY flume/flume-env.sh /app/conf/flume-env.sh
COPY flume/hdfs-site.xml /app/conf/hdfs-site.xml