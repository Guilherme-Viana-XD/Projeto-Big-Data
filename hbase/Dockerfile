FROM eclipse-temurin:8-jre-jammy

ARG HBASE_VERSION=2.2.7

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fL \
    https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
    -o /tmp/hbase.tar.gz \
    && tar -xzf /tmp/hbase.tar.gz -C /opt \
    && ln -s /opt/hbase-${HBASE_VERSION} /opt/hbase \
    && rm /tmp/hbase.tar.gz

ENV HBASE_HOME=/opt/hbase
ENV PATH="${HBASE_HOME}/bin:${PATH}"

COPY hbase/hbase-site.xml /opt/hbase/conf/hbase-site.xml

RUN mkdir -p /data/hbase /data/zookeeper

CMD ["/bin/bash", "-lc", "/opt/hbase/bin/start-hbase.sh && tail -F /opt/hbase/logs/*"]