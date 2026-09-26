FROM bde2020/hive:2.3.2-postgresql-metastore

USER root

RUN rm -f /opt/hive/lib/postgresql-*.jar

ADD https://jdbc.postgresql.org/download/postgresql-42.7.13.jar /opt/hive/lib/postgresql-42.7.13.jar

RUN chmod 644 /opt/hive/lib/postgresql-42.7.13.jar