FROM apache/hive:3.1.3@sha256:d102ba29ad07e93c303894896203a80b903c0001d80221f1cb9fea92dcac06e4

USER root

COPY core-site.xml /opt/hive/conf/core-site.xml

RUN find /opt/hive/lib -maxdepth 1 -type f -iname 'postgresql*.jar' -delete
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar /opt/hive/lib/ \
    && cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG POSTGRES_JDBC_SHA256
ADD --checksum=sha256:${POSTGRES_JDBC_SHA256} \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/hive/lib/postgresql-jdbc.jar

RUN chmod 0644 /opt/hive/lib/postgresql-jdbc.jar

USER hive
