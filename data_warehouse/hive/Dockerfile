FROM eclipse-temurin:8-jre

ARG HIVE_VERSION=3.1.3
ARG HADOOP_VERSION=3.3.6

ENV HIVE_HOME=/opt/hive \
    HADOOP_HOME=/opt/hadoop \
    HIVE_CONF_DIR=/opt/hive/conf \
    PATH=/opt/hive/bin:/opt/hadoop/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bash \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" -o /tmp/hive.tgz \
    && tar -xzf /tmp/hive.tgz -C /opt \
    && ln -s /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive \
    && rm -f /tmp/hive.tgz

RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tgz \
    && tar -xzf /tmp/hadoop.tgz -C /opt \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm -f /tmp/hadoop.tgz

RUN curl -fsSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
      -o /opt/hive/lib/postgresql-42.7.4.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY start-hive.sh /start-hive.sh

RUN chmod +x /start-hive.sh

EXPOSE 10000 10002 9083

CMD ["/start-hive.sh"]
