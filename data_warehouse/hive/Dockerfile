FROM eclipse-temurin:17-jre

ARG HIVE_VERSION=4.1.0
ARG HADOOP_VERSION=3.3.6
ARG POSTGRES_JDBC_VERSION=42.7.4

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

COPY deps /opt/deps

RUN if [ -f "/opt/deps/apache-hive-${HIVE_VERSION}-bin.tar.gz" ]; then \
            cp "/opt/deps/apache-hive-${HIVE_VERSION}-bin.tar.gz" /tmp/hive.tgz; \
        else \
            curl -fsSL "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" -o /tmp/hive.tgz; \
        fi \
        && tar -xzf /tmp/hive.tgz -C /opt \
    && ln -s /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive \
    && rm -f /tmp/hive.tgz

RUN if [ -f "/opt/deps/hadoop-${HADOOP_VERSION}.tar.gz" ]; then \
            cp "/opt/deps/hadoop-${HADOOP_VERSION}.tar.gz" /tmp/hadoop.tgz; \
        else \
            curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tgz; \
        fi \
        && tar -xzf /tmp/hadoop.tgz -C /opt \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm -f /tmp/hadoop.tgz

RUN if [ -f "/opt/deps/postgresql-${POSTGRES_JDBC_VERSION}.jar" ]; then \
            cp "/opt/deps/postgresql-${POSTGRES_JDBC_VERSION}.jar" "/opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
        else \
            curl -fsSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
                -o "/opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
        fi

COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY start-hive.sh /start-hive.sh

RUN chmod +x /start-hive.sh

EXPOSE 10000 10002 9083

CMD ["/start-hive.sh"]
