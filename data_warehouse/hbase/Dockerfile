FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.4.17

ENV HBASE_HOME=/opt/hbase \
    HBASE_CONF_DIR=/opt/hbase/conf \
    PATH=/opt/hbase/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bash \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" -o /tmp/hbase.tgz \
    && tar -xzf /tmp/hbase.tgz -C /opt \
    && ln -s /opt/hbase-${HBASE_VERSION} /opt/hbase \
    && rm -f /tmp/hbase.tgz

COPY hbase-site.xml /opt/hbase/conf/hbase-site.xml
COPY start-hbase.sh /start-hbase.sh

RUN chmod +x /start-hbase.sh

EXPOSE 16010 16000 2181

CMD ["/start-hbase.sh"]
