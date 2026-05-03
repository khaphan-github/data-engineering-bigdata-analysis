FROM eclipse-temurin:17-jre

ARG KAFKA_VERSION=3.9.0
ARG SCALA_VERSION=2.13
ARG KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
ARG KAFKA_DOWNLOAD_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "$KAFKA_DOWNLOAD_URL" -o "/tmp/${KAFKA_TGZ}" \
    && tar -xzf "/tmp/${KAFKA_TGZ}" -C /opt \
    && ln -s "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka \
    && rm -f "/tmp/${KAFKA_TGZ}"

RUN useradd -r -m -u 1001 -s /usr/sbin/nologin kafka \
    && mkdir -p /var/lib/kafka/data \
    && chown -R kafka:kafka /var/lib/kafka /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}

COPY start-kafka.sh /usr/local/bin/start-kafka.sh

WORKDIR /opt/kafka

EXPOSE 9092 9093 9094 9095 9096

ENTRYPOINT ["/usr/local/bin/start-kafka.sh"]
