FROM eclipse-temurin:17-jre AS jre

FROM hadoop-base:3.3.6

USER root

# Upgrade runtime to Java 17 without apt (avoids repo/network issues)
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=jre $JAVA_HOME $JAVA_HOME
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --no-cache-dir \
    jupyterlab \
    hdfs \
    pyspark==3.5.1

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--IdentityProvider.token=", "--PasswordIdentityProvider.password="]
