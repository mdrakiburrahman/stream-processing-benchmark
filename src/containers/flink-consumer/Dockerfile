FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src/ src/
RUN mvn package -B -DskipTests

FROM eclipse-temurin:17-jre

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

ARG FLINK_VERSION=1.16.3
ARG FLINK_AZURE_FS_VERSION=1.17.2
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/flink/flink-azure-fs-hadoop/${FLINK_AZURE_FS_VERSION}/flink-azure-fs-hadoop-${FLINK_AZURE_FS_VERSION}.jar" \
         -o /opt/flink/plugins/azure-fs-hadoop/flink-azure-fs-hadoop-${FLINK_AZURE_FS_VERSION}.jar

COPY --from=builder /app/target/flink-benchmark-1.0.jar /opt/benchmark.jar
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENV FLINK_PLUGINS_DIR=/opt/flink/plugins

ENTRYPOINT ["/opt/entrypoint.sh"]
