#!/bin/bash
set -euo pipefail

HADOOP_CONF_DIR="/tmp/hadoop-conf"
mkdir -p "$HADOOP_CONF_DIR"
cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.azure.account.key.${ADLSG2_ACCOUNT_NAME}.dfs.core.windows.net</name>
    <value>${ADLSG2_ACCOUNT_KEY}</value>
  </property>
  <property>
    <name>fs.abfss.impl</name>
    <value>org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem</value>
  </property>
  <property>
    <name>fs.file.impl</name>
    <value>org.apache.hadoop.fs.LocalFileSystem</value>
  </property>
  <property>
    <name>fs.azure.always.use.https</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.azure.write.request.size</name>
    <value>${ABFS_WRITE_REQUEST_SIZE:-8388608}</value>
  </property>
  <property>
    <name>fs.azure.write.max.concurrent.requests</name>
    <value>${ABFS_WRITE_MAX_CONCURRENT:-48}</value>
  </property>
  <property>
    <name>fs.azure.enable.flush</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.azure.disable.outputstream.flush</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.azure.data.blocks.buffer</name>
    <value>array</value>
  </property>
</configuration>
EOF
export HADOOP_CONF_DIR

exec java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp /opt/benchmark.jar benchmark.FlinkStreamConsumer
