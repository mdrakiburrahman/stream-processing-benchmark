package benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlinkStreamConsumer {

    public static void main(String[] args) throws Exception {
        // ── Required environment variables ──────────────────────────────
        String ehConnStr     = requireEnv("EVENTHUB_CONNECTION_STRING");
        String adlsAccount   = requireEnv("ADLSG2_ACCOUNT_NAME");
        String adlsKey       = requireEnv("ADLSG2_ACCOUNT_KEY");
        String adlsContainer = requireEnv("ADLSG2_CONTAINER");

        // ── Optional environment variables ──────────────────────────────
        String ehName         = env("EVENTHUB_NAME", "benchmark");
        String consumerGroup  = env("CONSUMER_GROUP", "flink22");
        String deltaTablePath = env("DELTA_TABLE_PATH", "flink2.2/benchmark");

        int parallelism   = Integer.parseInt(env("FLINK_PARALLELISM", "24"));
        long checkpointMs = Long.parseLong(env("CHECKPOINT_INTERVAL_MS", "5000"));
        int healthPort    = Integer.parseInt(env("HEALTH_PORT", "8080"));

        // Kafka consumer tuning (mirrors Spark consumer values)
        String kafkaMaxPollRecords     = env("KAFKA_MAX_POLL_RECORDS", "50000000");
        String kafkaFetchMaxBytes      = env("KAFKA_FETCH_MAX_BYTES", "2097152000");
        String kafkaMaxPartFetchBytes  = env("KAFKA_MAX_PARTITION_FETCH_BYTES", "1048576000");
        String kafkaReceiveBufferBytes = env("KAFKA_RECEIVE_BUFFER_BYTES", "10485760");
        String kafkaFetchMinBytes      = env("KAFKA_FETCH_MIN_BYTES", "1");
        String kafkaFetchMaxWaitMs     = env("KAFKA_FETCH_MAX_WAIT_MS", "500");
        String kafkaRequestTimeoutMs   = env("KAFKA_REQUEST_TIMEOUT_MS", "120000");
        String kafkaSessionTimeoutMs   = env("KAFKA_SESSION_TIMEOUT_MS", "60000");

        // ABFS write tuning
        String abfsWriteRequestSize   = env("ABFS_WRITE_REQUEST_SIZE", "8388608");
        String abfsWriteMaxConcurrent = env("ABFS_WRITE_MAX_CONCURRENT", "48");

        // ── Derived values ─────────────────────────────────────────────
        String namespace        = extractNamespace(ehConnStr);
        String bootstrapServers = namespace + ".servicebus.windows.net:9093";
        String abfsBase         = "abfss://" + adlsContainer + "@" + adlsAccount + ".dfs.core.windows.net";
        String deltaPath        = abfsBase + "/" + deltaTablePath;
        String checkpointPath   = env("CHECKPOINT_PATH", "/tmp/flink-checkpoint");

        // ── Hadoop configuration for ADLS Gen2 ────────────────────────
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.azure.account.key." + adlsAccount + ".dfs.core.windows.net", adlsKey);
        hadoopConf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
        hadoopConf.set("fs.azure.always.use.https", "true");
        hadoopConf.set("fs.azure.write.request.size", abfsWriteRequestSize);
        hadoopConf.set("fs.azure.write.max.concurrent.requests", abfsWriteMaxConcurrent);
        hadoopConf.set("fs.azure.enable.flush", "false");
        hadoopConf.set("fs.azure.disable.outputstream.flush", "true");
        // Use array (in-memory) data blocks to avoid LocalFileSystem dependency in plugin classloader
        hadoopConf.set("fs.azure.data.blocks.buffer", "array");
        hadoopConf.set("parquet.compression", "SNAPPY");

        // ── Health-check HTTP server (daemon) ──────────────────────────
        AtomicBoolean jobStarted = new AtomicBoolean(false);
        startHealthCheckServer(healthPort, jobStarted);

        // ── Flink execution environment ────────────────────────────────
        Configuration flinkConf = new Configuration();
        flinkConf.setString("execution.buffer-timeout", "0 ms");
        // Azure FS plugin reads these from Flink config
        flinkConf.setString("fs.azure.account.key." + adlsAccount + ".dfs.core.windows.net", adlsKey);
        flinkConf.setString("fs.azure.data.blocks.buffer", "array");
        flinkConf.setString("fs.azure.write.request.size", abfsWriteRequestSize);
        flinkConf.setString("fs.azure.write.max.concurrent.requests", abfsWriteMaxConcurrent);
        flinkConf.setString("fs.azure.enable.flush", "false");
        flinkConf.setString("fs.azure.disable.outputstream.flush", "true");
        flinkConf.setString("fs.azure.always.use.https", "true");

        // Load Flink FS plugins (flink-azure-fs-hadoop) from FLINK_PLUGINS_DIR
        org.apache.flink.core.plugin.PluginManager pluginManager =
                org.apache.flink.core.plugin.PluginUtils.createPluginManagerFromRootFolder(flinkConf);
        FileSystem.initialize(flinkConf, pluginManager);

        StreamExecutionEnvironment flink = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
        flink.setParallelism(parallelism);
        flink.enableCheckpointing(checkpointMs);
        flink.getCheckpointConfig().setCheckpointStorage("file://" + checkpointPath);
        flink.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // ── Kafka source ───────────────────────────────────────────────
        String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"$ConnectionString\" password=\"" + ehConnStr + "\";";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(ehName)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty("sasl.jaas.config", jaasConfig)
                .setProperty("max.poll.records", kafkaMaxPollRecords)
                .setProperty("fetch.max.bytes", kafkaFetchMaxBytes)
                .setProperty("max.partition.fetch.bytes", kafkaMaxPartFetchBytes)
                .setProperty("receive.buffer.bytes", kafkaReceiveBufferBytes)
                .setProperty("fetch.min.bytes", kafkaFetchMinBytes)
                .setProperty("fetch.max.wait.ms", kafkaFetchMaxWaitMs)
                .setProperty("request.timeout.ms", kafkaRequestTimeoutMs)
                .setProperty("session.timeout.ms", kafkaSessionTimeoutMs)
                .build();

        // ── Delta output schema ────────────────────────────────────────
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("ts", new TimestampType(3))
        ));

        // ── Processing pipeline ────────────────────────────────────────
        DataStream<RowData> processed = flink
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(new JsonToRowDataMapper())
                .returns(InternalTypeInfo.of(rowType));

        // ── Pre-create Delta table with disabled log checkpoints ─────────
        io.delta.standalone.DeltaLog deltaLog = io.delta.standalone.DeltaLog.forTable(hadoopConf, deltaPath);
        if (!deltaLog.tableExists()) {
            System.out.println("[INFO] Pre-creating Delta table with checkpointInterval=10000000");
            Map<String, String> tableConfig = new HashMap<>();
            tableConfig.put("delta.checkpointInterval", "10000000");
            io.delta.standalone.types.StructType schema =
                    new io.delta.standalone.types.StructType(
                            new io.delta.standalone.types.StructField[]{
                                    new io.delta.standalone.types.StructField(
                                            "ts",
                                            new io.delta.standalone.types.TimestampType(),
                                            true)
                            });
            io.delta.standalone.actions.Metadata metadata =
                    io.delta.standalone.actions.Metadata.builder()
                            .schema(schema)
                            .configuration(tableConfig)
                            .build();
            deltaLog.startTransaction().commit(
                    Collections.singletonList(metadata),
                    new io.delta.standalone.Operation(
                            io.delta.standalone.Operation.Name.CREATE_TABLE),
                    "Flink Benchmark");
        }

        // ── Delta Lake sink ────────────────────────────────────────────
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new org.apache.flink.core.fs.Path(deltaPath),
                        hadoopConf,
                        rowType)
                .build();

        processed.sinkTo(deltaSink);

        // ── Execute ────────────────────────────────────────────────────
        System.out.println("[INFO] Starting Flink stream consumer");
        System.out.println("[INFO] Bootstrap:  " + bootstrapServers);
        System.out.println("[INFO] Topic:      " + ehName);
        System.out.println("[INFO] Group:      " + consumerGroup);
        System.out.println("[INFO] Delta path: " + deltaPath);
        System.out.println("[INFO] Parallelism:  " + parallelism);
        System.out.println("[INFO] Checkpoint:   " + checkpointMs + " ms");

        jobStarted.set(true);
        flink.execute("Flink Stream Consumer Benchmark");
    }

    // ── Map function: JSON → RowData(ts) ──────────────────────────────
    public static class JsonToRowDataMapper extends RichMapFunction<String, RowData> {
        private transient ObjectMapper mapper;
        private transient long count;

        @Override
        public void open(Configuration parameters) {
            this.mapper = new ObjectMapper();
            this.count = 0;
        }

        @Override
        public RowData map(String json) throws Exception {
            if (++count % 100000 == 1) {
                System.out.println("[FLINK-MAP] Processed " + count + " records. Sample: " + json.substring(0, Math.min(json.length(), 80)));
            }
            JsonNode node = mapper.readTree(json);
            String tsStr = node.get("ts").asText();

            // C# "O" format: 2024-01-15T10:30:45.1234567Z
            OffsetDateTime odt = OffsetDateTime.parse(tsStr);
            long tsEpochMs = odt.toInstant().toEpochMilli();
            GenericRowData row = new GenericRowData(1);
            row.setField(0, TimestampData.fromEpochMillis(tsEpochMs));
            return row;
        }
    }

    // ── Health-check server ───────────────────────────────────────────
    private static void startHealthCheckServer(int port, AtomicBoolean jobStarted) {
        Thread t = new Thread("health-probe") {
            @Override
            public void run() {
                try (ServerSocket server = new ServerSocket(port)) {
                    server.setReuseAddress(true);
                    System.out.println("[INFO] Health probe listening on port " + port);
                    while (!isInterrupted()) {
                        try {
                            Socket client = server.accept();
                            try {
                                BufferedReader in = new BufferedReader(
                                        new InputStreamReader(client.getInputStream()));
                                String line = in.readLine();
                                while (line != null && !line.isEmpty()) line = in.readLine();

                                String status, body;
                                if (jobStarted.get()) {
                                    status = "200 OK";
                                    body = "ok";
                                } else {
                                    status = "503 Service Unavailable";
                                    body = "starting";
                                }

                                byte[] bodyBytes = body.getBytes("UTF-8");
                                String header = "HTTP/1.1 " + status + "\r\n"
                                        + "Content-Length: " + bodyBytes.length + "\r\n"
                                        + "Connection: close\r\n\r\n";
                                OutputStream os = client.getOutputStream();
                                os.write(header.getBytes("UTF-8"));
                                os.write(bodyBytes);
                                os.flush();
                            } finally {
                                client.close();
                            }
                        } catch (Exception ignored) { }
                    }
                } catch (IOException e) {
                    System.err.println("[ERROR] Health server failed: " + e.getMessage());
                }
            }
        };
        t.setDaemon(true);
        t.start();
    }

    // ── Helpers ────────────────────────────────────────────────────────
    private static String extractNamespace(String connStr) {
        Matcher m = Pattern.compile("Endpoint=sb://([^.]+)\\.servicebus\\.windows\\.net")
                .matcher(connStr);
        if (m.find()) return m.group(1);
        throw new IllegalArgumentException("Cannot extract namespace from connection string");
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty())
            throw new IllegalArgumentException("Required environment variable missing: " + name);
        return v;
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }
}
