package benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.hadoop.fs.Path

object StreamConsumer {
  def main(args: Array[String]): Unit = {
    val connectionString         = sys.env("EVENTHUB_CONNECTION_STRING")
    val eventHubName             = sys.env.getOrElse("EVENTHUB_NAME", "benchmark")
    val consumerGroup            = sys.env.getOrElse("CONSUMER_GROUP", "spark35")
    val triggerInterval          = sys.env.getOrElse("TRIGGER_INTERVAL", "0 seconds")
    val deltaTablePath           = sys.env.getOrElse("DELTA_TABLE_PATH", "benchmark")
    val sparkMaster              = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    val adlsAccountName          = sys.env("ADLSG2_ACCOUNT_NAME")
    val adlsAccountKey           = sys.env("ADLSG2_ACCOUNT_KEY")
    val adlsContainer            = sys.env("ADLSG2_CONTAINER")
    val abfsBase                 = s"abfs://$adlsContainer@$adlsAccountName.dfs.core.windows.net"
    val deltaPath                = s"$abfsBase/$deltaTablePath"
    val checkpointPath           = sys.env.getOrElse("CHECKPOINT_PATH", s"$abfsBase/$deltaTablePath/_checkpoint")
    val shufflePartitions        = sys.env.getOrElse("SPARK_SHUFFLE_PARTITIONS", "4")
    val kafkaMaxPollRecords      = sys.env.getOrElse("KAFKA_MAX_POLL_RECORDS", "500000")
    val kafkaFetchMaxBytes       = sys.env.getOrElse("KAFKA_FETCH_MAX_BYTES", "209715200")
    val kafkaPartFetchMaxBytes   = sys.env.getOrElse("KAFKA_MAX_PARTITION_FETCH_BYTES", "104857600")
    val kafkaMinPartitions       = sys.env.getOrElse("KAFKA_MIN_PARTITIONS", "24")
    val abfsWriteRequestSize     = sys.env.getOrElse("ABFS_WRITE_REQUEST_SIZE", "8388608")
    val codegenEnabled           = sys.env.getOrElse("SPARK_CODEGEN_ENABLED", "true")
    val minOffsetsPerTrigger     = sys.env.getOrElse("MIN_OFFSETS_PER_TRIGGER", "1")
    val maxOffsetsPerTrigger     = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "400000")
    val maxTriggerDelay          = sys.env.getOrElse("MAX_TRIGGER_DELAY", "1s")
    val pollTimeoutMs            = sys.env.getOrElse("KAFKA_CONSUMER_POLL_TIMEOUT_MS", "120000")
    val fetchOffsetRetries       = sys.env.getOrElse("KAFKA_FETCH_OFFSET_NUM_RETRIES", "10")
    val fetchOffsetRetryMs       = sys.env.getOrElse("KAFKA_FETCH_OFFSET_RETRY_INTERVAL_MS", "10")
    val failOnDataLoss           = sys.env.getOrElse("FAIL_ON_DATA_LOSS", "false")
    val outputCoalescePartitions = sys.env.getOrElse("OUTPUT_COALESCE_PARTITIONS", "4").toInt
    val abfsWriteMaxConcurrent   = sys.env.getOrElse("ABFS_WRITE_MAX_CONCURRENT", "24")
    val kafkaReceiveBufferBytes  = sys.env.getOrElse("KAFKA_RECEIVE_BUFFER_BYTES", "1048576")
    val kafkaFetchMinBytes       = sys.env.getOrElse("KAFKA_FETCH_MIN_BYTES", "1")
    val kafkaFetchMaxWaitMs      = sys.env.getOrElse("KAFKA_FETCH_MAX_WAIT_MS", "500")
    val kafkaRequestTimeoutMs    = sys.env.getOrElse("KAFKA_REQUEST_TIMEOUT_MS", "120000")
    val kafkaSessionTimeoutMs    = sys.env.getOrElse("KAFKA_SESSION_TIMEOUT_MS", "60000")
    val consumerCacheCapacity    = sys.env.getOrElse("CONSUMER_CACHE_CAPACITY", "256")

    val namespacePattern = """Endpoint=sb://([^.]+)\.servicebus\.windows\.net""".r
    val namespace = namespacePattern.findFirstMatchIn(connectionString) match {
      case Some(m) => m.group(1)
      case None    => throw new IllegalArgumentException("Cannot extract namespace from connection string")
    }
    val bootstrapServers = s"$namespace.servicebus.windows.net:9093"
    val jaasConfig = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$$ConnectionString" password="$connectionString";"""

    val spark = SparkSession.builder()
      .appName("StreamConsumer")
      .master(sparkMaster)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config(s"spark.hadoop.fs.azure.account.key.$adlsAccountName.dfs.core.windows.net", adlsAccountKey)
      .config("spark.hadoop.fs.azure.write.request.size", abfsWriteRequestSize)
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.codegen.wholeStage", codegenEnabled)
      .config("spark.databricks.delta.properties.defaults.checkpointInterval", "10000000")
      .config("spark.hadoop.fs.azure.enable.flush", "false")
      .config("spark.hadoop.fs.azure.disable.outputstream.flush", "true")
      .config("spark.hadoop.fs.azure.write.max.concurrent.requests", abfsWriteMaxConcurrent)
      .config("spark.kafka.consumer.cache.capacity", consumerCacheCapacity)
      .getOrCreate()

    val schema = new StructType()
      .add("ts", StringType)
      .add("producer_id", IntegerType)
      .add("seq", LongType)

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit =
        println(s"[TELEMETRY] Query started: id=${event.id}")
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val p = event.progress
        val d = p.durationMs
        println(f"[TELEMETRY] Batch ${p.batchId}: records=${p.numInputRows}, " +
          f"processedRows/s=${p.processedRowsPerSecond}%.0f, " +
          f"triggerMs=${d.get("triggerExecution")}, " +
          f"latestOffsetMs=${d.get("latestOffset")}, " +
          f"getBatchMs=${d.get("getBatch")}, " +
          f"addBatchMs=${d.get("addBatch")}, " +
          f"commitMs=${d.get("commitOffsets")}, " +
          f"walCommitMs=${d.get("walCommit")}")
      }
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit =
        println(s"[TELEMETRY] Query terminated: id=${event.id}")
    })

    println(s"Writing to: $deltaPath")

    val healthPort = sys.env.getOrElse("HEALTH_PORT", "8080").toInt
    val deltaTableReady = new AtomicBoolean(false)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val deltaLogPath = new Path(s"$deltaPath/_delta_log")

    val healthThread = new Thread("health-probe") {
      override def run(): Unit = {
        val server = new java.net.ServerSocket(healthPort)
        server.setReuseAddress(true)
        println(s"Health probe listening on port $healthPort")
        while (!isInterrupted) {
          try {
            val client = server.accept()
            try {
              val in = new java.io.BufferedReader(new java.io.InputStreamReader(client.getInputStream))
              var line = in.readLine()
              while (line != null && !line.isEmpty) line = in.readLine()

              if (!deltaTableReady.get()) {
                val fs = deltaLogPath.getFileSystem(hadoopConf)
                deltaTableReady.set(fs.exists(deltaLogPath))
              }

              val (status, body) = if (deltaTableReady.get())
                ("200 OK", "ok")
              else
                ("503 Service Unavailable", "waiting for delta table")
              val bodyBytes = body.getBytes("UTF-8")
              val header = s"HTTP/1.1 $status\r\nContent-Length: ${bodyBytes.length}\r\nConnection: close\r\n\r\n"
              val os = client.getOutputStream
              os.write(header.getBytes("UTF-8"))
              os.write(bodyBytes)
              os.flush()
            } finally {
              client.close()
            }
          } catch {
            case _: Exception =>
          }
        }
      }
    }
    healthThread.setDaemon(true)
    healthThread.start()

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", jaasConfig)
      .option("subscribe", eventHubName)
      .option("kafka.group.id", consumerGroup)
      .option("startingOffsets", "latest")
      .option("kafka.request.timeout.ms", kafkaRequestTimeoutMs)
      .option("kafka.session.timeout.ms", kafkaSessionTimeoutMs)
      .option("kafka.max.poll.records", kafkaMaxPollRecords)
      .option("kafka.fetch.max.bytes", kafkaFetchMaxBytes)
      .option("kafka.max.partition.fetch.bytes", kafkaPartFetchMaxBytes)
      .option("kafka.receive.buffer.bytes", kafkaReceiveBufferBytes)
      .option("kafka.fetch.min.bytes", kafkaFetchMinBytes)
      .option("kafka.fetch.max.wait.ms", kafkaFetchMaxWaitMs)
      .option("minPartitions", kafkaMinPartitions)
      .option("kafkaConsumer.pollTimeoutMs", pollTimeoutMs)
      .option("fetchOffset.numRetries", fetchOffsetRetries)
      .option("fetchOffset.retryIntervalMs", fetchOffsetRetryMs)
      .option("failOnDataLoss", failOnDataLoss)
      .option("minOffsetsPerTrigger", minOffsetsPerTrigger)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .option("maxTriggerDelay", maxTriggerDelay)
      .load()

    val parsed = kafkaDF
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), schema).as("data"))
      .select(col("data.ts").as("ts_str"))
      .withColumn("ts", to_timestamp(col("ts_str")))
      .select("ts")

    parsed.coalesce(outputCoalescePartitions).writeStream
      .format("delta")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .option("checkpointLocation", checkpointPath)
      .start(deltaPath)
      .awaitTermination()
  }
}
