package benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

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
    val kafkaMaxPollRecords      = sys.env.getOrElse("KAFKA_MAX_POLL_RECORDS", "50000")
    val kafkaFetchMaxBytes       = sys.env.getOrElse("KAFKA_FETCH_MAX_BYTES", "104857600")
    val kafkaPartFetchMaxBytes   = sys.env.getOrElse("KAFKA_MAX_PARTITION_FETCH_BYTES", "20971520")
    val kafkaMinPartitions       = sys.env.getOrElse("KAFKA_MIN_PARTITIONS", "12")
    val abfsWriteRequestSize     = sys.env.getOrElse("ABFS_WRITE_REQUEST_SIZE", "8388608")
    val codegenEnabled           = sys.env.getOrElse("SPARK_CODEGEN_ENABLED", "true")
    val maxOffsetsPerTrigger     = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000000")
    val minOffsetsPerTrigger     = sys.env.getOrElse("MIN_OFFSETS_PER_TRIGGER", "500000")
    val maxTriggerDelay          = sys.env.getOrElse("MAX_TRIGGER_DELAY", "15s")
    val pollTimeoutMs            = sys.env.getOrElse("KAFKA_CONSUMER_POLL_TIMEOUT_MS", "30000")
    val fetchOffsetRetries       = sys.env.getOrElse("KAFKA_FETCH_OFFSET_NUM_RETRIES", "5")
    val fetchOffsetRetryMs       = sys.env.getOrElse("KAFKA_FETCH_OFFSET_RETRY_INTERVAL_MS", "10")
    val failOnDataLoss           = sys.env.getOrElse("FAIL_ON_DATA_LOSS", "false")
    val outputCoalescePartitions = sys.env.getOrElse("OUTPUT_COALESCE_PARTITIONS", "4").toInt
    val abfsWriteMaxConcurrent   = sys.env.getOrElse("ABFS_WRITE_MAX_CONCURRENT", "24")

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
      .config("spark.hadoop.fs.azure.enable.flush", "false")
      .config("spark.hadoop.fs.azure.disable.outputstream.flush", "true")
      .config("spark.hadoop.fs.azure.write.max.concurrent.requests", abfsWriteMaxConcurrent)
      .getOrCreate()

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit =
        println(s"[TELEMETRY] Query started: id=${event.id}")
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val p = event.progress
        val d = p.durationMs
        println(f"[TELEMETRY] Batch ${p.batchId}: records=${p.numInputRows}, " +
          f"processedRows/s=${p.processedRowsPerSecond}%.0f, " +
          f"triggerMs=${d.get("triggerExecution")}, " +
          f"addBatchMs=${d.get("addBatch")}, " +
          f"commitMs=${d.get("commitOffsets")}")
      }
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit =
        println(s"[TELEMETRY] Query terminated: id=${event.id}")
    })

    println(s"Writing to: $deltaPath")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", jaasConfig)
      .option("subscribe", eventHubName)
      .option("kafka.group.id", consumerGroup)
      .option("startingOffsets", "latest")
      .option("kafka.request.timeout.ms", "60000")
      .option("kafka.session.timeout.ms", "30000")
      .option("kafka.max.poll.records", kafkaMaxPollRecords)
      .option("kafka.fetch.max.bytes", kafkaFetchMaxBytes)
      .option("kafka.max.partition.fetch.bytes", kafkaPartFetchMaxBytes)
      .option("minPartitions", kafkaMinPartitions)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .option("minOffsetsPerTrigger", minOffsetsPerTrigger)
      .option("maxTriggerDelay", maxTriggerDelay)
      .option("kafkaConsumer.pollTimeoutMs", pollTimeoutMs)
      .option("fetchOffset.numRetries", fetchOffsetRetries)
      .option("fetchOffset.retryIntervalMs", fetchOffsetRetryMs)
      .option("failOnDataLoss", failOnDataLoss)
      .load()

    val schema = new StructType()
      .add("ts", StringType)
      .add("producer_id", IntegerType)
      .add("seq", LongType)

    val parsed = kafkaDF
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), schema).as("data"))
      .select("data.*")
      .withColumn("ts", to_timestamp(col("ts")))
      .withColumn("adls_ingest_time", current_timestamp())
      .withColumn("latency_ms", ((col("adls_ingest_time").cast("double") - col("ts").cast("double")) * 1000).cast("long"))

    parsed.coalesce(outputCoalescePartitions).writeStream
      .format("delta")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .option("checkpointLocation", checkpointPath)
      .start(deltaPath)
      .awaitTermination()
  }
}
