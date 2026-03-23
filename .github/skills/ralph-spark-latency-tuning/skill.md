---
name: ralph-spark-latency-tuning
description: "Iteratively tune Spark Structured Streaming pipeline until end-to-end latency stays below 5 seconds while keeping up with producer throughput and writing to ADLS Gen2."
user-invocable: true
---

# Spark Latency Tuning Loop

Iterative tuning loop to achieve **< 5 second sustained latency** in Apache Spark Structured Streaming.

---

## CRITICAL: Rules & Constraints

**NEVER `git add` or `git commit`** — all changes will be reviewed, committed and pushed by a human.

**Hard constraints (ALL must be met simultaneously):**

1. **Latency < 5 seconds** — every data point in `results/latency_timeseries.csv` must be below 5s. Never exceed.
2. **Keep up with producer** — the consumer must process messages at least as fast as the producer emits them. Latency must not trend upward over time.
3. **Must write to ADLS Gen2** — Delta Lake writes to ADLS must remain functional. Do not disable or bypass the sink (e.g. by writing locally first).
4. **Must not introduce changes that causes Spark Shuffles or slowdowns** — e.g. `repartition` force shuffles which slows down the microbatch.

**Allowed files — you may ONLY modify these two files:**

1. `docker-compose.yml` — tuning knobs in `x-spark-tuning`, per-service env vars, container resources (`cpus`, `mem_limit`)
2. `src/containers/spark-consumer/src/main/scala/benchmark/StreamConsumer.scala` — Spark/Kafka/Delta configuration

**No magic numbers in Scala** — if you tune a hardcoded value in `StreamConsumer.scala`, you MUST externalize it as an environment variable (read via `sys.env.getOrElse("VAR_NAME", "default")`) and place the default in `docker-compose.yml` under `x-spark-tuning`.

**NEVER emit `{ "status": "Succeeded" }` unless ALL constraints are met.** If latency exceeds 5s anywhere in the CSV, the job is not done — go back and tune further.

---

## The Job

Run the benchmark, analyze the latency timeseries, tune Spark streaming parameters, and iterate until the sawtooth pattern is eliminated and all latency readings stay below 5 seconds.

---

## Context

- **Project root**: `/home/mdrrahman/stream-processing-benchmark`
- **Benchmark script**: `./src/.scripts/build-and-run.sh 120` (runs both Spark 3.5 and 4.2 consumers for 120s each)
- **Results**: `results/latency_timeseries.csv` (columns: `elapsed_s`, `latency_s`, `version`)
- **Latency chart**: `results/latency_chart.png`

### Tuning Knobs

Current defaults in `docker-compose.yml` (`x-spark-tuning` anchor):

| Variable                          | Default            | Purpose                          |
| --------------------------------- | ------------------ | -------------------------------- |
| `SPARK_SHUFFLE_PARTITIONS`        | `4`                | Shuffle partitions for Spark SQL |
| `KAFKA_MAX_POLL_RECORDS`          | `10000`            | Max records per Kafka poll       |
| `KAFKA_FETCH_MAX_BYTES`           | `52428800` (50 MB) | Max total fetch size             |
| `KAFKA_MAX_PARTITION_FETCH_BYTES` | `10485760` (10 MB) | Max bytes per partition fetch    |
| `KAFKA_MIN_PARTITIONS`            | `32`               | Min Kafka source partitions      |
| `ABFS_WRITE_REQUEST_SIZE`         | `8388608` (8 MB)   | ABFS write buffer size           |

Per-service settings:

| Variable           | Default     | Purpose                      |
| ------------------ | ----------- | ---------------------------- |
| `TRIGGER_INTERVAL` | `0 seconds` | Micro-batch trigger interval |
| `cpus`             | `12`        | Container CPU allocation     |
| `mem_limit`        | `48g`       | Container memory limit       |

Currently hardcoded in `StreamConsumer.scala` (candidates for externalization):

| Value                                 | Current  | Location             |
| ------------------------------------- | -------- | -------------------- |
| `kafka.request.timeout.ms`            | `60000`  | Kafka reader options |
| `kafka.session.timeout.ms`            | `30000`  | Kafka reader options |
| `startingOffsets`                     | `latest` | Kafka reader options |
| `spark.sql.parquet.compression.codec` | `snappy` | SparkSession config  |

You are allowed to change existing values and or introduce new values.
You are also allowed to write code that emits telemetry (e.g. to console via Spark Listeners) to identify bottlenecks. 
You are encouraged to aggressively pre-fetch from Kafka, since the consumer produces about 200K msgs/second.

---

## Step 0: Baseline Run

Run the benchmark to capture current latency data:

```bash
cd /home/mdrrahman/stream-processing-benchmark
./src/.scripts/build-and-run.sh 120
```

Then examine the results:

```bash
cat results/latency_timeseries.csv
```

Record the baseline:

- **Max latency** observed (per version)
- **Sawtooth pattern** — does latency spike and recover cyclically?
- **Trend** — is latency trending up (falling behind) or stable?

If the baseline already meets all constraints (all readings < 5s, no upward trend), skip to Step 4.

---

## Step 1: Analyze the latency

Study the CSV data to understand the root cause:

1. **Identify the cycle** — how many seconds between latency spikes?
2. **Identify the peak** — what is the maximum latency reached?
3. **Identify the trough** — the consumer can clearly catch up (latency drops to < 1s), so throughput capacity exists
4. **Hypothesize** — the sawtooth typically means micro-batch overhead (ADLS write flush, checkpoint, or shuffle) causes a backlog that the consumer then drains

---

## Step 2: Tune Parameters

Based on Step 1 analysis, modify `docker-compose.yml` and/or `StreamConsumer.scala`.

**Remember:** Any new tuning parameter in Scala must be externalized:

```scala
// In StreamConsumer.scala — read from env with fallback
val myParam = sys.env.getOrElse("MY_PARAM", "default_value")
```

```yaml
# In docker-compose.yml — add to x-spark-tuning anchor
x-spark-tuning: &spark-tuning
  MY_PARAM: "tuned_value"
```

---

## Step 3: Re-run Benchmark & Validate

```bash
cd /home/mdrrahman/stream-processing-benchmark
./src/.scripts/build-and-run.sh 120
```

After the run completes, validate ALL constraints:

```bash
cat results/latency_timeseries.csv
```

**Check each constraint:**

1. **Max latency < 5s**: Scan the `latency_s` column — every single value must be below 5.
2. **No upward trend**: Latency should be stable or decreasing over time, not climbing.
3. **ADLS writes functional**: The benchmark completed without errors and results were produced.

If ANY constraint fails, return to Step 2 with the new data and iterate.

---

## Step 4: Completion Signal

**CRITICAL: You MUST emit exactly one of these JSON objects as the absolute last line of your output.**

If ALL constraints are met (every latency reading < 5s, no upward trend, ADLS writes working):

```
{ "status": "Succeeded" }
```

If you were unable to meet all constraints after exhausting your approaches:

```
{ "status": "Failed" }
```

**The Ralph loop script parses your final output for this signal.** If neither is found, the loop assumes the task is incomplete and will re-invoke you for another iteration. Always emit a status.
