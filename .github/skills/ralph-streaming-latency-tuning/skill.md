---
name: ralph-streaming-latency-tuning
description: "Iteratively tune Spark Structured Streaming, Flink DataStream, and Feldera IVM pipelines until end-to-end latency stays below 5 seconds while keeping up with producer throughput and writing to ADLS Gen2."
user-invocable: true
---

# Streaming Latency Tuning Loop

Iterative tuning loop to achieve **< 5 second sustained latency** in Apache Spark Structured Streaming, Apache Flink DataStream, **and** Feldera IVM.

---

## CRITICAL: Rules & Constraints

**NEVER `git add` or `git commit`** — all changes will be reviewed, committed and pushed by a human.

**Hard constraints (ALL must be met simultaneously for every consumer — Spark 3.5, Spark 4.2, Flink 1.16, and Feldera):**

1. **Sustained latency < 5 seconds** — every data point in `results/latency_timeseries.csv` must be below 5s. Never exceed.
2. **Keep up with producer** — the consumer must process messages at least as fast as the producer emits them. Latency must not trend upward over time.
3. **Must write to ADLS Gen2** — Delta Lake writes to ADLS must remain functional. Do not disable or bypass the sink (e.g. by writing locally first).
4. **Must not introduce changes that cause shuffles or slowdowns** — e.g. Spark `repartition` forces shuffles; Flink `rebalance()` or unnecessary `keyBy()` can cause similar issues.

**Allowed files — you may ONLY modify these files:**

1. `docker-compose.yml` — tuning knobs in `x-spark-tuning` and `x-flink-tuning`, per-service env vars, container resources (`cpus`, `mem_limit`)
2. `src/containers/spark-consumer/src/main/scala/benchmark/StreamConsumer.scala` — Spark/Kafka/Delta configuration
3. `src/containers/flink-consumer/src/main/java/benchmark/FlinkStreamConsumer.java` — Flink/Kafka/Delta configuration
4. `src/.scripts/feldera-pipeline.py` — Feldera pipeline SQL program, runtime config, connector config

**No magic numbers in application code** — if you tune a hardcoded value in `StreamConsumer.scala` or `FlinkStreamConsumer.java`, you MUST externalize it as an environment variable (Scala: `sys.env.getOrElse("VAR_NAME", "default")`; Java: `env("VAR_NAME", "default")`) and place the default in `docker-compose.yml` under `x-spark-tuning` or `x-flink-tuning` respectively.

**NEVER emit `{ "status": "Succeeded" }` unless ALL constraints are met for ALL consumers.** If latency exceeds 5s anywhere in the CSV for any version, the job is not done — go back and tune further.

---

## The Job

Run the benchmark, analyze the latency timeseries for all three consumers (Spark 3.5, Spark 4.2, Flink 1.16), tune streaming parameters, and iterate until the sawtooth pattern is eliminated and all latency readings stay below 5 seconds across every consumer.

---

## Context

- **Project root**: `/home/mdrrahman/stream-processing-benchmark`
- **Benchmark script**: `./src/.scripts/build-and-run.sh 120` (runs Spark 3.5, Spark 4.2, and Flink 1.16 consumers for 120s each)
- **Results**: `results/latency_timeseries.csv` (columns: `elapsed_s`, `latency_s`, `version`)
- **Latency chart**: `results/latency_chart.png`

### Spark Tuning Knobs

* You are **encouraged** to make the settings as aggressive as possible to pull out as much data out of Kafka as fast as possible - see:
  - Docs: https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html
  - Code: https://github.com/apache/spark/tree/master/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010

  > Clone Spark locally here if not already cloned: `/home/mdrrahman/stream-processing-benchmark/.temp/spark`

### Flink Tuning Knobs

* Flink uses a continuous processing model (not micro-batches like Spark), so latency is driven by checkpoint overhead, Kafka consumer throughput, and Delta Lake sink flush time.
* You are **encouraged** to make the Kafka consumer settings as aggressive as possible — see:
  - Flink Kafka Connector docs: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/datastream/kafka/
  - Flink Configuration docs: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/
  - Flink Checkpointing docs: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/ops/state/checkpoints/
  - Delta Flink Connector: https://docs.delta.io/latest/delta-flink.html

  > Clone Flink locally here if not already cloned: `/home/mdrrahman/stream-processing-benchmark/.temp/flink`

### Feldera Tuning Knobs

* Feldera uses Incremental View Maintenance (IVM) — it processes input changes incrementally rather than reprocessing entire datasets. Latency is driven by Kafka ingestion speed, IVM engine step time, and Delta Lake output flushing.
* The Feldera pipeline configuration lives in `src/.scripts/feldera-pipeline.py`. Edit the `runtime_config` dict and connector configs there.
* Key tuning parameters (in `feldera-pipeline.py`):
  - `runtime_config.workers` — Number of DBSP worker threads (default 16, sweet spot 4-16)
  - `runtime_config.min_batch_size_records` — Minimum records before processing a batch (default 0)
  - `runtime_config.max_buffering_delay_usecs` — Max delay waiting for batch fill (default 0)
  - `max_output_buffer_time_millis` — Delta Lake output flush interval in ms (default 1000)
  - Kafka connector `poller_threads` — Number of parallel Kafka poll threads (default 3)
* You are **encouraged** to tune these aggressively for maximum throughput and minimum latency.
* Feldera docs: https://docs.feldera.com/
  - Runtime config: https://docs.feldera.com/api/list-pipelines (runtime_config section)
  - Kafka connector: https://docs.feldera.com/connectors/sources/kafka
  - Delta Lake sink: https://docs.feldera.com/connectors/sinks/delta

### General Guidance (applies to both Spark and Flink)

* You are **allowed** to change existing values and or introduce new values.
* You are **encouraged** to aggressively pre-fetch from Kafka, since the producer produces about 200K msgs/second.
* You are **encouraged** to write code that emits telemetry (e.g. Spark Listeners, Flink metrics/logging) to identify bottlenecks.

  > Container logs are stored here after each run for you to examine: `/home/mdrrahman/stream-processing-benchmark/.logs`
  > The general idea is, we must get our consumer to consume events FASTER than the producer can produce it.

* Make small changes. If a change has improvement, make a note of it and keep it. If a change causes performance regressions, revert it.

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

Record the baseline for each consumer version (`Spark 3.5`, `Spark 4.2`, `Flink 1.16`, `Feldera`):

- **Max latency** observed (per version)
- **Sawtooth pattern** — does latency spike and recover cyclically?
- **Trend** — is latency trending up (falling behind) or stable?

If the baseline already meets all constraints for all versions (all readings < 5s, no upward trend), skip to Step 4.

---

## Step 1: Analyze the latency

Study the CSV data **per version** to understand the root cause:

1. **Identify the cycle** — how many seconds between latency spikes?
2. **Identify the peak** — what is the maximum latency reached?
3. **Identify the trough** — the consumer can clearly catch up (latency drops to < 1s), so throughput capacity exists
4. **Hypothesize** — for Spark, the sawtooth typically means micro-batch overhead (ADLS write flush, checkpoint, or shuffle) causes a backlog that the consumer then drains. For Flink, spikes typically correlate with checkpoint barriers or Delta sink commits.

---

## Step 2: Tune Parameters

Based on Step 1 analysis, modify `docker-compose.yml` and/or the appropriate consumer source files.

**Remember:** Any new tuning parameter must be externalized:

For Spark (`StreamConsumer.scala`):
```scala
// Read from env with fallback
val myParam = sys.env.getOrElse("MY_PARAM", "default_value")
```

```yaml
# In docker-compose.yml — add to x-spark-tuning anchor
x-spark-tuning: &spark-tuning
  MY_PARAM: "tuned_value"
```

For Flink (`FlinkStreamConsumer.java`):
```java
// Read from env with fallback
String myParam = env("MY_PARAM", "default_value");
```

```yaml
# In docker-compose.yml — add to x-flink-tuning anchor
x-flink-tuning: &flink-tuning
  MY_PARAM: "tuned_value"
```

---

## Step 3: Re-run Benchmark & Validate

```bash
cd /home/mdrrahman/stream-processing-benchmark
./src/.scripts/build-and-run.sh 120
```

After the run completes, validate ALL constraints **for every version**:

```bash
cat results/latency_timeseries.csv
```

**Check each constraint (for Spark 3.5, Spark 4.2, Flink 1.16, AND Feldera):**

1. **Max latency < 5s**: Scan the `latency_s` column — every single value must be below 5, for every version.
2. **No upward trend**: Latency should be stable or decreasing over time, not climbing.
3. **ADLS writes functional**: The benchmark completed without errors and results were produced.

If ANY constraint fails for ANY version, return to Step 2 with the new data and iterate.

---

## Step 4: Completion Signal

**CRITICAL: You MUST emit exactly one of these JSON objects as the absolute last line of your output.**

If ALL constraints are met for ALL consumers including Feldera (every latency reading < 5s, no upward trend, ADLS writes working):

```
{ "status": "Succeeded" }
```

If you were unable to meet all constraints after exhausting your approaches:

```
{ "status": "Failed" }
```

**The Ralph loop script parses your final output for this signal.** If neither is found, the loop assumes the task is incomplete and will re-invoke you for another iteration. Always emit a status.
