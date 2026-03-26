import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

RESULTS_DIR = "/opt/results"

CONTAINER_LABELS = {
    "producer-csharp": "Producer (C#)",
    "spark-consumer-35": "Spark 3.5",
    "spark-consumer-42": "Spark 4.2",
    "flink-consumer-116": "Flink 1.16",
    "feldera-consumer": "Feldera",
}

CONTAINER_COLORS = {
    "producer-csharp": "orange",
    "spark-consumer-35": "blue",
    "spark-consumer-42": "red",
    "flink-consumer-116": "green",
    "feldera-consumer": "purple",
}


def create_spark_session():
    account_name = os.environ["ADLSG2_ACCOUNT_NAME"]
    account_key = os.environ["ADLSG2_ACCOUNT_KEY"]
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "64g")

    builder = (
        SparkSession.builder
        .appName("BenchmarkAnalytics")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"spark.hadoop.fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)
        .config("spark.sql.debug.maxToStringFields", "200")
    )
    return configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-azure:3.3.4"]).getOrCreate()


def build_commit_timestamp_map(spark, delta_path, storage_options):
    """Build a mapping from parquet file name to Delta commit timestamp.

    Reads _delta_log/*.json directly via fsspec/adlfs, parsing each JSONL log file 
    for commitInfo.timestamp and add.path entries.
    """
    import json
    import fsspec
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    import pandas as pd

    # Convert abfs:// path to container/prefix for adlfs
    # abfs://container@account.dfs.core.windows.net/path → container, path/_delta_log
    stripped = delta_path.replace("abfs://", "").replace("abfss://", "")
    container_and_account, *rest = stripped.split("/", 1)
    container_name = container_and_account.split("@")[0]
    prefix = rest[0] if rest else ""
    log_prefix = f"{prefix}/_delta_log"

    fs = fsspec.filesystem(
        "abfs",
        account_name=storage_options["account_name"],
        account_key=storage_options["account_key"],
    )

    log_files = sorted(
        f for f in fs.ls(f"{container_name}/{log_prefix}", detail=False)
        if f.endswith(".json")
    )

    rows = []
    for log_file in log_files:
        commit_ts = None
        files_added = []
        with fs.open(log_file, "r") as fh:
            for line in fh:
                entry = json.loads(line)
                if "commitInfo" in entry:
                    ms = entry["commitInfo"]["timestamp"]
                    commit_ts = pd.Timestamp(ms, unit="ms", tz="UTC").tz_localize(None)
                if "add" in entry:
                    path = entry["add"]["path"]
                    files_added.append(path.rsplit("/", 1)[-1])
        for f in files_added:
            rows.append({"file_name": f, "commit_ts": commit_ts})

    if not rows:
        schema = StructType([
            StructField("file_name", StringType()),
            StructField("commit_ts", TimestampType()),
        ])
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(pd.DataFrame(rows))


def load_timeseries(spark, delta_path, label, storage_options):
    """Read Delta table, compute latency from transaction log commit timestamps.

    For each row, latency = delta_commit_timestamp - producer_send_timestamp.
    This is uniform across all consumers (Spark, Flink, Feldera).
    """
    from pyspark.sql import functions as F

    try:
        df = spark.read.format("delta").load(delta_path)
        if not df.head(1):
            print(f"WARNING: {label} Delta table is empty, skipping.")
            return None
    except Exception as e:
        print(f"WARNING: Could not read {label} at {delta_path}: {e}")
        return None

    # Handle ts as either TIMESTAMP or VARCHAR
    ts_field = [f for f in df.schema.fields if f.name == "ts"]
    if ts_field and ts_field[0].dataType.typeName() == "string":
        df = df.withColumn("ts", F.to_timestamp(F.col("ts")))

    # Select ts and the parquet file name from metadata
    df = df.select(
        F.col("ts"),
        F.col("_metadata.file_name").alias("file_name"),
    )

    # Build commit timestamp map from the Delta transaction log
    commit_map = build_commit_timestamp_map(spark, delta_path, storage_options)

    # Join data rows with their commit timestamps (broadcast the small map)
    df = df.join(F.broadcast(commit_map), "file_name")

    # Compute latency: time from producer send to Delta commit
    df = df.withColumn(
        "latency_ms",
        ((F.col("commit_ts").cast("double") - F.col("ts").cast("double")) * 1000).cast("long"),
    )

    min_ts = df.agg(F.min("ts")).collect()[0][0]
    binned = (
        df.withColumn(
            "elapsed_s",
            F.floor(
                (F.col("ts").cast("double") - F.lit(min_ts).cast("double"))
            ).cast("int"),
        )
        .withColumn("latency_s", F.col("latency_ms") / 1000.0)
        .groupBy("elapsed_s")
        .agg(F.avg("latency_s").alias("latency_s"), F.count("*").alias("msg_count"))
        .orderBy("elapsed_s")
    )

    pdf = binned.toPandas()
    pdf["version"] = label
    return pdf


def load_resource_stats(spark, abfs_base, resource_path):
    """Read resource stats CSV from ADLS, return Pandas DataFrame."""
    full_path = f"{abfs_base}/{resource_path}"
    expected_cols = {"timestamp", "container", "cpu_pct", "mem_mb", "net_in_mb", "net_out_mb"}
    try:
        df = spark.read.csv(full_path, header=True, inferSchema=True)
        if not df.head(1):
            print(f"WARNING: Resource stats CSV is empty at {full_path}")
            return None
        actual_cols = set(df.columns)
        missing = expected_cols - actual_cols
        if missing:
            print(f"WARNING: Resource stats CSV is missing columns {missing}. "
                  f"Actual columns: {df.columns}. "
                  f"The CSV may be missing its header row.")
            return None
        pdf = df.toPandas()
        pdf = pdf[~pdf["container"].str.contains("analytics-pyspark", na=False)]
        pdf["elapsed_s"] = pdf.groupby("container")["timestamp"].transform(
            lambda x: (x - x.min()).round().astype(int)
        )
        return pdf
    except Exception as e:
        print(f"WARNING: Could not read resource stats at {full_path}: {e}")
        return None


def main():
    spark = create_spark_session()

    account_name = os.environ["ADLSG2_ACCOUNT_NAME"]
    account_key = os.environ["ADLSG2_ACCOUNT_KEY"]
    container = os.environ["ADLSG2_CONTAINER"]
    abfs_base = f"abfs://{container}@{account_name}.dfs.core.windows.net"

    storage_options = {
        "account_name": account_name,
        "account_key": account_key,
    }

    path_35      = os.environ.get("DELTA_TABLE_PATH_35", "spark35/benchmark")
    path_42      = os.environ.get("DELTA_TABLE_PATH_42", "spark42/benchmark")
    path_flink116 = os.environ.get("DELTA_TABLE_PATH_FLINK116", "flink1.16/benchmark")
    resource_path = os.environ.get("RESOURCE_STATS_PATH", "resource_stats/resource_stats.csv")
    path_feldera  = os.environ.get("DELTA_TABLE_PATH_FELDERA", "feldera/benchmark")

    os.makedirs(RESULTS_DIR, exist_ok=True)

    series_35       = load_timeseries(spark, f"{abfs_base}/{path_35}", "Spark 3.5", storage_options)
    series_42       = load_timeseries(spark, f"{abfs_base}/{path_42}", "Spark 4.2", storage_options)
    series_flink116 = load_timeseries(spark, f"{abfs_base}/{path_flink116}", "Flink 1.16", storage_options)
    series_feldera  = load_timeseries(spark, f"{abfs_base}/{path_feldera}", "Feldera", storage_options)
    resource_stats  = load_resource_stats(spark, abfs_base, resource_path)
    spark.stop()

    all_series = [series_35, series_42, series_flink116, series_feldera]
    if all(s is None for s in all_series):
        print("ERROR: No data available. Exiting.")
        sys.exit(1)

    combined = pd.concat([s for s in all_series if s is not None], ignore_index=True)
    csv_path = os.path.join(RESULTS_DIR, "latency_timeseries.csv")
    combined.to_csv(csv_path, index=False)
    print(f"Time series saved to {csv_path}")

    has_resources = resource_stats is not None and not resource_stats.empty
    num_panels = 6 if has_resources else 2
    fig, axes = plt.subplots(num_panels, 1, figsize=(14, 5 * num_panels), sharex=False)
    if num_panels == 2:
        ax_lat, ax_thr = axes
    else:
        ax_lat, ax_thr, ax_cpu, ax_mem, ax_net_in, ax_net_out = axes

    # ── Latency panel ─────────────────────────────────────────────
    for series, color in [(series_35, "blue"), (series_42, "red"), (series_flink116, "green"), (series_feldera, "purple")]:
        if series is None:
            continue
        label = series["version"].iloc[0]
        ax_lat.plot(series["elapsed_s"], series["latency_s"], color=color, label=label, linewidth=1.8, alpha=0.75)
        avg_lat = series["latency_s"].mean()
        ax_lat.axhline(y=avg_lat, color=color, linestyle="--", alpha=0.10, linewidth=2)
        ax_lat.text(series["elapsed_s"].iloc[-1], avg_lat, f" {label} avg: {avg_lat:.2f}s",
                    color=color, alpha=0.6, fontsize=9, va="bottom")

        ax_thr.plot(series["elapsed_s"], series["msg_count"], color=color, label=label, linewidth=1.8, alpha=0.75)
        avg_thr = series["msg_count"].mean()
        ax_thr.axhline(y=avg_thr, color=color, linestyle="--", alpha=0.10, linewidth=2)
        ax_thr.text(series["elapsed_s"].iloc[-1], avg_thr, f" {label} avg: {avg_thr:,.0f}",
                    color=color, alpha=0.6, fontsize=9, va="bottom")

    ax_lat.set_ylabel("Latency (s)")
    ax_lat.set_title("E2E Latency: Spark 3.5 vs Spark 4.2 vs Flink 1.16 vs Feldera")
    ax_lat.legend(fontsize=12)
    ax_lat.grid(True, alpha=0.3)

    ax_thr.set_xlabel("Elapsed Time (seconds since first message)")
    ax_thr.set_ylabel("Messages / second")
    ax_thr.set_title("Throughput: Spark 3.5 vs Spark 4.2 vs Flink 1.16 vs Feldera")
    ax_thr.legend(fontsize=12)
    ax_thr.grid(True, alpha=0.3)

    # ── Resource panels ───────────────────────────────────────────
    if has_resources:
        for cname in resource_stats["container"].unique():
            cdata = resource_stats[resource_stats["container"] == cname].sort_values("elapsed_s")
            label = CONTAINER_LABELS.get(cname, cname)
            color = CONTAINER_COLORS.get(cname, "gray")

            ax_cpu.plot(cdata["elapsed_s"], cdata["cpu_pct"], color=color, label=label, linewidth=1.5, alpha=0.75)
            ax_mem.plot(cdata["elapsed_s"], cdata["mem_mb"], color=color, label=label, linewidth=1.5, alpha=0.75)
            ax_net_in.plot(cdata["elapsed_s"], cdata["net_in_mb"], color=color, label=label, linewidth=1.5, alpha=0.75)
            ax_net_out.plot(cdata["elapsed_s"], cdata["net_out_mb"], color=color, label=label, linewidth=1.5, alpha=0.75)

        ax_cpu.set_ylabel("CPU %")
        ax_cpu.set_title("CPU Usage per Container")
        ax_cpu.legend(fontsize=10)
        ax_cpu.grid(True, alpha=0.3)

        ax_mem.set_ylabel("Memory (MB)")
        ax_mem.set_title("Memory Usage per Container")
        ax_mem.legend(fontsize=10)
        ax_mem.grid(True, alpha=0.3)

        ax_net_in.set_ylabel("Network In (MB)")
        ax_net_in.set_title("Cumulative Network In per Container")
        ax_net_in.legend(fontsize=10)
        ax_net_in.grid(True, alpha=0.3)

        ax_net_out.set_xlabel("Elapsed Time (seconds)")
        ax_net_out.set_ylabel("Network Out (MB)")
        ax_net_out.set_title("Cumulative Network Out per Container")
        ax_net_out.legend(fontsize=10)
        ax_net_out.grid(True, alpha=0.3)

    fig.tight_layout()

    chart_path = os.path.join(RESULTS_DIR, "latency_chart.png")
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)
    print(f"Chart saved to {chart_path}")


if __name__ == "__main__":
    main()
