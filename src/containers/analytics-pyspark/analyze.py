import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

RESULTS_DIR = "/opt/results"


def create_spark_session():
    account_name = os.environ["ADLSG2_ACCOUNT_NAME"]
    account_key = os.environ["ADLSG2_ACCOUNT_KEY"]

    builder = (
        SparkSession.builder
        .appName("BenchmarkAnalytics")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"spark.hadoop.fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)
    )
    return configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-azure:3.3.4"]).getOrCreate()


def load_timeseries(spark, delta_path, label):
    """Read Delta table, aggregate to 1-second bins in Spark, return Pandas."""
    from pyspark.sql import functions as F

    try:
        df = spark.read.format("delta").load(delta_path)
        if not df.head(1):
            print(f"WARNING: {label} Delta table is empty, skipping.")
            return None
    except Exception as e:
        print(f"WARNING: Could not read {label} at {delta_path}: {e}")
        return None

    min_ts = df.agg(F.min("ts")).collect()[0][0]
    binned = (
        df.withColumn("elapsed_s",
            F.floor((F.col("ts").cast("double") - F.lit(min_ts).cast("double"))).cast("int"))
        .withColumn("latency_s", F.col("latency_ms") / 1000.0)
        .groupBy("elapsed_s")
        .agg(F.avg("latency_s").alias("latency_s"), F.count("*").alias("msg_count"))
        .orderBy("elapsed_s")
    )

    pdf = binned.toPandas()
    pdf["version"] = label
    return pdf


def main():
    spark = create_spark_session()

    account_name = os.environ["ADLSG2_ACCOUNT_NAME"]
    container = os.environ["ADLSG2_CONTAINER"]
    abfs_base = f"abfs://{container}@{account_name}.dfs.core.windows.net"

    path_35 = os.environ.get("DELTA_TABLE_PATH_35", "spark35/benchmark")
    path_42 = os.environ.get("DELTA_TABLE_PATH_42", "spark42/benchmark")

    os.makedirs(RESULTS_DIR, exist_ok=True)

    series_35 = load_timeseries(spark, f"{abfs_base}/{path_35}", "Spark 3.5")
    series_42 = load_timeseries(spark, f"{abfs_base}/{path_42}", "Spark 4.2")
    spark.stop()

    if series_35 is None and series_42 is None:
        print("ERROR: No data available. Exiting.")
        sys.exit(1)

    combined = pd.concat([s for s in [series_35, series_42] if s is not None], ignore_index=True)
    csv_path = os.path.join(RESULTS_DIR, "latency_timeseries.csv")
    combined.to_csv(csv_path, index=False)
    print(f"Time series saved to {csv_path}")

    fig, (ax_lat, ax_thr) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    for series, color in [(series_35, "blue"), (series_42, "red")]:
        if series is None:
            continue
        label = series["version"].iloc[0]
        ax_lat.plot(series["elapsed_s"], series["latency_s"], color=color, label=label, linewidth=1.8, alpha=0.85)
        ax_thr.plot(series["elapsed_s"], series["msg_count"], color=color, label=label, linewidth=1.8, alpha=0.85)

    ax_lat.set_ylabel("Latency (s)")
    ax_lat.set_title("E2E Latency: Spark 3.5 vs Spark 4.2")
    ax_lat.legend(fontsize=12)
    ax_lat.grid(True, alpha=0.3)

    ax_thr.set_xlabel("Elapsed Time (seconds since first message)")
    ax_thr.set_ylabel("Messages / second")
    ax_thr.set_title("Throughput: Spark 3.5 vs Spark 4.2")
    ax_thr.legend(fontsize=12)
    ax_thr.grid(True, alpha=0.3)

    fig.tight_layout()

    chart_path = os.path.join(RESULTS_DIR, "latency_chart.png")
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)
    print(f"Chart saved to {chart_path}")


if __name__ == "__main__":
    main()
