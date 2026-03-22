import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


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


def compute_windowed_stats(df, label):
    """Compute latency statistics per 10-second time bucket."""
    return (
        df.groupBy(F.window("ts", "10 seconds"))
        .agg(
            F.count("*").alias("count"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.percentile_approx("latency_ms", 0.5).alias("p50"),
            F.percentile_approx("latency_ms", 0.95).alias("p95"),
            F.percentile_approx("latency_ms", 0.99).alias("p99"),
        )
        .withColumn("label", F.lit(label))
        .select(
            F.col("window.start").alias("window_start"),
            "count",
            "avg_latency_ms",
            "p50",
            "p95",
            "p99",
            "label",
        )
        .orderBy("window_start")
    )


def compute_overall_summary(df, label):
    """Compute overall summary statistics for a version."""
    row = df.agg(
        F.count("*").alias("total_rows"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.max("latency_ms").alias("max_latency_ms"),
        F.avg("latency_ms").alias("avg_latency_ms"),
        F.percentile_approx("latency_ms", 0.5).alias("p50"),
        F.percentile_approx("latency_ms", 0.95).alias("p95"),
        F.percentile_approx("latency_ms", 0.99).alias("p99"),
    ).collect()[0]

    return {
        "version": label,
        "total_rows": row["total_rows"],
        "min_latency_ms": row["min_latency_ms"],
        "max_latency_ms": row["max_latency_ms"],
        "avg_latency_ms": row["avg_latency_ms"],
        "p50": row["p50"],
        "p95": row["p95"],
        "p99": row["p99"],
    }


def plot_latency_chart(latency_35, latency_42, summary_rows, output_path):
    """Generate a line chart of latency over elapsed experiment time."""
    fig, ax = plt.subplots(figsize=(14, 7))

    for latency_arr, label, color in [
        (latency_35, "Spark 3.5", "blue"),
        (latency_42, "Spark 4.2", "red"),
    ]:
        if latency_arr is None:
            continue
        pdf = latency_arr.sort_values("ts").reset_index(drop=True)
        # Elapsed time from first message in seconds
        ts = pd.to_datetime(pdf["ts"])
        elapsed = (ts - ts.iloc[0]).dt.total_seconds()
        pdf["elapsed_s"] = elapsed
        pdf["latency_s"] = pdf["latency_ms"].astype(float) / 1000.0
        # Resample: bin into 50ms intervals and take mean
        pdf["bin"] = (pdf["elapsed_s"] * 1000 // 50).astype(int)
        binned = pdf.groupby("bin").agg(
            elapsed_s=("elapsed_s", "mean"),
            latency_s=("latency_s", "mean"),
        ).reset_index()
        ax.plot(binned["elapsed_s"], binned["latency_s"],
                color=color, label=label, linewidth=1.8, alpha=0.85)

    ax.set_xlabel("Elapsed Time (seconds since first message)")
    ax.set_ylabel("Latency (s)")
    ax.set_title("E2E Latency: Spark 3.5 vs Spark 4.2 (ProcessingTime, 0s trigger)")
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    print(f"Chart saved to {output_path}")


def main():
    spark = create_spark_session()

    account_name = os.environ["ADLSG2_ACCOUNT_NAME"]
    container = os.environ["ADLSG2_CONTAINER"]
    abfs_base = f"abfs://{container}@{account_name}.dfs.core.windows.net"

    path_35 = os.environ.get("DELTA_TABLE_PATH_35", "spark35/benchmark")
    path_42 = os.environ.get("DELTA_TABLE_PATH_42", "spark42/benchmark")

    results_dir = "/opt/results"
    os.makedirs(results_dir, exist_ok=True)

    # Read Delta tables
    latency_35 = None
    latency_42 = None
    summary_rows = []

    try:
        full_path_35 = f"{abfs_base}/{path_35}"
        df_35 = spark.read.format("delta").load(full_path_35)
        if df_35.head(1):
            latency_35 = df_35.select("ts", "latency_ms").toPandas()
            summary_rows.append(compute_overall_summary(df_35, "Spark 3.5"))
            print(f"Spark 3.5: loaded {summary_rows[-1]['total_rows']} rows")
        else:
            print("WARNING: Spark 3.5 Delta table is empty, skipping.")
    except Exception as e:
        print(f"WARNING: Could not read Spark 3.5 Delta table at {full_path_35}: {e}")

    try:
        full_path_42 = f"{abfs_base}/{path_42}"
        df_42 = spark.read.format("delta").load(full_path_42)
        if df_42.head(1):
            latency_42 = df_42.select("ts", "latency_ms").toPandas()
            summary_rows.append(compute_overall_summary(df_42, "Spark 4.2"))
            print(f"Spark 4.2: loaded {summary_rows[-1]['total_rows']} rows")
        else:
            print("WARNING: Spark 4.2 Delta table is empty, skipping.")
    except Exception as e:
        print(f"WARNING: Could not read Spark 4.2 Delta table at {full_path_42}: {e}")

    if not summary_rows:
        print("ERROR: No data available from either Delta table. Exiting.")
        spark.stop()
        sys.exit(1)

    # Plot chart
    chart_path = os.path.join(results_dir, "latency_chart.png")
    plot_latency_chart(latency_35, latency_42, summary_rows, chart_path)

    # Save summary CSV
    summary_df = pd.DataFrame(summary_rows)
    summary_path = os.path.join(results_dir, "summary.csv")
    summary_df.to_csv(summary_path, index=False)
    print(f"Summary saved to {summary_path}")

    # Print summary to stdout
    print("\n=== Benchmark Summary ===")
    print(summary_df.to_string(index=False))

    spark.stop()


if __name__ == "__main__":
    main()
