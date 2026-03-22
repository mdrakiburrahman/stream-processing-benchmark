import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

RESULTS_DIR = "/opt/results"
BIN_MS = 50


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
    """Read Delta table and return a binned time series DataFrame."""
    try:
        df = spark.read.format("delta").load(delta_path)
        if not df.head(1):
            print(f"WARNING: {label} Delta table is empty, skipping.")
            return None
    except Exception as e:
        print(f"WARNING: Could not read {label} at {delta_path}: {e}")
        return None

    pdf = df.select("ts", "latency_ms").toPandas()
    print(f"{label}: loaded {len(pdf)} rows")

    pdf = pdf.sort_values("ts").reset_index(drop=True)
    ts = pd.to_datetime(pdf["ts"])
    pdf["elapsed_s"] = (ts - ts.iloc[0]).dt.total_seconds()
    pdf["latency_s"] = pdf["latency_ms"].astype(float) / 1000.0
    pdf["bin"] = (pdf["elapsed_s"] * 1000 // BIN_MS).astype(int)

    binned = pdf.groupby("bin").agg(
        elapsed_s=("elapsed_s", "mean"),
        latency_s=("latency_s", "mean"),
    ).reset_index(drop=True)
    binned["version"] = label
    return binned


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

    # Save combined time series CSV
    combined = pd.concat([s for s in [series_35, series_42] if s is not None], ignore_index=True)
    csv_path = os.path.join(RESULTS_DIR, "latency_timeseries.csv")
    combined.to_csv(csv_path, index=False)
    print(f"Time series saved to {csv_path}")

    # Plot
    fig, ax = plt.subplots(figsize=(14, 7))
    for series, color in [(series_35, "blue"), (series_42, "red")]:
        if series is None:
            continue
        label = series["version"].iloc[0]
        ax.plot(series["elapsed_s"], series["latency_s"],
                color=color, label=label, linewidth=1.8, alpha=0.85)

    ax.set_xlabel("Elapsed Time (seconds since first message)")
    ax.set_ylabel("Latency (s)")
    ax.set_title("E2E Latency: Spark 3.5 vs Spark 4.2")
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    chart_path = os.path.join(RESULTS_DIR, "latency_chart.png")
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)
    print(f"Chart saved to {chart_path}")


if __name__ == "__main__":
    main()
