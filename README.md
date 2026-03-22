# Stream Processing Benchmark

E2E latency benchmark: **Spark 3.5.8** vs **Spark 4.2.0-preview3** — Azure Event Hubs → Delta Lake on ADLS Gen2.

## Dev Setup

See [`contrib/README.md`](contrib/README.md).

## Prerequisites

> TODO: Automate this with bicep and only prompt the user for Subscription ID and RG Name, fire a single "run.sh".

1. Azure Event Hub with consumer groups `spark35` and `spark42`
2. ADLS Gen2 storage account + container (HNS enabled)
3. `cp .env.template .env` and fill in credentials

## Usage

```bash
# Specify duration in seconds and run benchmark
./src/.scripts/build-and-run.sh 180

# → results/latency_chart.png, results/summary.csv
```
