# Stream Processing Benchmark

E2E latency and throughput benchmark:

```mermaid
graph LR
    subgraph Docker Compose
        P[Producer<br/><i>C# .NET</i>]
        S35[Spark Consumer<br/><i>Spark 3.5 / Scala 2.12</i>]
        S42[Spark Consumer<br/><i>Spark 4.2 / Scala 2.13</i>]
        A[Analytics<br/><i>PySpark</i>]
    end

    EH[Azure Event Hubs<br/><i>Kafka protocol</i>]
    ADLS[(ADLS Gen2<br/><i>Delta Lake</i>)]

    P -- "produce msgs" --> EH
    EH -- "consumer group: spark35" --> S35
    EH -- "consumer group: spark42" --> S42
    S35 -- "write Delta" --> ADLS
    S42 -- "write Delta" --> ADLS
    ADLS -- "read Delta" --> A
    A -- "latency &<br/>throughput charts" --> R[results/]
```

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
./src/.scripts/benchmark.sh 360
```

![Results](results/latency_chart.png)