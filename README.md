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

## Quickstart

Azure CLI (`az`) logged in with access to your target subscription:

```bash
az login
az account set -s <subscription-id>
```

### 1. Deploy Azure infrastructure

```bash
./src/infra/deploy.sh --subscription <subscription-id> --resource-group <rg-name>
```

This creates the Azure resource group (if needed), deploys Event Hubs and ADLS Gen2 via Bicep, and hydrates `.env` from `.env.template`.

### 2. Run benchmark

```bash
./src/.scripts/benchmark.sh 360
```

### 3. Destroy infrastructure

```bash
./src/infra/destroy.sh --subscription <subscription-id> --resource-group <rg-name>
```

This deletes the resource group and all resources within, and removes `.env`.

![Results](results/latency_chart.png)