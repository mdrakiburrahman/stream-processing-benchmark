FROM rust:1.88 AS builder

WORKDIR /build

RUN git clone https://github.com/delta-io/kafka-delta-ingest.git . \
    && git checkout da9c932

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=kdi-cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=kdi-cargo-git \
    --mount=type=cache,target=target,id=kdi-azure-target \
    cargo build --release --features azure && cp target/release/kafka-delta-ingest .

FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get -y install \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir deltalake==0.25.5

WORKDIR /build

COPY --from=builder /build/kafka-delta-ingest ./
COPY entrypoint.sh /opt/entrypoint.sh
COPY create_table.py /opt/create_table.py
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
