FROM images.feldera.com/feldera/pipeline-manager:v0.273.0@sha256:8e16a12eddd3d1f4da4a11fa8389891ac25e3b695bfa81b6724b94b691a56e48

COPY --chown=ubuntu:ubuntu Cargo.toml /home/ubuntu/runner/Cargo.toml
COPY --chown=ubuntu:ubuntu src/ /home/ubuntu/runner/src/

RUN cd /home/ubuntu/runner && cargo build --release \
    && cp target/release/feldera-runner /home/ubuntu/feldera-runner \
    && rm -rf /home/ubuntu/runner

ENTRYPOINT ["/home/ubuntu/feldera-runner"]
